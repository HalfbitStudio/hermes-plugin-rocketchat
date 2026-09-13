"""DDP / WebSocket transport: realtime inbound stream with reconnect.

Frames the adapter sends and expects (DDP protocol version 1):

* ``connect``  -> server ``connected`` (or ``failed`` on version mismatch)
* ``method login {resume: <PAT>}`` -> ``result`` with ``error`` on a rejected token
* ``sub stream-room-messages ["__my_messages__", false]`` -> ``ready``/``nosub``
* ``changed`` frames whose ``fields.args`` is ``[message, room_meta]`` where
  ``room_meta`` is ``{roomParticipant, roomType, roomName}`` computed server-side
  for the bot (``notifications.module.ts``); every mutation of a message document
  republishes it through this stream (see ``helpers.is_mutation_republish``).
* server ``ping`` -> client ``pong``; aiohttp additionally sends protocol-level
  PINGs (``ROCKETCHAT_WS_HEARTBEAT_SECONDS``) so a half-open socket raises.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import uuid
from typing import Any, Dict, List, Optional

from .helpers import (
    _DDP_PROTOCOL_VERSION,
    _RECONNECT_BASE_DELAY,
    _RECONNECT_JITTER,
    _RECONNECT_MAX_DELAY,
    inbound_max_concurrency,
    websocket_endpoint_matches,
    websocket_url,
    ws_heartbeat_seconds,
)

logger = logging.getLogger(__name__)

_LOGIN_TIMEOUT_SECONDS = 20.0
_MAX_ROOM_LOCKS = 512
_DDP_FRAME_MAX_BYTES = 4 * 1024 * 1024


class DdpAuthError(RuntimeError):
    """The server rejected the resume token or the WebSocket handshake credentials."""


class DdpProtocolError(RuntimeError):
    """The server refused the DDP protocol version or answered with a protocol error."""


class DdpTransportMixin:
    """DDP WebSocket layer of :class:`~.adapter.RocketchatAdapter`."""

    # ------------------------------------------------------------------
    # Low-level frame helpers
    # ------------------------------------------------------------------

    async def _ddp_send(self, payload: Dict[str, Any]) -> None:
        """Send a DDP frame if the socket is open."""
        if not self._ws or self._ws.closed:
            return
        await self._ws.send_json(payload)

    async def _ddp_method(self, method: str, params: List[Any]) -> str:
        """Invoke a DDP method. Returns the method id; ``result`` frames are routed by id."""
        call_id = str(self._ddp_next_id)
        self._ddp_next_id += 1
        await self._ddp_send({
            "msg": "method",
            "method": method,
            "id": call_id,
            "params": params,
        })
        return call_id

    async def _ddp_sub(self, name: str, params: List[Any]) -> str:
        """Subscribe to a DDP publication. Returns the sub id."""
        sub_id = str(uuid.uuid4())
        self._ddp_subs[sub_id] = False
        await self._ddp_send({
            "msg": "sub",
            "id": sub_id,
            "name": name,
            "params": params,
        })
        return sub_id

    # ------------------------------------------------------------------
    # Connection loop
    # ------------------------------------------------------------------

    async def _ws_loop(self) -> None:
        """Connect to the DDP socket and listen for events, reconnecting on failure.

        Backoff resets only after a session in which the stream subscription became
        ready, so a server that accepts the socket and then closes it is not hammered
        every two seconds.  Permanent authentication failures stop the loop and are
        escalated through the gateway's fatal-error path instead of a bare return.
        """
        import aiohttp

        delay = _RECONNECT_BASE_DELAY
        while not self._closing:
            self._ddp_stream_ready = False
            try:
                await self._ws_connect_and_listen()
                if self._ddp_stream_ready:
                    delay = _RECONNECT_BASE_DELAY
            except asyncio.CancelledError:
                return
            except DdpAuthError as exc:
                if self._closing:
                    return
                logger.error("Rocket.Chat WS authentication failed: %s — stopping reconnect", exc)
                await self._fail_permanently("rocketchat_ddp_login_failed", str(exc))
                return
            except DdpProtocolError as exc:
                if self._closing:
                    return
                logger.error("Rocket.Chat DDP protocol error: %s — stopping reconnect", exc)
                await self._fail_permanently("rocketchat_ddp_protocol", str(exc))
                return
            except Exception as exc:
                if self._closing:
                    return
                if isinstance(exc, aiohttp.WSServerHandshakeError) and exc.status in (401, 403):
                    logger.error("Rocket.Chat WS handshake rejected (HTTP %d) — stopping reconnect", exc.status)
                    await self._fail_permanently(
                        "rocketchat_ws_handshake_rejected",
                        f"Rocket.Chat rejected the WebSocket handshake (HTTP {exc.status})",
                    )
                    return
                logger.warning(
                    "Rocket.Chat WS error (%s) — reconnecting in %.0fs",
                    type(exc).__name__,
                    delay,
                )

            if self._closing:
                return

            jitter = delay * _RECONNECT_JITTER * random.random()
            await asyncio.sleep(delay + jitter)
            delay = min(delay * 2, _RECONNECT_MAX_DELAY)

    async def _fail_permanently(self, code: str, message: str) -> None:
        """Report a non-retryable transport failure to Hermes and mark the adapter down."""
        try:
            self._set_fatal_error(code, message, retryable=False)
        except Exception:
            logger.debug("Rocket.Chat: could not record fatal error", exc_info=True)
        notify = getattr(self, "_notify_fatal_error", None)
        if callable(notify):
            try:
                await notify()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.debug("Rocket.Chat: fatal error notification failed", exc_info=True)

    async def _ws_connect_and_listen(self) -> None:
        """Single DDP WebSocket session: connect, login, subscribe, listen."""
        ws_url = websocket_url(self._base_url)
        logger.info("Rocket.Chat: DDP connecting")

        # A half-open socket raises nothing: without keepalive the read loop
        # below blocks forever, `_ws_loop` never reconnects, and inbound
        # traffic stops for good while outbound REST keeps working.  The
        # protocol-level PING is what converts that silence into an exception.
        heartbeat = ws_heartbeat_seconds()
        self._ws = await self._session.ws_connect(
            ws_url, heartbeat=heartbeat, max_msg_size=_DDP_FRAME_MAX_BYTES
        )
        response = getattr(self._ws, "_response", None)
        final_url = getattr(response, "url", None)
        if not websocket_endpoint_matches(ws_url, final_url):
            await self._ws.close()
            self._ws = None
            raise RuntimeError("Rocket.Chat WebSocket endpoint changed")

        self._ddp_subs.clear()
        self._ddp_login_id = None
        self._ddp_stream_sub_id = None
        self._ddp_logged_in = False
        self._ddp_auth_error = None
        self._ddp_protocol_error = None
        self._ddp_stream_ready = False

        try:
            await self._ddp_send({
                "msg": "connect",
                "version": _DDP_PROTOCOL_VERSION,
                "support": [_DDP_PROTOCOL_VERSION],
            })
            self._ddp_login_id = await self._ddp_method("login", [{"resume": self._token}])
            self._arm_login_watchdog()

            async for raw_msg in self._ws:
                if self._closing:
                    return

                if raw_msg.type in (raw_msg.type.TEXT, raw_msg.type.BINARY):
                    try:
                        event = json.loads(raw_msg.data)
                    except (json.JSONDecodeError, TypeError, ValueError):
                        continue
                    if not isinstance(event, dict):
                        continue
                    await self._handle_ddp_frame(event)
                    if self._ddp_auth_error or self._ddp_protocol_error:
                        break
                elif raw_msg.type in (
                    raw_msg.type.ERROR, raw_msg.type.CLOSE,
                    raw_msg.type.CLOSING, raw_msg.type.CLOSED,
                ):
                    logger.info("Rocket.Chat: DDP WebSocket closed (%s)", raw_msg.type)
                    break
        finally:
            self._disarm_login_watchdog()
            ws, self._ws = self._ws, None
            if ws is not None and not ws.closed:
                with contextlib.suppress(Exception):
                    await ws.close()

        if self._ddp_auth_error:
            raise DdpAuthError(self._ddp_auth_error)
        if self._ddp_protocol_error:
            raise DdpProtocolError(self._ddp_protocol_error)

    # ------------------------------------------------------------------
    # Login watchdog
    # ------------------------------------------------------------------

    def _arm_login_watchdog(self) -> None:
        self._disarm_login_watchdog()
        self._login_watchdog = asyncio.create_task(self._login_watchdog_body())

    def _disarm_login_watchdog(self) -> None:
        task = getattr(self, "_login_watchdog", None)
        self._login_watchdog = None
        if task is not None and not task.done():
            task.cancel()

    async def _login_watchdog_body(self) -> None:
        """Close the socket when the login result never arrives, so the loop reconnects."""
        try:
            await asyncio.sleep(_LOGIN_TIMEOUT_SECONDS)
        except asyncio.CancelledError:
            return
        if self._closing or self._ddp_logged_in:
            return
        logger.warning("Rocket.Chat: DDP login result not received in time — reconnecting")
        ws = self._ws
        if ws is not None and not ws.closed:
            with contextlib.suppress(Exception):
                await ws.close()

    # ------------------------------------------------------------------
    # Frame dispatch
    # ------------------------------------------------------------------

    @staticmethod
    def _error_summary(error: Any) -> str:
        """Content-free summary of a DDP error object (type/code/reason only)."""
        if not isinstance(error, dict):
            return "unknown error"
        parts = []
        for key in ("errorType", "error", "reason"):
            value = error.get(key)
            if isinstance(value, (str, int)) and str(value).strip():
                parts.append(f"{key}={str(value).strip()[:120]}")
        return ", ".join(parts) or "unknown error"

    async def _handle_ddp_frame(self, event: Dict[str, Any]) -> None:
        """Dispatch a single DDP frame."""
        kind = event.get("msg")
        if kind == "ping":
            pong: Dict[str, Any] = {"msg": "pong"}
            if "id" in event:
                pong["id"] = event["id"]
            await self._ddp_send(pong)
            return

        if kind == "connected":
            logger.debug("Rocket.Chat: DDP session established")
            return

        if kind == "failed":
            self._ddp_protocol_error = (
                f"server requires DDP version {event.get('version')!s}"
            )
            return

        if kind == "error":
            logger.warning("Rocket.Chat: DDP error frame (%s)", str(event.get("reason") or "")[:120])
            return

        if kind == "result":
            if event.get("id") != self._ddp_login_id:
                return
            error = event.get("error")
            if error:
                self._ddp_auth_error = (
                    "Rocket.Chat rejected the DDP resume token "
                    f"({self._error_summary(error)}); re-create ROCKETCHAT_TOKEN"
                )
                return
            self._ddp_logged_in = True
            self._disarm_login_watchdog()
            self._ddp_stream_sub_id = await self._ddp_sub(
                "stream-room-messages", ["__my_messages__", False]
            )
            logger.info("Rocket.Chat: DDP logged in")
            return

        if kind == "ready":
            subs = event.get("subs")
            if not isinstance(subs, list):
                return
            for sub_id in subs:
                if isinstance(sub_id, str):
                    self._ddp_subs[sub_id] = True
            if self._ddp_stream_sub_id in subs:
                self._ddp_stream_ready = True
                logger.info("Rocket.Chat: subscribed to the message stream")
            return

        if kind == "nosub":
            sub_id = event.get("id", "")
            err = event.get("error") or {}
            self._ddp_subs.pop(sub_id, None)
            if err:
                logger.warning(
                    "Rocket.Chat: DDP subscription rejected (%s)", self._error_summary(err)
                )
            if sub_id == self._ddp_stream_sub_id and self._ddp_stream_sub_id is not None:
                # The message stream is the only reason this socket exists.
                self._ddp_stream_ready = False
                ws = self._ws
                if ws is not None and not ws.closed:
                    with contextlib.suppress(Exception):
                        await ws.close()
            return

        if kind == "changed":
            if event.get("collection") != "stream-room-messages":
                return
            fields = event.get("fields")
            if not isinstance(fields, dict):
                return
            args = fields.get("args")
            if not isinstance(args, list) or not args or not isinstance(args[0], dict):
                return
            room_meta = args[1] if len(args) > 1 and isinstance(args[1], dict) else None
            self._spawn_inbound(args[0], room_meta)
            return

    # ------------------------------------------------------------------
    # Off-loop inbound processing
    # ------------------------------------------------------------------

    def _spawn_inbound(self, post: Dict[str, Any], room_meta: Optional[Dict[str, Any]]) -> None:
        """Process a frame in its own task so the read loop keeps answering pings.

        Frames from the same room are serialized (per-room lock) so a user's
        consecutive messages keep their order; different rooms proceed
        concurrently up to ``ROCKETCHAT_INBOUND_MAX_CONCURRENCY``.
        """
        task = asyncio.create_task(self._run_inbound(post, room_meta))
        self._inbound_tasks.add(task)
        task.add_done_callback(self._inbound_tasks.discard)

    def _inbound_slot(self) -> asyncio.Semaphore:
        limit = inbound_max_concurrency()
        semaphore = getattr(self, "_inbound_semaphore", None)
        if semaphore is None or getattr(self, "_inbound_semaphore_limit", None) != limit:
            semaphore = asyncio.Semaphore(limit)
            self._inbound_semaphore = semaphore
            self._inbound_semaphore_limit = limit
        return semaphore

    def _room_lock(self, room_id: str) -> asyncio.Lock:
        lock = self._room_locks.get(room_id)
        if lock is None:
            if len(self._room_locks) >= _MAX_ROOM_LOCKS:
                for key, candidate in list(self._room_locks.items()):
                    if not candidate.locked():
                        del self._room_locks[key]
                        break
            lock = asyncio.Lock()
            self._room_locks[room_id] = lock
        return lock

    async def _run_inbound(self, post: Dict[str, Any], room_meta: Optional[Dict[str, Any]]) -> None:
        room_id = post.get("rid") if isinstance(post, dict) else None
        lock = self._room_lock(room_id) if isinstance(room_id, str) and room_id else None
        try:
            async with self._inbound_slot():
                if lock is not None:
                    async with lock:
                        await self._handle_message(post, room_meta)
                else:
                    await self._handle_message(post, room_meta)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("Rocket.Chat inbound handling failed (%s)", type(exc).__name__, exc_info=True)

    async def _cancel_inbound_tasks(self) -> None:
        tasks = [task for task in list(self._inbound_tasks) if not task.done()]
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(BaseException):
                await task
        self._inbound_tasks.clear()
        self._room_locks.clear()
