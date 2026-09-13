"""Rocket.Chat gateway adapter: REST client, connection lifecycle,
message sending, and session-title→room-topic sync.

Transport, inbound handling, and media live in sibling modules
(ddp.py, inbound.py, media.py); plugin-level helpers in helpers.py.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
from collections import OrderedDict
from typing import Any, Callable, Dict, Optional

from gateway.config import Platform
from gateway.platforms.base import BasePlatformAdapter, SendResult, utf16_len
from gateway.platforms.helpers import MessageDeduplicator

from .ddp import DdpTransportMixin
from .helpers import (
    INBOUND_DEDUP_MAX_ENTRIES,
    INBOUND_DEDUP_TTL_SECONDS,
    MAX_MESSAGE_LENGTH,
    _ROOM_TYPE_MAP,
    _env_flag,
    build_delegation_envelope,
    env_get,
    is_valid_server_identifier,
    read_bounded_json_response,
    validate_auth_config,
)
from .inbound import InboundMixin
from .media import MediaMixin

logger = logging.getLogger(__name__)

_HERMES_HOME_CHANNEL_NOTICE = (
    "📬 No home channel is set for Rocketchat. "
    "A home channel is where Hermes delivers cron job results "
    "and cross-platform messages.\n\n"
    "Type /sethome to make this chat your home channel, "
    "or ignore to skip."
)
_MAX_ACTIVE_DELEGATION_ROOMS = 256
_MAX_REMEMBERED_SOURCES = 512


class RocketchatAdapter(
    InboundMixin, MediaMixin, DdpTransportMixin, BasePlatformAdapter
):
    """Gateway adapter for Rocket.Chat (self-hosted).

    Mixins come first so platform-specific hooks (reactions, media,
    DDP) override the BasePlatformAdapter defaults.
    """

    # Rocket.Chat enforces ``Message_MaxAllowedSize`` (default 5000) in UTF-16
    # code units (JavaScript ``String.length``); Hermes' delivery router and the
    # stream consumer read these attributes to split before sending.
    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH
    splits_long_messages = True

    @property
    def message_len_fn(self) -> Callable[[str], int]:
        return utf16_len

    def __init__(self, config, **kwargs):
        platform = Platform("rocketchat")
        super().__init__(config=config, platform=platform)

        extra = getattr(config, "extra", {}) or {}

        self._base_url: str = (
            extra.get("url", "")
            or env_get("ROCKETCHAT_URL", "")
        ).rstrip("/")
        self._token: str = (
            getattr(config, "token", None)
            or extra.get("token", "")
            or env_get("ROCKETCHAT_TOKEN", "")
        )
        self._bot_user_id: str = (
            extra.get("user_id", "")
            or env_get("ROCKETCHAT_USER_ID", "")
        )

        # Filled in by connect() once we look up the bot's username.
        self._bot_username: str = ""
        # Name the server expects in ``user-activity`` typing frames
        # (``UI_Use_Real_Name`` switches it to the display name).
        self._typing_name: str = ""
        self._lock_key: Optional[str] = None
        self._auth_failure_reported = False

        # aiohttp session + websocket handle
        self._session: Any = None  # aiohttp.ClientSession
        self._ws: Any = None       # aiohttp.ClientWebSocketResponse
        self._ws_task: Optional[asyncio.Task] = None
        self._reconnect_task: Optional[asyncio.Task] = None
        self._closing = False

        # DDP bookkeeping
        self._ddp_next_id = 1
        self._ddp_subs: Dict[str, bool] = {}  # sub-id -> ready
        self._ddp_login_id: Optional[str] = None
        self._ddp_stream_sub_id: Optional[str] = None
        self._ddp_logged_in = False
        self._ddp_stream_ready = False
        self._ddp_auth_error: Optional[str] = None
        self._ddp_protocol_error: Optional[str] = None
        self._login_watchdog: Optional[asyncio.Task] = None
        self._inbound_tasks: set = set()
        self._room_locks: Dict[str, asyncio.Lock] = {}
        self._inbound_semaphore: Optional[asyncio.Semaphore] = None
        self._inbound_semaphore_limit: Optional[int] = None

        # Room type cache (roomId -> "dm"/"group"/"channel").
        self._room_type_cache: Dict[str, str] = {}

        # Reply mode: "thread" to nest replies, "off" for flat messages.
        self._reply_mode: str = (
            extra.get("reply_mode", "")
            or env_get("ROCKETCHAT_REPLY_MODE", "off")
        ).lower()

        suppress_home_notice = (
            extra.get("suppress_home_channel_notice")
            if "suppress_home_channel_notice" in extra
            else env_get("ROCKETCHAT_SUPPRESS_HOME_CHANNEL_NOTICE", "false")
        )
        self._suppress_home_channel_notice = (
            str(suppress_home_notice).strip().lower()
            in {"1", "true", "yes", "on"}
        )

        # Dedup cache.  Hermes' 5-minute default is shorter than a working
        # conversation; a republished message id must still be recognized hours
        # later (second guard behind ``is_mutation_republish``).
        self._dedup = MessageDeduplicator(
            max_size=INBOUND_DEDUP_MAX_ENTRIES,
            ttl_seconds=INBOUND_DEDUP_TTL_SECONDS,
        )

        # Last admitted inbound source per room: topic sync and gateway media
        # delivery run after Hermes has cleared the task-local session context.
        self._last_source: OrderedDict[str, Any] = OrderedDict()

        # One-shot bot-to-bot tasks. Replies in these DM rooms carry a
        # terminal result envelope so they cannot start another agent turn.
        self._delegation_tasks: OrderedDict[str, str] = OrderedDict()

        # Title→topic sync state: rate-limit and last-known topic per room.
        self._last_topic_sync: Dict[str, float] = {}  # room_id → timestamp
        self._last_topic: Dict[str, str] = {}  # room_id → last known topic

    def _remember_delegation_task(
        self, room_id: str, delegation_id: str
    ) -> None:
        """Remember a bounded number of DM rooms awaiting terminal results."""
        if (
            not is_valid_server_identifier(room_id)
            or not re.fullmatch(r"[0-9a-f]{32}", delegation_id)
        ):
            return
        self._delegation_tasks[room_id] = delegation_id
        self._delegation_tasks.move_to_end(room_id)
        while len(self._delegation_tasks) > _MAX_ACTIVE_DELEGATION_ROOMS:
            self._delegation_tasks.popitem(last=False)

    def _remember_source(self, room_id: str, source: Any) -> None:
        """Keep a bounded map of the latest admitted inbound source per room."""
        if not is_valid_server_identifier(room_id) or source is None:
            return
        self._last_source[room_id] = source
        self._last_source.move_to_end(room_id)
        while len(self._last_source) > _MAX_REMEMBERED_SOURCES:
            self._last_source.popitem(last=False)

    def _decorate_delegation_result(self, room_id: str, content: str) -> str:
        """Mark a delegated-task reply as terminal, non-conversational data."""
        delegation_id = self._delegation_tasks.get(room_id)
        if not delegation_id:
            return content
        self._delegation_tasks.move_to_end(room_id)
        return build_delegation_envelope("result", delegation_id, content)

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _headers(self) -> Dict[str, str]:
        return {
            "X-Auth-Token": self._token,
            "X-User-Id": self._bot_user_id,
            "Content-Type": "application/json",
        }

    def _validate_auth_config(self) -> bool:
        """Validate the PAT target before any authenticated network access."""
        try:
            self._base_url, self._token, self._bot_user_id = (
                validate_auth_config(
                    self._base_url, self._token, self._bot_user_id
                )
            )
            return True
        except ValueError:
            logger.error("Rocket.Chat configuration is invalid")
            return False

    async def _rejected(self, method: str, path: str, resp: Any) -> Dict[str, Any]:
        """Log a rejected REST call with its content-free diagnostics and escalate 401."""
        error_type = ""
        try:
            body = await read_bounded_json_response(resp)
            for key in ("errorType", "error"):
                value = body.get(key)
                if isinstance(value, str) and value.strip():
                    error_type = value.strip()[:80]
                    break
        except Exception:
            pass
        retry_after = resp.headers.get("Retry-After") or resp.headers.get("X-RateLimit-Reset") or ""
        logger.error(
            "RC API %s %s rejected with HTTP %s%s%s",
            method,
            path.split("/", 1)[0],
            resp.status,
            f" ({error_type})" if error_type else "",
            f" retry-after={str(retry_after)[:32]}" if resp.status == 429 and retry_after else "",
        )
        if resp.status == 401 and not self._auth_failure_reported:
            self._auth_failure_reported = True
            logger.error(
                "Rocket.Chat rejected the Personal Access Token (HTTP 401); "
                "re-create ROCKETCHAT_TOKEN and restart the gateway"
            )
            self._set_fatal_error(
                "rocketchat_token_rejected",
                "Rocket.Chat rejected the Personal Access Token (HTTP 401)",
                retryable=False,
            )
            notify = getattr(self, "_notify_fatal_error", None)
            if callable(notify) and not self._closing:
                task = asyncio.create_task(notify())
                self._inbound_tasks.add(task)
                task.add_done_callback(self._inbound_tasks.discard)
        return {}

    async def _api_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """GET /api/v1/{path}."""
        import aiohttp
        if not self._validate_auth_config():
            return {}
        url = f"{self._base_url}/api/v1/{path.lstrip('/')}"
        try:
            async with self._session.get(
                url, headers=self._headers(), params=params,
                timeout=aiohttp.ClientTimeout(total=30),
                allow_redirects=False,
            ) as resp:
                if resp.status < 200 or resp.status >= 300:
                    return await self._rejected("GET", path, resp)
                return await read_bounded_json_response(resp)
        except (TimeoutError, aiohttp.ClientError, TypeError, ValueError) as exc:
            logger.error("RC API GET failed (%s)", type(exc).__name__)
            return {}

    async def _api_post(
        self, path: str, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """POST /api/v1/{path} with JSON body."""
        import aiohttp
        if not self._validate_auth_config():
            return {}
        url = f"{self._base_url}/api/v1/{path.lstrip('/')}"
        try:
            async with self._session.post(
                url, headers=self._headers(), json=payload,
                timeout=aiohttp.ClientTimeout(total=30),
                allow_redirects=False,
            ) as resp:
                if resp.status < 200 or resp.status >= 300:
                    return await self._rejected("POST", path, resp)
                return await read_bounded_json_response(resp)
        except (TimeoutError, aiohttp.ClientError, TypeError, ValueError) as exc:
            logger.error("RC API POST failed (%s)", type(exc).__name__)
            return {}

    # ------------------------------------------------------------------
    # Required overrides
    # ------------------------------------------------------------------

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Connect to Rocket.Chat and start the DDP listener.

        Rocket.Chat's DDP stream has no server-side update queue whose startup
        policy depends on ``is_reconnect``.  The argument is accepted to match
        the current Hermes platform-adapter contract.
        """
        import aiohttp

        if not self._validate_auth_config():
            return False

        # One PAT must not be driven by two gateways or profiles at once:
        # both would answer every message.
        lock_key = f"{self._base_url}:{self._bot_user_id}"
        try:
            from gateway.status import acquire_scoped_lock

            acquired, _holder = acquire_scoped_lock("rocketchat", lock_key)
            if not acquired:
                logger.error(
                    "Rocket.Chat: this bot account is already in use by another Hermes profile or gateway"
                )
                self._set_fatal_error(
                    "lock_conflict",
                    "Rocket.Chat bot account in use by another profile",
                    retryable=False,
                )
                return False
            self._lock_key = lock_key
        except ImportError:
            self._lock_key = None
        except Exception:
            logger.debug("Rocket.Chat: scoped lock unavailable", exc_info=True)
            self._lock_key = None

        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30), trust_env=False
        )
        self._closing = False
        self._auth_failure_reported = False

        # Verify credentials and fetch bot identity.
        me = await self._api_get("me")
        if not me or not me.get("success"):
            logger.error(
                "Rocket.Chat: failed to authenticate — check "
                "ROCKETCHAT_TOKEN, ROCKETCHAT_USER_ID, ROCKETCHAT_URL"
            )
            await self._session.close()
            self._release_scoped_lock()
            return False

        if me.get("_id") and me["_id"] != self._bot_user_id:
            logger.warning(
                "Rocket.Chat: ROCKETCHAT_USER_ID (%s) doesn't match /me (%s) — using /me",
                self._bot_user_id, me["_id"],
            )
            self._bot_user_id = me["_id"]
        self._bot_username = me.get("username", "")
        self._typing_name = await self._resolve_typing_name(me)
        logger.info(
            "Rocket.Chat: authenticated as @%s on %s",
            self._bot_username,
            self._base_url,
        )

        # Start DDP WebSocket in background.
        self._ws_task = asyncio.create_task(self._ws_loop())
        self._mark_connected()
        return True

    async def _resolve_typing_name(self, me: Dict[str, Any]) -> str:
        """Return the identity string the server accepts in ``user-activity`` frames.

        ``canType`` compares the argument against ``user.name`` when the workspace
        setting ``UI_Use_Real_Name`` is on and against ``user.username`` otherwise;
        a mismatch is dropped silently.  A failed lookup keeps the login name.
        """
        username = me.get("username") if isinstance(me.get("username"), str) else ""
        display = me.get("name") if isinstance(me.get("name"), str) else ""
        if not display:
            return username
        try:
            data = await self._api_get("settings.public", params={"_id": "UI_Use_Real_Name"})
        except Exception:
            return username
        settings = data.get("settings") if isinstance(data, dict) else None
        if isinstance(settings, list):
            for entry in settings:
                if isinstance(entry, dict) and entry.get("_id") == "UI_Use_Real_Name":
                    return display if entry.get("value") is True else username
        return username

    def _release_scoped_lock(self) -> None:
        if not self._lock_key:
            return
        with contextlib.suppress(Exception):
            from gateway.status import release_scoped_lock

            release_scoped_lock("rocketchat", self._lock_key)
        self._lock_key = None

    async def disconnect(self) -> None:
        """Disconnect from Rocket.Chat."""
        self._closing = True

        if self._ws_task and not self._ws_task.done():
            self._ws_task.cancel()
            try:
                await self._ws_task
            except (asyncio.CancelledError, Exception):
                pass

        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()

        cancel_inbound = getattr(self, "_cancel_inbound_tasks", None)
        if callable(cancel_inbound):
            with contextlib.suppress(Exception):
                await cancel_inbound()

        if self._ws:
            with contextlib.suppress(Exception):
                await self._ws.close()
            self._ws = None

        if self._session and not self._session.closed:
            await self._session.close()

        self._release_scoped_lock()
        self._mark_disconnected()
        logger.info("Rocket.Chat: disconnected")

    async def _thread_target_for_reply(
        self,
        chat_id: str,
        reply_to: Optional[str],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """Return the Rocket.Chat thread root for a non-DM reply.

        Hermes passes the root of an existing thread in metadata and the
        triggering message id in ``reply_to``.  DMs intentionally stay flat,
        even when ``ROCKETCHAT_REPLY_MODE=thread``.  Unknown room types also
        fail flat; normal inbound handling populates the cache before send().
        """
        thread_target = (metadata or {}).get("thread_id") or reply_to
        if self._reply_mode != "thread" or not thread_target:
            return None

        chat_type = self._room_type_cache.get(chat_id)
        if chat_type is None:
            # _resolve_room_type() caches only verified API results. Its
            # user-facing fallback is "channel", but an unresolved target
            # must stay flat so a transient lookup failure cannot thread a DM.
            await self._resolve_room_type(chat_id)
            chat_type = self._room_type_cache.get(chat_id)

        if chat_type not in ("channel", "group"):
            return None
        return thread_target

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a message (or multiple chunks) to a room."""
        if not content:
            return SendResult(success=True)
        if (
            self._suppress_home_channel_notice
            and content == _HERMES_HOME_CHANNEL_NOTICE
        ):
            logger.debug(
                "Rocket.Chat: suppressed Hermes home-channel onboarding notice"
            )
            return SendResult(success=True)

        formatted = self.format_message(content)
        if not formatted:
            # Only delivery directives were present; the media path delivers them.
            return SendResult(success=True)
        delegation_id = self._delegation_tasks.get(chat_id)
        if delegation_id:
            envelope_overhead = utf16_len(
                build_delegation_envelope("result", delegation_id, "")
            )
            chunks = self.truncate_message(
                formatted, MAX_MESSAGE_LENGTH - envelope_overhead, len_fn=utf16_len
            )
            chunks = [
                build_delegation_envelope("result", delegation_id, chunk)
                for chunk in chunks
            ]
            self._delegation_tasks.move_to_end(chat_id)
        else:
            chunks = self.truncate_message(formatted, MAX_MESSAGE_LENGTH, len_fn=utf16_len)
        thread_target = await self._thread_target_for_reply(
            chat_id, reply_to, metadata
        )

        last_id = None
        for chunk in chunks:
            payload: Dict[str, Any] = {
                "roomId": chat_id,
                "text": chunk,
            }
            if thread_target:
                payload["tmid"] = thread_target

            data = await self._api_post("chat.postMessage", payload)
            if not isinstance(data, dict) or data.get("success") is not True:
                return SendResult(success=False, error="Failed to post message")
            msg = data.get("message")
            returned_tmid = msg.get("tmid") if isinstance(msg, dict) else None
            if (
                not isinstance(msg, dict)
                or not is_valid_server_identifier(msg.get("_id"))
                or msg.get("rid") != chat_id
                or (returned_tmid or None) != (thread_target or None)
            ):
                return SendResult(
                    success=False, error="Rocket.Chat returned an invalid message target"
                )
            last_id = msg["_id"]
            logger.debug(
                "Rocket.Chat: posted message (threaded=%s)", bool(msg.get("tmid"))
            )

        # After sending, sync session title → RC topic for DMs.
        # This fires on every outgoing message but is rate-limited and
        # short-circuits when the title hasn't changed.
        if self._topic_sync_enabled():
            try:
                await self._sync_title_to_rc_topic(chat_id)
            except Exception:
                logger.debug("Title sync failed in send()", exc_info=True)

        return SendResult(success=True, message_id=last_id)

    @staticmethod
    def _set_topic_endpoint(chat_type: str) -> str:
        """Return the RC endpoint key for setting a room topic based on room type."""
        return {
            "dm": "dm.setTopic",
            "channel": "channels.setTopic",
            "group": "groups.setTopic",
        }.get(chat_type, "channels.setTopic")

    @staticmethod
    def _topic_sync_enabled() -> bool:
        """Topic writes are an independent, default-off PAT capability."""
        return _env_flag("ROCKETCHAT_TOPIC_SYNC", default=False)

    async def _sync_title_to_rc_topic(self, chat_id: str) -> None:
        """Sync Hermes session title to RC room topic for DMs/groups/channels.

        Called after every outgoing send().  Checks the current session title
        and updates the RC topic if they differ.  This covers:
          - Auto-title (first-reply title generated by Hermes)
          - /title command (already handled in _handle_message, but also
            catches manual session_db / CLI rename changes that happened
            between messages)
        Rate-limited to at most once every 5 seconds per room.
        """
        import time
        now = time.time()
        if chat_id in self._last_topic_sync and now - self._last_topic_sync[chat_id] < 5:
            return
        self._last_topic_sync[chat_id] = now

        # Only for DM/group/channel rooms where topic setting makes sense
        chat_type = self._room_type_cache.get(chat_id)
        if not chat_type:
            try:
                chat_type = await self._resolve_room_type(chat_id)
            except Exception:
                return
        if chat_type not in ("dm", "group", "channel"):
            return

        # Look up the session of the last admitted inbound message in this room
        # without creating one: the source carries the real chat type, user and
        # thread, which is what the session key is built from.
        session_store = getattr(self, "_session_store", None)
        source = self._last_source.get(chat_id)
        if not session_store or source is None:
            return
        try:
            from gateway.session import build_session_key

            store_cfg = getattr(session_store, "config", None)
            session_key = build_session_key(
                source,
                group_sessions_per_user=(
                    getattr(store_cfg, "group_sessions_per_user", True) if store_cfg else True
                ),
                thread_sessions_per_user=(
                    getattr(store_cfg, "thread_sessions_per_user", False) if store_cfg else False
                ),
            )
            session_store._ensure_loaded()
            entry = session_store._entries.get(session_key)
        except Exception as exc:
            logger.debug("Title sync: session lookup failed: %s", type(exc).__name__)
            return
        if entry is None:
            return

        # Get the session title from the SQLite DB
        db = getattr(session_store, "_db", None)
        if not db:
            return
        try:
            title = db.get_session_title(entry.session_id)
        except Exception as exc:
            logger.debug("Title sync: get_title failed: %s", type(exc).__name__)
            return
        if not title:
            return

        # Get the current RC topic
        data = await self._api_get("rooms.info", params={"roomId": chat_id})
        room = (data or {}).get("room") or {}
        if not isinstance(room, dict) or room.get("_id") != chat_id:
            return
        current_topic = (room.get("topic") or "").strip()

        # Only call the API if topics differ
        if title != current_topic:
            endpoint = self._set_topic_endpoint(chat_type)
            try:
                resp = await self._api_post(endpoint, {
                    "roomId": chat_id,
                    "topic": title,
                })
                if resp and resp.get("success"):
                    self._last_topic[chat_id] = title
                    logger.debug("Rocket.Chat: synced session title to %s topic", chat_type)
            except Exception as exc:
                logger.debug("Title sync: %s failed: %s", endpoint, exc)
        else:
            # Already in sync — just update the cache
            self._last_topic[chat_id] = current_topic

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Return room name and type.

        Rocket.Chat exposes one unified ``rooms.info`` endpoint that works
        for channels, private groups, and DMs.
        """
        data = await self._api_get("rooms.info", params={"roomId": chat_id})
        room = (data or {}).get("room") or {}
        if not isinstance(room, dict) or room.get("_id") != chat_id:
            return {"name": chat_id, "type": "channel"}

        raw_type = room.get("t")
        chat_type = _ROOM_TYPE_MAP.get(raw_type, "channel")
        if raw_type in _ROOM_TYPE_MAP:
            self._room_type_cache[chat_id] = chat_type

        if chat_type == "dm":
            others = [
                u for u in room.get("usernames", [])
                if u and u != self._bot_username
            ]
            name = others[0] if others else chat_id
        else:
            name = room.get("fname") or room.get("name") or chat_id

        return {"name": name, "type": chat_type, "chat_id": chat_id}

    # ------------------------------------------------------------------
    # Optional overrides
    # ------------------------------------------------------------------

    async def send_typing(
        self, chat_id: str, metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Notify that the bot is typing.

        Rocket.Chat 6.x+ replaced the legacy ``/typing`` stream with
        ``/user-activity``, and 8.x expects the activity string ``"user-typing"``.
        """
        await self._emit_user_activity(chat_id, ["user-typing"], metadata)

    async def stop_typing(
        self, chat_id: str, metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Clear the typing indicator (empty user-activity list)."""
        await self._emit_user_activity(chat_id, [], metadata)

    async def _emit_user_activity(
        self, chat_id: str, activities: list, metadata: Optional[Dict[str, Any]]
    ) -> None:
        if not self._ws or self._ws.closed or not self._ddp_logged_in:
            return
        name = self._typing_name or self._bot_username
        if not name:
            return
        extras: Dict[str, Any] = {}
        thread_id = (metadata or {}).get("thread_id") if isinstance(metadata, dict) else None
        if is_valid_server_identifier(thread_id) and self._room_type_cache.get(chat_id) in ("channel", "group"):
            extras["tmid"] = thread_id
        await self._ddp_method(
            "stream-notify-room",
            [f"{chat_id}/user-activity", name, activities, extras],
        )

    async def delete_message(self, chat_id: str, message_id: str) -> bool:
        """Delete one of the bot's own messages via ``chat.delete``."""
        if not is_valid_server_identifier(chat_id) or not is_valid_server_identifier(message_id):
            return False
        data = await self._api_post("chat.delete", {"roomId": chat_id, "msgId": message_id})
        return bool(isinstance(data, dict) and data.get("success") is True)

    async def edit_message(
        self, chat_id: str, message_id: str, content: str, *, finalize: bool = False
    ) -> SendResult:
        """Edit an existing message via chat.update."""
        formatted = self._decorate_delegation_result(
            chat_id, self.format_message(content)
        )
        data = await self._api_post(
            "chat.update",
            {"roomId": chat_id, "msgId": message_id, "text": formatted},
        )
        if not isinstance(data, dict) or data.get("success") is not True:
            return SendResult(success=False, error="Failed to edit message")
        msg = data.get("message")
        if (
            not isinstance(msg, dict)
            or msg.get("_id") != message_id
            or msg.get("rid") != chat_id
        ):
            return SendResult(
                success=False, error="Rocket.Chat returned an invalid message target"
            )
        return SendResult(success=True, message_id=message_id)

    def format_message(self, content: str) -> str:
        """Rocket.Chat renders Markdown natively and previews plain image
        URLs — strip image markdown to match Mattermost's behavior.

        Also strip Hermes-internal delivery directives (MEDIA:,
        [[audio_as_voice]], [[image]], [[file]]) — the gateway already
        delivers media via send_voice/send_image/send_document methods,
        and these tokens must not reach the Rocket.Chat API as text.
        """
        # Strip image markdown: ![alt](url) → url
        content = re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", r"\2", content)
        # Strip whole lines that are delivery directives.  ``MEDIA`` needs the
        # colon and a path token so prose such as "MEDIA coverage" survives.
        content = re.sub(
            r"(?m)^[ \t]*(?:\[\[audio_as_voice\]\]|\[\[image\]\]|\[\[file\]\])[ \t]*:?.*(?:\n|$)",
            "",
            content,
        )
        content = re.sub(r"(?m)^[ \t]*MEDIA:[ \t]*\S.*(?:\n|$)", "", content)
        # Also strip orphan MEDIA: references not at line start
        content = re.sub(r"\s*MEDIA:\S+\s*", " ", content)
        return content.strip()
