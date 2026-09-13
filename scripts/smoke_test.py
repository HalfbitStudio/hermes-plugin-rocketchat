#!/usr/bin/env python3
"""Live smoke test: drive the real adapter against a Rocket.Chat workspace.

No gateway and no model are involved.  The adapter is instantiated with a
recording message handler; a second ("human") user drives Rocket.Chat over REST
and the script checks what the adapter did.  Every write lands in a dedicated
test channel and the bot/human DM and is deleted at the end (unless --keep).

    python scripts/smoke_test.py --env-file ~/.rc-smoke.env

Env file (KEY=VALUE lines):
    RC_SMOKE_URL                  workspace URL
    RC_SMOKE_BOT_USER_ID          bot user _id
    RC_SMOKE_BOT_TOKEN            bot Personal Access Token
    RC_SMOKE_HUMAN_USER_ID        a test user's _id (never an admin)
    RC_SMOKE_HUMAN_USERNAME       that user's login name
    RC_SMOKE_HUMAN_TOKEN          that user's Personal Access Token
    RC_SMOKE_ADMIN_USER_ID / RC_SMOKE_ADMIN_TOKEN   optional; enables the setting-flip check
    RC_SMOKE_ALLOW_INSECURE_HTTP=true               for an http:// workspace
    RC_SMOKE_ALLOW_PRIVATE_FILE_REDIRECTS=true      for object storage on a private address
    RC_SMOKE_EXPECT_REDIRECT=true                   assert the attachment came via a redirect
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.util
import json
import logging
import os
import secrets
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_NAME = "rocketchat_smoke_plugin"


# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------


def load_env_file(path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def setup_hermes(hermes_path: Path) -> None:
    if not (hermes_path / "gateway").is_dir():
        raise SystemExit(f"hermes-agent checkout not found at {hermes_path}; pass --hermes")
    sys.path.insert(0, str(hermes_path))
    home = Path(tempfile.mkdtemp(prefix="rc-smoke-home-"))
    os.environ["HERMES_HOME"] = str(home)
    from gateway.platform_registry import PlatformEntry, platform_registry

    if not platform_registry.is_registered("rocketchat"):
        platform_registry.register(PlatformEntry(
            name="rocketchat", label="Rocket.Chat", adapter_factory=lambda cfg: None, check_fn=lambda: True,
        ))


def load_plugin():
    if MODULE_NAME in sys.modules:
        return sys.modules[MODULE_NAME]
    spec = importlib.util.spec_from_file_location(
        MODULE_NAME, REPO_ROOT / "__init__.py", submodule_search_locations=[str(REPO_ROOT)]
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[MODULE_NAME] = module
    spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class Api:
    """Minimal REST client for one Rocket.Chat user."""

    def __init__(self, base_url: str, token: str, user_id: str):
        import aiohttp

        self.base_url = base_url.rstrip("/")
        self.headers = {"X-Auth-Token": token, "X-User-Id": user_id}
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60), trust_env=False)

    async def close(self) -> None:
        await self.session.close()

    async def _call(self, method: str, path: str, **kwargs) -> Dict[str, Any]:
        async with self.session.request(method, f"{self.base_url}/api/v1/{path}", headers=self.headers, **kwargs) as resp:
            text = await resp.text()
            try:
                data = json.loads(text) if text else {}
            except ValueError:
                data = {"raw": text[:300]}
            if resp.status >= 300:
                raise RuntimeError(f"{method} {path} -> HTTP {resp.status}: {text[:300]}")
            return data

    async def get(self, path: str, **params) -> Dict[str, Any]:
        return await self._call("GET", path, params={k: v for k, v in params.items() if v is not None})

    async def post(self, path: str, **body) -> Dict[str, Any]:
        return await self._call("POST", path, json=body)

    async def upload(self, room_id: str, filename: str, data: bytes, content_type: str, caption: str = "") -> str:
        import aiohttp

        form = aiohttp.FormData()
        form.add_field("file", data, filename=filename, content_type=content_type)
        step1 = await self._call("POST", f"rooms.media/{room_id}", data=form)
        file_id = step1["file"]["_id"]
        payload = {"msg": caption} if caption else {}
        step2 = await self._call("POST", f"rooms.mediaConfirm/{room_id}/{file_id}", json=payload)
        return step2["message"]["_id"]


class Recorder:
    """Stands in for the Hermes runner: records dispatched MessageEvents."""

    def __init__(self):
        self.events: List[Any] = []
        self._changed = asyncio.Event()

    async def __call__(self, event) -> None:
        self.events.append(event)
        self._changed.set()

    async def wait_for(self, predicate: Callable[[Any], bool], timeout: float = 15.0):
        deadline = time.monotonic() + timeout
        while True:
            for event in self.events:
                if predicate(event):
                    return event
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            self._changed.clear()
            try:
                await asyncio.wait_for(self._changed.wait(), timeout=remaining)
            except TimeoutError:
                return None

    def count(self, predicate: Callable[[Any], bool]) -> int:
        return sum(1 for event in self.events if predicate(event))


class LogCapture(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.records: List[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)

    def find(self, needle: str) -> List[str]:
        return [r.getMessage() for r in self.records if needle in r.getMessage()]


class Ctx:
    def __init__(self):
        self.results: List[tuple[str, str, str]] = []
        self.rc = None
        self.adapter = None
        self.recorder = Recorder()
        self.logs = LogCapture()
        self.env: Dict[str, str] = {}
        self.bot: Optional[Api] = None
        self.human: Optional[Api] = None
        self.admin: Optional[Api] = None
        self.nonce = secrets.token_hex(3)
        self.dm_room: Optional[str] = None
        self.channel: Optional[str] = None
        self.bot_username = ""
        self.bot_name = ""
        self.redirects: List[str] = []
        self.keep = False

    def record(self, name: str, status: str, detail: str = "") -> None:
        self.results.append((name, status, detail))
        print(f"  [{status:4}] {name}{': ' + detail if detail else ''}")


async def check(ctx: Ctx, name: str, coro) -> None:
    try:
        detail = await coro
        ctx.record(name, "PASS", detail or "")
    except SkipCheck as exc:
        ctx.record(name, "SKIP", str(exc))
    except Exception as exc:  # noqa: BLE001 - every check failure must be reported, not raised
        ctx.record(name, "FAIL", f"{type(exc).__name__}: {exc}")


class SkipCheck(Exception):
    pass


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------


async def check_connect(ctx: Ctx) -> str:
    ok = await ctx.adapter.connect()
    if not ok:
        raise RuntimeError("connect() returned False; see log")
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline and not ctx.adapter._ddp_stream_ready:
        await asyncio.sleep(0.2)
    if not ctx.adapter._ddp_stream_ready:
        raise RuntimeError("stream subscription never became ready")
    ctx.bot_username = ctx.adapter._bot_username
    me = await ctx.bot.get("me")
    ctx.bot_name = me.get("name") or ""
    return f"logged in as @{ctx.bot_username}, stream ready, typing identity '{ctx.adapter._typing_name}'"


async def check_dm_roundtrip(ctx: Ctx) -> str:
    room = await ctx.human.post("im.create", username=ctx.bot_username)
    ctx.dm_room = room["room"]["_id"]
    text = f"smoke dm {ctx.nonce}"
    posted = await ctx.human.post("chat.postMessage", roomId=ctx.dm_room, text=text)
    message_id = posted["message"]["_id"]
    event = await ctx.recorder.wait_for(lambda e: e.text == text)
    if event is None:
        raise RuntimeError("DM never reached the adapter")
    if event.source.chat_type != "dm" or event.source.user_id != ctx.env["RC_SMOKE_HUMAN_USER_ID"]:
        raise RuntimeError(f"unexpected source {event.source}")
    if event.message_id != message_id:
        raise RuntimeError("message id mismatch")
    reply = await ctx.adapter.send(ctx.dm_room, f"smoke reply {ctx.nonce} 😀")
    if not reply.success or not reply.message_id:
        raise RuntimeError(f"send failed: {reply.error}")
    seen = await ctx.human.get("chat.getMessage", msgId=reply.message_id)
    if seen["message"]["rid"] != ctx.dm_room:
        raise RuntimeError("reply landed in the wrong room")
    ctx.reply_id = reply.message_id
    ctx.dm_event = event
    return f"event recorded, reply {reply.message_id} delivered"


async def check_reactions(ctx: Ctx) -> str:
    from gateway.platforms.base import ProcessingOutcome

    event = ctx.dm_event
    await ctx.adapter.on_processing_start(event)
    await asyncio.sleep(0.5)
    reactions = (await ctx.human.get("chat.getMessage", msgId=event.message_id))["message"].get("reactions") or {}
    if ctx.bot_username not in (reactions.get(":eyes:") or {}).get("usernames", []):
        raise RuntimeError(f"👀 not set: {reactions}")
    await ctx.adapter.on_processing_complete(event, ProcessingOutcome.SUCCESS)
    await asyncio.sleep(0.5)
    reactions = (await ctx.human.get("chat.getMessage", msgId=event.message_id))["message"].get("reactions") or {}
    if ":eyes:" in reactions:
        raise RuntimeError(f"👀 still present after completion: {reactions}")
    if ctx.bot_username not in (reactions.get(":white_check_mark:") or {}).get("usernames", []):
        raise RuntimeError(f"✅ not set: {reactions}")
    # Idempotence: removing again must not re-add.
    await ctx.adapter._remove_reaction(event.message_id, ":eyes:")
    await asyncio.sleep(0.3)
    reactions = (await ctx.human.get("chat.getMessage", msgId=event.message_id))["message"].get("reactions") or {}
    if ":eyes:" in reactions:
        raise RuntimeError("second removal toggled 👀 back on")
    return "👀 → ✅, removal idempotent"


async def check_republish(ctx: Ctx) -> str:
    created = await ctx.human.post("channels.create", name=f"smoke-{ctx.nonce}", members=[ctx.bot_username])
    ctx.channel = created["channel"]["_id"]
    root_text = f"@{ctx.bot_username} smoke root {ctx.nonce}"
    root = (await ctx.human.post("chat.postMessage", roomId=ctx.channel, text=root_text))["message"]
    root_id = root["_id"]
    event = await ctx.recorder.wait_for(lambda e: e.message_id == root_id)
    if event is None:
        raise RuntimeError("thread root never reached the adapter")
    if event.source.thread_id != root_id:
        raise RuntimeError("thread mode did not make the root its own thread id")
    for index in range(3):
        await ctx.human.post("chat.postMessage", roomId=ctx.channel, text=f"reply {index} {ctx.nonce}", tmid=root_id)
    await ctx.human.post("chat.react", messageId=root_id, emoji=":thumbsup:", shouldReact=True)
    await ctx.human.post("chat.pinMessage", messageId=root_id)
    await asyncio.sleep(3)
    root_events = ctx.recorder.count(lambda e: e.message_id == root_id)
    if root_events != 1:
        raise RuntimeError(f"thread root was dispatched {root_events} times (republish leak)")
    mentioned_reply = (await ctx.human.post(
        "chat.postMessage", roomId=ctx.channel, text=f"@{ctx.bot_username} in thread {ctx.nonce}", tmid=root_id,
    ))["message"]
    reply_event = await ctx.recorder.wait_for(lambda e: e.message_id == mentioned_reply["_id"])
    if reply_event is None:
        raise RuntimeError("mentioned thread reply was not dispatched")
    if reply_event.source.thread_id != root_id:
        raise RuntimeError("thread reply lost its thread id")
    if not (reply_event.channel_context or "").strip():
        raise RuntimeError("first turn in the thread carried no thread context")
    await asyncio.sleep(2)
    if ctx.recorder.count(lambda e: e.message_id == root_id) != 1:
        raise RuntimeError("thread root re-dispatched after the reply")
    return "root dispatched once across 3 replies, a reaction and a pin; mentioned reply got thread context"


async def check_attachment(ctx: Ctx) -> str:
    payload = f"smoke attachment {ctx.nonce}\n".encode() * 200
    digest = hashlib.sha256(payload).hexdigest()
    filename = f"smoke-{ctx.nonce}.txt"
    message_id = await ctx.human.upload(ctx.dm_room, filename, payload, "text/plain", caption=f"file {ctx.nonce}")
    event = await ctx.recorder.wait_for(lambda e: e.message_id == message_id, timeout=30)
    if event is None:
        raise RuntimeError("attachment message never reached the adapter")
    if not event.media_urls:
        raise RuntimeError("event has no media (download failed; see log)")
    local = Path(event.media_urls[0])
    if hashlib.sha256(local.read_bytes()).hexdigest() != digest:
        raise RuntimeError("downloaded bytes do not match the upload")
    expect_redirect = ctx.env.get("RC_SMOKE_EXPECT_REDIRECT", "").lower() == "true"
    if expect_redirect and not ctx.redirects:
        raise RuntimeError("expected a /file-upload redirect but the download was direct")
    via = f"via redirect to {ctx.redirects[-1].split('?')[0]}" if ctx.redirects else "direct download"
    return f"{len(payload)} bytes match, {via}"


async def check_typing_identity(ctx: Ctx) -> str:
    me = {"username": ctx.bot_username, "name": ctx.bot_name}
    public = await ctx.bot.get("settings.public", _id="UI_Use_Real_Name")
    entries = [s for s in public.get("settings", []) if s.get("_id") == "UI_Use_Real_Name"]
    if not entries:
        raise RuntimeError(f"settings.public did not return UI_Use_Real_Name: {public}")
    use_real_name = entries[0].get("value") is True
    expected = ctx.bot_name if (use_real_name and ctx.bot_name) else ctx.bot_username
    resolved = await ctx.adapter._resolve_typing_name(me)
    if resolved != expected:
        raise RuntimeError(f"resolved '{resolved}', expected '{expected}' (UI_Use_Real_Name={use_real_name})")
    if ctx.admin is None or not ctx.bot_name:
        return f"UI_Use_Real_Name={use_real_name} → '{resolved}' (flip check skipped: no admin token)"
    try:
        await ctx.admin.post("settings/UI_Use_Real_Name", value=not use_real_name)
        await asyncio.sleep(1)
        flipped = await ctx.adapter._resolve_typing_name(me)
        if flipped == resolved:
            raise RuntimeError("typing identity did not follow the flipped setting")
    finally:
        await ctx.admin.post("settings/UI_Use_Real_Name", value=use_real_name)
    await ctx.adapter._emit_user_activity(ctx.dm_room, ["user-typing"], None)
    await ctx.adapter._emit_user_activity(ctx.dm_room, [], None)
    return f"'{resolved}' with UI_Use_Real_Name={use_real_name}; flips to '{flipped}'"


async def check_dm_tool(ctx: Ctx) -> str:
    from gateway import session_context

    tokens = session_context.set_session_vars(
        platform="rocketchat", chat_id=ctx.dm_room, user_id=ctx.env["RC_SMOKE_HUMAN_USER_ID"],
        session_key=f"rocketchat:{ctx.dm_room}",
    )
    try:
        out = json.loads(await ctx.rc.tools.handle_dm({"username": ctx.env["RC_SMOKE_HUMAN_USERNAME"]}))
        if out.get("room_id") != ctx.dm_room:
            raise RuntimeError(f"handle_dm returned {out}")
        ghost = json.loads(await ctx.rc.tools.handle_dm({"username": f"nobody-{ctx.nonce}"}))
        if not ghost.get("error"):
            raise RuntimeError(f"unknown username was accepted: {ghost}")
        sent = json.loads(await ctx.rc.tools.handle_delegate({
            "username": ctx.env["RC_SMOKE_HUMAN_USERNAME"], "message": f"smoke delegate {ctx.nonce}",
        }))
        if not sent.get("delegation_id"):
            raise RuntimeError(f"delegate failed: {sent}")
    finally:
        session_context.clear_session_vars(tokens)
    return f"DM verified after im.create re-read, ghost room rejected ({ghost['error'][:60]})"


async def check_standalone_send(ctx: Ctx) -> str:
    from gateway.config import PlatformConfig

    with tempfile.NamedTemporaryFile("wb", suffix=".txt", delete=False, prefix="smoke-cron-") as handle:
        handle.write(f"cron attachment {ctx.nonce}\n".encode())
        path = handle.name
    pconfig = PlatformConfig(enabled=True, extra={
        "url": ctx.env["RC_SMOKE_URL"], "token": ctx.env["RC_SMOKE_BOT_TOKEN"], "user_id": ctx.env["RC_SMOKE_BOT_USER_ID"],
    })
    result = await ctx.rc.helpers._standalone_send(pconfig, ctx.dm_room, f"smoke cron {ctx.nonce}", media_files=[path])
    os.unlink(path)
    if not result.get("success"):
        raise RuntimeError(f"standalone send failed: {result}")
    history = await ctx.human.get("im.history", roomId=ctx.dm_room, count=5)
    with_file = [m for m in history.get("messages", []) if m.get("file") and m["_id"] == result["message_id"]]
    if not with_file:
        raise RuntimeError("cron attachment not visible in history")
    return f"text + file delivered ({result['delivered_files']} file), message {result['message_id']}"


async def check_delete(ctx: Ctx) -> str:
    if not await ctx.adapter.delete_message(ctx.dm_room, ctx.reply_id):
        raise RuntimeError("delete_message returned False")
    try:
        await ctx.human.get("chat.getMessage", msgId=ctx.reply_id)
    except RuntimeError:
        return "bot reply deleted"
    raise RuntimeError("message still readable after delete")


async def check_bad_token(ctx: Ctx) -> str:
    import aiohttp

    from gateway.config import PlatformConfig

    bad = ctx.rc.RocketchatAdapter(PlatformConfig(enabled=True, extra={
        "url": ctx.env["RC_SMOKE_URL"], "token": "smoke-invalid-token", "user_id": ctx.env["RC_SMOKE_BOT_USER_ID"],
    }))
    bad._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30), trust_env=False)
    try:
        try:
            await asyncio.wait_for(bad._ws_connect_and_listen(), timeout=30)
        except ctx.rc.ddp.DdpAuthError as exc:
            detail = str(exc)
        else:
            raise RuntimeError("DDP login with an invalid token did not raise DdpAuthError")
        bad._set_fatal_error = lambda code, message, *, retryable: setattr(bad, "_fatal", (code, retryable))
        bad._ddp_next_id = 1
        await asyncio.wait_for(bad._ws_loop(), timeout=30)
        if getattr(bad, "_fatal", None) != ("rocketchat_ddp_login_failed", False):
            raise RuntimeError(f"fatal error not reported: {getattr(bad, '_fatal', None)}")
        await bad._session.close()  # connect() opens and closes its own session
        if await bad.connect():
            raise RuntimeError("connect() succeeded with an invalid token")
    finally:
        if bad._session is not None and not bad._session.closed:
            await bad._session.close()
    return f"rejected within the login watchdog window: {detail[:80]}"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def run(args) -> int:
    ctx = Ctx()
    ctx.keep = args.keep
    ctx.env = load_env_file(Path(args.env_file).expanduser())
    for key in ("RC_SMOKE_URL", "RC_SMOKE_BOT_USER_ID", "RC_SMOKE_BOT_TOKEN", "RC_SMOKE_HUMAN_USER_ID",
                "RC_SMOKE_HUMAN_USERNAME", "RC_SMOKE_HUMAN_TOKEN"):
        if not ctx.env.get(key):
            raise SystemExit(f"{key} missing from {args.env_file}")

    env = ctx.env
    os.environ.update({
        "ROCKETCHAT_URL": env["RC_SMOKE_URL"],
        "ROCKETCHAT_TOKEN": env["RC_SMOKE_BOT_TOKEN"],
        "ROCKETCHAT_USER_ID": env["RC_SMOKE_BOT_USER_ID"],
        "ROCKETCHAT_ALLOWED_USERS": env["RC_SMOKE_HUMAN_USER_ID"],
        "ROCKETCHAT_REPLY_MODE": "thread",
        "ROCKETCHAT_REACTIONS": "true",
        "ROCKETCHAT_AGENT_WRITE_TOOLS": "true",
        "ROCKETCHAT_AGENT_WRITE_TRUSTED_USERS": env["RC_SMOKE_HUMAN_USER_ID"],
        "ROCKETCHAT_ALLOW_INSECURE_HTTP": env.get("RC_SMOKE_ALLOW_INSECURE_HTTP", "false"),
        "ROCKETCHAT_ALLOW_PRIVATE_FILE_REDIRECTS": env.get("RC_SMOKE_ALLOW_PRIVATE_FILE_REDIRECTS", "false"),
    })

    setup_hermes(Path(args.hermes).expanduser())
    ctx.rc = load_plugin()
    logging.getLogger(MODULE_NAME).addHandler(ctx.logs)
    logging.getLogger(MODULE_NAME).setLevel(logging.DEBUG)
    if args.verbose:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    from gateway.config import PlatformConfig

    ctx.adapter = ctx.rc.RocketchatAdapter(PlatformConfig(enabled=True, extra={
        "url": env["RC_SMOKE_URL"], "token": env["RC_SMOKE_BOT_TOKEN"], "user_id": env["RC_SMOKE_BOT_USER_ID"],
    }))
    ctx.adapter.handle_message = ctx.recorder
    ctx.adapter._inbound_authorization_checker = lambda source: source.user_id == env["RC_SMOKE_HUMAN_USER_ID"]
    original_follow = ctx.adapter._download_redirected_file

    async def spy_follow(location, maximum):
        ctx.redirects.append(str(location))
        return await original_follow(location, maximum)

    ctx.adapter._download_redirected_file = spy_follow

    ctx.bot = Api(env["RC_SMOKE_URL"], env["RC_SMOKE_BOT_TOKEN"], env["RC_SMOKE_BOT_USER_ID"])
    ctx.human = Api(env["RC_SMOKE_URL"], env["RC_SMOKE_HUMAN_TOKEN"], env["RC_SMOKE_HUMAN_USER_ID"])
    if env.get("RC_SMOKE_ADMIN_TOKEN") and env.get("RC_SMOKE_ADMIN_USER_ID"):
        ctx.admin = Api(env["RC_SMOKE_URL"], env["RC_SMOKE_ADMIN_TOKEN"], env["RC_SMOKE_ADMIN_USER_ID"])

    print(f"Smoke test against {env['RC_SMOKE_URL']} (nonce {ctx.nonce})")
    try:
        await check(ctx, "connect", check_connect(ctx))
        if ctx.adapter._ddp_stream_ready:
            await check(ctx, "dm_roundtrip", check_dm_roundtrip(ctx))
            if ctx.dm_room:
                await check(ctx, "reactions", check_reactions(ctx))
                await check(ctx, "republish_guard", check_republish(ctx))
                await check(ctx, "attachment_download", check_attachment(ctx))
                await check(ctx, "typing_identity", check_typing_identity(ctx))
                await check(ctx, "dm_tool_verification", check_dm_tool(ctx))
                await check(ctx, "standalone_cron_send", check_standalone_send(ctx))
                await check(ctx, "delete_message", check_delete(ctx))
        await check(ctx, "invalid_token_is_fatal", check_bad_token(ctx))
    finally:
        try:
            await ctx.adapter.disconnect()
        except Exception as exc:  # noqa: BLE001
            print(f"  disconnect raised {type(exc).__name__}: {exc}")
        if ctx.channel and not ctx.keep:
            try:
                await ctx.human.post("channels.delete", roomId=ctx.channel)
            except Exception as exc:  # noqa: BLE001
                print(f"  channel cleanup failed: {exc}")
        for api in (ctx.bot, ctx.human, ctx.admin):
            if api is not None:
                await api.close()

    failed = [r for r in ctx.results if r[1] == "FAIL"]
    print(f"\n{len(ctx.results) - len(failed)}/{len(ctx.results)} checks passed")
    if failed or args.verbose:
        interesting = [m for m in (r.getMessage() for r in ctx.logs.records) if r"rocketchat" in m.lower() or "Rocket.Chat" in m]
        for line in interesting[-40:]:
            print("  log:", line)
    return 1 if failed else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env-file", default="~/.rc-smoke.env")
    parser.add_argument("--hermes", default=os.environ.get("HERMES_AGENT_PATH", "~/.hermes/hermes-agent"))
    parser.add_argument("--keep", action="store_true", help="leave the test channel and messages in place")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    sys.exit(main())
