# Rocket.Chat Platform Plugin — AI Agent Guide

Reference for AI coding assistants working on this plugin. Read `docs/architecture.md` for the
full model; this file lists the decisions that are easy to get wrong.

## Overview

A Hermes gateway platform adapter for self-hosted Rocket.Chat. REST API v1 for everything sent,
the Realtime (DDP) WebSocket for everything received, `aiohttp` only (already a Hermes
dependency). Targets Hermes Agent 0.21+ and Rocket.Chat 6.x–8.x.

## File map

| File | Purpose |
|------|---------|
| `adapter.py` | `RocketchatAdapter`: lifecycle, REST helpers, `send`/`edit`/`delete`, typing, topic sync |
| `ddp.py` | DDP session: connect, login result, subscription readiness, ping/pong, reconnect, off-loop inbound tasks |
| `inbound.py` | Message document → `MessageEvent`: republish guard, room metadata, mention gate, preflight authorization, slash forwarding, topic writes, attachments, thread context, reactions |
| `media.py` | `rooms.media` two-step upload, URL media download (public-only DNS), local file delivery |
| `helpers.py` | `env_get` (profile-scoped config), YAML bridge, URL/credential validation, bounded readers, `is_mutation_republish`, standalone cron sender |
| `tools.py` | Ten agent tools with their own authorization layer, rate limiting, output budgets |
| `setup_wizard.py` | `hermes gateway setup` flow |
| `plugin.yaml` | Manifest: version, env inventory (keep in sync with README and `_YAML_BRIDGE`) |
| `__init__.py` | `register(ctx)` |
| `tests/` | `harness.py` (loader, adapter factory, fake aiohttp), `test_adapter.py` (legacy suite), `test_republish.py`, `test_ddp.py`, `test_inbound.py`, `test_standalone.py`, `test_config.py`, `test_plugin_load.py` |
| `docs/architecture.md` | Pipeline, DDP state machine, authorization table, invariants |

## Design decisions

### 1. The stream carries mutations, not only new posts

Rocket.Chat's `notifyOnMessageChange` broadcasts the whole message document after every mutation
(thread reply → root `tcount`/`tlm`/`replies`; reaction; pin; star; edit) and
`listeners.module.ts` forwards it to `__my_messages__` unchanged, shaped like a fresh post.
`helpers.is_mutation_republish` drops frames with any of those markers, then frames whose
`_updatedAt - ts` exceeds 60 s (both are server clocks; `sendMessage` resets client `ts` drift
over 10 s). Frames without timestamps fall through to the dedup cache (6 h / 20 000 ids). The
guard runs before dedup in `_handle_message`. Never use `urls[].meta` as a signal: URL previews
update seconds after the insert and are covered by dedup.

### 2. Room metadata comes from the stream

`changed` frames have `fields.args = [message, {roomParticipant, roomType, roomName}]`
(`notifications.module.ts` `allowEmit('__my_messages__')`). `roomType` is cached first;
`rooms.info` is the fallback. An unknown type is dropped with a warning, never defaulted to
`channel` (that mention-gated DMs). `roomParticipant == false` means a public room the bot can
read but has not joined; ignored unless `ROCKETCHAT_REQUIRE_MEMBERSHIP=false`.

### 3. DDP frames the code sends and expects

```
→ {"msg":"connect","version":"1","support":["1"]}
→ {"msg":"method","method":"login","id":"1","params":[{"resume":"<PAT>"}]}
← {"msg":"result","id":"1", "error":{...}}  → DdpAuthError → _set_fatal_error(retryable=False), loop stops
← {"msg":"result","id":"1", "result":{...}} → {"msg":"sub","name":"stream-room-messages","params":["__my_messages__",false]}
← {"msg":"ready","subs":[<sub id>]}          → stream ready (backoff resets after this session ends)
← {"msg":"nosub","id":<sub id>,"error":...}  → close socket → reconnect with backoff
← {"msg":"failed"}                           → DdpProtocolError → fatal
← {"msg":"ping"}                             → {"msg":"pong"}
```

A login watchdog closes the socket if no `result` arrives within 20 s. aiohttp keepalive
(`ROCKETCHAT_WS_HEARTBEAT_SECONDS`, default 30) turns a half-open socket into an exception.
Frames are bounded at 4 MiB. Handshake 401/403 is fatal; other errors reconnect (2 s → 60 s,
20 % jitter). There is no "power-on topic" or any other write on connect.

### 4. Inbound work runs off the read loop

`_handle_ddp_frame` spawns `_run_inbound` per frame: a global semaphore
(`ROCKETCHAT_INBOUND_MAX_CONCURRENCY`) plus a per-room lock so one room's messages stay in
order while the loop keeps answering pings during attachment downloads, ffmpeg, and thread
fetches. `disconnect()` cancels the tasks.

### 5. Admission: preflight, never `internal=True`

The runner performs the authoritative admission on dispatch (`pre_gateway_dispatch` hook,
allowlist, DM pairing). `_preflight_authorized` reuses the gateway's allowlist check
(`gateway_runner._is_user_authorized_for_source`, or `_is_user_authorized`, or an injected
`_inbound_authorization_checker` in tests) so the adapter spends no credentials for a sender
the runner will reject; such senders get a text-only dispatch. Do not mark events
`internal=True`: that flag disables the emergency stop, drain gate, idle accounting, and
transcript labelling. The hook is consulted a second time only in `_privileged_hook_verdict`,
on the two paths that happen instead of dispatch: forwarding to `commands.run` and `/title`
topic writes.

### 6. Slash commands: position 0 only, bare name to the server

Only text starting with `/` is a command. Hermes-known commands are never forwarded. A
Rocket.Chat-native command is forwarded through `commands.run` only when its exact bare name is
in `ROCKETCHAT_FORWARDED_SLASH_COMMANDS`, write tools are on, the sender is a trusted writer, and
the hook verdict left the command intact. `commands.run` takes `command: "giphy"` (no slash).

### 7. Delegation is one-shot and control-free

`rocketchat_delegate` sends `[hermes-delegation:v1:task:<32 hex>]\n<body>`. Inbound strips the
envelope in DMs only, dispatches the body with `allow_gateway_control=False` (no `/restart`,
`/update`, `/sethome`, `/model`), and remembers the room so every reply carries a `result`
envelope, which receivers drop before any processing. Ordinary bot-flagged messages
(`bot: true` or `bot: {i: ...}`, `u.type` bot/app, `bot` role, `ROCKETCHAT_BOT_PEERS`) are
ignored.

### 8. Reactions are idempotent

`chat.react` toggles when `shouldReact` is omitted (`setReaction.ts`), so a failed add left 👀
stuck. Always pass `shouldReact: true/false`.

### 9. DM verification re-reads the room

`im.create` returns `{_id, rid, t, usernames, inserted}` without `uids`/`usersCount`
(`createDirectRoom.ts`). `_open_verified_dm` re-reads the room with `rooms.info`, then
`_verified_dm_room` requires exactly two usernames and two uids including the bot and the
requested login, which is what rejects the one-member ghost room an unknown username creates.

### 10. Attachments and object storage

`/file-upload/<id>/<name>` is fetched with the PAT and `allow_redirects=False`. S3/GCS
workspaces without "Proxy uploads" answer 302 to a signed URL; `_download_redirected_file`
follows exactly one hop without the PAT, HTTPS only, through the public-only resolver unless
same-origin or `ROCKETCHAT_ALLOW_PRIVATE_FILE_REDIRECTS=true`.

### 11. Message length is UTF-16

`Message_MaxAllowedSize` is enforced in JavaScript string units. `send`, the standalone sender,
and the class attributes (`MAX_MESSAGE_LENGTH`, `splits_long_messages`, `message_len_fn`) all
use `utf16_len`.

### 12. Thread context is `channel_context`, newest first

`_fetch_thread_context` fetches the parent with `chat.getMessage` and the replies with
`chat.getThreadMessages` sorted `ts:-1`, verifies `rid`/`tmid` provenance, collapses each entry to
one line, tags senders the gateway would not authorize (`_is_sender_authorized`, falling back to
the env allowlist) as `[unverified sender]`, and hands the block to Hermes as
`MessageEvent.channel_context`, not as part of `text`.

### 13. Configuration is profile-scoped

Every `ROCKETCHAT_*` read goes through `helpers.env_get` (→ `gateway.platforms._shared.get_scoped_secret`),
never `os.getenv`, so a multiplexed secondary profile never borrows the default profile's PAT or
allowlists. `connect()` takes `acquire_scoped_lock("rocketchat", "<url>:<user_id>")`.
`_apply_yaml_config` bridges `platforms.rocketchat` keys from `config.yaml` (env wins).
`check_requirements` is a passive dependency probe; `validate_config` checks credentials.

### 14. Agent tools fail closed at a second authorization layer

Unchanged from 1.3.0: reads are scoped to the current room, cross-room reads and every write need
exact allowlists plus a trusted requester resolved from Hermes' task-local session context, host
files are read below configured roots via descriptor-relative opens, results are bounded and
marked untrusted. Model-emitted `MEDIA:` delivery runs after Hermes clears the session context,
so `_send_local_file` is gated by the file-upload capability and allowed roots only (the
destination is the session's own room by construction).

### 15. Sender identity and DM replies

`SessionSource.user_name` is `u.name → u.username → u._id`; authorization uses `u._id`. DM
replies never carry `tmid`; in `thread` mode a top-level channel/group message is its own thread
root and `_thread_target_for_reply` prefers `metadata["thread_id"]` over `reply_to`.

## Known pitfalls

| Pitfall | Detail | Mitigation |
|---|---|---|
| Token rejected on DDP only | REST `/me` works with a stale session token but `login {resume}` fails | Login result is checked; fatal error names `ROCKETCHAT_TOKEN` |
| Republished thread root | Every reply republishes the root | `is_mutation_republish` + 6 h dedup |
| DM treated as channel | `rooms.info` failure used to default to `channel` | Stream `roomType`; unknown ⇒ drop |
| Typing dropped silently | `canType` compares against `name` when `UI_Use_Real_Name` is on | `_resolve_typing_name` at connect |
| `groups.history` `inclusive` | String `"false"` is truthy on that endpoint | Send only when true |
| `dm.setTopic` | Needs global `edit-room` | Documented; feature default off |
| Desktop swallows `/new` | Client-side interception | `Message_AllowUnrecognizedSlashCommand` |
| Two gateways, one PAT | Both answer every message | Scoped credential lock |
| Hook double invocation | Privileged paths consult `pre_gateway_dispatch` before the runner does | Only on slash forwarding and topic writes; deterministic hooks give the same verdict |

## Testing

```bash
python -m pip install -e ~/.hermes/hermes-agent pytest pytest-asyncio pytest-timeout ruff
HERMES_AGENT_PATH=~/.hermes/hermes-agent make test
make lint
make doctor
```

`tests/harness.py` provides `load_plugin()`, `make_adapter()`, `make_post()` and fake aiohttp
doubles. Inbound tests inject `adapter._inbound_authorization_checker` and mock
`handle_message`, `_api_post`, `_api_get`, `_download_attachments`. Use a per-test timeout: a
regression in `_ws_loop` shows up as a hang, not a failure.

## History

Ported from hermes-agent PRs #4637 (@meron1122), #14869 (@cyb0rgk1tty), #30463 (@HearthCore);
file uploads from #1 (@YounesAmalou); keepalive and republish diagnosis from #4/#5 (@immodigit);
1.5.0 audit, hardening, smoke test and this documentation set by Andrew Vieyra (@andrewvieyra,
andrew@andrewvieyra.com). Release notes: `CHANGELOG.md`.
