# Architecture

## Goals

1. Every message the bot acts on is a genuine new post from an authorized human (or an explicit
   delegation from a peer agent). Republished documents, system events, and unauthorized senders
   never start an agent turn.
2. The bot's Personal Access Token is spent only after authorization, only against the configured
   origin, and only through bounded requests. Every capability that widens that (writes, host
   files, slash forwarding, topic writes, cross-room reads) is a separate default-off setting.
3. The plugin stays inside Hermes' documented platform-adapter surface (`BasePlatformAdapter`,
   `PlatformEntry`, profile-scoped secrets, the scoped credential lock, the YAML bridge) so it
   survives Hermes upgrades and multiplexed profiles.
4. Transport failures are loud: a rejected token stops reconnecting and is reported through the
   gateway's fatal-error path; a half-open socket is detected by keepalive; nothing "looks healthy"
   while silently receiving nothing.

## Modules

| Module | Responsibility | Depends on |
|---|---|---|
| `helpers` | Configuration (`env_get`, YAML bridge), URL/credential validation, bounded readers, republish classification, standalone cron sender | stdlib, Hermes `gateway.platforms._shared` (optional) |
| `ddp` | DDP WebSocket session: connect, login result, subscription readiness, ping/pong, reconnect with backoff, off-loop inbound dispatch | `helpers` |
| `inbound` | Message document to `MessageEvent`: guards, room metadata, mention gating, preflight authorization, slash forwarding, topic writes, attachments, thread context, reactions | `helpers`, `media`, Hermes `gateway.platforms.base` |
| `media` | Two-step `rooms.media` upload, URL media download with public-only DNS, local file delivery | `helpers`, `tools` (capability checks) |
| `adapter` | `RocketchatAdapter`: lifecycle, REST helpers, `send`/`edit`/`delete`, typing, topic sync | all of the above, Hermes base adapter |
| `tools` | Ten agent tools with their own authorization layer, rate limiting, and output budgets | `helpers`, Hermes `tools.registry` |
| `setup_wizard` | `hermes gateway setup` flow | Hermes `hermes_cli.setup` |
| `__init__` | `register(ctx)`: tools, platform entry, hooks | all |

`helpers` has no Hermes import at module load; everything Hermes-specific inside it is imported
lazily and degrades to plain `os.environ` reads, which is what keeps the classification and
configuration logic testable without a gateway.

## Inbound pipeline

```
DDP "changed" frame  args = [message, {roomParticipant, roomType, roomName}]
   │  ddp._handle_ddp_frame                 (read loop keeps answering pings)
   ▼
_spawn_inbound  ──► task, per-room lock, global semaphore (ROCKETCHAT_INBOUND_MAX_CONCURRENCY)
   ▼
inbound._handle_message
   1. shape checks: sender id, message id, own messages
   2. is_mutation_republish(post)            ── drop: edited / thread counters / reactions / pins / _updatedAt-ts > 60 s
   3. dedup (6 h window, 20 000 ids)         ── drop: seen id
   4. room type: stream roomType → cache → rooms.info; unknown ⇒ drop with a warning
      roomParticipant == false ⇒ drop unless ROCKETCHAT_REQUIRE_MEMBERSHIP=false
   5. system messages (t): only DM room_changed_topic becomes /title (topic sync on, authorized)
   6. delegation envelopes (DM only): result ⇒ drop; task ⇒ body, gateway control off
      other bot-flagged senders ⇒ drop
   7. mention gate (channels/groups): real @bot mention, free-response room, or active thread
   8. build SessionSource, remember it per room
   9. preflight authorization (gateway allowlist / pairing state)
        not authorized or unknown ⇒ dispatch text-only event, nothing else       ─► runner (hook, allowlist, pairing)
  10. privileged effects, each gated again:
        /command  → ROCKETCHAT_FORWARDED_SLASH_COMMANDS ∧ trusted writer ∧ hook verdict → commands.run (consumes the message)
        /title x  → ROCKETCHAT_TOPIC_SYNC ∧ trusted writer ∧ hook verdict → *.setTopic
  11. attachments (bounded, one redirect hop), audio → mp3
  12. first turn in a thread: newest replies as channel_context (tagged, redacted, collapsed)
  13. dispatch MessageEvent                                                         ─► runner
```

The runner performs the authoritative admission on dispatch: the `pre_gateway_dispatch` hook,
the allowlist, and DM pairing. The adapter's preflight reuses the same allowlist check so it
spends no credentials for a sender the runner will reject. The hook is consulted a second time
only on the two privileged paths in step 10, because those happen instead of dispatch.

## Republish classification

Rocket.Chat's `notifyOnMessageChange` broadcasts the whole message document on `watch.messages`
after every mutation, and `listeners.module.ts` forwards it to `__my_messages__` unchanged. A
thread reply updates the root (`tcount`, `tlm`, `replies`), a reaction updates `reactions`, a
pin sets `pinned`/`pinnedAt`, an edit sets `editedAt`. None of those fields exist on a fresh
insert, and a fresh insert's `_updatedAt` equals its `ts` within seconds because `sendMessage`
resets a client timestamp that drifts more than ten seconds. `helpers.is_mutation_republish`
therefore checks the structural markers first and the timestamp distance second; a frame without
usable timestamps passes to the dedup cache, which is the second guard.

## DDP session

```
connect ──► {"msg":"connect","version":"1"}
        ──► method login {resume: PAT}          watchdog: no result in 20 s ⇒ close ⇒ reconnect
   ◄── result(error)   ⇒ DdpAuthError ⇒ _set_fatal_error(retryable=False) + notify, loop stops
   ◄── result(ok)      ⇒ sub stream-room-messages ["__my_messages__", false]
   ◄── ready [sub id]  ⇒ stream ready; backoff resets on the next disconnect
   ◄── nosub(error)    ⇒ close socket ⇒ reconnect with backoff
   ◄── failed          ⇒ DdpProtocolError ⇒ fatal
   ◄── ping            ⇒ pong (aiohttp also pings every ROCKETCHAT_WS_HEARTBEAT_SECONDS)
   ◄── changed         ⇒ _spawn_inbound
```

Reconnect backoff is 2 s doubling to 60 s with 20 % jitter and resets only after a session in
which the subscription became ready, so a server that accepts the socket and drops it is not
hammered. A REST 401 after connect is escalated the same way as a rejected resume token.

## Outbound

`send` formats Markdown, strips delivery directives, splits at 5000 UTF-16 code units (the unit
Rocket.Chat's `Message_MaxAllowedSize` is enforced in), wraps delegated-room replies in a
terminal result envelope, threads under the root in `thread` mode for channels and groups, and
verifies the returned `rid`/`tmid`. Media goes through `rooms.media` + `rooms.mediaConfirm`.
Reactions use `chat.react` with an explicit `shouldReact` so retries are idempotent.
`delete_message` uses `chat.delete`, which lets Hermes' ephemeral notices expire.

The standalone sender used by cron and `send_message` shares none of the adapter state: it posts
chunks and then uploads each `media_files` entry with the same two-step flow.

## Authorization model

| Actor / action | Gate |
|---|---|
| Human sender starts a turn | gateway allowlist or pairing approval (runner), reused by the adapter preflight |
| Attachment download, ffmpeg, thread history | sender authorized |
| RC-native slash forwarding | authorized ∧ `ROCKETCHAT_AGENT_WRITE_TOOLS` ∧ `ROCKETCHAT_AGENT_WRITE_TRUSTED_USERS` ∧ exact `ROCKETCHAT_FORWARDED_SLASH_COMMANDS` ∧ hook verdict |
| Topic write from `/title` | as above plus `ROCKETCHAT_TOPIC_SYNC` |
| Peer bot message | dropped unless a delegation task envelope; task runs with gateway control disabled |
| Agent read tool | current room, or exact `ROCKETCHAT_RETRIEVAL_ALLOWED_ROOMS` ∧ `ROCKETCHAT_RETRIEVAL_TRUSTED_USERS` |
| Agent write tool | `ROCKETCHAT_AGENT_WRITE_TOOLS` ∧ current room, or exact room allowlist ∧ trusted user; room creation, DMs, name resolution always need a trusted user |
| Agent file upload | write tools ∧ `ROCKETCHAT_AGENT_FILE_UPLOADS` ∧ path below `ROCKETCHAT_AGENT_FILE_ALLOWED_ROOTS` |
| Model-emitted `MEDIA:` file delivery | `ROCKETCHAT_AGENT_FILE_UPLOADS` ∧ path below the allowed roots (destination is the session's own room by construction) |

Tool authorization reads Hermes' task-local session context only; a missing or partial context
fails closed.

## Configuration precedence

`PlatformConfig.extra` (seeded by the YAML bridge or `env_enablement_fn`) → profile-scoped
environment (`helpers.env_get`) → defaults. Under `gateway.multiplex_profiles` a secondary
profile never borrows the default profile's values from `os.environ`. `plugin.yaml`, the README
table, and `helpers._YAML_BRIDGE` are the three inventories that must stay in sync.

## Invariants worth a test

- A republished frame never reaches `handle_message`; a fresh frame with a URL preview update does.
- An unknown room type is dropped, never treated as a channel.
- A login `result` with `error` stops reconnecting and reports a non-retryable fatal error.
- Unauthorized senders cause no REST call before dispatch.
- Delegated task bodies dispatch with `allow_gateway_control=False`.
- `commands.run` receives the bare command name; `chat.react` always carries `shouldReact`.
- Attachment redirects are followed once, without the PAT, to HTTPS public hosts only (unless
  same-origin or explicitly allowed).
- Every REST call refuses redirects and bounds the body; every DDP frame is bounded at 4 MiB.

## Known limitations

- Messages posted while the socket is down are not backfilled on reconnect.
- `rocketchat_search_messages` forwards the query verbatim; Rocket.Chat interprets
  `/regex/` and `from:`/`before:` operators server-side.
- Typing indicators are dropped by the server when `UI_Use_Real_Name` is on and the settings
  lookup at connect time fails (the login name is then sent).
