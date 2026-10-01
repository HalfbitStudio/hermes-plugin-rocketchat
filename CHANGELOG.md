# Changelog

All notable changes to this plugin are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow semver.

## [Unreleased]

## [1.5.1] - 2026-10-01

### Fixed

- Move Ruff configuration from `pyproject.toml` to `ruff.toml`. Hermes Agent v0.21.5 treated the
  configuration-only file as a uv workspace package declaration, failed to build it, and disabled
  `rocketchat-platform` during managed environment generation. The plugin now remains directly
  loaded, with the same lint rules and no new dependencies
  ([#8](https://github.com/HalfbitStudio/hermes-plugin-rocketchat/issues/8)).
- Add a regression check for the installed plugin's packaging metadata and document updating and
  re-enabling installations that Hermes already disabled.

## [1.5.0] - 2026-09-24

### Fixed

- Republished message documents no longer start new agent turns. Rocket.Chat re-broadcasts a
  message on `stream-room-messages` after every mutation (a thread reply bumps the root's
  `tcount`/`tlm`, a reaction, pin, star, or edit rewrites it), and the frame is shaped exactly like
  a fresh post. `helpers.is_mutation_republish` now classifies frames by those structural
  markers first and by the `_updatedAt - ts` distance (both server clocks) second, and the
  inbound dedup window is 6 hours / 20 000 ids instead of 5 minutes / 2 000. Diagnosis and the
  production capture come from [@immodigit](https://github.com/immodigit)'s upstream PR #5.
- A rejected DDP resume token is detected: the `login` `result` frame is now inspected, the
  message-stream subscription is sent only after a successful login, `nosub` errors and
  `failed` frames close the session, and a permanent authentication failure stops reconnecting
  and is reported through Hermes' fatal-error path instead of leaving a "connected" adapter that
  receives nothing. A REST 401 after connect is escalated the same way.
- Inbound processing runs off the DDP read loop (per-room ordering, bounded concurrency), so
  protocol pings are answered while attachments download, ffmpeg runs, or thread history loads.
- Room type comes from the stream's room metadata (`args[1].roomType`), with `rooms.info` as the
  fallback. An unknown room type is dropped with a warning instead of being treated as a channel,
  which mention-gated DMs and silently dropped them on a transient lookup failure.
- `commands.run` receives the bare command name; the leading slash made every forwarded
  command fail with 400.
- Reactions pass `shouldReact` explicitly. The toggle form inverted state after any failed or
  duplicated call and left 👀 stuck.
- `im.create` responses carry no `uids`, so every DM, delegation, and username upload was
  rejected by the ghost-room check; the created room is re-read with `rooms.info` before
  verification.
- Attachments on S3/GCS-backed workspaces are downloaded: `/file-upload/` answers with a
  redirect to a signed URL, which is now followed exactly once, without the PAT, to public HTTPS
  hosts (or same-origin, or any host with `ROCKETCHAT_ALLOW_PRIVATE_FILE_REDIRECTS=true`).
- Messages are split at 5000 UTF-16 code units, the unit `Message_MaxAllowedSize` is enforced in;
  emoji-heavy chunks at the boundary were rejected.
- `send_voice` accepts the `is_voice` keyword every Hermes voice call site passes.
- Out-of-process cron delivery uploads `media_files` and chunks long text; attachments were
  silently dropped.
- Thread context and `rocketchat_get_thread` fetch the newest replies (`sort ts:-1`); long threads
  returned the oldest ones.
- `groups.history` treated the string `"false"` as `inclusive=true`; the parameter is only sent
  when true.
- `rocketchat_list_channels` paginates instead of reading one 100-item page.
- Topic sync looks up the session of the last admitted message in the room instead of creating a
  phantom DM session for every room type.
- Model-emitted `MEDIA:` file delivery is authorized by the file-upload capability and allowed
  roots; it ran after Hermes had cleared the session context and was always denied.
- `format_message` no longer deletes prose lines that merely start with "MEDIA".
- The typing indicator sends the display name when the workspace has `UI_Use_Real_Name` on.
- The WebSocket is closed on every exit path and the previous connection is not leaked across
  reconnects; backoff resets only after the subscription became ready.

### Added

- `ROCKETCHAT_REQUIRE_MEMBERSHIP` (default true): `__my_messages__` also streams public rooms the
  bot can read but has not joined; those are ignored unless disabled.
- `ROCKETCHAT_INBOUND_MAX_CONCURRENCY`, `ROCKETCHAT_ALLOW_PRIVATE_FILE_REDIRECTS`.
- `config.yaml` support: every setting is available under `platforms.rocketchat` through Hermes'
  YAML bridge (environment variables still win).
- `delete_message` via `chat.delete`, so ephemeral gateway notices expire.
- 429 responses are retried once using `Retry-After` / `X-RateLimit-Reset`; rejected REST calls
  log the server's `errorType`.
- Tests split into modules with a shared harness and fake aiohttp doubles, a loader test through
  Hermes' real `PluginManager`, ruff configuration, a Makefile, CI on Python 3.11–3.13 with lint
  and plugin-doctor jobs, `CONTRIBUTING.md`, `SECURITY.md`, `docs/architecture.md`.

### Changed

- Admitted messages are no longer marked `internal=True` to skip the runner's re-check. That flag
  also disabled Hermes' emergency stop, drain gate, idle accounting, and transcript labelling.
  The adapter now runs a preflight authorization (the gateway's allowlist) before any
  credentialed side effect and dispatches a text-only event for unauthorized senders so the
  runner's own pairing offer runs. The `pre_gateway_dispatch` hook is consulted again only on the
  two privileged paths that happen instead of dispatch (slash forwarding, topic writes).
- Credentials and allowlists are read through Hermes' profile-scoped secret reader
  (`get_scoped_secret`), and `connect()` takes the scoped credential lock so one PAT cannot be
  driven by two profiles or gateways.
- `check_fn` is a passive dependency probe (aiohttp importable); configuration is validated by
  `validate_config`, so `config.yaml`-only deployments are no longer refused before the bridge
  runs.
- `@all` and `@here` no longer count as mentions of the bot.
- Boolean settings share one parser: `1/true/yes/on` enable, anything else disables.
- Thread history is passed as `channel_context` instead of being prepended to the message text,
  entries are collapsed to one line each, and senders are tagged unverified using the gateway's
  registered authorization check (pairing approvals included) rather than the env allowlist alone.
- Inbound and tool audit lines use the same keyed identifier hash.
- Bot-peer detection accepts Rocket.Chat's `bot: {i: ...}` integration marker.

### Security

- A delegated task body dispatches with gateway control disabled: a peer agent can request a
  turn but never `/restart`, `/update`, `/sethome`, `/model`, or any other control command.
- DDP frames are bounded at 4 MiB; attachment redirects never carry the PAT.
- Session titles, room and message identifiers are no longer written to INFO logs.

### Documentation

- Live smoke test (`scripts/smoke_test.py`, `scripts/smoke/`): a disposable Rocket.Chat +
  MongoDB + MinIO stack and a script that drives the real adapter through DM, thread, reaction,
  attachment (via the object-storage redirect), typing-identity, DM verification, cron-media,
  delete, and invalid-token checks. All checks pass against Rocket.Chat 8.8.
- README rewritten: accurate PAT setup (token creation needs the `user` role's
  `create-personal-access-tokens` permission, not `bot`), complete configuration table,
  `config.yaml` example, membership and object-storage notes, troubleshooting for rejected tokens
  and republished messages. `AGENTS.md` DDP section matches the frames the code sends.

Audit, implementation, live verification, and documentation by
[Andrew Vieyra](https://github.com/andrewvieyra) (<andrew@andrewvieyra.com>); republish
diagnosis by [@immodigit](https://github.com/immodigit).

## [1.4.1] - 2026-08-31

### Fixed

- DDP WebSocket keepalive is armed by default (`heartbeat=30`). Previously the
  socket was opened with `heartbeat=None`, so a half-open connection raised
  nothing: the read loop blocked forever, the reconnect loop in `_ws_loop()`
  never fired, and inbound messages stopped permanently while outbound REST
  kept working and the process looked healthy (upstream issue #3).

### Added

- `ROCKETCHAT_WS_HEARTBEAT_SECONDS` to tune the keepalive interval (clamped to
  5–300 seconds). Only the literal value `0` disables the guard; invalid or
  out-of-range values keep the safe default rather than silently disabling it.

## [1.4.0] - 2026-07-23

### Added

- `rocketchat_delegate` for one-shot bot-to-bot task delegation over DM.
- Versioned task/result envelopes with random delegation IDs.
- `ROCKETCHAT_BOT_PEERS` as a compatibility fallback for Rocket.Chat servers
  that omit bot metadata from message events.

### Security

- Terminal delegation results are dropped before authorization, attachment
  processing, or agent dispatch, preventing recursive DM conversations.
- Ordinary bot-generated messages are ignored unless they are explicit
  delegation tasks. Human access can remain open without maintaining a human
  user allowlist.
- Text, edited, and media responses in delegated DM rooms all preserve terminal
  result semantics.

## [1.3.0] - 2026-07-22

### Added

- Four read-only agent tools: `rocketchat_search_messages`,
  `rocketchat_get_history`, `rocketchat_get_thread`, and
  `rocketchat_get_permalink`.
- Exact-`room_id` message search and bounded room-history retrieval with
  pagination and optional time-window filters.
- Compact normalized thread output with the root message fetched separately
  from its replies.
- Stable, URL-encoded message permalinks for public channels, private groups,
  and direct-message rooms.

### Security

- Retrieval now fails closed to the verified Rocket.Chat session's current
  room. Cross-room reads require both an exact room allowlist match and an exact
  trusted-requester match; resolved thread and permalink rooms receive the same
  check.
- Contextless retrieval is disabled by default and, when explicitly enabled,
  remains limited to exact allowlisted room IDs. Retrieval from other named
  platforms is rejected.
- Agent write tools are disabled by default and separated from read tools.
  Contextless or cross-platform writes require a second explicit opt-in.
- Local file upload has an independent default-off capability, canonical
  allowed-root policy, traversal/symlink rejection, exact two-member DM target
  verification, and a separate concurrency bound held through confirmation.
- Tool authorization now consumes task-local Hermes session provenance only;
  stale process-global `HERMES_SESSION_*` values cannot grant access.
- HTTPS is required by default, environment proxies and API redirects are
  ignored, and every JSON REST response has a pre-decode body limit. Agent REST
  calls additionally have concurrency and per-minute request budgets.
- Inbound authorization and pre-dispatch hooks now complete before slash/topic
  writes, attachment downloads, thread fetches, or audio conversion. Native
  slash forwarding and topic sync are exact-allowlisted/default-off capabilities.
- Network media has streaming byte limits and connection-time public-DNS
  enforcement; local `MEDIA:` delivery uses the same opt-in, trusted-user,
  allowed-root, descriptor, and size checks as the explicit upload tool.
- Retrieval results are marked untrusted, bounded locally, guarded against
  non-progressing pagination, and use privacy-preserving normalized records.
  File URLs, reaction identities, and stable user IDs require explicit opt-ins.
- Common credential patterns are redacted by default. This is a best-effort
  defense and does not eliminate stored prompt-injection risk.

### Changed

- Deployments that use `rocketchat_send_file` must now set all of
  `ROCKETCHAT_AGENT_WRITE_TOOLS=true`,
  `ROCKETCHAT_AGENT_FILE_UPLOADS=true`, and
  `ROCKETCHAT_AGENT_FILE_ALLOWED_ROOTS` to one or more absolute directories.
  Separate roots with `:` on Unix/macOS. Secure local upload requires POSIX
  descriptor APIs and is unavailable on Windows.
- Cross-room writes now require both `ROCKETCHAT_AGENT_WRITE_ALLOWED_ROOMS`
  and `ROCKETCHAT_AGENT_WRITE_TRUSTED_USERS`. RC-native command forwarding also
  requires `ROCKETCHAT_FORWARDED_SLASH_COMMANDS`; topic sync requires
  `ROCKETCHAT_TOPIC_SYNC=true`.

### Documentation

- Expanded the agent-tool reference from five to nine tools with business use
  cases, required permissions, parameters, pagination limits, and examples.
- Added secure-default deployment guidance covering room/requester scoping,
  read/write separation, least-privilege bot accounts, HTTPS, audit monitoring,
  data minimization, and residual prompt-injection risk.

## [1.2.0] - 2026-07-20

### Added

- Agent-callable `rocketchat_send_file` for local files.
- Channel/private-group targeting by name, exact `room_id` targeting, and DM
  targeting by real Rocket.Chat username.
- Optional captions, displayed filename overrides, automatic MIME detection,
  and thread placement through `tmid`.
- A configurable 100 MiB local safety guard through
  `ROCKETCHAT_AGENT_FILE_MAX_BYTES`.

### Changed

- File targets must be unambiguous: exactly one of `channel`, `room_id`, or
  `username` is accepted.
- Invalid or one-member DM targets are rejected before file bytes are uploaded.
- Local reads reject non-regular files and run outside the gateway event loop.
- Private groups can be resolved by name through the common `rooms.info` API.

### Documentation

- Documented installation updates, server upload settings, tool arguments,
  security considerations, and the two-step Rocket.Chat media flow.

Thanks to [@YounesAmalou](https://github.com/YounesAmalou) for the original
implementation in [PR #1](https://github.com/HalfbitStudio/hermes-plugin-rocketchat/pull/1).

## [1.1.1] - 2026-07-16

### Fixed

- Clarification prompts stay inside the active Rocket.Chat thread instead of landing in the room.

## [1.1.0] - 2026-07-16

### Fixed

- DM sender identity uses Rocket.Chat's display name with username and id fallbacks, so Hermes
  addresses the person by the name shown in the client.

## [1.0.0] - 2026-07-15

First standalone release, ported from the hermes-agent pull requests #4637, #14869 and #30463.

### Added

- Rocket.Chat platform adapter: REST API v1 outbound, DDP `__my_messages__` inbound, mention
  gating, threaded replies for channels and groups, flat DM replies, reactions, topic sync, voice
  message transcoding, attachment download, reconnect with backoff.
- Agent tools `rocketchat_list_channels`, `rocketchat_create_channel`, `rocketchat_post`,
  `rocketchat_dm`; thread context injection on the bot's first turn in a thread.
- Standalone REST sender for cron delivery, `hermes gateway setup` wizard,
  `ROCKETCHAT_SUPPRESS_HOME_CHANNEL_NOTICE`.

[Unreleased]: https://github.com/HalfbitStudio/hermes-plugin-rocketchat/compare/v1.5.1...HEAD
[1.5.1]: https://github.com/HalfbitStudio/hermes-plugin-rocketchat/compare/v1.5.0...v1.5.1
[1.5.0]: https://github.com/HalfbitStudio/hermes-plugin-rocketchat/compare/v1.4.0...v1.5.0
[1.4.1]: https://github.com/HalfbitStudio/hermes-plugin-rocketchat/compare/v1.4.0...989a04c
[1.4.0]: https://github.com/HalfbitStudio/hermes-plugin-rocketchat/compare/v1.3.0...v1.4.0
[1.3.0]: https://github.com/HalfbitStudio/hermes-plugin-rocketchat/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/HalfbitStudio/hermes-plugin-rocketchat/compare/v1.1.1...v1.2.0
[1.1.1]: https://github.com/HalfbitStudio/hermes-plugin-rocketchat/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/HalfbitStudio/hermes-plugin-rocketchat/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/HalfbitStudio/hermes-plugin-rocketchat/releases/tag/v1.0.0
