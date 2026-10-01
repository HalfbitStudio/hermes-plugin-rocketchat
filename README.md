# Rocket.Chat Plugin for Hermes Agent

Connects [Hermes Agent](https://hermes-agent.nousresearch.com/) to a self-hosted Rocket.Chat
workspace: REST API v1 for everything the bot sends, the Realtime (DDP) WebSocket for everything
it receives. Ships as a standalone plugin with no changes to Hermes core and no extra Python
dependencies (`aiohttp` is already part of Hermes).

[![CI](https://github.com/HalfbitStudio/hermes-plugin-rocketchat/actions/workflows/ci.yml/badge.svg)](https://github.com/HalfbitStudio/hermes-plugin-rocketchat/actions/workflows/ci.yml)
![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)
![License MIT](https://img.shields.io/badge/license-MIT-green)

```
alice   > @hermes-bot summarize this thread and post it to #reports
hermes  > 👀
hermes  > [rocketchat_get_thread]  12 replies, newest first
hermes  > [rocketchat_post]        #reports: "Deploy thread summary: ..."
hermes  > ✅ Posted the summary to #reports (message L9kq...).
```

## Installation

```bash
hermes plugins install HalfbitStudio/hermes-plugin-rocketchat
hermes plugins enable rocketchat-platform
hermes gateway restart
```

The installer clones this repository into `~/.hermes/plugins/rocketchat-platform/`. Update with
`hermes plugins update rocketchat-platform` followed by a gateway restart.

If Hermes disabled the plugin after updating to 1.5.0, update to 1.5.1 or newer and explicitly
re-enable it before restarting:

```bash
hermes plugins update rocketchat-platform
hermes plugins enable rocketchat-platform
hermes gateway restart
```

Requirements: Hermes Agent 0.21 or newer, Python 3.11+, a Rocket.Chat 6.x, 7.x or 8.x workspace,
and `ffmpeg` on the gateway host if voice messages should be transcribed.

## Quick start

### 1. Create the bot user

**Administration → Workspace → Users → New.** Give it a username (this is what people will
@mention, e.g. `hermes-bot`), keep the default `user` role and add `bot`. Uncheck "Require
password change" and skip the welcome email.

The `bot` role marks the account visually and keeps it out of active-user counts; the `user`
role is what carries the `create-personal-access-tokens` permission that the next step needs.
If you prefer a `bot`-only account, grant that permission to the `bot` role under
**Administration → Workspace → Permissions**.

### 2. Generate a Personal Access Token

Log in **as the bot user**, open the avatar menu → **Profile → Personal Access Tokens**, name the
token (e.g. `hermes-gateway`), tick **Ignore Two Factor Authentication**, and generate it. Copy
the **token** and the **user ID** right away; both are shown only once.

### 3. Configure Hermes

Either run the wizard:

```bash
hermes gateway setup
```

or write the three required values into `~/.hermes/.env`:

```bash
ROCKETCHAT_URL=https://rc.example.com
ROCKETCHAT_TOKEN=your_pat_token
ROCKETCHAT_USER_ID=your_bot_user_id
ROCKETCHAT_ALLOWED_USERS=your_user_id
```

or put the same keys under `platforms.rocketchat` in `~/.hermes/config.yaml` (see
[Configuration](#configuration)).

### 4. Restart the gateway and say hello

```bash
hermes gateway restart
```

Send the bot a direct message. In channels, invite it first (`/invite @hermes-bot`) and
@mention it.

## Configuring Rocket.Chat (server side)

### Room membership

The bot receives messages from the rooms it is a **member** of. Rocket.Chat's `__my_messages__`
stream also carries every public channel the bot is allowed to *read*; those are ignored unless
`ROCKETCHAT_REQUIRE_MEMBERSHIP=false`, so an invite is always the deliberate step. DMs need no
setup.

In channels and private groups the bot answers only when @mentioned (`@all` and `@here` do not
count), unless the room ID is in `ROCKETCHAT_FREE_RESPONSE_CHANNELS` or
`ROCKETCHAT_REQUIRE_MENTION=false`. With `ROCKETCHAT_REPLY_MODE=thread` only the first message of a
conversation needs the mention; replies inside that thread are picked up automatically.

### Admin settings worth checking

| Setting | Where | Why |
|---|---|---|
| `Message_AllowUnrecognizedSlashCommand` → on | Administration → Settings → Message | Desktop and browser clients swallow unknown `/` commands client-side, so Hermes commands like `/new` or `/status` never reach the server. Mobile clients are unaffected. Alternative: `OVERWRITE_SETTING_Message_AllowUnrecognizedSlashCommand=true` in the server environment. |
| Rate Limiter | Administration → Settings → Rate Limiter | A busy bot can hit `429`. The plugin retries once using the server's `Retry-After`; raise the limits or exempt the bot's IP for sustained load. |
| `Message_MaxAllowedSize` | Administration → Settings → Message | Replies are split at 5000 UTF-16 code units, Rocket.Chat's default. If you lowered the setting, long replies are rejected. |
| File Upload, storage type | Administration → Settings → File Upload | Uploads follow the workspace's enabled state, MIME restrictions and size limit. With Amazon S3 or Google Cloud Storage and "Proxy uploads" off, downloads redirect to a signed URL; the plugin follows that one hop without the token. For a LAN object store (MinIO on a private address) set `ROCKETCHAT_ALLOW_PRIVATE_FILE_REDIRECTS=true` or enable the proxy setting. |
| `UI_Use_Real_Name` | Administration → Settings → Layout → User Interface | Read at connect time so the typing indicator uses the identity the server expects. |

### Permissions for topic sync (optional, default off)

`ROCKETCHAT_TOPIC_SYNC=true` mirrors Hermes session titles to room topics. Channel and group topics
need room-edit rights (make the bot owner or moderator, or grant `edit-room` to its role). DM
topics require the global `edit-room` permission, which non-admin accounts do not have; leave the
feature off if that is not acceptable.

### Reverse proxy (nginx, traefik)

The inbound stream is a long-lived WebSocket at `/websocket`. Keep the proxy from closing it on
its default idle timeout:

```nginx
location /websocket {
    proxy_pass http://rocketchat;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_read_timeout 600s;
}
```

The adapter sends protocol pings every 30 seconds and reconnects with backoff, so a short proxy
timeout costs reconnect churn rather than messages.

## Configuration

Every setting is an environment variable in `~/.hermes/.env`. The same keys, lower-cased and
without the `ROCKETCHAT_` prefix, work under `platforms.rocketchat` in `config.yaml`; environment
variables win when both are set.

```yaml
platforms:
  rocketchat:
    url: https://rc.example.com
    token: your_pat_token
    user_id: your_bot_user_id
    allowed_users: [aliceUserId, bobUserId]
    reply_mode: thread
    free_response_channels: [GENERAL]
    home_channel: GENERAL
    agent_write_tools: true
    agent_write_trusted_users: [aliceUserId]
```

### Connection and identity

| Variable | Required | Default | Description |
|---|---|---|---|
| `ROCKETCHAT_URL` | yes | | Server URL, HTTPS unless `ROCKETCHAT_ALLOW_INSECURE_HTTP=true` |
| `ROCKETCHAT_TOKEN` | yes | | Personal Access Token of the bot user |
| `ROCKETCHAT_USER_ID` | yes | | Bot user `_id` |
| `ROCKETCHAT_ALLOW_INSECURE_HTTP` | | `false` | Permit a plain HTTP URL on an isolated network |
| `ROCKETCHAT_WS_HEARTBEAT_SECONDS` | | `30` | DDP keepalive interval, clamped 5–300; only the literal `0` disables it |
| `ROCKETCHAT_INBOUND_MAX_CONCURRENCY` | | `8` | Inbound messages processed concurrently (1–64); one room at a time |

### Who may talk to the bot

| Variable | Default | Description |
|---|---|---|
| `ROCKETCHAT_ALLOWED_USERS` | `""` | Comma-separated Rocket.Chat user IDs |
| `ROCKETCHAT_ALLOW_ALL_USERS` | `false` | Allow every user (development only) |
| `ROCKETCHAT_BOT_PEERS` | `""` | Bot usernames or IDs for servers that omit bot metadata; ordinary messages from them are ignored, delegation tasks are accepted |
| `ROCKETCHAT_REQUIRE_MENTION` | `true` | Require an @mention in channels and private groups |
| `ROCKETCHAT_REQUIRE_MEMBERSHIP` | `true` | Ignore public rooms the bot has not joined |
| `ROCKETCHAT_FREE_RESPONSE_CHANNELS` | `""` | Room IDs exempt from the mention requirement |

### Conversation behaviour

| Variable | Default | Description |
|---|---|---|
| `ROCKETCHAT_REPLY_MODE` | `off` | `thread` keeps channel and group conversations, including clarification prompts, in one thread; DMs always stay flat |
| `ROCKETCHAT_REACTIONS` | `true` | 👀 while working, ✅ or ❌ when done, on the triggering message |
| `ROCKETCHAT_THREAD_CONTEXT_MAX_CHARS` | `20000` | Thread history added to the bot's first turn in a thread (4 096–100 000) |
| `ROCKETCHAT_TOPIC_SYNC` | `false` | Mirror Hermes session titles to room topics and accept trusted `/title` writes |
| `ROCKETCHAT_HOME_CHANNEL` | | Room ID for cron and notification delivery |
| `ROCKETCHAT_HOME_CHANNEL_NAME` | room ID | Display name for the home channel in status output |
| `ROCKETCHAT_SUPPRESS_HOME_CHANNEL_NOTICE` | `false` | Hide the one-time `/sethome` notice; changes no routing |
| `ROCKETCHAT_FORWARDED_SLASH_COMMANDS` | `""` | Exact Rocket.Chat-native commands trusted writers may forward through `commands.run` |
| `ROCKETCHAT_MEDIA_DOWNLOAD_MAX_BYTES` | `104857600` | Inbound attachment budget per message and outbound URL-media limit (hard cap 1 GiB) |
| `ROCKETCHAT_ALLOW_PRIVATE_FILE_REDIRECTS` | `false` | Follow attachment redirects to non-public hosts (LAN object storage) |

### Agent tools

| Variable | Default | Description |
|---|---|---|
| `ROCKETCHAT_AGENT_WRITE_TOOLS` | `false` | Enable `rocketchat_create_channel`, `rocketchat_post`, `rocketchat_send_file`, `rocketchat_dm`, `rocketchat_delegate` |
| `ROCKETCHAT_AGENT_WRITE_ALLOWED_ROOMS` | `""` | Exact room IDs eligible for cross-room writes; requester must also be trusted |
| `ROCKETCHAT_AGENT_WRITE_TRUSTED_USERS` | `""` | User IDs allowed to create rooms, open DMs, resolve names, forward commands, write topics, and write cross-room |
| `ROCKETCHAT_AGENT_TOOLS_ALLOW_EXTERNAL` | `false` | Allow write tools from non-Rocket.Chat or contextless sessions |
| `ROCKETCHAT_AGENT_FILE_UPLOADS` | `false` | Enable host-file uploads (`rocketchat_send_file` and model-emitted `MEDIA:` paths) |
| `ROCKETCHAT_AGENT_FILE_ALLOWED_ROOTS` | `""` | Absolute directories files may come from, `:`-separated; POSIX only |
| `ROCKETCHAT_AGENT_FILE_MAX_BYTES` | `104857600` | Local size guard; only the literal `0` disables it |
| `ROCKETCHAT_AGENT_FILE_MAX_CONCURRENCY` | `1` | Concurrent file read/upload operations (1–4) |
| `ROCKETCHAT_RETRIEVAL_ALLOWED_ROOMS` | `""` | Exact room IDs readable outside the current room; requester must also be trusted |
| `ROCKETCHAT_RETRIEVAL_TRUSTED_USERS` | `""` | User IDs allowed to read the allowlisted rooms |
| `ROCKETCHAT_RETRIEVAL_ALLOW_CONTEXTLESS` | `false` | Allow reads without a Rocket.Chat session context, still limited to allowlisted rooms |
| `ROCKETCHAT_RETRIEVAL_MAX_RESULT_CHARS` | `75000` | Serialized tool result budget (4 096–500 000); whole records are dropped and `truncated` set |
| `ROCKETCHAT_RETRIEVAL_REDACT_SECRETS` | `true` | Redact common credential patterns in retrieved text (best effort) |
| `ROCKETCHAT_RETRIEVAL_INCLUDE_FILE_URLS` | `false` | Include attachment URLs in retrieval records |
| `ROCKETCHAT_RETRIEVAL_INCLUDE_REACTION_IDENTITIES` | `false` | Include usernames behind reactions |
| `ROCKETCHAT_RETRIEVAL_INCLUDE_USER_IDS` | `false` | Include stable user IDs in sender records |
| `ROCKETCHAT_AGENT_RESPONSE_MAX_BYTES` | `2097152` | Maximum JSON body per REST response (64 KiB–16 MiB) |
| `ROCKETCHAT_AGENT_MAX_CONCURRENCY` | `4` | Concurrent agent REST calls |
| `ROCKETCHAT_AGENT_REQUESTS_PER_MINUTE` | `120` | Per-process budget for agent REST calls |

Boolean values: `1`, `true`, `yes`, `on` enable; anything else disables.

The ten tools live in the `rocketchat_read` and `rocketchat_write` toolsets, which Hermes enables
for every platform by default. To restrict them, list the toolsets you want under
`platform_toolsets` in `config.yaml` (e.g. `rocketchat: [hermes-rocketchat, rocketchat_read]`).

## Security defaults

Rocket.Chat authorizes every API call as the bot account, not as the person who asked Hermes to
act, so the plugin adds its own authorization layer in front of every credentialed action.

- **Inbound.** The gateway's allowlist (or a pairing approval) is checked before the adapter
  downloads attachments, runs ffmpeg, fetches thread history, forwards a slash command, or writes
  a topic. Unauthorized senders get a text-only dispatch so Hermes can offer pairing; nothing else
  happens for them. Republished message documents, system messages, and messages from rooms the
  bot has not joined never start a turn.
- **Peer bots.** Messages Rocket.Chat flags as bot-generated are ignored unless they are a
  `rocketchat_delegate` task envelope. A delegated task may run an agent turn but can never
  execute a gateway control command (`/restart`, `/update`, `/sethome`, `/model` and friends), and
  replies into that DM carry a terminal envelope so two agents cannot loop.
- **Reads.** A retrieval call from a Rocket.Chat conversation may read only that room. A cross-room
  read needs the room in `ROCKETCHAT_RETRIEVAL_ALLOWED_ROOMS` **and** the requester in
  `ROCKETCHAT_RETRIEVAL_TRUSTED_USERS`. Thread and permalink calls authorize the room before looking
  up the opaque message ID, then require the returned `_id` and `rid` to match.
- **Writes.** Disabled until `ROCKETCHAT_AGENT_WRITE_TOOLS=true`. Posting to the current room is then
  allowed; creating rooms, opening DMs, resolving names, uploading host files, and any cross-room
  write need a trusted requester (and, for an existing room, the exact write allowlist).
- **Host files.** `ROCKETCHAT_AGENT_FILE_UPLOADS=true` plus at least one allowed root are required
  for `rocketchat_send_file` and for model-emitted `MEDIA:` paths. Paths are opened descriptor by
  descriptor without following symlinks and are size-bounded.
- **Network.** HTTPS by default, the token is sent only to the configured origin, API redirects are
  refused, attachment redirects are followed once without the token, and every response body, DDP
  frame, media download, thread context, and tool result is bounded.
- **Profiles.** Settings are read through Hermes' profile-scoped secret reader and one token is
  locked to one running gateway, so multiplexed profiles never share a bot.

Authorization uses Hermes' task-local session context; process-global `HERMES_SESSION_*` values
are ignored. Retrieved text is marked untrusted and heuristically redacted, but it can still
contain prompt injection: keep write tools off for research-only deployments and require human
review for consequential actions. Use a dedicated bot account with the minimum room memberships,
never an administrator token.

## Features

| Feature | Notes |
|---|---|
| Inbound stream | DDP `__my_messages__`; login result verified, subscription readiness tracked, republished documents filtered, keepalive and reconnect with backoff |
| Outbound | `chat.postMessage`, `chat.update`, `chat.delete`; UTF-16-aware splitting at 5000 |
| Threads | Channel and group replies, clarifications and status messages stay under one root in `thread` mode; DMs stay flat; first turn in a thread gets the newest replies as context |
| Media | Attachment download (including object-storage redirects), voice → MP3 via ffmpeg, uploads through `rooms.media` + `rooms.mediaConfirm`, cron deliveries with attachments |
| Reactions | 👀 / ✅ / ❌ with idempotent `shouldReact` |
| Typing indicator | `user-activity` with the identity the workspace expects, thread-aware |
| Topic sync | Optional, default off, trusted writers only |
| Slash commands | Hermes commands at position 0; Rocket.Chat-native commands forwarded only when allowlisted |
| Agent tools | Ten tools, read/write split, see below |
| Configuration | `.env` or `config.yaml`, profile-scoped, setup wizard |

## Agent tools

| Tool | Use it for | Parameters | Bot permission needed |
|---|---|---|---|
| `rocketchat_list_channels` | Discover the rooms the session may read | optional `filter`; paginates the server list | `view-c-room`; private groups only where the bot is a member |
| `rocketchat_search_messages` | Find decisions, incidents, owners inside one room | `room_id`, `query`; `count` 1–100 (25), `offset` | member with read access |
| `rocketchat_get_history` | Summarize or audit a slice of one room | `room_id`; `count` 1–100 (50), `offset`, `oldest`, `latest`, `inclusive`, `include_threads` | member with read access |
| `rocketchat_get_thread` | Reconstruct a thread, newest replies first | `tmid`; optional expected `room_id`; `limit` 1–500 (100) | read access to root and room |
| `rocketchat_get_permalink` | Stable link for a ticket or hand-off | `message_id`; optional expected `room_id` | read access to message and room |
| `rocketchat_create_channel` | Create a project or incident room | `name`; `private`, `members` | `create-c` / `create-p` |
| `rocketchat_post` | Deliver a result to another room | `message` plus `channel` or `room_id` | member able to post |
| `rocketchat_send_file` | Deliver a report or export | `file_path` plus exactly one of `room_id`, `username`, `channel`; `caption`, `file_name`, `tmid` | member able to upload |
| `rocketchat_dm` | Open a private workflow or a reminder target | `username`; optional `message`; returns the `room_id` | may create DMs |
| `rocketchat_delegate` | Hand one task to another Hermes agent | `username`, `message`; returns `delegation_id` | may create DMs |

Combined with the built-in `cronjob` tool: *"remind @zed about the deploy tomorrow at 9, in a
DM"* opens the DM with `rocketchat_dm` and schedules delivery to `rocketchat:<room_id>`, which
works even if the gateway restarts in between. Cron deliveries can carry attachments.

### Loop-safe delegation

Use `rocketchat_delegate`, not `rocketchat_dm`, when the target is another Hermes agent. The tool
wraps the request in a versioned task envelope; the receiving adapter strips it, runs the task once
with gateway control disabled, and marks every reply in that DM as a terminal result that the
sender ignores. Servers that omit bot metadata can list peer bots in `ROCKETCHAT_BOT_PEERS`.

### Retrieval notes

All retrieval tools return compact normalized records marked as untrusted, obey the privacy
opt-ins and the result budget, and slice locally even if the server ignores `count`. `total` is
`null` when the server omits it. Out-of-range counts and negative offsets are rejected rather than
clamped. Search forwards the query verbatim; Rocket.Chat interprets `/regex/` and `from:` /
`before:` operators server-side.

### Thread context

When the bot is mentioned in a thread it has not participated in, it fetches the thread root and
the newest replies (up to 30, 20 000 characters) and passes them to the model as channel context,
separately from the triggering message. Entries are collapsed to one line each and senders the
gateway would not authorize are tagged `[unverified sender]`. This happens only on the bot's first
turn in the thread; afterwards the session history carries the conversation.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| Plugin disabled after updating to 1.5.0; Rocket.Chat adapter missing | Fixed in 1.5.1: Ruff configuration no longer makes Hermes treat the plugin as a Python package. Update, run `hermes plugins enable rocketchat-platform`, then restart the gateway. |
| `Rocket.Chat rejected the DDP resume token` in the log, adapter shows a fatal error | The PAT is invalid, revoked, or was created without "Ignore Two Factor". Generate a new one and restart the gateway. |
| `failed to authenticate` at connect | Verify with `curl -H "X-Auth-Token: TOKEN" -H "X-User-Id: ID" https://rc/api/v1/me`. |
| Bot ignores a channel | Invite it (`/invite @bot`); a public room it has not joined is ignored by design. Check `ROCKETCHAT_ALLOWED_USERS`. |
| Bot re-answered an old thread question mid-conversation | Fixed in 1.5.0 (republished documents). Update the plugin. |
| Attachments never arrive on an S3/GCS workspace | Fixed in 1.5.0. For a private-address object store set `ROCKETCHAT_ALLOW_PRIVATE_FILE_REDIRECTS=true` or enable "Proxy uploads". |
| `429 Too Many Requests` | Tune the Rocket.Chat rate limiter or exempt the bot's IP. |
| Desktop client says "invalid command" for `/new` | Enable `Message_AllowUnrecognizedSlashCommand`. |
| Voice messages are not transcribed | Install `ffmpeg` on the gateway host. |
| `bot account is already in use by another Hermes profile` | Two gateways or profiles share one token; give each its own bot user. |
| WebSocket reconnects every minute | Raise the reverse proxy's read timeout (see above). |

## Verification

`hermes gateway status` lists Rocket.Chat as configured once the three required variables are
set; after a restart the log shows `DDP logged in` followed by `subscribed to the message
stream`. Send the bot a DM to confirm the full path.

## Architecture

```
Rocket.Chat ──DDP stream-room-messages──► ddp.py ──task──► inbound.py ──MessageEvent──► Hermes runner
            ◄──REST /api/v1/*──────────── adapter.py / media.py / tools.py ◄──────────── agent
```

The full model, including the inbound pipeline, the republish classifier, the DDP state machine,
the authorization table, and the invariants the tests pin, is in
[docs/architecture.md](docs/architecture.md). Security policy: [SECURITY.md](SECURITY.md).

## Development

```bash
git clone https://github.com/NousResearch/hermes-agent
python -m pip install -e ./hermes-agent pytest pytest-asyncio pytest-timeout ruff
HERMES_AGENT_PATH=./hermes-agent make test
make lint
make doctor   # hermes plugins doctor . --ci
```

The tests use fake aiohttp and DDP doubles and never contact a server. A live smoke test
against a disposable local workspace (Docker) or your own one is described in
[CONTRIBUTING.md](CONTRIBUTING.md).

## Credits

- Original adapter: [hermes-agent#4637](https://github.com/NousResearch/hermes-agent/pull/4637) by
  [@meron1122](https://github.com/meron1122) and
  [hermes-agent#14869](https://github.com/NousResearch/hermes-agent/pull/14869) by @cyb0rgk1tty
- Extended plugin (topic sync, slash commands, voice, reconnect):
  [hermes-agent#30463](https://github.com/NousResearch/hermes-agent/pull/30463) by
  [@HearthCore](https://github.com/HearthCore)
- Agent file uploads: [#1](https://github.com/HalfbitStudio/hermes-plugin-rocketchat/pull/1) by
  [@YounesAmalou](https://github.com/YounesAmalou)
- Keepalive fix and the republished-message diagnosis:
  [#4](https://github.com/HalfbitStudio/hermes-plugin-rocketchat/pull/4),
  [#5](https://github.com/HalfbitStudio/hermes-plugin-rocketchat/pull/5) by
  [@immodigit](https://github.com/immodigit)
- 1.5.0 audit and hardening by [Andrew Vieyra](https://github.com/andrewvieyra)
  (<andrew@andrewvieyra.com>): API-conformance and security audit against the Rocket.Chat
  docs and server source and the Hermes 0.21 adapter contract; the republish guard, DDP
  login/subscription handling, off-loop inbound processing, stream room metadata, idempotent
  reactions, `commands.run` and `im.create` fixes, object-storage redirects, UTF-16 splitting,
  cron media, profile-scoped configuration with the credential lock and `config.yaml` bridge,
  the admission model without `internal=True`; the split test suite, real-loader test, CI, ruff
  configuration; the live smoke test with its disposable Rocket.Chat + MinIO stack; and this
  documentation set (README, AGENTS.md, CHANGELOG, `docs/architecture.md`, SECURITY.md,
  CONTRIBUTING.md).

Published as a standalone repository per the
[hermes-agent plugin policy](https://github.com/NousResearch/hermes-agent/blob/main/CONTRIBUTING.md).
MIT licensed, same as hermes-agent.
