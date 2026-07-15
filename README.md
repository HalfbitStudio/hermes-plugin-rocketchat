# Rocket.Chat Plugin for Hermes Agent

Connects Hermes Agent to a self-hosted Rocket.Chat instance via REST API v1 (outbound) and DDP WebSocket (inbound). Ships as a standalone plugin — zero changes to Hermes core files, no extra Python dependencies (uses `aiohttp`, already shipped with Hermes).

---

## Installation

```bash
hermes plugins install meron1122/hermes-plugin-rocketchat
```

The installer clones this repo into `~/.hermes/plugins/rocketchat-platform/` and prompts you to enable it. If you skipped the prompt:

```bash
hermes plugins enable rocketchat-platform
hermes gateway restart
```

---

## Quick Start

### 1. Create a Bot on Rocket.Chat

1. Log into Rocket.Chat as admin
2. Go to **Admin** → **Users** → **New**
3. Set username to `hermes-bot`, role: `bot`
4. Save

### 2. Generate a Personal Access Token

1. Log in as the bot user
2. Go to **Account** → **Personal Access Tokens**
3. Give it a name (e.g. `hermes-gateway`)
4. **Check ☑ Ignore Two Factor Authentication** — this is critical
5. Copy the **Token** and **User ID** right away — you won't see them again

### 3. Configure

Either use the setup wizard:

```bash
hermes gateway setup
```

Select Rocket.Chat → enter URL, Token, and User ID when prompted.

Or configure manually in `~/.hermes/.env`:

```bash
ROCKETCHAT_URL=https://rc.example.com
ROCKETCHAT_TOKEN=your_pat_token
ROCKETCHAT_USER_ID=your_bot_user_id
ROCKETCHAT_ALLOWED_USERS=your_user_id
```

### 4. Restart the Gateway

```bash
systemctl restart hermes-gateway
# or via Telegram: /restart
```

---

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `ROCKETCHAT_URL` | ✅ | — | Server URL (e.g. `https://rc.example.com`) |
| `ROCKETCHAT_TOKEN` | ✅ | — | Personal Access Token (PAT) |
| `ROCKETCHAT_USER_ID` | ✅ | — | Bot user `_id` |
| `ROCKETCHAT_ALLOWED_USERS` | — | `""` | Comma-separated list of allowed user IDs |
| `ROCKETCHAT_ALLOW_ALL_USERS` | — | `false` | Allow all users (dev only) |
| `ROCKETCHAT_HOME_CHANNEL` | — | — | Room ID for cron / notification delivery |
| `ROCKETCHAT_REQUIRE_MENTION` | — | `true` | Require @mention to trigger in channels |
| `ROCKETCHAT_FREE_RESPONSE_CHANNELS` | — | — | Room IDs where @mention is not required |
| `ROCKETCHAT_REPLY_MODE` | — | `off` | `thread` for threaded replies, `off` for flat |

---

## Features

| Feature | Status |
|---------|--------|
| DDP WebSocket (inbound) | ✅ `__my_messages__` subscription |
| REST API (outbound) | ✅ `chat.postMessage` |
| File upload | ✅ Two-step `rooms.media` + `rooms.mediaConfirm` |
| Attachment download | ✅ With image/audio/document cache |
| Thread support | ✅ Via `tmid` |
| Mention gating | ✅ Configurable per room |
| Typing indicator | ✅ Rocket.Chat 8.x compatible |
| Reconnect | ✅ Exponential backoff (2s–60s) |
| Voice message → STT | ✅ ffmpeg MP3 conversion pipeline |
| Emoji reactions | ✅ 👀✅❌ on channel messages |
| Topic sync | ✅ Bidirectional (Hermes session title ↔ RC room topic) |
| Slash command routing | ✅ Position-0 only, gated via `is_gateway_known_command()` |
| Deferred attachments | ✅ File-only uploads merged with next text message |
| Cron delivery | ✅ Standalone REST-only sender |
| Setup wizard | ✅ `hermes gateway setup` |
| Plugin discovery | ✅ Auto-discover via `kind: platform` |

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `totp-required` | PAT was created without "Ignore Two Factor" — generate a new one with the checkbox checked |
| "Failed to authenticate" | Verify with: `curl -H "X-Auth-Token: TOKEN" -H "X-User-Id: ID" https://rc/api/v1/me` |
| Bot doesn't respond | Make sure the bot has been invited to the channel and check `ROCKETCHAT_ALLOWED_USERS` |
| WebSocket keeps disconnecting | Set `proxy_read_timeout 600s` in nginx; also check your Mongo Replica Set status |
| Rate-limited (429) | Tune the Rocket.Chat rate limiter for the bot's IP |
| Unrecognized slash commands on desktop | RC Desktop intercepts unknown `/` commands client-side. Set `Message_AllowUnrecognizedSlashCommand=true` in RC Admin (Settings → Message) or via env: `OVERWRITE_SETTING_Message_AllowUnrecognizedSlashCommand=true` |

---

## Verification

Once configured, `hermes status` should show:

```
Rocket.Chat 🚀 ✓ configured (plugin)
```

Send a DM to the bot in Rocket.Chat to test the connection end-to-end.

---

## Architecture

```
Rocket.Chat ←── REST /api/v1/chat.postMessage ──→ Hermes Agent
           ←── DDP WebSocket stream-room-messages ──→ (inbound)
```

- **Auth:** Personal Access Token (works for both REST and DDP)
- **Room detection:** `rooms.info` + lazy cache
- **System messages:** Filtered out by the `t` field (join/leave/role changes, etc.)
- **Desktop note:** RC Desktop/Browser may intercept unknown `/` commands. Mobile clients work out of the box.

---

## Development

Tests need a hermes-agent checkout (the adapter imports `gateway.*` at runtime):

```bash
git clone https://github.com/NousResearch/hermes-agent
pip install pytest pytest-asyncio aiohttp pyyaml
HERMES_AGENT_PATH=./hermes-agent pytest tests/ -v
```

`HERMES_AGENT_PATH` defaults to `../hermes-agent` when unset.

---

## Credits

- Original Rocket.Chat adapter: [hermes-agent#4637](https://github.com/NousResearch/hermes-agent/pull/4637) by [@meron1122](https://github.com/meron1122) and [hermes-agent#14869](https://github.com/NousResearch/hermes-agent/pull/14869) by @cyb0rgk1tty
- Extended plugin version (topic sync, slash commands, voice pipeline, reconnect): [hermes-agent#30463](https://github.com/NousResearch/hermes-agent/pull/30463) by [@HearthCore](https://github.com/HearthCore)

Published as a standalone repo per the [hermes-agent plugin policy](https://github.com/NousResearch/hermes-agent/blob/main/CONTRIBUTING.md) — third-party integrations ship as external plugins.

MIT licensed, same as hermes-agent.
