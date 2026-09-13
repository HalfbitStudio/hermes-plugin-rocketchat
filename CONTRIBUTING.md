# Contributing

This plugin is a standalone repository by design: Hermes does not merge third-party product
integrations into its core tree, so Rocket.Chat work happens here.

## Ground rules

- Every inbound message is untrusted data. Anything that spends the bot's credentials
  (attachment downloads, thread history, `commands.run`, topic writes, agent tools) must be gated
  by the gateway's authorization and by an explicit, default-off, exact-match operator setting.
  Do not add wildcard allowlists.
- Read configuration through `helpers.env_get` (profile-scoped), never `os.getenv`, so
  multiplexed Hermes profiles keep their own credentials and allowlists.
- Verify Rocket.Chat responses before acting on them: check `success`, the returned `_id`/`rid`
  against the requested target, and the room type. Never log message text or tokens.
- Behaviour that depends on a Rocket.Chat server quirk gets a comment naming the server file
  or endpoint that causes it, and a test that pins the quirk.
- Match the existing style: type hints, short docstrings that explain *why*, no dead code.

## Workflow

```bash
git clone https://github.com/HalfbitStudio/hermes-plugin-rocketchat ~/.hermes/plugins/rocketchat-platform
cd ~/.hermes/plugins/rocketchat-platform
python -m pip install -e ~/.hermes/hermes-agent pytest pytest-asyncio pytest-timeout ruff
make test          # HERMES_AGENT_PATH defaults to ~/.hermes/hermes-agent
make lint
make doctor        # needs the hermes CLI on PATH
```

The tests run against fake aiohttp/DDP doubles (`tests/harness.py`); no Rocket.Chat server is
contacted. `tests/test_plugin_load.py` loads the plugin through Hermes' real `PluginManager`
with a temporary `HERMES_HOME`, so registration mistakes surface without a gateway.

## Live smoke test

`scripts/smoke_test.py` drives the real adapter against a Rocket.Chat workspace without a
gateway or a model: a recording message handler stands in for the runner and a second "human"
user drives the server over REST. It checks the DDP login and subscription, a DM round trip,
reactions, the republished-message guard (thread replies, a reaction and a pin on a root must
not re-dispatch it), attachment download including the object-storage redirect, the typing
identity, DM verification, the standalone cron sender with a file, `delete_message`, and that an
invalid token is reported as a fatal error. It writes only into a test channel it creates and the
bot/human DM, and deletes what it created.

Disposable local workspace (needs Docker; Colima works on macOS):

```bash
LAN_IP=$(ipconfig getifaddr en0)   # address the Rocket.Chat container reaches MinIO on
docker compose -f scripts/smoke/compose.yml up -d
python scripts/smoke/bootstrap.py --lan-ip "$LAN_IP" --out /tmp/rc-smoke.env
HERMES_AGENT_PATH=~/.hermes/hermes-agent python scripts/smoke_test.py --env-file /tmp/rc-smoke.env
docker compose -f scripts/smoke/compose.yml down -v
```

MinIO plays the S3 backend with "Proxy uploads" off, so `/file-upload/` answers with a redirect
to a signed URL, which is the path a hosted workspace with S3 or GCS storage takes.

Against your own workspace, create a test user and Personal Access Tokens for it and the bot,
write them to an env file (see the docstring in `scripts/smoke_test.py` for the keys), and run
the script with `--env-file`. Use a staging workspace where possible.

## Pull requests

- One change per PR with a short description of the behaviour, not the diff.
- Add a line under `[Unreleased]` in `CHANGELOG.md`.
- Keep the three configuration inventories in sync: `plugin.yaml`, the README table, and the
  YAML bridge in `helpers.py`.
- CI must be green: tests on every supported Python, ruff clean.

## Reporting security issues

See [SECURITY.md](SECURITY.md).
