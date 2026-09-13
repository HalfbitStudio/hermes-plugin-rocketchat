"""Put the selected hermes-agent checkout on sys.path for adapter imports.

Tests import ``gateway.*`` directly from a Hermes source checkout. Set
``HERMES_AGENT_PATH`` to that checkout; it defaults to ``../hermes-agent``.
The checkout must also be installed with pip so Hermes runtime dependencies are
available; this path setting alone does not install them.
"""

import os
import sys
from pathlib import Path

_HERMES = Path(
    os.environ.get(
        "HERMES_AGENT_PATH",
        Path(__file__).resolve().parents[2] / "hermes-agent",
    )
).resolve()

if not (_HERMES / "gateway").is_dir():
    raise RuntimeError(
        f"hermes-agent checkout not found at {_HERMES}. "
        "Clone https://github.com/NousResearch/hermes-agent and set HERMES_AGENT_PATH."
    )

sys.path.insert(0, str(_HERMES))


import pytest  # noqa: E402


@pytest.fixture(autouse=True)
def _clean_rocketchat_env(monkeypatch):
    """Isolate every test from ambient ROCKETCHAT_* variables and give it a Rocket.Chat session context."""
    from gateway import session_context

    for key in list(os.environ):
        if key.startswith("ROCKETCHAT_"):
            monkeypatch.delenv(key, raising=False)
    for key in (
        "HERMES_SESSION_PLATFORM",
        "HERMES_SESSION_CHAT_ID",
        "HERMES_SESSION_USER_ID",
        "HERMES_SESSION_THREAD_ID",
        "HERMES_SESSION_KEY",
    ):
        monkeypatch.delenv(key, raising=False)
    tokens = session_context.set_session_vars(
        platform="rocketchat",
        chat_id="r1",
        user_id="u1",
        session_key="rocketchat:r1",
    )
    yield
    session_context.clear_session_vars(tokens)
