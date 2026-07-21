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
