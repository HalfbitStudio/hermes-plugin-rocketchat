"""Put a hermes-agent checkout on sys.path so the adapter's imports resolve.

hermes-agent is not distributed as a pip package — the adapter imports
``gateway.*`` from the repo root. Set ``HERMES_AGENT_PATH`` to your checkout;
defaults to ``../hermes-agent``.
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
