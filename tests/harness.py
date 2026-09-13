"""Shared test helpers: plugin loader, adapter factory, fake aiohttp doubles."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

PLUGIN_ROOT = Path(__file__).resolve().parents[1]
MODULE_NAME = "rocketchat_plugin"


def stub_platform_registration() -> None:
    """Make ``Platform("rocketchat")`` resolve under pytest, where the plugin system never runs."""
    from gateway.platform_registry import PlatformEntry, platform_registry

    if not platform_registry.is_registered("rocketchat"):
        platform_registry.register(
            PlatformEntry(
                name="rocketchat",
                label="Rocket.Chat",
                adapter_factory=lambda cfg: None,
                check_fn=lambda: True,
            )
        )


def load_plugin():
    """Import the plugin as a package under a fixed module name.

    The repository directory name is not a valid Python identifier, so import by
    path with explicit package search locations so relative imports work.
    """
    if MODULE_NAME in sys.modules:
        return sys.modules[MODULE_NAME]
    stub_platform_registration()
    spec = importlib.util.spec_from_file_location(
        MODULE_NAME, PLUGIN_ROOT / "__init__.py", submodule_search_locations=[str(PLUGIN_ROOT)]
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[MODULE_NAME] = module
    spec.loader.exec_module(module)
    return module


def submodule(name: str):
    load_plugin()
    return sys.modules[f"{MODULE_NAME}.{name}"]


def make_adapter(extra: Optional[dict] = None):
    from gateway.config import PlatformConfig

    rc = load_plugin()
    config = PlatformConfig(
        enabled=True,
        extra={
            "url": "https://rc.example.com",
            "token": "pat",
            "user_id": "bot_uid",
            **(extra or {}),
        },
    )
    adapter = rc.RocketchatAdapter(config)
    adapter._bot_username = "hermesbot"
    adapter._typing_name = "hermesbot"
    return adapter


def make_post(**overrides: Any) -> Dict[str, Any]:
    """A fresh Rocket.Chat message document as delivered over DDP (EJSON dates)."""
    post: Dict[str, Any] = {
        "_id": "msg1",
        "rid": "room1",
        "msg": "hello",
        "ts": {"$date": 1788432317864},
        "_updatedAt": {"$date": 1788432317950},
        "u": {"_id": "u1", "username": "alice", "name": "Alice"},
    }
    post.update(overrides)
    return post


class FakeResponse:
    """Minimal aiohttp response double usable with the plugin's bounded readers."""

    def __init__(self, status: int = 200, body: Any = None, headers: Optional[dict] = None,
                 content_type: str = "application/json"):
        self.status = status
        self._body = body if body is not None else {"success": True}
        self.headers = headers or {}
        self.content_type = content_type

    async def read(self) -> bytes:
        if isinstance(self._body, (bytes, bytearray)):
            return bytes(self._body)
        return json.dumps(self._body).encode("utf-8")

    async def json(self, content_type=None):
        return self._body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class FakeSession:
    """Records requests; ``routes`` maps a URL substring to a FakeResponse or a list of them."""

    def __init__(self, routes: Optional[Dict[str, Any]] = None):
        self.routes = routes or {}
        self.calls: List[Dict[str, Any]] = []
        self.closed = False

    def _respond(self, method: str, url: str, **kwargs) -> FakeResponse:
        self.calls.append({"method": method, "url": url, **kwargs})
        for needle, response in self.routes.items():
            if needle in url:
                if isinstance(response, list):
                    return response.pop(0) if response else FakeResponse(500, {"success": False})
                return response
        return FakeResponse(404, {"success": False, "error": "not found"})

    def get(self, url, **kwargs):
        return self._respond("GET", url, **kwargs)

    def post(self, url, **kwargs):
        return self._respond("POST", url, **kwargs)

    def request(self, method, url, **kwargs):
        return self._respond(method, url, **kwargs)

    async def close(self):
        self.closed = True

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False
