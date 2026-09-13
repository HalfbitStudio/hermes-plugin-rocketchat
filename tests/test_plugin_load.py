"""Load the plugin through Hermes' real ``PluginManager`` with a temporary HERMES_HOME."""

import shutil
import sys
import tempfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def hermes_home(monkeypatch):
    tmp = Path(tempfile.mkdtemp())
    home = tmp / "hermes-home"
    (home / "plugins").mkdir(parents=True)
    shutil.copytree(
        REPO_ROOT,
        home / "plugins" / "rocketchat-platform",
        ignore=shutil.ignore_patterns("tests", ".git", "__pycache__", ".github", ".audit", ".coverage"),
    )
    (home / "config.yaml").write_text("plugins:\n  enabled: [rocketchat-platform]\n")
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("ROCKETCHAT_URL", "https://rc.example.com")
    monkeypatch.setenv("ROCKETCHAT_TOKEN", "pat")
    monkeypatch.setenv("ROCKETCHAT_USER_ID", "bot_uid")
    monkeypatch.setenv("ROCKETCHAT_AGENT_WRITE_TOOLS", "true")
    yield home
    shutil.rmtree(tmp, ignore_errors=True)
    for name in [m for m in sys.modules if m.startswith("hermes_plugins.rocketchat")]:
        sys.modules.pop(name, None)


def test_registration_through_real_loader(hermes_home):
    from hermes_cli.plugins import PluginManager
    from tools.registry import registry

    mgr = PluginManager()
    mgr.discover_and_load()
    loaded = {p["name"]: p for p in mgr.list_plugins()}
    assert "rocketchat-platform" in loaded, sorted(loaded)
    assert loaded["rocketchat-platform"]["error"] is None
    assert loaded["rocketchat-platform"]["tools"] == 10
    for name, toolset in (
        ("rocketchat_search_messages", "rocketchat_read"),
        ("rocketchat_get_history", "rocketchat_read"),
        ("rocketchat_post", "rocketchat_write"),
        ("rocketchat_delegate", "rocketchat_write"),
    ):
        assert registry.get_schema(name), name
        assert registry.get_toolset_for_tool(name) == toolset

    from gateway.platform_registry import platform_registry

    entry = platform_registry.get("rocketchat") if hasattr(platform_registry, "get") else None
    assert platform_registry.is_registered("rocketchat")
    if entry is not None:
        assert entry.apply_yaml_config_fn is not None
        assert entry.standalone_sender_fn is not None
