"""Configuration surface: scoped env reads, YAML bridge, dependency probe."""

import os

import pytest

from harness import load_plugin, submodule

rc = load_plugin()
helpers = submodule("helpers")


class TestEnvGet:
    def test_unset_returns_default(self, monkeypatch):
        monkeypatch.delenv("ROCKETCHAT_URL", raising=False)
        assert helpers.env_get("ROCKETCHAT_URL") is None
        assert helpers.env_get("ROCKETCHAT_URL", "x") == "x"

    def test_set_value_wins(self, monkeypatch):
        monkeypatch.setenv("ROCKETCHAT_URL", "https://rc.example.com")
        assert helpers.env_get("ROCKETCHAT_URL") == "https://rc.example.com"

    def test_scoped_miss_never_borrows_process_env(self, monkeypatch):
        monkeypatch.setenv("ROCKETCHAT_TOKEN", "default-profile-pat")
        from agent import secret_scope

        secret_scope.set_multiplex_active(True)
        token = secret_scope.set_secret_scope({"OTHER": "1"})
        try:
            assert helpers.env_get("ROCKETCHAT_TOKEN") is None
        finally:
            secret_scope.reset_secret_scope(token) if hasattr(secret_scope, "reset_secret_scope") else secret_scope._SECRET_SCOPE.reset(token)
            secret_scope.set_multiplex_active(False)

    @pytest.mark.parametrize("raw,expected", [("true", True), ("on", True), ("1", True), ("off", False), ("false", False), ("0", False), ("no", False), ("", True)])
    def test_env_flag_default_true(self, monkeypatch, raw, expected):
        monkeypatch.setenv("ROCKETCHAT_REQUIRE_MENTION", raw)
        assert helpers._env_flag("ROCKETCHAT_REQUIRE_MENTION", default=True) is expected


class TestRequirementsSplit:
    def test_check_requirements_is_a_dependency_probe(self):
        assert helpers.check_requirements() is True

    def test_credentials_configured(self, monkeypatch):
        assert helpers.credentials_configured() is False
        monkeypatch.setenv("ROCKETCHAT_URL", "https://rc.example.com")
        monkeypatch.setenv("ROCKETCHAT_TOKEN", "pat")
        monkeypatch.setenv("ROCKETCHAT_USER_ID", "bot")
        assert helpers.credentials_configured() is True

    def test_tool_check_functions_require_credentials(self, monkeypatch):
        assert rc._read_tool_requirements() is False
        monkeypatch.setenv("ROCKETCHAT_URL", "https://rc.example.com")
        monkeypatch.setenv("ROCKETCHAT_TOKEN", "pat")
        monkeypatch.setenv("ROCKETCHAT_USER_ID", "bot")
        assert rc._read_tool_requirements() is True
        assert rc._write_tool_requirements() is False
        monkeypatch.setenv("ROCKETCHAT_AGENT_WRITE_TOOLS", "true")
        assert rc._write_tool_requirements() is True


class TestYamlBridge:
    def test_keys_become_env_and_extra(self, monkeypatch):
        seeded = helpers._apply_yaml_config(
            {},
            {
                "url": "https://rc.example.com",
                "token": "pat",
                "user_id": "bot",
                "require_mention": False,
                "allowed_users": ["u1", "u2"],
                "home_channel": "GENERAL",
                "agent_file_allowed_roots": ["/srv/reports", "/srv/exports"],
            },
        )
        assert os.environ["ROCKETCHAT_URL"] == "https://rc.example.com"
        assert os.environ["ROCKETCHAT_REQUIRE_MENTION"] == "false"
        assert os.environ["ROCKETCHAT_ALLOWED_USERS"] == "u1,u2"
        assert os.environ["ROCKETCHAT_AGENT_FILE_ALLOWED_ROOTS"] == os.pathsep.join(["/srv/reports", "/srv/exports"])
        assert seeded["home_channel"] == {"chat_id": "GENERAL", "name": "GENERAL"}
        assert seeded["url"] == "https://rc.example.com"

    def test_env_wins_over_yaml(self, monkeypatch):
        monkeypatch.setenv("ROCKETCHAT_URL", "https://env.example.com")
        helpers._apply_yaml_config({}, {"url": "https://yaml.example.com"})
        assert os.environ["ROCKETCHAT_URL"] == "https://env.example.com"

    def test_empty_or_invalid_config_is_ignored(self):
        assert helpers._apply_yaml_config({}, {}) is None
        assert helpers._apply_yaml_config({}, "nope") is None

    def test_registered_with_hermes(self):
        registered = {}

        class Ctx:
            manifest = type("M", (), {"name": "rocketchat-platform"})()

            def register_tool(self, **kw):
                pass

            def register_platform(self, **kw):
                registered.update(kw)

        rc.register(Ctx())
        assert registered["apply_yaml_config_fn"] is helpers._apply_yaml_config
        assert registered["check_fn"] is helpers.check_requirements
