"""Inbound behaviour changed in 1.5.0: stream metadata, membership, mentions, reactions, commands, media."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from harness import FakeResponse, load_plugin, make_adapter, make_post

rc = load_plugin()


def _wired(room_type=None, authorized=True):
    adapter = make_adapter()
    adapter._inbound_authorization_checker = (lambda source: authorized)
    adapter.handle_message = AsyncMock()
    adapter._api_post = AsyncMock(return_value={"success": True})
    adapter._api_get = AsyncMock(return_value={})
    adapter._download_attachments = AsyncMock(return_value=([], []))
    if room_type:
        adapter._room_type_cache["room1"] = room_type
    return adapter


class TestStreamRoomMetadata:
    @pytest.mark.asyncio
    async def test_room_type_from_stream_avoids_rest_lookup(self):
        adapter = _wired()
        await adapter._handle_message(make_post(), {"roomParticipant": True, "roomType": "d", "roomName": None})
        adapter._api_get.assert_not_awaited()
        assert adapter._room_type_cache["room1"] == "dm"
        adapter.handle_message.assert_awaited_once()
        assert adapter.handle_message.await_args.args[0].source.chat_type == "dm"

    @pytest.mark.asyncio
    async def test_unknown_room_type_is_dropped_not_guessed(self):
        adapter = _wired()
        adapter._api_get = AsyncMock(return_value={})  # rooms.info failed
        await adapter._handle_message(make_post())
        adapter.handle_message.assert_not_awaited()
        assert "room1" not in adapter._room_type_cache

    @pytest.mark.asyncio
    async def test_non_member_public_room_is_ignored_by_default(self):
        adapter = _wired()
        await adapter._handle_message(
            make_post(msg="@hermesbot hi", mentions=[{"_id": "bot_uid", "username": "hermesbot"}]),
            {"roomParticipant": False, "roomType": "c", "roomName": "general"},
        )
        adapter.handle_message.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_non_member_room_allowed_when_membership_not_required(self, monkeypatch):
        monkeypatch.setenv("ROCKETCHAT_REQUIRE_MEMBERSHIP", "false")
        adapter = _wired()
        await adapter._handle_message(
            make_post(msg="@hermesbot hi", mentions=[{"_id": "bot_uid", "username": "hermesbot"}]),
            {"roomParticipant": False, "roomType": "c", "roomName": "general"},
        )
        adapter.handle_message.assert_awaited_once()


class TestMentions:
    @pytest.mark.asyncio
    async def test_all_and_here_are_not_bot_mentions(self):
        adapter = _wired(room_type="channel")
        await adapter._handle_message(make_post(msg="@all standup in 5", mentions=[{"_id": "all", "username": "all"}]))
        await adapter._handle_message(make_post(_id="msg2", msg="@here lunch", mentions=[{"_id": "here", "username": "here"}]))
        adapter.handle_message.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_real_mention_still_triggers(self):
        adapter = _wired(room_type="channel")
        await adapter._handle_message(make_post(msg="@hermesbot hi", mentions=[{"_id": "bot_uid", "username": "hermesbot"}]))
        adapter.handle_message.assert_awaited_once()
        assert adapter.handle_message.await_args.args[0].text == "hi"


class TestBotPeersAndDelegation:
    @pytest.mark.asyncio
    async def test_integration_bot_object_flag_is_ignored(self):
        adapter = _wired(room_type="dm")
        await adapter._handle_message(make_post(bot={"i": "integration-1"}))
        adapter.handle_message.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_delegated_task_cannot_run_gateway_commands(self):
        adapter = _wired(room_type="dm")
        envelope = rc.helpers.build_delegation_envelope("task", "a" * 32, "/restart now please")
        await adapter._handle_message(make_post(msg=envelope, bot=True))
        event = adapter.handle_message.await_args.args[0]
        assert event.allow_gateway_control is False
        assert event.is_command() is False
        assert event.text == "/restart now please"
        adapter._api_post.assert_not_awaited()  # never forwarded to commands.run


class TestReactions:
    @pytest.mark.asyncio
    async def test_add_and_remove_use_should_react(self):
        adapter = _wired()
        await adapter._add_reaction("m1", ":eyes:")
        await adapter._remove_reaction("m1", ":eyes:")
        payloads = [call.args[1] for call in adapter._api_post.await_args_list]
        assert payloads == [
            {"messageId": "m1", "emoji": ":eyes:", "shouldReact": True},
            {"messageId": "m1", "emoji": ":eyes:", "shouldReact": False},
        ]

    @pytest.mark.parametrize("raw,enabled", [("false", False), ("off", False), ("0", False), ("true", True), ("on", True)])
    def test_reactions_flag_parsing(self, monkeypatch, raw, enabled):
        monkeypatch.setenv("ROCKETCHAT_REACTIONS", raw)
        assert make_adapter()._reactions_enabled() is enabled


class TestSlashForwarding:
    @pytest.mark.asyncio
    async def test_commands_run_gets_bare_command_name(self, monkeypatch):
        monkeypatch.setenv("ROCKETCHAT_AGENT_WRITE_TOOLS", "true")
        monkeypatch.setenv("ROCKETCHAT_AGENT_WRITE_TRUSTED_USERS", "u1")
        monkeypatch.setenv("ROCKETCHAT_FORWARDED_SLASH_COMMANDS", "giphy")
        adapter = _wired(room_type="dm")
        await adapter._handle_message(make_post(msg="/giphy cat", tmid=None))
        adapter._api_post.assert_awaited_once_with(
            "commands.run", {"command": "giphy", "roomId": "room1", "params": "cat"}
        )
        adapter.handle_message.assert_not_awaited()


class TestOutbound:
    @pytest.mark.asyncio
    async def test_send_chunks_in_utf16_units(self):
        adapter = _wired(room_type="dm")
        adapter._api_post = AsyncMock(return_value={"success": True, "message": {"_id": "x", "rid": "room1"}})
        result = await adapter.send("room1", "😀" * 3000)
        assert result.success
        assert adapter._api_post.await_count == 2
        from gateway.platforms.base import utf16_len

        for call in adapter._api_post.await_args_list:
            assert utf16_len(call.args[1]["text"]) <= rc.helpers.MAX_MESSAGE_LENGTH

    @pytest.mark.asyncio
    async def test_send_skips_post_when_only_directives_remain(self):
        adapter = _wired(room_type="dm")
        result = await adapter.send("room1", "MEDIA:/tmp/chart.png")
        assert result.success
        adapter._api_post.assert_not_awaited()

    def test_format_message_keeps_prose_starting_with_media(self):
        adapter = make_adapter()
        assert adapter.format_message("MEDIA coverage was great\nsee MEDIA:/tmp/x.png") == "MEDIA coverage was great\nsee"

    @pytest.mark.asyncio
    async def test_send_voice_accepts_hermes_kwargs(self, monkeypatch):
        adapter = _wired(room_type="dm")
        adapter._send_local_file = AsyncMock(return_value=MagicMock(success=True))
        await adapter.send_voice("room1", "/tmp/a.ogg", caption=None, is_voice=True)
        adapter._send_local_file.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_delete_message_uses_chat_delete(self):
        adapter = _wired()
        assert await adapter.delete_message("room1", "m1") is True
        adapter._api_post.assert_awaited_once_with("chat.delete", {"roomId": "room1", "msgId": "m1"})

    @pytest.mark.asyncio
    async def test_typing_uses_display_name_when_server_requires_it(self):
        adapter = make_adapter()
        adapter._api_get = AsyncMock(return_value={"success": True, "settings": [{"_id": "UI_Use_Real_Name", "value": True}]})
        assert await adapter._resolve_typing_name({"username": "hermesbot", "name": "Hermes Bot"}) == "Hermes Bot"
        adapter._api_get = AsyncMock(return_value={"success": True, "settings": [{"_id": "UI_Use_Real_Name", "value": False}]})
        assert await adapter._resolve_typing_name({"username": "hermesbot", "name": "Hermes Bot"}) == "hermesbot"

    @pytest.mark.asyncio
    async def test_typing_frames_carry_thread_in_thread_mode(self):
        adapter = make_adapter()
        adapter._ws = MagicMock(closed=False)
        adapter._ddp_logged_in = True
        adapter._ddp_method = AsyncMock()
        adapter._room_type_cache["room1"] = "channel"
        await adapter.send_typing("room1", metadata={"thread_id": "root1"})
        args = adapter._ddp_method.await_args.args[1]
        assert args == ["room1/user-activity", "hermesbot", ["user-typing"], {"tmid": "root1"}]


class TestAttachmentRedirect:
    @pytest.mark.asyncio
    async def test_redirect_to_signed_url_is_followed_without_pat(self, monkeypatch):
        adapter = _wired(room_type="dm")
        adapter._download_attachments = rc.inbound.InboundMixin._download_attachments.__get__(adapter)
        first = FakeResponse(302, b"", headers={"Location": "https://bucket.s3.amazonaws.com/f1?X-Amz-Signature=abc"})
        adapter._session = MagicMock()
        adapter._session.get = MagicMock(return_value=first)
        seen = {}

        async def fake_follow(location, maximum):
            seen["location"] = location
            return b"\x89PNG", "image/png"

        adapter._download_redirected_file = fake_follow
        monkeypatch.setattr("gateway.platforms.base.cache_image_from_bytes", lambda data, ext: "/cache/img.png")
        urls, types = await adapter._download_attachments(make_post(file={"_id": "f1", "name": "chart.png", "type": "image/png"}))
        assert seen["location"].startswith("https://bucket.s3.amazonaws.com/")
        assert urls == ["/cache/img.png"] and types == ["image/png"]

    @pytest.mark.asyncio
    async def test_off_origin_http_redirect_is_refused(self):
        adapter = make_adapter()
        assert await adapter._download_redirected_file("http://bucket.example.net/f1", 1024) is None

    @pytest.mark.asyncio
    async def test_userinfo_redirect_is_refused(self):
        adapter = make_adapter()
        assert await adapter._download_redirected_file("https://user:pw@bucket.example.net/f1", 1024) is None
