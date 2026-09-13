"""DDP transport: login result handling, stream readiness, off-loop dispatch."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from harness import load_plugin, make_adapter, make_post, submodule

load_plugin()
ddp = submodule("ddp")


def _frame_ws(frames):
    """A fake aiohttp WebSocket yielding JSON text frames then closing."""
    import aiohttp

    ws = MagicMock()
    ws._response.url = "wss://rc.example.com/websocket"
    ws.closed = False
    messages = [MagicMock(type=aiohttp.WSMsgType.TEXT, data=__import__("json").dumps(f)) for f in frames]
    ws.__aiter__.return_value = messages
    ws.send_json = AsyncMock()

    async def close():
        ws.closed = True

    ws.close = AsyncMock(side_effect=close)
    return ws


@pytest.mark.asyncio
async def test_login_error_raises_auth_error_and_stops_reconnect():
    adapter = make_adapter()
    ws = _frame_ws([
        {"msg": "connected", "session": "s1"},
        {"msg": "result", "id": "1", "error": {"error": 403, "reason": "You've been logged out by the server. Please log in again.", "errorType": "Meteor.Error"}},
    ])
    adapter._session = MagicMock()
    adapter._session.ws_connect = AsyncMock(return_value=ws)
    with pytest.raises(ddp.DdpAuthError):
        await adapter._ws_connect_and_listen()
    assert not any(call.args[0].get("msg") == "sub" for call in ws.send_json.await_args_list)

    adapter._set_fatal_error = MagicMock()
    adapter._notify_fatal_error = AsyncMock()
    adapter._ddp_next_id = 1  # the recorded frames answer login id "1"
    await adapter._ws_loop()
    adapter._set_fatal_error.assert_called_once()
    assert adapter._set_fatal_error.call_args.kwargs["retryable"] is False
    adapter._notify_fatal_error.assert_awaited_once()


@pytest.mark.asyncio
async def test_subscription_is_sent_only_after_login_succeeds():
    adapter = make_adapter()
    ws = _frame_ws([
        {"msg": "connected", "session": "s1"},
        {"msg": "result", "id": "1", "result": {"id": "bot_uid", "token": "pat", "tokenExpires": {"$date": 1}}},
    ])
    adapter._session = MagicMock()
    adapter._session.ws_connect = AsyncMock(return_value=ws)
    await adapter._ws_connect_and_listen()
    sent = [call.args[0] for call in ws.send_json.await_args_list]
    kinds = [frame["msg"] for frame in sent]
    assert kinds[:2] == ["connect", "method"]
    assert kinds[-1] == "sub"
    assert sent[-1]["name"] == "stream-room-messages"
    assert sent[-1]["params"] == ["__my_messages__", False]
    assert adapter._ddp_logged_in is True


@pytest.mark.asyncio
async def test_ready_marks_stream_and_resets_backoff_state():
    adapter = make_adapter()
    adapter._ws = MagicMock(closed=False)
    adapter._ddp_send = AsyncMock()
    adapter._ddp_login_id = "1"
    await adapter._handle_ddp_frame({"msg": "result", "id": "1", "result": {}})
    sub_id = adapter._ddp_stream_sub_id
    assert sub_id
    await adapter._handle_ddp_frame({"msg": "ready", "subs": [sub_id]})
    assert adapter._ddp_stream_ready is True


@pytest.mark.asyncio
async def test_failed_frame_is_a_protocol_error():
    adapter = make_adapter()
    ws = _frame_ws([{"msg": "failed", "version": "pre2"}])
    adapter._session = MagicMock()
    adapter._session.ws_connect = AsyncMock(return_value=ws)
    with pytest.raises(ddp.DdpProtocolError):
        await adapter._ws_connect_and_listen()


@pytest.mark.asyncio
async def test_nosub_for_stream_closes_socket_for_reconnect():
    adapter = make_adapter()
    adapter._ws = MagicMock(closed=False)
    adapter._ws.close = AsyncMock()
    adapter._ddp_send = AsyncMock()
    adapter._ddp_stream_sub_id = "sub-1"
    adapter._ddp_stream_ready = True
    await adapter._handle_ddp_frame({"msg": "nosub", "id": "sub-1", "error": {"error": 403, "errorType": "Meteor.Error"}})
    adapter._ws.close.assert_awaited_once()
    assert adapter._ddp_stream_ready is False


@pytest.mark.asyncio
async def test_changed_frame_dispatches_off_loop_with_room_meta():
    adapter = make_adapter()
    adapter._handle_message = AsyncMock()
    post = make_post()
    meta = {"roomParticipant": True, "roomType": "d", "roomName": None}
    await adapter._handle_ddp_frame({
        "msg": "changed",
        "collection": "stream-room-messages",
        "id": "id",
        "fields": {"eventName": "__my_messages__", "args": [post, meta]},
    })
    # Dispatched as a task: not awaited inline.
    adapter._handle_message.assert_not_awaited()
    await asyncio.gather(*adapter._inbound_tasks)
    adapter._handle_message.assert_awaited_once_with(post, meta)


@pytest.mark.asyncio
async def test_malformed_changed_frames_are_ignored():
    adapter = make_adapter()
    adapter._handle_message = AsyncMock()
    for frame in (
        {"msg": "changed", "collection": "stream-room-messages", "fields": None},
        {"msg": "changed", "collection": "stream-room-messages", "fields": {"args": "nope"}},
        {"msg": "changed", "collection": "stream-room-messages", "fields": {"args": []}},
        {"msg": "changed", "collection": "stream-room-messages", "fields": {"args": ["text"]}},
        {"msg": "ready", "subs": "not-a-list"},
    ):
        await adapter._handle_ddp_frame(frame)
    assert not adapter._inbound_tasks


@pytest.mark.asyncio
async def test_same_room_frames_are_serialized():
    adapter = make_adapter()
    order = []

    async def slow(post, meta):
        order.append(("start", post["_id"]))
        await asyncio.sleep(0.01)
        order.append(("end", post["_id"]))

    adapter._handle_message = slow
    adapter._spawn_inbound(make_post(_id="a"), None)
    adapter._spawn_inbound(make_post(_id="b"), None)
    await asyncio.gather(*adapter._inbound_tasks)
    assert order == [("start", "a"), ("end", "a"), ("start", "b"), ("end", "b")]


@pytest.mark.asyncio
async def test_disconnect_cancels_inbound_tasks():
    adapter = make_adapter()
    started = asyncio.Event()

    async def hang(post, meta):
        started.set()
        await asyncio.sleep(60)

    adapter._handle_message = hang
    adapter._spawn_inbound(make_post(), None)
    await started.wait()
    await adapter.disconnect()
    assert not adapter._inbound_tasks
