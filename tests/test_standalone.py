"""Out-of-process cron delivery: text chunking and media upload."""

from unittest.mock import MagicMock

import pytest

from harness import FakeResponse, FakeSession, load_plugin, submodule

load_plugin()
helpers = submodule("helpers")


def _pconfig():
    cfg = MagicMock()
    cfg.token = ""
    cfg.extra = {"url": "https://rc.example.com", "token": "pat", "user_id": "bot_uid"}
    return cfg


@pytest.mark.asyncio
async def test_text_then_media_are_delivered(monkeypatch, tmp_path):
    report = tmp_path / "report.pdf"
    report.write_bytes(b"%PDF-1.4 fake")
    session = FakeSession({
        "chat.postMessage": FakeResponse(200, {"success": True, "message": {"_id": "m1", "rid": "GENERAL"}}),
        "rooms.mediaConfirm": FakeResponse(200, {"success": True, "message": {"_id": "m2", "rid": "GENERAL"}}),
        "rooms.media/": FakeResponse(200, {"success": True, "file": {"_id": "f1"}}),
    })
    import aiohttp

    monkeypatch.setattr(aiohttp, "ClientSession", lambda *a, **k: session)
    result = await helpers._standalone_send(_pconfig(), "GENERAL", "Sprint report", media_files=[str(report)])
    assert result == {"success": True, "message_id": "m2", "delivered_files": 1}
    urls = [call["url"] for call in session.calls]
    assert urls == [
        "https://rc.example.com/api/v1/chat.postMessage",
        "https://rc.example.com/api/v1/rooms.media/GENERAL",
        "https://rc.example.com/api/v1/rooms.mediaConfirm/GENERAL/f1",
    ]
    assert "Content-Type" not in session.calls[1]["headers"]


@pytest.mark.asyncio
async def test_media_only_delivery_needs_no_text(monkeypatch, tmp_path):
    image = tmp_path / "chart.png"
    image.write_bytes(b"\x89PNG")
    session = FakeSession({
        "rooms.mediaConfirm": FakeResponse(200, {"success": True, "message": {"_id": "m2", "rid": "GENERAL"}}),
        "rooms.media/": FakeResponse(200, {"success": True, "file": {"_id": "f1"}}),
    })
    import aiohttp

    monkeypatch.setattr(aiohttp, "ClientSession", lambda *a, **k: session)
    result = await helpers._standalone_send(_pconfig(), "GENERAL", "", media_files=[str(image)])
    assert result["success"] is True
    assert not any("chat.postMessage" in call["url"] for call in session.calls)


@pytest.mark.asyncio
async def test_missing_file_reports_partial_delivery(monkeypatch, tmp_path):
    session = FakeSession({
        "chat.postMessage": FakeResponse(200, {"success": True, "message": {"_id": "m1", "rid": "GENERAL"}}),
    })
    import aiohttp

    monkeypatch.setattr(aiohttp, "ClientSession", lambda *a, **k: session)
    result = await helpers._standalone_send(
        _pconfig(), "GENERAL", "hi", media_files=[str(tmp_path / "missing.bin")]
    )
    assert result["error"] and result["message_id"] == "m1" and result["delivered_files"] == 0


@pytest.mark.asyncio
async def test_long_text_is_chunked_in_utf16_units(monkeypatch):
    responses = [FakeResponse(200, {"success": True, "message": {"_id": f"m{i}", "rid": "GENERAL"}}) for i in range(3)]
    session = FakeSession({"chat.postMessage": responses})
    import aiohttp

    monkeypatch.setattr(aiohttp, "ClientSession", lambda *a, **k: session)
    text = "😀" * 3000  # 3000 code points, 6000 UTF-16 units
    result = await helpers._standalone_send(_pconfig(), "GENERAL", text)
    assert result["success"] is True
    assert len(session.calls) == 2
    from gateway.platforms.base import utf16_len

    assert all(utf16_len(call["json"]["text"]) <= helpers.MAX_MESSAGE_LENGTH for call in session.calls)


@pytest.mark.asyncio
async def test_nothing_to_deliver_is_an_error():
    result = await helpers._standalone_send(_pconfig(), "GENERAL", "   ")
    assert "error" in result
