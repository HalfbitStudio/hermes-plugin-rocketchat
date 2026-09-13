"""Republished message documents must never become new agent turns."""

from unittest.mock import AsyncMock

import pytest

from harness import load_plugin, make_adapter, make_post, submodule

load_plugin()
helpers = submodule("helpers")

# Values captured in production (upstream PR #5): a thread root posted at 10:45
# republished at 12:21 because a reply bumped its counter.
REPUBLISHED_ROOT = {
    "_id": "S7Lza",
    "rid": "room1",
    "msg": "opening question",
    "ts": {"$date": 1788432317864},
    "_updatedAt": {"$date": 1788438842119},
    "tcount": 56,
    "tlm": {"$date": 1788438842000},
    "u": {"_id": "u1", "username": "alice"},
}


class TestTimestampParsing:
    def test_ejson_date(self):
        assert helpers.parse_rocketchat_timestamp({"$date": 1788432317864}) == pytest.approx(1788432317.864)

    def test_iso_strings(self):
        zulu = helpers.parse_rocketchat_timestamp("2026-09-03T10:45:17.864Z")
        offset = helpers.parse_rocketchat_timestamp("2026-09-03T12:45:17.864+02:00")
        assert zulu == pytest.approx(offset)

    @pytest.mark.parametrize("value", [None, True, "", "not a date", [], {"$date": None}])
    def test_garbage_is_none(self, value):
        assert helpers.parse_rocketchat_timestamp(value) is None


class TestIsMutationRepublish:
    def test_fresh_post_passes(self):
        assert helpers.is_mutation_republish(make_post()) is False

    def test_fresh_post_with_url_preview_update_passes(self):
        post = make_post(_updatedAt={"$date": 1788432317864 + 5_000}, urls=[{"url": "https://x", "meta": {}}])
        assert helpers.is_mutation_republish(post) is False

    def test_production_thread_root_republish(self):
        assert helpers.is_mutation_republish(REPUBLISHED_ROOT) is True

    @pytest.mark.parametrize(
        "field,value",
        [
            ("editedAt", {"$date": 1788432400000}),
            ("tcount", 1),
            ("tlm", {"$date": 1788432400000}),
            ("replies", ["u2"]),
            ("reactions", {":eyes:": {"usernames": ["hermesbot"]}}),
            ("pinned", True),
            ("pinnedAt", {"$date": 1788432400000}),
            ("starred", [{"_id": "u1"}]),
        ],
    )
    def test_structural_markers(self, field, value):
        post = make_post(**{field: value})
        assert helpers.is_mutation_republish(post) is True

    @pytest.mark.parametrize("field,value", [("reactions", {}), ("replies", []), ("pinned", False), ("starred", [])])
    def test_empty_markers_are_not_mutations(self, field, value):
        assert helpers.is_mutation_republish(make_post(**{field: value})) is False

    def test_timestamp_fallback(self):
        post = make_post(_updatedAt={"$date": 1788432317864 + 61_000})
        assert helpers.is_mutation_republish(post) is True

    def test_missing_timestamps_fail_open(self):
        post = make_post()
        del post["_updatedAt"]
        assert helpers.is_mutation_republish(post) is False

    def test_rest_iso_shapes(self):
        post = make_post(ts="2026-09-03T10:45:17.864Z", _updatedAt="2026-09-03T12:21:00.000Z")
        assert helpers.is_mutation_republish(post) is True


class TestInboundGuard:
    @pytest.mark.asyncio
    async def test_republished_frame_is_dropped_before_dispatch(self):
        adapter = make_adapter()
        adapter._inbound_authorization_checker = lambda source: True
        adapter.handle_message = AsyncMock()
        adapter._room_type_cache["room1"] = "dm"
        await adapter._handle_message(dict(REPUBLISHED_ROOT))
        adapter.handle_message.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fresh_frame_still_dispatches(self):
        adapter = make_adapter()
        adapter._inbound_authorization_checker = lambda source: True
        adapter.handle_message = AsyncMock()
        adapter._download_attachments = AsyncMock(return_value=([], []))
        adapter._room_type_cache["room1"] = "dm"
        await adapter._handle_message(make_post())
        adapter.handle_message.assert_awaited_once()

    def test_dedup_window_outlasts_a_conversation(self):
        adapter = make_adapter()
        assert adapter._dedup._ttl == helpers.INBOUND_DEDUP_TTL_SECONDS == 6 * 3600
        assert adapter._dedup._max_size == helpers.INBOUND_DEDUP_MAX_ENTRIES
