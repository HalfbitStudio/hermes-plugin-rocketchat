"""Agent tools for Rocket.Chat writes, uploads, and bounded message retrieval.

Registered into the ``rocketchat`` toolset (see ``register()`` in
``__init__.py``), which the gateway auto-includes for Rocket.Chat
sessions. Handlers are REST-only one-shots using env credentials, so
they also work outside the gateway process (e.g. in cron job sessions).
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional
from urllib.parse import quote

from tools.registry import tool_error, tool_result


DEFAULT_MAX_AGENT_FILE_BYTES = 100 * 1024 * 1024


def _max_agent_file_bytes() -> int:
    """Return the local safety limit for agent-triggered uploads; 0 disables it."""
    raw = os.getenv(
        "ROCKETCHAT_AGENT_FILE_MAX_BYTES",
        str(DEFAULT_MAX_AGENT_FILE_BYTES),
    )
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return DEFAULT_MAX_AGENT_FILE_BYTES


async def _api(
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """One-shot authenticated ``/api/v1`` call. Errors come back under ``_error``."""
    import aiohttp

    url = os.getenv("ROCKETCHAT_URL", "").rstrip("/")
    headers = {
        "X-Auth-Token": os.getenv("ROCKETCHAT_TOKEN", ""),
        "X-User-Id": os.getenv("ROCKETCHAT_USER_ID", ""),
        "Content-Type": "application/json",
    }
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30)
        ) as session:
            async with session.request(
                method,
                f"{url}/api/v1/{path}",
                headers=headers,
                params=params,
                json=payload,
            ) as resp:
                data = await resp.json(content_type=None) or {}
                if resp.status >= 400 or not data.get("success", True):
                    err = data.get("error") or f"HTTP {resp.status}"
                    return {"_error": str(err)}
                return data
    except Exception as exc:
        return {"_error": str(exc)}


def _bounded_count_arg(
    args: dict,
    name: str,
    *,
    default: int,
    maximum: int,
) -> tuple[Optional[int], Optional[str]]:
    """Parse a positive count/limit argument without silently clamping it."""
    raw = args.get(name, default)
    if isinstance(raw, bool) or not isinstance(raw, int):
        return None, f"{name} must be an integer between 1 and {maximum}"
    if raw < 1 or raw > maximum:
        return None, f"{name} must be between 1 and {maximum}"
    return raw, None


def _offset_arg(args: dict, *, default: int = 0) -> tuple[Optional[int], Optional[str]]:
    """Parse a non-negative pagination offset."""
    raw = args.get("offset", default)
    if isinstance(raw, bool) or not isinstance(raw, int):
        return None, "offset must be a non-negative integer"
    if raw < 0:
        return None, "offset must be a non-negative integer"
    return raw, None


def _boolean_arg(
    args: dict, name: str, *, default: bool = False
) -> tuple[Optional[bool], Optional[str]]:
    """Parse a JSON boolean argument without treating non-empty strings as true."""
    raw = args.get(name, default)
    if not isinstance(raw, bool):
        return None, f"{name} must be a boolean"
    return raw, None


def _compact_file_metadata(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Return the useful, stable subset of Rocket.Chat file metadata."""
    result = {
        "file_id": raw.get("_id") or raw.get("id"),
        "name": raw.get("name") or raw.get("title"),
        "content_type": raw.get("type") or raw.get("contentType"),
        "size": raw.get("size"),
        "url": (
            raw.get("url")
            or raw.get("title_link")
            or raw.get("image_url")
            or raw.get("audio_url")
            or raw.get("video_url")
        ),
    }
    return {key: value for key, value in result.items() if value is not None}


def _normalize_message(message: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a Rocket.Chat message into a compact agent-facing shape."""
    sender = message.get("u") or {}
    normalized: Dict[str, Any] = {
        "message_id": message.get("_id"),
        "room_id": message.get("rid"),
        "thread_id": message.get("tmid"),
        "text": message.get("msg") or "",
        "timestamp": message.get("ts"),
        "updated_at": message.get("_updatedAt"),
        "sender": {
            "user_id": sender.get("_id"),
            "username": sender.get("username"),
            "name": sender.get("name"),
        },
        "type": message.get("t") or "message",
    }

    raw_files = message.get("files") or []
    if isinstance(raw_files, dict):
        raw_files = [raw_files]
    elif not isinstance(raw_files, list):
        raw_files = []
    single_file = message.get("file")
    if isinstance(single_file, dict):
        raw_files = [single_file, *raw_files]

    files = []
    seen_files = set()
    for raw_file in raw_files:
        if not isinstance(raw_file, dict):
            continue
        metadata = _compact_file_metadata(raw_file)
        if not metadata:
            continue
        dedup_key = (
            metadata.get("file_id"),
            metadata.get("name"),
            metadata.get("url"),
        )
        if dedup_key in seen_files:
            continue
        seen_files.add(dedup_key)
        files.append(metadata)
    if files:
        normalized["files"] = files

    reactions = message.get("reactions")
    if isinstance(reactions, dict):
        compact_reactions = []
        for emoji, details in reactions.items():
            if not isinstance(details, dict):
                continue
            usernames = details.get("usernames") or []
            user_ids = details.get("userIds") or []
            if not isinstance(usernames, list):
                usernames = []
            if not isinstance(user_ids, list):
                user_ids = []
            reaction = {
                "emoji": emoji,
                "count": max(len(usernames), len(user_ids)),
            }
            if usernames:
                reaction["usernames"] = usernames
            if user_ids:
                reaction["user_ids"] = user_ids
            compact_reactions.append(reaction)
        if compact_reactions:
            normalized["reactions"] = compact_reactions
    return normalized


def _response_int(value: Any, fallback: int) -> int:
    """Return integer pagination metadata, falling back on malformed responses."""
    if isinstance(value, bool):
        return fallback
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


_ROOM_TYPE_NAMES = {
    "c": "channel",
    "p": "group",
    "d": "dm",
}


async def handle_search_messages(args: dict, **kw) -> str:
    """Search messages in one room."""
    room_id = str(args.get("room_id") or "").strip()
    if not room_id:
        return tool_error("room_id is required")
    query = str(args.get("query") or "").strip()
    if not query:
        return tool_error("query is required")

    count, error = _bounded_count_arg(args, "count", default=25, maximum=100)
    if error:
        return tool_error(error)
    offset, error = _offset_arg(args)
    if error:
        return tool_error(error)

    data = await _api(
        "GET",
        "chat.search",
        params={
            "roomId": room_id,
            "searchText": query,
            "count": count,
            "offset": offset,
        },
    )
    if "_error" in data:
        return tool_error(f"Could not search room {room_id}: {data['_error']}")

    raw_messages = data.get("messages") or []
    messages = [
        _normalize_message(message)
        for message in raw_messages
        if isinstance(message, dict)
    ]
    return tool_result(
        room_id=room_id,
        query=query,
        messages=messages,
        count=_response_int(data.get("count"), len(messages)),
        # Current Rocket.Chat releases omit total/count/offset from chat.search.
        # Preserve pagination metadata from versions that provide it, but do not
        # invent a misleading total for versions that do not.
        total=(
            _response_int(data.get("total"), len(messages))
            if data.get("total") is not None
            else None
        ),
        offset=_response_int(data.get("offset"), offset),
    )


async def handle_get_history(args: dict, **kw) -> str:
    """Return normalized message history for a channel, group, or DM."""
    room_id = str(args.get("room_id") or "").strip()
    if not room_id:
        return tool_error("room_id is required")

    count, error = _bounded_count_arg(args, "count", default=50, maximum=100)
    if error:
        return tool_error(error)
    offset, error = _offset_arg(args)
    if error:
        return tool_error(error)
    inclusive, error = _boolean_arg(args, "inclusive")
    if error:
        return tool_error(error)
    include_threads, error = _boolean_arg(args, "include_threads")
    if error:
        return tool_error(error)

    info = await _api("GET", "rooms.info", params={"roomId": room_id})
    if "_error" in info:
        return tool_error(f"Could not inspect room {room_id}: {info['_error']}")
    room = info.get("room") or {}
    room_type_code = str(room.get("t") or "").strip().lower()
    endpoint = {
        "c": "channels.history",
        "p": "groups.history",
        "d": "im.history",
    }.get(room_type_code)
    if not endpoint:
        return tool_error(
            f"Unsupported Rocket.Chat room type: {room_type_code or 'unknown'}"
        )
    params: Dict[str, Any] = {
        "roomId": room_id,
        "count": count,
        "offset": offset,
        "inclusive": "true" if inclusive else "false",
        # groups.history defaults this to true while channels/im default it to
        # false, so always send the value to keep one cross-room contract.
        "showThreadMessages": "true" if include_threads else "false",
    }
    oldest = str(args.get("oldest") or "").strip()
    latest = str(args.get("latest") or "").strip()
    if oldest:
        params["oldest"] = oldest
    if latest:
        params["latest"] = latest
    data = await _api("GET", endpoint, params=params)
    if "_error" in data:
        return tool_error(f"Could not fetch history for {room_id}: {data['_error']}")

    raw_messages = data.get("messages") or []
    messages = [
        _normalize_message(message)
        for message in raw_messages
        if isinstance(message, dict)
    ]
    return tool_result(
        room_id=room_id,
        room_type=_ROOM_TYPE_NAMES[room_type_code],
        messages=messages,
        count=_response_int(data.get("count"), len(messages)),
        total=(
            _response_int(data.get("total"), len(messages))
            if data.get("total") is not None
            else None
        ),
        offset=_response_int(data.get("offset"), offset),
    )


async def handle_get_thread(args: dict, **kw) -> str:
    """Return a thread parent and paginated replies in chronological order."""
    thread_id = str(args.get("tmid") or "").strip()
    if not thread_id:
        return tool_error("tmid is required")
    limit, error = _bounded_count_arg(args, "limit", default=100, maximum=500)
    if error:
        return tool_error(error)

    parent_data = await _api(
        "GET", "chat.getMessage", params={"msgId": thread_id}
    )
    if "_error" in parent_data:
        return tool_error(f"Could not fetch thread parent {thread_id}: {parent_data['_error']}")
    raw_parent = parent_data.get("message") or {}
    if not isinstance(raw_parent, dict) or not raw_parent.get("_id"):
        return tool_error(f"Thread parent {thread_id} was not found")

    parent = _normalize_message(raw_parent)
    parent_id = raw_parent.get("_id")
    seen_ids = {parent_id}
    replies = []
    page_offset = 0
    total_hint: Optional[int] = None
    last_page_size = 0

    while len(replies) < limit:
        page_size = min(100, limit - len(replies))
        data = await _api(
            "GET",
            "chat.getThreadMessages",
            params={
                "tmid": thread_id,
                "count": page_size,
                "offset": page_offset,
            },
        )
        if "_error" in data:
            return tool_error(f"Could not fetch thread {thread_id}: {data['_error']}")

        raw_page = data.get("messages") or []
        if not isinstance(raw_page, list):
            raw_page = []
        last_page_size = len(raw_page)
        if data.get("total") is not None:
            total_hint = _response_int(data.get("total"), len(replies))

        for raw_message in raw_page:
            if not isinstance(raw_message, dict):
                continue
            message_id = raw_message.get("_id")
            if message_id and message_id in seen_ids:
                continue
            if message_id:
                seen_ids.add(message_id)
            replies.append(_normalize_message(raw_message))
            if len(replies) >= limit:
                break

        if not raw_page:
            break
        page_offset += len(raw_page)
        if total_hint is not None and page_offset >= total_hint:
            break
        if total_hint is None and len(raw_page) < page_size:
            break

    replies.sort(key=lambda message: str(message.get("timestamp") or ""))
    # Keep the root first even if a malformed reply has an earlier/missing ts.
    messages = [parent, *replies]
    total_replies = total_hint if total_hint is not None else len(replies)
    truncated = total_replies > len(replies)
    if total_hint is None and len(replies) >= limit and last_page_size:
        truncated = True

    return tool_result(
        thread_id=thread_id,
        parent=parent,
        messages=messages,
        total_replies=total_replies,
        truncated=truncated,
    )


async def handle_get_permalink(args: dict, **kw) -> str:
    """Build a Rocket.Chat web permalink for one message."""
    message_id = str(args.get("message_id") or "").strip()
    if not message_id:
        return tool_error("message_id is required")

    data = await _api("GET", "chat.getMessage", params={"msgId": message_id})
    if "_error" in data:
        return tool_error(f"Could not fetch message {message_id}: {data['_error']}")
    message = data.get("message") or {}
    room_id = str(message.get("rid") or "").strip()
    if not room_id:
        return tool_error(f"Message {message_id} returned no room id")

    info = await _api("GET", "rooms.info", params={"roomId": room_id})
    if "_error" in info:
        return tool_error(f"Could not inspect room {room_id}: {info['_error']}")
    room = info.get("room") or {}
    room_type_code = str(room.get("t") or "").strip().lower()
    room_type = _ROOM_TYPE_NAMES.get(room_type_code)
    if not room_type:
        return tool_error(
            f"Unsupported Rocket.Chat room type: {room_type_code or 'unknown'}"
        )

    if room_type_code in {"c", "p"}:
        room_name = str(room.get("name") or "").strip()
        if not room_name:
            return tool_error(f"Room {room_id} returned no name")
        route = "channel" if room_type_code == "c" else "group"
        path = f"/{route}/{quote(room_name, safe='')}"
    else:
        path = f"/direct/{quote(room_id, safe='')}"

    base_url = os.getenv("ROCKETCHAT_URL", "").rstrip("/")
    if not base_url:
        return tool_error("ROCKETCHAT_URL is required to build a permalink")
    permalink = f"{base_url}{path}?msg={quote(message_id, safe='')}"
    return tool_result(
        message_id=message_id,
        room_id=room_id,
        room_type=room_type,
        permalink=permalink,
    )


async def handle_list_channels(args: dict, **kw) -> str:
    """List public channels and private groups visible to the bot."""
    rooms: list = []
    errors: list = []
    for path, key, rtype in (
        ("channels.list", "channels", "channel"),
        ("groups.list", "groups", "group"),
    ):
        data = await _api("GET", path, params={"count": 100})
        if "_error" in data:
            errors.append(f"{path}: {data['_error']}")
            continue
        for room in data.get(key) or []:
            rooms.append(
                {
                    "room_id": room.get("_id"),
                    "name": room.get("name"),
                    "type": rtype,
                    "topic": room.get("topic") or "",
                    "members": room.get("usersCount"),
                }
            )
    name_filter = str(args.get("filter") or "").strip().lower()
    if name_filter:
        rooms = [r for r in rooms if name_filter in (r["name"] or "").lower()]
    if not rooms and errors:
        return tool_error("; ".join(errors))
    result: Dict[str, Any] = {"channels": rooms, "count": len(rooms)}
    if errors:
        # channels.list needs the view-c-room permission; groups.list only
        # returns rooms the bot is a member of — partial results are normal.
        result["warnings"] = errors
    return tool_result(result)


async def handle_create_channel(args: dict, **kw) -> str:
    """Create a public channel or private group, optionally inviting members."""
    name = str(args.get("name") or "").strip()
    if not name:
        return tool_error("name is required")
    private = bool(args.get("private"))
    payload: Dict[str, Any] = {"name": name}
    members = args.get("members") or []
    if members:
        payload["members"] = [str(m).strip().lstrip("@") for m in members if str(m).strip()]
    path = "groups.create" if private else "channels.create"
    data = await _api("POST", path, payload=payload)
    if "_error" in data:
        return tool_error(f"Failed to create {'group' if private else 'channel'}: {data['_error']}")
    room = data.get("group" if private else "channel") or {}
    return tool_result(
        room_id=room.get("_id"),
        name=room.get("name"),
        private=private,
        members=payload.get("members", []),
    )


async def handle_post(args: dict, **kw) -> str:
    """Post a message to a channel/group by name or room id."""
    message = str(args.get("message") or "").strip()
    if not message:
        return tool_error("message is required")
    room_id = str(args.get("room_id") or "").strip()
    channel = str(args.get("channel") or "").strip().lstrip("#")
    if not room_id and not channel:
        return tool_error("channel (name) or room_id is required")

    payload: Dict[str, Any] = {"text": message}
    if room_id:
        payload["roomId"] = room_id
        target = room_id
    else:
        payload["channel"] = f"#{channel}"
        target = f"#{channel}"
    data = await _api("POST", "chat.postMessage", payload=payload)
    if "_error" in data:
        return tool_error(
            f"Failed to post to {target}: {data['_error']} "
            "(is the bot a member of the room?)"
        )
    msg = data.get("message") or {}
    return tool_result(
        sent=True,
        target=target,
        room_id=msg.get("rid") or room_id,
        message_id=msg.get("_id"),
    )


async def _upload_media(
    room_id: str,
    file_data: bytes,
    filename: str,
    content_type: str,
) -> Dict[str, Any]:
    """Upload bytes with ``rooms.media`` and return its JSON response."""
    import aiohttp

    url = os.getenv("ROCKETCHAT_URL", "").rstrip("/")
    headers = {
        "X-Auth-Token": os.getenv("ROCKETCHAT_TOKEN", ""),
        "X-User-Id": os.getenv("ROCKETCHAT_USER_ID", ""),
    }
    form = aiohttp.FormData()
    form.add_field(
        "file",
        file_data,
        filename=filename,
        content_type=content_type,
    )
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=120)
        ) as session:
            async with session.post(
                f"{url}/api/v1/rooms.media/{room_id}",
                headers=headers,
                data=form,
            ) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    return {"_error": f"HTTP {resp.status}: {body[:200]}"}
                data = await resp.json(content_type=None)
                if not isinstance(data, dict):
                    return {"_error": "response was not a JSON object"}
                if not data.get("success", True):
                    return {"_error": str(data.get("error") or "upload rejected")}
                return data
    except Exception as exc:
        return {"_error": str(exc)}


async def handle_send_file(args: dict, **kw) -> str:
    """Upload a local file to a Rocket.Chat channel, group, or DM via rooms.media (two-step).

    Exactly one target is required:
      1. room_id  — exact room ID (preferred; from rocketchat_dm or rocketchat_list_channels)
      2. username — a REAL Rocket.Chat login (not a display name); resolved via im.create
      3. channel  — channel/group name (resolved via rooms.info)
    Never construct or guess a room_id from a name. Pass a literal ID you already hold.
    """
    import asyncio
    import mimetypes
    from pathlib import Path

    file_path = str(args.get("file_path") or "").strip()
    if not file_path:
        return tool_error("file_path is required")

    p = Path(file_path)
    if not p.is_file():
        return tool_error(f"File not found or not a regular file: {file_path}")
    try:
        file_size = p.stat().st_size
    except OSError as exc:
        return tool_error(f"Could not inspect file {file_path}: {exc}")
    max_bytes = _max_agent_file_bytes()
    if max_bytes and file_size > max_bytes:
        return tool_error(
            f"File is too large ({file_size} bytes; local limit is {max_bytes}). "
            "Adjust ROCKETCHAT_AGENT_FILE_MAX_BYTES if the server accepts larger uploads."
        )

    room_id = str(args.get("room_id") or "").strip()
    username = str(args.get("username") or "").strip().lstrip("@")
    channel = str(args.get("channel") or "").strip().lstrip("#")

    targets = [value for value in (room_id, username, channel) if value]
    if len(targets) != 1:
        return tool_error(
            "Exactly one of room_id, username, or channel is required. "
            "Use a literal room_id (from rocketchat_dm) or a real username — do not guess."
        )

    requested_name = str(args.get("file_name") or "").strip()
    fname = Path(requested_name).name if requested_name else p.name
    if not fname:
        return tool_error("file_name must contain a filename")
    ct = mimetypes.guess_type(fname)[0] or "application/octet-stream"
    try:
        file_data = await asyncio.to_thread(p.read_bytes)
    except OSError as exc:
        return tool_error(f"Could not read file {file_path}: {exc}")
    caption = str(args.get("caption") or "").strip() or None
    tmid = str(args.get("tmid") or "").strip() or None

    # Resolve room_id from a real username via im.create (idempotent: reuses existing DM)
    if not room_id and username:
        data = await _api("POST", "im.create", payload={"username": username})
        if "_error" in data:
            return tool_error(f"Could not open DM with @{username}: {data['_error']}")
        room = data.get("room") or {}
        room_id = room.get("_id") or ""
        # Ghost-room guard: a valid DM must contain the target user + the bot (>= 2 members)
        members = room.get("usernames") or []
        member_names = {str(member).casefold() for member in members}
        if len(members) < 2 or username.casefold() not in member_names:
            return tool_error(
                f"DM room for @{username} has no real recipient (members: {members}). "
                f"The username is incorrect or the user does not exist — file not sent."
            )
        if not room_id:
            return tool_error(f"im.create returned no room id for @{username}")

    # Resolve room_id from channel name if needed
    if not room_id:
        data = await _api("GET", "rooms.info", params={"roomName": channel})
        if "_error" in data:
            return tool_error(f"Could not find room #{channel}: {data['_error']}")
        room_id = (data.get("room") or {}).get("_id")
        if not room_id:
            return tool_error(f"Room #{channel} returned no room id")

    # Step 1: upload bytes
    step1 = await _upload_media(room_id, file_data, fname, ct)
    if "_error" in step1:
        return tool_error(f"Upload step 1 failed: {step1['_error']}")

    file_id = (step1.get("file") or {}).get("_id")
    if not file_id:
        return tool_error(f"Upload step 1 returned no file id: {step1}")

    # Step 2: confirm + create message
    step2_payload: Dict[str, Any] = {}
    if caption:
        step2_payload["msg"] = caption
    if tmid:
        step2_payload["tmid"] = tmid

    step2_data = await _api(
        "POST",
        f"rooms.mediaConfirm/{room_id}/{file_id}",
        payload=step2_payload,
    )
    if "_error" in step2_data:
        return tool_error(f"Upload step 2 failed: {step2_data['_error']}")

    msg = step2_data.get("message") or {}
    message_id = msg.get("_id")
    if not message_id:
        return tool_error("Upload step 2 returned no message id")
    target = f"@{username}" if username else (f"#{channel}" if channel else room_id)
    return tool_result(
        sent=True,
        target=target,
        room_id=room_id,
        message_id=message_id,
        file=fname,
        size=file_size,
    )


async def handle_dm(args: dict, **kw) -> str:
    """Open (or reuse) a DM room with a user; optionally send a message."""
    username = str(args.get("username") or "").strip().lstrip("@")
    if not username:
        return tool_error("username is required")
    data = await _api("POST", "im.create", payload={"username": username})
    if "_error" in data:
        return tool_error(f"Could not open DM with @{username}: {data['_error']}")
    room_id = (data.get("room") or {}).get("_id")
    if not room_id:
        return tool_error(f"im.create returned no room id for @{username}")

    message = str(args.get("message") or "").strip()
    if not message:
        return tool_result(
            room_id=room_id,
            username=username,
            sent=False,
            hint=(
                f"DM room is open. Send now by calling this tool with a message, "
                f"or schedule delivery with cronjob deliver='rocketchat:{room_id}'."
            ),
        )
    sent = await _api(
        "POST", "chat.postMessage", payload={"roomId": room_id, "text": message}
    )
    if "_error" in sent:
        return tool_error(f"DM room open but send failed: {sent['_error']}")
    return tool_result(
        room_id=room_id,
        username=username,
        sent=True,
        message_id=(sent.get("message") or {}).get("_id"),
    )


LIST_CHANNELS_SCHEMA = {
    "name": "rocketchat_list_channels",
    "description": (
        "List channels and private groups on the Rocket.Chat server with their "
        "room_id, name, topic, and member count. Use the room_id as a target for "
        "send_message or cronjob delivery (deliver='rocketchat:<room_id>'). "
        "Public channels require the bot to have the view-c-room permission; "
        "private groups are listed only if the bot is a member."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "filter": {
                "type": "string",
                "description": "Optional case-insensitive substring to filter channel names",
            },
        },
        "required": [],
    },
}

SEARCH_MESSAGES_SCHEMA = {
    "name": "rocketchat_search_messages",
    "description": (
        "Search message text inside one Rocket.Chat room. Returns compact, "
        "normalized messages with sender and thread metadata."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "room_id": {
                "type": "string",
                "description": "Exact Rocket.Chat room ID to search",
            },
            "query": {
                "type": "string",
                "description": "Text to search for in the room",
            },
            "count": {
                "type": "integer",
                "minimum": 1,
                "maximum": 100,
                "default": 25,
                "description": "Maximum results to return (default 25, max 100)",
            },
            "offset": {
                "type": "integer",
                "minimum": 0,
                "default": 0,
                "description": "Pagination offset (default 0)",
            },
        },
        "required": ["room_id", "query"],
    },
}

GET_HISTORY_SCHEMA = {
    "name": "rocketchat_get_history",
    "description": (
        "Read recent message history from a Rocket.Chat channel, private group, "
        "or DM. The room type is detected automatically."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "room_id": {
                "type": "string",
                "description": "Exact Rocket.Chat room ID",
            },
            "count": {
                "type": "integer",
                "minimum": 1,
                "maximum": 100,
                "default": 50,
                "description": "Maximum messages to return (default 50, max 100)",
            },
            "offset": {
                "type": "integer",
                "minimum": 0,
                "default": 0,
                "description": "Pagination offset (default 0)",
            },
            "oldest": {
                "type": "string",
                "description": "Optional oldest ISO timestamp accepted by Rocket.Chat",
            },
            "latest": {
                "type": "string",
                "description": "Optional latest ISO timestamp accepted by Rocket.Chat",
            },
            "inclusive": {
                "type": "boolean",
                "default": False,
                "description": "Include messages exactly at oldest/latest boundaries",
            },
            "include_threads": {
                "type": "boolean",
                "default": False,
                "description": "Include thread replies in channel, group, or DM history",
            },
        },
        "required": ["room_id"],
    },
}

GET_THREAD_SCHEMA = {
    "name": "rocketchat_get_thread",
    "description": (
        "Read a bounded Rocket.Chat thread by root message ID. Returns the parent "
        "plus deduplicated replies in chronological order and reports when the "
        "result was truncated."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "tmid": {
                "type": "string",
                "description": "Thread root message ID",
            },
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": 500,
                "default": 100,
                "description": "Maximum replies to return (default 100, max 500)",
            },
        },
        "required": ["tmid"],
    },
}

GET_PERMALINK_SCHEMA = {
    "name": "rocketchat_get_permalink",
    "description": (
        "Build a Rocket.Chat web permalink for a message after resolving its room "
        "type and canonical room name."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "message_id": {
                "type": "string",
                "description": "Rocket.Chat message ID",
            },
        },
        "required": ["message_id"],
    },
}

CREATE_CHANNEL_SCHEMA = {
    "name": "rocketchat_create_channel",
    "description": (
        "Create a new Rocket.Chat channel (public) or private group, optionally "
        "inviting members by username. Requires the bot to have the "
        "create-c / create-p permission — expect an error otherwise."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
                "description": "Channel name (no spaces; use-dashes-or-underscores)",
            },
            "private": {
                "type": "boolean",
                "description": "Create a private group instead of a public channel (default false)",
            },
            "members": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Usernames to invite (with or without leading @)",
            },
        },
        "required": ["name"],
    },
}

POST_SCHEMA = {
    "name": "rocketchat_post",
    "description": (
        "Post a message to a Rocket.Chat channel or private group — use this "
        "to deliver results to a different room than the current conversation "
        "(e.g. 'research this thread and post the summary to #reports'). "
        "Target by channel name (leading # optional) or by room_id (from "
        "rocketchat_list_channels). The bot must be a member of the room. "
        "For scheduled posts use cronjob deliver='rocketchat:<room_id>' instead."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "channel": {
                "type": "string",
                "description": "Channel/group name, e.g. '#reports' or 'reports'",
            },
            "room_id": {
                "type": "string",
                "description": "Exact room id (takes precedence over channel)",
            },
            "message": {
                "type": "string",
                "description": "Message text to post (Rocket.Chat renders Markdown)",
            },
        },
        "required": ["message"],
    },
}

SEND_FILE_SCHEMA = {
    "name": "rocketchat_send_file",
    "description": (
        "Upload a local file to a Rocket.Chat channel, group, or DM. "
        "Uses the two-step rooms.media flow and follows the server/proxy upload limits. "
        "Returns the message_id of the created file message. "
        "TARGET (pick EXACTLY ONE): "
        "1) room_id — the exact room ID you already hold (from rocketchat_dm or "
        "rocketchat_list_channels). PREFERRED. "
        "2) username — a REAL Rocket.Chat login (e.g. 'younesamalou'), NOT a display name. "
        "3) channel — a channel/group name. "
        "NEVER guess or construct a room_id from a name; pass a literal ID you received."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "file_path": {
                "type": "string",
                "description": "Absolute path to the local file to upload",
            },
            "room_id": {
                "type": "string",
                "description": (
                    "Exact room ID (takes precedence over username/channel). "
                    "Use the literal ID returned by rocketchat_dm or rocketchat_list_channels — "
                    "do not invent or derive it from a username."
                ),
            },
            "username": {
                "type": "string",
                "description": (
                    "Target user's REAL Rocket.Chat login (no leading @), e.g. 'younesamalou'. "
                    "Must be an actual username, not a display name. Resolved via im.create; "
                    "the send is rejected if the user does not exist."
                ),
            },
            "channel": {
                "type": "string",
                "description": "Channel/group name, e.g. '#reports' or 'reports'",
            },
            "caption": {
                "type": "string",
                "description": "Optional message text to attach to the file",
            },
            "file_name": {
                "type": "string",
                "description": "Override the displayed filename (default: basename of file_path)",
            },
            "tmid": {
                "type": "string",
                "description": "Optional thread root message id — file will be posted inside that thread",
            },
        },
        "required": ["file_path"],
    },
}

DM_SCHEMA = {
    "name": "rocketchat_dm",
    "description": (
        "Open a direct-message room with a Rocket.Chat user by username and "
        "optionally send them a message right away. Always returns the DM "
        "room_id — for scheduled/future delivery (e.g. 'remind @user about X "
        "tomorrow') call this without a message to get the room_id, then "
        "create a cronjob with deliver='rocketchat:<room_id>'."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "username": {
                "type": "string",
                "description": "Rocket.Chat username (with or without leading @)",
            },
            "message": {
                "type": "string",
                "description": "Message to send immediately (omit to just open the room and get its id)",
            },
        },
        "required": ["username"],
    },
}

TOOLS = (
    ("rocketchat_list_channels", LIST_CHANNELS_SCHEMA, handle_list_channels, "📋"),
    ("rocketchat_search_messages", SEARCH_MESSAGES_SCHEMA, handle_search_messages, "🔎"),
    ("rocketchat_get_history", GET_HISTORY_SCHEMA, handle_get_history, "📜"),
    ("rocketchat_get_thread", GET_THREAD_SCHEMA, handle_get_thread, "🧵"),
    ("rocketchat_get_permalink", GET_PERMALINK_SCHEMA, handle_get_permalink, "🔗"),
    ("rocketchat_create_channel", CREATE_CHANNEL_SCHEMA, handle_create_channel, "➕"),
    ("rocketchat_post", POST_SCHEMA, handle_post, "📣"),
    ("rocketchat_send_file", SEND_FILE_SCHEMA, handle_send_file, "📎"),
    ("rocketchat_dm", DM_SCHEMA, handle_dm, "✉️"),
)
