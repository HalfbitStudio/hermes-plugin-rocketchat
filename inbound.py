"""Inbound pipeline: DDP posts → Hermes MessageEvents, attachment
download, voice→MP3 conversion, and emoji reaction hooks."""

from __future__ import annotations

import asyncio
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List

from gateway.platforms.base import MessageEvent, MessageType, ProcessingOutcome

from .helpers import _ROOM_TYPE_MAP

logger = logging.getLogger(__name__)


class InboundMixin:
    """Inbound handling of :class:`~.adapter.RocketchatAdapter`."""

    async def _handle_message(self, post: Dict[str, Any]) -> None:
        """Process an incoming Rocket.Chat message."""
        sender = post.get("u") or {}
        sender_id = sender.get("_id", "")
        sender_name = sender.get("username", "") or sender_id

        # Ignore own messages.
        if sender_id == self._bot_user_id:
            return

        post_id = post.get("_id", "")
        if self._dedup.is_duplicate(post_id):
            return

        room_id = post.get("rid", "")
        if not room_id:
            return

        # Look up room type lazily; cache forever.
        chat_type = self._room_type_cache.get(room_id)
        if chat_type is None:
            chat_type = await self._resolve_room_type(room_id)
            self._room_type_cache[room_id] = chat_type

        # Handle system messages: skip all except topic changes in DMs.
        t_type = post.get("t")
        if t_type:
            if t_type == "room_changed_topic" and chat_type == "dm":
                topic_text = (post.get("msg") or "").strip()
                if topic_text:
                    # Update topic cache immediately (avoids extra API call
                    # in _sync_title_to_rc_topic on the next send())
                    self._last_topic[room_id] = topic_text

                    source = self.build_source(
                        chat_id=room_id,
                        chat_type=chat_type,
                        user_id=sender_id,
                        user_name=sender_name,
                        thread_id=None,
                    )
                    from gateway.platforms.base import resolve_channel_prompt
                    channel_prompt = resolve_channel_prompt(
                        self.config.extra, room_id, None,
                    )
                    cmd_msg = MessageEvent(
                        text=f"/title {topic_text}",
                        message_type=MessageType.COMMAND,
                        source=source,
                        raw_message=post,
                        message_id=post_id,
                        channel_prompt=channel_prompt,
                    )
                    await self.handle_message(cmd_msg)
            return  # All other system messages: skip

        message_text = post.get("msg", "") or ""

        # Mention gating for non-DM rooms.
        if chat_type != "dm":
            require_mention = os.getenv(
                "ROCKETCHAT_REQUIRE_MENTION", "true"
            ).lower() not in ("false", "0", "no")

            free_channels_raw = os.getenv("ROCKETCHAT_FREE_RESPONSE_CHANNELS", "")
            free_channels = {ch.strip() for ch in free_channels_raw.split(",") if ch.strip()}
            is_free_channel = room_id in free_channels

            mentions = post.get("mentions") or []
            mention_ids = {m.get("_id") for m in mentions if isinstance(m, dict)}
            mention_names = {m.get("username") for m in mentions if isinstance(m, dict)}
            has_mention = (
                self._bot_user_id in mention_ids
                or self._bot_username in mention_names
                or "all" in mention_ids or "here" in mention_ids
            )
            if not has_mention and self._bot_username:
                pattern = re.compile(
                    rf"(?:^|\W)@{re.escape(self._bot_username)}(?:\W|$)",
                    re.IGNORECASE,
                )
                has_mention = bool(pattern.search(message_text))

            if require_mention and not is_free_channel and not has_mention:
                return

            if has_mention and self._bot_username:
                message_text = re.sub(
                    rf"(^|\W)@{re.escape(self._bot_username)}(\W|$)",
                    r"\1\2",
                    message_text,
                    flags=re.IGNORECASE,
                ).strip()

        

        thread_id = post.get("tmid") or None

        # Route RC-native slash commands back to Rocket.Chat.
        # Check both the raw post text AND the stripped message_text.
        # In DMs the @mention is never stripped, so raw_msg will contain
        # e.g. "@lobster.bot /dashboard". The dual-text loop handles that:
        # raw_msg has the mention prefix, message_text has it stripped — one
        # of them will have "/" at position 0 for a real slash command.
        #
        # IMPORTANT: we ONLY match "/" at position 0, NOT mid-sentence.
        # A message like "ich find /status doof" is NOT a slash command —
        # it's just text that happens to contain "/status".
        #
        # For known Hermes gateway commands (like /new, /approve, /dashboard,
        # /workspace, etc.) we skip the RC commands.run call entirely —
        # RC doesn't know them and would return 400.  This avoids spurious
        # "command does not exist" error logs and the unnecessary API round-trip.
        # Unknown/RC-native commands still get routed to RC first.
        raw_msg = post.get("msg", "") or ""
        _found_slash_cmd = False
        cmd_full = ""
        for candidate_text in (raw_msg, message_text):
            slash_pos = candidate_text.find("/")
            if slash_pos == 0:
                cmd_raw = candidate_text[slash_pos:]
                cmd_token = cmd_raw.split(None, 1)[0]
                cmd_params = cmd_raw[len(cmd_token):].strip()
                
                _found_slash_cmd = True
                cmd_full = cmd_raw
                
                # Skip RC routing for known Hermes gateway commands.
                _is_hermes_cmd = False
                try:
                    from hermes_cli.commands import is_gateway_known_command
                    # Strip leading "/" before checking — is_gateway_known_command
                    # expects the bare name (e.g. "new", not "/new").
                    bare_cmd = cmd_token.lstrip("/").lower()
                    _is_hermes_cmd = is_gateway_known_command(bare_cmd)
                except Exception:
                    pass  # defensive: if import fails, fall through to RC route
                
                if not _is_hermes_cmd:
                    rc_payload: Dict[str, Any] = {
                        "command": cmd_token,
                        "roomId": room_id,
                        "params": cmd_params,
                    }
                    if thread_id:
                        rc_payload["tmid"] = thread_id
                    data = await self._api_post("commands.run", rc_payload)
                    if data and data.get("success"):
                        logger.info(
                            "Rocket.Chat: routed command %s to RC (room=%s)",
                            cmd_token, room_id,
                        )
                        return  # RC handled it
                break  # tried one text, fall through to agent

        # If we found and tried to route a / command, replace message_text
        # with the extracted command so downstream (coerce_plaintext_gateway_command,
        # msg_type detection, etc.) sees the cleaned command text.
        if _found_slash_cmd:
            message_text = cmd_full

        # Bidirectional title sync: when /title is used, update RC topic.
        # This runs BEFORE the gateway processes the /title command so both happen:
        # RC topic is updated (here) and session title is set (in gateway).
        if _found_slash_cmd and cmd_full.startswith("/title "):
            _title_val = cmd_full[len("/title "):].strip()
            if _title_val:
                _topic_endpoint = self._set_topic_endpoint(chat_type)
                try:
                    data = await self._api_post(_topic_endpoint, {
                        "roomId": room_id,
                        "topic": _title_val,
                    })
                    if data and data.get("success"):
                        self._last_topic[room_id] = _title_val
                except Exception:
                    logger.debug("Failed to sync RC topic from /title via %s", _topic_endpoint, exc_info=True)

        msg_type = MessageType.TEXT
        if message_text.startswith("/"):
            msg_type = MessageType.COMMAND
        # Also handle the case where routing found a / but RC didn't know it
        # (message_text might still contain @mention in DMs)
        if _found_slash_cmd and msg_type != MessageType.COMMAND:
            msg_type = MessageType.COMMAND

        media_urls, media_types = await self._download_attachments(post)

        if media_types and msg_type == MessageType.TEXT:
            if any(m.startswith("image/") for m in media_types):
                msg_type = MessageType.PHOTO
            elif any(m.startswith("audio/") for m in media_types):
                msg_type = MessageType.VOICE
            else:
                msg_type = MessageType.DOCUMENT

        source = self.build_source(
            chat_id=room_id,
            chat_type=chat_type,
            user_id=sender_id,
            user_name=sender_name,
            thread_id=thread_id,
        )

        from gateway.platforms.base import resolve_channel_prompt
        channel_prompt = resolve_channel_prompt(
            self.config.extra, room_id, None,
        )

        msg_event = MessageEvent(
            text=message_text,
            message_type=msg_type,
            source=source,
            raw_message=post,
            message_id=post_id,
            media_urls=media_urls if media_urls else None,
            media_types=media_types if media_types else None,
            channel_prompt=channel_prompt,
        )

        await self.handle_message(msg_event)

    async def _resolve_room_type(self, room_id: str) -> str:
        """Look up a room's type via REST. Defaults to 'channel' on failure."""
        data = await self._api_get("rooms.info", params={"roomId": room_id})
        room = (data or {}).get("room") or {}
        return _ROOM_TYPE_MAP.get(room.get("t", "c"), "channel")

    async def _download_attachments(
        self, post: Dict[str, Any]
    ) -> tuple[List[str], List[str]]:
        """Download every file attached to *post* into the local cache."""
        import aiohttp

        media_urls: List[str] = []
        media_types: List[str] = []

        candidates: List[Dict[str, str]] = []

        # Primary single-file attachment.
        primary = post.get("file") or {}
        if isinstance(primary, dict) and primary.get("_id"):
            candidates.append({
                "id": primary["_id"],
                "name": primary.get("name", f"file_{primary['_id']}"),
                "type": primary.get("type", "application/octet-stream"),
            })

        # Multi-attachment payload.
        for att in post.get("attachments") or []:
            if not isinstance(att, dict):
                continue
            path = (
                att.get("image_url")
                or att.get("audio_url")
                or att.get("video_url")
                or att.get("title_link")
                or ""
            )
            m = re.match(r"^/file-upload/([^/?#]+)/([^/?#]+)", path)
            if not m:
                continue
            fid = m.group(1)
            if any(c["id"] == fid for c in candidates):
                continue
            fname = att.get("title") or m.group(2)
            if att.get("image_url"):
                mime = att.get("image_type") or "image/png"
            elif att.get("audio_url"):
                mime = att.get("audio_type") or "audio/ogg"
            elif att.get("video_url"):
                mime = att.get("video_type") or "video/mp4"
            else:
                mime = "application/octet-stream"
            candidates.append({"id": fid, "name": fname, "type": mime})

        for cand in candidates:
            try:
                url = f"{self._base_url}/file-upload/{cand['id']}/{cand['name']}"
                async with self._session.get(
                    url,
                    headers={
                        "X-Auth-Token": self._token,
                        "X-User-Id": self._bot_user_id,
                    },
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status >= 400:
                        logger.warning("Rocket.Chat: failed to download file %s: HTTP %s",
                                       cand["id"], resp.status)
                        continue
                    file_data = await resp.read()
                    mime = resp.content_type or cand["type"]
                    ext = Path(cand["name"]).suffix

                    from gateway.platforms.base import (
                        cache_image_from_bytes,
                        cache_audio_from_bytes,
                        cache_document_from_bytes,
                    )
                    if mime.startswith("image/"):
                        local_path = cache_image_from_bytes(file_data, ext or ".png")
                    elif mime.startswith("audio/"):
                        # Convert to MP3 first (Groq STT needs a widely-supported format)
                        raw_ext = ext or ".ogg"
                        raw_path = cache_audio_from_bytes(file_data, raw_ext)
                        local_path = await self._convert_audio_to_mp3(raw_path)
                        if local_path is None:
                            local_path = raw_path  # fallback: use original
                    else:
                        local_path = cache_document_from_bytes(file_data, cand["name"])
                    media_urls.append(local_path)
                    media_types.append(mime)
            except Exception as exc:
                logger.warning("Rocket.Chat: error downloading file %s: %s", cand["id"], exc)

        return media_urls, media_types

    # ── Audio conversion ──────────────────────────────────────────────

    async def _convert_audio_to_mp3(self, src_path: str) -> str | None:
        """Convert an audio file to MP3 using ffmpeg (for STT compatibility).

        Returns the converted MP3 path, or None if conversion failed.
        ffmpeg must be installed on the system.
        """
        if src_path.endswith(".mp3"):
            return src_path  # already MP3, skip
        dst_path = src_path.rsplit(".", 1)[0] + ".mp3"
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-y", "-i", src_path, "-ar", "16000", "-ac", "1",
                "-b:a", "64k", dst_path,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await asyncio.wait_for(proc.communicate(), timeout=30)
            if proc.returncode == 0:
                return dst_path
            logger.warning("Rocket.Chat: ffmpeg conversion failed (rc=%d)", proc.returncode)
        except FileNotFoundError:
            logger.warning("Rocket.Chat: ffmpeg not found — audio sent as-is to STT")
        except Exception as exc:
            logger.warning("Rocket.Chat: ffmpeg error: %s", exc)
        return None

    # ── Reactions ─────────────────────────────────────────────────────

    async def _add_reaction(self, message_id: str, emoji: str) -> bool:
        """Add an emoji reaction to a Rocket.Chat message.

        Rocket.Chat uses ``POST /api/v1/chat.react``. If the bot already
        reacted with this emoji, it removes the reaction (toggle).
        """
        data = await self._api_post(
            "chat.react",
            {"messageId": message_id, "emoji": emoji},
        )
        return bool(data and data.get("success"))

    async def _remove_reaction(self, message_id: str, emoji: str) -> bool:
        """Remove the bot's own emoji reaction from a message.

        ``chat.react`` toggles — calling it again removes the reaction.
        """
        return await self._add_reaction(message_id, emoji)

    def _reactions_enabled(self) -> bool:
        """Check if message reactions are enabled via config/env."""
        return os.getenv("ROCKETCHAT_REACTIONS", "true").lower() not in {
            "false", "0", "no",
        }

    async def on_processing_start(self, event: MessageEvent) -> None:
        """Add an in-progress 👀 reaction when processing begins."""
        if not self._reactions_enabled():
            return
        message_id = event.message_id
        if message_id:
            await self._add_reaction(message_id, ":eyes:")

    async def on_processing_complete(self, event: MessageEvent, outcome: ProcessingOutcome) -> None:
        """Swap the 👀 reaction for ✅ (success) or ❌ (failure)."""
        if not self._reactions_enabled():
            return
        message_id = event.message_id
        if not message_id:
            return
        await self._remove_reaction(message_id, ":eyes:")
        if outcome == ProcessingOutcome.SUCCESS:
            await self._add_reaction(message_id, ":white_check_mark:")
        elif outcome == ProcessingOutcome.FAILURE:
            await self._add_reaction(message_id, ":x:")
