"""File upload/download: the two-step rooms.media flow and media sends."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from gateway.platforms.base import SendResult

logger = logging.getLogger(__name__)


class MediaMixin:
    """Media sending for :class:`~.adapter.RocketchatAdapter`."""

    async def _upload_file(
        self,
        room_id: str,
        file_data: bytes,
        filename: str,
        content_type: str,
        caption: Optional[str] = None,
        tmid: Optional[str] = None,
    ) -> Optional[str]:
        """Upload a file via the two-step rooms.media flow.

        Step 1 uploads the bytes; step 2 confirms and creates the message.
        Returns the message _id on success, None on failure.
        """
        import aiohttp

        # Step 1: upload the file bytes.
        step1_url = f"{self._base_url}/api/v1/rooms.media/{room_id}"
        form = aiohttp.FormData()
        form.add_field(
            "file",
            file_data,
            filename=filename,
            content_type=content_type,
        )
        headers = {
            "X-Auth-Token": self._token,
            "X-User-Id": self._bot_user_id,
        }
        try:
            async with self._session.post(
                step1_url, headers=headers, data=form,
                timeout=aiohttp.ClientTimeout(total=120),
            ) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    logger.error("RC rooms.media → %s: %s", resp.status, body[:200])
                    return None
                step1 = await resp.json()
        except aiohttp.ClientError as exc:
            logger.error("RC rooms.media network error: %s", exc)
            return None

        file_id = (step1.get("file") or {}).get("_id")
        if not file_id:
            logger.error("RC rooms.media returned no file id: %s", step1)
            return None

        # Step 2: confirm — this creates the message.
        step2_path = f"rooms.mediaConfirm/{room_id}/{file_id}"
        payload: Dict[str, Any] = {}
        if caption:
            payload["msg"] = caption
        if tmid and self._reply_mode == "thread":
            payload["tmid"] = tmid
        step2 = await self._api_post(step2_path, payload)
        msg = step2.get("message") or {}
        return msg.get("_id")

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Download an image and upload it as a file attachment."""
        return await self._send_url_as_file(
            chat_id, image_url, caption, reply_to, "image"
        )

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Upload a local image file."""
        return await self._send_local_file(
            chat_id, image_path, caption, reply_to
        )

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Upload a local file as a document."""
        return await self._send_local_file(
            chat_id, file_path, caption, reply_to, file_name
        )

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Upload an audio file."""
        return await self._send_local_file(
            chat_id, audio_path, caption, reply_to
        )

    async def send_video(
        self,
        chat_id: str,
        video_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Upload a video file."""
        return await self._send_local_file(
            chat_id, video_path, caption, reply_to
        )

    # ------------------------------------------------------------------
    # File helpers
    # ------------------------------------------------------------------

    async def _send_url_as_file(
        self,
        chat_id: str,
        url: str,
        caption: Optional[str],
        reply_to: Optional[str],
        kind: str = "file",
    ) -> SendResult:
        """Download a URL and upload it as a file attachment."""
        from tools.url_safety import is_safe_url
        if not is_safe_url(url):
            logger.warning("Rocket.Chat: blocked unsafe URL (SSRF protection)")
            return await self.send(chat_id, f"{caption or ''}\n{url}".strip(), reply_to)

        import aiohttp

        file_data = None
        ct = "application/octet-stream"
        fname = url.rsplit("/", 1)[-1].split("?")[0] or f"{kind}.png"

        for attempt in range(3):
            try:
                async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status >= 500 or resp.status == 429:
                        if attempt < 2:
                            await asyncio.sleep(1.5 * (attempt + 1))
                            continue
                    if resp.status >= 400:
                        return await self.send(chat_id, f"{caption or ''}\n{url}".strip(), reply_to)
                    file_data = await resp.read()
                    ct = resp.content_type or "application/octet-stream"
                    break
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt < 2:
                    await asyncio.sleep(1.5 * (attempt + 1))
                    continue
                return await self.send(chat_id, f"{caption or ''}\n{url}".strip(), reply_to)

        if file_data is None:
            return await self.send(chat_id, f"{caption or ''}\n{url}".strip(), reply_to)

        msg_id = await self._upload_file(
            chat_id, file_data, fname, ct, caption, reply_to,
        )
        if not msg_id:
            return await self.send(chat_id, f"{caption or ''}\n{url}".strip(), reply_to)
        return SendResult(success=True, message_id=msg_id)

    async def _send_local_file(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str],
        reply_to: Optional[str],
        file_name: Optional[str] = None,
    ) -> SendResult:
        """Upload a local file via the two-step rooms.media flow."""
        import mimetypes

        p = Path(file_path)
        if not p.exists():
            return await self.send(
                chat_id, f"{caption or ''}\n(file not found: {file_path})", reply_to
            )

        fname = file_name or p.name
        ct = mimetypes.guess_type(fname)[0] or "application/octet-stream"
        file_data = p.read_bytes()

        msg_id = await self._upload_file(
            chat_id, file_data, fname, ct, caption, reply_to,
        )
        if not msg_id:
            return SendResult(success=False, error="File upload failed")
        return SendResult(success=True, message_id=msg_id)
