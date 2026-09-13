"""Plugin-level helpers: requirement checks, config validation, env
enablement, shared constants, and the standalone cron sender (REST-only,
no live adapter needed)."""

from __future__ import annotations

import inspect
import json
import logging
import mimetypes
import os
import re
import unicodedata
from datetime import datetime, UTC
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote, unquote, urlsplit, urlunsplit

logger = logging.getLogger(__name__)

# Rocket.Chat's default Message_MaxAllowedSize is 5000; admins can raise it
# but the safe default for multi-line messages is 5000.
MAX_MESSAGE_LENGTH = 5000

# Inbound attachments and outbound URL-backed media are buffered before Hermes
# caches or uploads them.  Keep a hard process-local ceiling even if an operator
# misconfigures the deployment value; unlike the agent-upload guard, zero never
# means unlimited for network-controlled response bodies.
DEFAULT_MEDIA_DOWNLOAD_MAX_BYTES = 100 * 1024 * 1024
HARD_MEDIA_DOWNLOAD_MAX_BYTES = 1024 * 1024 * 1024

# Every authenticated Rocket.Chat JSON response is network-controlled.  Bound
# it before decoding so a malicious/compromised server (or an unexpectedly huge
# thread) cannot make the gateway buffer and parse an unbounded body.
DEFAULT_API_RESPONSE_MAX_BYTES = 2 * 1024 * 1024
MIN_API_RESPONSE_MAX_BYTES = 64 * 1024
HARD_API_RESPONSE_MAX_BYTES = 16 * 1024 * 1024

# Room type codes returned by the Rocket.Chat API.
#   d = direct message (1:1)
#   c = public channel
#   p = private group (private channel)
#   l = livechat / omnichannel
_ROOM_TYPE_MAP = {
    "d": "dm",
    "c": "channel",
    "p": "group",
    "l": "group",
}

# Reconnect parameters (exponential backoff).
_RECONNECT_BASE_DELAY = 2.0
_RECONNECT_MAX_DELAY = 60.0
_RECONNECT_JITTER = 0.2

# WebSocket keepalive.  aiohttp sends a protocol-level PING every N seconds and
# raises when the PONG does not arrive, which is the only thing that turns a
# silently half-open socket back into a reconnect.  Without it the read loop
# blocks forever and inbound traffic stops permanently while the process still
# looks healthy.
DEFAULT_WS_HEARTBEAT_SECONDS = 30.0
MIN_WS_HEARTBEAT_SECONDS = 5.0
MAX_WS_HEARTBEAT_SECONDS = 300.0

# DDP protocol version. Rocket.Chat supports "1" across 7.x/8.x.
_DDP_PROTOCOL_VERSION = "1"

# Rocket.Chat broadcasts a whole message document on ``stream-room-messages``
# for every mutation of that document, not only for the insert: a thread reply
# bumps the root's ``tcount``/``tlm``, a reaction (including this adapter's own
# 👀/✅ markers), a pin, a star, or an edit rewrites it (server:
# ``notifyOnMessageChange`` -> ``watch.messages`` -> ``__my_messages__``).  The
# republished frame has exactly the shape of a fresh post.  A fresh insert never
# carries the fields below, and its ``_updatedAt`` equals ``ts`` within seconds
# (``sendMessage`` resets a client ``ts`` more than 10 s off the server clock),
# so the two signals together classify a frame without a server round trip.
_REPUBLISH_MARKER_FIELDS = ("editedAt", "tcount", "tlm", "replies", "pinnedAt", "pinnedBy", "starred")
REPUBLISH_TOLERANCE_SECONDS = 60.0

# Inbound dedup window.  Hermes' default (300 s / 2000 ids) is shorter than a
# working conversation; a message id must stay recognizable for longer than
# any thread it may be republished from, so both guards interlock.
INBOUND_DEDUP_TTL_SECONDS = 6 * 60 * 60.0
INBOUND_DEDUP_MAX_ENTRIES = 20_000

# Inbound frames are processed off the DDP read loop so protocol pings are
# answered while attachments download, ffmpeg runs, or thread history loads.
DEFAULT_INBOUND_MAX_CONCURRENCY = 8

_TRUE_VALUES = {"1", "true", "yes", "on"}


def env_get(name: str, default: Optional[str] = None) -> Optional[str]:
    """Read a ``ROCKETCHAT_*`` variable through Hermes' profile-scoped secret reader.

    Under ``gateway.multiplex_profiles`` a secondary profile's ``.env`` exists only
    in its secret scope while ``os.environ`` holds the DEFAULT profile's values.
    ``get_scoped_secret`` returns ``default`` on a scoped miss (never another
    profile's credential or allowlist) and reads ``os.environ`` only when no scope
    is installed.  Outside a Hermes runtime the plain environment is used.
    """
    try:
        from gateway.platforms._shared import get_scoped_secret
    except Exception:
        value = os.getenv(name)
    else:
        try:
            value = get_scoped_secret(name, None)
        except Exception:
            value = None
    if value is None:
        return default
    return str(value)


def parse_rocketchat_timestamp(value: Any) -> Optional[float]:
    """Return a POSIX timestamp for a Rocket.Chat date field, else ``None``.

    DDP frames carry EJSON dates (``{"$date": <epoch milliseconds>}``); the REST
    API returns ISO 8601 strings.  Both shapes reach inbound handling.
    """
    if isinstance(value, dict):
        value = value.get("$date")
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value) / 1000.0
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.timestamp()
    return None


def is_mutation_republish(post: Any) -> bool:
    """Return whether *post* is a republished document rather than a new post.

    Structural markers are checked first: a fresh insert never has an edit
    stamp, thread counters, pins, stars, or reactions.  The ``_updatedAt - ts``
    distance (both server-side clocks) is the fallback for mutations that clear
    those fields again.  Frames without usable timestamps are reported as new
    posts; the inbound dedup cache is the second guard.
    """
    if not isinstance(post, dict):
        return False
    for field in _REPUBLISH_MARKER_FIELDS:
        value = post.get(field)
        if value is None or value is False or value == [] or value == {}:
            continue
        if field in {"tcount", "tlm", "replies", "starred"} and value == 0:
            continue
        return True
    if post.get("pinned") is True:
        return True
    reactions = post.get("reactions")
    if isinstance(reactions, dict) and reactions:
        return True
    posted_at = parse_rocketchat_timestamp(post.get("ts"))
    updated_at = parse_rocketchat_timestamp(post.get("_updatedAt"))
    if posted_at is None or updated_at is None:
        return False
    return (updated_at - posted_at) > REPUBLISH_TOLERANCE_SECONDS


def inbound_max_concurrency() -> int:
    """Concurrent inbound frames processed off the DDP read loop (1-64)."""
    raw = env_get("ROCKETCHAT_INBOUND_MAX_CONCURRENCY")
    try:
        value = int(str(raw).strip()) if raw is not None else DEFAULT_INBOUND_MAX_CONCURRENCY
    except (TypeError, ValueError):
        return DEFAULT_INBOUND_MAX_CONCURRENCY
    return min(64, max(1, value))


_PERCENT_ESCAPE_RE = re.compile(r"%(?![0-9A-Fa-f]{2})")
_DELEGATION_ENVELOPE_RE = re.compile(
    r"\A\[hermes-delegation:v1:(task|result):([0-9a-f]{32})\](?:\r?\n)?"
)


def build_delegation_envelope(kind: str, delegation_id: str, text: str) -> str:
    """Build a bounded, machine-readable one-shot bot delegation message."""
    if kind not in {"task", "result"}:
        raise ValueError("invalid delegation kind")
    if not re.fullmatch(r"[0-9a-f]{32}", delegation_id):
        raise ValueError("invalid delegation id")
    if not isinstance(text, str):
        raise TypeError("invalid delegation text")
    return f"[hermes-delegation:v1:{kind}:{delegation_id}]\n{text}"


def parse_delegation_envelope(
    text: Any,
) -> tuple[Optional[str], Optional[str], str]:
    """Return ``(kind, id, body)`` without treating ordinary text as protocol."""
    if not isinstance(text, str):
        return None, None, ""
    match = _DELEGATION_ENVELOPE_RE.match(text)
    if match is None:
        return None, None, text
    return match.group(1), match.group(2), text[match.end():]


def _env_flag(name: str, *, default: bool = False) -> bool:
    """Read a conventional boolean flag: 1/true/yes/on enable, anything else disables."""
    raw = env_get(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in _TRUE_VALUES


def is_valid_server_identifier(value: Any) -> bool:
    """Return whether *value* is a compact, unambiguous Rocket.Chat id/name."""
    return (
        isinstance(value, str)
        and bool(value)
        and value == value.strip()
        and len(value) <= 255
        and not any(
            unicodedata.category(char) in {"Cc", "Cf", "Cs"}
            for char in value
        )
    )


def is_valid_url_path_identifier(value: Any) -> bool:
    """Return whether an opaque id is safe as one URL path component."""
    return bool(
        is_valid_server_identifier(value)
        and value not in {".", ".."}
        and "/" not in value
        and "\\" not in value
    )


def media_download_max_bytes() -> int:
    """Return the bounded network-media response limit.

    Values below one byte or otherwise invalid retain the secure default.  A
    deployment may lower the limit freely, while the hard 1 GiB ceiling cannot
    be disabled through configuration.
    """
    raw = env_get(
        "ROCKETCHAT_MEDIA_DOWNLOAD_MAX_BYTES",
        str(DEFAULT_MEDIA_DOWNLOAD_MAX_BYTES),
    )
    try:
        value = int(raw.strip())
    except (AttributeError, TypeError, ValueError):
        return DEFAULT_MEDIA_DOWNLOAD_MAX_BYTES
    if value < 1:
        return DEFAULT_MEDIA_DOWNLOAD_MAX_BYTES
    return min(value, HARD_MEDIA_DOWNLOAD_MAX_BYTES)


def ws_heartbeat_seconds() -> float | None:
    """Return the DDP WebSocket keepalive interval in seconds.

    ``None`` means aiohttp keepalive stays off, which is only reachable through
    the literal value ``0``.  Every other invalid or out-of-range value keeps
    the safe default rather than silently disabling the guard: a deployment
    that fat-fingers this variable must not end up with the permanently silent
    inbound stream this setting exists to prevent.
    """
    raw = env_get("ROCKETCHAT_WS_HEARTBEAT_SECONDS")
    if raw is None:
        return DEFAULT_WS_HEARTBEAT_SECONDS
    stripped = raw.strip()
    if stripped == "0":
        return None
    try:
        value = float(stripped)
    except (AttributeError, TypeError, ValueError):
        return DEFAULT_WS_HEARTBEAT_SECONDS
    if value <= 0:
        return DEFAULT_WS_HEARTBEAT_SECONDS
    return min(MAX_WS_HEARTBEAT_SECONDS, max(MIN_WS_HEARTBEAT_SECONDS, value))


class MediaDownloadTooLarge(ValueError):
    """Raised before a network-controlled body can exceed its memory budget."""


class ApiResponseTooLarge(ValueError):
    """Raised before a Rocket.Chat JSON body can exceed its memory budget."""


def api_response_max_bytes() -> int:
    """Return the process-wide limit for Rocket.Chat JSON REST responses."""
    raw = env_get(
        "ROCKETCHAT_AGENT_RESPONSE_MAX_BYTES",
        str(DEFAULT_API_RESPONSE_MAX_BYTES),
    )
    try:
        value = int(raw.strip())
    except (AttributeError, TypeError, ValueError):
        return DEFAULT_API_RESPONSE_MAX_BYTES
    return min(
        HARD_API_RESPONSE_MAX_BYTES,
        max(MIN_API_RESPONSE_MAX_BYTES, value),
    )


async def read_bounded_json_response(response: Any) -> Dict[str, Any]:
    """Read and decode one bounded Rocket.Chat JSON object response.

    The byte count applies to the actual (possibly decompressed) chunks yielded
    by aiohttp, rather than trusting only ``Content-Length``.  The final JSON
    value must be an object because every plugin call site expects a mapping.
    """
    maximum = api_response_max_bytes()
    content_length = getattr(response, "content_length", None)
    if (
        isinstance(content_length, int)
        and not isinstance(content_length, bool)
        and content_length > maximum
    ):
        raise ApiResponseTooLarge(
            "Rocket.Chat API response exceeded the configured limit"
        )

    content = getattr(response, "content", None)
    body: Optional[bytes] = None
    if content is not None and hasattr(content, "iter_chunked"):
        chunks = bytearray()
        async for chunk in content.iter_chunked(64 * 1024):
            if not isinstance(chunk, (bytes, bytearray, memoryview)):
                raise ValueError("Rocket.Chat API response is invalid")
            chunks.extend(chunk)
            if len(chunks) > maximum:
                raise ApiResponseTooLarge(
                    "Rocket.Chat API response exceeded the configured limit"
                )
        body = bytes(chunks)
    else:
        read = getattr(response, "read", None)
        if callable(read) and inspect.iscoroutinefunction(read):
            raw_body = await read()
            if not isinstance(raw_body, (bytes, bytearray, memoryview)):
                raise ValueError("Rocket.Chat API response is invalid")
            if len(raw_body) > maximum:
                raise ApiResponseTooLarge(
                    "Rocket.Chat API response exceeded the configured limit"
                )
            body = bytes(raw_body)

    if body is not None:
        data = json.loads(body.decode("utf-8")) if body else {}
    else:
        # Compatibility for minimal unit-test doubles.  Real aiohttp responses
        # always expose ``content``/``read`` and therefore take a bounded path.
        data = await response.json(content_type=None)
    if not isinstance(data, dict):
        raise ValueError("Rocket.Chat API response is invalid")
    return data


async def read_bounded_response_bytes(
    response: Any, *, maximum: Optional[int] = None
) -> bytes:
    """Stream one HTTP response into memory without exceeding the media limit."""
    configured_maximum = media_download_max_bytes()
    if maximum is None:
        maximum = configured_maximum
    elif not isinstance(maximum, int) or isinstance(maximum, bool) or maximum < 1:
        raise MediaDownloadTooLarge("Rocket.Chat media response is too large")
    else:
        maximum = min(maximum, configured_maximum)
    content_length = getattr(response, "content_length", None)
    if (
        isinstance(content_length, int)
        and not isinstance(content_length, bool)
        and content_length > maximum
    ):
        raise MediaDownloadTooLarge("Rocket.Chat media response is too large")

    content = getattr(response, "content", None)
    if content is not None and hasattr(content, "iter_chunked"):
        body = bytearray()
        async for chunk in content.iter_chunked(64 * 1024):
            if not isinstance(chunk, (bytes, bytearray, memoryview)):
                raise ValueError("Rocket.Chat media response is invalid")
            body.extend(chunk)
            if len(body) > maximum:
                raise MediaDownloadTooLarge(
                    "Rocket.Chat media response is too large"
                )
        return bytes(body)

    body = await response.read()
    if not isinstance(body, (bytes, bytearray, memoryview)):
        raise ValueError("Rocket.Chat media response is invalid")
    if len(body) > maximum:
        raise MediaDownloadTooLarge("Rocket.Chat media response is too large")
    return bytes(body)


def _has_forbidden_url_chars(value: str, *, decoded: bool = False) -> bool:
    """Reject characters that can hide or split an authenticated URL."""
    for char in value:
        category = unicodedata.category(char)
        if category in {"Cc", "Cf", "Cs"}:
            return True
        if not decoded and char.isspace():
            return True
    return False


def validate_server_url(raw_url: Any) -> str:
    """Return a normalized Rocket.Chat HTTP base URL or raise ``ValueError``.

    The URL is an authentication boundary because PAT-bearing requests and the
    DDP resume token are sent to this origin.  HTTPS is mandatory unless the
    operator explicitly opts in to HTTP for a trusted local deployment via
    ``ROCKETCHAT_ALLOW_INSECURE_HTTP=true``.
    """
    if not isinstance(raw_url, str) or not raw_url:
        raise ValueError("Rocket.Chat server URL is invalid")
    if (
        _has_forbidden_url_chars(raw_url)
        or "\\" in raw_url
        or _PERCENT_ESCAPE_RE.search(raw_url)
    ):
        raise ValueError("Rocket.Chat server URL is invalid")
    decoded = unquote(raw_url)
    if _has_forbidden_url_chars(decoded, decoded=True):
        raise ValueError("Rocket.Chat server URL is invalid")

    try:
        parsed = urlsplit(raw_url)
        scheme = parsed.scheme.lower()
        if scheme not in {"https", "http"}:
            raise ValueError("Rocket.Chat server URL is invalid")
        if scheme == "http" and not _env_flag(
            "ROCKETCHAT_ALLOW_INSECURE_HTTP"
        ):
            raise ValueError("Rocket.Chat server URL must use HTTPS")
        if (
            not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or "@" in parsed.netloc
            or parsed.query
            or parsed.fragment
            or parsed.netloc.endswith(":")
            or "%" in parsed.hostname
        ):
            raise ValueError("Rocket.Chat server URL is invalid")
        parsed.port  # noqa: B018 - validates malformed and out-of-range ports eagerly
    except (TypeError, ValueError) as exc:
        if isinstance(exc, ValueError) and str(exc).startswith("Rocket.Chat"):
            raise
        raise ValueError("Rocket.Chat server URL is invalid") from None

    path = parsed.path.rstrip("/")
    return urlunsplit((scheme, parsed.netloc, path, "", ""))


def _normalize_auth_value(raw_value: Any, *, maximum: int) -> str:
    """Normalize a header credential without ever including it in errors."""
    if not isinstance(raw_value, str):
        raise ValueError("Rocket.Chat credentials are invalid")
    value = raw_value.strip()
    if (
        not value
        or len(value) > maximum
        or any(
            char.isspace()
            or unicodedata.category(char) in {"Cc", "Cf", "Cs"}
            for char in value
        )
    ):
        raise ValueError("Rocket.Chat credentials are invalid")
    return value


def validate_auth_config(
    raw_url: Any, raw_token: Any, raw_user_id: Any
) -> tuple[str, str, str]:
    """Validate and normalize one PAT-authenticated Rocket.Chat config."""
    return (
        validate_server_url(raw_url),
        _normalize_auth_value(raw_token, maximum=4096),
        _normalize_auth_value(raw_user_id, maximum=255),
    )


def websocket_url(raw_base_url: Any) -> str:
    """Build the DDP endpoint from a validated HTTP(S) server URL."""
    base_url = validate_server_url(raw_base_url)
    parsed = urlsplit(base_url)
    scheme = "wss" if parsed.scheme == "https" else "ws"
    path = f"{parsed.path}/websocket"
    return urlunsplit((scheme, parsed.netloc, path, "", ""))


def websocket_endpoint_matches(expected_url: Any, actual_url: Any) -> bool:
    """Return whether a completed WS handshake stayed on its exact endpoint."""

    def endpoint_key(raw_url: Any) -> tuple[str, str, int, str]:
        if not isinstance(raw_url, str) or not raw_url:
            raise ValueError
        if (
            _has_forbidden_url_chars(raw_url)
            or "\\" in raw_url
            or _PERCENT_ESCAPE_RE.search(raw_url)
        ):
            raise ValueError
        parsed = urlsplit(raw_url)
        scheme = parsed.scheme.lower()
        if (
            scheme not in {"ws", "wss"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or "@" in parsed.netloc
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError
        port = parsed.port
        if port is None:
            port = 443 if scheme == "wss" else 80
        return scheme, parsed.hostname.lower(), port, parsed.path

    try:
        return endpoint_key(expected_url) == endpoint_key(str(actual_url))
    except (TypeError, ValueError):
        return False


def check_requirements() -> bool:
    """Passive dependency probe for the platform registry: is aiohttp importable?

    Hermes calls ``check_fn`` from status displays and from ``create_adapter()``;
    it must not depend on configuration (a ``config.yaml``-only deployment has no
    ``ROCKETCHAT_*`` variables until the YAML bridge runs).  Credentials are
    checked by :func:`validate_config` and :func:`credentials_configured`.
    """
    try:
        import aiohttp  # noqa: F401
    except ImportError:
        return False
    return True


def credentials_configured() -> bool:
    """Return whether the scoped environment carries a valid PAT configuration."""
    try:
        validate_auth_config(
            env_get("ROCKETCHAT_URL", ""),
            env_get("ROCKETCHAT_TOKEN", ""),
            env_get("ROCKETCHAT_USER_ID", ""),
        )
    except ValueError:
        return False
    return True


def validate_config(config) -> bool:
    """Validate that the platform config has enough info to connect."""
    extra = getattr(config, "extra", {}) or {}
    url = env_get("ROCKETCHAT_URL") or extra.get("url", "")
    token = env_get("ROCKETCHAT_TOKEN") or getattr(config, "token", "") or extra.get("token", "")
    user_id = env_get("ROCKETCHAT_USER_ID") or extra.get("user_id", "")
    try:
        validate_auth_config(url, token, user_id)
        return True
    except ValueError:
        return False


def is_connected(config) -> bool:
    """Check whether Rocket.Chat is configured (env or config.yaml)."""
    return validate_config(config)


def _env_enablement() -> dict | None:
    """Seed ``PlatformConfig.extra`` from env vars during gateway config load.

    Called by the platform registry's env-enablement hook BEFORE adapter
    construction, so ``gateway status`` reflects env-only configuration
    without instantiating the Rocket.Chat client.

    Returns ``None`` when Rocket.Chat isn't minimally configured.
    """
    raw_url = env_get("ROCKETCHAT_URL", "")
    token = env_get("ROCKETCHAT_TOKEN", "").strip()
    user_id = env_get("ROCKETCHAT_USER_ID", "").strip()
    try:
        url, token, user_id = validate_auth_config(raw_url, token, user_id)
    except ValueError:
        return None

    seed: dict = {
        "url": url,
        "token": token,
        "user_id": user_id,
    }

    reply_mode = env_get("ROCKETCHAT_REPLY_MODE", "").strip()
    if reply_mode:
        seed["reply_mode"] = reply_mode

    suppress_home_notice = env_get(
        "ROCKETCHAT_SUPPRESS_HOME_CHANNEL_NOTICE", ""
    ).strip()
    if suppress_home_notice:
        seed["suppress_home_channel_notice"] = suppress_home_notice

    home = env_get("ROCKETCHAT_HOME_CHANNEL", "").strip()
    if home:
        seed["home_channel"] = {
            "chat_id": home,
            "name": env_get("ROCKETCHAT_HOME_CHANNEL_NAME", home),
        }

    return seed


def _csv_env(value: Any) -> str:
    if isinstance(value, (list, tuple, set)):
        return ",".join(str(item).strip() for item in value if str(item).strip())
    return str(value).strip()


def _pathsep_env(value: Any) -> str:
    if isinstance(value, (list, tuple, set)):
        return os.pathsep.join(str(item).strip() for item in value if str(item).strip())
    return str(value).strip()


def _lower_env(value: Any) -> str:
    return str(value).strip().lower()


# (config.yaml key under ``platforms.rocketchat``, env var, yaml value -> env string)
_YAML_BRIDGE = (
    ("url", "ROCKETCHAT_URL", str),
    ("token", "ROCKETCHAT_TOKEN", str),
    ("user_id", "ROCKETCHAT_USER_ID", str),
    ("allowed_users", "ROCKETCHAT_ALLOWED_USERS", _csv_env),
    ("allow_all_users", "ROCKETCHAT_ALLOW_ALL_USERS", _lower_env),
    ("bot_peers", "ROCKETCHAT_BOT_PEERS", _csv_env),
    ("home_channel", "ROCKETCHAT_HOME_CHANNEL", str),
    ("home_channel_name", "ROCKETCHAT_HOME_CHANNEL_NAME", str),
    ("suppress_home_channel_notice", "ROCKETCHAT_SUPPRESS_HOME_CHANNEL_NOTICE", _lower_env),
    ("require_mention", "ROCKETCHAT_REQUIRE_MENTION", _lower_env),
    ("require_membership", "ROCKETCHAT_REQUIRE_MEMBERSHIP", _lower_env),
    ("free_response_channels", "ROCKETCHAT_FREE_RESPONSE_CHANNELS", _csv_env),
    ("reply_mode", "ROCKETCHAT_REPLY_MODE", _lower_env),
    ("reactions", "ROCKETCHAT_REACTIONS", _lower_env),
    ("topic_sync", "ROCKETCHAT_TOPIC_SYNC", _lower_env),
    ("ws_heartbeat_seconds", "ROCKETCHAT_WS_HEARTBEAT_SECONDS", str),
    ("inbound_max_concurrency", "ROCKETCHAT_INBOUND_MAX_CONCURRENCY", str),
    ("allow_insecure_http", "ROCKETCHAT_ALLOW_INSECURE_HTTP", _lower_env),
    ("allow_private_file_redirects", "ROCKETCHAT_ALLOW_PRIVATE_FILE_REDIRECTS", _lower_env),
    ("thread_context_max_chars", "ROCKETCHAT_THREAD_CONTEXT_MAX_CHARS", str),
    ("media_download_max_bytes", "ROCKETCHAT_MEDIA_DOWNLOAD_MAX_BYTES", str),
    ("forwarded_slash_commands", "ROCKETCHAT_FORWARDED_SLASH_COMMANDS", _csv_env),
    ("agent_write_tools", "ROCKETCHAT_AGENT_WRITE_TOOLS", _lower_env),
    ("agent_write_allowed_rooms", "ROCKETCHAT_AGENT_WRITE_ALLOWED_ROOMS", _csv_env),
    ("agent_write_trusted_users", "ROCKETCHAT_AGENT_WRITE_TRUSTED_USERS", _csv_env),
    ("agent_tools_allow_external", "ROCKETCHAT_AGENT_TOOLS_ALLOW_EXTERNAL", _lower_env),
    ("agent_file_uploads", "ROCKETCHAT_AGENT_FILE_UPLOADS", _lower_env),
    ("agent_file_allowed_roots", "ROCKETCHAT_AGENT_FILE_ALLOWED_ROOTS", _pathsep_env),
    ("agent_file_max_bytes", "ROCKETCHAT_AGENT_FILE_MAX_BYTES", str),
    ("retrieval_allowed_rooms", "ROCKETCHAT_RETRIEVAL_ALLOWED_ROOMS", _csv_env),
    ("retrieval_trusted_users", "ROCKETCHAT_RETRIEVAL_TRUSTED_USERS", _csv_env),
    ("retrieval_allow_contextless", "ROCKETCHAT_RETRIEVAL_ALLOW_CONTEXTLESS", _lower_env),
    ("retrieval_max_result_chars", "ROCKETCHAT_RETRIEVAL_MAX_RESULT_CHARS", str),
    ("retrieval_redact_secrets", "ROCKETCHAT_RETRIEVAL_REDACT_SECRETS", _lower_env),
    ("retrieval_include_file_urls", "ROCKETCHAT_RETRIEVAL_INCLUDE_FILE_URLS", _lower_env),
    ("retrieval_include_reaction_identities", "ROCKETCHAT_RETRIEVAL_INCLUDE_REACTION_IDENTITIES", _lower_env),
    ("retrieval_include_user_ids", "ROCKETCHAT_RETRIEVAL_INCLUDE_USER_IDS", _lower_env),
    ("agent_file_max_concurrency", "ROCKETCHAT_AGENT_FILE_MAX_CONCURRENCY", str),
    ("agent_response_max_bytes", "ROCKETCHAT_AGENT_RESPONSE_MAX_BYTES", str),
    ("agent_max_concurrency", "ROCKETCHAT_AGENT_MAX_CONCURRENCY", str),
    ("agent_requests_per_minute", "ROCKETCHAT_AGENT_REQUESTS_PER_MINUTE", str),
)


def _apply_yaml_config(yaml_cfg: dict, rocketchat_cfg: dict) -> Optional[dict]:
    """Translate ``platforms.rocketchat`` keys from ``config.yaml`` into env vars and ``extra``.

    Implements Hermes' ``apply_yaml_config_fn`` contract.  Environment variables win
    (writes are guarded by ``not os.getenv``).  Under a multiplexed secondary
    profile the env write is skipped, because ``os.environ`` is shared by every
    profile; the values are still returned so the caller seeds this profile's
    ``PlatformConfig.extra``, which the adapter reads first.
    """
    if not isinstance(rocketchat_cfg, dict):
        return None
    try:
        from gateway.platforms._shared import profile_scoped

        skip_env_bridge = bool(profile_scoped())
    except Exception:
        skip_env_bridge = False
    seeded: dict = {}
    for key, env, to_env in _YAML_BRIDGE:
        if key not in rocketchat_cfg or rocketchat_cfg[key] is None:
            continue
        value = rocketchat_cfg[key]
        seeded[key] = value
        if not skip_env_bridge and not os.getenv(env):
            os.environ[env] = to_env(value)
    home = seeded.get("home_channel")
    if home is not None and not isinstance(home, dict):
        seeded["home_channel"] = {
            "chat_id": str(home).strip(),
            "name": str(seeded.get("home_channel_name") or home).strip(),
        }
    return seeded or None


async def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[List[str]] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    """Open an ephemeral REST-only connection to send a message for cron delivery.

    Uses ``chat.postMessage`` via aiohttp (no DDP WebSocket), chunking text at
    Rocket.Chat's UTF-16 message limit, then uploads each ``media_files`` entry
    through the two-step ``rooms.media`` flow.  ``force_document`` is accepted
    for signature parity: Rocket.Chat has one attachment type.
    """
    if (
        not is_valid_server_identifier(chat_id)
        or not isinstance(message, str)
        or (thread_id is not None and not is_valid_server_identifier(thread_id))
    ):
        return {"error": "Rocket.Chat standalone send target is invalid"}
    extra = getattr(pconfig, "extra", {}) or {}
    raw_url = env_get("ROCKETCHAT_URL") or extra.get("url", "")
    raw_token = env_get("ROCKETCHAT_TOKEN") or getattr(pconfig, "token", "") or extra.get("token", "")
    raw_user_id = env_get("ROCKETCHAT_USER_ID") or extra.get("user_id", "")
    try:
        url, token, user_id = validate_auth_config(
            raw_url, raw_token, raw_user_id
        )
    except ValueError:
        return {"error": "Rocket.Chat standalone send configuration is invalid"}

    files = [str(item) for item in (media_files or []) if isinstance(item, (str, Path)) and str(item)]
    if not message.strip() and not files:
        return {"error": "Rocket.Chat standalone send has nothing to deliver"}

    headers = {"X-Auth-Token": token, "X-User-Id": user_id}
    json_headers = {**headers, "Content-Type": "application/json"}

    import aiohttp

    try:
        from gateway.platforms.base import BasePlatformAdapter, utf16_len

        chunks = (
            BasePlatformAdapter.truncate_message(message, MAX_MESSAGE_LENGTH, len_fn=utf16_len)
            if message.strip()
            else []
        )
    except Exception:
        chunks = [message] if message.strip() else []

    last_id: Optional[str] = None
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=120), trust_env=False
        ) as session:
            for chunk in chunks:
                payload: Dict[str, Any] = {"roomId": chat_id, "text": chunk}
                if thread_id:
                    payload["tmid"] = thread_id
                async with session.post(
                    f"{url}/api/v1/chat.postMessage",
                    headers=json_headers,
                    json=payload,
                    allow_redirects=False,
                ) as resp:
                    if resp.status < 200 or resp.status >= 300:
                        logger.warning(
                            "Rocket.Chat standalone send rejected with HTTP %s",
                            resp.status,
                        )
                        return {"error": "Rocket.Chat standalone send was rejected"}
                    data = await read_bounded_json_response(resp)
                    if not isinstance(data, dict) or data.get("success") is not True:
                        return {"error": "Rocket.Chat standalone send returned an invalid response"}
                    msg = data.get("message")
                    if (
                        not isinstance(msg, dict)
                        or not is_valid_server_identifier(msg.get("_id"))
                        or msg.get("rid") != chat_id
                        or (thread_id and msg.get("tmid") != thread_id)
                    ):
                        return {"error": "Rocket.Chat standalone send returned an invalid target"}
                    last_id = msg["_id"]

            delivered = 0
            for file_path in files:
                message_id = await _standalone_upload(
                    session, url, headers, chat_id, file_path, thread_id
                )
                if message_id is None:
                    return {
                        "error": "Rocket.Chat standalone media upload failed",
                        "message_id": last_id,
                        "delivered_files": delivered,
                    }
                last_id = message_id
                delivered += 1
        result: Dict[str, Any] = {"success": True, "message_id": last_id}
        if files:
            result["delivered_files"] = len(files)
        return result
    except Exception as exc:
        logger.warning(
            "Rocket.Chat standalone send failed (%s)", type(exc).__name__
        )
        return {"error": "Rocket.Chat standalone send failed"}


async def _standalone_upload(
    session: Any,
    base_url: str,
    headers: Dict[str, str],
    room_id: str,
    file_path: str,
    thread_id: Optional[str],
) -> Optional[str]:
    """Upload one Hermes-produced file through ``rooms.media`` + ``rooms.mediaConfirm``.

    Paths come from Hermes itself (cron output, ``send_message`` media), not from
    the model, so no allowed-root policy applies; the file must still be a regular
    file below the network-media byte budget.
    """
    import asyncio

    import aiohttp

    if not is_valid_url_path_identifier(room_id):
        return None
    path = Path(file_path)
    try:
        if not path.is_file():
            logger.warning("Rocket.Chat standalone media path is not a regular file")
            return None
        size = path.stat().st_size
    except OSError:
        return None
    if size > media_download_max_bytes():
        logger.warning("Rocket.Chat standalone media exceeds the configured byte budget")
        return None
    data = await asyncio.to_thread(path.read_bytes)
    filename = path.name or "file.bin"
    content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
    form = aiohttp.FormData()
    form.add_field("file", data, filename=filename, content_type=content_type)
    async with session.post(
        f"{base_url}/api/v1/rooms.media/{quote(room_id, safe='')}",
        headers=headers,
        data=form,
        allow_redirects=False,
    ) as resp:
        if resp.status < 200 or resp.status >= 300:
            logger.warning("Rocket.Chat standalone rooms.media rejected with HTTP %s", resp.status)
            return None
        step1 = await read_bounded_json_response(resp)
    uploaded = step1.get("file") if isinstance(step1, dict) else None
    file_id = uploaded.get("_id") if isinstance(uploaded, dict) else None
    if step1.get("success", True) is not True or not is_valid_url_path_identifier(file_id):
        return None
    payload: Dict[str, Any] = {}
    if thread_id:
        payload["tmid"] = thread_id
    async with session.post(
        f"{base_url}/api/v1/rooms.mediaConfirm/{quote(room_id, safe='')}/{quote(str(file_id), safe='')}",
        headers={**headers, "Content-Type": "application/json"},
        json=payload,
        allow_redirects=False,
    ) as resp:
        if resp.status < 200 or resp.status >= 300:
            logger.warning("Rocket.Chat standalone rooms.mediaConfirm rejected with HTTP %s", resp.status)
            return None
        step2 = await read_bounded_json_response(resp)
    msg = step2.get("message") if isinstance(step2, dict) else None
    if (
        not isinstance(msg, dict)
        or not is_valid_server_identifier(msg.get("_id"))
        or msg.get("rid") != room_id
        or (thread_id and msg.get("tmid") != thread_id)
    ):
        return None
    return msg["_id"]
