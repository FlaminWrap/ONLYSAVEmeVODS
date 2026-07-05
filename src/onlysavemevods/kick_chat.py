from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen
import json
import logging
import re
import tempfile
import time

from .models import LiveStream


LOGGER = logging.getLogger(__name__)
KICK_CHAT_HISTORY_STEP_SECONDS = 5
KICK_CHAT_HISTORY_ENDPOINT_HOSTS = ("https://web.kick.com", "https://kick.com")
KICK_EMOTE_RE = re.compile(r"\[emote:(?P<id>\d+):(?P<name>[^\]\r\n]+)\]")
KICK_CHAT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)


class KickChatReplayError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class KickVodChatMetadata:
    vod_uuid: str
    source_url: str
    channel_id: str
    chatroom_id: str
    stream_start: datetime
    duration_seconds: float
    title: str = ""


@dataclass(frozen=True, slots=True)
class KickChatReplayResult:
    ok: bool
    message: str
    chat_file: Path | None = None
    messages: int = 0


JsonRequester = Callable[[str], dict[str, Any]]
ProgressCallback = Callable[[str, float | None], None]


def download_kick_vod_chat_replay(
    stream: LiveStream,
    output_template: Path,
    *,
    requester: JsonRequester | None = None,
    progress: ProgressCallback | None = None,
    sleep_seconds: float = 0.05,
) -> KickChatReplayResult:
    if stream.platform != "kick":
        return KickChatReplayResult(False, "stream is not a Kick VOD")

    try:
        metadata = kick_vod_chat_metadata(stream, requester=requester)
        messages = fetch_kick_chat_history(
            metadata,
            requester=requester,
            progress=progress,
            sleep_seconds=sleep_seconds,
        )
    except KickChatReplayError as exc:
        return KickChatReplayResult(False, str(exc))

    chat_file = vod_chat_sidecar_for_output_template(output_template)
    write_kick_chat_sidecar(chat_file, metadata, messages, stream=stream)
    return KickChatReplayResult(
        True,
        f"Kick chat replay downloaded: {chat_file.name} ({len(messages)} messages)",
        chat_file=chat_file,
        messages=len(messages),
    )


def kick_vod_chat_metadata(
    stream: LiveStream,
    *,
    requester: JsonRequester | None = None,
) -> KickVodChatMetadata:
    raw = dict(stream.raw or {})
    if not metadata_has_kick_chat_fields(raw):
        fetched = fetch_kick_vod_metadata(stream.url, requester=requester)
        raw = merge_kick_metadata(raw, fetched)

    vod_uuid = str(raw.get("uuid") or raw.get("id") or kick_vod_uuid_from_url(stream.url)).strip()
    if not vod_uuid:
        raise KickChatReplayError("Kick VOD metadata did not include a VOD id")

    channel_id = first_text(
        raw.get("channel_id"),
        nested(raw, "livestream", "channel_id"),
        nested(raw, "livestream", "channel", "id"),
        nested(raw, "channel", "id"),
    )
    chatroom_id = first_text(
        raw.get("chatroom_id"),
        nested(raw, "chatroom", "id"),
        nested(raw, "livestream", "chatroom_id"),
        nested(raw, "livestream", "channel", "chatroom", "id"),
        nested(raw, "channel", "chatroom", "id"),
    )
    if not channel_id and not chatroom_id:
        raise KickChatReplayError("Kick VOD metadata did not include a channel or chatroom id")

    stream_start = first_datetime(
        raw.get("start_time"),
        raw.get("actual_start_timestamp"),
        raw.get("start_timestamp"),
        raw.get("timestamp"),
        nested(raw, "livestream", "start_time"),
    )
    if stream_start is None:
        raise KickChatReplayError("Kick VOD metadata did not include a stream start time")

    duration_seconds = first_duration_seconds(
        raw.get("duration"),
        nested(raw, "livestream", "duration"),
        nested(raw, "video", "duration"),
    )
    if duration_seconds is None or duration_seconds <= 0:
        raise KickChatReplayError("Kick VOD metadata did not include a duration")

    return KickVodChatMetadata(
        vod_uuid=vod_uuid,
        source_url=stream.url,
        channel_id=channel_id,
        chatroom_id=chatroom_id,
        stream_start=stream_start,
        duration_seconds=duration_seconds,
        title=str(raw.get("title") or nested(raw, "livestream", "session_title") or stream.title or ""),
    )


def fetch_kick_vod_metadata(
    vod_url: str,
    *,
    requester: JsonRequester | None = None,
) -> dict[str, Any]:
    vod_uuid = kick_vod_uuid_from_url(vod_url)
    if not vod_uuid:
        return {}

    requester = requester or request_kick_json
    last_error = ""
    for host in KICK_CHAT_HISTORY_ENDPOINT_HOSTS:
        url = f"{host}/api/v1/video/{quote(vod_uuid)}"
        try:
            payload = requester(url)
        except KickChatReplayError as exc:
            last_error = str(exc)
            continue
        if payload:
            return payload
    if last_error:
        LOGGER.debug("Unable to fetch Kick VOD metadata for %s: %s", vod_url, last_error)
    return {}


def fetch_kick_chat_history(
    metadata: KickVodChatMetadata,
    *,
    requester: JsonRequester | None = None,
    progress: ProgressCallback | None = None,
    sleep_seconds: float = 0.05,
) -> list[dict[str, Any]]:
    requester = requester or request_kick_json
    candidates = [
        candidate
        for candidate in (metadata.channel_id, metadata.chatroom_id)
        if candidate
    ]
    if not candidates:
        raise KickChatReplayError("Kick VOD metadata did not include chat history ids")

    last_error = ""
    for base_url in KICK_CHAT_HISTORY_ENDPOINT_HOSTS:
        for history_id in dict.fromkeys(candidates):
            try:
                messages = fetch_kick_chat_history_for_id(
                    metadata,
                    base_url,
                    history_id,
                    requester=requester,
                    progress=progress,
                    sleep_seconds=sleep_seconds,
                )
            except KickChatReplayError as exc:
                last_error = str(exc)
                continue
            if messages:
                return messages

    raise KickChatReplayError(last_error or "Kick chat replay was not available")


def fetch_kick_chat_history_for_id(
    metadata: KickVodChatMetadata,
    base_url: str,
    history_id: str,
    *,
    requester: JsonRequester,
    progress: ProgressCallback | None,
    sleep_seconds: float,
) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    seen: set[str] = set()
    total_steps = max(1, int(metadata.duration_seconds // KICK_CHAT_HISTORY_STEP_SECONDS) + 1)
    empty_pages = 0
    last_error = ""

    for step in range(total_steps):
        offset_seconds = step * KICK_CHAT_HISTORY_STEP_SECONDS
        start_time = metadata.stream_start.timestamp() + offset_seconds
        start_iso = datetime.fromtimestamp(start_time, tz=timezone.utc).isoformat().replace("+00:00", "Z")
        url = (
            f"{base_url}/api/v1/chat/{quote(str(history_id))}/history"
            f"?start_time={quote(start_iso, safe='')}"
        )
        try:
            payload = requester(url)
        except KickChatReplayError as exc:
            last_error = str(exc)
            if step == 0:
                raise
            empty_pages += 1
            if empty_pages >= 12:
                break
            continue

        page_messages = kick_history_messages(payload)
        if page_messages:
            empty_pages = 0
        else:
            empty_pages += 1

        for raw_message in page_messages:
            normalized = normalize_kick_chat_message(raw_message, metadata.stream_start)
            if normalized is None:
                continue
            message_id = normalized["id"]
            if message_id in seen:
                continue
            seen.add(message_id)
            messages.append(normalized)

        if progress is not None and step % 12 == 0:
            progress(
                f"Fetching Kick chat replay {min(offset_seconds, metadata.duration_seconds):.0f}s/"
                f"{metadata.duration_seconds:.0f}s",
                min(0.99, offset_seconds / metadata.duration_seconds),
            )
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    messages.sort(key=lambda message: int(message.get("offset_ms") or 0))
    if messages:
        return messages
    raise KickChatReplayError(last_error or "Kick chat replay returned no messages")


def kick_history_messages(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data")
    if isinstance(data, dict) and isinstance(data.get("messages"), list):
        return [item for item in data["messages"] if isinstance(item, dict)]
    if isinstance(payload.get("messages"), list):
        return [item for item in payload["messages"] if isinstance(item, dict)]
    return []


def normalize_kick_chat_message(
    raw_message: dict[str, Any],
    stream_start: datetime,
) -> dict[str, Any] | None:
    created_at = first_datetime(raw_message.get("created_at"), raw_message.get("timestamp"))
    if created_at is None:
        return None
    offset_ms = max(0, int(round((created_at.timestamp() - stream_start.timestamp()) * 1000)))
    content = str(raw_message.get("content") or raw_message.get("message") or "").strip()
    if not content:
        return None

    sender = raw_message.get("sender")
    sender_dict = sender if isinstance(sender, dict) else {}
    author = str(
        sender_dict.get("username")
        or sender_dict.get("slug")
        or raw_message.get("username")
        or raw_message.get("author")
        or "Unknown"
    ).strip() or "Unknown"
    message_id = str(
        raw_message.get("id")
        or raw_message.get("message_id")
        or f"{created_at.isoformat()}:{author}:{content}"
    )
    return {
        "id": message_id,
        "created_at": created_at.isoformat().replace("+00:00", "Z"),
        "offset_ms": offset_ms,
        "author": author,
        "message": content,
        "badges": list(raw_message.get("badges") or sender_dict.get("badges") or []),
        "emotes": normalized_kick_emotes(raw_message.get("emotes"), content),
        "raw": raw_message,
    }


def normalized_kick_emotes(raw_emotes: Any, content: str) -> list[dict[str, Any]]:
    emotes: list[dict[str, Any]] = []
    seen: set[str] = set()
    if isinstance(raw_emotes, list):
        for raw in raw_emotes:
            if not isinstance(raw, dict):
                continue
            emote_id = first_text(raw.get("id"), raw.get("emote_id"), raw.get("emoteId"))
            name = first_text(raw.get("name"), raw.get("text"), raw.get("code"))
            if not emote_id or emote_id in seen:
                continue
            seen.add(emote_id)
            emotes.append(
                {
                    "id": emote_id,
                    "name": name,
                    "image_url": kick_emote_image_url(emote_id),
                    "raw": raw,
                }
            )

    for match in KICK_EMOTE_RE.finditer(content):
        emote_id = match.group("id")
        if emote_id in seen:
            continue
        seen.add(emote_id)
        emotes.append(
            {
                "id": emote_id,
                "name": match.group("name").strip(),
                "image_url": kick_emote_image_url(emote_id),
            }
        )
    return emotes


def kick_emote_image_url(emote_id: str) -> str:
    return f"https://files.kick.com/emotes/{emote_id}/fullsize"


def write_kick_chat_sidecar(
    chat_file: Path,
    metadata: KickVodChatMetadata,
    messages: list[dict[str, Any]],
    *,
    stream: LiveStream,
) -> None:
    chat_file.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "platform": "kick",
        "source": stream.source or metadata.source_url,
        "video_id": stream.video_id,
        "vod_uuid": metadata.vod_uuid,
        "title": metadata.title or stream.title,
        "stream_start": metadata.stream_start.isoformat().replace("+00:00", "Z"),
        "duration_seconds": metadata.duration_seconds,
        "messages": messages,
    }
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=chat_file.parent,
        prefix=f".{chat_file.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        temp_path = Path(handle.name)
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    temp_path.replace(chat_file)


def request_kick_json(url: str) -> dict[str, Any]:
    request = Request(
        url,
        headers={
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://kick.com",
            "Referer": "https://kick.com/",
            "User-Agent": KICK_CHAT_USER_AGENT,
        },
    )
    try:
        with urlopen(request, timeout=30) as response:
            body = response.read()
    except HTTPError as exc:
        raise KickChatReplayError(f"Kick returned HTTP {exc.code}") from exc
    except URLError as exc:
        raise KickChatReplayError(str(exc.reason) or exc.__class__.__name__) from exc
    except OSError as exc:
        raise KickChatReplayError(str(exc) or exc.__class__.__name__) from exc

    try:
        parsed = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise KickChatReplayError("Kick returned invalid JSON") from exc
    if not isinstance(parsed, dict):
        raise KickChatReplayError("Kick returned an unexpected chat payload")
    if "message" in parsed and not parsed.get("data") and not parsed.get("messages"):
        message = str(parsed.get("message") or "Kick chat replay was not available").strip()
        raise KickChatReplayError(message or "Kick chat replay was not available")
    return parsed


def metadata_has_kick_chat_fields(raw: dict[str, Any]) -> bool:
    return (
        first_text(
            raw.get("channel_id"),
            nested(raw, "livestream", "channel_id"),
            nested(raw, "livestream", "channel", "id"),
            nested(raw, "channel", "id"),
        )
        and first_datetime(
            raw.get("start_time"),
            raw.get("actual_start_timestamp"),
            raw.get("start_timestamp"),
            raw.get("timestamp"),
            nested(raw, "livestream", "start_time"),
        )
        is not None
        and first_duration_seconds(
            raw.get("duration"),
            nested(raw, "livestream", "duration"),
            nested(raw, "video", "duration"),
        )
        is not None
    )


def merge_kick_metadata(primary: dict[str, Any], fallback: dict[str, Any]) -> dict[str, Any]:
    merged = dict(fallback)
    merged.update({key: value for key, value in primary.items() if value not in (None, "")})
    return merged


def kick_vod_uuid_from_url(url: str) -> str:
    path_parts = [part for part in urlsplit(url).path.split("/") if part]
    if "videos" in path_parts:
        index = path_parts.index("videos")
        if index + 1 < len(path_parts):
            return path_parts[index + 1]
    return ""


def vod_chat_sidecar_for_output_template(output_template: Path) -> Path:
    template = str(output_template)
    if "%(ext)s" in template:
        return Path(template.replace("%(ext)s", "live_chat.json"))
    return output_template.with_suffix(".live_chat.json")


def nested(value: dict[str, Any], *keys: str) -> Any:
    current: Any = value
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def first_text(*values: Any) -> str:
    for value in values:
        if value is None or isinstance(value, bool):
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def first_datetime(*values: Any) -> datetime | None:
    for value in values:
        parsed = parse_datetime(value)
        if parsed is not None:
            return parsed
    return None


def parse_datetime(value: Any) -> datetime | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        if value <= 0:
            return None
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            if stripped.isdigit():
                return datetime.fromtimestamp(float(stripped), tz=timezone.utc)
            parsed = datetime.fromisoformat(stripped.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return None


def first_duration_seconds(*values: Any) -> float | None:
    for value in values:
        duration = parse_duration_seconds(value)
        if duration is not None:
            return duration
    return None


def parse_duration_seconds(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        parsed = float(value)
    elif isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            parsed = float(stripped)
        except ValueError:
            return None
    else:
        return None
    if parsed <= 0:
        return None
    if parsed > 86_400:
        return parsed / 1000.0
    return parsed
