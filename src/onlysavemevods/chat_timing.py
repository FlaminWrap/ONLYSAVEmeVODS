from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
import json


CHAT_TIMING_SUFFIX = ".timing.json"
YOUTUBE_START_TIMESTAMP_KEYS = (
    "actual_start_timestamp",
    "live_start_timestamp",
    "start_timestamp",
    "release_timestamp",
    "timestamp",
)


@dataclass(frozen=True, slots=True)
class ChatTiming:
    video_id: str
    segment_index: int
    stream_started_at: str | None = None
    media_started_at: str | None = None
    chat_started_at: str | None = None
    media_live_from_start: bool = True
    last_exit_at: str | None = None
    updated_at: str | None = None


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def is_chat_timing_file(name: str) -> bool:
    return name.endswith(CHAT_TIMING_SUFFIX)


def chat_timing_file_for_chat_file(chat_file: Path) -> Path:
    if chat_file.name.endswith(".live_chat.json"):
        stem = chat_file.name.removesuffix(".live_chat.json")
    else:
        stem = chat_file.stem
    return chat_file.with_name(f"{stem}{CHAT_TIMING_SUFFIX}")


def read_chat_timing(path: Path | None) -> ChatTiming | None:
    if path is None or not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None

    video_id = str(payload.get("video_id") or "")
    if not video_id:
        return None
    segment_index = coerce_int(payload.get("segment_index")) or 1
    return ChatTiming(
        video_id=video_id,
        segment_index=segment_index,
        stream_started_at=coerce_str(payload.get("stream_started_at")),
        media_started_at=coerce_str(payload.get("media_started_at")),
        chat_started_at=coerce_str(payload.get("chat_started_at")),
        media_live_from_start=coerce_bool(payload.get("media_live_from_start"), True),
        last_exit_at=coerce_str(payload.get("last_exit_at")),
        updated_at=coerce_str(payload.get("updated_at")),
    )


def write_chat_timing(path: Path, timing: ChatTiming) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        key: value
        for key, value in asdict(timing).items()
        if value is not None
    }
    replacement = path.with_name(f"{path.name}.writing")
    replacement.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    replacement.replace(path)


def update_chat_timing(path: Path, **changes: Any) -> ChatTiming:
    existing = read_chat_timing(path)
    payload = asdict(existing) if existing is not None else {}
    payload.update(changes)
    payload["updated_at"] = utc_now_iso()
    timing = ChatTiming(
        video_id=str(payload.get("video_id") or ""),
        segment_index=coerce_int(payload.get("segment_index")) or 1,
        stream_started_at=coerce_str(payload.get("stream_started_at")),
        media_started_at=coerce_str(payload.get("media_started_at")),
        chat_started_at=coerce_str(payload.get("chat_started_at")),
        media_live_from_start=coerce_bool(payload.get("media_live_from_start"), True),
        last_exit_at=coerce_str(payload.get("last_exit_at")),
        updated_at=coerce_str(payload.get("updated_at")),
    )
    write_chat_timing(path, timing)
    return timing


def stream_start_timestamp_us(metadata: Mapping[str, Any]) -> int | None:
    for key in YOUTUBE_START_TIMESTAMP_KEYS:
        timestamp = timestamp_value_to_us(metadata.get(key))
        if timestamp is not None:
            return timestamp
    return None


def stream_start_iso(metadata: Mapping[str, Any]) -> str | None:
    timestamp_us = stream_start_timestamp_us(metadata)
    if timestamp_us is None:
        return None
    return datetime.fromtimestamp(timestamp_us / 1_000_000, tz=timezone.utc).isoformat()


def iso_timestamp_to_us(value: str | None) -> int | None:
    if not value:
        return None
    try:
        normalized = value.replace("Z", "+00:00")
        return round(datetime.fromisoformat(normalized).timestamp() * 1_000_000)
    except ValueError:
        return None


def timestamp_value_to_us(value: Any) -> int | None:
    number = coerce_float(value)
    if number is not None and number > 0:
        if number > 10_000_000_000_000:
            return round(number)
        if number > 10_000_000_000:
            return round(number * 1000)
        return round(number * 1_000_000)
    if isinstance(value, str) and value:
        return iso_timestamp_to_us(value)
    return None


def coerce_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text or None


def coerce_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return round(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def coerce_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    return default
