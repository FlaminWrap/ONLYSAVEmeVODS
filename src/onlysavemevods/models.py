from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True, slots=True)
class LiveStream:
    video_id: str
    url: str
    title: str = ""
    channel: str = ""
    live_status: str = ""
    is_live: bool = True
    platform: str = "youtube"
    source: str = ""
    raw: Mapping[str, Any] = field(default_factory=dict, compare=False)


def video_url(video_id: str) -> str:
    if video_id.startswith("youtube:"):
        video_id = video_id.split(":", 1)[1]
    return f"https://www.youtube.com/watch?v={video_id}"


def qualified_stream_id(platform: str, raw_id: str) -> str:
    normalized_platform = platform.strip().casefold() or "unknown"
    raw = raw_id.strip()
    if raw.startswith(f"{normalized_platform}:"):
        return raw
    return f"{normalized_platform}:{raw}"


def unqualified_stream_id(video_id: str, platform: str = "youtube") -> str:
    prefix = f"{platform.strip().casefold()}:"
    if video_id.startswith(prefix):
        return video_id.split(":", 1)[1]
    return video_id
