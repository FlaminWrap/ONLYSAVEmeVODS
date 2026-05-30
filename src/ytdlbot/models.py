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
    raw: Mapping[str, Any] = field(default_factory=dict, compare=False)


def video_url(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}"
