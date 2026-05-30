from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Iterator, Sequence
from urllib.error import URLError
from urllib.request import urlopen
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import hashlib
import json
import logging
import os
import re
import shutil
import shlex
import subprocess
import tempfile
import textwrap
import time
from concurrent.futures import ProcessPoolExecutor, as_completed


CHAT_VIDEO_WIDTH = 1920
CHAT_VIDEO_HEIGHT = 1080
CHAT_MEDIA_WIDTH = 1440
CHAT_PANEL_WIDTH = 480
CHAT_PANEL_X = CHAT_MEDIA_WIDTH + 20
CHAT_ROW_TOP = 82
CHAT_ROW_HEIGHT = 94
CHAT_ROW_COUNT = 10
CHAT_FINAL_EVENT_PADDING_SECONDS = 7 * 24 * 60 * 60
CHAT_WRAP_WIDTH = 34
CHAT_MAX_MESSAGE_LENGTH = 280
CHAT_MESSAGE_MAX_LINES = 6

CHAT_RENDERER_KEYS = {
    "liveChatMembershipItemRenderer",
    "liveChatPaidMessageRenderer",
    "liveChatPaidStickerRenderer",
    "liveChatSponsorshipsGiftPurchaseAnnouncementRenderer",
    "liveChatTextMessageRenderer",
}

YOUTUBE_EMOJI_FALLBACKS = {
    ":body-blue-raised-arms:": "🙌",
    ":body-green-covering-eyes:": "🙈",
    ":buffering:": "⏳",
    ":dothefive:": "✋",
    ":elbowbump:": "💪",
    ":elbowcough:": "🤧",
    ":eyes-pink-heart-shape:": "😍",
    ":eyes-purple-crying:": "😭",
    ":face-blue-smiling:": "🙂",
    ":face-blue-wide-eyes:": "😳",
    ":face-fuchsia-tongue-out:": "😜",
    ":face-green-smiling:": "😊",
    ":face-orange-biting-nails:": "😬",
    ":face-orange-raised-eyebrow:": "🤨",
    ":face-pink-tears:": "😂",
    ":face-purple-crying:": "😭",
    ":face-red-droopy-eyes:": "🥴",
    ":fire:": "🔥",
    ":goodvibes:": "✨",
    ":hand-orange-covering-eyes:": "🙈",
    ":hand-pink-waving:": "👋",
    ":hydrate:": "💧",
    ":mushroom:": "🍄",
    ":oops:": "😅",
    ":popcorn-yellow-striped-smile:": "🍿",
    ":stayhome:": "🏠",
    ":thanksdoc:": "🩺",
    ":trophy-yellow-smiling:": "🏆",
    ":videocall:": "📹",
    ":virtualhug:": "🤗",
    ":washhands:": "🧼",
    ":wave:": "👋",
    ":yougotthis:": "💪",
    ":yt:": "▶️",
}

YOUTUBE_CUSTOM_EMOJI_LABELS = {
    ":body-blue-raised-arms:": "[raised arms]",
    ":body-green-covering-eyes:": "[peek]",
    ":eyes-pink-heart-shape:": "[love]",
    ":eyes-purple-crying:": "[crying]",
    ":face-blue-smiling:": "[smile]",
    ":face-blue-wide-eyes:": "[shocked]",
    ":face-fuchsia-tongue-out:": "[silly]",
    ":face-green-smiling:": "[smile]",
    ":face-orange-biting-nails:": "[nervous]",
    ":face-orange-raised-eyebrow:": "[hmm]",
    ":face-pink-tears:": "[laughing]",
    ":face-purple-crying:": "[crying]",
    ":face-red-droopy-eyes:": "[bruh]",
    ":hand-orange-covering-eyes:": "[peek]",
    ":hand-pink-waving:": "[wave]",
    ":popcorn-yellow-striped-smile:": "[popcorn]",
    ":trophy-yellow-smiling:": "[trophy]",
}

ASS_CHAT_TEXT_COLOR = "&HFAF6F3&"
ASS_EMOJI_COLORS = {
    "▶️": "&HFF3DFF&",
    "🔥": "&H187AFF&",
    "🍄": "&H5C5CEF&",
    "🏆": "&H2BC4FF&",
    "✨": "&H3CD8FF&",
    "💧": "&HFFB864&",
    "💪": "&H65B8F4&",
    "🙌": "&H65B8F4&",
    "👋": "&H65B8F4&",
    "🧼": "&HEDD6A8&",
}

CHAT_PANEL_BACKGROUND = (17, 24, 32)
CHAT_PANEL_HEADER = (17, 24, 32)
CHAT_PANEL_SEPARATOR = (74, 86, 96)
CHAT_TEXT_FILL = (243, 246, 250)
CHAT_AUTHOR_FILL = (248, 249, 252)
CHAT_AUTHOR_STROKE_FILL = (9, 13, 18)
CHAT_AUTHOR_COLORS = (
    (255, 184, 108),
    (126, 231, 135),
    (107, 203, 255),
    (255, 123, 172),
    (196, 181, 253),
    (255, 214, 102),
    (95, 223, 211),
    (255, 154, 130),
    (167, 243, 208),
    (147, 197, 253),
    (253, 186, 116),
    (244, 114, 182),
    (190, 242, 100),
    (250, 204, 21),
    (129, 230, 217),
    (216, 180, 254),
    (252, 165, 165),
    (134, 239, 172),
    (125, 211, 252),
    (251, 146, 60),
    (232, 121, 249),
    (192, 132, 252),
    (94, 234, 212),
    (253, 224, 71),
    (163, 230, 53),
    (110, 231, 183),
    (103, 232, 249),
    (165, 180, 252),
    (249, 168, 212),
    (253, 164, 175),
    (251, 191, 36),
    (74, 222, 128),
    (45, 212, 191),
    (56, 189, 248),
    (129, 140, 248),
    (245, 158, 11),
    (255, 153, 153),
    (255, 128, 128),
)
CHAT_PANEL_FPS = 30
DEFAULT_CHAT_RENDER_TIMEOUT_SECONDS = 60 * 60
KIRKLAND_TIME_ZONE = "America/Los_Angeles"


LOGGER = logging.getLogger(__name__)
_CPU_COUNT_UNSET = object()


class VideoProbeError(RuntimeError):
    pass


class ChatPanelRenderError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ChatToken:
    text: str
    image_url: str = ""
    image_key: str = ""
    is_emoji: bool = False


@dataclass(frozen=True, slots=True)
class ChatEntry:
    offset_seconds: float
    author: str
    message: str
    tokens: tuple[ChatToken, ...] = ()
    timestamp_us: int | None = None


@dataclass(frozen=True, slots=True)
class VideoDimensions:
    width: int
    height: int


@dataclass(frozen=True, slots=True)
class NvencEnvironment:
    nvidia_devices: list[str]
    ffmpeg_has_h264_nvenc: bool


@dataclass(frozen=True, slots=True)
class ChatLayout:
    video_width: int
    video_height: int
    panel_width: int
    output_width: int
    output_height: int
    panel_padding_x: int
    panel_x: int
    title_y: int
    title_font_size: int
    row_top: int
    row_height: int
    row_count: int
    font_size: int
    wrap_width: int


@dataclass(frozen=True, slots=True)
class RawChatEntry:
    offset_ms: int | None
    timestamp_us: int | None
    author: str
    message: str
    tokens: tuple[ChatToken, ...] = ()


@dataclass(frozen=True, slots=True)
class ChatLineItem:
    text: str = ""
    image_url: str = ""
    image_key: str = ""
    is_image: bool = False


@dataclass(frozen=True, slots=True)
class ChatPanelFonts:
    regular: Any
    bold: Any


@dataclass(frozen=True, slots=True)
class ChatPanelFrameJob:
    index: int
    start: float
    end: float
    path: Path
    stack: list[tuple[ChatEntry, int]]
    header_time_text: str
    duration: float


_CHAT_PANEL_WORKER_LAYOUT: ChatLayout | None = None
_CHAT_PANEL_WORKER_FONTS: ChatPanelFonts | None = None
_CHAT_PANEL_WORKER_CACHE: "EmojiImageCache | None" = None


def probe_video_dimensions(
    media_file: Path,
    ffprobe_path: str = "ffprobe",
) -> VideoDimensions:
    command = [
        ffprobe_path,
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=width,height",
        "-of",
        "json",
        str(media_file),
    ]
    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
        )
        payload = json.loads(result.stdout)
        stream = payload["streams"][0]
        width = int(stream["width"])
        height = int(stream["height"])
    except (OSError, subprocess.SubprocessError, KeyError, IndexError, ValueError, TypeError) as exc:
        raise VideoProbeError(f"Unable to probe video dimensions for {media_file}") from exc

    if width <= 0 or height <= 0:
        raise VideoProbeError(f"Invalid video dimensions for {media_file}: {width}x{height}")
    return VideoDimensions(width=width, height=height)


def probe_video_duration(
    media_file: Path,
    ffprobe_path: str = "ffprobe",
) -> float:
    command = [
        ffprobe_path,
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=nk=1:nw=1",
        str(media_file),
    ]
    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
        )
        duration = float(result.stdout.strip())
    except (OSError, subprocess.SubprocessError, ValueError) as exc:
        raise VideoProbeError(f"Unable to probe video duration for {media_file}") from exc

    if duration <= 0:
        raise VideoProbeError(f"Invalid video duration for {media_file}: {duration}")
    return duration


def ffprobe_path_for(ffmpeg_path: str) -> str:
    path = Path(ffmpeg_path)
    return str(path.with_name("ffprobe"))


def default_chat_layout() -> ChatLayout:
    return ChatLayout(
        video_width=CHAT_MEDIA_WIDTH,
        video_height=CHAT_VIDEO_HEIGHT,
        panel_width=CHAT_PANEL_WIDTH,
        output_width=CHAT_VIDEO_WIDTH,
        output_height=CHAT_VIDEO_HEIGHT,
        panel_padding_x=20,
        panel_x=CHAT_PANEL_X,
        title_y=20,
        title_font_size=32,
        row_top=CHAT_ROW_TOP,
        row_height=CHAT_ROW_HEIGHT,
        row_count=CHAT_ROW_COUNT,
        font_size=22,
        wrap_width=CHAT_WRAP_WIDTH,
    )


def chat_layout_for_video(
    width: int,
    height: int,
    panel_width: int = CHAT_PANEL_WIDTH,
) -> ChatLayout:
    video_width = make_even(width)
    video_height = make_even(height)
    panel_width = make_even(panel_width)
    scale = min(1.25, max(0.65, video_height / CHAT_VIDEO_HEIGHT))
    padding_x = max(14, round(20 * scale))
    title_y = max(14, round(20 * scale))
    title_font_size = max(22, round(32 * scale))
    font_size = max(16, round(22 * scale))
    row_top = max(
        title_y + title_font_size + max(22, round(26 * scale)),
        round(CHAT_ROW_TOP * scale),
    )
    row_height = max(
        font_size * 3 + max(10, round(18 * scale)),
        round(CHAT_ROW_HEIGHT * scale),
    )
    bottom_padding = max(14, round(20 * scale))
    row_count = max(1, (video_height - row_top - bottom_padding) // row_height)
    available_text_width = max(1, panel_width - padding_x * 2)
    wrap_width = min(
        42,
        max(18, int(available_text_width / max(1, font_size * 0.58))),
    )

    return ChatLayout(
        video_width=video_width,
        video_height=video_height,
        panel_width=panel_width,
        output_width=video_width + panel_width,
        output_height=video_height,
        panel_padding_x=padding_x,
        panel_x=video_width + padding_x,
        title_y=title_y,
        title_font_size=title_font_size,
        row_top=row_top,
        row_height=row_height,
        row_count=row_count,
        font_size=font_size,
        wrap_width=wrap_width,
    )


def make_even(value: int) -> int:
    return value if value % 2 == 0 else value + 1


def parse_live_chat_file(path: Path) -> list[ChatEntry]:
    raw_entries: list[RawChatEntry] = []
    for item in iter_live_chat_json_objects(path):
        collect_chat_entries(item, raw_entries)

    timestamps = [
        entry.timestamp_us
        for entry in raw_entries
        if entry.offset_ms is None and entry.timestamp_us is not None
    ]
    first_timestamp = min(timestamps, default=None)
    entries: list[ChatEntry] = []
    seen: set[tuple[int, str, str]] = set()
    for entry in raw_entries:
        if entry.offset_ms is not None:
            offset_seconds = max(0.0, entry.offset_ms / 1000)
        elif entry.timestamp_us is not None and first_timestamp is not None:
            offset_seconds = max(0.0, (entry.timestamp_us - first_timestamp) / 1_000_000)
        else:
            offset_seconds = 0.0

        author = clean_chat_text(entry.author) or "Unknown"
        message = clean_chat_text(entry.message)
        if not message:
            continue
        tokens = clean_chat_tokens(entry.tokens, message)

        key = (round(offset_seconds * 100), author, message)
        if key in seen:
            continue
        seen.add(key)
        entries.append(
            ChatEntry(
                offset_seconds=offset_seconds,
                author=author,
                message=message,
                tokens=tokens,
                timestamp_us=entry.timestamp_us,
            )
        )

    return sorted(entries, key=lambda entry: entry.offset_seconds)


def iter_live_chat_json_objects(path: Path) -> Iterator[Any]:
    parsed_lines = False
    with path.open("r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
                parsed_lines = True
            except json.JSONDecodeError:
                if parsed_lines:
                    continue
                break
        else:
            return

    content = path.read_text(encoding="utf-8")
    if not content.strip():
        return
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        return
    if isinstance(parsed, list):
        yield from parsed
    else:
        yield parsed


def collect_chat_entries(
    node: Any,
    entries: list[RawChatEntry],
    offset_ms: int | None = None,
    timestamp_us: int | None = None,
) -> None:
    if isinstance(node, list):
        for item in node:
            collect_chat_entries(item, entries, offset_ms, timestamp_us)
        return

    if not isinstance(node, dict):
        return

    current_offset = coerce_int(node.get("videoOffsetTimeMsec"), offset_ms)
    current_timestamp = coerce_int(node.get("timestampUsec"), timestamp_us)

    for key in CHAT_RENDERER_KEYS:
        renderer = node.get(key)
        if not isinstance(renderer, dict):
            continue
        extracted = extract_renderer_text(renderer)
        if extracted is None:
            continue
        renderer_offset = coerce_int(renderer.get("videoOffsetTimeMsec"), current_offset)
        renderer_timestamp = coerce_int(renderer.get("timestampUsec"), current_timestamp)
        author, message, tokens = extracted
        entries.append(
            RawChatEntry(
                offset_ms=renderer_offset,
                timestamp_us=renderer_timestamp,
                author=author,
                message=message,
                tokens=tokens,
            )
        )

    for value in node.values():
        collect_chat_entries(value, entries, current_offset, current_timestamp)


def extract_renderer_text(renderer: dict[str, Any]) -> tuple[str, str, tuple[ChatToken, ...]] | None:
    author = text_from_node(renderer.get("authorName"))
    message_node = renderer.get("message")
    tokens = tuple(tokens_from_node(message_node))
    message = "".join(token.text for token in tokens) if tokens else text_from_node(message_node)
    purchase = text_from_node(renderer.get("purchaseAmountText"))

    if not message:
        tokens = tuple(tokens_from_node(renderer.get("headerPrimaryText")))
        message = (
            "".join(token.text for token in tokens)
            if tokens
            else text_from_node(renderer.get("headerPrimaryText"))
        )
    if not message:
        tokens = tuple(tokens_from_node(renderer.get("headerSubtext")))
        message = (
            "".join(token.text for token in tokens)
            if tokens
            else text_from_node(renderer.get("headerSubtext"))
        )
    if purchase:
        message = f"{purchase} {message}".strip()
        tokens = (ChatToken(f"{purchase} "), *tokens) if tokens else (ChatToken(message),)
    if not author and not message:
        return None
    return author, message, tokens


def text_from_node(node: Any) -> str:
    if node is None:
        return ""
    if isinstance(node, str):
        return node
    if isinstance(node, list):
        return "".join(text_from_node(item) for item in node)
    if not isinstance(node, dict):
        return ""
    if isinstance(node.get("simpleText"), str):
        return node["simpleText"]
    if isinstance(node.get("text"), str):
        return node["text"]
    if isinstance(node.get("runs"), list):
        return "".join(text_from_node(run) for run in node["runs"])
    emoji = node.get("emoji")
    if isinstance(emoji, dict):
        return text_from_emoji_node(emoji)
    return ""


def tokens_from_node(node: Any) -> list[ChatToken]:
    if node is None:
        return []
    if isinstance(node, str):
        return [ChatToken(node)]
    if isinstance(node, list):
        tokens: list[ChatToken] = []
        for item in node:
            tokens.extend(tokens_from_node(item))
        return tokens
    if not isinstance(node, dict):
        return []
    if isinstance(node.get("simpleText"), str):
        return [ChatToken(node["simpleText"])]
    if isinstance(node.get("text"), str):
        return [ChatToken(node["text"])]
    if isinstance(node.get("runs"), list):
        tokens: list[ChatToken] = []
        for run in node["runs"]:
            tokens.extend(tokens_from_node(run))
        return tokens
    emoji = node.get("emoji")
    if isinstance(emoji, dict):
        return [token_from_emoji_node(emoji)]
    return []


def token_from_emoji_node(emoji: dict[str, Any]) -> ChatToken:
    text = text_from_emoji_node(emoji)
    return ChatToken(
        text=text,
        image_url=best_emoji_thumbnail_url(emoji),
        image_key=emoji_key(emoji),
        is_emoji=True,
    )


def best_emoji_thumbnail_url(emoji: dict[str, Any]) -> str:
    image = emoji.get("image")
    thumbnails = image.get("thumbnails") if isinstance(image, dict) else None
    if not isinstance(thumbnails, list):
        return ""

    best_url = ""
    best_width = -1
    for thumbnail in thumbnails:
        if not isinstance(thumbnail, dict):
            continue
        url = thumbnail.get("url")
        if not isinstance(url, str) or not url:
            continue
        width = coerce_int(thumbnail.get("width"), 0) or 0
        if width > best_width:
            best_url = url
            best_width = width
    return best_url


def emoji_key(emoji: dict[str, Any]) -> str:
    emoji_id = emoji.get("emojiId")
    if isinstance(emoji_id, str) and emoji_id:
        return emoji_id
    shortcuts = emoji.get("shortcuts")
    if isinstance(shortcuts, list) and shortcuts:
        return str(shortcuts[0])
    return emoji_accessibility_label(emoji)


def text_from_emoji_node(emoji: dict[str, Any]) -> str:
    if is_youtube_custom_emoji(emoji):
        return emoji_text_fragment(youtube_custom_emoji_label(emoji))

    emoji_id = emoji.get("emojiId")
    if isinstance(emoji_id, str) and is_unicode_emoji_id(emoji_id):
        return emoji_text_fragment(emoji_id)

    for value in emoji_text_candidates(emoji):
        fallback = youtube_emoji_fallback(value)
        if fallback:
            return emoji_text_fragment(fallback)

    shortcuts = emoji.get("shortcuts")
    if isinstance(shortcuts, list) and shortcuts:
        return str(shortcuts[0])

    label = emoji_accessibility_label(emoji)
    return str(label) if label else ""


def emoji_text_fragment(value: str) -> str:
    return f" {value} "


def is_youtube_custom_emoji(emoji: dict[str, Any]) -> bool:
    if emoji.get("isCustomEmoji") is True:
        return True
    emoji_id = emoji.get("emojiId")
    return isinstance(emoji_id, str) and (emoji_id.startswith("UC") or "/" in emoji_id)


def youtube_custom_emoji_label(emoji: dict[str, Any]) -> str:
    for value in emoji_text_candidates(emoji):
        key = f":{value.strip().strip(':')}:"
        if key in YOUTUBE_CUSTOM_EMOJI_LABELS:
            return YOUTUBE_CUSTOM_EMOJI_LABELS[key]

    label = emoji_accessibility_label(emoji)
    if not label:
        shortcuts = emoji.get("shortcuts")
        if isinstance(shortcuts, list) and shortcuts:
            label = str(shortcuts[0])

    label = label.strip().strip(":").replace("-", " ")
    return f"[{label}]" if label else "[emoji]"


def emoji_text_candidates(emoji: dict[str, Any]) -> Iterator[str]:
    shortcuts = emoji.get("shortcuts")
    if isinstance(shortcuts, list):
        for shortcut in shortcuts:
            if isinstance(shortcut, str):
                yield shortcut

    search_terms = emoji.get("searchTerms")
    if isinstance(search_terms, list):
        for term in search_terms:
            if isinstance(term, str):
                yield term

    label = emoji_accessibility_label(emoji)
    if label:
        yield label


def emoji_accessibility_label(emoji: dict[str, Any]) -> str:
    image = emoji.get("image")
    accessibility = image.get("accessibility") if isinstance(image, dict) else {}
    accessibility_data = (
        accessibility.get("accessibilityData")
        if isinstance(accessibility, dict)
        else {}
    )
    label = (
        accessibility_data.get("label")
        if isinstance(accessibility_data, dict)
        else ""
    )
    return str(label) if label else ""


def is_unicode_emoji_id(value: str) -> bool:
    return "/" not in value and not value.startswith("UC") and any(
        ord(character) > 127 for character in value
    )


def youtube_emoji_fallback(value: str) -> str:
    key = value.strip()
    if key in YOUTUBE_EMOJI_FALLBACKS:
        return YOUTUBE_EMOJI_FALLBACKS[key]
    shortcode_key = f":{key.strip(':')}:"
    return YOUTUBE_EMOJI_FALLBACKS.get(shortcode_key, "")


def coerce_int(value: Any, fallback: int | None = None) -> int | None:
    if isinstance(value, bool):
        return fallback
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return fallback
    return fallback


def clean_chat_text(value: str) -> str:
    cleaned = " ".join(value.replace("\r", " ").replace("\n", " ").split())
    if len(cleaned) > CHAT_MAX_MESSAGE_LENGTH:
        cleaned = cleaned[: CHAT_MAX_MESSAGE_LENGTH - 3].rstrip() + "..."
    return cleaned


def clean_chat_tokens(
    tokens: tuple[ChatToken, ...],
    fallback_message: str,
) -> tuple[ChatToken, ...]:
    if not tokens:
        return (ChatToken(fallback_message),)

    cleaned: list[ChatToken] = []
    for token in tokens:
        text = token.text.replace("\r", " ").replace("\n", " ")
        if not text:
            continue
        cleaned.append(
            ChatToken(
                text=text,
                image_url=token.image_url,
                image_key=token.image_key,
                is_emoji=token.is_emoji,
            )
        )

    if not cleaned:
        return (ChatToken(fallback_message),)
    if len("".join(token.text for token in cleaned)) > CHAT_MAX_MESSAGE_LENGTH:
        return (ChatToken(fallback_message),)
    return tuple(cleaned)


def render_chat_panel_video(
    entries: list[ChatEntry],
    layout: ChatLayout,
    output_file: Path,
    duration_seconds: float,
    ffmpeg_path: str = "ffmpeg",
    cache_dir: Path | None = None,
    panel_workers: int = 0,
    use_nvenc: bool = False,
    nvenc_device: str = "",
) -> bool:
    try:
        from PIL import Image  # noqa: F401
    except ImportError as exc:
        raise ChatPanelRenderError("Pillow is required for image chat rendering") from exc

    if duration_seconds <= 0:
        raise ChatPanelRenderError("Chat panel duration must be positive")

    started_at = time.monotonic()
    resolved_workers = resolve_chat_render_panel_workers(panel_workers)
    LOGGER.info(
        "Rendering chat panel video output=%s entries=%d duration=%.2fs "
        "panel=%sx%s configured_workers=%d resolved_workers=%d encoder=%s "
        "nvenc_device=%s",
        output_file,
        len(entries),
        duration_seconds,
        layout.panel_width,
        layout.video_height,
        panel_workers,
        resolved_workers,
        chat_render_video_encoder_name(use_nvenc),
        nvenc_device or "default",
    )
    resolved_cache_dir = cache_dir or output_file.parent / ".emoji-cache"
    cache = EmojiImageCache(resolved_cache_dir)
    fonts = load_chat_panel_fonts(layout)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.unlink(missing_ok=True)

    with tempfile.TemporaryDirectory(
        prefix=f"{output_file.stem}.frames.",
        dir=str(output_file.parent),
    ) as tmp:
        frame_dir = Path(tmp)
        clock_origin_us = chat_clock_origin_us(entries)
        segments = chat_panel_segments(entries, duration_seconds, layout, fonts)
        base_segment_count = len(segments)
        if clock_origin_us is not None:
            segments = split_chat_panel_segments_by_clock_minutes(
                segments,
                clock_origin_us,
            )
        if not segments:
            segments = [(0.0, duration_seconds, [])]
        LOGGER.info(
            "Prepared chat panel frames output=%s frames=%d chat_segments=%d "
            "clock=%s resolved_workers=%d",
            output_file,
            len(segments),
            base_segment_count,
            "yes" if clock_origin_us is not None else "no",
            resolved_workers,
        )

        frame_jobs = chat_panel_frame_jobs(segments, frame_dir, clock_origin_us)
        if resolved_workers > 1 and len(frame_jobs) > 1:
            prewarmed = prewarm_emoji_cache(entries, cache)
            LOGGER.debug(
                "Prewarmed chat emoji cache output=%s images=%d cache_dir=%s",
                output_file,
                prewarmed,
                resolved_cache_dir,
            )
        render_chat_panel_frame_jobs(
            frame_jobs,
            layout,
            fonts,
            cache,
            resolved_cache_dir,
            resolved_workers,
            output_file,
        )

        concat_file = frame_dir / "frames.txt"
        with concat_file.open("w", encoding="utf-8") as file:
            for job in frame_jobs:
                file.write(f"file '{escape_concat_path(job.path)}'\n")
                file.write(f"duration {job.duration:.3f}\n")
            file.write(f"file '{escape_concat_path(frame_jobs[-1].path)}'\n")

        command = [
            ffmpeg_path,
            "-hide_banner",
            "-loglevel",
            "warning",
            "-y",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(concat_file),
            "-vf",
            "format=yuv420p",
            "-r",
            str(CHAT_PANEL_FPS),
            *chat_render_video_encoder_args(
                use_nvenc=use_nvenc,
                quality=18,
                nvenc_device=nvenc_device,
            ),
            str(output_file),
        ]
        LOGGER.debug("ffmpeg chat panel command: %s", shlex.join(command))
        ffmpeg_started_at = time.monotonic()
        result = subprocess.run(command, capture_output=True)
        ffmpeg_elapsed = time.monotonic() - ffmpeg_started_at
        if result.returncode != 0:
            output_file.unlink(missing_ok=True)
            message = (result.stderr or result.stdout).decode("utf-8", "replace").strip()
            raise ChatPanelRenderError(f"ffmpeg failed while rendering chat panel: {message}")
        LOGGER.info(
            "Rendered chat panel video output=%s frames=%d elapsed=%.1fs "
            "ffmpeg_elapsed=%.1fs",
            output_file,
            len(frame_jobs),
            time.monotonic() - started_at,
            ffmpeg_elapsed,
        )

    return True


def resolve_chat_render_panel_workers(
    config_value: int,
    cpu_count: int | None | object = _CPU_COUNT_UNSET,
) -> int:
    if config_value < 0:
        raise ValueError("chat render panel workers must be non-negative")
    if config_value > 0:
        return config_value
    detected = os.cpu_count() if cpu_count is _CPU_COUNT_UNSET else cpu_count
    return max(1, detected or 1)


def chat_panel_frame_jobs(
    segments: list[tuple[float, float, list[tuple[ChatEntry, int]]]],
    frame_dir: Path,
    clock_origin_us: int | None,
) -> list[ChatPanelFrameJob]:
    jobs: list[ChatPanelFrameJob] = []
    for index, (start, end, stack) in enumerate(segments):
        next_start = segments[index + 1][0] if index + 1 < len(segments) else end
        duration = max(0.001, end - start)
        if index + 1 < len(segments):
            duration = max(0.001, next_start - start)
        jobs.append(
            ChatPanelFrameJob(
                index=index,
                start=start,
                end=end,
                path=frame_dir / f"frame-{index:06d}.png",
                stack=stack,
                header_time_text=kirkland_time_for_offset(clock_origin_us, start),
                duration=duration,
            )
        )
    return jobs


def render_chat_panel_frame_jobs(
    jobs: list[ChatPanelFrameJob],
    layout: ChatLayout,
    fonts: ChatPanelFonts,
    cache: "EmojiImageCache",
    cache_dir: Path,
    workers: int,
    output_file: Path,
) -> None:
    if workers <= 1 or len(jobs) <= 1:
        for job in jobs:
            render_chat_panel_frame_job(job, layout, fonts, cache)
            log_chat_panel_frame_progress(job, len(jobs), output_file)
        return

    LOGGER.info(
        "Rendering chat panel frames in parallel output=%s frames=%d workers=%d",
        output_file,
        len(jobs),
        workers,
    )
    try:
        render_chat_panel_frame_jobs_parallel(jobs, layout, cache_dir, workers, output_file)
    except Exception as exc:
        LOGGER.warning(
            "Parallel chat panel frame rendering failed; retrying serial output=%s "
            "workers=%d error=%s",
            output_file,
            workers,
            exc,
        )
        try:
            for job in jobs:
                render_chat_panel_frame_job(job, layout, fonts, cache)
                log_chat_panel_frame_progress(job, len(jobs), output_file)
        except Exception as serial_exc:
            raise ChatPanelRenderError("Unable to render chat panel frames") from serial_exc


def render_chat_panel_frame_jobs_parallel(
    jobs: list[ChatPanelFrameJob],
    layout: ChatLayout,
    cache_dir: Path,
    workers: int,
    output_file: Path,
) -> None:
    with ProcessPoolExecutor(
        max_workers=workers,
        initializer=init_chat_panel_frame_worker,
        initargs=(layout, cache_dir),
    ) as executor:
        futures = [
            executor.submit(render_chat_panel_frame_job_in_worker, job)
            for job in jobs
        ]
        completed = 0
        for future in as_completed(futures):
            index = future.result()
            completed += 1
            if completed == 1 or completed % 100 == 0 or completed == len(jobs):
                LOGGER.debug(
                    "Rendered chat panel frame %d/%d output=%s latest_index=%d",
                    completed,
                    len(jobs),
                    output_file,
                    index + 1,
                )


def render_chat_panel_frame_job(
    job: ChatPanelFrameJob,
    layout: ChatLayout,
    fonts: ChatPanelFonts,
    cache: "EmojiImageCache",
) -> None:
    image = render_chat_panel_frame(
        job.stack,
        layout,
        fonts,
        cache,
        job.header_time_text,
    )
    image.save(job.path)


def log_chat_panel_frame_progress(
    job: ChatPanelFrameJob,
    total: int,
    output_file: Path,
) -> None:
    if job.index == 0 or (job.index + 1) % 100 == 0 or job.index + 1 == total:
        LOGGER.debug(
            "Rendered chat panel frame %d/%d output=%s "
            "window=%.2f-%.2f visible_entries=%d",
            job.index + 1,
            total,
            output_file,
            job.start,
            job.end,
            len(job.stack),
        )


def init_chat_panel_frame_worker(layout: ChatLayout, cache_dir: Path) -> None:
    global _CHAT_PANEL_WORKER_LAYOUT, _CHAT_PANEL_WORKER_FONTS, _CHAT_PANEL_WORKER_CACHE
    _CHAT_PANEL_WORKER_LAYOUT = layout
    _CHAT_PANEL_WORKER_FONTS = load_chat_panel_fonts(layout)
    _CHAT_PANEL_WORKER_CACHE = EmojiImageCache(cache_dir)


def render_chat_panel_frame_job_in_worker(job: ChatPanelFrameJob) -> int:
    if (
        _CHAT_PANEL_WORKER_LAYOUT is None
        or _CHAT_PANEL_WORKER_FONTS is None
        or _CHAT_PANEL_WORKER_CACHE is None
    ):
        raise ChatPanelRenderError("Chat panel frame worker was not initialized")
    render_chat_panel_frame_job(
        job,
        _CHAT_PANEL_WORKER_LAYOUT,
        _CHAT_PANEL_WORKER_FONTS,
        _CHAT_PANEL_WORKER_CACHE,
    )
    return job.index


def prewarm_emoji_cache(entries: list[ChatEntry], cache: "EmojiImageCache") -> int:
    seen: set[tuple[str, str]] = set()
    for entry in entries:
        for token in entry.tokens:
            if token.image_url:
                seen.add((token.image_url, token.image_key))
    for image_url, image_key in seen:
        cache.get(image_url, image_key)
    return len(seen)


def chat_panel_segments(
    entries: list[ChatEntry],
    duration_seconds: float,
    layout: ChatLayout,
    fonts: ChatPanelFonts,
) -> list[tuple[float, float, list[tuple[ChatEntry, int]]]]:
    sorted_entries = sorted(entries, key=lambda entry: entry.offset_seconds)
    segments: list[tuple[float, float, list[tuple[ChatEntry, int]]]] = []
    start_index = 0
    previous_time = 0.0

    while start_index < len(sorted_entries):
        start = sorted_entries[start_index].offset_seconds
        end_index = start_index + 1
        start_key = chat_time_key(start)
        while end_index < len(sorted_entries):
            if chat_time_key(sorted_entries[end_index].offset_seconds) != start_key:
                break
            end_index += 1

        event_time = min(max(0.0, start), duration_seconds)
        if event_time > previous_time:
            segments.append((previous_time, event_time, visible_panel_chat_stack(
                sorted_entries[:start_index],
                layout,
                fonts,
            )))

        next_time = (
            sorted_entries[end_index].offset_seconds
            if end_index < len(sorted_entries)
            else duration_seconds
        )
        segment_end = min(max(event_time, next_time), duration_seconds)
        if segment_end > event_time:
            segments.append((event_time, segment_end, visible_panel_chat_stack(
                sorted_entries[:end_index],
                layout,
                fonts,
            )))
        previous_time = segment_end
        if previous_time >= duration_seconds:
            break
        start_index = end_index

    if previous_time < duration_seconds:
        segments.append((previous_time, duration_seconds, visible_panel_chat_stack(
            sorted_entries,
            layout,
            fonts,
        )))
    return segments


def split_chat_panel_segments_by_clock_minutes(
    segments: list[tuple[float, float, list[tuple[ChatEntry, int]]]],
    clock_origin_us: int,
) -> list[tuple[float, float, list[tuple[ChatEntry, int]]]]:
    split_segments: list[tuple[float, float, list[tuple[ChatEntry, int]]]] = []
    for start, end, stack in segments:
        current = start
        while current + 0.0005 < end:
            next_minute = next_clock_minute_offset_seconds(clock_origin_us, current)
            next_end = min(end, next_minute)
            if next_end <= current + 0.0005:
                next_end = min(end, current + 60)
            split_segments.append((current, next_end, stack))
            current = next_end
    return split_segments


def next_clock_minute_offset_seconds(clock_origin_us: int, offset_seconds: float) -> float:
    timestamp_us = clock_origin_us + round(offset_seconds * 1_000_000)
    next_minute_us = ((timestamp_us // 60_000_000) + 1) * 60_000_000
    return (next_minute_us - clock_origin_us) / 1_000_000


def chat_clock_origin_us(entries: list[ChatEntry]) -> int | None:
    origins = [
        entry.timestamp_us - round(entry.offset_seconds * 1_000_000)
        for entry in entries
        if entry.timestamp_us is not None
    ]
    return min(origins, default=None)


def kirkland_time_for_offset(
    clock_origin_us: int | None,
    offset_seconds: float,
) -> str:
    if clock_origin_us is None:
        return ""
    return format_kirkland_time(clock_origin_us + round(offset_seconds * 1_000_000))


def format_kirkland_time(timestamp_us: int) -> str:
    try:
        time_zone = ZoneInfo(KIRKLAND_TIME_ZONE)
        moment = datetime.fromtimestamp(timestamp_us / 1_000_000, time_zone)
    except (OSError, OverflowError, ValueError, ZoneInfoNotFoundError):
        return ""
    return moment.strftime("%I:%M %p").lstrip("0")


def render_chat_panel_frame(
    stack: list[tuple[ChatEntry, int]],
    layout: ChatLayout,
    fonts: ChatPanelFonts | None = None,
    cache: "EmojiImageCache | None" = None,
    header_time_text: str = "",
) -> Any:
    from PIL import Image, ImageDraw

    fonts = fonts or load_chat_panel_fonts(layout)
    cache = cache or EmojiImageCache(None)
    image = Image.new(
        "RGB",
        (layout.panel_width, layout.video_height),
        CHAT_PANEL_BACKGROUND,
    )
    draw = ImageDraw.Draw(image)

    draw_chat_messages(draw, image, stack, layout, fonts, cache)
    draw_chat_header(draw, layout, fonts, header_time_text)
    return image


def draw_chat_header(
    draw: Any,
    layout: ChatLayout,
    fonts: ChatPanelFonts,
    header_time_text: str = "",
) -> None:
    separator_y = chat_header_separator_y(layout)
    separator_height = chat_header_separator_height(layout)
    band_bottom = separator_y + separator_height + chat_header_bottom_padding(layout)
    draw.rectangle(
        (0, 0, layout.panel_width, band_bottom),
        fill=CHAT_PANEL_HEADER,
    )
    draw.rectangle(
        (
            layout.panel_padding_x,
            separator_y,
            layout.panel_width - layout.panel_padding_x,
            separator_y + separator_height,
        ),
        fill=CHAT_PANEL_SEPARATOR,
    )
    draw.text(
        (layout.panel_padding_x, layout.title_y),
        "Live Chat",
        font=fonts.bold,
        fill=CHAT_AUTHOR_FILL,
    )
    if header_time_text:
        fitted_time_text = fitted_chat_header_time_text(
            draw,
            layout,
            fonts,
            header_time_text,
        )
        if fitted_time_text:
            time_width = text_width(draw, fonts.bold, fitted_time_text)
            time_x = layout.panel_width - layout.panel_padding_x - time_width
            draw.text(
                (time_x, layout.title_y),
                fitted_time_text,
                font=fonts.bold,
                fill=CHAT_AUTHOR_FILL,
            )


def fitted_chat_header_time_text(
    draw: Any,
    layout: ChatLayout,
    fonts: ChatPanelFonts,
    header_time_text: str,
) -> str:
    gap = max(8, round(layout.font_size * 0.6))
    title_right = layout.panel_padding_x + text_width(draw, fonts.bold, "Live Chat")
    available_width = layout.panel_width - layout.panel_padding_x - title_right - gap
    if available_width <= 0:
        return ""
    if text_width(draw, fonts.bold, header_time_text) <= available_width:
        return header_time_text
    return ""


def draw_chat_messages(
    draw: Any,
    image: Any,
    stack: list[tuple[ChatEntry, int]],
    layout: ChatLayout,
    fonts: ChatPanelFonts,
    cache: "EmojiImageCache",
) -> None:
    for entry, y in stack:
        draw_chat_entry(draw, image, entry, layout.panel_padding_x, y, layout, fonts, cache)


def draw_chat_entry(
    draw: Any,
    image: Any,
    entry: ChatEntry,
    x: int,
    y: int,
    layout: ChatLayout,
    fonts: ChatPanelFonts,
    cache: "EmojiImageCache",
) -> None:
    line_height = panel_line_height(layout)
    draw.text(
        (x, y),
        entry.author,
        font=fonts.bold,
        fill=chat_author_color(entry.author),
        stroke_width=chat_author_stroke_width(layout),
        stroke_fill=CHAT_AUTHOR_STROKE_FILL,
    )
    current_y = y + line_height
    for line in wrap_panel_message_lines(entry, layout, fonts):
        draw_panel_line(draw, image, line, x, current_y, layout, fonts, cache)
        current_y += line_height


def draw_panel_line(
    draw: Any,
    image: Any,
    line: list[ChatLineItem],
    x: int,
    y: int,
    layout: ChatLayout,
    fonts: ChatPanelFonts,
    cache: "EmojiImageCache",
) -> None:
    current_x = x
    emoji_size = panel_emoji_size(layout)
    for item in line:
        if item.is_image and item.image_url:
            emoji = cache.get(item.image_url, item.image_key)
            if emoji is not None:
                resized = emoji.resize((emoji_size, emoji_size))
                image.paste(
                    resized,
                    (current_x, panel_emoji_y(y, layout, fonts)),
                    resized if resized.mode == "RGBA" else None,
                )
                current_x += emoji_size + panel_inline_gap(layout)
                continue

        if item.text:
            draw.text((current_x, y), item.text, font=fonts.regular, fill=CHAT_TEXT_FILL)
            current_x += text_width(draw, fonts.regular, item.text)


def visible_panel_chat_stack(
    entries: list[ChatEntry],
    layout: ChatLayout,
    fonts: ChatPanelFonts,
) -> list[tuple[ChatEntry, int]]:
    bottom = layout.output_height - chat_bottom_padding(layout)
    gap = chat_entry_gap(layout)
    positioned: list[tuple[ChatEntry, int]] = []

    for entry in reversed(entries):
        height = panel_chat_entry_height(entry, layout, fonts)
        y = bottom - height
        if y + height <= 0:
            break
        positioned.append((entry, y))
        bottom = y - gap

    return list(reversed(positioned))


def panel_chat_entry_height(
    entry: ChatEntry,
    layout: ChatLayout,
    fonts: ChatPanelFonts,
) -> int:
    return (1 + len(wrap_panel_message_lines(entry, layout, fonts))) * panel_line_height(layout)


def wrap_panel_message_lines(
    entry: ChatEntry,
    layout: ChatLayout,
    fonts: ChatPanelFonts,
) -> list[list[ChatLineItem]]:
    from PIL import Image, ImageDraw

    image = Image.new("RGB", (1, 1))
    draw = ImageDraw.Draw(image)
    max_width = layout.panel_width - layout.panel_padding_x * 2
    return wrap_inline_items(
        inline_items_from_entry(entry),
        max_width,
        CHAT_MESSAGE_MAX_LINES,
        draw,
        fonts,
        layout,
    )


def inline_items_from_entry(entry: ChatEntry) -> list[ChatLineItem]:
    tokens = entry.tokens or (ChatToken(entry.message),)
    items: list[ChatLineItem] = []
    for token in tokens:
        if token.is_emoji:
            items.append(
                ChatLineItem(
                    text=token.text.strip() or "[emoji]",
                    image_url=token.image_url,
                    image_key=token.image_key,
                    is_image=bool(token.image_url),
                )
            )
            continue

        for part in re.findall(r"\S+\s*", token.text):
            items.append(ChatLineItem(text=part))
    return items


def wrap_inline_items(
    items: list[ChatLineItem],
    max_width: int,
    max_lines: int,
    draw: Any,
    fonts: ChatPanelFonts,
    layout: ChatLayout,
) -> list[list[ChatLineItem]]:
    lines: list[list[ChatLineItem]] = []
    current: list[ChatLineItem] = []
    current_width = 0

    for item in items:
        width = panel_item_width(draw, fonts, layout, item)
        if current and current_width + width > max_width:
            lines.append(current)
            current = []
            current_width = 0
            if len(lines) >= max_lines:
                return truncate_panel_lines(lines, max_width, draw, fonts, layout)
        current.append(item)
        current_width += width

    if current:
        lines.append(current)
    if len(lines) > max_lines:
        return truncate_panel_lines(lines[:max_lines], max_width, draw, fonts, layout)
    return lines


def truncate_panel_lines(
    lines: list[list[ChatLineItem]],
    max_width: int,
    draw: Any,
    fonts: ChatPanelFonts,
    layout: ChatLayout,
) -> list[list[ChatLineItem]]:
    if not lines:
        return []
    last = lines[-1]
    ellipsis = ChatLineItem(text="...")
    while last and (
        sum(panel_item_width(draw, fonts, layout, item) for item in last)
        + panel_item_width(draw, fonts, layout, ellipsis)
        > max_width
    ):
        last.pop()
    if last and last[-1].text.endswith("..."):
        return lines
    last.append(ellipsis)
    return lines


def panel_item_width(
    draw: Any,
    fonts: ChatPanelFonts,
    layout: ChatLayout,
    item: ChatLineItem,
) -> int:
    if item.is_image:
        return panel_emoji_size(layout) + panel_inline_gap(layout)
    return text_width(draw, fonts.regular, item.text)


def text_width(draw: Any, font: Any, text: str) -> int:
    if not text:
        return 0
    left, _top, right, _bottom = draw.textbbox((0, 0), text, font=font)
    return max(0, right - left)


def panel_line_height(layout: ChatLayout) -> int:
    return max(chat_line_height(layout), panel_emoji_size(layout) + 2)


def panel_emoji_y(y: int, layout: ChatLayout, fonts: ChatPanelFonts) -> int:
    _left, _top, _right, text_bottom = fonts.regular.getbbox("Ag")
    return y + max(0, text_bottom - panel_emoji_size(layout))


def panel_emoji_size(layout: ChatLayout) -> int:
    return max(layout.font_size, round(layout.font_size * 1.1))


def panel_inline_gap(layout: ChatLayout) -> int:
    return max(3, round(layout.font_size * 0.2))


def chat_author_color(author: str) -> tuple[int, int, int]:
    key = author.strip().casefold().encode("utf-8") or b"unknown"
    digest = hashlib.sha256(key).digest()
    index = int.from_bytes(digest[:2], "big") % len(CHAT_AUTHOR_COLORS)
    return CHAT_AUTHOR_COLORS[index]


def chat_author_stroke_width(layout: ChatLayout | None = None) -> int:
    layout = layout or default_chat_layout()
    return max(1, round(layout.font_size * 0.08))


def ass_color_from_rgb(color: tuple[int, int, int]) -> str:
    red, green, blue = color
    return f"&H{blue:02X}{green:02X}{red:02X}&"


def load_chat_panel_fonts(layout: ChatLayout) -> ChatPanelFonts:
    from PIL import ImageFont

    regular_font = find_font_file("DejaVu Sans") or "DejaVuSans.ttf"
    bold_font = find_font_file("DejaVu Sans:style=Bold") or regular_font
    try:
        regular = ImageFont.truetype(regular_font, layout.font_size)
    except OSError:
        regular = ImageFont.load_default()
    try:
        bold = ImageFont.truetype(bold_font, layout.font_size)
    except OSError:
        bold = regular
    return ChatPanelFonts(regular=regular, bold=bold)


def find_font_file(pattern: str) -> str:
    try:
        result = subprocess.run(
            ["fc-match", "-f", "%{file}\\n", pattern],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    return result.stdout.strip()


class EmojiImageCache:
    def __init__(self, cache_dir: Path | None) -> None:
        self.cache_dir = cache_dir
        self.memory: dict[str, Any] = {}
        if self.cache_dir is not None:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get(self, url: str, key: str = "") -> Any | None:
        if not url:
            return None
        cache_key = key or url
        if cache_key in self.memory:
            return self.memory[cache_key]

        image = self._load(url, cache_key)
        if image is not None:
            self.memory[cache_key] = image
        return image

    def _load(self, url: str, cache_key: str) -> Any | None:
        from PIL import Image

        path = self._cache_path(cache_key)
        if path is not None and path.exists():
            try:
                return Image.open(path).convert("RGBA")
            except OSError:
                path.unlink(missing_ok=True)

        try:
            with urlopen(url, timeout=10) as response:
                data = response.read()
        except (OSError, URLError):
            return None

        try:
            image = Image.open(BytesIO(data)).convert("RGBA")
        except OSError:
            return None

        if path is not None:
            try:
                image.save(path)
            except OSError:
                pass
        return image

    def _cache_path(self, cache_key: str) -> Path | None:
        if self.cache_dir is None:
            return None
        digest = hashlib.sha256(cache_key.encode("utf-8")).hexdigest()
        return self.cache_dir / f"{digest}.png"


def escape_concat_path(path: Path) -> str:
    return str(path).replace("'", "'\\''")


def write_chat_ass_file(
    path: Path,
    entries: list[ChatEntry],
    layout: ChatLayout | None = None,
) -> None:
    path.write_text(render_chat_ass(entries, layout), encoding="utf-8")


def render_chat_ass(
    entries: list[ChatEntry],
    layout: ChatLayout | None = None,
) -> str:
    layout = layout or default_chat_layout()
    sorted_entries = sorted(entries, key=lambda entry: entry.offset_seconds)
    title_end = chat_render_end_time(sorted_entries)
    lines = [
        "[Script Info]",
        "ScriptType: v4.00+",
        f"PlayResX: {layout.output_width}",
        f"PlayResY: {layout.output_height}",
        "WrapStyle: 0",
        "ScaledBorderAndShadow: yes",
        "",
        "[V4+ Styles]",
        (
            "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, "
            "OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, "
            "ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, "
            "Alignment, MarginL, MarginR, MarginV, Encoding"
        ),
        (
            f"Style: Chat,Arial,{layout.font_size},&H00F3F6FA,&H000000FF,&H9011161D,"
            "&HC811161D,0,0,0,0,100,100,0,0,3,1,0,7,"
            f"{layout.panel_x},20,20,1"
        ),
        "",
        "[Events]",
        "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text",
    ]
    lines.extend(render_chat_stack_dialogues(sorted_entries, title_end, layout))
    lines.extend(render_chat_header_dialogues(title_end, layout, sorted_entries))

    return "\n".join(lines) + "\n"


def chat_render_end_time(entries: list[ChatEntry]) -> float:
    if not entries:
        return CHAT_FINAL_EVENT_PADDING_SECONDS
    return entries[-1].offset_seconds + CHAT_FINAL_EVENT_PADDING_SECONDS


def render_chat_stack_dialogues(
    entries: list[ChatEntry],
    final_end: float,
    layout: ChatLayout | None = None,
) -> list[str]:
    layout = layout or default_chat_layout()
    lines: list[str] = []
    start_index = 0
    while start_index < len(entries):
        start = entries[start_index].offset_seconds
        end_index = start_index + 1
        start_key = chat_time_key(start)
        while end_index < len(entries):
            if chat_time_key(entries[end_index].offset_seconds) != start_key:
                break
            end_index += 1

        end = entries[end_index].offset_seconds if end_index < len(entries) else final_end
        if end <= start:
            start_index = end_index
            continue

        active_entries = visible_chat_stack(entries[:end_index], layout)
        for entry, y in active_entries:
            lines.append(
                "Dialogue: 0,"
                f"{format_ass_time(start)},{format_ass_time(end)},Chat,,0,0,0,,"
                f"{{\\pos({layout.panel_x},{y})}}"
                f"{render_chat_dialog_text(entry, layout)}"
            )
        start_index = end_index
    return lines


def chat_time_key(seconds: float) -> int:
    return round(seconds * 100)


def chat_stack_row_y(
    active_index: int,
    active_count: int,
    layout: ChatLayout | None = None,
) -> int:
    layout = layout or default_chat_layout()
    newest_index = active_count - 1
    rows_above_newest = newest_index - active_index
    return layout.row_top + (layout.row_count - 1 - rows_above_newest) * layout.row_height


def visible_chat_stack(
    entries: list[ChatEntry],
    layout: ChatLayout | None = None,
) -> list[tuple[ChatEntry, int]]:
    layout = layout or default_chat_layout()
    bottom = layout.output_height - chat_bottom_padding(layout)
    gap = chat_entry_gap(layout)
    positioned: list[tuple[ChatEntry, int]] = []

    for entry in reversed(entries):
        height = chat_entry_height(entry, layout)
        y = bottom - height
        if y + height <= 0:
            break
        positioned.append((entry, y))
        bottom = y - gap

    return list(reversed(positioned))


def render_chat_header_dialogues(
    final_end: float,
    layout: ChatLayout | None = None,
    entries: list[ChatEntry] | None = None,
) -> list[str]:
    layout = layout or default_chat_layout()
    end = format_ass_time(final_end)
    separator_y = chat_header_separator_y(layout)
    separator_height = chat_header_separator_height(layout)
    band_bottom = separator_y + separator_height + chat_header_bottom_padding(layout)
    panel_right = layout.video_width + layout.panel_width
    separator_right = panel_right - layout.panel_padding_x
    return [
        (
            f"Dialogue: 2,{format_ass_time(0)},{end},Chat,,0,0,0,,"
            f"{{\\an7\\pos(0,0)\\bord0\\shad0\\1c&H201811&\\alpha&H00&\\p1}}"
            f"m {layout.video_width} 0 l {panel_right} 0 "
            f"l {panel_right} {band_bottom} l {layout.video_width} {band_bottom}"
        ),
        (
            f"Dialogue: 3,{format_ass_time(0)},{end},Chat,,0,0,0,,"
            f"{{\\an7\\pos(0,0)\\bord0\\shad0\\1c&H60564A&\\alpha&H20&\\p1}}"
            f"m {layout.panel_x} {separator_y} l {separator_right} {separator_y} "
            f"l {separator_right} {separator_y + separator_height} "
            f"l {layout.panel_x} {separator_y + separator_height}"
        ),
        (
            f"Dialogue: 4,{format_ass_time(0)},{end},Chat,,0,0,0,,"
            f"{{\\pos({layout.panel_x},{layout.title_y})"
            f"\\fs{layout.title_font_size}\\b1}}Live Chat"
        ),
        *render_chat_header_time_dialogues(final_end, layout, entries or []),
    ]


def render_chat_header_time_dialogues(
    final_end: float,
    layout: ChatLayout,
    entries: list[ChatEntry],
) -> list[str]:
    clock_origin_us = chat_clock_origin_us(entries)
    if clock_origin_us is None:
        return []
    time_x = layout.video_width + layout.panel_width - layout.panel_padding_x
    lines: list[str] = []
    start = 0.0
    while start < final_end:
        end = min(next_clock_minute_offset_seconds(clock_origin_us, start), final_end)
        if end <= start:
            end = min(start + 60, final_end)
        time_label = kirkland_time_for_offset(clock_origin_us, start)
        if time_label:
            lines.append(
                f"Dialogue: 5,{format_ass_time(start)},{format_ass_time(end)},"
                "Chat,,0,0,0,,"
                f"{{\\an9\\pos({time_x},{layout.title_y})"
                f"\\fs{layout.title_font_size}\\b1}}{ass_escape(time_label)}"
            )
        start = end
    return lines


def chat_header_separator_y(layout: ChatLayout | None = None) -> int:
    layout = layout or default_chat_layout()
    return layout.title_y + layout.title_font_size + max(10, round(layout.font_size * 0.45))


def chat_header_separator_height(layout: ChatLayout | None = None) -> int:
    layout = layout or default_chat_layout()
    return max(1, round(layout.output_height / 720))


def chat_header_bottom_padding(layout: ChatLayout | None = None) -> int:
    layout = layout or default_chat_layout()
    return max(8, round(layout.font_size * 0.5))


def chat_entry_height(
    entry: ChatEntry,
    layout: ChatLayout | None = None,
) -> int:
    layout = layout or default_chat_layout()
    return len(chat_dialog_lines(entry, layout)) * chat_line_height(layout)


def chat_dialog_lines(
    entry: ChatEntry,
    layout: ChatLayout | None = None,
) -> list[str]:
    author_color = ass_color_from_rgb(chat_author_color(entry.author))
    return [
        (
            f"{{\\b1\\c{author_color}}}{ass_escape(entry.author)}"
            f"{{\\b0\\c{ASS_CHAT_TEXT_COLOR}}}"
        ),
        *(
            ass_escape_chat_message(line)
            for line in wrap_chat_message_lines(entry.message, layout)
        ),
    ]


def chat_line_height(layout: ChatLayout | None = None) -> int:
    layout = layout or default_chat_layout()
    return max(layout.font_size + 3, round(layout.font_size * 1.22))


def chat_entry_gap(layout: ChatLayout | None = None) -> int:
    layout = layout or default_chat_layout()
    return max(7, round(layout.font_size * 0.55))


def chat_bottom_padding(layout: ChatLayout | None = None) -> int:
    layout = layout or default_chat_layout()
    return max(14, round(layout.output_height * 0.0185))


def render_chat_dialog_text(
    entry: ChatEntry,
    layout: ChatLayout | None = None,
) -> str:
    return r"\N".join(chat_dialog_lines(entry, layout))


def wrap_chat_message_lines(
    message: str,
    layout: ChatLayout | None = None,
) -> list[str]:
    layout = layout or default_chat_layout()
    return textwrap.wrap(
        message,
        width=layout.wrap_width,
        max_lines=CHAT_MESSAGE_MAX_LINES,
        placeholder="...",
    )


def ass_escape(value: str) -> str:
    return (
        value.replace("\\", "/")
        .replace("{", "(")
        .replace("}", ")")
        .replace("\n", " ")
    )


def ass_escape_chat_message(value: str) -> str:
    escaped = ass_escape(value)
    for emoji, color in sorted(
        ASS_EMOJI_COLORS.items(),
        key=lambda item: len(item[0]),
        reverse=True,
    ):
        escaped = escaped.replace(
            emoji,
            f"{{\\c{color}}}{emoji}{{\\c{ASS_CHAT_TEXT_COLOR}}}",
        )
    return escaped


def format_ass_time(seconds: float) -> str:
    seconds = max(0.0, seconds)
    centiseconds = int(round(seconds * 100))
    total_seconds, cs = divmod(centiseconds, 100)
    minutes, sec = divmod(total_seconds, 60)
    hours, minute = divmod(minutes, 60)
    return f"{hours}:{minute:02d}:{sec:02d}.{cs:02d}"


def chat_video_output_file(media_file: Path) -> Path:
    return media_file.with_name(f"{media_file.stem} - chat.mp4")


def build_render_chat_file_process_command(
    python_executable: str,
    config_path: Path,
    media_file: Path,
    chat_file: Path,
    output_file: Path,
    *,
    overwrite: bool = False,
    nice: bool = True,
) -> list[str]:
    command = [
        python_executable,
        "-m",
        "ytdlbot",
        "render-chat-file",
        "--config",
        str(config_path),
        "--media",
        str(media_file),
        "--chat",
        str(chat_file),
        "--output",
        str(output_file),
    ]
    if overwrite:
        command.append("--overwrite")
    nice_path = shutil.which("nice") if nice else None
    if nice_path:
        return [nice_path, "-n", "10", *command]
    return command


def chat_render_video_encoder_name(use_nvenc: bool) -> str:
    return "h264_nvenc" if use_nvenc else "libx264"


def choose_chat_render_nvenc_device(
    devices: Sequence[str],
    selection_key: int | str | Path | None = None,
) -> str:
    if not devices:
        return ""
    if len(devices) == 1:
        return devices[0]
    if isinstance(selection_key, int):
        return devices[selection_key % len(devices)]
    if selection_key is None:
        return devices[0]

    digest = hashlib.sha256(str(selection_key).encode("utf-8")).digest()
    index = int.from_bytes(digest[:8], "big") % len(devices)
    return devices[index]


def chat_render_video_encoder_args(
    *,
    use_nvenc: bool,
    quality: int,
    nvenc_device: str = "",
) -> list[str]:
    if use_nvenc:
        args = [
            "-c:v",
            "h264_nvenc",
            "-preset",
            "fast",
            "-rc",
            "vbr",
            "-cq",
            str(quality),
            "-b:v",
            "0",
        ]
        if nvenc_device:
            args.extend(["-gpu", nvenc_device])
        args.extend(["-pix_fmt", "yuv420p"])
        return args

    return [
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        str(quality),
        "-pix_fmt",
        "yuv420p",
    ]


def inspect_nvenc_environment(ffmpeg_path: str = "ffmpeg") -> NvencEnvironment:
    return NvencEnvironment(
        nvidia_devices=detect_nvidia_devices(),
        ffmpeg_has_h264_nvenc=ffmpeg_supports_h264_nvenc(ffmpeg_path),
    )


def detect_nvidia_devices(nvidia_smi_path: str = "nvidia-smi") -> list[str]:
    try:
        result = subprocess.run(
            [
                nvidia_smi_path,
                "--query-gpu=index,name",
                "--format=csv,noheader",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return []

    if result.returncode != 0:
        return []

    devices: list[str] = []
    for line in result.stdout.splitlines():
        index, _separator, name = line.partition(",")
        index = index.strip()
        name = name.strip()
        if index:
            devices.append(f"{index}: {name}" if name else index)
    return devices


def ffmpeg_supports_h264_nvenc(ffmpeg_path: str = "ffmpeg") -> bool:
    try:
        result = subprocess.run(
            [ffmpeg_path, "-hide_banner", "-encoders"],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return False

    output = f"{result.stdout}\n{result.stderr}"
    return "h264_nvenc" in output


def log_nvenc_environment(ffmpeg_path: str, use_nvenc: bool) -> NvencEnvironment:
    environment = inspect_nvenc_environment(ffmpeg_path)
    if not environment.nvidia_devices:
        if use_nvenc:
            LOGGER.warning(
                "NVENC chat rendering is enabled but no NVIDIA GPUs were detected "
                "with nvidia-smi; FFmpeg may fail unless a GPU is available"
            )
        return environment

    LOGGER.info(
        "Detected NVIDIA GPUs for chat rendering: %s",
        ", ".join(environment.nvidia_devices),
    )
    if environment.ffmpeg_has_h264_nvenc:
        LOGGER.info("FFmpeg advertises h264_nvenc support for chat rendering")
    else:
        LOGGER.warning(
            "NVIDIA GPUs were detected, but FFmpeg does not advertise h264_nvenc. "
            "Rerun the systemd installer without YTDLBOT_SKIP_OS_DEPS=1 or "
            "YTDLBOT_SKIP_NVIDIA_DEPS=1 to install NVIDIA/NVENC dependencies on "
            "supported DNF systems."
        )
    if environment.ffmpeg_has_h264_nvenc and not use_nvenc:
        LOGGER.info(
            "NVIDIA/NVENC is available; set chat_render_use_nvenc = true to use it "
            "for chat video encoding"
        )
    return environment


def render_chat_video_file(
    media_file: Path,
    chat_file: Path,
    *,
    ffmpeg_path: str = "ffmpeg",
    output_file: Path | None = None,
    timeout_seconds: float = DEFAULT_CHAT_RENDER_TIMEOUT_SECONDS,
    overwrite: bool = False,
    panel_workers: int = 0,
    use_nvenc: bool = False,
    nvenc_device: str = "",
    nvenc_devices: Sequence[str] | None = None,
) -> Path:
    started_at = time.monotonic()
    output_file = output_file or chat_video_output_file(media_file)
    candidate_devices = (
        nvenc_devices
        if nvenc_devices is not None
        else ([nvenc_device] if nvenc_device else [])
    )
    selected_nvenc_device = choose_chat_render_nvenc_device(
        candidate_devices,
        output_file,
    )
    temp_output = output_file.with_name(
        f"{output_file.stem}.rendering{output_file.suffix}"
    )
    ass_file = output_file.with_name(f"{output_file.stem}.ass")
    panel_file = output_file.with_name(f"{output_file.stem}.panel.mp4")

    if output_file.exists() and not overwrite:
        LOGGER.info("Chat video already exists; skipping render output=%s", output_file)
        return output_file

    LOGGER.info(
        "Starting chat video %s media=%s chat=%s output=%s encoder=%s "
        "nvenc_device=%s",
        "regeneration" if overwrite else "render",
        media_file,
        chat_file,
        output_file,
        chat_render_video_encoder_name(use_nvenc),
        selected_nvenc_device or "default",
    )
    try:
        entries = parse_live_chat_file(chat_file)
    except OSError as exc:
        raise ChatPanelRenderError(f"Unable to read live chat file {chat_file}") from exc

    if not entries:
        raise ChatPanelRenderError(f"No live chat messages found in {chat_file}")
    LOGGER.info(
        "Parsed live chat file chat=%s entries=%d first_offset=%.2fs "
        "last_offset=%.2fs",
        chat_file,
        len(entries),
        entries[0].offset_seconds,
        entries[-1].offset_seconds,
    )

    try:
        dimensions = probe_video_dimensions(media_file, ffprobe_path_for(ffmpeg_path))
        duration = probe_video_duration(media_file, ffprobe_path_for(ffmpeg_path))
        layout: ChatLayout | None = chat_layout_for_video(
            dimensions.width,
            dimensions.height,
        )
        LOGGER.info(
            "Probed media for chat render media=%s video=%sx%s duration=%.2fs "
            "output=%sx%s panel_width=%s",
            media_file,
            dimensions.width,
            dimensions.height,
            duration,
            layout.output_width,
            layout.output_height,
            layout.panel_width,
        )
    except VideoProbeError:
        LOGGER.exception(
            "Unable to probe video size for chat render; using fallback layout for %s",
            media_file,
        )
        layout = None
        duration = 0.0

    output_file.parent.mkdir(parents=True, exist_ok=True)
    temp_output.unlink(missing_ok=True)
    panel_file.unlink(missing_ok=True)

    try:
        if layout is not None and duration > 0:
            try:
                LOGGER.info(
                    "Rendering image chat panel panel=%s media=%s entries=%d",
                    panel_file,
                    media_file,
                    len(entries),
                )
                render_chat_panel_video(
                    entries,
                    layout,
                    panel_file,
                    duration,
                    ffmpeg_path,
                    output_file.parent / ".emoji-cache",
                    panel_workers,
                    use_nvenc,
                    selected_nvenc_device,
                )
                command = build_chat_panel_merge_command(
                    ffmpeg_path,
                    media_file,
                    panel_file,
                    temp_output,
                    layout,
                    use_nvenc=use_nvenc,
                    nvenc_device=selected_nvenc_device,
                )
                LOGGER.info(
                    "Merging media with rendered chat panel media=%s panel=%s "
                    "output=%s",
                    media_file,
                    panel_file,
                    output_file,
                )
            except ChatPanelRenderError:
                panel_file.unlink(missing_ok=True)
                LOGGER.exception(
                    "Unable to render image chat panel; falling back to subtitle renderer"
                )
                write_chat_ass_file(ass_file, entries, layout)
                LOGGER.info(
                    "Writing subtitle fallback for chat render ass=%s output=%s",
                    ass_file,
                    output_file,
                )
                command = build_chat_video_command(
                    ffmpeg_path,
                    media_file,
                    ass_file,
                    temp_output,
                    layout,
                    use_nvenc=use_nvenc,
                    nvenc_device=selected_nvenc_device,
                )
        else:
            write_chat_ass_file(ass_file, entries, layout)
            LOGGER.info(
                "Writing subtitle fallback for chat render ass=%s output=%s",
                ass_file,
                output_file,
            )
            command = build_chat_video_command(
                ffmpeg_path,
                media_file,
                ass_file,
                temp_output,
                layout,
                use_nvenc=use_nvenc,
                nvenc_device=selected_nvenc_device,
            )

        LOGGER.debug("ffmpeg chat render command: %s", shlex.join(command))
        try:
            ffmpeg_started_at = time.monotonic()
            result = subprocess.run(
                command,
                check=False,
                capture_output=True,
                timeout=timeout_seconds,
            )
            ffmpeg_elapsed = time.monotonic() - ffmpeg_started_at
        except FileNotFoundError as exc:
            raise ChatPanelRenderError(f"ffmpeg not found: {ffmpeg_path}") from exc
        except subprocess.TimeoutExpired as exc:
            temp_output.unlink(missing_ok=True)
            raise ChatPanelRenderError("ffmpeg timed out while rendering chat video") from exc
        except OSError as exc:
            raise ChatPanelRenderError("Unable to start ffmpeg for chat video render") from exc

        if result.returncode != 0:
            temp_output.unlink(missing_ok=True)
            message = (result.stderr or result.stdout).decode("utf-8", "replace").strip()
            raise ChatPanelRenderError(
                f"ffmpeg failed while rendering chat video: {message}"
            )

        temp_output.replace(output_file)
        output_size = output_file.stat().st_size if output_file.exists() else 0
        LOGGER.info(
            "Rendered chat video output=%s size=%s elapsed=%.1fs "
            "ffmpeg_elapsed=%.1fs",
            output_file,
            output_size,
            time.monotonic() - started_at,
            ffmpeg_elapsed,
        )
        return output_file
    except OSError as exc:
        temp_output.unlink(missing_ok=True)
        raise ChatPanelRenderError(f"Unable to write chat video {output_file}") from exc
    finally:
        ass_file.unlink(missing_ok=True)
        panel_file.unlink(missing_ok=True)


def build_chat_video_command(
    ffmpeg_path: str,
    media_file: Path,
    ass_file: Path,
    output_file: Path,
    layout: ChatLayout | None = None,
    *,
    use_nvenc: bool = False,
    nvenc_device: str = "",
) -> list[str]:
    layout = layout or default_chat_layout()
    escaped_ass = escape_ffmpeg_filter_path(str(ass_file))
    filter_complex = (
        f"[0:v]setsar=1,pad={layout.video_width}:{layout.video_height}:0:0:black[v];"
        f"color=c=0x111820:s={layout.panel_width}x{layout.video_height}:r=30[panel];"
        "[v][panel]hstack=inputs=2[base];"
        f"[base]subtitles=filename='{escaped_ass}'[outv]"
    )
    return [
        ffmpeg_path,
        "-hide_banner",
        "-loglevel",
        "warning",
        "-y",
        "-i",
        str(media_file),
        "-filter_complex",
        filter_complex,
        "-map",
        "[outv]",
        "-map",
        "0:a?",
        *chat_render_video_encoder_args(
            use_nvenc=use_nvenc,
            quality=23,
            nvenc_device=nvenc_device,
        ),
        "-c:a",
        "aac",
        "-b:a",
        "160k",
        "-shortest",
        str(output_file),
    ]


def build_chat_panel_merge_command(
    ffmpeg_path: str,
    media_file: Path,
    panel_file: Path,
    output_file: Path,
    layout: ChatLayout | None = None,
    *,
    use_nvenc: bool = False,
    nvenc_device: str = "",
) -> list[str]:
    layout = layout or default_chat_layout()
    filter_complex = (
        f"[0:v]setsar=1,pad={layout.video_width}:{layout.video_height}:0:0:black[v];"
        "[1:v]setsar=1[panel];"
        "[v][panel]hstack=inputs=2[outv]"
    )
    return [
        ffmpeg_path,
        "-hide_banner",
        "-loglevel",
        "warning",
        "-y",
        "-i",
        str(media_file),
        "-i",
        str(panel_file),
        "-filter_complex",
        filter_complex,
        "-map",
        "[outv]",
        "-map",
        "0:a?",
        *chat_render_video_encoder_args(
            use_nvenc=use_nvenc,
            quality=23,
            nvenc_device=nvenc_device,
        ),
        "-c:a",
        "aac",
        "-b:a",
        "160k",
        "-shortest",
        str(output_file),
    ]


def escape_ffmpeg_filter_path(value: str) -> str:
    return (
        value.replace("\\", "\\\\")
        .replace("'", r"\'")
        .replace(":", r"\:")
        .replace(",", r"\,")
        .replace("[", r"\[")
        .replace("]", r"\]")
    )
