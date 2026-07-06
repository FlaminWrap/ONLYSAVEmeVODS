from __future__ import annotations

from bisect import insort
from collections import OrderedDict
from dataclasses import asdict, dataclass, field
from html import escape
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock, Thread
from typing import Any
from urllib.parse import parse_qs, quote, urlencode, urlsplit
from datetime import datetime
from email.parser import BytesParser
from email.policy import default as email_policy
import asyncio
import csv
import hashlib
import io
import json
import logging
import mimetypes
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import time

from . import __version__ as APP_VERSION
from .chat_render import (
    build_render_chat_file_process_command,
    chat_video_output_file,
    choose_chat_render_nvenc_device,
    detect_nvidia_devices,
    render_chat_video_file,
)
from .chat_refresh import ChatRefreshResult, refresh_chat_sidecar
from .chat_timing import is_chat_timing_file
from .content_events import (
    ContentEventDetectorUnavailable,
    content_event_detector_status,
    content_events_exist,
    detect_content_events_for_media,
    load_content_events,
)
from .config import (
    BotConfig,
    ConfigError,
    StreamEventDetectionConfig,
    StreamEventRuleConfig,
    VoiceDetectionConfig,
    VoiceProfileConfig,
    add_voice_sample_to_profile,
    load_config,
    monitored_sources,
    remove_streamer_config,
    streamer_display_name_for_channel,
    update_channel_speaker_labels_config,
    update_channel_voice_detection_config,
    update_config_values,
    update_global_stream_event_rules_config,
    update_streamer_config,
    update_streamer_speaker_labels_config,
    update_streamer_stream_event_config,
    update_streamer_voice_detection_config,
    update_streamer_voice_profile_config,
    sanitize_voice_sample_filename,
    validate_voice_name,
    voice_sample_dir,
)
from .downloader import (
    command_for_log,
    cleanup_files,
    is_live_chat_file,
    is_yt_dlp_temporary_file,
    log_process_output,
    named_segment_file_stem,
    safe_filename_stem,
    segment_directory,
)
from .job_tracker import (
    finish_tracked_job,
    list_tracked_jobs,
    start_tracked_job,
    update_tracked_job,
)
from .kick_chat import download_kick_vod_chat_replay
from .log_buffer import LogEntry, get_recent_log_entries
from .models import LiveStream
from .powerchat import (
    POWERCHAT_EVENT_SUFFIX,
    is_powerchat_event_file,
    load_powerchat_sidecar,
    powerchat_totals,
)
from .sources import SourceError, live_stream_from_generic_info, resolve_source
from .state import StateStore, StreamEventRecord, StreamRecord, WatermarkCopyRecord
from .youtube import YtDlpError, YtDlpRunner, live_stream_from_info
from .voice_match import (
    create_transcript_voice_sample,
    load_transcript_segments,
    match_known_voices_for_media,
    speaker_labels_in_segments,
    update_voice_attribution_decision,
    voice_attribution_file,
    voice_match_rows_for_media,
    voice_matcher_status,
)
from .transcription import (
    load_whisperx_subtitle_segments,
    rewrite_speaker_labels_for_media,
    speaker_labels_for_channel,
    transcribe_media_file,
    transcription_config_for_channel,
    transcription_outputs_exist,
    voice_detection_mode,
    voice_detection_speaker_summary,
)
from .twitch_ad_repair import repair_twitch_ads_for_media
from .watermark import (
    WATERMARK_STATUS_DONE,
    WATERMARK_STATUS_FAILED,
    WATERMARK_STATUS_INTERRUPTED,
    WATERMARK_STATUS_QUEUED,
    WATERMARK_STATUS_RUNNING,
    WatermarkError,
    create_watermarked_copy,
    detect_watermark,
    detection_result_to_dict,
    elapsed_message,
    new_copy_id,
    require_watermark_secret,
    resolve_watermark_output_file,
    validate_recipient_label,
    watermarked_output_name,
    watermark_secret,
)


LOGGER = logging.getLogger(__name__)
PACKAGE_DIR = Path(__file__).resolve().parent
SOURCE_PLATFORM_LABELS = {
    "youtube": "YouTube",
    "twitch": "Twitch",
    "kick": "Kick",
    "rumble": "Rumble",
    "unknown": "Unknown",
}
SOURCE_PLATFORM_INITIALS = {
    "youtube": "Y",
    "twitch": "T",
    "kick": "K",
    "rumble": "R",
    "unknown": "?",
}


def env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError:
        LOGGER.warning("Ignoring invalid float env %s=%r", name, raw)
        return default


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError:
        LOGGER.warning("Ignoring invalid integer env %s=%r", name, raw)
        return default


WEB_SLOW_REQUEST_SECONDS = env_float("ONLYSAVEMEVODS_WEB_SLOW_REQUEST_SECONDS", 0.5)
WEB_SLOW_STEP_SECONDS = env_float("ONLYSAVEMEVODS_WEB_SLOW_STEP_SECONDS", 0.25)
WEB_SLOW_STREAM_SECONDS = env_float("ONLYSAVEMEVODS_WEB_SLOW_STREAM_SECONDS", 0.15)
WEB_SLOW_FILE_SECONDS = env_float("ONLYSAVEMEVODS_WEB_SLOW_FILE_SECONDS", 0.08)
WEB_FILE_SCAN_CACHE_SECONDS = env_float("ONLYSAVEMEVODS_WEB_FILE_SCAN_CACHE_SECONDS", 30.0)
WEB_FILE_SCAN_ACTIVE_CACHE_SECONDS = env_float("ONLYSAVEMEVODS_WEB_FILE_SCAN_ACTIVE_CACHE_SECONDS", 2.0)
WEB_FILE_SCAN_CACHE_MAX_ENTRIES = max(0, env_int("ONLYSAVEMEVODS_WEB_FILE_SCAN_CACHE_MAX_ENTRIES", 32))


def perf_elapsed(started_at: float) -> float:
    return time.perf_counter() - started_at


def perf_step(steps: list[tuple[str, float]], name: str, started_at: float) -> None:
    steps.append((name, perf_elapsed(started_at)))


def should_log_perf(elapsed: float, threshold: float) -> bool:
    return threshold >= 0 and elapsed >= threshold


def format_perf_steps(steps: list[tuple[str, float]]) -> str:
    return ", ".join(f"{name}={elapsed:.3f}s" for name, elapsed in steps)


def log_perf(label: str, elapsed: float, threshold: float, **fields: Any) -> None:
    if not should_log_perf(elapsed, threshold):
        return
    details = " ".join(f"{name}={value}" for name, value in fields.items())
    if details:
        LOGGER.warning("Slow web %s elapsed=%.3fs %s", label, elapsed, details)
    else:
        LOGGER.warning("Slow web %s elapsed=%.3fs", label, elapsed)


def first_existing_dir(*candidates: Path) -> Path:
    for candidate in candidates:
        if candidate.is_dir():
            return candidate
    return candidates[0]


def find_asset_file(directory: Path, *names: str) -> Path | None:
    for name in names:
        candidate = directory / name
        if candidate.is_file():
            return candidate
    if not directory.is_dir():
        return None
    available = {path.name.casefold(): path for path in directory.iterdir() if path.is_file()}
    for name in names:
        match = available.get(name.casefold())
        if match is not None:
            return match
    return None


FAVICON_DIR = first_existing_dir(
    PACKAGE_DIR / "assets" / "favicon",
    PACKAGE_DIR / "assets" / "favicons",
    PACKAGE_DIR / "favicons",
    PACKAGE_DIR,
)
PLATFORM_DIR = first_existing_dir(
    PACKAGE_DIR / "assets" / "platforms",
    PACKAGE_DIR / "platforms",
)
FAVICON_ROUTES = {
    "/favicon.ico": FAVICON_DIR / "favicon.ico",
    "/favicon-16x16.png": FAVICON_DIR / "favicon-16x16.png",
    "/favicon-32x32.png": FAVICON_DIR / "favicon-32x32.png",
    "/apple-touch-icon.png": FAVICON_DIR / "apple-touch-icon.png",
    "/android-chrome-192x192.png": FAVICON_DIR / "android-chrome-192x192.png",
    "/android-chrome-512x512.png": FAVICON_DIR / "android-chrome-512x512.png",
    "/Favicon.png": FAVICON_DIR / "Favicon.png",
}
PLATFORM_ICON_ROUTES = {
    f"/assets/platforms/{platform}.svg": path
    for platform, path in (
        (
            platform,
            find_asset_file(
                PLATFORM_DIR,
                f"{platform}.svg",
                f"{platform.title()}.svg",
                f"{label}.svg",
            ),
        )
        for platform, label in SOURCE_PLATFORM_LABELS.items()
        if platform != "unknown"
    )
    if path is not None
}
PLATFORM_ICON_URLS = {
    route.rsplit("/", 1)[-1].removesuffix(".svg"): route
    for route in PLATFORM_ICON_ROUTES
}
ASSET_ROUTES = {**FAVICON_ROUTES, **PLATFORM_ICON_ROUTES}
STREAM_LIMIT = 100
FILE_LIMIT_PER_STREAM = 80
STREAM_EVENT_LIMIT = 8
LOG_LIMIT = 200
JOB_LIMIT = 200
STREAMER_JOB_PAGE_SIZE = 5
STREAMER_STREAM_PAGE_SIZE = 5
STREAMER_STREAM_PAGE_SIZE_OPTIONS = (5, 10, 25, 50)
CHAT_RENDER_PROGRESS_POLL_SECONDS = 2.0
SEGMENT_NAME_RE = re.compile(
    r"^(?P<segment>segment-\d{3})(?:\.f(?P<format_id>\d+))?"
)
LIVE_CHAT_SUFFIX = ".live_chat.json"
CHAT_RENDER_MEDIA_SUFFIXES = (".mp4", ".mkv", ".webm", ".mov")
CHAT_RENDER_OUTPUT_SUFFIX = " - chat.mp4"
ATTENTION_STATUSES = {"checking_after_exit", "interrupted", "waiting_retry"}
VOD_DOWNLOAD_BLOCKED_STATUSES = {"detected", "downloading", "checking_after_exit", "waiting_retry"}
VOD_DOWNLOAD_PROGRESS_RE = re.compile(r"\[download\]\s+(?P<percent>\d+(?:\.\d+)?)%")
STATUS_LABELS = {
    "checking_after_exit": "checking after exit",
    "detected": "detected",
    "downloading": "downloading",
    "ended": "ended",
    "interrupted": "interrupted",
    "waiting_retry": "waiting retry",
}
CHAT_RENDER_JOBS: dict[str, RenderChatJob] = {}
CHAT_RENDER_JOBS_LOCK = Lock()
CHAT_REFRESH_JOBS: dict[str, RefreshChatJob] = {}
CHAT_REFRESH_JOBS_LOCK = Lock()
TRANSCRIPTION_JOBS: dict[str, TranscriptionJob] = {}
TRANSCRIPTION_JOBS_LOCK = Lock()
EVENT_DETECTION_JOBS: dict[str, EventDetectionJob] = {}
EVENT_DETECTION_JOBS_LOCK = Lock()
WATERMARK_JOB_STATUSES = {
    WATERMARK_STATUS_DONE,
    WATERMARK_STATUS_FAILED,
    WATERMARK_STATUS_INTERRUPTED,
    WATERMARK_STATUS_QUEUED,
    WATERMARK_STATUS_RUNNING,
}


def mark_stale_watermark_jobs(config: BotConfig) -> None:
    state = StateStore(config.db_path)
    try:
        state.mark_stale_watermarks_interrupted()
    finally:
        state.close()


@dataclass(frozen=True, slots=True)
class FileStatus:
    video_id: str
    name: str
    size_bytes: int
    modified_at: float
    kind: str
    segment: str | None
    format_id: str | None
    download_url: str | None
    render_chat_url: str | None
    render_chat_output_url: str | None
    render_chat_status: str | None
    render_chat_message: str | None
    refresh_chat_url: str | None
    refresh_chat_status: str | None
    refresh_chat_message: str | None
    transcription_url: str | None
    transcription_status: str | None
    transcription_message: str | None
    event_detection_url: str | None
    event_detection_status: str | None
    event_detection_message: str | None
    watermark_url: str | None
    watermark_copies: list[WatermarkCopyStatus]
    watermark_copy_id: str | None
    watermark_recipient_label: str | None
    watermark_delete_url: str | None


@dataclass(frozen=True, slots=True)
class CachedFileEntry:
    name: str
    size_bytes: int
    modified_at: float
    kind: str
    segment: str | None
    format_id: str | None


@dataclass(frozen=True, slots=True)
class DirectoryScanSummary:
    directory_entry_count: int
    file_count: int
    total_bytes: int
    bytes_by_kind: dict[str, int]
    counts_by_kind: dict[str, int]
    latest_modified_at: float | None
    part_segments: tuple[str, ...]
    final_format_segments: tuple[str, ...]
    visible_entries: tuple[CachedFileEntry, ...]


@dataclass(frozen=True, slots=True)
class DirectoryScanCacheEntry:
    fingerprint: tuple[int, int]
    cached_at: float
    summary: DirectoryScanSummary


@dataclass(frozen=True, slots=True)
class StreamFileSummary:
    file_count: int
    total_bytes: int
    bytes_by_kind: dict[str, int]
    counts_by_kind: dict[str, int]
    latest_modified_at: float | None
    part_segments: tuple[str, ...]
    final_format_segments: tuple[str, ...]
    files: list[FileStatus]


FILE_SCAN_CACHE: OrderedDict[str, DirectoryScanCacheEntry] = OrderedDict()
FILE_SCAN_CACHE_LOCK = Lock()


@dataclass(frozen=True, slots=True)
class WatermarkCopyStatus:
    copy_id: str
    source_name: str
    output_name: str
    recipient_label: str
    status: str
    message: str
    error: str
    phase: str
    progress: float | None
    created_at: str
    updated_at: str
    started_at: str | None
    finished_at: str | None
    download_url: str | None
    delete_url: str | None


@dataclass(frozen=True, slots=True)
class RenderChatJob:
    video_id: str
    chat_name: str
    media_name: str
    output_name: str
    status: str
    message: str
    started_at: float
    finished_at: float | None = None
    phase: str = ""
    progress: float | None = None
    updated_at: float | None = None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class RefreshChatJob:
    video_id: str
    chat_name: str
    media_name: str
    status: str
    message: str
    started_at: float
    finished_at: float | None = None
    phase: str = ""
    progress: float | None = None
    updated_at: float | None = None


@dataclass(frozen=True, slots=True)
class TranscriptionJob:
    video_id: str
    media_name: str
    status: str
    message: str
    started_at: float
    finished_at: float | None = None
    phase: str = ""
    progress: float | None = None
    updated_at: float | None = None


@dataclass(frozen=True, slots=True)
class EventDetectionJob:
    video_id: str
    media_name: str
    status: str
    message: str
    started_at: float
    finished_at: float | None = None
    phase: str = ""
    progress: float | None = None
    updated_at: float | None = None


@dataclass(frozen=True, slots=True)
class JobStatus:
    job_id: str
    kind: str
    status: str
    phase: str
    progress: float | None
    video_id: str
    item: str
    detail: str
    message: str
    started_at: float | None
    updated_at: float | None
    finished_at: float | None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class StreamEventStatus:
    event_id: int
    level: str
    message: str
    segment_index: int | None
    created_at: str


@dataclass(frozen=True, slots=True)
class ContentEventStatus:
    media_name: str
    start: float
    end: float
    duration: float
    rule: str
    severity: str
    score: float
    loudness_dbfs: float | None
    labels: list[dict[str, Any]]
    keywords: list[str]
    voice: str
    text: str


@dataclass(frozen=True, slots=True)
class PowerchatEventStatus:
    source: str
    received_at: str
    offset_seconds: float | None
    kind: str
    donor: str
    platform: str
    message: str
    money_amount: float | None
    money_currency: str
    unit_amount: float | None
    unit: str


@dataclass(frozen=True, slots=True)
class StreamStatus:
    video_id: str
    title: str
    channel: str
    url: str
    platform: str
    source: str
    status: str
    segment_index: int
    first_seen_at: str
    updated_at: str
    last_started_at: str | None
    last_exit_at: str | None
    exit_code: int | None
    directory: str
    file_count: int
    total_bytes: int
    part_bytes: int
    final_bytes: int
    chat_bytes: int
    fragment_bytes: int
    state_bytes: int
    temporary_bytes: int
    file_kind_counts: dict[str, int]
    latest_file_modified_at: float | None
    has_part_files: bool
    has_mixed_formats: bool
    events: list[StreamEventStatus]
    content_event_count: int
    content_events: list[ContentEventStatus]
    powerchat_event_count: int
    powerchat_money_totals: list[dict[str, Any]]
    powerchat_unit_totals: list[dict[str, Any]]
    powerchat_events: list[PowerchatEventStatus]
    jobs: list[JobStatus]
    files: list[FileStatus]


@dataclass(frozen=True, slots=True)
class ChannelStatus:
    name: str
    configured_sources: list[str]
    stream_count: int
    active_count: int
    checking_count: int
    ended_count: int
    attention_count: int
    file_count: int
    downloadable_count: int
    total_bytes: int
    part_bytes: int
    final_bytes: int
    chat_bytes: int
    fragment_bytes: int
    latest_updated_at: str | None
    latest_file_modified_at: float | None


@dataclass(frozen=True, slots=True)
class SpeakerLabelStatus:
    channel: str
    configured_sources: list[str]
    detected_labels: list[str]
    labels: dict[str, str]
    transcript_count: int


@dataclass(frozen=True, slots=True)
class VoiceProfileStatus:
    name: str
    enabled: bool
    sample_count: int
    threshold: float
    notes: str
    samples: list[str]


@dataclass(frozen=True, slots=True)
class StreamerStatus:
    name: str
    sources: list[str]
    download_dir_name: str
    powerchat_enabled: bool
    powerchat_username: str
    voice_detection: str
    speaker_label_count: int
    voices: list[VoiceProfileStatus]
    stream_event_detection: dict[str, Any]
    stream_event_rules: list[dict[str, Any]]


@dataclass(frozen=True, slots=True)
class StreamerStatStatus:
    name: str
    sources: list[str]
    download_dir_name: str
    powerchat_enabled: bool
    powerchat_username: str
    configured: bool
    needs_grouping: bool
    voice_detection: str
    speaker_label_count: int
    voices: list[VoiceProfileStatus]
    stream_event_detection: dict[str, Any]
    stream_event_rules: list[dict[str, Any]]
    stream_count: int
    active_count: int
    checking_count: int
    ended_count: int
    attention_count: int
    file_count: int
    downloadable_count: int
    total_bytes: int
    part_bytes: int
    final_bytes: int
    chat_bytes: int
    fragment_bytes: int
    latest_updated_at: str | None
    latest_file_modified_at: float | None
    latest_activity_at: float | None
    jobs: list[JobStatus]
    streams: list[StreamStatus]


@dataclass(frozen=True, slots=True)
class AppInfo:
    name: str
    version: str
    python_version: str
    executable: str
    platform: str


@dataclass(frozen=True, slots=True)
class ConfigFormField:
    key: str
    section: str
    kind: str
    options: tuple[str, ...] = ()
    minimum: int | None = None
    rows: int = 1


@dataclass(frozen=True, slots=True)
class StatusSnapshot:
    generated_at: float
    stream_revision: str
    job_revision: str
    app: AppInfo
    download_dir: str
    state_db: str
    counts: dict[str, int]
    total_bytes: int
    part_bytes: int
    final_bytes: int
    chat_bytes: int
    fragment_bytes: int
    state_bytes: int
    temporary_bytes: int
    stream_limit: int
    configured_channels: list[str]
    streamer_stats: list[StreamerStatStatus]
    powerchat_stats: dict[str, Any]
    streamer_groups: list[StreamerStatus]
    configuration: dict[str, dict[str, Any]]
    channel_stats: list[ChannelStatus]
    speaker_labels: list[SpeakerLabelStatus]
    recent_logs: list[LogEntry]
    log_limit: int
    jobs: list[JobStatus]
    job_limit: int
    streams: list[StreamStatus]


CONFIG_FORM_FIELDS: tuple[ConfigFormField, ...] = (
    ConfigFormField("channels", "Channels", "str_list", rows=5),
    ConfigFormField("download_dir", "Paths", "text"),
    ConfigFormField("state_dir", "Paths", "text"),
    ConfigFormField("poll_interval_seconds", "Discovery", "int", minimum=1),
    ConfigFormField("channel_scan_limit", "Discovery", "int", minimum=1),
    ConfigFormField("discovery_probe_concurrency", "Discovery", "int", minimum=1),
    ConfigFormField("max_concurrent_downloads", "Discovery", "int", minimum=1),
    ConfigFormField("live_from_start", "Download", "bool"),
    ConfigFormField("keep_fragments_for_resume", "Download", "bool"),
    ConfigFormField("reconnect_interval_seconds", "Download", "int", minimum=0),
    ConfigFormField("post_exit_check_seconds", "Download", "int_list", rows=3),
    ConfigFormField("retry_backoff_seconds", "Download", "int_list", rows=2),
    ConfigFormField("extra_yt_dlp_args", "Download", "extra_args", rows=4),
    ConfigFormField("record_live_chat", "Live Chat", "bool"),
    ConfigFormField("render_live_chat_video", "Live Chat", "bool"),
    ConfigFormField("chat_render_panel_workers", "Live Chat", "int", minimum=0),
    ConfigFormField("chat_render_timeout_seconds", "Live Chat", "int", minimum=0),
    ConfigFormField("chat_render_use_nvenc", "Live Chat", "bool"),
    ConfigFormField("chat_render_nvenc_devices", "Live Chat", "str_list", rows=2),
    ConfigFormField("transcribe_subtitles", "Transcription", "bool"),
    ConfigFormField("transcription_max_concurrent", "Transcription", "int", minimum=1),
    ConfigFormField("whisperx_path", "Transcription", "text"),
    ConfigFormField("whisperx_model", "Transcription", "text"),
    ConfigFormField("whisperx_device", "Transcription", "text"),
    ConfigFormField("whisperx_compute_type", "Transcription", "text"),
    ConfigFormField("whisperx_batch_size", "Transcription", "int", minimum=1),
    ConfigFormField("whisperx_language", "Transcription", "optional_text"),
    ConfigFormField("voice_match_enabled", "Transcription", "bool"),
    ConfigFormField("voice_match_model", "Transcription", "text"),
    ConfigFormField("voice_match_threshold", "Transcription", "float", minimum=0),
    ConfigFormField("voice_match_min_margin", "Transcription", "float", minimum=0),
    ConfigFormField("voice_sample_max_bytes", "Transcription", "int", minimum=1),
    ConfigFormField("stream_event_detection_enabled", "Content Events", "bool"),
    ConfigFormField("stream_event_model", "Content Events", "text"),
    ConfigFormField("stream_event_device", "Content Events", "text"),
    ConfigFormField("stream_event_window_seconds", "Content Events", "float", minimum=0.001),
    ConfigFormField("stream_event_hop_seconds", "Content Events", "float", minimum=0.001),
    ConfigFormField("stream_event_min_confidence", "Content Events", "float", minimum=0),
    ConfigFormField("stream_event_max_events_per_media", "Content Events", "int", minimum=1),
    ConfigFormField("twitch_ad_repair_enabled", "Twitch Ads", "bool"),
    ConfigFormField("twitch_ad_repair_tesseract_path", "Twitch Ads", "text"),
    ConfigFormField("twitch_ad_repair_scan_seconds", "Twitch Ads", "int", minimum=0),
    ConfigFormField("twitch_ad_repair_sample_seconds", "Twitch Ads", "int", minimum=1),
    ConfigFormField("twitch_ad_repair_max_seconds", "Twitch Ads", "int", minimum=1),
    ConfigFormField("twitch_ad_repair_vod_search_limit", "Twitch Ads", "int", minimum=1),
    ConfigFormField("web_enabled", "Web", "bool"),
    ConfigFormField("web_host", "Web", "text"),
    ConfigFormField("web_port", "Web", "int", minimum=1),
    ConfigFormField("log_level", "Tools", "choice", options=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")),
    ConfigFormField("yt_dlp_path", "Tools", "text"),
    ConfigFormField("ffmpeg_path", "Tools", "text"),
    ConfigFormField("watermark_enabled", "Watermark", "bool"),
    ConfigFormField("watermark_secret_env", "Watermark", "text"),
    ConfigFormField("watermark_strength", "Watermark", "choice", options=("invisible", "balanced", "robust")),
    ConfigFormField("watermark_detect_upload_max_bytes", "Watermark", "int", minimum=1),
)
CONFIG_FORM_SECTIONS = tuple(dict.fromkeys(field.section for field in CONFIG_FORM_FIELDS))


class StatusWebServer:
    def __init__(
        self,
        config: BotConfig,
        *,
        host: str | None = None,
        port: int | None = None,
        logger: logging.Logger = LOGGER,
    ) -> None:
        self.config = config
        self.host = host if host is not None else config.web_host
        self.port = port if port is not None else config.web_port
        self.logger = logger
        self._server: ThreadingHTTPServer | None = None
        self._thread: Thread | None = None

    def start(self) -> None:
        if self._server is not None:
            return

        mark_stale_watermark_jobs(self.config)
        self.logger.info(
            "Starting status web interface on %s:%s",
            self.host,
            self.port,
        )
        handler = build_handler(self.config)
        server = ThreadingHTTPServer((self.host, self.port), handler)
        server.daemon_threads = True
        thread = Thread(target=server.serve_forever, name="onlysavemevods-web", daemon=True)
        thread.start()

        self._server = server
        self._thread = thread
        actual_host, actual_port = server.server_address[:2]
        self.logger.info(
            "Status web interface listening on http://%s:%s",
            actual_host,
            actual_port,
        )

    def serve_forever(self) -> None:
        if self._server is not None:
            raise RuntimeError("Status web server already started")
        mark_stale_watermark_jobs(self.config)
        self.logger.info(
            "Starting status web interface on %s:%s",
            self.host,
            self.port,
        )
        handler = build_handler(self.config)
        with ThreadingHTTPServer((self.host, self.port), handler) as server:
            server.daemon_threads = True
            actual_host, actual_port = server.server_address[:2]
            self.logger.info(
                "Status web interface listening on http://%s:%s",
                actual_host,
                actual_port,
            )
            server.serve_forever()

    def stop(self) -> None:
        server = self._server
        if server is None:
            return

        server.shutdown()
        server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._server = None
        self._thread = None


def build_handler(config: BotConfig) -> type[BaseHTTPRequestHandler]:
    class StatusRequestHandler(BaseHTTPRequestHandler):
        server_version = "ONLYSAVEmeVODSStatus/1.0"

        def do_GET(self) -> None:
            started_at = time.perf_counter()
            parts = urlsplit(self.path)
            path = parts.path
            try:
                if path in ("", "/", "/status"):
                    self._send_html(render_status_html(build_status_snapshot(config, include_speaker_scan=False)))
                    return
                if path == "/status.json":
                    self._send_status_json(parts.query)
                    return
                if path == "/streamer-voice-details":
                    self._send_streamer_voice_details(parts.query)
                    return
                if path == "/stream-voice-speakers":
                    self._send_stream_voice_speakers(parts.query)
                    return
                if path == "/healthz":
                    self._send_text("ok\n", "text/plain; charset=utf-8")
                    return
                asset_path = ASSET_ROUTES.get(path)
                if asset_path is not None:
                    self._send_package_asset(asset_path)
                    return
                if path == "/download":
                    self._send_download(parts.query)
                    return
                if path == "/download-watermark":
                    self._send_watermark_download(parts.query)
                    return
                if path == "/powerchat-events":
                    self._send_powerchat_events(parts.query)
                    return
                self.send_error(HTTPStatus.NOT_FOUND)
            finally:
                log_perf(
                    "request",
                    perf_elapsed(started_at),
                    WEB_SLOW_REQUEST_SECONDS,
                    method="GET",
                    path=path or "/",
                    query="yes" if parts.query else "no",
                )

        def do_POST(self) -> None:
            started_at = time.perf_counter()
            parts = urlsplit(self.path)
            path = parts.path
            try:
                if path == "/render-chat":
                    self._start_render_chat(parts.query)
                    return
                if path == "/refresh-chat":
                    self._start_refresh_chat(parts.query)
                    return
                if path == "/transcribe":
                    self._start_transcription(parts.query)
                    return
                if path == "/cleanup-fragments":
                    self._cleanup_fragments(parts.query)
                    return
                if path == "/delete-stream":
                    self._delete_stream()
                    return
                if path == "/vod-download":
                    self._start_vod_download()
                    return
                if path == "/detect-events":
                    self._start_event_detection(parts.query)
                    return
                if path == "/voice-detection":
                    self._update_voice_detection()
                    return
                if path == "/stream-event-rules":
                    self._update_stream_event_rules()
                    return
                if path == "/speaker-labels":
                    self._update_speaker_labels()
                    return
                if path == "/streamer-voices":
                    self._update_streamer_voice()
                    return
                if path == "/streamer-voice-samples":
                    self._upload_streamer_voice_sample()
                    return
                if path == "/streamer-voice-samples/from-transcript":
                    self._create_streamer_voice_sample_from_transcript()
                    return
                if path == "/streamer-voice-attributions":
                    self._update_streamer_voice_attribution()
                    return
                if path == "/streamers":
                    self._update_streamers()
                    return
                if path == "/config":
                    self._update_config()
                    return
                if path == "/watermark":
                    self._start_watermark()
                    return
                if path == "/delete-watermark":
                    self._delete_watermark()
                    return
                if path == "/detect-watermark":
                    self._detect_watermark_upload()
                    return
                self.send_error(HTTPStatus.NOT_FOUND)
            finally:
                log_perf(
                    "request",
                    perf_elapsed(started_at),
                    WEB_SLOW_REQUEST_SECONDS,
                    method="POST",
                    path=path or "/",
                    query="yes" if parts.query else "no",
                )

        def log_message(self, fmt: str, *args: Any) -> None:
            LOGGER.debug("status web: " + fmt, *args)

        def _send_html(self, body: str) -> None:
            self._send_text(body, "text/html; charset=utf-8")

        def _send_status_json(self, query: str) -> None:
            started_at = time.perf_counter()
            params = parse_qs(query)
            if query_flag(params, "lite"):
                payload = build_lite_status_payload(config)
                log_perf(
                    "status-json-build",
                    perf_elapsed(started_at),
                    WEB_SLOW_STEP_SECONDS,
                    detail="lite",
                )
                self._send_json(payload)
                return
            include_speaker_scan = not query_flag(params, "dashboard")
            snapshot = build_status_snapshot(
                config,
                include_speaker_scan=include_speaker_scan,
            )
            log_perf(
                "status-json-build",
                perf_elapsed(started_at),
                WEB_SLOW_STEP_SECONDS,
                detail="full",
                include_speaker_scan=include_speaker_scan,
            )
            self._send_json(snapshot_to_dict(snapshot))

        def _send_json(self, payload: dict[str, Any]) -> None:
            started_at = time.perf_counter()
            encode_started_at = time.perf_counter()
            body = json.dumps(payload, indent=2, sort_keys=True) + "\n"
            encode_elapsed = perf_elapsed(encode_started_at)
            self._send_text(body, "application/json; charset=utf-8")
            log_perf(
                "json-response",
                perf_elapsed(started_at),
                WEB_SLOW_STEP_SECONDS,
                bytes=len(body.encode("utf-8")),
                encode=f"{encode_elapsed:.3f}s",
            )

        def _send_package_asset(self, path: Path) -> None:
            if path not in ASSET_ROUTES.values():
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            if not path.is_file():
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
            stat = path.stat()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(stat.st_size))
            self.send_header("Cache-Control", "public, max-age=86400")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            with path.open("rb") as handle:
                shutil.copyfileobj(handle, self.wfile)

        def _send_download(self, query: str) -> None:
            params = parse_qs(query)
            video_id = first_query_value(params, "video_id")
            filename = first_query_value(params, "name")
            path = resolve_download_file(config, video_id, filename)
            if path is None:
                self.send_error(HTTPStatus.NOT_FOUND)
                return

            content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
            stat = path.stat()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(stat.st_size))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header(
                "Content-Disposition",
                f"attachment; filename*=UTF-8''{quote(path.name)}",
            )
            self.end_headers()
            with path.open("rb") as file:
                shutil.copyfileobj(file, self.wfile)

        def _send_powerchat_events(self, query: str) -> None:
            params = parse_qs(query)
            export_format = (first_query_value(params, "format") or "json").strip().lower()
            if export_format not in {"json", "csv"}:
                self.send_error(HTTPStatus.BAD_REQUEST, "format must be json or csv")
                return
            payload = build_powerchat_export_payload(config, params)
            filename = powerchat_export_filename(payload["filters"], export_format)
            if export_format == "csv":
                body = powerchat_export_csv(payload["events"])
                content_type = "text/csv; charset=utf-8"
            else:
                body = json.dumps(payload, indent=2, sort_keys=True) + "\n"
                content_type = "application/json; charset=utf-8"

            encoded = body.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(encoded)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header(
                "Content-Disposition",
                f"attachment; filename*=UTF-8''{quote(filename)}",
            )
            self.end_headers()
            self.wfile.write(encoded)

        def _send_streamer_voice_details(self, query: str) -> None:
            params = parse_qs(query)
            streamer_name = first_query_value(params, "streamer").strip()
            try:
                payload = build_streamer_voice_details_payload(config, streamer_name)
            except ConfigError as exc:
                self.send_error(HTTPStatus.BAD_REQUEST, str(exc))
                return
            self._send_json(payload)

        def _send_stream_voice_speakers(self, query: str) -> None:
            params = parse_qs(query)
            streamer_name = first_query_value(params, "streamer").strip()
            video_id = first_query_value(params, "video_id").strip()
            try:
                payload = build_stream_voice_speakers_payload(config, streamer_name, video_id)
            except ConfigError as exc:
                self.send_error(HTTPStatus.BAD_REQUEST, str(exc))
                return
            self._send_json(payload)

        def _send_watermark_download(self, query: str) -> None:
            params = parse_qs(query)
            copy_id = first_query_value(params, "copy_id")
            path = resolve_watermark_download_file(config, copy_id)
            if path is None:
                self.send_error(HTTPStatus.NOT_FOUND)
                return

            content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
            stat = path.stat()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(stat.st_size))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header(
                "Content-Disposition",
                f"attachment; filename*=UTF-8''{quote(path.name)}",
            )
            self.end_headers()
            with path.open("rb") as file:
                shutil.copyfileobj(file, self.wfile)

        def _start_render_chat(self, query: str) -> None:
            self._discard_request_body()
            params = parse_qs(query)
            video_id = first_query_value(params, "video_id")
            chat_name = first_query_value(params, "chat")
            regenerate = first_query_value(params, "regenerate").lower() in {
                "1",
                "true",
                "yes",
            }
            ok, message = start_render_chat_job(
                config,
                video_id,
                chat_name,
                regenerate=regenerate,
            )
            if not ok:
                self.send_error(HTTPStatus.BAD_REQUEST, message)
                return

            self.send_response(HTTPStatus.SEE_OTHER)
            self.send_header("Location", "/#streamers")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()

        def _start_refresh_chat(self, query: str) -> None:
            self._discard_request_body()
            params = parse_qs(query)
            video_id = first_query_value(params, "video_id")
            chat_name = first_query_value(params, "chat")
            ok, message = start_refresh_chat_job(config, video_id, chat_name)
            if not ok:
                self.send_error(HTTPStatus.BAD_REQUEST, message)
                return

            self.send_response(HTTPStatus.SEE_OTHER)
            self.send_header("Location", "/#streamers")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()

        def _start_transcription(self, query: str) -> None:
            self._discard_request_body()
            params = parse_qs(query)
            video_id = first_query_value(params, "video_id")
            filename = first_query_value(params, "name")
            regenerate = first_query_value(params, "regenerate").lower() in {
                "1",
                "true",
                "yes",
            }
            ok, message = start_transcription_job(
                config,
                video_id,
                filename,
                regenerate=regenerate,
            )
            if not ok:
                self.send_error(HTTPStatus.BAD_REQUEST, message)
                return

            self.send_response(HTTPStatus.SEE_OTHER)
            self.send_header("Location", "/#streamers")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()

        def _cleanup_fragments(self, query: str) -> None:
            self._discard_request_body()
            params = parse_qs(query)
            video_id = first_query_value(params, "video_id")
            try:
                count, bytes_removed = cleanup_stream_fragments(config, video_id)
            except ConfigError as exc:
                self.send_error(HTTPStatus.BAD_REQUEST, str(exc))
                return

            LOGGER.info(
                "Cleaned stream fragments video_id=%s files=%s bytes=%s",
                video_id,
                count,
                bytes_removed,
            )
            self.send_response(HTTPStatus.SEE_OTHER)
            self.send_header("Location", "/#streamers")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()

        def _delete_stream(self) -> None:
            body = self._read_request_body(4096)
            if body is None:
                return
            try:
                params = parse_qs(body.decode("utf-8", "replace"))
            except UnicodeDecodeError:
                self.send_error(HTTPStatus.BAD_REQUEST, "Invalid form body")
                return
            video_id = first_query_value(params, "video_id")
            confirm_delete = first_query_value(params, "confirm_delete")
            ok, message = delete_stream(config, video_id, confirm_delete)
            if not ok:
                self.send_error(HTTPStatus.BAD_REQUEST, message)
                return

            self.send_response(HTTPStatus.SEE_OTHER)
            self.send_header("Location", "/#streamers")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()

        def _start_vod_download(self) -> None:
            body = self._read_request_body(16 * 1024)
            if body is None:
                return
            try:
                params = parse_qs(
                    body.decode("utf-8", "replace"),
                    keep_blank_values=True,
                )
            except UnicodeDecodeError:
                self.send_error(HTTPStatus.BAD_REQUEST, "Invalid form body")
                return

            action = first_query_value(params, "action") or "redownload"
            vod_url = first_query_value(params, "vod_url")
            if action == "manual":
                ok, message = start_manual_vod_download_job(
                    config,
                    first_query_value(params, "streamer_name"),
                    vod_url,
                )
            else:
                ok, message = start_vod_redownload_job(
                    config,
                    first_query_value(params, "video_id"),
                    vod_url,
                )
            if not ok:
                self.send_error(HTTPStatus.BAD_REQUEST, message)
                return

            self.send_response(HTTPStatus.SEE_OTHER)
            self.send_header("Location", "/#streamers")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()

        def _start_event_detection(self, query: str) -> None:
            self._discard_request_body()
            params = parse_qs(query)
            video_id = first_query_value(params, "video_id")
            filename = first_query_value(params, "name")
            regenerate = first_query_value(params, "regenerate").lower() in {
                "1",
                "true",
                "yes",
            }
            ok, message = start_event_detection_job(
                config,
                video_id,
                filename,
                regenerate=regenerate,
            )
            if not ok:
                self.send_error(HTTPStatus.BAD_REQUEST, message)
                return

            self.send_response(HTTPStatus.SEE_OTHER)
            self.send_header("Location", "/#streamers")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()

        def _update_voice_detection(self) -> None:
            body = self._read_request_body(32 * 1024)
            if body is None:
                return
            try:
                params = parse_qs(body.decode("utf-8", "replace"))
                update_voice_detection_from_form(config, params)
            except ConfigError as exc:
                self.send_error(HTTPStatus.BAD_REQUEST, str(exc))
                return

            self.send_response(HTTPStatus.SEE_OTHER)
            self.send_header("Location", "/#config")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()

        def _update_stream_event_rules(self) -> None:
            body = self._read_request_body(128 * 1024)
            if body is None:
                return
            try:
                params = parse_qs(
                    body.decode("utf-8", "replace"),
                    keep_blank_values=True,
                )
                update_stream_event_rules_from_form(config, params)
            except ConfigError as exc:
                self.send_error(HTTPStatus.BAD_REQUEST, str(exc))
                return

            self.send_response(HTTPStatus.SEE_OTHER)
            self.send_header("Location", "/#config")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()

        def _update_speaker_labels(self) -> None:
            body = self._read_request_body(64 * 1024)
            if body is None:
                return
            try:
                params = parse_qs(
                    body.decode("utf-8", "replace"),
                    keep_blank_values=True,
                )
                update_speaker_labels_from_form(config, params)
            except ConfigError as exc:
                self.send_error(HTTPStatus.BAD_REQUEST, str(exc))
                return

            self.send_response(HTTPStatus.SEE_OTHER)
            self.send_header("Location", "/#config")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()

        def _update_streamer_voice(self) -> None:
            content_type = self.headers.get("Content-Type", "")
            try:
                if content_type.startswith("multipart/form-data"):
                    length_header = self.headers.get("Content-Length", "0")
                    try:
                        length = int(length_header)
                    except ValueError:
                        self.send_error(HTTPStatus.BAD_REQUEST, "Invalid content length")
                        return
                    if length > config.voice_sample_max_bytes + 1024 * 1024:
                        self.send_error(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "Upload is too large")
                        self.rfile.read(min(length, 1024 * 1024))
                        return
                    body = self.rfile.read(length)
                    fields, files = parse_multipart_form(content_type, body)
                    update_streamer_voice_with_optional_sample(config, fields, files)
                else:
                    body = self._read_request_body(128 * 1024)
                    if body is None:
                        return
                    params = parse_qs(
                        body.decode("utf-8", "replace"),
                        keep_blank_values=True,
                    )
                    update_streamer_voice_from_form(config, params)
            except ConfigError as exc:
                self.send_error(HTTPStatus.BAD_REQUEST, str(exc))
                return

            self.send_response(HTTPStatus.SEE_OTHER)
            self.send_header("Location", "/#streamers")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()

        def _upload_streamer_voice_sample(self) -> None:
            length_header = self.headers.get("Content-Length", "0")
            try:
                length = int(length_header)
            except ValueError:
                self.send_error(HTTPStatus.BAD_REQUEST, "Invalid content length")
                return
            if length <= 0:
                self.send_error(HTTPStatus.BAD_REQUEST, "No upload supplied")
                return
            if length > config.voice_sample_max_bytes + 1024 * 1024:
                self.send_error(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "Upload is too large")
                self.rfile.read(min(length, 1024 * 1024))
                return
            content_type = self.headers.get("Content-Type", "")
            if not content_type.startswith("multipart/form-data"):
                self.send_error(HTTPStatus.BAD_REQUEST, "Expected multipart form upload")
                self.rfile.read(length)
                return
            body = self.rfile.read(length)
            try:
                fields, files = parse_multipart_form(content_type, body)
                store_streamer_voice_sample_upload(config, fields, files)
            except ConfigError as exc:
                self.send_error(HTTPStatus.BAD_REQUEST, str(exc))
                return

            self.send_response(HTTPStatus.SEE_OTHER)
            self.send_header("Location", "/#streamers")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()

        def _create_streamer_voice_sample_from_transcript(self) -> None:
            body = self._read_request_body(64 * 1024)
            if body is None:
                return
            try:
                params = parse_qs(
                    body.decode("utf-8", "replace"),
                    keep_blank_values=True,
                )
                create_streamer_voice_sample_from_transcript_form(config, params)
            except ConfigError as exc:
                self.send_error(HTTPStatus.BAD_REQUEST, str(exc))
                return

            self.send_response(HTTPStatus.SEE_OTHER)
            self.send_header("Location", "/#streamers")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()

        def _update_streamer_voice_attribution(self) -> None:
            body = self._read_request_body(64 * 1024)
            if body is None:
                return
            try:
                params = parse_qs(
                    body.decode("utf-8", "replace"),
                    keep_blank_values=True,
                )
                update_streamer_voice_attribution_from_form(config, params)
            except ConfigError as exc:
                self.send_error(HTTPStatus.BAD_REQUEST, str(exc))
                return

            self.send_response(HTTPStatus.SEE_OTHER)
            self.send_header("Location", "/#streamers")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()

        def _update_config(self) -> None:
            body = self._read_request_body(256 * 1024)
            if body is None:
                return
            try:
                params = parse_qs(
                    body.decode("utf-8", "replace"),
                    keep_blank_values=True,
                )
                update_app_config_from_form(config, params)
            except ConfigError as exc:
                self.send_error(HTTPStatus.BAD_REQUEST, str(exc))
                return

            self.send_response(HTTPStatus.SEE_OTHER)
            self.send_header("Location", "/#config")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()

        def _update_streamers(self) -> None:
            body = self._read_request_body(64 * 1024)
            if body is None:
                return
            try:
                params = parse_qs(
                    body.decode("utf-8", "replace"),
                    keep_blank_values=True,
                )
                update_streamer_from_form(config, params)
            except ConfigError as exc:
                self.send_error(HTTPStatus.BAD_REQUEST, str(exc))
                return

            self.send_response(HTTPStatus.SEE_OTHER)
            self.send_header("Location", "/#streamers")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()

        def _start_watermark(self) -> None:
            body = self._read_request_body(config.watermark_detect_upload_max_bytes)
            if body is None:
                return
            try:
                params = parse_qs(body.decode("utf-8", "replace"))
            except UnicodeDecodeError:
                self.send_error(HTTPStatus.BAD_REQUEST, "Invalid form body")
                return
            video_id = first_query_value(params, "video_id")
            filename = first_query_value(params, "name")
            recipient_label = first_query_value(params, "recipient_label")
            ok, message = start_watermark_job(
                config,
                video_id,
                filename,
                recipient_label,
            )
            if not ok:
                self.send_error(HTTPStatus.BAD_REQUEST, message)
                return

            self.send_response(HTTPStatus.SEE_OTHER)
            self.send_header("Location", "/#streamers")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()

        def _delete_watermark(self) -> None:
            body = self._read_request_body(4096)
            if body is None:
                return
            try:
                params = parse_qs(body.decode("utf-8", "replace"))
            except UnicodeDecodeError:
                self.send_error(HTTPStatus.BAD_REQUEST, "Invalid form body")
                return
            copy_id = first_query_value(params, "copy_id")
            ok, message = delete_watermark_copy(config, copy_id)
            if not ok:
                self.send_error(HTTPStatus.BAD_REQUEST, message)
                return

            self.send_response(HTTPStatus.SEE_OTHER)
            self.send_header("Location", "/#streamers")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()

        def _detect_watermark_upload(self) -> None:
            length_header = self.headers.get("Content-Length", "0")
            try:
                length = int(length_header)
            except ValueError:
                self.send_error(HTTPStatus.BAD_REQUEST, "Invalid content length")
                return
            if length <= 0:
                self.send_error(HTTPStatus.BAD_REQUEST, "No upload supplied")
                return
            if length > config.watermark_detect_upload_max_bytes:
                self.send_error(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "Upload is too large")
                self.rfile.read(min(length, 1024 * 1024))
                return

            content_type = self.headers.get("Content-Type", "")
            if not content_type.startswith("multipart/form-data"):
                self.send_error(HTTPStatus.BAD_REQUEST, "Expected multipart form upload")
                self.rfile.read(length)
                return

            body = self.rfile.read(length)
            upload_filename, upload_bytes = parse_multipart_upload(
                content_type,
                body,
                "media",
            )
            if upload_bytes is None:
                self.send_error(HTTPStatus.BAD_REQUEST, "Missing media upload")
                return

            suffix = Path(upload_filename or "suspect.mp4").suffix or ".mp4"
            temp_path = ""
            try:
                with tempfile.NamedTemporaryFile(
                    suffix=suffix,
                    prefix="onlysavemevods-watermark-",
                    delete=False,
                ) as temp:
                    temp.write(upload_bytes)
                    temp_path = temp.name
                result = detect_watermark_file(config, Path(temp_path))
            except WatermarkError as exc:
                self._send_html(render_watermark_detection_error(str(exc)))
                return
            finally:
                if temp_path:
                    try:
                        Path(temp_path).unlink(missing_ok=True)
                    except OSError:
                        LOGGER.debug("Unable to remove watermark upload temp file %s", temp_path)

            self._send_html(render_watermark_detection_result(result))

        def _read_request_body(self, max_bytes: int) -> bytes | None:
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                self.send_error(HTTPStatus.BAD_REQUEST, "Invalid content length")
                return None
            if length > max_bytes:
                self.send_error(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "Request body is too large")
                self.rfile.read(min(length, 1024 * 1024))
                return None
            return self.rfile.read(length) if length > 0 else b""

        def _discard_request_body(self) -> None:
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                length = 0
            if length > 0:
                self.rfile.read(length)

        def _send_text(self, body: str, content_type: str) -> None:
            encoded = body.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(encoded)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(encoded)

    return StatusRequestHandler


def build_status_snapshot(
    config: BotConfig,
    *,
    include_speaker_scan: bool = True,
) -> StatusSnapshot:
    started_at = time.perf_counter()
    steps: list[tuple[str, float]] = []

    step_started_at = time.perf_counter()
    state = StateStore(config.db_path)
    try:
        records = state.list_streams(STREAM_LIMIT)
        watermark_records = state.list_watermark_copies(limit=1000)
        stream_events = state.list_stream_events(
            [record.video_id for record in records],
            limit_per_stream=STREAM_EVENT_LIMIT,
        )
    finally:
        state.close()
    perf_step(steps, "db", step_started_at)

    step_started_at = time.perf_counter()
    watermarks_by_video: dict[str, list[WatermarkCopyRecord]] = {}
    for watermark_record in watermark_records:
        watermarks_by_video.setdefault(watermark_record.video_id, []).append(
            watermark_record
        )

    jobs = build_job_statuses(watermark_records)
    jobs_by_video: dict[str, list[JobStatus]] = {}
    for job in jobs:
        jobs_by_video.setdefault(job.video_id, []).append(job)
    perf_step(steps, "jobs", step_started_at)

    step_started_at = time.perf_counter()
    streams = [
        stream_status_from_record(
            config,
            record,
            watermarks_by_video.get(record.video_id, []),
            stream_events.get(record.video_id, []),
            jobs_by_video.get(record.video_id, []),
        )
        for record in records
    ]
    perf_step(steps, "streams", step_started_at)

    step_started_at = time.perf_counter()
    counts: dict[str, int] = {}
    for stream in streams:
        counts[stream.status] = counts.get(stream.status, 0) + 1

    channel_stats = build_channel_stats(streams, config)
    streamer_stats = build_streamer_stats(config, streams, jobs)
    powerchat_stats = build_powerchat_stats(streamer_stats)
    perf_step(steps, "aggregate", step_started_at)

    step_started_at = time.perf_counter()
    speaker_labels = build_speaker_label_statuses(
        config,
        streams,
        channel_stats,
        include_detected=include_speaker_scan,
    )
    perf_step(steps, "speaker_labels", step_started_at)

    step_started_at = time.perf_counter()
    snapshot = StatusSnapshot(
        generated_at=time.time(),
        stream_revision=stream_revision_for_records(records),
        job_revision=job_revision_for_jobs(jobs),
        app=build_app_info(),
        download_dir=str(config.download_dir),
        state_db=str(config.db_path),
        counts=counts,
        total_bytes=sum(stream.total_bytes for stream in streams),
        part_bytes=sum(stream.part_bytes for stream in streams),
        final_bytes=sum(stream.final_bytes for stream in streams),
        chat_bytes=sum(stream.chat_bytes for stream in streams),
        fragment_bytes=sum(stream.fragment_bytes for stream in streams),
        state_bytes=sum(stream.state_bytes for stream in streams),
        temporary_bytes=sum(stream.temporary_bytes for stream in streams),
        stream_limit=STREAM_LIMIT,
        configured_channels=list(config.channels),
        streamer_stats=streamer_stats,
        powerchat_stats=powerchat_stats,
        streamer_groups=build_streamer_statuses(config),
        configuration=build_config_summary(config),
        channel_stats=channel_stats,
        speaker_labels=speaker_labels,
        recent_logs=get_recent_log_entries(LOG_LIMIT),
        log_limit=LOG_LIMIT,
        jobs=jobs,
        job_limit=JOB_LIMIT,
        streams=streams,
    )
    perf_step(steps, "snapshot", step_started_at)

    elapsed = perf_elapsed(started_at)
    log_perf(
        "status-snapshot",
        elapsed,
        WEB_SLOW_STEP_SECONDS,
        streams=len(streams),
        files=sum(stream.file_count for stream in streams),
        jobs=len(jobs),
        include_speaker_scan=include_speaker_scan,
        steps=format_perf_steps(steps),
    )
    return snapshot



def stream_revision_for_records(records: list[StreamRecord]) -> str:
    if not records:
        return ""
    latest = max((record.updated_at or "" for record in records), default="")
    statuses = ";".join(
        f"{record.video_id}:{record.status}:{record.segment_index}:{record.updated_at}"
        for record in records
    )
    digest = hashlib.sha1(statuses.encode("utf-8")).hexdigest()[:16]
    return f"{latest}|{len(records)}|{digest}"


def job_revision_for_jobs(jobs: list[JobStatus]) -> str:
    if not jobs:
        return ""
    active_bits = [
        f"{job.job_id}:{job.status}:{job.kind}:{job.video_id}:"
        f"{job.phase}:{job.progress}:{job.updated_at or job.started_at or 0.0:.3f}"
        for job in jobs
        if job.status in {"queued", "running"}
    ]
    if not active_bits:
        latest = max((job.updated_at or job.finished_at or job.started_at or 0.0 for job in jobs), default=0.0)
        done_bits = [
            f"{job.job_id}:{job.status}:{job.kind}:{job.video_id}"
            for job in jobs[:25]
        ]
        digest = hashlib.sha1(";".join(sorted(done_bits)).encode("utf-8")).hexdigest()[:16]
        return f"done:{latest:.3f}:{len(jobs)}:{digest}"
    digest = hashlib.sha1(";".join(sorted(active_bits)).encode("utf-8")).hexdigest()[:16]
    return f"active:{len(active_bits)}:{digest}"


def build_lite_status_payload(config: BotConfig) -> dict[str, Any]:
    started_at = time.perf_counter()
    steps: list[tuple[str, float]] = []

    step_started_at = time.perf_counter()
    state = StateStore(config.db_path)
    try:
        records = state.list_streams(STREAM_LIMIT)
        watermark_records = state.list_watermark_copies(limit=1000)
    finally:
        state.close()
    perf_step(steps, "db", step_started_at)

    step_started_at = time.perf_counter()
    counts: dict[str, int] = {}
    attention_statuses = {"checking_after_exit", "interrupted", "waiting_retry"}
    attention_count = 0
    for record in records:
        counts[record.status] = counts.get(record.status, 0) + 1
        if record.status in attention_statuses:
            attention_count += 1

    jobs = build_job_statuses(watermark_records)
    perf_step(steps, "aggregate", step_started_at)

    payload = {
        "detail": "lite",
        "generated_at": time.time(),
        "stream_revision": stream_revision_for_records(records),
        "job_revision": job_revision_for_jobs(jobs),
        "stream_count": len(records),
        "streamer_count": len(config.streamers),
        "attention_count": attention_count,
        "counts": counts,
        "recent_logs": [asdict(entry) for entry in get_recent_log_entries(LOG_LIMIT)],
        "log_limit": LOG_LIMIT,
        "jobs": [asdict(job) for job in jobs],
        "job_limit": JOB_LIMIT,
        "app": asdict(build_app_info()),
    }
    log_perf(
        "status-lite",
        perf_elapsed(started_at),
        WEB_SLOW_STEP_SECONDS,
        streams=len(records),
        jobs=len(jobs),
        steps=format_perf_steps(steps),
    )
    return payload


def build_job_statuses(
    watermark_records: list[WatermarkCopyRecord],
) -> list[JobStatus]:
    jobs: list[JobStatus] = []
    with CHAT_RENDER_JOBS_LOCK:
        render_jobs = list(CHAT_RENDER_JOBS.values())
    with CHAT_REFRESH_JOBS_LOCK:
        refresh_jobs = list(CHAT_REFRESH_JOBS.values())
    with TRANSCRIPTION_JOBS_LOCK:
        transcription_jobs = list(TRANSCRIPTION_JOBS.values())
    with EVENT_DETECTION_JOBS_LOCK:
        event_detection_jobs = list(EVENT_DETECTION_JOBS.values())
    tracked_jobs = list_tracked_jobs(JOB_LIMIT)

    for job in tracked_jobs:
        jobs.append(
            JobStatus(
                job_id=job.job_id,
                kind=job.kind,
                status=job.status,
                phase=job.phase or job.message,
                progress=job.progress,
                video_id=job.video_id,
                item=job.item,
                detail=job.detail,
                message=job.message,
                started_at=job.started_at,
                updated_at=job.updated_at,
                finished_at=job.finished_at,
            )
        )

    for job in render_jobs:
        jobs.append(
            JobStatus(
                job_id=f"chat-render:{job.video_id}:{job.chat_name}",
                kind="Chat render",
                status=job.status,
                phase=job.phase or job.message,
                progress=job.progress,
                video_id=job.video_id,
                item=job.output_name,
                detail=f"{job.media_name} + {job.chat_name}",
                message=job.message,
                started_at=job.started_at,
                updated_at=job.updated_at or job.finished_at or job.started_at,
                finished_at=job.finished_at,
                details=render_chat_status_details(job),
            )
        )
    for job in refresh_jobs:
        jobs.append(
            JobStatus(
                job_id=f"chat-refresh:{job.video_id}:{job.chat_name}",
                kind="Chat refresh",
                status=job.status,
                phase=job.phase or job.message,
                progress=job.progress,
                video_id=job.video_id,
                item=job.chat_name,
                detail=job.media_name,
                message=job.message,
                started_at=job.started_at,
                updated_at=job.updated_at or job.finished_at or job.started_at,
                finished_at=job.finished_at,
            )
        )
    for job in transcription_jobs:
        jobs.append(
            JobStatus(
                job_id=f"transcription:{job.video_id}:{job.media_name}",
                kind="Transcription",
                status=job.status,
                phase=job.phase or job.message,
                progress=job.progress,
                video_id=job.video_id,
                item=job.media_name,
                detail="WhisperX subtitles",
                message=job.message,
                started_at=job.started_at,
                updated_at=job.updated_at or job.finished_at or job.started_at,
                finished_at=job.finished_at,
            )
        )
    for job in event_detection_jobs:
        jobs.append(
            JobStatus(
                job_id=f"event-detection:{job.video_id}:{job.media_name}",
                kind="Event detection",
                status=job.status,
                phase=job.phase or job.message,
                progress=job.progress,
                video_id=job.video_id,
                item=job.media_name,
                detail="Content events",
                message=job.message,
                started_at=job.started_at,
                updated_at=job.updated_at or job.finished_at or job.started_at,
                finished_at=job.finished_at,
            )
        )
    for record in watermark_records:
        started_at = iso_to_epoch(record.started_at) or iso_to_epoch(record.created_at)
        updated_at = iso_to_epoch(record.updated_at)
        finished_at = iso_to_epoch(record.finished_at)
        jobs.append(
            JobStatus(
                job_id=f"watermark:{record.copy_id}",
                kind="Watermark",
                status=record.status,
                phase=record.phase or record.message,
                progress=record.progress,
                video_id=record.video_id,
                item=record.output_name,
                detail=record.recipient_label,
                message=record.error or record.message,
                started_at=started_at,
                updated_at=updated_at,
                finished_at=finished_at,
            )
        )

    return sorted(jobs, key=job_started_sort_key, reverse=True)[:JOB_LIMIT]


def job_started_sort_key(job: JobStatus) -> tuple[float, str]:
    return (job.started_at or job.updated_at or 0.0, job.job_id)


def render_chat_status_details(job: RenderChatJob) -> dict[str, Any]:
    details = dict(job.details)
    details.setdefault("media_name", job.media_name)
    details.setdefault("chat_name", job.chat_name)
    details.setdefault("output_name", job.output_name)
    if job.started_at and "elapsed_seconds" not in details:
        ended_at = job.finished_at if job.finished_at is not None else time.time()
        elapsed_seconds = max(0.0, ended_at - job.started_at)
        details["elapsed_seconds"] = elapsed_seconds
        details["elapsed"] = format_duration(int(elapsed_seconds))
    if job.message and "diagnostic" not in details:
        details["diagnostic"] = job.message
    return details


def iso_to_epoch(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).timestamp()
    except ValueError:
        return None


def build_app_info() -> AppInfo:
    return AppInfo(
        name="ONLYSAVEmeVODS",
        version=APP_VERSION,
        python_version=platform.python_version(),
        executable=sys.executable,
        platform=platform.platform(),
    )


def build_config_summary(config: BotConfig) -> dict[str, dict[str, Any]]:
    return {
        "Paths": {
            "config_path": str(config.config_path) if config.config_path else "-",
            "download_dir": str(config.download_dir),
            "state_dir": str(config.state_dir),
            "state_db": str(config.db_path),
        },
        "Channels": {
            "count": len(config.channels),
            "channels": list(config.channels),
            "monitored_source_count": len(monitored_sources(config)),
            "monitored_sources": monitored_sources(config),
        },
        "Streamers": streamer_summary_for_config(config),
        "Discovery": {
            "poll_interval_seconds": config.poll_interval_seconds,
            "channel_scan_limit": config.channel_scan_limit,
            "discovery_probe_concurrency": config.discovery_probe_concurrency,
            "max_concurrent_downloads": config.max_concurrent_downloads,
        },
        "Download": {
            "live_from_start": config.live_from_start,
            "keep_fragments_for_resume": config.keep_fragments_for_resume,
            "reconnect_interval_seconds": config.reconnect_interval_seconds,
            "post_exit_check_seconds": list(config.post_exit_check_seconds),
            "retry_backoff_seconds": list(config.retry_backoff_seconds),
            "extra_yt_dlp_args": redacted_extra_args(config.extra_yt_dlp_args),
            "extra_yt_dlp_args_configured": bool(config.extra_yt_dlp_args),
        },
        "Live Chat": {
            "record_live_chat": config.record_live_chat,
            "render_live_chat_video": config.render_live_chat_video,
            "chat_render_panel_workers": config.chat_render_panel_workers,
            "chat_render_timeout_seconds": config.chat_render_timeout_seconds,
            "chat_render_use_nvenc": config.chat_render_use_nvenc,
            "chat_render_nvenc_devices": nvenc_devices_for_config_summary(
                config.chat_render_nvenc_devices
            ),
            "chat_render_nvenc_device_values": list(config.chat_render_nvenc_devices),
        },
        "Transcription": {
            "transcribe_subtitles": config.transcribe_subtitles,
            "transcription_max_concurrent": config.transcription_max_concurrent,
            "voice_detection": voice_detection_mode(config),
            "voice_detection_speakers": voice_detection_speaker_summary(config),
            "channel_overrides": voice_detection_overrides_for_summary(config),
            "whisperx_path": config.whisperx_path,
            "whisperx_model": config.whisperx_model,
            "whisperx_device": config.whisperx_device,
            "whisperx_compute_type": config.whisperx_compute_type,
            "whisperx_batch_size": config.whisperx_batch_size,
            "whisperx_language": config.whisperx_language or "auto",
            "whisperx_language_value": config.whisperx_language,
            "whisperx_diarize": config.whisperx_diarize,
            "whisperx_hf_token_env": config.whisperx_hf_token_env or "-",
            "whisperx_hf_token_configured": bool(
                config.whisperx_hf_token_env
                and os_environ_has(config.whisperx_hf_token_env)
            ),
            "whisperx_min_speakers": config.whisperx_min_speakers or "-",
            "whisperx_max_speakers": config.whisperx_max_speakers or "-",
            "voice_match_enabled": config.voice_match_enabled,
            "voice_match_model": config.voice_match_model,
            "voice_match_threshold": config.voice_match_threshold,
            "voice_match_min_margin": config.voice_match_min_margin,
            "voice_sample_max_bytes": config.voice_sample_max_bytes,
            "voice_match_backend": voice_matcher_status(config),
        },
        "Content Events": {
            "stream_event_detection_enabled": config.stream_event_detection_enabled,
            "stream_event_model": config.stream_event_model,
            "stream_event_device": config.stream_event_device,
            "stream_event_window_seconds": config.stream_event_window_seconds,
            "stream_event_hop_seconds": config.stream_event_hop_seconds,
            "stream_event_min_confidence": config.stream_event_min_confidence,
            "stream_event_max_events_per_media": config.stream_event_max_events_per_media,
            "backend": content_event_detector_status(config),
            "rules": [stream_event_rule_summary(rule) for rule in config.stream_event_rules],
        },
        "Twitch Ads": {
            "twitch_ad_repair_enabled": config.twitch_ad_repair_enabled,
            "twitch_ad_repair_tesseract_path": config.twitch_ad_repair_tesseract_path,
            "twitch_ad_repair_scan_seconds": config.twitch_ad_repair_scan_seconds,
            "twitch_ad_repair_sample_seconds": config.twitch_ad_repair_sample_seconds,
            "twitch_ad_repair_max_seconds": config.twitch_ad_repair_max_seconds,
            "twitch_ad_repair_vod_search_limit": config.twitch_ad_repair_vod_search_limit,
        },
        "Watermark": {
            "watermark_enabled": config.watermark_enabled,
            "watermark_secret_env": config.watermark_secret_env,
            "watermark_strength": config.watermark_strength,
            "watermark_secret_configured": bool(watermark_secret(config)),
            "watermark_detect_upload_max_bytes": config.watermark_detect_upload_max_bytes,
        },
        "Web": {
            "web_enabled": config.web_enabled,
            "web_host": config.web_host,
            "web_port": config.web_port,
        },
        "Tools": {
            "log_level": config.log_level,
            "yt_dlp_path": config.yt_dlp_path,
            "ffmpeg_path": config.ffmpeg_path,
        },
    }


def voice_profile_statuses(voices: dict[str, VoiceProfileConfig]) -> list[VoiceProfileStatus]:
    return [
        VoiceProfileStatus(
            name=name,
            enabled=profile.enabled,
            sample_count=len(profile.samples),
            threshold=profile.threshold,
            notes=profile.notes,
            samples=list(profile.samples),
        )
        for name, profile in sorted(voices.items())
    ]


def stream_event_rule_summary(rule: StreamEventRuleConfig) -> dict[str, Any]:
    return {
        "name": rule.name,
        "enabled": rule.enabled,
        "labels": list(rule.labels),
        "keywords": list(rule.keywords),
        "voice": rule.voice,
        "min_loudness_dbfs": rule.min_loudness_dbfs,
        "min_duration_seconds": rule.min_duration_seconds,
        "max_duration_seconds": rule.max_duration_seconds,
        "severity": rule.severity,
    }


def stream_event_detection_summary(
    detection: StreamEventDetectionConfig | None,
) -> dict[str, Any]:
    if detection is None:
        return {}
    return {
        "enabled": detection.enabled,
        "model": detection.model,
        "device": detection.device,
        "window_seconds": detection.window_seconds,
        "hop_seconds": detection.hop_seconds,
        "min_confidence": detection.min_confidence,
        "max_events_per_media": detection.max_events_per_media,
    }


def build_streamer_statuses(config: BotConfig) -> list[StreamerStatus]:
    return [
        StreamerStatus(
            name=name,
            sources=list(streamer.sources),
            download_dir_name=streamer.download_dir_name,
            powerchat_enabled=streamer.powerchat_enabled,
            powerchat_username=streamer.powerchat_username,
            voice_detection=(
                voice_detection_config_summary(streamer.voice_detection)
                if streamer.voice_detection is not None
                else "default"
            ),
            speaker_label_count=len(streamer.speaker_labels),
            voices=voice_profile_statuses(streamer.voices),
            stream_event_detection=stream_event_detection_summary(
                streamer.stream_event_detection
            ),
            stream_event_rules=[
                stream_event_rule_summary(rule)
                for rule in streamer.stream_event_rules
            ],
        )
        for name, streamer in sorted(config.streamers.items())
    ]


def build_streamer_stats(
    config: BotConfig,
    streams: list[StreamStatus],
    jobs: list[JobStatus],
) -> list[StreamerStatStatus]:
    streams_by_key: dict[str, list[StreamStatus]] = {}
    display_names: dict[str, str] = {}
    for stream in streams:
        raw_name = stream.channel or "unknown channel"
        name = streamer_display_name_for_channel(config, raw_name) or raw_name
        key = channel_group_key(name)
        streams_by_key.setdefault(key, []).append(stream)
        if name != "unknown channel" or key not in display_names:
            display_names[key] = name

    stats: list[StreamerStatStatus] = []
    claimed_keys: set[str] = set()
    for name, streamer in sorted(config.streamers.items()):
        key = channel_group_key(name)
        claimed_keys.add(key)
        streamer_streams = streams_by_key.get(key, [])
        status = channel_status_from_streams(name, streamer_streams, list(streamer.sources))
        voice_detection = (
            voice_detection_config_summary(streamer.voice_detection)
            if streamer.voice_detection is not None
            else "default"
        )
        stats.append(
            streamer_stat_from_channel_status(
                status,
                configured=True,
                needs_grouping=False,
                download_dir_name=streamer.download_dir_name or name,
                powerchat_enabled=streamer.powerchat_enabled,
                powerchat_username=streamer.powerchat_username,
                voice_detection=voice_detection,
                speaker_label_count=len(streamer.speaker_labels),
                voices=voice_profile_statuses(streamer.voices),
                stream_event_detection=stream_event_detection_summary(
                    streamer.stream_event_detection
                ),
                stream_event_rules=[
                    stream_event_rule_summary(rule)
                    for rule in streamer.stream_event_rules
                ],
                jobs=jobs_for_streams(jobs, streamer_streams),
                streams=streamer_streams,
            )
        )

    ungrouped_sources: dict[str, list[str]] = {}
    ungrouped_names: dict[str, str] = {}
    for channel in config.channels:
        if streamer_display_name_for_channel(config, channel):
            continue
        name = channel_display_name(channel)
        key = channel_group_key(name)
        ungrouped_sources.setdefault(key, []).append(channel)
        ungrouped_names.setdefault(key, display_names.get(key, name))

    for key, sources in sorted(ungrouped_sources.items(), key=lambda item: item[1][0].lower()):
        claimed_keys.add(key)
        name = display_names.get(key, ungrouped_names.get(key, key))
        group_streams = streams_by_key.get(key, [])
        status = channel_status_from_streams(name, group_streams, sources)
        stats.append(
            streamer_stat_from_channel_status(
                status,
                configured=False,
                needs_grouping=True,
                download_dir_name=name,
                powerchat_enabled=False,
                powerchat_username="",
                voice_detection=voice_detection_summary_for_source_group(config, name, sources),
                speaker_label_count=speaker_label_count_for_source_group(config, name, sources),
                voices=[],
                stream_event_detection={},
                stream_event_rules=[],
                jobs=jobs_for_streams(jobs, group_streams),
                streams=group_streams,
            )
        )

    for key, group_streams in streams_by_key.items():
        if key in claimed_keys:
            continue
        name = display_names.get(key, key)
        status = channel_status_from_streams(name, group_streams, [])
        stats.append(
            streamer_stat_from_channel_status(
                status,
                configured=False,
                needs_grouping=True,
                download_dir_name=name,
                powerchat_enabled=False,
                powerchat_username="",
                voice_detection=voice_detection_summary_for_source_group(config, name, []),
                speaker_label_count=speaker_label_count_for_source_group(config, name, []),
                voices=[],
                stream_event_detection={},
                stream_event_rules=[],
                jobs=jobs_for_streams(jobs, group_streams),
                streams=group_streams,
            )
        )

    return sorted(stats, key=streamer_stat_sort_key)


def streamer_stat_from_channel_status(
    status: ChannelStatus,
    *,
    configured: bool,
    needs_grouping: bool,
    download_dir_name: str,
    powerchat_enabled: bool,
    powerchat_username: str,
    voice_detection: str,
    speaker_label_count: int,
    voices: list[VoiceProfileStatus],
    stream_event_detection: dict[str, Any],
    stream_event_rules: list[dict[str, Any]],
    jobs: list[JobStatus],
    streams: list[StreamStatus],
) -> StreamerStatStatus:
    latest_updated = iso_to_epoch(status.latest_updated_at)
    latest_file = status.latest_file_modified_at
    latest_activity = max(
        (value for value in (latest_updated, latest_file) if value is not None),
        default=None,
    )
    return StreamerStatStatus(
        name=status.name,
        sources=list(status.configured_sources),
        download_dir_name=download_dir_name,
        powerchat_enabled=powerchat_enabled,
        powerchat_username=powerchat_username,
        configured=configured,
        needs_grouping=needs_grouping,
        voice_detection=voice_detection,
        speaker_label_count=speaker_label_count,
        voices=list(voices),
        stream_event_detection=dict(stream_event_detection),
        stream_event_rules=list(stream_event_rules),
        stream_count=status.stream_count,
        active_count=status.active_count,
        checking_count=status.checking_count,
        ended_count=status.ended_count,
        attention_count=status.attention_count,
        file_count=status.file_count,
        downloadable_count=status.downloadable_count,
        total_bytes=status.total_bytes,
        part_bytes=status.part_bytes,
        final_bytes=status.final_bytes,
        chat_bytes=status.chat_bytes,
        fragment_bytes=status.fragment_bytes,
        latest_updated_at=status.latest_updated_at,
        latest_file_modified_at=status.latest_file_modified_at,
        latest_activity_at=latest_activity,
        jobs=jobs,
        streams=list(streams),
    )


def jobs_for_streams(
    jobs: list[JobStatus],
    streams: list[StreamStatus],
) -> list[JobStatus]:
    video_ids = {stream.video_id for stream in streams}
    if not video_ids:
        return []
    return [job for job in jobs if job.video_id in video_ids]


def voice_detection_summary_for_source_group(
    config: BotConfig,
    name: str,
    sources: list[str],
) -> str:
    for target in [name, *sources]:
        override = config.channel_voice_detection.get(target)
        if override is not None:
            return voice_detection_config_summary(override)
    return "default"


def speaker_label_count_for_source_group(
    config: BotConfig,
    name: str,
    sources: list[str],
) -> int:
    labels = speaker_labels_for_channel(config, name)
    for source in sources:
        labels.update(speaker_labels_for_channel(config, source))
    return len(labels)


def streamer_stat_sort_key(status: StreamerStatStatus) -> tuple[bool, bool, bool, float, str]:
    return (
        not status.configured,
        status.active_count == 0,
        status.attention_count == 0,
        -(status.latest_activity_at or 0.0),
        status.name.lower(),
    )


def build_powerchat_stats(streamer_stats: list[StreamerStatStatus]) -> dict[str, Any]:
    totals = new_powerchat_accumulator()
    donors: dict[str, dict[str, Any]] = {}
    streamers: dict[str, dict[str, Any]] = {}
    hours: dict[int, dict[str, Any]] = {}
    stream_rows: list[dict[str, Any]] = []
    event_rows: list[dict[str, Any]] = []
    streams_with_powerchat = 0
    duration_seconds = 0.0
    events_without_offset = 0
    seen_streams: set[str] = set()

    for streamer in streamer_stats:
        streamer_acc = streamers.setdefault(
            streamer.name,
            {
                "streamer": streamer.name,
                "event_count": 0,
                "stream_count": 0,
                "duration_seconds": 0.0,
                "accumulator": new_powerchat_accumulator(),
                "donors": {},
                "hours": {},
                "stream_rows": [],
                "events_without_offset": 0,
            },
        )
        for stream in streamer.streams:
            if stream.video_id in seen_streams:
                continue
            seen_streams.add(stream.video_id)
            events = list(stream.powerchat_events)
            if not events:
                continue

            streams_with_powerchat += 1
            stream_duration = powerchat_stream_duration_seconds(stream, events)
            duration_seconds += stream_duration
            streamer_acc["stream_count"] += 1
            streamer_acc["duration_seconds"] += stream_duration

            stream_acc = new_powerchat_accumulator()
            for event in events:
                row = powerchat_dashboard_event_row(streamer.name, stream, event)
                event_rows.append(row)
                add_powerchat_event_to_accumulator(totals, event)
                add_powerchat_event_to_accumulator(streamer_acc["accumulator"], event)
                add_powerchat_event_to_accumulator(stream_acc, event)

                donor_name = event.donor or "Unknown donor"
                donor_acc = donors.setdefault(
                    donor_name,
                    {
                        "donor": donor_name,
                        "event_count": 0,
                        "accumulator": new_powerchat_accumulator(),
                        "latest_received_at": "",
                    },
                )
                donor_acc["event_count"] += 1
                if event.received_at and event.received_at > donor_acc["latest_received_at"]:
                    donor_acc["latest_received_at"] = event.received_at
                add_powerchat_event_to_accumulator(donor_acc["accumulator"], event)

                streamer_donors = streamer_acc["donors"]
                streamer_donor_acc = streamer_donors.setdefault(
                    donor_name,
                    {
                        "donor": donor_name,
                        "event_count": 0,
                        "accumulator": new_powerchat_accumulator(),
                        "latest_received_at": "",
                    },
                )
                streamer_donor_acc["event_count"] += 1
                if event.received_at and event.received_at > streamer_donor_acc["latest_received_at"]:
                    streamer_donor_acc["latest_received_at"] = event.received_at
                add_powerchat_event_to_accumulator(streamer_donor_acc["accumulator"], event)

                if row["hour_index"] is None:
                    events_without_offset += 1
                    streamer_acc["events_without_offset"] += 1
                    continue
                hour = hours.setdefault(
                    row["hour_index"],
                    {
                        "hour_index": row["hour_index"],
                        "hour_label": row["hour_label"],
                        "event_count": 0,
                        "accumulator": new_powerchat_accumulator(),
                    },
                )
                hour["event_count"] += 1
                add_powerchat_event_to_accumulator(hour["accumulator"], event)

                streamer_hours = streamer_acc["hours"]
                streamer_hour = streamer_hours.setdefault(
                    row["hour_index"],
                    {
                        "hour_index": row["hour_index"],
                        "hour_label": row["hour_label"],
                        "event_count": 0,
                        "accumulator": new_powerchat_accumulator(),
                    },
                )
                streamer_hour["event_count"] += 1
                add_powerchat_event_to_accumulator(streamer_hour["accumulator"], event)

            streamer_acc["event_count"] += len(events)
            stream_row = finalize_powerchat_stream_row(
                streamer.name,
                stream,
                stream_acc,
                len(events),
                stream_duration,
            )
            stream_rows.append(stream_row)
            streamer_acc["stream_rows"].append(stream_row)

    donor_rows = [finalize_powerchat_donor_row(item) for item in donors.values()]
    streamer_rows = [finalize_powerchat_streamer_row(item) for item in streamers.values() if item["event_count"]]
    streamer_dashboards = [
        finalize_powerchat_streamer_dashboard(item)
        for item in streamers.values()
        if item["event_count"]
    ]
    hour_rows = [finalize_powerchat_hour_row(item) for item in hours.values()]
    event_rows.sort(key=powerchat_event_row_sort_key, reverse=True)

    return {
        "event_count": totals["event_count"],
        "streams_with_powerchat": streams_with_powerchat,
        "duration_seconds": round(duration_seconds, 3),
        "duration_hours": round(duration_seconds / 3600, 3) if duration_seconds > 0 else 0.0,
        "events_without_offset": events_without_offset,
        "money_totals": powerchat_accumulator_money_totals(totals),
        "unit_totals": powerchat_accumulator_unit_totals(totals),
        "money_rates": powerchat_money_rates(totals, duration_seconds),
        "top_donors": sorted(donor_rows, key=powerchat_summary_sort_key, reverse=True)[:25],
        "streamer_totals": sorted(streamer_rows, key=powerchat_summary_sort_key, reverse=True),
        "streamer_dashboards": sorted(streamer_dashboards, key=powerchat_summary_sort_key, reverse=True),
        "stream_totals": sorted(stream_rows, key=powerchat_summary_sort_key, reverse=True),
        "hourly_totals": sorted(hour_rows, key=lambda row: int(row.get("hour_index") or 0)),
        "events": event_rows,
    }


def build_stream_powerchat_stats(stream: StreamStatus, streamer_name: str = "") -> dict[str, Any]:
    events = list(stream.powerchat_events)
    totals = new_powerchat_accumulator()
    donors: dict[str, dict[str, Any]] = {}
    hours: dict[int, dict[str, Any]] = {}
    event_rows: list[dict[str, Any]] = []
    events_without_offset = 0
    duration_seconds = powerchat_stream_duration_seconds(stream, events)

    for event in events:
        row = powerchat_dashboard_event_row(streamer_name or stream.channel, stream, event)
        event_rows.append(row)
        add_powerchat_event_to_accumulator(totals, event)

        donor_name = event.donor or "Unknown donor"
        donor_acc = donors.setdefault(
            donor_name,
            {
                "donor": donor_name,
                "event_count": 0,
                "accumulator": new_powerchat_accumulator(),
                "latest_received_at": "",
            },
        )
        donor_acc["event_count"] += 1
        if event.received_at and event.received_at > donor_acc["latest_received_at"]:
            donor_acc["latest_received_at"] = event.received_at
        add_powerchat_event_to_accumulator(donor_acc["accumulator"], event)

        if row["hour_index"] is None:
            events_without_offset += 1
            continue
        hour = hours.setdefault(
            row["hour_index"],
            {
                "hour_index": row["hour_index"],
                "hour_label": row["hour_label"],
                "event_count": 0,
                "accumulator": new_powerchat_accumulator(),
            },
        )
        hour["event_count"] += 1
        add_powerchat_event_to_accumulator(hour["accumulator"], event)

    donor_rows = [finalize_powerchat_donor_row(item) for item in donors.values()]
    hour_rows = [finalize_powerchat_hour_row(item) for item in hours.values()]
    event_rows.sort(key=powerchat_event_row_sort_key, reverse=True)
    return {
        "event_count": totals["event_count"],
        "duration_seconds": round(duration_seconds, 3),
        "duration_hours": round(duration_seconds / 3600, 3) if duration_seconds > 0 else 0.0,
        "events_without_offset": events_without_offset,
        "money_totals": powerchat_accumulator_money_totals(totals),
        "unit_totals": powerchat_accumulator_unit_totals(totals),
        "money_rates": powerchat_money_rates(totals, duration_seconds),
        "top_donors": sorted(donor_rows, key=powerchat_summary_sort_key, reverse=True)[:10],
        "hourly_totals": sorted(hour_rows, key=lambda row: int(row.get("hour_index") or 0)),
        "events": event_rows,
    }


def build_powerchat_export_payload(
    config: BotConfig,
    params: dict[str, list[str]],
) -> dict[str, Any]:
    filters = powerchat_export_filters(params)
    snapshot = build_status_snapshot(config, include_speaker_scan=False)
    stats = snapshot.powerchat_stats or {}
    events = filter_powerchat_export_events(stats.get("events", []), filters)
    totals = powerchat_export_totals(events)
    return {
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "filters": filters,
        "event_count": len(events),
        "money_totals": totals["money"],
        "unit_totals": totals["units"],
        "events": events,
    }


def powerchat_export_filters(params: dict[str, list[str]]) -> dict[str, str]:
    filters: dict[str, str] = {}
    for key in ("streamer", "video_id", "platform", "kind", "from", "to", "search"):
        value = first_query_value(params, key).strip()
        if value and value != "all":
            filters[key] = value
    return filters


def filter_powerchat_export_events(
    events: list[dict[str, Any]],
    filters: dict[str, str],
) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    query = filters.get("search", "").strip().casefold()
    for event in events:
        if filters.get("streamer") and event.get("streamer") != filters["streamer"]:
            continue
        if filters.get("video_id") and event.get("video_id") != filters["video_id"]:
            continue
        if filters.get("platform") and event.get("platform") != filters["platform"]:
            continue
        if filters.get("kind") and event.get("kind") != filters["kind"]:
            continue
        event_date = str(event.get("received_at") or "")[:10]
        if filters.get("from") and event_date and event_date < filters["from"]:
            continue
        if filters.get("to") and event_date and event_date > filters["to"]:
            continue
        if query:
            haystack = " ".join(
                str(event.get(key) or "")
                for key in (
                    "donor",
                    "message",
                    "stream_title",
                    "streamer",
                    "video_id",
                    "platform",
                    "source",
                )
            ).casefold()
            if query not in haystack:
                continue
        filtered.append(dict(event))
    return filtered


def powerchat_export_totals(events: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    money: dict[str, float] = {}
    units: dict[tuple[str, str], float] = {}
    for event in events:
        if event.get("kind") == "money" and event.get("money_amount") is not None and event.get("money_currency"):
            currency = str(event.get("money_currency") or "").upper()
            money[currency] = money.get(currency, 0.0) + float(event.get("money_amount") or 0.0)
        elif event.get("kind") == "unit" and event.get("unit_amount") is not None and event.get("unit"):
            key = (str(event.get("platform") or ""), str(event.get("unit") or ""))
            units[key] = units.get(key, 0.0) + float(event.get("unit_amount") or 0.0)
    return {
        "money": [
            {"currency": currency, "amount": round(amount, 2)}
            for currency, amount in sorted(money.items())
        ],
        "units": [
            {"platform": platform, "unit": unit, "amount": round(amount, 2)}
            for (platform, unit), amount in sorted(units.items())
        ],
    }


POWERCHAT_EXPORT_COLUMNS = [
    "received_at",
    "offset_seconds",
    "hour_label",
    "streamer",
    "stream_title",
    "video_id",
    "donor",
    "kind",
    "money_amount",
    "money_currency",
    "unit_amount",
    "unit",
    "platform",
    "source",
    "message",
]


def powerchat_export_csv(events: list[dict[str, Any]]) -> str:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=POWERCHAT_EXPORT_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    for event in events:
        writer.writerow(event)
    return buffer.getvalue()


def powerchat_export_filename(filters: dict[str, str], export_format: str) -> str:
    parts = ["powerchat-events"]
    if filters.get("streamer"):
        parts.append(filters["streamer"])
    if filters.get("video_id"):
        parts.append(filters["video_id"])
    if filters.get("from") or filters.get("to"):
        parts.append(f'{filters.get("from", "start")}-to-{filters.get("to", "end")}')
    stem = safe_filename_stem(" - ".join(parts))
    suffix = "csv" if export_format == "csv" else "json"
    return f"{stem}.{suffix}"


def new_powerchat_accumulator() -> dict[str, Any]:
    return {
        "event_count": 0,
        "money": {},
        "money_event_counts": {},
        "units": {},
    }


def add_powerchat_event_to_accumulator(
    accumulator: dict[str, Any],
    event: PowerchatEventStatus,
) -> None:
    accumulator["event_count"] = int(accumulator.get("event_count") or 0) + 1
    if event.kind == "money" and event.money_amount is not None and event.money_currency:
        currency = event.money_currency.upper()
        money = accumulator.setdefault("money", {})
        money[currency] = float(money.get(currency, 0.0)) + float(event.money_amount)
        counts = accumulator.setdefault("money_event_counts", {})
        counts[currency] = int(counts.get(currency, 0)) + 1
    elif event.kind == "unit" and event.unit_amount is not None and event.unit:
        platform = event.platform or ""
        key = f"{platform}\0{event.unit}"
        units = accumulator.setdefault("units", {})
        units[key] = float(units.get(key, 0.0)) + float(event.unit_amount)


def powerchat_accumulator_money_totals(accumulator: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {"currency": currency, "amount": round(float(amount), 2)}
        for currency, amount in sorted((accumulator.get("money") or {}).items())
    ]


def powerchat_accumulator_unit_totals(accumulator: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, amount in sorted((accumulator.get("units") or {}).items()):
        platform, _separator, unit = str(key).partition("\0")
        rows.append({"platform": platform, "unit": unit, "amount": round(float(amount), 2)})
    return rows


def powerchat_money_averages(accumulator: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    counts = accumulator.get("money_event_counts") or {}
    for currency, amount in sorted((accumulator.get("money") or {}).items()):
        count = int(counts.get(currency) or 0)
        if count > 0:
            rows.append({"currency": currency, "amount": round(float(amount) / count, 2)})
    return rows


def powerchat_money_rates(
    accumulator: dict[str, Any],
    duration_seconds: float,
) -> list[dict[str, Any]]:
    if duration_seconds <= 0:
        return []
    duration_hours = duration_seconds / 3600
    return [
        {
            "currency": row["currency"],
            "amount": row["amount"],
            "duration_hours": round(duration_hours, 3),
            "amount_per_hour": round(float(row["amount"]) / duration_hours, 2),
        }
        for row in powerchat_accumulator_money_totals(accumulator)
    ]


def powerchat_stream_duration_seconds(
    stream: StreamStatus,
    events: list[PowerchatEventStatus],
) -> float:
    started_at = iso_to_epoch(stream.last_started_at) or iso_to_epoch(stream.first_seen_at)
    ended_at = iso_to_epoch(stream.last_exit_at)
    if started_at is not None and ended_at is not None and ended_at > started_at:
        return max(0.0, ended_at - started_at)
    if started_at is not None and stream.status in {"detected", "downloading", "checking_after_exit", "waiting_retry"}:
        updated_at = iso_to_epoch(stream.updated_at) or time.time()
        if updated_at > started_at:
            return max(0.0, updated_at - started_at)
    offsets = [event.offset_seconds for event in events if event.offset_seconds is not None]
    if len(offsets) >= 2:
        return max(0.0, max(offsets) - min(offsets))
    if len(offsets) == 1:
        return max(0.0, offsets[0] or 0.0)
    return 0.0


def powerchat_dashboard_event_row(
    streamer_name: str,
    stream: StreamStatus,
    event: PowerchatEventStatus,
) -> dict[str, Any]:
    hour_index = powerchat_event_hour_index(event)
    return {
        "streamer": streamer_name,
        "video_id": stream.video_id,
        "stream_title": stream.title,
        "stream_url": stream.url,
        "stream_platform": stream.platform,
        "stream_source": stream.source,
        "source": event.source,
        "received_at": event.received_at,
        "offset_seconds": event.offset_seconds,
        "hour_index": hour_index,
        "hour_label": powerchat_hour_label(hour_index) if hour_index is not None else "No stream offset",
        "kind": event.kind,
        "donor": event.donor,
        "platform": event.platform,
        "message": event.message,
        "money_amount": event.money_amount,
        "money_currency": event.money_currency,
        "unit_amount": event.unit_amount,
        "unit": event.unit,
    }


def powerchat_event_hour_index(event: PowerchatEventStatus) -> int | None:
    if event.offset_seconds is None:
        return None
    return max(0, int(event.offset_seconds // 3600))


def powerchat_hour_label(hour_index: int | None) -> str:
    if hour_index is None:
        return "No stream offset"
    return f"{hour_index}:00-{hour_index}:59"


def finalize_powerchat_stream_row(
    streamer_name: str,
    stream: StreamStatus,
    accumulator: dict[str, Any],
    event_count: int,
    duration_seconds: float,
) -> dict[str, Any]:
    return {
        "streamer": streamer_name,
        "video_id": stream.video_id,
        "title": stream.title,
        "platform": stream.platform,
        "source": stream.source,
        "url": stream.url,
        "status": stream.status,
        "event_count": event_count,
        "duration_seconds": round(duration_seconds, 3),
        "duration_hours": round(duration_seconds / 3600, 3) if duration_seconds > 0 else 0.0,
        "money_totals": powerchat_accumulator_money_totals(accumulator),
        "unit_totals": powerchat_accumulator_unit_totals(accumulator),
        "money_rates": powerchat_money_rates(accumulator, duration_seconds),
        "sort_amount": powerchat_sort_amount(accumulator),
    }


def finalize_powerchat_streamer_row(item: dict[str, Any]) -> dict[str, Any]:
    accumulator = item["accumulator"]
    duration_seconds = float(item.get("duration_seconds") or 0.0)
    return {
        "streamer": item["streamer"],
        "event_count": item["event_count"],
        "stream_count": item["stream_count"],
        "duration_seconds": round(duration_seconds, 3),
        "duration_hours": round(duration_seconds / 3600, 3) if duration_seconds > 0 else 0.0,
        "money_totals": powerchat_accumulator_money_totals(accumulator),
        "unit_totals": powerchat_accumulator_unit_totals(accumulator),
        "money_rates": powerchat_money_rates(accumulator, duration_seconds),
        "sort_amount": powerchat_sort_amount(accumulator),
    }


def finalize_powerchat_streamer_dashboard(item: dict[str, Any]) -> dict[str, Any]:
    row = finalize_powerchat_streamer_row(item)
    donor_rows = [finalize_powerchat_donor_row(donor) for donor in item.get("donors", {}).values()]
    hour_rows = [finalize_powerchat_hour_row(hour) for hour in item.get("hours", {}).values()]
    stream_rows = list(item.get("stream_rows") or [])
    row.update(
        {
            "events_without_offset": int(item.get("events_without_offset") or 0),
            "top_donors": sorted(donor_rows, key=powerchat_summary_sort_key, reverse=True)[:10],
            "hourly_totals": sorted(hour_rows, key=lambda hour: int(hour.get("hour_index") or 0)),
            "stream_totals": sorted(stream_rows, key=powerchat_summary_sort_key, reverse=True),
        }
    )
    return row


def finalize_powerchat_donor_row(item: dict[str, Any]) -> dict[str, Any]:
    accumulator = item["accumulator"]
    return {
        "donor": item["donor"],
        "event_count": item["event_count"],
        "latest_received_at": item["latest_received_at"],
        "money_totals": powerchat_accumulator_money_totals(accumulator),
        "unit_totals": powerchat_accumulator_unit_totals(accumulator),
        "sort_amount": powerchat_sort_amount(accumulator),
    }


def finalize_powerchat_hour_row(item: dict[str, Any]) -> dict[str, Any]:
    accumulator = item["accumulator"]
    return {
        "hour_index": item["hour_index"],
        "hour_label": item["hour_label"],
        "event_count": item["event_count"],
        "money_totals": powerchat_accumulator_money_totals(accumulator),
        "unit_totals": powerchat_accumulator_unit_totals(accumulator),
        "average_money": powerchat_money_averages(accumulator),
        "sort_amount": powerchat_sort_amount(accumulator),
    }


def powerchat_sort_amount(accumulator: dict[str, Any]) -> float:
    return round(sum(float(amount) for amount in (accumulator.get("money") or {}).values()), 2)


def powerchat_summary_sort_key(row: dict[str, Any]) -> tuple[float, int, str]:
    return (
        float(row.get("sort_amount") or 0.0),
        int(row.get("event_count") or 0),
        str(row.get("streamer") or row.get("donor") or row.get("title") or "").lower(),
    )


def powerchat_event_row_sort_key(row: dict[str, Any]) -> tuple[str, float, str]:
    return (
        str(row.get("received_at") or ""),
        float(row.get("offset_seconds") or 0.0),
        str(row.get("video_id") or ""),
    )


def nvenc_devices_for_config_summary(devices: list[str]) -> list[str]:
    if not devices:
        return []

    detected = detect_nvidia_devices()
    detected_by_index: dict[str, str] = {}
    for label in detected:
        index, separator, name = label.partition(":")
        if separator and index.strip():
            detected_by_index[index.strip()] = f"{index.strip()}: {name.strip()}"

    if not detected_by_index:
        return list(devices)

    return [
        detected_by_index.get(device, f"{device}: not detected")
        for device in devices
    ]


def os_environ_has(name: str) -> bool:
    return bool(name and os.environ.get(name))


def redacted_extra_args(args: list[str]) -> str:
    if not args:
        return "-"
    rendered = command_for_log(["yt-dlp", *args])
    return rendered.removeprefix("yt-dlp").strip() or "-"


def build_channel_stats(
    streams: list[StreamStatus],
    config: BotConfig,
) -> list[ChannelStatus]:
    groups: dict[str, list[StreamStatus]] = {}
    configured: dict[str, list[str]] = {}
    display_names: dict[str, str] = {}

    for channel in monitored_sources(config):
        name = streamer_display_name_for_channel(config, channel) or channel_display_name(channel)
        key = channel_group_key(name)
        configured.setdefault(key, []).append(channel)
        groups.setdefault(key, [])
        display_names.setdefault(key, name)

    for stream in streams:
        raw_name = stream.channel or "unknown channel"
        name = streamer_display_name_for_channel(config, raw_name) or raw_name
        key = channel_group_key(name)
        groups.setdefault(key, []).append(stream)
        if name != "unknown channel" or key not in display_names:
            display_names[key] = name

    stats = [
        channel_status_from_streams(
            display_names.get(key, key),
            channel_streams,
            configured.get(key, []),
        )
        for key, channel_streams in groups.items()
    ]
    return sorted(
        stats,
        key=lambda channel: (
            channel.active_count > 0,
            channel.attention_count > 0,
            channel.latest_updated_at or "",
            channel.name.lower(),
        ),
        reverse=True,
    )


def channel_status_from_streams(
    name: str,
    streams: list[StreamStatus],
    configured_sources: list[str],
) -> ChannelStatus:
    return ChannelStatus(
        name=name,
        configured_sources=configured_sources,
        stream_count=len(streams),
        active_count=sum(stream.status == "downloading" for stream in streams),
        checking_count=sum(stream.status == "checking_after_exit" for stream in streams),
        ended_count=sum(stream.status == "ended" for stream in streams),
        attention_count=sum(stream_needs_attention(stream) for stream in streams),
        file_count=sum(stream.file_count for stream in streams),
        downloadable_count=sum(
            1
            for stream in streams
            for file in stream.files
            if file.download_url
        ),
        total_bytes=sum(stream.total_bytes for stream in streams),
        part_bytes=sum(stream.part_bytes for stream in streams),
        final_bytes=sum(stream.final_bytes for stream in streams),
        chat_bytes=sum(stream.chat_bytes for stream in streams),
        fragment_bytes=sum(stream.fragment_bytes for stream in streams),
        latest_updated_at=max(
            (stream.updated_at for stream in streams if stream.updated_at),
            default=None,
        ),
        latest_file_modified_at=max(
            (
                stream.latest_file_modified_at
                for stream in streams
                if stream.latest_file_modified_at is not None
            ),
            default=None,
        ),
    )


def channel_display_name(channel: str) -> str:
    target = channel.strip().rstrip("/")
    if not target:
        return "unknown channel"
    if "/" in target:
        target = target.rsplit("/", 1)[-1]
    return target or channel


def channel_group_key(channel: str) -> str:
    target = channel_display_name(channel)
    if target.startswith("@"):
        target = target[1:]
    folded = target.casefold()
    compact = re.sub(r"[^a-z0-9]+", "", folded)
    return compact or folded or "unknown channel"


def build_speaker_label_statuses(
    config: BotConfig,
    streams: list[StreamStatus],
    channel_stats: list[ChannelStatus],
    *,
    include_detected: bool = True,
) -> list[SpeakerLabelStatus]:
    streams_by_key: dict[str, list[StreamStatus]] = {}
    for stream in streams:
        name = streamer_display_name_for_channel(config, stream.channel) or stream.channel
        streams_by_key.setdefault(channel_group_key(name), []).append(stream)

    statuses_by_key: dict[str, ChannelStatus] = {
        channel_group_key(status.name): status for status in channel_stats
    }
    for channel in config.channel_speaker_labels:
        key = channel_group_key(channel)
        statuses_by_key.setdefault(
            key,
            ChannelStatus(
                name=channel,
                configured_sources=[],
                stream_count=0,
                active_count=0,
                checking_count=0,
                ended_count=0,
                attention_count=0,
                file_count=0,
                downloadable_count=0,
                total_bytes=0,
                part_bytes=0,
                final_bytes=0,
                chat_bytes=0,
                fragment_bytes=0,
                latest_updated_at=None,
                latest_file_modified_at=None,
            ),
        )
    for streamer_name, streamer in config.streamers.items():
        if not streamer.speaker_labels:
            continue
        key = channel_group_key(streamer_name)
        statuses_by_key.setdefault(
            key,
            ChannelStatus(
                name=streamer_name,
                configured_sources=list(streamer.sources),
                stream_count=0,
                active_count=0,
                checking_count=0,
                ended_count=0,
                attention_count=0,
                file_count=0,
                downloadable_count=0,
                total_bytes=0,
                part_bytes=0,
                final_bytes=0,
                chat_bytes=0,
                fragment_bytes=0,
                latest_updated_at=None,
                latest_file_modified_at=None,
            ),
        )

    speaker_statuses: list[SpeakerLabelStatus] = []
    for key, status in statuses_by_key.items():
        detected: set[str] = set()
        transcript_count = 0
        if include_detected:
            for stream in streams_by_key.get(key, []):
                labels, count = detected_speaker_labels_for_directory(Path(stream.directory))
                detected.update(labels)
                transcript_count += count
        configured = speaker_labels_for_channel(config, status.name)
        if detected or configured or status.configured_sources:
            speaker_statuses.append(
                SpeakerLabelStatus(
                    channel=status.name,
                    configured_sources=status.configured_sources,
                    detected_labels=sorted(detected),
                    labels=configured,
                    transcript_count=transcript_count,
                )
            )
    streamer_keys = {channel_group_key(name) for name in config.streamers}
    return sorted(
        speaker_statuses,
        key=lambda item: (
            channel_group_key(item.channel) not in streamer_keys,
            item.channel.lower(),
        ),
    )


def detected_speaker_labels_for_directory(directory: Path) -> tuple[set[str], int]:
    labels: set[str] = set()
    transcript_count = 0
    for json_file in transcript_json_files(directory):
        segments = load_whisperx_subtitle_segments(json_file, logger=LOGGER)
        segment_labels = {
            str(segment.get("speaker") or "").strip()
            for segment in segments
            if str(segment.get("speaker") or "").strip()
        }
        if segment_labels:
            transcript_count += 1
            labels.update(segment_labels)
    return labels, transcript_count


def transcript_json_files(directory: Path) -> list[Path]:
    if not directory.is_dir():
        return []
    return sorted(
        path
        for path in directory.glob("*.json")
        if path.is_file()
        and not path.name.endswith(".voice-attribution.json")
        and not path.name.endswith(".stream-events.json")
        and not is_powerchat_event_file(path.name)
        and not is_live_chat_file(path.name)
        and not is_chat_timing_file(path.name)
    )


def content_event_statuses_for_directory(directory: Path) -> list[ContentEventStatus]:
    if not directory.is_dir():
        return []
    rows: list[ContentEventStatus] = []
    for sidecar in sorted(directory.glob("*.stream-events.json")):
        try:
            raw_payload = json.loads(sidecar.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        payload = raw_payload if isinstance(raw_payload, dict) else {}
        media_name = str(
            payload.get("media")
            or sidecar.name.removesuffix(".stream-events.json")
        )
        payload_events = [
            event
            for event in payload.get("events", [])
            if isinstance(event, dict)
        ]
        for event in payload_events:
            rows.append(content_event_status(media_name, event))
    return sorted(rows, key=lambda item: item.start)


def content_event_status(media_name: str, event: dict[str, Any]) -> ContentEventStatus:
    return ContentEventStatus(
        media_name=media_name,
        start=float(event.get("start") or 0.0),
        end=float(event.get("end") or 0.0),
        duration=float(event.get("duration") or 0.0),
        rule=str(event.get("rule") or ""),
        severity=str(event.get("severity") or "info"),
        score=float(event.get("score") or 0.0),
        loudness_dbfs=(
            float(event["loudness_dbfs"])
            if event.get("loudness_dbfs") is not None
            else None
        ),
        labels=[label for label in event.get("labels", []) if isinstance(label, dict)],
        keywords=[str(keyword) for keyword in event.get("keywords", [])],
        voice=str(event.get("voice") or ""),
        text=str(event.get("text") or ""),
    )


def powerchat_status_for_directory(
    directory: Path,
) -> tuple[list[PowerchatEventStatus], list[dict[str, Any]], list[dict[str, Any]]]:
    if not directory.is_dir():
        return [], [], []
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for sidecar in sorted(directory.glob(f"*{POWERCHAT_EVENT_SUFFIX}")):
        payload = load_powerchat_sidecar(sidecar)
        for event in payload.get("events", []):
            if not isinstance(event, dict):
                continue
            key = str(event.get("dedupe_key") or event.get("id") or "").strip()
            if not key:
                key = hashlib.sha1(
                    json.dumps(event, sort_keys=True, default=str).encode("utf-8")
                ).hexdigest()
            if key in seen:
                continue
            seen.add(key)
            rows.append(event)
    rows.sort(
        key=lambda event: (
            event.get("offset_seconds") is None,
            float(event.get("offset_seconds") or 0.0),
            str(event.get("received_at") or ""),
        )
    )
    totals = powerchat_totals(rows)
    return (
        [powerchat_event_status(event) for event in rows],
        totals.get("money", []),
        totals.get("units", []),
    )


def powerchat_event_status(event: dict[str, Any]) -> PowerchatEventStatus:
    return PowerchatEventStatus(
        source=str(event.get("source") or ""),
        received_at=str(event.get("received_at") or ""),
        offset_seconds=(
            float(event["offset_seconds"])
            if event.get("offset_seconds") is not None
            else None
        ),
        kind=str(event.get("kind") or "unknown"),
        donor=str(event.get("donor") or ""),
        platform=str(event.get("platform") or ""),
        message=str(event.get("message") or ""),
        money_amount=(
            float(event["money_amount"])
            if event.get("money_amount") is not None
            else None
        ),
        money_currency=str(event.get("money_currency") or ""),
        unit_amount=(
            float(event["unit_amount"])
            if event.get("unit_amount") is not None
            else None
        ),
        unit=str(event.get("unit") or ""),
    )


def stream_status_from_record(
    config: BotConfig,
    record: StreamRecord,
    watermark_records: list[WatermarkCopyRecord] | None = None,
    event_records: list[StreamEventRecord] | None = None,
    job_records: list[JobStatus] | None = None,
) -> StreamStatus:
    started_at = time.perf_counter()
    steps: list[tuple[str, float]] = []
    directory = segment_directory(config, record.video_id, record.channel)

    file_scan_cache_ttl = (
        WEB_FILE_SCAN_ACTIVE_CACHE_SECONDS
        if record.status in {"detected", "downloading", "checking_after_exit", "waiting_retry"}
        else WEB_FILE_SCAN_CACHE_SECONDS
    )
    step_started_at = time.perf_counter()
    file_summary = summarize_files(
        config,
        directory,
        record.video_id,
        config.watermark_enabled and bool(watermark_secret(config)),
        watermark_records or [],
        platform=record.platform,
        cache_ttl_seconds=file_scan_cache_ttl,
    )
    perf_step(steps, "files", step_started_at)

    total_bytes = file_summary.total_bytes
    bytes_by_kind = file_summary.bytes_by_kind
    counts_by_kind = file_summary.counts_by_kind
    latest_file_modified_at = file_summary.latest_modified_at

    step_started_at = time.perf_counter()
    content_events = content_event_statuses_for_directory(directory)
    perf_step(steps, "content_events", step_started_at)

    step_started_at = time.perf_counter()
    powerchat_events, powerchat_money_totals, powerchat_unit_totals = (
        powerchat_status_for_directory(directory)
    )
    perf_step(steps, "powerchat", step_started_at)

    step_started_at = time.perf_counter()
    segment_name = f"segment-{record.segment_index:03d}"
    has_part_files = segment_name in file_summary.part_segments
    has_mixed_formats = has_part_files and segment_name in file_summary.final_format_segments
    perf_step(steps, "resume_flags", step_started_at)

    status = StreamStatus(
        video_id=record.video_id,
        title=record.title,
        channel=record.channel,
        url=record.url,
        platform=record.platform,
        source=record.source,
        status=record.status,
        segment_index=record.segment_index,
        first_seen_at=record.first_seen_at,
        updated_at=record.updated_at,
        last_started_at=record.last_started_at,
        last_exit_at=record.last_exit_at,
        exit_code=record.exit_code,
        directory=str(directory),
        file_count=file_summary.file_count,
        total_bytes=total_bytes,
        part_bytes=bytes_by_kind.get("part", 0),
        final_bytes=bytes_by_kind.get("final", 0),
        chat_bytes=bytes_by_kind.get("chat", 0),
        fragment_bytes=bytes_by_kind.get("fragment", 0),
        state_bytes=bytes_by_kind.get("state", 0),
        temporary_bytes=bytes_by_kind.get("temporary", 0),
        file_kind_counts=counts_by_kind,
        latest_file_modified_at=latest_file_modified_at,
        has_part_files=has_part_files,
        has_mixed_formats=has_mixed_formats,
        events=[stream_event_status(event) for event in event_records or []],
        content_event_count=len(content_events),
        content_events=content_events,
        powerchat_event_count=len(powerchat_events),
        powerchat_money_totals=powerchat_money_totals,
        powerchat_unit_totals=powerchat_unit_totals,
        powerchat_events=powerchat_events,
        jobs=list(job_records or []),
        files=file_summary.files,
    )
    log_perf(
        "stream-status",
        perf_elapsed(started_at),
        WEB_SLOW_STREAM_SECONDS,
        video_id=record.video_id,
        status=record.status,
        files=file_summary.file_count,
        directory=directory,
        steps=format_perf_steps(steps),
    )
    return status


def stream_event_status(event: StreamEventRecord) -> StreamEventStatus:
    return StreamEventStatus(
        event_id=event.event_id,
        level=event.level,
        message=event.message,
        segment_index=event.segment_index,
        created_at=event.created_at,
    )


def empty_stream_file_summary() -> StreamFileSummary:
    return stream_file_summary_from_scan(empty_directory_scan_summary(), [])


def empty_directory_scan_summary() -> DirectoryScanSummary:
    return DirectoryScanSummary(
        directory_entry_count=0,
        file_count=0,
        total_bytes=0,
        bytes_by_kind={},
        counts_by_kind={},
        latest_modified_at=None,
        part_segments=(),
        final_format_segments=(),
        visible_entries=(),
    )


def stream_file_summary_from_scan(
    scan: DirectoryScanSummary,
    files: list[FileStatus],
) -> StreamFileSummary:
    return StreamFileSummary(
        file_count=scan.file_count,
        total_bytes=scan.total_bytes,
        bytes_by_kind=dict(scan.bytes_by_kind),
        counts_by_kind=dict(scan.counts_by_kind),
        latest_modified_at=scan.latest_modified_at,
        part_segments=scan.part_segments,
        final_format_segments=scan.final_format_segments,
        files=files,
    )


def directory_scan_fingerprint(directory: Path) -> tuple[int, int] | None:
    try:
        stat = directory.stat()
    except OSError:
        return None
    return (stat.st_mtime_ns, stat.st_size)


def cached_directory_scan_summary(
    directory: Path,
    video_id: str,
    cache_ttl_seconds: float,
) -> tuple[DirectoryScanSummary, str]:
    fingerprint = directory_scan_fingerprint(directory)
    if fingerprint is None:
        return empty_directory_scan_summary(), "missing"

    cache_key = str(directory)
    now = time.monotonic()
    cache_status = "disabled"
    if cache_ttl_seconds >= 0 and WEB_FILE_SCAN_CACHE_MAX_ENTRIES > 0:
        with FILE_SCAN_CACHE_LOCK:
            cached = FILE_SCAN_CACHE.get(cache_key)
            if cached is None:
                cache_status = "miss"
            elif cached.fingerprint != fingerprint:
                cache_status = "changed"
            elif now - cached.cached_at <= cache_ttl_seconds:
                FILE_SCAN_CACHE.move_to_end(cache_key)
                return cached.summary, "hit"
            else:
                cache_status = "expired"

    summary = scan_directory_uncached(directory, video_id)
    if cache_ttl_seconds >= 0 and WEB_FILE_SCAN_CACHE_MAX_ENTRIES > 0:
        with FILE_SCAN_CACHE_LOCK:
            FILE_SCAN_CACHE[cache_key] = DirectoryScanCacheEntry(
                fingerprint=fingerprint,
                cached_at=now,
                summary=summary,
            )
            FILE_SCAN_CACHE.move_to_end(cache_key)
            while len(FILE_SCAN_CACHE) > WEB_FILE_SCAN_CACHE_MAX_ENTRIES:
                FILE_SCAN_CACHE.popitem(last=False)
    return summary, cache_status


def scan_directory_uncached(directory: Path, video_id: str) -> DirectoryScanSummary:
    directory_entry_count = 0
    file_count = 0
    total_bytes = 0
    bytes_by_kind: dict[str, int] = {}
    counts_by_kind: dict[str, int] = {}
    latest_modified_at: float | None = None
    part_segments: set[str] = set()
    final_format_segments: set[str] = set()
    visible_entries: list[tuple[str, int, CachedFileEntry]] = []
    sequence = 0

    try:
        with os.scandir(directory) as entries:
            for entry in entries:
                directory_entry_count += 1
                try:
                    if not entry.is_file(follow_symlinks=False):
                        continue
                    stat = entry.stat(follow_symlinks=False)
                except OSError:
                    continue

                kind = file_kind(entry.name)
                segment, format_id = file_details(entry.name)
                file_count += 1
                total_bytes += stat.st_size
                bytes_by_kind[kind] = bytes_by_kind.get(kind, 0) + stat.st_size
                counts_by_kind[kind] = counts_by_kind.get(kind, 0) + 1
                if latest_modified_at is None or stat.st_mtime > latest_modified_at:
                    latest_modified_at = stat.st_mtime
                if segment and kind == "part":
                    part_segments.add(segment)
                elif segment and format_id and kind == "final":
                    final_format_segments.add(segment)

                if FILE_LIMIT_PER_STREAM <= 0:
                    continue
                cached_entry = CachedFileEntry(
                    name=entry.name,
                    size_bytes=stat.st_size,
                    modified_at=stat.st_mtime,
                    kind=kind,
                    segment=segment,
                    format_id=format_id,
                )
                if len(visible_entries) < FILE_LIMIT_PER_STREAM:
                    insort(visible_entries, (entry.name, sequence, cached_entry))
                elif entry.name < visible_entries[-1][0]:
                    visible_entries.pop()
                    insort(visible_entries, (entry.name, sequence, cached_entry))
                sequence += 1
    except OSError as exc:
        LOGGER.warning(
            "Unable to scan stream directory video_id=%s directory=%s error=%s",
            video_id,
            directory,
            exc,
        )
        return empty_directory_scan_summary()

    return DirectoryScanSummary(
        directory_entry_count=directory_entry_count,
        file_count=file_count,
        total_bytes=total_bytes,
        bytes_by_kind=bytes_by_kind,
        counts_by_kind=counts_by_kind,
        latest_modified_at=latest_modified_at,
        part_segments=tuple(sorted(part_segments)),
        final_format_segments=tuple(sorted(final_format_segments)),
        visible_entries=tuple(item[2] for item in visible_entries),
    )


def summarize_files(
    config: BotConfig,
    directory: Path,
    video_id: str,
    watermark_enabled: bool = False,
    watermark_records: list[WatermarkCopyRecord] | None = None,
    platform: str = "youtube",
    cache_ttl_seconds: float = WEB_FILE_SCAN_CACHE_SECONDS,
) -> StreamFileSummary:
    started_at = time.perf_counter()
    if not directory.exists():
        return empty_stream_file_summary()

    watermarks_by_source: dict[str, list[WatermarkCopyRecord]] = {}
    for record in watermark_records or []:
        watermarks_by_source.setdefault(record.source_name, []).append(record)

    scan_started_at = time.perf_counter()
    scan_summary, scan_cache_status = cached_directory_scan_summary(
        directory,
        video_id,
        cache_ttl_seconds,
    )
    scan_elapsed = perf_elapsed(scan_started_at)

    files: list[FileStatus] = []
    slow_files = 0

    for entry in scan_summary.visible_entries:
        file_started_at = time.perf_counter()
        file_steps: list[tuple[str, float]] = []
        chat_actions_enabled = platform in {"youtube", "kick"}

        action_started_at = time.perf_counter()
        render_chat_url, render_chat_output_url, render_chat_status, render_chat_message = (
            chat_render_action_for_file(
                directory,
                video_id,
                entry.name,
                allow_single_media_fallback=False,
            )
            if chat_actions_enabled
            else (None, None, None, None)
        )
        perf_step(file_steps, "chat_render", action_started_at)

        action_started_at = time.perf_counter()
        refresh_chat_url, refresh_chat_status, refresh_chat_message = (
            chat_refresh_action_for_file(
                config,
                directory,
                video_id,
                entry.name,
                allow_single_media_fallback=False,
            )
            if chat_actions_enabled
            else (None, None, None)
        )
        if platform == "kick" and refresh_chat_url is None:
            refresh_chat_url, refresh_chat_status, refresh_chat_message = (
                kick_chat_download_action_for_file(directory, video_id, entry.name)
            )
        perf_step(file_steps, "chat_refresh", action_started_at)

        action_started_at = time.perf_counter()
        transcription_url, transcription_status, transcription_message = (
            transcription_action_for_file(directory, video_id, entry.name)
        )
        perf_step(file_steps, "transcription", action_started_at)

        action_started_at = time.perf_counter()
        event_detection_url, event_detection_status, event_detection_message = (
            event_detection_action_for_file(config, directory, video_id, entry.name)
        )
        perf_step(file_steps, "event_detection", action_started_at)

        action_started_at = time.perf_counter()
        watermark_copies = [
            watermark_copy_status(copy)
            for copy in watermarks_by_source.get(entry.name, [])
            if copy.status in WATERMARK_JOB_STATUSES
        ]
        watermark_url = (
            watermark_url_for(video_id, entry.name)
            if watermark_enabled and is_watermarkable_media_file(entry.name)
            else None
        )
        perf_step(file_steps, "watermark", action_started_at)

        files.append(
            FileStatus(
                video_id=video_id,
                name=entry.name,
                size_bytes=entry.size_bytes,
                modified_at=entry.modified_at,
                kind=entry.kind,
                segment=entry.segment,
                format_id=entry.format_id,
                download_url=download_url_for(video_id, entry.name)
                if is_downloadable_file(entry.name)
                else None,
                render_chat_url=render_chat_url,
                render_chat_output_url=render_chat_output_url,
                render_chat_status=render_chat_status,
                render_chat_message=render_chat_message,
                refresh_chat_url=refresh_chat_url,
                refresh_chat_status=refresh_chat_status,
                refresh_chat_message=refresh_chat_message,
                transcription_url=transcription_url,
                transcription_status=transcription_status,
                transcription_message=transcription_message,
                event_detection_url=event_detection_url,
                event_detection_status=event_detection_status,
                event_detection_message=event_detection_message,
                watermark_url=watermark_url,
                watermark_copies=watermark_copies,
                watermark_copy_id=None,
                watermark_recipient_label=None,
                watermark_delete_url=None,
            )
        )
        file_elapsed = perf_elapsed(file_started_at)
        if should_log_perf(file_elapsed, WEB_SLOW_FILE_SECONDS):
            slow_files += 1
            LOGGER.warning(
                "Slow web file-status elapsed=%.3fs video_id=%s file=%s kind=%s steps=%s",
                file_elapsed,
                video_id,
                entry.name,
                entry.kind,
                format_perf_steps(file_steps),
            )

    watermark_file_count = 0
    watermark_total_bytes = 0
    latest_modified_at = scan_summary.latest_modified_at
    counts_by_kind = dict(scan_summary.counts_by_kind)
    bytes_by_kind = dict(scan_summary.bytes_by_kind)
    for record in watermark_records or []:
        if record.status != WATERMARK_STATUS_DONE:
            continue
        output_file = resolve_watermark_output_file(directory, record.output_name)
        if output_file is None or not output_file.is_file():
            continue
        try:
            stat = output_file.stat()
        except OSError:
            continue
        watermark_file_count += 1
        watermark_total_bytes += stat.st_size
        counts_by_kind["watermark"] = counts_by_kind.get("watermark", 0) + 1
        bytes_by_kind["watermark"] = bytes_by_kind.get("watermark", 0) + stat.st_size
        if latest_modified_at is None or stat.st_mtime > latest_modified_at:
            latest_modified_at = stat.st_mtime
        files.append(watermark_file_status(video_id, record, stat.st_size, stat.st_mtime))

    files.sort(key=lambda file: file.name.casefold())
    log_perf(
        "file-scan",
        perf_elapsed(started_at),
        WEB_SLOW_STREAM_SECONDS,
        video_id=video_id,
        directory=directory,
        entries=scan_summary.directory_entry_count,
        files=scan_summary.file_count + watermark_file_count,
        detailed=len(files),
        skipped_detail_files=max(0, scan_summary.file_count + watermark_file_count - len(files)),
        slow_files=slow_files,
        cache=scan_cache_status,
        scan=f"{scan_elapsed:.3f}s",
    )
    return StreamFileSummary(
        file_count=scan_summary.file_count + watermark_file_count,
        total_bytes=scan_summary.total_bytes + watermark_total_bytes,
        bytes_by_kind=bytes_by_kind,
        counts_by_kind=counts_by_kind,
        latest_modified_at=latest_modified_at,
        part_segments=scan_summary.part_segments,
        final_format_segments=scan_summary.final_format_segments,
        files=files,
    )


def watermark_file_status(
    video_id: str,
    record: WatermarkCopyRecord,
    size_bytes: int,
    modified_at: float,
) -> FileStatus:
    return FileStatus(
        video_id=video_id,
        name=record.output_name,
        size_bytes=size_bytes,
        modified_at=modified_at,
        kind="watermark",
        segment=None,
        format_id=None,
        download_url=watermark_download_url_for(record.copy_id),
        render_chat_url=None,
        render_chat_output_url=None,
        render_chat_status=None,
        render_chat_message=None,
        refresh_chat_url=None,
        refresh_chat_status=None,
        refresh_chat_message=None,
        transcription_url=None,
        transcription_status=None,
        transcription_message=None,
        event_detection_url=None,
        event_detection_status=None,
        event_detection_message=None,
        watermark_url=None,
        watermark_copies=[],
        watermark_copy_id=record.copy_id,
        watermark_recipient_label=record.recipient_label,
        watermark_delete_url=watermark_delete_url_for(record.copy_id)
        if watermark_copy_delete_allowed(record)
        else None,
    )


def download_url_for(video_id: str, filename: str) -> str:
    return "/download?" + urlencode({"video_id": video_id, "name": filename})


def powerchat_export_url(
    export_format: str,
    *,
    streamer: str = "",
    video_id: str = "",
) -> str:
    params = {"format": export_format}
    if streamer:
        params["streamer"] = streamer
    if video_id:
        params["video_id"] = video_id
    return "/powerchat-events?" + urlencode(params)


def render_chat_url_for(
    video_id: str,
    chat_filename: str,
    *,
    regenerate: bool = False,
) -> str:
    params = {"video_id": video_id, "chat": chat_filename}
    if regenerate:
        params["regenerate"] = "1"
    return "/render-chat?" + urlencode(params)


def refresh_chat_url_for(video_id: str, chat_filename: str) -> str:
    return "/refresh-chat?" + urlencode({"video_id": video_id, "chat": chat_filename})


def transcription_url_for(
    video_id: str,
    filename: str,
    *,
    regenerate: bool = False,
) -> str:
    params = {"video_id": video_id, "name": filename}
    if regenerate:
        params["regenerate"] = "1"
    return "/transcribe?" + urlencode(params)


def event_detection_url_for(
    video_id: str,
    filename: str,
    *,
    regenerate: bool = False,
) -> str:
    params = {"video_id": video_id, "name": filename}
    if regenerate:
        params["regenerate"] = "1"
    return "/detect-events?" + urlencode(params)


def watermark_url_for(video_id: str, filename: str) -> str:
    return "/watermark"


def watermark_download_url_for(copy_id: str) -> str:
    return "/download-watermark?" + urlencode({"copy_id": copy_id})


def watermark_delete_url_for(copy_id: str) -> str:
    return "/delete-watermark"


def watermark_copy_delete_allowed(record: WatermarkCopyRecord) -> bool:
    return record.status not in {WATERMARK_STATUS_QUEUED, WATERMARK_STATUS_RUNNING}


def watermark_copy_status(record: WatermarkCopyRecord) -> WatermarkCopyStatus:
    return WatermarkCopyStatus(
        copy_id=record.copy_id,
        source_name=record.source_name,
        output_name=record.output_name,
        recipient_label=record.recipient_label,
        status=record.status,
        message=record.message,
        error=record.error,
        phase=record.phase,
        progress=record.progress,
        created_at=record.created_at,
        updated_at=record.updated_at,
        started_at=record.started_at,
        finished_at=record.finished_at,
        download_url=watermark_download_url_for(record.copy_id)
        if record.status == WATERMARK_STATUS_DONE
        else None,
        delete_url=watermark_delete_url_for(record.copy_id)
        if watermark_copy_delete_allowed(record)
        else None,
    )


def first_query_value(params: dict[str, list[str]], key: str) -> str:
    values = params.get(key) or []
    return values[0] if values else ""


def query_flag(params: dict[str, list[str]], key: str) -> bool:
    value = first_query_value(params, key).strip().casefold()
    return value in {"1", "true", "yes", "on"}


def parse_multipart_upload(
    content_type: str,
    body: bytes,
    field_name: str,
) -> tuple[str, bytes | None]:
    message = BytesParser(policy=email_policy).parsebytes(
        (
            f"Content-Type: {content_type}\r\n"
            "MIME-Version: 1.0\r\n\r\n"
        ).encode("utf-8")
        + body
    )
    if not message.is_multipart():
        return "", None
    for part in message.iter_parts():
        if part.get_content_disposition() != "form-data":
            continue
        if part.get_param("name", header="content-disposition") != field_name:
            continue
        filename = part.get_filename() or ""
        payload = part.get_payload(decode=True)
        if payload is None:
            return filename, None
        return filename, payload
    return "", None


def parse_multipart_form(
    content_type: str,
    body: bytes,
) -> tuple[dict[str, list[str]], dict[str, tuple[str, bytes]]]:
    message = BytesParser(policy=email_policy).parsebytes(
        (
            f"Content-Type: {content_type}\r\n"
            "MIME-Version: 1.0\r\n\r\n"
        ).encode("utf-8")
        + body
    )
    if not message.is_multipart():
        raise ConfigError("Expected multipart form upload")
    fields: dict[str, list[str]] = {}
    files: dict[str, tuple[str, bytes]] = {}
    for part in message.iter_parts():
        if part.get_content_disposition() != "form-data":
            continue
        name = part.get_param("name", header="content-disposition") or ""
        if not name:
            continue
        payload = part.get_payload(decode=True) or b""
        filename = part.get_filename()
        if filename is not None:
            files[name] = (filename, payload)
        else:
            fields.setdefault(name, []).append(payload.decode("utf-8", "replace"))
    return fields, files


def resolve_download_file(
    config: BotConfig,
    video_id: str,
    filename: str,
) -> Path | None:
    if not video_id or not filename:
        return None
    if Path(filename).name != filename or "/" in filename or "\\" in filename:
        return None

    state = StateStore(config.db_path)
    try:
        record = state.get_stream(video_id)
    finally:
        state.close()
    if record is None:
        return None

    directory = segment_directory(config, record.video_id, record.channel)
    candidate = directory / filename
    try:
        directory_path = directory.resolve(strict=True)
        candidate_path = candidate.resolve(strict=True)
    except OSError:
        return None

    if candidate_path.parent != directory_path:
        return None
    if not candidate_path.is_file() or not is_downloadable_file(candidate_path.name):
        return None
    return candidate_path


FRAGMENT_CLEANUP_BLOCKED_STATUSES = {
    "checking_after_exit",
    "downloading",
    "waiting_retry",
}
STREAM_DELETE_BLOCKED_STATUSES = {
    "checking_after_exit",
    "detected",
    "downloading",
    "waiting_retry",
}
STREAM_DELETE_CONFIRM_VALUE = "delete_stream"


def cleanup_stream_fragments(config: BotConfig, video_id: str) -> tuple[int, int]:
    if not video_id:
        raise ConfigError("Missing stream id")

    state = StateStore(config.db_path)
    try:
        record = state.get_stream(video_id)
    finally:
        state.close()
    if record is None:
        raise ConfigError("Stream was not found")
    if record.status in FRAGMENT_CLEANUP_BLOCKED_STATUSES:
        raise ConfigError("Cannot clean fragments while the stream may still resume")

    directory = segment_directory(config, record.video_id, record.channel)
    try:
        directory_path = directory.resolve(strict=True)
    except OSError:
        return 0, 0

    try:
        candidates = list(directory_path.iterdir())
    except OSError:
        return 0, 0

    fragments: list[Path] = []
    for path in candidates:
        if path.is_symlink() or not path.is_file() or file_kind(path.name) != "fragment":
            continue
        try:
            candidate = path.resolve(strict=True)
        except OSError:
            continue
        if candidate.parent == directory_path:
            fragments.append(path)

    bytes_removed = 0
    for path in fragments:
        try:
            bytes_removed += path.stat().st_size
        except OSError:
            continue
    cleanup_files(fragments, LOGGER)

    state = StateStore(config.db_path)
    try:
        state.add_stream_event(
            record.video_id,
            f"Cleaned {len(fragments)} fragment file(s), freed {format_bytes(bytes_removed)}",
        )
    finally:
        state.close()
    return len(fragments), bytes_removed


def delete_stream(
    config: BotConfig,
    video_id: str,
    confirm_delete: str = "",
) -> tuple[bool, str]:
    video_id = video_id.strip()
    if not video_id:
        return False, "Stream id is required"
    if confirm_delete != STREAM_DELETE_CONFIRM_VALUE:
        return False, "Stream deletion was not confirmed"

    active_jobs = [
        job
        for job in list_tracked_jobs(limit=1000)
        if job.video_id == video_id and job.status in {"queued", "running"}
    ]
    if active_jobs:
        return False, "Cannot delete a stream while dashboard jobs are still running"

    state = StateStore(config.db_path)
    try:
        record = state.get_stream(video_id)
        if record is None:
            return False, "Stream was not found"
        if record.status in STREAM_DELETE_BLOCKED_STATUSES:
            return False, "Cannot delete a stream while it may still be active or resume"
        active_watermarks = state.list_watermark_copies(
            video_id=video_id,
            statuses=[WATERMARK_STATUS_QUEUED, WATERMARK_STATUS_RUNNING],
            limit=1,
        )
        if active_watermarks:
            return False, "Cannot delete a stream while watermark jobs are still running"
    finally:
        state.close()

    directory = segment_directory(config, record.video_id, record.channel)
    bytes_removed = 0
    files_removed = 0
    removed_directory = False
    if directory.exists():
        try:
            directory_path = safe_stream_directory_for_delete(config, directory)
            files_removed, bytes_removed = directory_file_totals(directory_path)
            shutil.rmtree(directory_path)
            removed_directory = True
        except OSError as exc:
            return False, f"Unable to delete stream files: {exc}"
        except ConfigError as exc:
            return False, str(exc)

    state = StateStore(config.db_path)
    try:
        deleted = state.delete_stream(video_id)
    finally:
        state.close()
    if not deleted:
        return False, "Stream was not found"

    with FILE_SCAN_CACHE_LOCK:
        FILE_SCAN_CACHE.pop(str(directory), None)
        if directory.exists():
            FILE_SCAN_CACHE.pop(str(directory.resolve()), None)

    LOGGER.info(
        "Deleted stream video_id=%s files=%s bytes=%s directory_removed=%s",
        video_id,
        files_removed,
        bytes_removed,
        removed_directory,
    )
    detail = f", removed {files_removed} file(s) / {format_bytes(bytes_removed)}" if removed_directory else ""
    return True, f"Stream deleted{detail}"


def start_vod_redownload_job(
    config: BotConfig,
    video_id: str,
    vod_url: str,
) -> tuple[bool, str]:
    video_id = video_id.strip()
    vod_url = vod_url.strip()
    if not video_id:
        return False, "Stream id is required"
    if not vod_url:
        return False, "VOD URL is required"
    try:
        resolve_source(vod_url)
    except SourceError as exc:
        return False, str(exc)

    state = StateStore(config.db_path)
    try:
        record = state.get_stream(video_id)
    finally:
        state.close()
    if record is None:
        return False, "Stream was not found"
    if record.status in VOD_DOWNLOAD_BLOCKED_STATUSES:
        return False, "Cannot redownload from VOD while the stream may still be active or resume"

    job_id = vod_download_job_id(record.video_id, vod_url)
    if tracked_job_is_running(job_id):
        return True, "VOD download is already running"

    stream = stream_from_record(record, url=vod_url, is_live=False)
    output_template = vod_output_template_for(config, stream, force_copy=True)
    queue_vod_download_job(
        config,
        job_id,
        stream,
        vod_url,
        output_template,
        previous_status=record.status,
        queued_message=f"Started VOD redownload from {vod_url}",
        item=output_template.name.replace("%(ext)s", "media"),
    )
    return True, "VOD redownload queued"


def start_manual_vod_download_job(
    config: BotConfig,
    streamer_name: str,
    vod_url: str,
) -> tuple[bool, str]:
    streamer_name = streamer_name.strip()
    vod_url = vod_url.strip()
    if not streamer_name:
        return False, "Streamer name is required"
    if streamer_name not in config.streamers:
        return False, "Streamer was not found"
    if not vod_url:
        return False, "VOD URL is required"

    try:
        stream = probe_vod_stream(config, vod_url, channel_override=streamer_name)
    except (SourceError, YtDlpError) as exc:
        return False, str(exc)

    state = StateStore(config.db_path)
    try:
        existing = state.get_stream(stream.video_id)
    finally:
        state.close()
    if existing is not None and existing.status in VOD_DOWNLOAD_BLOCKED_STATUSES:
        return False, "A matching stream is still active or may resume"

    job_id = vod_download_job_id(stream.video_id, vod_url)
    if tracked_job_is_running(job_id):
        return True, "VOD download is already running"

    output_template = vod_output_template_for(config, stream, force_copy=False)
    queue_vod_download_job(
        config,
        job_id,
        stream,
        vod_url,
        output_template,
        previous_status=existing.status if existing is not None else None,
        queued_message=f"Started manual VOD download from {vod_url}",
        item=output_template.name.replace("%(ext)s", "media"),
    )
    return True, "Manual VOD download queued"


def probe_vod_stream(
    config: BotConfig,
    vod_url: str,
    *,
    channel_override: str = "",
) -> LiveStream:
    spec = resolve_source(vod_url)
    info = YtDlpRunner(config.yt_dlp_path).run_json(
        [
            "--dump-json",
            "--skip-download",
            "--no-playlist",
            "--no-warnings",
            spec.url,
        ]
    )
    if spec.platform == "youtube":
        stream = live_stream_from_info(info, fallback_url=spec.url)
    else:
        stream = live_stream_from_generic_info(
            info,
            platform=spec.platform,
            fallback_url=spec.url,
            source=vod_url,
        )
    return LiveStream(
        video_id=stream.video_id,
        url=stream.url or spec.url,
        title=stream.title,
        channel=channel_override or stream.channel,
        live_status=stream.live_status,
        is_live=False,
        platform=stream.platform,
        source=vod_url,
        raw=stream.raw,
    )


def queue_vod_download_job(
    config: BotConfig,
    job_id: str,
    stream: LiveStream,
    vod_url: str,
    output_template: Path,
    *,
    previous_status: str | None,
    queued_message: str,
    item: str,
) -> None:
    output_template.parent.mkdir(parents=True, exist_ok=True)
    state = StateStore(config.db_path)
    try:
        state.mark_vod_downloading(stream, message=queued_message)
    finally:
        state.close()

    start_tracked_job(
        job_id,
        kind="VOD download",
        video_id=stream.video_id,
        item=item,
        detail=vod_url,
        phase="Queued",
        message="Queued VOD download",
        progress=0.0,
    )
    thread = Thread(
        target=run_vod_download_job,
        args=(config, job_id, stream, vod_url, output_template, previous_status),
        name=f"onlysavemevods-vod-download-{stream.video_id}",
        daemon=True,
    )
    thread.start()
    LOGGER.info(
        "Queued VOD download video_id=%s url=%s output=%s",
        stream.video_id,
        vod_url,
        output_template,
    )


def run_vod_download_job(
    config: BotConfig,
    job_id: str,
    stream: LiveStream,
    vod_url: str,
    output_template: Path,
    previous_status: str | None = None,
) -> None:
    command = build_vod_download_command(config, vod_url, output_template)
    update_tracked_job(
        job_id,
        phase="Starting yt-dlp",
        message="Starting VOD download",
        progress=0.02,
    )
    LOGGER.debug("yt-dlp VOD command for %s: %s", stream.video_id, command_for_log(command))
    last_output = ""
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
    except FileNotFoundError as exc:
        message = f"yt-dlp binary not found: {config.yt_dlp_path}"
        finish_failed_vod_download(config, job_id, stream.video_id, message, previous_status)
        LOGGER.exception("Unable to start yt-dlp for VOD download")
        return
    except OSError as exc:
        message = str(exc) or exc.__class__.__name__
        finish_failed_vod_download(config, job_id, stream.video_id, message, previous_status)
        LOGGER.exception("Unable to start VOD download for %s", stream.video_id)
        return

    if process.stdout is not None:
        for line in process.stdout:
            stripped = line.strip()
            if stripped:
                last_output = stripped
            progress = vod_download_progress_from_line(stripped)
            if progress is not None:
                update_tracked_job(
                    job_id,
                    phase=f"Downloading {progress * 100:.1f}%",
                    message=stripped,
                    progress=min(0.98, max(0.02, progress)),
                )
            elif stripped:
                update_tracked_job(job_id, message=stripped)
    return_code = process.wait()
    if return_code != 0:
        message = last_output or f"yt-dlp exited with code {return_code}"
        finish_failed_vod_download(
            config,
            job_id,
            stream.video_id,
            message,
            previous_status,
            exit_code=return_code,
        )
        LOGGER.warning(
            "VOD download failed video_id=%s code=%s output=%s",
            stream.video_id,
            return_code,
            message,
        )
        return

    job_message = "VOD download completed"
    stream_message = f"VOD download completed from {vod_url}"
    if stream.platform == "youtube":
        chat_ok, chat_message = run_youtube_vod_chat_download_job(
            config,
            job_id,
            stream,
            vod_url,
            output_template,
        )
        if chat_ok:
            job_message = "VOD download completed with live chat replay"
            stream_message = f"VOD download completed with live chat replay from {vod_url}"
        else:
            reason = chat_message or "live chat replay was not available"
            job_message = "VOD download completed; live chat replay unavailable"
            stream_message = f"VOD download completed from {vod_url}; live chat replay unavailable"
            record_stream_event(
                config,
                stream.video_id,
                f"YouTube VOD live chat replay unavailable: {reason}",
                level="warning",
                segment_index=1,
            )
    elif stream.platform == "kick":
        chat_ok, chat_message = run_kick_vod_chat_download_job(
            config,
            job_id,
            stream,
            output_template,
        )
        if chat_ok:
            job_message = "VOD download completed with Kick chat replay"
            stream_message = f"VOD download completed with Kick chat replay from {vod_url}"
        else:
            reason = chat_message or "Kick chat replay was not available"
            job_message = "VOD download completed; Kick chat replay unavailable"
            stream_message = f"VOD download completed from {vod_url}; Kick chat replay unavailable"
            record_stream_event(
                config,
                stream.video_id,
                f"Kick VOD chat replay unavailable: {reason}",
                level="warning",
                segment_index=1,
            )

    finish_tracked_job(
        job_id,
        status="done",
        phase="Complete",
        message=job_message,
        progress=1.0,
    )
    state = StateStore(config.db_path)
    try:
        state.mark_vod_download_finished(
            stream.video_id,
            message=stream_message,
        )
    finally:
        state.close()
    with FILE_SCAN_CACHE_LOCK:
        FILE_SCAN_CACHE.pop(str(output_template.parent), None)
    queue_vod_post_processing_jobs(config, stream, output_template)
    LOGGER.info("VOD download completed video_id=%s output=%s", stream.video_id, output_template)


def queue_vod_post_processing_jobs(
    config: BotConfig,
    stream: LiveStream,
    output_template: Path,
) -> None:
    media_file = vod_media_file_for_output_template(output_template)
    if media_file is None:
        record_stream_event(
            config,
            stream.video_id,
            "VOD post-processing skipped: downloaded media file was not found",
            level="warning",
            segment_index=1,
        )
        return

    if config.twitch_ad_repair_enabled and stream.platform == "twitch":
        start_vod_twitch_ad_repair_job(config, stream, media_file)
    if config.transcribe_subtitles:
        ok, message = start_transcription_job(config, stream.video_id, media_file.name)
        if not ok:
            record_stream_event(
                config,
                stream.video_id,
                f"VOD transcription was not queued for {media_file.name}: {message}",
                level="warning",
                segment_index=1,
            )
    if config.stream_event_detection_enabled:
        ok, message = start_event_detection_job(config, stream.video_id, media_file.name)
        if not ok:
            record_stream_event(
                config,
                stream.video_id,
                f"VOD content event detection was not queued for {media_file.name}: {message}",
                level="warning",
                segment_index=1,
            )
    if config.render_live_chat_video and stream.platform in {"youtube", "kick"}:
        chat_file = vod_chat_sidecar_for_media_file(media_file)
        if chat_file.is_file():
            ok, message = start_render_chat_job(config, stream.video_id, chat_file.name)
            if not ok:
                record_stream_event(
                    config,
                    stream.video_id,
                    f"VOD chat render was not queued for {chat_file.name}: {message}",
                    level="warning",
                    segment_index=1,
                )


def vod_media_file_for_output_template(output_template: Path) -> Path | None:
    template_name = output_template.name
    if "%(ext)s" in template_name:
        prefix, _placeholder, suffix = template_name.partition("%(ext)s")
        candidates = [
            path
            for path in output_template.parent.iterdir()
            if path.is_file()
            and path.name.startswith(prefix)
            and path.name.endswith(suffix)
            and is_renderable_media_file(path.name)
            and not is_live_chat_file(path.name)
        ]
    else:
        candidates = [output_template] if output_template.is_file() else []
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def vod_chat_sidecar_for_media_file(media_file: Path) -> Path:
    return media_file.with_name(f"{media_file.stem}.live_chat.json")


def start_vod_twitch_ad_repair_job(
    config: BotConfig,
    stream: LiveStream,
    media_file: Path,
) -> tuple[bool, str]:
    job_id = f"twitch-ad-repair:{stream.video_id}:{media_file.name}"
    if tracked_job_is_running(job_id):
        return True, "Twitch ad repair is already running"
    start_tracked_job(
        job_id,
        kind="Twitch ad repair",
        video_id=stream.video_id,
        item=media_file.name,
        detail="Automatic post-VOD Twitch commercial break detection and repair",
        phase="Queued",
        message="Queued Twitch ad repair",
        progress=0.0,
    )
    record_stream_event(
        config,
        stream.video_id,
        f"Queued Twitch ad repair for {media_file.name}",
        segment_index=1,
    )
    thread = Thread(
        target=run_vod_twitch_ad_repair_job,
        args=(config, job_id, stream, media_file),
        name=f"onlysavemevods-vod-twitch-ad-repair-{stream.video_id}",
        daemon=True,
    )
    thread.start()
    return True, "Twitch ad repair queued"


def run_vod_twitch_ad_repair_job(
    config: BotConfig,
    job_id: str,
    stream: LiveStream,
    media_file: Path,
) -> None:
    def report_progress(phase: str, value: float | None) -> None:
        update_tracked_job(job_id, phase=phase, progress=value, message=phase)

    state = StateStore(config.db_path)
    try:
        record = state.get_stream(stream.video_id)
    finally:
        state.close()
    started_at = record.last_started_at if record else None
    try:
        result = repair_twitch_ads_for_media(
            config,
            stream,
            media_file,
            started_at=started_at,
            progress_callback=report_progress,
            logger=LOGGER,
        )
    except Exception as exc:  # noqa: BLE001 - background job must capture failures.
        LOGGER.exception("VOD Twitch ad repair failed for media=%s", media_file)
        message = str(exc) or exc.__class__.__name__
        finish_tracked_job(
            job_id,
            status="failed",
            phase="Failed",
            message=message,
            progress=None,
        )
        record_stream_event(
            config,
            stream.video_id,
            f"Twitch ad repair failed for {media_file.name}: {message}",
            level="error",
            segment_index=1,
        )
        return

    finish_tracked_job(
        job_id,
        status="done",
        phase="Complete",
        message=result.message,
        progress=1.0,
    )
    record_stream_event(
        config,
        stream.video_id,
        f"Twitch ad repair {'completed' if result.repaired else 'skipped'} for {media_file.name}: {result.message}",
        segment_index=1,
    )


def run_youtube_vod_chat_download_job(
    config: BotConfig,
    job_id: str,
    stream: LiveStream,
    vod_url: str,
    output_template: Path,
) -> tuple[bool, str]:
    update_tracked_job(
        job_id,
        phase="Downloading live chat replay",
        message="Trying YouTube live chat replay",
        progress=0.99,
    )
    command = build_vod_chat_download_command(config, vod_url, output_template)
    LOGGER.debug(
        "yt-dlp YouTube VOD chat command for %s: %s",
        stream.video_id,
        command_for_log(command),
    )
    last_output = ""
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
    except FileNotFoundError:
        message = f"yt-dlp binary not found: {config.yt_dlp_path}"
        LOGGER.warning("Unable to start yt-dlp for YouTube VOD chat: %s", message)
        return False, message
    except OSError as exc:
        message = str(exc) or exc.__class__.__name__
        LOGGER.warning("Unable to start YouTube VOD chat download for %s: %s", stream.video_id, message)
        return False, message

    if process.stdout is not None:
        for line in process.stdout:
            stripped = line.strip()
            if not stripped:
                continue
            last_output = stripped
            update_tracked_job(
                job_id,
                phase="Downloading live chat replay",
                message=stripped,
                progress=0.99,
            )
    return_code = process.wait()
    if return_code != 0:
        return False, last_output or f"yt-dlp exited with code {return_code}"

    chat_file = vod_chat_sidecar_for_output_template(output_template)
    if not chat_file.is_file():
        return False, last_output or "yt-dlp did not create a live chat replay sidecar"
    record_stream_event(
        config,
        stream.video_id,
        f"YouTube VOD live chat replay downloaded: {chat_file.name}",
        segment_index=1,
    )
    return True, f"Live chat replay downloaded: {chat_file.name}"


def run_kick_vod_chat_download_job(
    config: BotConfig,
    job_id: str,
    stream: LiveStream,
    output_template: Path,
) -> tuple[bool, str]:
    update_tracked_job(
        job_id,
        phase="Downloading Kick chat replay",
        message="Trying Kick chat replay",
        progress=0.99,
    )
    result = download_kick_vod_chat_replay(
        stream,
        output_template,
        progress=lambda phase, value: update_tracked_job(
            job_id,
            phase=phase,
            message=phase,
            progress=0.99 if value is None else min(0.99, max(0.02, value)),
        ),
    )
    if not result.ok:
        return False, result.message
    if result.chat_file is not None:
        record_stream_event(
            config,
            stream.video_id,
            f"Kick VOD chat replay downloaded: {result.chat_file.name}",
            segment_index=1,
        )
    return True, result.message


def finish_failed_vod_download(
    config: BotConfig,
    job_id: str,
    video_id: str,
    message: str,
    previous_status: str | None,
    *,
    exit_code: int | None = None,
) -> None:
    finish_tracked_job(
        job_id,
        status="failed",
        phase="Failed",
        message=message,
        progress=None,
    )
    restore_status = previous_status if previous_status in {"ended", "interrupted"} else None
    state = StateStore(config.db_path)
    try:
        state.mark_vod_download_failed(
            video_id,
            f"VOD download failed: {message}",
            restore_status=restore_status,
            exit_code=exit_code,
        )
    finally:
        state.close()


def build_vod_download_command(
    config: BotConfig,
    vod_url: str,
    output_template: Path,
) -> list[str]:
    return [
        config.yt_dlp_path,
        *config.extra_yt_dlp_args,
        "--continue",
        "--part",
        "--progress",
        "--newline",
        "--progress-delta",
        "5",
        "--no-playlist",
        "-o",
        str(output_template),
        vod_url,
    ]


def build_vod_chat_download_command(
    config: BotConfig,
    vod_url: str,
    output_template: Path,
) -> list[str]:
    return [
        config.yt_dlp_path,
        *config.extra_yt_dlp_args,
        "--skip-download",
        "--write-subs",
        "--sub-langs",
        "live_chat",
        "--continue",
        "--part",
        "--progress",
        "--newline",
        "--progress-delta",
        "5",
        "--no-playlist",
        "-o",
        str(output_template),
        vod_url,
    ]


def vod_chat_sidecar_for_output_template(output_template: Path) -> Path:
    template = str(output_template)
    if "%(ext)s" in template:
        return Path(template.replace("%(ext)s", "live_chat.json"))
    return output_template.with_suffix(".live_chat.json")


def output_template_for_media_file(media_file: Path) -> Path:
    return media_file.with_name(f"{media_file.stem}.%(ext)s")


def vod_download_progress_from_line(line: str) -> float | None:
    match = VOD_DOWNLOAD_PROGRESS_RE.search(line)
    if not match:
        return None
    try:
        return float(match.group("percent")) / 100.0
    except ValueError:
        return None


def vod_output_template_for(
    config: BotConfig,
    stream: LiveStream,
    *,
    force_copy: bool = False,
) -> Path:
    directory = segment_directory(config, stream.video_id, stream.channel)
    stem = named_segment_file_stem(stream.title or stream.video_id, stream.video_id, 1)
    if force_copy or output_stem_exists(directory, stem):
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        stem = f"{stem} - vod-{stamp}"
    return directory / f"{stem}.%(ext)s"


def output_stem_exists(directory: Path, stem: str) -> bool:
    if not directory.is_dir():
        return False
    try:
        return any(path.is_file() and path.stem == stem for path in directory.iterdir())
    except OSError:
        return False


def vod_download_job_id(video_id: str, vod_url: str) -> str:
    digest = hashlib.sha1(vod_url.encode("utf-8", "replace")).hexdigest()[:16]
    return f"vod-download:{video_id}:{digest}"


def tracked_job_is_running(job_id: str) -> bool:
    return any(
        job.job_id == job_id and job.status in {"queued", "running"}
        for job in list_tracked_jobs(limit=1000)
    )


def stream_from_record(
    record: StreamRecord,
    *,
    url: str | None = None,
    is_live: bool = False,
) -> LiveStream:
    return LiveStream(
        video_id=record.video_id,
        url=url or record.url,
        title=record.title,
        channel=record.channel,
        live_status="",
        is_live=is_live,
        platform=record.platform,
        source=record.source,
    )


def safe_stream_directory_for_delete(config: BotConfig, directory: Path) -> Path:
    if directory.is_symlink():
        raise ConfigError("Refusing to delete a symlinked stream directory")
    if not directory.is_dir():
        raise ConfigError("Stream path is not a directory")
    try:
        root_path = config.download_dir.resolve(strict=True)
        directory_path = directory.resolve(strict=True)
    except OSError as exc:
        raise ConfigError(f"Unable to resolve stream directory: {exc}") from exc
    try:
        directory_path.relative_to(root_path)
    except ValueError as exc:
        raise ConfigError("Refusing to delete a stream directory outside download_dir") from exc
    return directory_path


def directory_file_totals(directory: Path) -> tuple[int, int]:
    file_count = 0
    byte_count = 0
    for path in directory.rglob("*"):
        if path.is_symlink() or not path.is_file():
            continue
        file_count += 1
        try:
            byte_count += path.stat().st_size
        except OSError:
            continue
    return file_count, byte_count


def resolve_watermark_download_file(
    config: BotConfig,
    copy_id: str,
) -> Path | None:
    if not copy_id:
        return None
    state = StateStore(config.db_path)
    try:
        copy = state.get_watermark_copy(copy_id)
        if copy is None or copy.status != WATERMARK_STATUS_DONE:
            return None
        record = state.get_stream(copy.video_id)
    finally:
        state.close()
    if record is None:
        return None

    directory = segment_directory(config, record.video_id, record.channel)
    candidate = resolve_watermark_output_file(directory, copy.output_name)
    if candidate is None:
        return None
    try:
        directory_path = directory.resolve(strict=True)
        candidate_path = candidate.resolve(strict=True)
    except OSError:
        return None
    if directory_path not in candidate_path.parents:
        return None
    if not candidate_path.is_file():
        return None
    return candidate_path


def resolve_watermark_source_file(
    config: BotConfig,
    video_id: str,
    filename: str,
) -> tuple[StreamRecord, Path] | None:
    if not video_id or not filename:
        return None
    if Path(filename).name != filename or "/" in filename or "\\" in filename:
        return None
    if not is_watermarkable_media_file(filename):
        return None

    state = StateStore(config.db_path)
    try:
        record = state.get_stream(video_id)
    finally:
        state.close()
    if record is None:
        return None

    directory = segment_directory(config, record.video_id, record.channel)
    candidate = directory / filename
    try:
        directory_path = directory.resolve(strict=True)
        candidate_path = candidate.resolve(strict=True)
    except OSError:
        return None
    if candidate_path.parent != directory_path:
        return None
    if not candidate_path.is_file() or not is_watermarkable_media_file(candidate_path.name):
        return None
    return record, candidate_path


def resolve_transcription_source_file(
    config: BotConfig,
    video_id: str,
    filename: str,
) -> tuple[StreamRecord, Path] | None:
    if not video_id or not filename:
        return None
    if Path(filename).name != filename or "/" in filename or "\\" in filename:
        return None
    if not is_transcribable_media_file(filename):
        return None

    state = StateStore(config.db_path)
    try:
        record = state.get_stream(video_id)
    finally:
        state.close()
    if record is None:
        return None

    directory = segment_directory(config, record.video_id, record.channel)
    candidate = directory / filename
    try:
        directory_path = directory.resolve(strict=True)
        candidate_path = candidate.resolve(strict=True)
    except OSError:
        return None
    if candidate_path.parent != directory_path:
        return None
    if not candidate_path.is_file() or not is_transcribable_media_file(candidate_path.name):
        return None
    return record, candidate_path


def resolve_render_chat_files(
    config: BotConfig,
    video_id: str,
    chat_filename: str,
) -> tuple[Path, Path, Path] | None:
    resolved = resolve_chat_sidecar_files(config, video_id, chat_filename)
    if resolved is None:
        return None
    _record, media_file, chat_path = resolved
    output_file = chat_video_output_file(media_file)
    return media_file, chat_path, output_file


def resolve_refresh_chat_files(
    config: BotConfig,
    video_id: str,
    chat_filename: str,
) -> tuple[StreamRecord, Path, Path] | None:
    resolved = resolve_chat_sidecar_files(
        config,
        video_id,
        chat_filename,
        strict_media_match=True,
    )
    if resolved is None:
        return None
    record, media_file, chat_path = resolved
    if record.status != "ended":
        return None
    return record, media_file, chat_path


def resolve_kick_chat_replay_files(
    config: BotConfig,
    video_id: str,
    chat_filename: str,
) -> tuple[StreamRecord, Path, Path] | None:
    if not video_id or not chat_filename:
        return None
    if Path(chat_filename).name != chat_filename or "/" in chat_filename or "\\" in chat_filename:
        return None
    if not is_live_chat_file(chat_filename):
        return None

    state = StateStore(config.db_path)
    try:
        record = state.get_stream(video_id)
    finally:
        state.close()
    if record is None or record.platform != "kick" or record.status != "ended":
        return None

    directory = segment_directory(config, record.video_id, record.channel)
    chat_file = directory / chat_filename
    try:
        directory_path = directory.resolve(strict=True)
    except OSError:
        return None
    try:
        chat_path = chat_file.resolve(strict=False)
    except OSError:
        return None
    if chat_path.parent != directory_path:
        return None

    media_file = chat_media_file_for_missing_chat_file(directory_path, chat_filename)
    if media_file is None:
        return None
    return record, media_file, chat_path


def resolve_chat_sidecar_files(
    config: BotConfig,
    video_id: str,
    chat_filename: str,
    *,
    strict_media_match: bool = False,
) -> tuple[StreamRecord, Path, Path] | None:
    if not video_id or not chat_filename:
        return None
    if Path(chat_filename).name != chat_filename or "/" in chat_filename or "\\" in chat_filename:
        return None
    if not is_live_chat_file(chat_filename):
        return None

    state = StateStore(config.db_path)
    try:
        record = state.get_stream(video_id)
    finally:
        state.close()
    if record is None:
        return None

    directory = segment_directory(config, record.video_id, record.channel)
    chat_file = directory / chat_filename
    try:
        directory_path = directory.resolve(strict=True)
        chat_path = chat_file.resolve(strict=True)
    except OSError:
        return None

    if chat_path.parent != directory_path or not chat_path.is_file():
        return None

    media_file = chat_media_file_for_chat_file(
        directory_path,
        chat_path.name,
        allow_single_media_fallback=not strict_media_match,
    )
    if media_file is None:
        return None
    return record, media_file, chat_path


def chat_media_file_for_missing_chat_file(
    directory: Path,
    chat_filename: str,
) -> Path | None:
    if not is_live_chat_file(chat_filename):
        return None
    stem = chat_filename.removesuffix(LIVE_CHAT_SUFFIX)
    for suffix in CHAT_RENDER_MEDIA_SUFFIXES:
        candidate = directory / f"{stem}{suffix}"
        if candidate.is_file() and is_renderable_media_file(candidate.name):
            return candidate
    return None


def chat_media_file_for_chat_file(
    directory: Path,
    chat_filename: str,
    *,
    allow_single_media_fallback: bool = True,
) -> Path | None:
    if not is_live_chat_file(chat_filename):
        return None
    if not (directory / chat_filename).is_file():
        return None
    stem = chat_filename.removesuffix(LIVE_CHAT_SUFFIX)
    for suffix in CHAT_RENDER_MEDIA_SUFFIXES:
        candidate = directory / f"{stem}{suffix}"
        if candidate.is_file() and is_renderable_media_file(candidate.name):
            return candidate
    if allow_single_media_fallback:
        candidates = sorted(
            path
            for path in directory.iterdir()
            if path.is_file() and is_renderable_media_file(path.name)
        )
        if len(candidates) == 1:
            return candidates[0]
    return None


def is_renderable_media_file(name: str) -> bool:
    if is_live_chat_file(name):
        return False
    if name.endswith(CHAT_RENDER_OUTPUT_SUFFIX):
        return False
    if is_rendering_temporary_file(name):
        return False
    if Path(name).suffix.lower() not in CHAT_RENDER_MEDIA_SUFFIXES:
        return False
    return is_downloadable_file(name)


def is_rendering_temporary_file(name: str) -> bool:
    path = Path(name)
    return path.suffix.lower() in CHAT_RENDER_MEDIA_SUFFIXES and path.stem.endswith(
        ".rendering"
    )


def is_watermarkable_media_file(name: str) -> bool:
    if is_live_chat_file(name):
        return False
    if is_rendering_temporary_file(name):
        return False
    if Path(name).suffix.lower() not in {".mp4", ".mkv", ".mov", ".webm"}:
        return False
    return is_downloadable_file(name)


def is_transcribable_media_file(name: str) -> bool:
    if not is_renderable_media_file(name):
        return False
    return Path(name).suffix.lower() in {".mp4", ".mkv", ".mov", ".webm"}


def chat_render_action_for_file(
    directory: Path,
    video_id: str,
    filename: str,
    *,
    allow_single_media_fallback: bool = True,
) -> tuple[str | None, str | None, str | None, str | None]:
    if not is_live_chat_file(filename):
        return None, None, None, None

    media_file = chat_media_file_for_chat_file(
        directory,
        filename,
        allow_single_media_fallback=allow_single_media_fallback,
    )
    if media_file is None:
        return None, None, None, None

    output_file = chat_video_output_file(media_file)
    if output_file.exists():
        return (
            render_chat_url_for(video_id, filename, regenerate=True),
            None,
            "rendered",
            None,
        )

    job = chat_render_job_for(video_id, filename)
    if job is not None and job.status == "running":
        return None, None, "rendering", job.message
    if job is not None and job.status == "failed":
        return render_chat_url_for(video_id, filename), None, "failed", job.message
    return render_chat_url_for(video_id, filename), None, "ready", None


def chat_refresh_action_for_file(
    config: BotConfig,
    directory: Path,
    video_id: str,
    filename: str,
    *,
    allow_single_media_fallback: bool = True,
) -> tuple[str | None, str | None, str | None]:
    if not is_live_chat_file(filename):
        return None, None, None
    if chat_media_file_for_chat_file(
        directory,
        filename,
        allow_single_media_fallback=allow_single_media_fallback,
    ) is None:
        return None, None, None
    if resolve_refresh_chat_files(config, video_id, filename) is None:
        return None, None, None

    job = refresh_chat_job_for(video_id, filename)
    if job is not None and job.status == "running":
        return None, "running", job.message
    if job is not None and job.status == "failed":
        return refresh_chat_url_for(video_id, filename), "failed", job.message
    return refresh_chat_url_for(video_id, filename), "ready", None


def kick_chat_download_action_for_file(
    directory: Path,
    video_id: str,
    filename: str,
) -> tuple[str | None, str | None, str | None]:
    if not is_renderable_media_file(filename):
        return None, None, None
    media_file = directory / filename
    if not media_file.is_file():
        return None, None, None
    chat_filename = f"{media_file.stem}{LIVE_CHAT_SUFFIX}"
    if (directory / chat_filename).is_file():
        return None, None, None

    job = refresh_chat_job_for(video_id, chat_filename)
    if job is not None and job.status == "running":
        return None, "running", job.message
    if job is not None and job.status == "failed":
        return refresh_chat_url_for(video_id, chat_filename), "failed", job.message
    return refresh_chat_url_for(video_id, chat_filename), "download", None


def transcription_action_for_file(
    directory: Path,
    video_id: str,
    filename: str,
) -> tuple[str | None, str | None, str | None]:
    if not is_transcribable_media_file(filename):
        return None, None, None

    media_file = directory / filename
    if not media_file.is_file():
        return None, None, None

    job = transcription_job_for(video_id, filename)
    if job is not None and job.status == "running":
        return None, "running", job.message

    has_outputs = transcription_outputs_exist(media_file)
    if job is not None and job.status == "failed":
        return (
            transcription_url_for(video_id, filename, regenerate=has_outputs),
            "failed",
            job.message,
        )
    if has_outputs:
        return (
            transcription_url_for(video_id, filename, regenerate=True),
            "transcribed",
            None,
        )
    return transcription_url_for(video_id, filename), "ready", None


def event_detection_action_for_file(
    config: BotConfig,
    directory: Path,
    video_id: str,
    filename: str,
) -> tuple[str | None, str | None, str | None]:
    if not is_transcribable_media_file(filename):
        return None, None, None
    if not config.stream_event_detection_enabled:
        return None, None, None

    media_file = directory / filename
    if not media_file.is_file():
        return None, None, None

    job = event_detection_job_for(video_id, filename)
    if job is not None and job.status == "running":
        return None, "running", job.message

    has_outputs = content_events_exist(media_file)
    if job is not None and job.status == "failed":
        return (
            event_detection_url_for(video_id, filename, regenerate=has_outputs),
            "failed",
            job.message,
        )
    if has_outputs:
        return (
            event_detection_url_for(video_id, filename, regenerate=True),
            "detected",
            None,
        )
    return event_detection_url_for(video_id, filename), "ready", None


def record_stream_event(
    config: BotConfig,
    video_id: str,
    message: str,
    *,
    level: str = "info",
    segment_index: int | None = None,
) -> None:
    state: StateStore | None = None
    try:
        state = StateStore(config.db_path)
        state.add_stream_event(
            video_id,
            message,
            level=level,
            segment_index=segment_index,
        )
    except Exception as exc:  # noqa: BLE001 - stream logging must not break jobs.
        LOGGER.warning(
            "Unable to record stream event for %s: %s",
            video_id,
            str(exc) or exc.__class__.__name__,
        )
    finally:
        if state is not None:
            state.close()


def video_id_from_job_key(key: str) -> str:
    return key.partition("\0")[0]


def chat_render_timeout_seconds(config: BotConfig) -> float | None:
    if config.chat_render_timeout_seconds <= 0:
        return None
    return float(config.chat_render_timeout_seconds)


def start_render_chat_job(
    config: BotConfig,
    video_id: str,
    chat_filename: str,
    *,
    regenerate: bool = False,
) -> tuple[bool, str]:
    resolved = resolve_render_chat_files(config, video_id, chat_filename)
    if resolved is None:
        return False, "No matching finalized video and live chat file found"

    media_file, chat_file, output_file = resolved
    if output_file.exists() and not regenerate:
        return True, "Chat video already exists"

    key = chat_render_job_key(video_id, chat_file.name)
    now = time.time()
    with CHAT_RENDER_JOBS_LOCK:
        existing = CHAT_RENDER_JOBS.get(key)
        if existing is not None and existing.status == "running":
            return True, "Chat render is already running"
        CHAT_RENDER_JOBS[key] = RenderChatJob(
            video_id=video_id,
            chat_name=chat_file.name,
            media_name=media_file.name,
            output_name=output_file.name,
            status="running",
            message="Regenerating chat video" if regenerate else "Rendering chat video",
            started_at=now,
            phase="Queued",
            progress=0.0,
            updated_at=now,
            details=chat_render_job_details(
                media_file,
                chat_file,
                output_file,
                elapsed_seconds=0.0,
                diagnostic="Queued",
            ),
        )

    record_stream_event(
        config,
        video_id,
        ("Queued chat video regeneration" if regenerate else "Queued chat video render")
        + f" for {chat_file.name}",
    )
    thread = Thread(
        target=run_render_chat_job,
        args=(config, key, media_file, chat_file, output_file, regenerate),
        name=f"onlysavemevods-chat-render-{video_id}",
        daemon=True,
    )
    thread.start()
    LOGGER.info(
        "Queued manual chat %s for %s using media=%s chat=%s",
        "regeneration" if regenerate else "render",
        video_id,
        media_file.name,
        chat_file.name,
    )
    return True, "Chat regeneration queued" if regenerate else "Chat render queued"


def start_refresh_chat_job(
    config: BotConfig,
    video_id: str,
    chat_filename: str,
) -> tuple[bool, str]:
    resolved = resolve_refresh_chat_files(config, video_id, chat_filename)
    if resolved is None:
        resolved = resolve_kick_chat_replay_files(config, video_id, chat_filename)
    if resolved is None:
        return False, "No matching finalized video and live chat file found"

    record, media_file, chat_file = resolved
    key = refresh_chat_job_key(video_id, chat_file.name)
    now = time.time()
    with CHAT_REFRESH_JOBS_LOCK:
        existing = CHAT_REFRESH_JOBS.get(key)
        if existing is not None and existing.status == "running":
            return True, "Chat refresh is already running"
        CHAT_REFRESH_JOBS[key] = RefreshChatJob(
            video_id=video_id,
            chat_name=chat_file.name,
            media_name=media_file.name,
            status="running",
            message=(
                "Downloading Kick chat replay"
                if record.platform == "kick" and not chat_file.exists()
                else "Refreshing chat"
            ),
            started_at=now,
            phase="Queued",
            progress=0.0,
            updated_at=now,
        )

    record_stream_event(
        config,
        video_id,
        f"Queued chat refresh for {chat_file.name}",
        segment_index=record.segment_index,
    )
    thread = Thread(
        target=run_refresh_chat_job,
        args=(config, key, record, media_file, chat_file),
        name=f"onlysavemevods-chat-refresh-{video_id}",
        daemon=True,
    )
    thread.start()
    LOGGER.info(
        "Queued manual chat refresh for %s using media=%s chat=%s",
        video_id,
        media_file.name,
        chat_file.name,
    )
    return True, "Chat refresh queued"


def run_refresh_chat_job(
    config: BotConfig,
    key: str,
    record: StreamRecord,
    media_file: Path,
    chat_file: Path,
) -> None:
    update_refresh_chat_job(
        key,
        phase="Downloading Kick chat replay" if record.platform == "kick" else "Refreshing chat replay",
        progress=0.2,
        message="Downloading Kick chat replay" if record.platform == "kick" else "Refreshing chat replay",
        updated_at=time.time(),
    )
    try:
        if record.platform == "kick":
            kick_result = download_kick_vod_chat_replay(
                stream_from_record(record, url=record.url, is_live=False),
                output_template_for_media_file(media_file),
                progress=lambda phase, value: update_refresh_chat_job(
                    key,
                    phase=phase,
                    progress=value,
                    message=phase,
                    updated_at=time.time(),
                ),
            )
            result = ChatRefreshResult(
                ok=kick_result.ok,
                changed=kick_result.ok,
                source="kick-replay",
                message=kick_result.message,
            )
        else:
            result = refresh_chat_sidecar(
                config,
                video_url=record.url,
                media_file=media_file,
                chat_file=chat_file,
                last_exit_at=record.last_exit_at,
                logger=LOGGER,
            )
    except Exception as exc:  # noqa: BLE001 - web job should capture refresh failures.
        LOGGER.exception(
            "Manual chat refresh failed for media=%s chat=%s",
            media_file,
            chat_file,
        )
        finished = time.time()
        message = str(exc) or exc.__class__.__name__
        update_refresh_chat_job(
            key,
            status="failed",
            message=message,
            phase="Failed",
            finished_at=finished,
            updated_at=finished,
        )
        record_stream_event(
            config,
            record.video_id,
            f"Chat refresh failed for {chat_file.name}: {message}",
            level="error",
            segment_index=record.segment_index,
        )
        return

    if not result.ok:
        finished = time.time()
        update_refresh_chat_job(
            key,
            status="failed",
            message=result.message,
            phase="Failed",
            finished_at=finished,
            updated_at=finished,
        )
        record_stream_event(
            config,
            record.video_id,
            f"Chat refresh failed for {chat_file.name}: {result.message}",
            level="error",
            segment_index=record.segment_index,
        )
        return

    LOGGER.info(
        "Manual chat refresh completed chat=%s source=%s message=%s",
        chat_file,
        result.source,
        result.message,
    )
    finished = time.time()
    update_refresh_chat_job(
        key,
        status="done",
        message=result.message,
        phase="Complete",
        progress=1.0,
        finished_at=finished,
        updated_at=finished,
    )
    record_stream_event(
        config,
        record.video_id,
        f"Chat refresh completed for {chat_file.name}: {result.message}",
        segment_index=record.segment_index,
    )


def run_render_chat_job(
    config: BotConfig,
    key: str,
    media_file: Path,
    chat_file: Path,
    output_file: Path,
    regenerate: bool = False,
) -> None:
    if config.config_path is not None:
        run_render_chat_process_job(
            config,
            key,
            media_file,
            chat_file,
            output_file,
            regenerate,
        )
        return

    run_render_chat_in_process_job(
        config,
        key,
        media_file,
        chat_file,
        output_file,
        regenerate,
    )


def run_render_chat_process_job(
    config: BotConfig,
    key: str,
    media_file: Path,
    chat_file: Path,
    output_file: Path,
    regenerate: bool = False,
) -> None:
    assert config.config_path is not None
    progress_file = create_isolated_render_progress_file(output_file)
    command = build_render_chat_file_process_command(
        sys.executable,
        config.config_path,
        media_file,
        chat_file,
        output_file,
        overwrite=regenerate,
        progress_file=progress_file,
    )
    LOGGER.info(
        "Starting isolated manual chat render process media=%s chat=%s output=%s",
        media_file,
        chat_file,
        output_file,
    )
    LOGGER.debug("isolated manual chat render command: %s", command_for_log(command))
    started_at = time.time()
    update_render_chat_job(
        key,
        phase="Starting isolated renderer",
        progress=0.05,
        message="Starting isolated renderer",
        updated_at=started_at,
        details=chat_render_job_details(
            media_file,
            chat_file,
            output_file,
            elapsed_seconds=0.0,
            diagnostic="Starting isolated renderer",
        ),
    )
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except OSError as exc:
        LOGGER.exception("Unable to start isolated manual chat render process")
        message = str(exc) or exc.__class__.__name__
        update_render_chat_job(
            key,
            status="failed",
            message=message,
            phase="Failed",
            finished_at=time.time(),
            updated_at=time.time(),
            details=chat_render_job_details(
                media_file,
                chat_file,
                output_file,
                elapsed_seconds=max(0.0, time.time() - started_at),
                diagnostic=message,
            ),
        )
        cleanup_isolated_render_progress_file(progress_file)
        record_stream_event(
            config,
            video_id_from_job_key(key),
            f"Chat video render failed for {chat_file.name}: {message}",
            level="error",
        )
        return

    stdout = b""
    stderr = b""
    while True:
        try:
            stdout, stderr = process.communicate(
                timeout=CHAT_RENDER_PROGRESS_POLL_SECONDS,
            )
            break
        except subprocess.TimeoutExpired:
            update_isolated_render_chat_progress(
                key,
                output_file,
                started_at,
                progress_file=progress_file,
                media_file=media_file,
                chat_file=chat_file,
            )

    returncode = process.returncode if process.returncode is not None else 0
    log_process_output(
        LOGGER,
        "isolated manual chat render",
        stdout or b"",
        stderr or b"",
        failed=returncode != 0,
    )
    if returncode != 0:
        message = process_failure_message(stdout or b"", stderr or b"")
        failure_message = message or f"Chat render exited with code {returncode}"
        update_render_chat_job(
            key,
            status="failed",
            message=failure_message,
            phase="Failed",
            finished_at=time.time(),
            updated_at=time.time(),
            details=chat_render_job_details(
                media_file,
                chat_file,
                output_file,
                elapsed_seconds=max(0.0, time.time() - started_at),
                progress_payload=read_isolated_render_progress(progress_file),
                diagnostic=failure_message,
            ),
        )
        cleanup_isolated_render_progress_file(progress_file)
        record_stream_event(
            config,
            video_id_from_job_key(key),
            f"Chat video render failed for {chat_file.name}: {failure_message}",
            level="error",
        )
        return

    LOGGER.info("Manual chat render completed: %s", output_file)
    update_render_chat_job(
        key,
        status="done",
        message="Rendered chat video",
        phase="Complete",
        progress=1.0,
        finished_at=time.time(),
        updated_at=time.time(),
        details=chat_render_job_details(
            media_file,
            chat_file,
            output_file,
            elapsed_seconds=max(0.0, time.time() - started_at),
            progress_payload=read_isolated_render_progress(progress_file),
            diagnostic="Rendered chat video",
        ),
    )
    cleanup_isolated_render_progress_file(progress_file)
    record_stream_event(
        config,
        video_id_from_job_key(key),
        f"Chat video rendered: {output_file.name}",
    )


def update_isolated_render_chat_progress(
    key: str,
    output_file: Path,
    started_at: float,
    *,
    progress_file: Path | None = None,
    media_file: Path | None = None,
    chat_file: Path | None = None,
) -> None:
    now = time.time()
    elapsed_seconds = max(0.0, now - started_at)
    payload = read_isolated_render_progress(progress_file)
    phase = isolated_render_chat_progress_phase(output_file, elapsed_seconds)
    progress = None
    if payload is not None:
        payload_phase = str(payload.get("phase") or "").strip()
        if payload_phase:
            phase = payload_phase
        progress = optional_float(payload.get("progress"))
    update_render_chat_job(
        key,
        phase=phase,
        progress=progress,
        message=phase,
        updated_at=now,
        details=chat_render_job_details(
            media_file,
            chat_file,
            output_file,
            elapsed_seconds=elapsed_seconds,
            progress_payload=payload,
            diagnostic=phase,
        ),
    )


def isolated_render_chat_progress_phase(output_file: Path, elapsed_seconds: float) -> str:
    output = current_isolated_render_chat_output(output_file)
    elapsed = format_duration(max(0, int(elapsed_seconds)))
    if output is None:
        return f"Rendering in isolated process; elapsed {elapsed}"
    label, path, size_bytes = output
    return (
        f"Rendering in isolated process; {label} {format_bytes(size_bytes)} "
        f"written to {path.name}; elapsed {elapsed}"
    )


def create_isolated_render_progress_file(output_file: Path) -> Path | None:
    try:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=output_file.parent,
            prefix=f".{output_file.stem}.progress.",
            suffix=".json",
            delete=False,
        ) as handle:
            return Path(handle.name)
    except OSError as exc:
        LOGGER.warning(
            "Unable to create isolated chat render progress file output=%s error=%s",
            output_file,
            exc,
        )
        return None


def cleanup_isolated_render_progress_file(progress_file: Path | None) -> None:
    if progress_file is None:
        return
    try:
        progress_file.unlink(missing_ok=True)
    except OSError:
        pass


def read_isolated_render_progress(progress_file: Path | None) -> dict[str, Any] | None:
    if progress_file is None:
        return None
    try:
        payload = json.loads(progress_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def chat_render_job_details(
    media_file: Path | None,
    chat_file: Path | None,
    output_file: Path,
    *,
    elapsed_seconds: float,
    progress_payload: dict[str, Any] | None = None,
    diagnostic: str = "",
) -> dict[str, Any]:
    details: dict[str, Any] = {
        "elapsed_seconds": max(0.0, elapsed_seconds),
        "elapsed": format_duration(max(0, int(elapsed_seconds))),
        "output_name": output_file.name,
    }
    if media_file is not None:
        details["media_name"] = media_file.name
    if chat_file is not None:
        details["chat_name"] = chat_file.name
    if diagnostic:
        details["diagnostic"] = diagnostic

    if progress_payload is not None:
        for key in ("phase", "progress", "updated_at", "media_name", "chat_name", "output_name"):
            value = progress_payload.get(key)
            if value not in (None, ""):
                details[key] = value

    current = current_render_chat_output_detail(output_file, progress_payload)
    if current is not None:
        label, name, size_bytes = current
        details["current_label"] = label
        details["current_name"] = name
        details["current_size_bytes"] = size_bytes
        details["current_size"] = format_bytes(size_bytes)
    return details


def current_render_chat_output_detail(
    output_file: Path,
    progress_payload: dict[str, Any] | None,
) -> tuple[str, str, int] | None:
    if progress_payload is not None:
        outputs = progress_payload.get("outputs")
        if isinstance(outputs, dict):
            for key, label in (("final", "final video"), ("panel", "chat panel"), ("output", "output")):
                raw = outputs.get(key)
                if not isinstance(raw, dict):
                    continue
                name = str(raw.get("name") or "").strip()
                size_bytes = optional_int(raw.get("size_bytes"))
                if name and size_bytes is not None:
                    return label, name, size_bytes

    current = current_isolated_render_chat_output(output_file)
    if current is None:
        return None
    label, path, size_bytes = current
    return label, path.name, size_bytes


def optional_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def optional_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def current_isolated_render_chat_output(
    output_file: Path,
) -> tuple[str, Path, int] | None:
    candidates = [
        (
            "final video",
            output_file.with_name(f"{output_file.stem}.rendering{output_file.suffix}"),
        ),
        (
            "chat panel",
            output_file.with_name(f"{output_file.stem}.panel{output_file.suffix}"),
        ),
    ]
    for label, path in candidates:
        try:
            if path.is_file():
                return label, path, path.stat().st_size
        except OSError:
            continue
    return None


def process_failure_message(stdout: bytes, stderr: bytes) -> str:
    output = (stderr or stdout).decode("utf-8", "replace").strip()
    if not output:
        return ""
    return output.splitlines()[-1][-500:]


def run_render_chat_in_process_job(
    config: BotConfig,
    key: str,
    media_file: Path,
    chat_file: Path,
    output_file: Path,
    regenerate: bool = False,
) -> None:
    nvenc_device = choose_chat_render_nvenc_device(
        config.chat_render_nvenc_devices,
        output_file,
    )
    if config.chat_render_use_nvenc:
        LOGGER.info(
            "Selected NVENC device for manual chat render media=%s device=%s",
            media_file,
            nvenc_device or "default",
        )
    started_at = time.time()

    def report_progress(phase: str, value: float | None) -> None:
        now = time.time()
        update_render_chat_job(
            key,
            phase=phase,
            progress=value,
            message=phase,
            updated_at=now,
            details=chat_render_job_details(
                media_file,
                chat_file,
                output_file,
                elapsed_seconds=max(0.0, now - started_at),
                diagnostic=phase,
            ),
        )

    try:
        render_chat_video_file(
            media_file,
            chat_file,
            ffmpeg_path=config.ffmpeg_path,
            output_file=output_file,
            overwrite=regenerate,
            panel_workers=config.chat_render_panel_workers,
            timeout_seconds=chat_render_timeout_seconds(config),
            use_nvenc=config.chat_render_use_nvenc,
            nvenc_device=nvenc_device,
            progress_callback=report_progress,
        )
    except Exception as exc:  # noqa: BLE001 - web job should capture renderer failures.
        LOGGER.exception(
            "Manual chat render failed for media=%s chat=%s",
            media_file,
            chat_file,
        )
        message = str(exc) or exc.__class__.__name__
        update_render_chat_job(
            key,
            status="failed",
            message=message,
            phase="Failed",
            finished_at=time.time(),
            updated_at=time.time(),
            details=chat_render_job_details(
                media_file,
                chat_file,
                output_file,
                elapsed_seconds=max(0.0, time.time() - started_at),
                diagnostic=message,
            ),
        )
        record_stream_event(
            config,
            video_id_from_job_key(key),
            f"Chat video render failed for {chat_file.name}: {message}",
            level="error",
        )
        return

    LOGGER.info("Manual chat render completed: %s", output_file)
    update_render_chat_job(
        key,
        status="done",
        message="Rendered chat video",
        phase="Complete",
        progress=1.0,
        finished_at=time.time(),
        updated_at=time.time(),
        details=chat_render_job_details(
            media_file,
            chat_file,
            output_file,
            elapsed_seconds=max(0.0, time.time() - started_at),
            diagnostic="Rendered chat video",
        ),
    )
    record_stream_event(
        config,
        video_id_from_job_key(key),
        f"Chat video rendered: {output_file.name}",
    )


def chat_render_job_key(video_id: str, chat_filename: str) -> str:
    return f"{video_id}\0{chat_filename}"


def chat_render_job_for(video_id: str, chat_filename: str) -> RenderChatJob | None:
    with CHAT_RENDER_JOBS_LOCK:
        return CHAT_RENDER_JOBS.get(chat_render_job_key(video_id, chat_filename))


def refresh_chat_job_key(video_id: str, chat_filename: str) -> str:
    return f"{video_id}\0{chat_filename}"


def refresh_chat_job_for(video_id: str, chat_filename: str) -> RefreshChatJob | None:
    with CHAT_REFRESH_JOBS_LOCK:
        return CHAT_REFRESH_JOBS.get(refresh_chat_job_key(video_id, chat_filename))


def update_render_chat_job(key: str, **changes: Any) -> None:
    with CHAT_RENDER_JOBS_LOCK:
        job = CHAT_RENDER_JOBS.get(key)
        if job is None:
            return
        CHAT_RENDER_JOBS[key] = RenderChatJob(
            video_id=changes.get("video_id", job.video_id),
            chat_name=changes.get("chat_name", job.chat_name),
            media_name=changes.get("media_name", job.media_name),
            output_name=changes.get("output_name", job.output_name),
            status=changes.get("status", job.status),
            message=changes.get("message", job.message),
            started_at=changes.get("started_at", job.started_at),
            finished_at=changes.get("finished_at", job.finished_at),
            phase=changes.get("phase", job.phase),
            progress=changes.get("progress", job.progress),
            updated_at=changes.get("updated_at", job.updated_at),
            details=changes.get("details", job.details),
        )


def update_refresh_chat_job(key: str, **changes: Any) -> None:
    with CHAT_REFRESH_JOBS_LOCK:
        job = CHAT_REFRESH_JOBS.get(key)
        if job is None:
            return
        CHAT_REFRESH_JOBS[key] = RefreshChatJob(
            video_id=changes.get("video_id", job.video_id),
            chat_name=changes.get("chat_name", job.chat_name),
            media_name=changes.get("media_name", job.media_name),
            status=changes.get("status", job.status),
            message=changes.get("message", job.message),
            started_at=changes.get("started_at", job.started_at),
            finished_at=changes.get("finished_at", job.finished_at),
            phase=changes.get("phase", job.phase),
            progress=changes.get("progress", job.progress),
            updated_at=changes.get("updated_at", job.updated_at),
        )


def update_app_config_from_form(
    config: BotConfig,
    params: dict[str, list[str]],
) -> None:
    if config.config_path is None:
        raise ConfigError("Config path is not available")
    updates = app_config_updates_from_form(params)
    if not updates:
        return

    load_config_update_preview(config.config_path, updates)
    update_config_values(config.config_path, updates)
    reload_running_config(config)


def app_config_updates_from_form(params: dict[str, list[str]]) -> dict[str, object]:
    updates: dict[str, object] = {}
    for field in CONFIG_FORM_FIELDS:
        if field.kind == "extra_args":
            extra_args = extra_yt_dlp_args_update_from_form(params)
            if extra_args is not None:
                updates[field.key] = extra_args
            continue
        updates[field.key] = config_form_value_from_params(field, params)
    return updates


def config_form_value_from_params(
    field: ConfigFormField,
    params: dict[str, list[str]],
) -> object:
    raw = first_query_value(params, field.key)
    if field.kind == "bool":
        return form_bool(raw, field.key)
    if field.kind == "choice":
        value = raw.strip()
        if value not in field.options:
            allowed = ", ".join(field.options)
            raise ConfigError(f"{field.key} must be one of: {allowed}")
        return value
    if field.kind == "int":
        return form_int(raw, field.key, minimum=field.minimum)
    if field.kind == "float":
        return form_float(raw, field.key, minimum=field.minimum)
    if field.kind == "int_list":
        return form_int_list(raw, field.key)
    if field.kind == "str_list":
        return form_string_list(raw, field.key)
    if field.kind == "optional_text":
        return raw.strip()
    if field.kind == "text":
        return raw.strip()
    raise ConfigError(f"Unsupported config form field: {field.key}")


def extra_yt_dlp_args_update_from_form(
    params: dict[str, list[str]],
) -> list[str] | None:
    mode = (first_query_value(params, "extra_yt_dlp_args_mode") or "keep").strip()
    if mode == "keep":
        return None
    if mode == "clear":
        return []
    if mode == "replace":
        return form_string_list(
            first_query_value(params, "extra_yt_dlp_args"),
            "extra_yt_dlp_args",
            split_commas=False,
        )
    raise ConfigError("extra_yt_dlp_args_mode must be keep, replace, or clear")


def form_bool(value: str, name: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ConfigError(f"{name} must be true or false")


def form_float(value: str, name: str, *, minimum: int | None = None) -> float:
    raw = value.strip()
    if not raw:
        raise ConfigError(f"{name} must be a number")
    try:
        parsed = float(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number") from exc
    if minimum is not None and parsed < minimum:
        raise ConfigError(f"{name} must be at least {minimum}")
    return parsed


def form_int(value: str, name: str, *, minimum: int | None = None) -> int:
    raw = value.strip()
    if not raw:
        raise ConfigError(f"{name} must be an integer")
    try:
        parsed = int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer") from exc
    if minimum is not None and parsed < minimum:
        raise ConfigError(f"{name} must be at least {minimum}")
    return parsed


def form_int_list(value: str, name: str) -> list[int]:
    parts = [part for part in re.split(r"[,\s]+", value.strip()) if part]
    result: list[int] = []
    for part in parts:
        try:
            result.append(int(part))
        except ValueError as exc:
            raise ConfigError(f"{name} must contain only integers") from exc
    return result


def form_string_list(
    value: str,
    name: str,
    *,
    split_commas: bool = True,
) -> list[str]:
    if not value.strip():
        return []
    if split_commas and "\n" not in value:
        parts = value.split(",")
    else:
        parts = value.splitlines()
    result = [part.strip() for part in parts if part.strip()]
    if any(not item for item in result):
        raise ConfigError(f"{name} must not contain empty values")
    return result


def load_config_update_preview(
    config_path: Path,
    updates: dict[str, object],
) -> BotConfig:
    target = Path(config_path).expanduser()
    try:
        current_text = target.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ConfigError(f"Config file does not exist: {target}") from exc
    except OSError as exc:
        raise ConfigError(f"Unable to read config file {target}: {exc}") from exc

    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=target.parent,
            prefix=f".{target.name}.",
            suffix=".tmp",
            delete=False,
        ) as temp:
            temp.write(current_text)
            temp_path = Path(temp.name)
        update_config_values(temp_path, updates)
        return load_config(temp_path)
    finally:
        if temp_path is not None:
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass


def reload_running_config(config: BotConfig) -> None:
    if config.config_path is None:
        raise ConfigError("Config path is not available")
    loaded = load_config(config.config_path)
    for name in config.__dataclass_fields__:
        if name == "config_path":
            continue
        setattr(config, name, getattr(loaded, name))

def update_streamer_from_form(
    config: BotConfig,
    params: dict[str, list[str]],
) -> None:
    if config.config_path is None:
        raise ConfigError("Config path is not available")
    form_kind = (first_query_value(params, "form_kind") or "streamer_form").strip()
    if form_kind == "streamer_wizard":
        update_streamer_from_wizard_form(config, params)
        return

    action = (first_query_value(params, "action") or "save").strip().lower()
    streamer_name = first_query_value(params, "streamer_name").strip()
    if action == "delete":
        remove_streamer_config(config.config_path, streamer_name)
        reload_running_config(config)
        return
    if action != "save":
        raise ConfigError("Unknown streamer action")
    sources = form_string_list(
        first_query_value(params, "sources"),
        "sources",
    )
    download_dir_name = first_query_value(params, "download_dir_name").strip()
    powerchat_enabled = query_flag(params, "powerchat_enabled")
    powerchat_username = first_query_value(params, "powerchat_username").strip()
    update_streamer_config(
        config.config_path,
        streamer_name,
        sources,
        download_dir_name,
        powerchat_enabled,
        powerchat_username,
    )
    reload_running_config(config)


def update_streamer_from_wizard_form(
    config: BotConfig,
    params: dict[str, list[str]],
) -> None:
    assert config.config_path is not None
    action = (first_query_value(params, "action") or "save").strip().lower()
    if action != "save":
        raise ConfigError("Unknown streamer action")

    streamer_name = first_query_value(params, "streamer_name").strip()
    if not streamer_name:
        raise ConfigError("streamer name is required")
    sources = form_string_list(
        first_query_value(params, "sources"),
        "sources",
    )
    download_dir_name = first_query_value(params, "download_dir_name").strip()
    mode = (first_query_value(params, "mode") or "inherit").strip().lower()
    if mode not in {"inherit", "off", "auto", "fixed", "range"}:
        raise ConfigError("Unknown voice detection mode")
    voice_config = (
        None
        if mode == "inherit"
        else voice_detection_override_from_form(params, mode)
    )
    labels = speaker_labels_from_form(params)

    update_streamer_config(
        config.config_path,
        streamer_name,
        sources,
        download_dir_name,
    )
    if mode != "inherit":
        update_streamer_voice_detection_config(
            config.config_path,
            streamer_name,
            voice_config,
        )
    if labels:
        update_streamer_speaker_labels_config(
            config.config_path,
            streamer_name,
            labels,
        )
    reload_running_config(config)
    if labels:
        rewritten = rewrite_speaker_subtitles_for_channel(config, streamer_name)
        LOGGER.info(
            "Created streamer from wizard streamer=%s labels=%d subtitles_rewritten=%d",
            streamer_name,
            len(labels),
            rewritten,
        )


def update_voice_detection_from_form(
    config: BotConfig,
    params: dict[str, list[str]],
) -> None:
    if config.config_path is None:
        raise ConfigError("Config path is not available")
    scope = first_query_value(params, "scope") or "global"
    mode = first_query_value(params, "mode") or "auto"
    if scope == "global":
        updates = voice_detection_root_updates(params, mode)
        update_config_values(config.config_path, updates)
        config.whisperx_diarize = bool(updates["whisperx_diarize"])
        config.whisperx_min_speakers = int(updates["whisperx_min_speakers"])
        config.whisperx_max_speakers = int(updates["whisperx_max_speakers"])
        if "whisperx_hf_token_env" in updates:
            config.whisperx_hf_token_env = str(updates["whisperx_hf_token_env"])
        return
    if scope == "channel":
        channel = first_query_value(params, "channel").strip()
        if channel in config.streamers:
            if mode == "inherit":
                update_streamer_voice_detection_config(config.config_path, channel, None)
            else:
                update_streamer_voice_detection_config(
                    config.config_path,
                    channel,
                    voice_detection_override_from_form(params, mode),
                )
            reload_running_config(config)
            return
        if mode == "inherit":
            update_channel_voice_detection_config(config.config_path, channel, None)
            config.channel_voice_detection.pop(channel, None)
            return
        override = voice_detection_override_from_form(params, mode)
        update_channel_voice_detection_config(
            config.config_path,
            channel,
            override,
        )
        config.channel_voice_detection[channel] = override
        return
    raise ConfigError("Unknown voice detection scope")


def update_stream_event_rules_from_form(
    config: BotConfig,
    params: dict[str, list[str]],
) -> None:
    if config.config_path is None:
        raise ConfigError("Config path is not available")
    scope = (first_query_value(params, "scope") or "global").strip().lower()
    rules = stream_event_rules_from_form(params)
    if scope == "global":
        update_global_stream_event_rules_config(config.config_path, rules)
        reload_running_config(config)
        return
    if scope == "streamer":
        streamer_name = first_query_value(params, "streamer_name").strip()
        if streamer_name not in config.streamers:
            raise ConfigError(f"streamer is not configured: {streamer_name}")
        detection = streamer_event_detection_from_form(params)
        update_streamer_stream_event_config(
            config.config_path,
            streamer_name,
            detection,
            rules,
        )
        reload_running_config(config)
        return
    raise ConfigError("Unknown content event settings scope")


def stream_event_rules_from_form(
    params: dict[str, list[str]],
) -> list[StreamEventRuleConfig]:
    names = params.get("rule_name", [])
    count = max(
        len(names),
        len(params.get("rule_enabled", [])),
        len(params.get("rule_labels", [])),
        len(params.get("rule_keywords", [])),
        len(params.get("rule_voice", [])),
        len(params.get("rule_min_loudness_dbfs", [])),
        len(params.get("rule_min_duration_seconds", [])),
        len(params.get("rule_max_duration_seconds", [])),
        len(params.get("rule_severity", [])),
    )
    rules: list[StreamEventRuleConfig] = []
    for index in range(count):
        name = form_list_value(params, "rule_name", index).strip()
        enabled_text = form_list_value(params, "rule_enabled", index) or "true"
        labels_text = form_list_value(params, "rule_labels", index)
        keywords_text = form_list_value(params, "rule_keywords", index)
        voice = form_list_value(params, "rule_voice", index).strip()
        loudness_text = form_list_value(params, "rule_min_loudness_dbfs", index)
        min_duration_text = form_list_value(params, "rule_min_duration_seconds", index)
        max_duration_text = form_list_value(params, "rule_max_duration_seconds", index)
        severity = form_list_value(params, "rule_severity", index).strip() or "info"
        delete_text = first_query_value(params, f"rule_delete_{index}").strip().lower()
        if delete_text in {"1", "true", "yes", "on", "delete"}:
            continue
        if not any(
            item.strip()
            for item in (
                name,
                labels_text,
                keywords_text,
                voice,
                loudness_text,
                min_duration_text,
                max_duration_text,
            )
        ):
            continue
        if not name:
            raise ConfigError("content event rule name is required")
        rules.append(
            StreamEventRuleConfig(
                name=name,
                enabled=form_bool(enabled_text, "rule enabled"),
                labels=form_string_list(labels_text, "rule labels"),
                keywords=form_string_list(keywords_text, "rule keywords"),
                voice=voice,
                min_loudness_dbfs=optional_signed_form_float(
                    loudness_text,
                    "rule min_loudness_dbfs",
                ),
                min_duration_seconds=optional_form_float(
                    min_duration_text,
                    "rule min_duration_seconds",
                ),
                max_duration_seconds=optional_form_float(
                    max_duration_text,
                    "rule max_duration_seconds",
                ),
                severity=severity,
            )
        )
    return rules


def streamer_event_detection_from_form(
    params: dict[str, list[str]],
) -> StreamEventDetectionConfig | None:
    mode = (first_query_value(params, "event_enabled") or "inherit").strip().lower()
    if mode not in {"inherit", "true", "false"}:
        raise ConfigError("event_enabled must be inherit, true, or false")
    model = first_query_value(params, "event_model").strip()
    device = first_query_value(params, "event_device").strip()
    window = optional_form_float(first_query_value(params, "event_window_seconds"), "event_window_seconds")
    hop = optional_form_float(first_query_value(params, "event_hop_seconds"), "event_hop_seconds")
    min_confidence = optional_probability_form_float(
        first_query_value(params, "event_min_confidence"),
        "event_min_confidence",
    )
    max_events = optional_form_int(
        first_query_value(params, "event_max_events_per_media"),
        "event_max_events_per_media",
    )
    enabled = None if mode == "inherit" else mode == "true"
    if (
        enabled is None
        and not model
        and not device
        and not window
        and not hop
        and min_confidence < 0
        and not max_events
    ):
        return None
    return StreamEventDetectionConfig(
        enabled=enabled,
        model=model,
        device=device,
        window_seconds=window,
        hop_seconds=hop,
        min_confidence=min_confidence,
        max_events_per_media=max_events,
    )


def form_list_value(params: dict[str, list[str]], key: str, index: int) -> str:
    values = params.get(key, [])
    if index >= len(values):
        return ""
    return values[index]


def update_streamer_voice_from_form(
    config: BotConfig,
    params: dict[str, list[str]],
) -> None:
    if config.config_path is None:
        raise ConfigError("Config path is not available")
    streamer_name = first_query_value(params, "streamer_name").strip()
    voice_name = validate_voice_name(first_query_value(params, "voice_name"))
    action = (first_query_value(params, "action") or "save").strip().lower()
    if streamer_name not in config.streamers:
        raise ConfigError(f"streamer is not configured: {streamer_name}")
    if action == "delete":
        update_streamer_voice_profile_config(config.config_path, streamer_name, voice_name, None)
        reload_running_config(config)
        return
    if action != "save":
        raise ConfigError("Unknown voice profile action")

    existing = config.streamers[streamer_name].voices.get(voice_name)
    samples_text = first_query_value(params, "samples")
    samples = (
        form_string_list(samples_text, "samples", split_commas=False)
        if samples_text.strip()
        else list(existing.samples if existing is not None else [])
    )
    threshold = optional_form_float(first_query_value(params, "threshold"), "threshold")
    profile = VoiceProfileConfig(
        enabled=form_bool(first_query_value(params, "enabled") or "true", "enabled"),
        samples=samples,
        threshold=threshold,
        notes=first_query_value(params, "notes").strip(),
    )
    update_streamer_voice_profile_config(
        config.config_path,
        streamer_name,
        voice_name,
        profile,
    )
    reload_running_config(config)


def update_streamer_voice_with_optional_sample(
    config: BotConfig,
    fields: dict[str, list[str]],
    files: dict[str, tuple[str, bytes]],
) -> None:
    action = (first_query_value(fields, "action") or "save").strip().lower()
    update_streamer_voice_from_form(config, fields)
    if action != "save":
        return
    upload = files.get("media")
    if upload is None:
        return
    upload_filename, upload_bytes = upload
    if not upload_filename and not upload_bytes:
        return
    store_streamer_voice_sample_upload(config, fields, {"media": upload})


def optional_form_float(value: str, name: str) -> float:
    raw = value.strip()
    if not raw:
        return 0.0
    try:
        parsed = float(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number") from exc
    if parsed < 0:
        raise ConfigError(f"{name} must not be negative")
    return parsed


def optional_signed_form_float(value: str, name: str) -> float | None:
    raw = value.strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number") from exc


def optional_probability_form_float(value: str, name: str) -> float:
    raw = value.strip()
    if not raw:
        return -1.0
    parsed = form_float(raw, name, minimum=0)
    if parsed > 1:
        raise ConfigError(f"{name} must be between 0 and 1")
    return parsed


def optional_form_int(value: str, name: str) -> int:
    raw = value.strip()
    if not raw:
        return 0
    return form_int(raw, name, minimum=1)


def store_streamer_voice_sample_upload(
    config: BotConfig,
    fields: dict[str, list[str]],
    files: dict[str, tuple[str, bytes]],
) -> None:
    if config.config_path is None:
        raise ConfigError("Config path is not available")
    streamer_name = first_query_value(fields, "streamer_name").strip()
    voice_name = validate_voice_name(first_query_value(fields, "voice_name"))
    if streamer_name not in config.streamers:
        raise ConfigError(f"streamer is not configured: {streamer_name}")
    upload = files.get("media")
    if upload is None:
        raise ConfigError("Missing voice sample upload")
    upload_filename, upload_bytes = upload
    if not upload_bytes:
        raise ConfigError("Voice sample upload is empty")
    if len(upload_bytes) > config.voice_sample_max_bytes:
        raise ConfigError("Voice sample upload is too large")

    directory = voice_sample_dir(config, streamer_name, voice_name)
    directory.mkdir(parents=True, exist_ok=True)
    sample_name = unique_upload_sample_name(directory, sanitize_voice_sample_filename(upload_filename))
    (directory / sample_name).write_bytes(upload_bytes)

    streamer = config.streamers[streamer_name]
    profile = add_voice_sample_to_profile(streamer.voices.get(voice_name), sample_name)
    update_streamer_voice_profile_config(
        config.config_path,
        streamer_name,
        voice_name,
        profile,
    )
    reload_running_config(config)


def unique_upload_sample_name(directory: Path, sample_name: str) -> str:
    candidate = sample_name
    if not (directory / candidate).exists():
        return candidate
    stem = Path(sample_name).stem
    suffix = Path(sample_name).suffix
    for index in range(2, 1000):
        candidate = f"{stem}-{index}{suffix}"
        if not (directory / candidate).exists():
            return candidate
    raise ConfigError("Unable to allocate a unique voice sample filename")


def create_streamer_voice_sample_from_transcript_form(
    config: BotConfig,
    params: dict[str, list[str]],
) -> None:
    if config.config_path is None:
        raise ConfigError("Config path is not available")
    streamer_name = first_query_value(params, "streamer_name").strip()
    voice_name = validate_voice_name(first_query_value(params, "voice_name"))
    video_id = first_query_value(params, "video_id")
    media_name = first_query_value(params, "media_name")
    speaker_label = first_query_value(params, "speaker_label").strip()
    if streamer_name not in config.streamers:
        raise ConfigError(f"streamer is not configured: {streamer_name}")
    resolved = resolve_transcription_source_file(config, video_id, media_name)
    if resolved is None:
        raise ConfigError("Transcript source media was not found")
    record, media_file = resolved
    if streamer_display_name_for_channel(config, record.channel) != streamer_name:
        raise ConfigError("Transcript source does not belong to this streamer")
    try:
        sample_name = create_transcript_voice_sample(
            config,
            streamer_name,
            voice_name,
            media_file,
            speaker_label,
        )
    except ValueError as exc:
        raise ConfigError(str(exc)) from exc

    streamer = config.streamers[streamer_name]
    profile = add_voice_sample_to_profile(streamer.voices.get(voice_name), sample_name)
    update_streamer_voice_profile_config(
        config.config_path,
        streamer_name,
        voice_name,
        profile,
    )
    reload_running_config(config)


def update_streamer_voice_attribution_from_form(
    config: BotConfig,
    params: dict[str, list[str]],
) -> None:
    action = (first_query_value(params, "action") or "").strip().lower()
    video_id = first_query_value(params, "video_id")
    media_name = first_query_value(params, "media_name")
    resolved = resolve_transcription_source_file(config, video_id, media_name)
    if resolved is None:
        raise ConfigError("Transcript source media was not found")
    record, media_file = resolved
    effective = transcription_config_for_channel(config, record.channel)
    if action == "rematch":
        match_known_voices_for_media(effective, media_file, channel=record.channel, logger=LOGGER)
        rewrite_speaker_labels_for_media(effective, media_file, channel=record.channel, logger=LOGGER)
        return
    if action not in {"approve", "reject"}:
        raise ConfigError("Unknown voice attribution action")
    speaker_label = first_query_value(params, "speaker_label")
    voice_name = first_query_value(params, "voice_name")
    if not update_voice_attribution_decision(
        media_file,
        speaker_label,
        action,
        voice_name=voice_name,
    ):
        raise ConfigError("Unable to update voice attribution")
    rewrite_speaker_labels_for_media(effective, media_file, channel=record.channel, logger=LOGGER)


def update_speaker_labels_from_form(
    config: BotConfig,
    params: dict[str, list[str]],
) -> None:
    if config.config_path is None:
        raise ConfigError("Config path is not available")
    channel = first_query_value(params, "channel").strip()
    if not channel:
        raise ConfigError("speaker labels require a channel")
    labels = speaker_labels_from_form(params)
    if channel in config.streamers:
        update_streamer_speaker_labels_config(config.config_path, channel, labels)
    else:
        update_channel_speaker_labels_config(config.config_path, channel, labels)
    reload_running_config(config)
    rewritten = rewrite_speaker_subtitles_for_channel(config, channel)
    LOGGER.info(
        "Updated speaker labels channel=%s labels=%d subtitles_rewritten=%d",
        channel,
        len(labels),
        rewritten,
    )


def speaker_labels_from_form(params: dict[str, list[str]]) -> dict[str, str]:
    raw_labels = params.get("speaker_label") or []
    raw_names = params.get("speaker_name") or []
    labels: dict[str, str] = {}
    for index, raw_label in enumerate(raw_labels):
        label = raw_label.strip()
        name = raw_names[index].strip() if index < len(raw_names) else ""
        if not label and not name:
            continue
        if not label:
            raise ConfigError("speaker label is required when a speaker name is set")
        if any(char.isspace() for char in label):
            raise ConfigError("speaker labels must not contain whitespace")
        if name:
            labels[label] = name
    return labels


def rewrite_speaker_subtitles_for_channel(config: BotConfig, channel: str) -> int:
    target_key = channel_group_key(channel)
    state = StateStore(config.db_path)
    try:
        records = state.list_streams(limit=5000)
    finally:
        state.close()

    rewritten = 0
    for record in records:
        record_name = streamer_display_name_for_channel(config, record.channel) or record.channel
        if channel_group_key(record_name) != target_key:
            continue
        directory = segment_directory(config, record.video_id, record.channel)
        if not directory.is_dir():
            continue
        for media_file in sorted(directory.iterdir(), key=lambda item: item.name):
            if (
                not media_file.is_file()
                or not is_transcribable_media_file(media_file.name)
            ):
                continue
            if rewrite_speaker_labels_for_media(
                transcription_config_for_channel(config, record.channel),
                media_file,
                channel=record.channel,
                logger=LOGGER,
            ):
                rewritten += 1
    return rewritten


def voice_detection_root_updates(
    params: dict[str, list[str]],
    mode: str,
) -> dict[str, object]:
    override = voice_detection_override_from_form(params, mode)
    updates: dict[str, object] = {
        "whisperx_diarize": override.mode != "off",
        "whisperx_min_speakers": override.min_speakers,
        "whisperx_max_speakers": override.max_speakers,
    }
    if override.hf_token_env:
        updates["whisperx_hf_token_env"] = override.hf_token_env
    return updates


def voice_detection_override_from_form(
    params: dict[str, list[str]],
    mode: str,
) -> VoiceDetectionConfig:
    mode = mode.strip().lower()
    speakers = (
        positive_form_int(first_query_value(params, "speakers"), "speakers")
        if mode == "fixed"
        else 0
    )
    min_speakers = (
        positive_form_int(first_query_value(params, "min_speakers"), "min_speakers")
        if mode == "range"
        else 0
    )
    max_speakers = (
        positive_form_int(first_query_value(params, "max_speakers"), "max_speakers")
        if mode == "range"
        else 0
    )
    hf_token_env = first_query_value(params, "hf_token_env").strip()
    if hf_token_env:
        validate_env_var_name(hf_token_env, "hf_token_env")

    if mode == "off":
        return VoiceDetectionConfig(mode="off", hf_token_env=hf_token_env)
    if mode == "auto":
        return VoiceDetectionConfig(mode="auto", hf_token_env=hf_token_env)
    if mode == "fixed":
        if not speakers:
            raise ConfigError("fixed voice detection requires speakers")
        return VoiceDetectionConfig(
            mode="fixed",
            min_speakers=speakers,
            max_speakers=speakers,
            hf_token_env=hf_token_env,
        )
    if mode == "range":
        if not min_speakers and not max_speakers:
            raise ConfigError("range voice detection requires min and/or max speakers")
        if min_speakers and max_speakers and min_speakers > max_speakers:
            raise ConfigError("min_speakers must be less than or equal to max_speakers")
        return VoiceDetectionConfig(
            mode="range",
            min_speakers=min_speakers,
            max_speakers=max_speakers,
            hf_token_env=hf_token_env,
        )
    raise ConfigError("Unknown voice detection mode")


def positive_form_int(value: str, name: str) -> int:
    value = value.strip()
    if not value:
        return 0
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ConfigError(f"{name} must be a positive integer") from exc
    if parsed <= 0:
        raise ConfigError(f"{name} must be a positive integer")
    return parsed


def validate_env_var_name(value: str, name: str) -> None:
    if value.startswith("hf_"):
        raise ConfigError(f"{name} must be an environment variable name, not a token")
    if not value.replace("_", "A").isalnum() or value[0].isdigit():
        raise ConfigError(f"{name} must be a valid environment variable name")


def start_transcription_job(
    config: BotConfig,
    video_id: str,
    filename: str,
    *,
    regenerate: bool = False,
) -> tuple[bool, str]:
    resolved = resolve_transcription_source_file(config, video_id, filename)
    if resolved is None:
        return False, "No matching finalized media file found"

    record, media_file = resolved
    if transcription_outputs_exist(media_file) and not regenerate:
        return True, "Transcript already exists"

    key = transcription_job_key(video_id, media_file.name)
    now = time.time()
    with TRANSCRIPTION_JOBS_LOCK:
        existing = TRANSCRIPTION_JOBS.get(key)
        if existing is not None and existing.status == "running":
            return True, "Transcription is already running"
        TRANSCRIPTION_JOBS[key] = TranscriptionJob(
            video_id=video_id,
            media_name=media_file.name,
            status="running",
            message=(
                "Retranscribing subtitles" if regenerate else "Transcribing subtitles"
            ),
            started_at=now,
            phase="Queued",
            progress=0.0,
            updated_at=now,
        )

    record_stream_event(
        config,
        video_id,
        ("Queued subtitle retranscription" if regenerate else "Queued subtitle transcription")
        + f" for {media_file.name}",
        segment_index=record.segment_index,
    )
    thread = Thread(
        target=run_transcription_job,
        args=(config, key, media_file, regenerate, record.channel),
        name=f"onlysavemevods-transcribe-{video_id}",
        daemon=True,
    )
    thread.start()
    LOGGER.info(
        "Queued manual %s for %s using media=%s",
        "retranscription" if regenerate else "transcription",
        video_id,
        media_file.name,
    )
    return True, "Retranscription queued" if regenerate else "Transcription queued"


def run_transcription_job(
    config: BotConfig,
    key: str,
    media_file: Path,
    regenerate: bool = False,
    channel: str = "",
) -> None:
    def progress(phase: str, value: float | None = None) -> None:
        update_transcription_job(
            key,
            phase=phase,
            progress=value,
            message=phase,
            updated_at=time.time(),
        )

    progress("Starting transcription", 0.02)
    try:
        ok = asyncio.run(
            transcribe_media_file(
                transcription_config_for_channel(config, channel),
                media_file,
                overwrite=regenerate,
                logger=LOGGER,
                progress_callback=progress,
                channel=channel,
            )
        )
    except Exception as exc:  # noqa: BLE001 - background job must capture failures.
        LOGGER.exception("Manual transcription failed for media=%s", media_file)
        message = str(exc) or exc.__class__.__name__
        update_transcription_job(
            key,
            status="failed",
            message=message,
            phase="Failed",
            finished_at=time.time(),
            updated_at=time.time(),
        )
        record_stream_event(
            config,
            video_id_from_job_key(key),
            f"Subtitle transcription failed for {media_file.name}: {message}",
            level="error",
        )
        return

    if not ok:
        message = "WhisperX did not produce both .srt and .vtt outputs"
        update_transcription_job(
            key,
            status="failed",
            message=message,
            phase="Failed",
            finished_at=time.time(),
            updated_at=time.time(),
        )
        record_stream_event(
            config,
            video_id_from_job_key(key),
            f"Subtitle transcription failed for {media_file.name}: {message}",
            level="error",
        )
        return

    LOGGER.info("Manual transcription completed: %s", media_file)
    finished = time.time()
    update_transcription_job(
        key,
        status="done",
        message="Transcribed subtitles",
        phase="Complete",
        progress=1.0,
        finished_at=finished,
        updated_at=finished,
    )
    record_stream_event(
        config,
        video_id_from_job_key(key),
        f"Subtitle transcription completed for {media_file.name}",
    )


def transcription_job_key(video_id: str, filename: str) -> str:
    return f"{video_id}\0{filename}"


def transcription_job_for(video_id: str, filename: str) -> TranscriptionJob | None:
    with TRANSCRIPTION_JOBS_LOCK:
        return TRANSCRIPTION_JOBS.get(transcription_job_key(video_id, filename))


def update_transcription_job(key: str, **changes: Any) -> None:
    with TRANSCRIPTION_JOBS_LOCK:
        job = TRANSCRIPTION_JOBS.get(key)
        if job is None:
            return
        TRANSCRIPTION_JOBS[key] = TranscriptionJob(
            video_id=changes.get("video_id", job.video_id),
            media_name=changes.get("media_name", job.media_name),
            status=changes.get("status", job.status),
            message=changes.get("message", job.message),
            started_at=changes.get("started_at", job.started_at),
            finished_at=changes.get("finished_at", job.finished_at),
            phase=changes.get("phase", job.phase),
            progress=changes.get("progress", job.progress),
            updated_at=changes.get("updated_at", job.updated_at),
        )


def start_event_detection_job(
    config: BotConfig,
    video_id: str,
    filename: str,
    *,
    regenerate: bool = False,
) -> tuple[bool, str]:
    if not config.stream_event_detection_enabled:
        return False, "Content event detection is disabled in config"
    resolved = resolve_transcription_source_file(config, video_id, filename)
    if resolved is None:
        return False, "No matching finalized media file found"

    record, media_file = resolved
    if content_events_exist(media_file) and not regenerate:
        return True, "Content events already detected"

    key = event_detection_job_key(video_id, media_file.name)
    now = time.time()
    with EVENT_DETECTION_JOBS_LOCK:
        existing = EVENT_DETECTION_JOBS.get(key)
        if existing is not None and existing.status == "running":
            return True, "Content event detection is already running"
        EVENT_DETECTION_JOBS[key] = EventDetectionJob(
            video_id=video_id,
            media_name=media_file.name,
            status="running",
            message=("Redetecting content events" if regenerate else "Detecting content events"),
            started_at=now,
            phase="Queued",
            progress=0.0,
            updated_at=now,
        )

    record_stream_event(
        config,
        video_id,
        ("Queued content event redetection" if regenerate else "Queued content event detection")
        + f" for {media_file.name}",
        segment_index=record.segment_index,
    )
    thread = Thread(
        target=run_event_detection_job,
        args=(config, key, media_file, regenerate, record.channel),
        name=f"onlysavemevods-events-{video_id}",
        daemon=True,
    )
    thread.start()
    LOGGER.info(
        "Queued manual content event %s for %s using media=%s",
        "redetection" if regenerate else "detection",
        video_id,
        media_file.name,
    )
    return True, "Content event redetection queued" if regenerate else "Content event detection queued"


def run_event_detection_job(
    config: BotConfig,
    key: str,
    media_file: Path,
    regenerate: bool = False,
    channel: str = "",
) -> None:
    def progress(phase: str, value: float | None = None) -> None:
        update_event_detection_job(
            key,
            phase=phase,
            progress=value,
            message=phase,
            updated_at=time.time(),
        )

    progress("Starting content event detection", 0.02)
    try:
        ok = detect_content_events_for_media(
            config,
            media_file,
            overwrite=regenerate,
            logger=LOGGER,
            progress_callback=progress,
            channel=channel,
        )
    except ContentEventDetectorUnavailable as exc:
        message = str(exc) or exc.__class__.__name__
        update_event_detection_job(
            key,
            status="failed",
            message=message,
            phase="Unavailable",
            finished_at=time.time(),
            updated_at=time.time(),
        )
        record_stream_event(
            config,
            video_id_from_job_key(key),
            f"Content event detection unavailable for {media_file.name}: {message}",
            level="warning",
        )
        return
    except Exception as exc:  # noqa: BLE001 - background job must capture failures.
        LOGGER.exception("Content event detection failed for media=%s", media_file)
        message = str(exc) or exc.__class__.__name__
        update_event_detection_job(
            key,
            status="failed",
            message=message,
            phase="Failed",
            finished_at=time.time(),
            updated_at=time.time(),
        )
        record_stream_event(
            config,
            video_id_from_job_key(key),
            f"Content event detection failed for {media_file.name}: {message}",
            level="error",
        )
        return

    if not ok:
        message = "Content event detection did not run"
        update_event_detection_job(
            key,
            status="failed",
            message=message,
            phase="Skipped",
            finished_at=time.time(),
            updated_at=time.time(),
        )
        return

    finished = time.time()
    events = load_content_events(media_file)
    update_event_detection_job(
        key,
        status="done",
        message=f"Detected {len(events)} content event(s)",
        phase="Complete",
        progress=1.0,
        finished_at=finished,
        updated_at=finished,
    )
    record_stream_event(
        config,
        video_id_from_job_key(key),
        f"Content event detection completed for {media_file.name}: {len(events)} event(s)",
    )


def event_detection_job_key(video_id: str, filename: str) -> str:
    return f"{video_id}\0{filename}"


def event_detection_job_for(video_id: str, filename: str) -> EventDetectionJob | None:
    with EVENT_DETECTION_JOBS_LOCK:
        return EVENT_DETECTION_JOBS.get(event_detection_job_key(video_id, filename))


def update_event_detection_job(key: str, **changes: Any) -> None:
    with EVENT_DETECTION_JOBS_LOCK:
        job = EVENT_DETECTION_JOBS.get(key)
        if job is None:
            return
        EVENT_DETECTION_JOBS[key] = EventDetectionJob(
            video_id=changes.get("video_id", job.video_id),
            media_name=changes.get("media_name", job.media_name),
            status=changes.get("status", job.status),
            message=changes.get("message", job.message),
            started_at=changes.get("started_at", job.started_at),
            finished_at=changes.get("finished_at", job.finished_at),
            phase=changes.get("phase", job.phase),
            progress=changes.get("progress", job.progress),
            updated_at=changes.get("updated_at", job.updated_at),
        )


def start_watermark_job(
    config: BotConfig,
    video_id: str,
    filename: str,
    recipient_label: str,
) -> tuple[bool, str]:
    if not config.watermark_enabled:
        return False, "Watermarking is disabled in config"
    try:
        require_watermark_secret(config)
        label = validate_recipient_label(recipient_label)
    except WatermarkError as exc:
        return False, str(exc)

    resolved = resolve_watermark_source_file(config, video_id, filename)
    if resolved is None:
        return False, "No matching finalized video file found"

    _record, source_file = resolved
    copy_id = new_copy_id()
    output_name = watermarked_output_name(source_file.name, copy_id)
    state = StateStore(config.db_path)
    try:
        state.create_watermark_copy(
            copy_id=copy_id,
            video_id=video_id,
            source_name=source_file.name,
            output_name=output_name,
            recipient_label=label,
            message="Queued watermark render",
        )
        state.add_stream_event(
            video_id,
            f"Queued watermark copy for {source_file.name} recipient={label!r}",
        )
    finally:
        state.close()

    thread = Thread(
        target=run_watermark_job,
        args=(config, copy_id),
        name=f"onlysavemevods-watermark-{video_id}",
        daemon=True,
    )
    thread.start()
    LOGGER.info(
        "Queued watermark copy video_id=%s source=%s copy_id=%s recipient=%r",
        video_id,
        source_file.name,
        copy_id,
        label,
    )
    return True, "Watermark job queued"


def delete_watermark_copy(config: BotConfig, copy_id: str) -> tuple[bool, str]:
    copy_id = copy_id.strip()
    if not copy_id:
        return False, "Watermark copy id is required"

    state = StateStore(config.db_path)
    directory: Path | None = None
    try:
        copy = state.get_watermark_copy(copy_id)
        if copy is None:
            return False, "Watermark copy not found"
        if copy.status in {WATERMARK_STATUS_QUEUED, WATERMARK_STATUS_RUNNING}:
            return False, "Watermark copy is still running"

        record = state.get_stream(copy.video_id)
        if record is not None:
            directory = segment_directory(config, record.video_id, record.channel)
            output_file = resolve_watermark_output_file(directory, copy.output_name)
            if output_file is None:
                return False, "Invalid watermark output path"
            try:
                output_file.unlink(missing_ok=True)
            except OSError as exc:
                return False, f"Unable to delete watermark file: {exc}"
        deleted = state.delete_watermark_copy(copy_id)
        if not deleted:
            return False, "Watermark copy not found"
        state.add_stream_event(
            copy.video_id,
            f"Deleted watermark copy copy_id={copy_id} recipient={copy.recipient_label!r}",
        )
    finally:
        state.close()

    if directory is not None:
        with FILE_SCAN_CACHE_LOCK:
            FILE_SCAN_CACHE.pop(str(directory), None)
    LOGGER.info("Deleted watermark copy copy_id=%s", copy_id)
    return True, "Watermark copy deleted"


def run_watermark_job(config: BotConfig, copy_id: str) -> None:
    state = StateStore(config.db_path)
    started_at = time.monotonic()
    copy: WatermarkCopyRecord | None = None
    try:
        copy = state.get_watermark_copy(copy_id)
        if copy is None:
            state.close()
            return
        record = state.get_stream(copy.video_id)
        if record is None:
            state.update_watermark_copy(
                copy_id,
                status=WATERMARK_STATUS_FAILED,
                message="Stream record not found",
                error="Stream record not found",
                finished=True,
            )
            state.add_stream_event(
                copy.video_id,
                f"Watermark copy failed copy_id={copy_id}: stream record not found",
                level="error",
            )
            state.close()
            return

        directory = segment_directory(config, record.video_id, record.channel)
        source_file = directory / copy.source_name
        output_file = resolve_watermark_output_file(directory, copy.output_name)
        if output_file is None:
            raise WatermarkError("Invalid watermark output path")
        if not source_file.is_file() or not is_watermarkable_media_file(source_file.name):
            raise WatermarkError("Source video is no longer available")

        state.update_watermark_copy(
            copy_id,
            status=WATERMARK_STATUS_RUNNING,
            message="Rendering watermarked copy",
            error="",
            started=True,
            phase="Preparing watermark",
            progress=0.02,
        )
        secret = require_watermark_secret(config)
        create_watermarked_copy(
            source_file=source_file,
            output_file=output_file,
            secret=secret,
            copy_id=copy.copy_id,
            video_id=copy.video_id,
            source_name=copy.source_name,
            strength=config.watermark_strength,
            ffmpeg_path=config.ffmpeg_path,
            overwrite=True,
            progress_callback=lambda phase, value: state.update_watermark_copy(
                copy_id,
                message=phase,
                phase=phase,
                progress=value,
            ),
        )
    except Exception as exc:  # noqa: BLE001 - background job must capture failures.
        LOGGER.exception("Watermark render failed copy_id=%s", copy_id)
        message = str(exc) or exc.__class__.__name__
        state.update_watermark_copy(
            copy_id,
            status=WATERMARK_STATUS_FAILED,
            message="Watermark render failed",
            error=message,
            finished=True,
        )
        if copy is not None:
            state.add_stream_event(
                copy.video_id,
                f"Watermark copy failed copy_id={copy_id}: {message}",
                level="error",
            )
        state.close()
        return

    state.update_watermark_copy(
        copy_id,
        status=WATERMARK_STATUS_DONE,
        message=elapsed_message(started_at),
        error="",
        finished=True,
    )
    state.add_stream_event(
        copy.video_id,
        f"Watermark copy completed copy_id={copy_id} output={copy.output_name}",
    )
    state.close()
    LOGGER.info("Watermark render completed copy_id=%s", copy_id)


def detect_watermark_file(config: BotConfig, media_file: Path) -> Any:
    secret = require_watermark_secret(config)
    state = StateStore(config.db_path)
    try:
        records = state.list_watermark_copies(
            statuses=[WATERMARK_STATUS_DONE],
            limit=5000,
        )
    finally:
        state.close()
    return detect_watermark(media_file=media_file, records=records, secret=secret)


def is_downloadable_file(name: str) -> bool:
    if is_powerchat_event_file(name):
        return True
    kind = file_kind(name)
    if kind == "chat":
        return True
    if kind != "final":
        return False
    _segment, format_id = file_details(name)
    return format_id is None


def summarize_bytes_by_kind(files: list[FileStatus]) -> dict[str, int]:
    totals: dict[str, int] = {}
    for file in files:
        totals[file.kind] = totals.get(file.kind, 0) + file.size_bytes
    return totals


def summarize_counts_by_kind(files: list[FileStatus]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for file in files:
        counts[file.kind] = counts.get(file.kind, 0) + 1
    return counts


def file_details(name: str) -> tuple[str | None, str | None]:
    match = SEGMENT_NAME_RE.match(name)
    if not match:
        return None, None
    return match.group("segment"), match.group("format_id")


def file_kind(name: str) -> str:
    if ".part-Frag" in name:
        return "fragment"
    if name.endswith(".part"):
        return "part"
    if name.endswith(".ytdl"):
        return "state"
    if (
        is_chat_timing_file(name)
        or name.endswith(".voice-attribution.json")
        or name.endswith(".stream-events.json")
        or is_powerchat_event_file(name)
    ):
        return "state"
    if is_live_chat_file(name):
        return "chat"
    if is_yt_dlp_temporary_file(name) or is_rendering_temporary_file(name):
        return "temporary"
    return "final"


def snapshot_to_dict(snapshot: StatusSnapshot) -> dict[str, Any]:
    payload = asdict(snapshot)
    payload["detail"] = "full"
    return payload


def json_script_payload(value: Any) -> str:
    return (
        json.dumps(value, sort_keys=True)
        .replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
    )


def render_status_html(snapshot: StatusSnapshot) -> str:
    started_at = time.perf_counter()
    generated = time.strftime(
        "%Y-%m-%d %H:%M:%S %Z",
        time.localtime(snapshot.generated_at),
    )
    active = snapshot.counts.get("downloading", 0)
    checking = snapshot.counts.get("checking_after_exit", 0)
    total = len(snapshot.streams)
    attention_streams = [
        stream
        for stream in snapshot.streams
        if stream_needs_attention(stream)
    ]
    status_counts = render_status_counts(snapshot.counts)
    streamer_toolbar = render_streamer_toolbar(snapshot)
    streamer_wizard = render_streamer_wizard(snapshot)
    streamer_cards = render_streamer_dashboard(snapshot)
    job_rows = render_job_rows(snapshot.jobs)
    config_sections = render_config_sections(snapshot.configuration)
    content_event_rules_panel = render_content_event_rules_panel(snapshot)
    voice_detection_panel = render_voice_detection_panel(snapshot)
    speaker_labels_panel = render_speaker_labels_panel(snapshot)
    app_config_form = render_app_config_form(snapshot)
    log_rows = render_log_rows(snapshot.recent_logs)
    watermark_detection = render_watermark_detection_panel(snapshot.configuration)
    about_panel = render_about_panel(snapshot)
    powerchat_dashboard = render_powerchat_dashboard(snapshot.powerchat_stats)
    powerchat_stats_json = json_script_payload(snapshot.powerchat_stats)
    script = dashboard_script()

    body = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="icon" href="/favicon.ico?v={escape(APP_VERSION, quote=True)}" sizes="any">
  <link rel="icon" type="image/png" sizes="32x32" href="/favicon-32x32.png?v={escape(APP_VERSION, quote=True)}">
  <link rel="icon" type="image/png" sizes="16x16" href="/favicon-16x16.png?v={escape(APP_VERSION, quote=True)}">
  <link rel="apple-touch-icon" sizes="180x180" href="/apple-touch-icon.png?v={escape(APP_VERSION, quote=True)}">
  <link rel="icon" type="image/png" sizes="192x192" href="/android-chrome-192x192.png?v={escape(APP_VERSION, quote=True)}">
  <title>ONLYSAVEmeVODS Status</title>
  <style>
    :root {{
      color-scheme: light dark;
      --bg: #f6f7f9;
      --panel: #ffffff;
      --panel-strong: #eef2f6;
      --text: #18202a;
      --muted: #647184;
      --line: #d7dde5;
      --active: #0f7b44;
      --warn: #ad5f00;
      --bad: #a4262c;
    }}
    @media (prefers-color-scheme: dark) {{
      :root {{
        --bg: #11161d;
        --panel: #171e27;
        --panel-strong: #202a35;
        --text: #eef3f8;
        --muted: #9da9b8;
        --line: #2c3745;
      }}
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font: 14px/1.45 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    a {{ color: inherit; }}
    header {{
      border-bottom: 1px solid var(--line);
      background: var(--panel);
      padding: 18px clamp(16px, 4vw, 42px);
    }}
    main {{ padding: 18px clamp(16px, 4vw, 42px) 36px; }}
    h1 {{ margin: 0 0 12px; font-size: 24px; font-weight: 700; }}
    h2 {{ margin: 0 0 10px; font-size: 16px; }}
    h3 {{ margin: 0 0 8px; font-size: 14px; }}
    .summary {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      gap: 10px;
    }}
    .metric, .stream, .empty, .panel, .streamer-section {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
    }}
    .metric {{ padding: 10px 12px; }}
    .metric strong {{ display: block; font-size: 22px; }}
    .metric span, .meta, .file-meta, .muted, dd {{ color: var(--muted); }}
    .toolbar {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      margin-top: 12px;
      color: var(--muted);
    }}
    .tab-radio {{
      position: absolute;
      opacity: 0;
      pointer-events: none;
    }}
    .tabs {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      border-bottom: 1px solid var(--line);
      margin-bottom: 14px;
    }}
    .tabs label {{
      border: 1px solid var(--line);
      border-bottom: 0;
      border-radius: 8px 8px 0 0;
      padding: 8px 12px;
      cursor: pointer;
      color: var(--muted);
      background: var(--panel);
    }}
    .tab-panel {{ display: none; }}
    #tab-streamers:checked ~ .tabs label[for="tab-streamers"],
    #tab-powerchat:checked ~ .tabs label[for="tab-powerchat"],
    #tab-jobs:checked ~ .tabs label[for="tab-jobs"],
    #tab-logs:checked ~ .tabs label[for="tab-logs"],
    #tab-about:checked ~ .tabs label[for="tab-about"],
    #tab-config:checked ~ .tabs label[for="tab-config"] {{
      color: var(--text);
      background: var(--panel-strong);
      font-weight: 650;
    }}
    #tab-streamers:checked ~ .streamers-panel,
    #tab-powerchat:checked ~ .powerchat-panel,
    #tab-jobs:checked ~ .jobs-panel,
    #tab-logs:checked ~ .logs-panel,
    #tab-about:checked ~ .about-panel,
    #tab-config:checked ~ .config-panel {{ display: block; }}
    .dashboard-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 14px;
      margin-bottom: 14px;
    }}
    .config-stack {{
      display: grid;
      grid-template-columns: minmax(0, 1fr);
      gap: 14px;
      margin-top: 14px;
    }}
    .panel {{ padding: 14px; }}
    .about-heading {{
      display: flex;
      align-items: center;
      gap: 14px;
      margin-bottom: 14px;
    }}
    .about-icon {{
      width: clamp(72px, 12vw, 128px);
      height: clamp(72px, 12vw, 128px);
      object-fit: contain;
      flex: 0 0 auto;
    }}
    .about-title {{ min-width: 0; }}
    .about-title h2 {{ margin: 0 0 4px; }}
    @media (max-width: 520px) {{
      .about-heading {{ align-items: flex-start; }}
      .about-icon {{ width: 72px; height: 72px; }}
    }}
    dl {{
      display: grid;
      grid-template-columns: minmax(110px, max-content) minmax(0, 1fr);
      gap: 7px 14px;
      margin: 0;
    }}
    dt {{ color: var(--text); font-weight: 600; }}
    dd {{ margin: 0; overflow-wrap: anywhere; }}
    .status-counts {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 12px;
    }}
    .stream {{ padding: 14px; margin-top: 14px; }}
    .streamer-toolbar {{
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 14px;
    }}
    .primary-action {{ font-weight: 650; }}
    .streamer-list {{ display: grid; gap: 12px; }}
    .streamer-section {{ padding: 14px; }}
    .streamer-section.needs-grouping {{
      border-left: 4px solid var(--warn);
      padding-left: 12px;
    }}
    .streamer-head {{
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: start;
    }}
    .streamer-title {{ display: grid; gap: 6px; min-width: 0; }}
    .streamer-title h2 {{ margin: 0; }}
    .streamer-details {{ margin-top: 12px; }}
    .streamer-section.collapsed .streamer-details {{ display: none; }}
    .streamer-badges, .source-chips {{
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      align-items: center;
    }}
    .streamer-badges {{ justify-content: flex-end; }}
    .source-chip {{
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 2px 8px 2px 4px;
      color: var(--muted);
      background: var(--panel-strong);
      overflow-wrap: anywhere;
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }}
    .source-builder {{
      display: grid;
      gap: 8px;
    }}
    .source-list {{
      display: grid;
      gap: 7px;
    }}
    .source-list-row {{
      display: grid;
      grid-template-columns: max-content minmax(0, 1fr) max-content max-content;
      gap: 8px;
      align-items: center;
      border: 1px solid var(--line);
      border-radius: 7px;
      padding: 8px;
      background: var(--panel);
    }}
    .source-list-empty {{
      border: 1px dashed var(--line);
      border-radius: 7px;
      padding: 10px;
    }}
    .source-platform-icon {{
      width: 26px;
      height: 26px;
      display: inline-grid;
      place-items: center;
      border-radius: 6px;
      color: var(--muted);
      background: transparent;
      border: 0;
      font-size: 12px;
      font-weight: 750;
      line-height: 1;
      overflow: hidden;
    }}
    .source-platform-icon img {{
      width: 100%;
      height: 100%;
      object-fit: contain;
      padding: 3px;
    }}
    .source-platform-icon img + .source-platform-initial {{ display: none; }}
    .source-platform-icon.youtube,
    .source-platform-icon.twitch,
    .source-platform-icon.kick,
    .source-platform-icon.rumble,
    .source-platform-icon.unknown {{ background: transparent; }}
    .source-name {{ font-weight: 650; overflow-wrap: anywhere; }}
    .source-raw {{ overflow-wrap: anywhere; }}
    .source-builder-actions {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
    }}
    .source-popover {{
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px;
      background: var(--panel);
      box-shadow: 0 10px 26px rgba(15, 23, 42, 0.12);
    }}
    .source-popover[hidden] {{ display: none; }}
    .source-popover-head {{
      display: flex;
      justify-content: space-between;
      gap: 10px;
      align-items: center;
      margin-bottom: 8px;
    }}
    .source-popover-fields {{
      display: grid;
      grid-template-columns: minmax(130px, 170px) minmax(220px, 1fr) max-content;
      gap: 8px;
      align-items: end;
    }}
    .source-popover-fields label {{
      display: grid;
      gap: 4px;
      color: var(--muted);
      font-size: 12px;
    }}
    .source-popover-fields input,
    .source-popover-fields select {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 6px 8px;
      color: var(--text);
      background: var(--panel);
      font: inherit;
    }}
    @media (max-width: 760px) {{
      .source-list-row, .source-popover-fields {{ grid-template-columns: 1fr; }}
      .source-platform-icon {{ justify-self: start; }}
    }}
    .streamer-stat-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(145px, 1fr));
      gap: 8px 12px;
      margin: 10px 0 14px;
    }}
    .streamer-stat-grid div {{ min-width: 0; overflow-wrap: anywhere; }}
    .streamer-settings-panel {{ margin-bottom: 14px; }}
    .streamer-settings-panel[hidden] {{ display: none; }}
    .streamer-body-grid {{
      display: grid;
      grid-template-columns: minmax(260px, 1fr);
      gap: 14px;
      align-items: start;
      margin-bottom: 14px;
    }}
    @media (max-width: 760px) {{
      .streamer-head, .streamer-body-grid {{ display: grid; }}
    }}
    .streamer-streams {{ display: grid; gap: 8px; }}
    .streamer-streams .stream {{ margin-top: 0; }}
    .stream-browser {{ display: grid; gap: 8px; }}
    .stream-browser-controls {{
      display: grid;
      grid-template-columns: minmax(135px, 0.7fr) minmax(220px, 1.4fr) repeat(2, minmax(140px, 0.7fr)) minmax(120px, 0.55fr);
      gap: 8px;
      align-items: end;
      padding: 10px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
    }}
    .stream-browser-controls label {{
      display: grid;
      gap: 4px;
      color: var(--muted);
      font-size: 12px;
      min-width: 0;
    }}
    .stream-browser-controls input,
    .stream-browser-controls select {{
      width: 100%;
      min-width: 0;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 6px 8px;
      color: var(--text);
      background: var(--panel);
      font: inherit;
    }}
    .stream-browser-footer {{
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      align-items: center;
      gap: 8px;
    }}
    .stream-browser-pager {{ display: flex; align-items: center; gap: 6px; }}
    .stream-browser-list {{ display: grid; gap: 8px; }}
    @media (max-width: 980px) {{
      .stream-browser-controls {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    }}
    @media (max-width: 640px) {{
      .stream-browser-controls {{ grid-template-columns: 1fr; }}
    }}
    .streamer-jobs {{
      display: grid;
      gap: 8px;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
    }}
    .streamer-job-page {{ display: grid; }}
    .streamer-job-page[hidden] {{ display: none; }}
    .streamer-job-pager {{
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 6px;
      padding-top: 4px;
      border-top: 1px solid var(--line);
    }}
    .streamer-job-page-button[aria-current="page"] {{
      color: var(--text);
      border-color: color-mix(in srgb, var(--active), transparent 45%);
      background: var(--panel-strong);
      font-weight: 650;
    }}
    .streamer-job-row {{
      display: grid;
      grid-template-columns: max-content minmax(0, 1fr);
      gap: 8px 10px;
      align-items: start;
      padding: 8px 0;
    }}
    .streamer-job-row + .streamer-job-row {{ border-top: 1px solid var(--line); }}
    .streamer-job-body {{
      min-width: 0;
      display: grid;
      gap: 5px;
    }}
    .streamer-job-heading {{
      min-width: 0;
      display: flex;
      gap: 6px 10px;
      align-items: baseline;
      flex-wrap: wrap;
    }}
    .streamer-job-kind {{ font-weight: 650; color: var(--text); }}
    .streamer-job-phase, .streamer-job-detail {{ min-width: 0; overflow-wrap: anywhere; }}
    .streamer-job-meta {{ display: flex; flex-wrap: wrap; gap: 4px 10px; min-width: 0; }}
    .streamer-job-meta span {{ min-width: 0; overflow-wrap: anywhere; }}
    .streamer-job-details {{ min-width: 0; }}
    .streamer-job-details summary {{ cursor: pointer; width: fit-content; color: var(--muted); }}
    .streamer-job-details-grid {{
      display: grid;
      grid-template-columns: max-content minmax(0, 1fr);
      gap: 4px 10px;
      margin: 6px 0 0;
    }}
    .streamer-job-details-grid dt {{ color: var(--muted); }}
    .streamer-job-details-grid dd {{ margin: 0; min-width: 0; overflow-wrap: anywhere; }}
    .streamer-job-row .job-progress {{ width: min(420px, 100%); min-width: 0; }}
    .stream-head {{
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: start;
    }}
    .stream-title-block {{
      display: grid;
      grid-template-columns: max-content minmax(0, 1fr);
      gap: 8px;
      align-items: start;
      min-width: 0;
    }}
    .stream-title-text {{ min-width: 0; }}
    .stream-actions {{
      display: flex;
      flex-wrap: wrap;
      justify-content: flex-end;
      align-items: center;
      gap: 8px;
    }}
    .stream-toggle, .streamer-toggle, .streamer-settings-toggle {{
      color: inherit;
      cursor: pointer;
      font: inherit;
    }}
    .stream.collapsed .stream-body {{ display: none; }}
    .stream-detail-tabs {{ margin-top: 12px; }}
    .stream-tab-radio {{
      position: absolute;
      opacity: 0;
      pointer-events: none;
    }}
    .stream-tab-labels {{
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      border-bottom: 1px solid var(--line);
      margin-bottom: 10px;
    }}
    .stream-tab-labels label {{
      border: 1px solid var(--line);
      border-bottom: 0;
      border-radius: 6px 6px 0 0;
      padding: 6px 10px;
      color: var(--muted);
      background: var(--panel);
      cursor: pointer;
    }}
    .stream-tab-panel {{ display: none; }}
    .stream-tab-files-toggle:checked ~ .stream-tab-labels .stream-tab-files-label,
    .stream-tab-events-toggle:checked ~ .stream-tab-labels .stream-tab-events-label,
    .stream-tab-powerchat-toggle:checked ~ .stream-tab-labels .stream-tab-powerchat-label,
    .stream-tab-speakers-toggle:checked ~ .stream-tab-labels .stream-tab-speakers-label,
    .stream-tab-log-toggle:checked ~ .stream-tab-labels .stream-tab-log-label,
    .stream-tab-jobs-toggle:checked ~ .stream-tab-labels .stream-tab-jobs-label {{
      color: var(--text);
      background: var(--panel-strong);
      font-weight: 650;
    }}
    .stream-tab-files-toggle:checked ~ .stream-tab-panels .stream-tab-files,
    .stream-tab-events-toggle:checked ~ .stream-tab-panels .stream-tab-events,
    .stream-tab-powerchat-toggle:checked ~ .stream-tab-panels .stream-tab-powerchat,
    .stream-tab-speakers-toggle:checked ~ .stream-tab-panels .stream-tab-speakers,
    .stream-tab-log-toggle:checked ~ .stream-tab-panels .stream-tab-log,
    .stream-tab-jobs-toggle:checked ~ .stream-tab-panels .stream-tab-jobs {{ display: block; }}
    .content-events {{ display: grid; gap: 8px; }}
    .powerchat-events {{ display: grid; gap: 8px; }}
    .stream-powerchat-dashboard {{ display: grid; gap: 12px; }}
    .powerchat-event {{
      display: grid;
      grid-template-columns: max-content minmax(0, 1fr) minmax(160px, 0.35fr);
      gap: 12px;
      align-items: start;
      border: 1px solid var(--line);
      border-left: 4px solid var(--active);
      border-radius: 6px;
      padding: 10px;
      background: var(--panel);
    }}
    .powerchat-event.unknown {{ border-left-color: var(--muted); }}
    .powerchat-event-amount {{ font-weight: 700; }}
    .powerchat-dashboard {{ display: grid; gap: 12px; }}
    .powerchat-summary-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 8px; }}
    .powerchat-summary-card {{ border: 1px solid var(--line); border-radius: 6px; padding: 10px; background: #fff; }}
    .powerchat-summary-card strong {{ display: block; font-size: 18px; }}
    .powerchat-dashboard-controls {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 8px; align-items: end; }}
    .powerchat-dashboard-controls label {{ display: grid; gap: 4px; color: var(--muted); font-size: 12px; }}
    .powerchat-dashboard-controls input,
    .powerchat-dashboard-controls select {{ width: 100%; }}
    .powerchat-export-actions {{ display: flex; gap: 8px; align-items: center; justify-content: flex-end; flex-wrap: wrap; }}
    .powerchat-dashboard-section {{ display: grid; gap: 8px; }}
    .powerchat-dashboard-section h3,
    .powerchat-dashboard-section h4 {{ margin: 0; }}
    .powerchat-dashboard table {{ width: 100%; border-collapse: collapse; min-width: 560px; }}
    .powerchat-dashboard th,
    .powerchat-dashboard td {{ border-top: 1px solid var(--line); padding: 8px 10px; text-align: left; vertical-align: top; }}
    .powerchat-dashboard th {{ color: var(--muted); font-size: 12px; font-weight: 650; white-space: nowrap; }}
    .powerchat-dashboard tbody tr:hover td {{ background: var(--panel-strong); }}
    .powerchat-streamer-list {{ display: grid; gap: 10px; }}
    .powerchat-streamer-card {{ border: 1px solid var(--line); border-radius: 6px; background: #fff; overflow: hidden; }}
    .powerchat-streamer-card > summary {{ display: grid; grid-template-columns: minmax(220px, 1fr) minmax(160px, 0.6fr) minmax(120px, 0.45fr) max-content max-content; gap: 10px; align-items: center; padding: 10px; cursor: pointer; }}
    .powerchat-streamer-card > summary span {{ color: var(--muted); }}
    .powerchat-streamer-card-body {{ display: grid; gap: 10px; padding: 10px; border-top: 1px solid var(--line); background: var(--panel); }}
    .powerchat-streamer-card-body .powerchat-dashboard-section,
    .powerchat-overall-body .powerchat-dashboard-section {{ border: 1px solid var(--line); border-radius: 6px; padding: 10px; background: #fff; }}
    .powerchat-overall-breakdown {{ border: 1px solid var(--line); border-radius: 6px; background: #fff; overflow: hidden; }}
    .powerchat-overall-breakdown > summary {{ display: flex; gap: 10px; align-items: center; padding: 10px; cursor: pointer; }}
    .powerchat-overall-body {{ display: grid; gap: 10px; padding: 10px; border-top: 1px solid var(--line); background: var(--panel); }}
    .powerchat-ledger-footer {{ display: flex; justify-content: space-between; gap: 8px; align-items: center; flex-wrap: wrap; }}
    .content-event {{
      display: grid;
      grid-template-columns: max-content minmax(0, 1fr) minmax(180px, 0.4fr);
      gap: 12px;
      align-items: start;
      border: 1px solid var(--line);
      border-left: 4px solid var(--active);
      border-radius: 7px;
      padding: 9px 10px;
      background: var(--panel-strong);
    }}
    .content-event.warning {{ border-left-color: var(--warn); }}
    .content-event.error {{ border-left-color: var(--bad); }}
    .content-event-time {{
      display: grid;
      gap: 2px;
      font-weight: 700;
      font-variant-numeric: tabular-nums;
      white-space: nowrap;
    }}
    .content-event-end {{ color: var(--muted); font-size: 0.82rem; font-weight: 500; }}
    .content-event-main {{ min-width: 0; }}
    .content-event-main strong {{ margin-right: 8px; }}
    .content-event-meta {{ display: grid; gap: 4px; color: var(--muted); min-width: 0; }}
    .content-event-meta b {{ color: var(--text); font-weight: 650; }}
    .stream-events {{
      display: grid;
      gap: 8px;
    }}
    .stream-event {{
      display: grid;
      grid-template-columns: minmax(145px, max-content) max-content max-content minmax(0, 1fr);
      gap: 10px;
      align-items: start;
      border: 1px solid var(--line);
      border-left-width: 4px;
      border-radius: 7px;
      padding: 8px 10px;
      background: var(--panel-strong);
    }}
    .stream-event.debug {{ border-left-color: var(--muted); }}
    .stream-event.info {{ border-left-color: var(--active); }}
    .stream-event.warning {{ border-left-color: var(--warn); }}
    .stream-event.error {{ border-left-color: var(--bad); }}
    .stream-event-time {{ color: var(--muted); white-space: nowrap; }}
    .stream-event-level, .stream-event-segment {{
      border: 1px solid var(--line);
      border-radius: 5px;
      padding: 1px 6px;
      color: var(--muted);
      background: var(--panel);
      font-size: 0.78rem;
      line-height: 1.35;
      white-space: nowrap;
    }}
    .stream-event.warning .stream-event-level {{ color: var(--warn); }}
    .stream-event.error .stream-event-level {{ color: var(--bad); }}
    .stream-event-message {{
      min-width: 0;
      line-height: 1.45;
      overflow-wrap: anywhere;
    }}
    @media (max-width: 820px) {{
      .stream-event, .content-event {{ grid-template-columns: 1fr; }}
      .stream-event-time, .stream-event-level, .stream-event-segment, .content-event-time {{ justify-self: start; }}
    }}
    .stream-job-list {{ display: grid; gap: 8px; }}
    .title {{ font-weight: 650; overflow-wrap: anywhere; }}
    .badge {{
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 3px 8px;
      white-space: nowrap;
      color: var(--muted);
      background: var(--panel-strong);
    }}
    .badge.downloading, .badge.running, .badge.done {{ color: var(--active); border-color: color-mix(in srgb, var(--active), transparent 55%); }}
    .badge.checking_after_exit, .badge.waiting_retry, .badge.interrupted, .badge.queued {{ color: var(--warn); }}
    .badge.failed {{ color: var(--bad); border-color: color-mix(in srgb, var(--bad), transparent 55%); }}
    .badge.ended {{ color: var(--muted); }}
    .signals {{
      margin-top: 8px;
      color: var(--warn);
      font-weight: 600;
    }}
    .meta {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
      gap: 6px 14px;
      margin: 10px 0;
    }}
    .meta div {{ min-width: 0; overflow-wrap: anywhere; }}
    .wide {{ grid-column: 1 / -1; }}
    .table-wrap {{ overflow-x: auto; }}
    .files, .channels, .jobs, .logs, .config-table, .voice-table, .speaker-table {{
      width: 100%;
      border-collapse: collapse;
      margin-top: 8px;
      min-width: 860px;
    }}
    .channels {{ min-width: 980px; }}
    .jobs {{ min-width: 1180px; }}
    .logs {{ min-width: 860px; }}
    .config-table {{ table-layout: fixed; }}
    .config-table th:first-child, .config-table td:first-child {{ width: 32%; }}
    .config-table th:last-child, .config-table td:last-child {{ width: 68%; }}
    .files th, .files td, .channels th, .channels td, .jobs th, .jobs td, .logs th, .logs td,
    .config-table th, .config-table td, .voice-table th, .voice-table td,
    .speaker-table th, .speaker-table td {{
      border-top: 1px solid var(--line);
      padding: 7px 6px;
      text-align: left;
      vertical-align: top;
    }}
    .files th:last-child, .files td:last-child,
    .channels th:last-child, .channels td:last-child {{ text-align: right; }}
    .logs th:last-child, .logs td:last-child {{ text-align: left; }}
    .job-progress {{
      display: grid;
      grid-template-columns: minmax(90px, 1fr) max-content;
      align-items: center;
      gap: 6px;
      min-width: 150px;
    }}
    .job-progress progress {{ width: 100%; height: 10px; }}
    .file-name {{ overflow-wrap: anywhere; }}
    .log-message {{
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      font-family: ui-monospace, SFMono-Regular, Consolas, "Liberation Mono", monospace;
      font-size: 12px;
    }}
    .config-value {{
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      font-family: ui-monospace, SFMono-Regular, Consolas, "Liberation Mono", monospace;
      font-size: 12px;
    }}
    .level-ERROR, .level-CRITICAL {{ color: var(--bad); font-weight: 650; }}
    .level-WARNING {{ color: var(--warn); font-weight: 650; }}
    .level-INFO {{ color: var(--active); }}
    .empty {{ padding: 18px; color: var(--muted); }}
    .kind {{ text-transform: capitalize; }}
    .download {{
      display: inline-block;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 2px 7px;
      text-decoration: none;
      background: var(--panel-strong);
    }}
    .actions {{
      display: flex;
      flex-wrap: wrap;
      justify-content: flex-end;
      align-items: center;
      gap: 6px;
    }}
    .inline-form {{ display: inline; margin: 0; }}
    .action-button {{
      color: inherit;
      cursor: pointer;
      font: inherit;
    }}
    .danger-action {{
      border-color: #fecaca;
      color: #991b1b;
      background: #fff5f5;
    }}
    .watermark-form {{
      display: inline-flex;
      align-items: center;
      gap: 4px;
    }}
    .recipient-input {{
      max-width: 150px;
      min-width: 110px;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 3px 6px;
      color: var(--text);
      background: var(--panel);
      font: inherit;
    }}
    .voice-form {{
      display: flex;
      flex-wrap: wrap;
      align-items: end;
      gap: 8px;
      margin: 8px 0 12px;
    }}
    .voice-form label {{
      display: grid;
      gap: 3px;
      color: var(--muted);
      font-size: 12px;
    }}
    .voice-form select, .voice-form input {{
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 4px 6px;
      color: var(--text);
      background: var(--panel);
      font: inherit;
    }}
    .small-input {{ width: 74px; }}
    .env-input {{ width: 150px; }}
    .voice-table {{ min-width: 1040px; table-layout: fixed; }}
    .voice-table th {{ white-space: nowrap; }}
    .voice-table th:nth-child(1), .voice-table td:nth-child(1) {{ width: 22%; }}
    .voice-table th:nth-child(2), .voice-table td:nth-child(2) {{ width: 20%; }}
    .voice-table th:nth-child(3), .voice-table td:nth-child(3) {{ width: 18%; }}
    .voice-table th:nth-child(4), .voice-table td:nth-child(4) {{ width: 40%; }}
    .voice-channel-form {{
      flex-wrap: nowrap;
      align-items: center;
      margin: 0;
      gap: 6px;
    }}
    .voice-channel-form select {{ flex: 0 0 118px; width: 118px; }}
    .voice-channel-form .small-input {{ flex: 0 0 70px; width: 70px; }}
    .voice-channel-form .env-input {{ flex: 1 0 140px; min-width: 140px; }}
    .voice-channel-form .action-button {{ flex: 0 0 auto; }}
    .speaker-table {{ min-width: 1040px; table-layout: fixed; }}
    .speaker-table th {{ white-space: nowrap; }}
    .speaker-table th:nth-child(1), .speaker-table td:nth-child(1) {{ width: 18%; }}
    .speaker-table th:nth-child(2), .speaker-table td:nth-child(2) {{ width: 18%; }}
    .speaker-table th:nth-child(3), .speaker-table td:nth-child(3) {{ width: 20%; }}
    .speaker-table th:nth-child(4), .speaker-table td:nth-child(4) {{ width: 44%; }}
    .speaker-label-form {{ display: grid; gap: 7px; margin: 0; }}
    .speaker-label-pair {{
      display: grid;
      grid-template-columns: 130px minmax(170px, 1fr);
      gap: 6px;
      align-items: center;
    }}
    .speaker-label-pair input {{
      min-width: 0;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 4px 6px;
      color: var(--text);
      background: var(--panel);
      font: inherit;
    }}
    .speaker-label-pair input[readonly] {{ background: var(--panel-strong); }}
    .speaker-label-actions {{ display: flex; justify-content: flex-end; }}
    .settings-form {{
      display: grid;
      gap: 14px;
      margin-top: 8px;
    }}
    .settings-fieldset {{
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
      min-width: 0;
    }}
    .settings-fieldset legend {{
      padding: 0 6px;
      font-weight: 650;
    }}
    .settings-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 10px 12px;
    }}
    .settings-field {{
      display: grid;
      gap: 4px;
      min-width: 0;
      color: var(--muted);
      font-size: 12px;
    }}
    .settings-field.wide {{ grid-column: 1 / -1; }}
    .settings-field input, .settings-field select, .settings-field textarea {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 6px 8px;
      color: var(--text);
      background: var(--panel);
      font: inherit;
    }}
    .settings-field.checkbox-field span {{ display: flex; align-items: center; gap: 8px; min-height: 34px; color: var(--text); }}
    .settings-field.checkbox-field input {{ width: auto; padding: 0; }}
    .settings-field textarea {{
      min-height: 72px;
      resize: vertical;
      font-family: ui-monospace, SFMono-Regular, Consolas, "Liberation Mono", monospace;
      font-size: 12px;
    }}
    .settings-actions {{
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 10px;
    }}
    .streamer-groups {{
      display: grid;
      gap: 12px;
      margin-top: 8px;
    }}
    .streamer-form {{
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 10px 12px;
    }}
    .streamer-form .wide, .streamer-form .settings-actions {{ grid-column: 1 / -1; }}
    .streamer-meta {{ color: var(--muted); align-self: end; }}
    .manual-vod-panel, .vod-download-box {{
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
      margin-top: 10px;
      background: var(--panel);
    }}
    .manual-vod-panel h4 {{ margin: 0 0 8px; font-size: 13px; }}
    .vod-download-box summary {{ cursor: pointer; width: fit-content; }}
    .vod-download-form {{
      display: grid;
      grid-template-columns: minmax(220px, 1fr) max-content;
      gap: 8px;
      align-items: end;
      margin-top: 8px;
    }}
    .vod-download-form .wide {{ grid-column: auto; }}
    @media (max-width: 760px) {{
      .vod-download-form {{ grid-template-columns: 1fr; }}
      .vod-download-form .wide {{ grid-column: 1 / -1; }}
    }}
    .streamer-wizard {{
      width: min(760px, calc(100vw - 32px));
      max-height: min(760px, calc(100vh - 32px));
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 0;
      color: var(--text);
      background: var(--panel);
    }}
    .streamer-wizard::backdrop {{ background: rgba(0, 0, 0, 0.42); }}
    .streamer-wizard-form {{ display: grid; gap: 0; }}
    .wizard-head, .wizard-footer {{
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      padding: 14px;
      border-bottom: 1px solid var(--line);
    }}
    .wizard-footer {{ border-top: 1px solid var(--line); border-bottom: 0; }}
    .wizard-steps {{
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      padding: 12px 14px 0;
    }}
    .wizard-step-tab {{
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 3px 9px;
      color: var(--muted);
      background: var(--panel-strong);
    }}
    .wizard-step-tab.active {{ color: var(--text); font-weight: 650; }}
    .wizard-step {{ padding: 14px; }}
    .wizard-step[hidden] {{ display: none; }}
    .wizard-speaker-rows {{ display: grid; gap: 7px; margin-bottom: 8px; }}
    .voice-manager {{
      width: min(980px, calc(100vw - 28px));
      max-height: min(860px, calc(100vh - 28px));
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 0;
      color: var(--text);
      background: var(--panel);
    }}
    .voice-manager::backdrop {{ background: rgba(0, 0, 0, 0.42); }}
    .voice-manager-head {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 12px 14px;
      border-bottom: 1px solid var(--line);
    }}
    .voice-manager-head h2 {{ margin: 0; font-size: 1rem; }}
    .voice-manager-actions {{ display: flex; align-items: center; gap: 8px; }}
    .voice-manager-note {{ padding: 10px 14px 0; }}
    .voice-settings {{
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
      overflow: visible;
    }}
    .voice-settings .voice-manager-head {{ padding: 12px; }}
    .voice-settings .voice-manager-note {{ padding: 10px 12px 0; }}
    .voice-settings .voice-tabs {{ padding: 12px; }}
    .voice-add-menu {{ position: relative; }}
    .voice-add-menu > summary {{ list-style: none; cursor: pointer; }}
    .voice-add-menu > summary::-webkit-details-marker {{ display: none; }}
    .voice-add-popover {{
      position: absolute;
      right: 0;
      top: calc(100% + 8px);
      z-index: 5;
      width: min(560px, calc(100vw - 56px));
      padding: 10px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
      box-shadow: 0 18px 45px rgba(15, 23, 42, 0.18);
    }}
    .voice-tabs {{
      display: grid;
      grid-template-columns: repeat(2, max-content) 1fr;
      gap: 0 8px;
      padding: 12px 14px 14px;
    }}
    .voice-tabs > input {{ position: absolute; opacity: 0; pointer-events: none; }}
    .voice-tabs > label {{
      grid-row: 1;
      padding: 7px 10px;
      border: 1px solid var(--line);
      border-bottom: 0;
      border-radius: 7px 7px 0 0;
      background: var(--panel-strong);
      cursor: pointer;
      font-size: 0.9rem;
    }}
    .voice-tabs > section {{
      display: none;
      grid-column: 1 / -1;
      grid-row: 2;
      border: 1px solid var(--line);
      padding: 12px;
      min-height: 160px;
      overflow: auto;
    }}
    .voice-tabs > input:checked + label {{ background: var(--panel); font-weight: 650; }}
    .voice-tabs > input:checked + label + section {{ display: grid; gap: 10px; }}
    .voice-profile-form, .voice-sample-row, .voice-match-row {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 12px;
      align-items: end;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
      margin: 0;
      background: var(--panel-strong);
    }}
    .voice-profile-title, .voice-task-title, .voice-profile-form .wide, .voice-sample-row .file-name, .voice-match-row .file-name {{ grid-column: 1 / -1; }}
    .voice-profile-form label, .voice-sample-row label, .voice-match-row label {{
      display: grid;
      gap: 5px;
      min-width: 0;
      color: var(--muted);
      font-size: 12px;
      font-weight: 650;
    }}
    .voice-profile-form label > :where(input, select, textarea),
    .voice-sample-row label > :where(input, select, textarea),
    .voice-match-row label > :where(input, select, textarea) {{
      width: 100%;
      min-width: 0;
      box-sizing: border-box;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 6px 8px;
      color: var(--text);
      background: var(--panel);
      font: inherit;
      font-weight: 400;
    }}
    .voice-profile-form label > input[type="file"],
    .voice-sample-row label > input[type="file"] {{
      padding: 5px;
    }}
    .voice-profile-title {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      color: var(--text);
    }}
    .voice-advanced {{
      border: 1px dashed var(--line);
      border-radius: 7px;
      padding: 8px 10px;
      background: var(--panel);
    }}
    .voice-advanced > summary {{
      cursor: pointer;
      color: var(--muted);
      font-size: 12px;
      font-weight: 650;
      list-style-position: inside;
    }}
    .voice-advanced label {{ margin-top: 8px; }}
    .voice-profile-form .settings-actions {{
      grid-column: 1 / -1;
      margin-top: 2px;
    }}
    .voice-task-title {{
      margin: 0 0 2px;
      font-size: 0.95rem;
    }}
    .voice-task-form {{
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      align-items: end;
    }}
    .event-rules-form {{ display: grid; gap: 12px; margin-top: 10px; }}
    .event-settings-box {{
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
      background: var(--panel-strong);
      min-width: 0;
    }}
    .event-settings-box > legend {{ padding: 0 6px; font-weight: 650; }}
    .event-rule-toolbar {{
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      gap: 10px;
      align-items: center;
      margin-bottom: 10px;
    }}
    .event-rule-list {{ display: grid; gap: 8px; }}
    .event-rule-card {{
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
      min-width: 0;
      overflow: hidden;
    }}
    .event-rule-card[open] {{ box-shadow: 0 6px 18px rgba(15, 23, 42, 0.06); }}
    .event-rule-card > summary {{
      display: grid;
      grid-template-columns: minmax(160px, 1fr) minmax(0, 1.3fr) max-content;
      gap: 10px;
      align-items: center;
      padding: 9px 10px;
      cursor: pointer;
      list-style: none;
    }}
    .event-rule-card > summary::-webkit-details-marker {{ display: none; }}
    .event-rule-title {{ font-weight: 650; min-width: 0; overflow-wrap: anywhere; }}
    .event-rule-summary {{ color: var(--muted); min-width: 0; overflow-wrap: anywhere; }}
    .event-rule-action {{ color: var(--active); font-weight: 650; white-space: nowrap; }}
    .event-rule-card.disabled .event-rule-title {{ color: var(--muted); text-decoration: line-through; }}
    .event-rule-empty {{ color: var(--muted); padding: 8px 0; }}
    .event-rule-add {{ border-style: dashed; }}
    .event-rule-add > summary {{ grid-template-columns: 1fr max-content; }}
    .event-rule-editor {{ display: grid; gap: 10px; padding: 0 10px 10px; }}
    .event-rule-primary, .event-rule-criteria {{
      display: grid;
      gap: 10px;
      align-items: end;
    }}
    .event-rule-primary {{ grid-template-columns: minmax(220px, 1fr) minmax(95px, 0.35fr) minmax(120px, 0.45fr); }}
    .event-rule-criteria {{ grid-template-columns: repeat(2, minmax(210px, 1fr)) repeat(3, minmax(115px, 0.45fr)); }}
    .event-delete-option {{
      display: flex;
      gap: 8px;
      align-items: center;
      color: var(--bad);
      font-weight: 650;
    }}
    .event-rule-card .settings-field input,
    .event-rule-card .settings-field select,
    .event-settings-box .settings-field input,
    .event-settings-box .settings-field select {{ box-sizing: border-box; }}
    .streamer-settings-tabs {{ display: grid; gap: 0; margin-top: 10px; }}
    .streamer-settings-tabs > input {{ position: absolute; opacity: 0; pointer-events: none; }}
    .streamer-settings-tab-labels {{ display: flex; flex-wrap: wrap; gap: 6px; border-bottom: 1px solid var(--line); }}
    .streamer-settings-tab-labels label {{
      border: 1px solid var(--line);
      border-bottom: 0;
      border-radius: 6px 6px 0 0;
      padding: 6px 10px;
      color: var(--muted);
      background: var(--panel);
      cursor: pointer;
    }}
    .streamer-settings-tabs .streamer-settings-panel {{ display: none; padding-top: 12px; }}
    .streamer-settings-main-toggle:checked ~ .streamer-settings-tab-labels .streamer-settings-main-label,
    .streamer-settings-events-toggle:checked ~ .streamer-settings-tab-labels .streamer-settings-events-label,
    .streamer-settings-voices-toggle:checked ~ .streamer-settings-tab-labels .streamer-settings-voices-label {{
      color: var(--text);
      background: var(--panel-strong);
      font-weight: 650;
    }}
    .streamer-settings-main-toggle:checked ~ .streamer-settings-panels .streamer-settings-main,
    .streamer-settings-events-toggle:checked ~ .streamer-settings-panels .streamer-settings-events,
    .streamer-settings-voices-toggle:checked ~ .streamer-settings-panels .streamer-settings-voices {{ display: block; }}
    .streamer-event-summary-title {{ font-weight: 650; }}
    .compact-grid {{ grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); }}
    @media (max-width: 980px) {{
      .event-rule-primary, .event-rule-criteria, .event-rule-card > summary {{ grid-template-columns: 1fr; }}
      .event-rule-action {{ justify-self: start; }}
    }}
    .voice-profile-form textarea {{
      min-height: 68px;
      resize: vertical;
      font-family: ui-monospace, SFMono-Regular, Consolas, "Liberation Mono", monospace;
      font-size: 12px;
    }}
    .voice-list {{ display: grid; gap: 8px; }}
    .voice-card {{ border: 1px solid var(--line); border-radius: 8px; background: var(--panel); overflow: hidden; }}
    .voice-card[open] {{ box-shadow: 0 6px 18px rgba(15, 23, 42, 0.06); }}
    .voice-card > summary {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) repeat(3, max-content);
      gap: 8px;
      align-items: center;
      padding: 10px;
      cursor: pointer;
      list-style: none;
    }}
    .voice-card[open] > summary {{
      border-bottom: 1px solid var(--line);
      background: var(--panel-strong);
    }}
    .voice-card > summary::-webkit-details-marker {{ display: none; }}
    .voice-card-name {{ font-weight: 650; overflow-wrap: anywhere; }}
    .voice-card-action {{
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 2px 8px;
      color: var(--muted);
      background: var(--panel);
      font-size: 12px;
    }}
    .voice-card-body {{ display: grid; gap: 10px; padding: 10px; }}
    .voice-sample-form {{ grid-template-columns: minmax(220px, 1fr) max-content; }}
    .voice-sample-form button {{ justify-self: end; }}
    .voice-match-row {{ grid-template-columns: max-content 1fr repeat(2, max-content); align-items: center; }}
    @media (max-width: 760px) {{
      .voice-manager-head {{ align-items: flex-start; }}
      .voice-manager-actions {{ flex-wrap: wrap; justify-content: flex-end; }}
      .voice-card > summary {{ grid-template-columns: 1fr; justify-items: start; }}
      .voice-sample-form {{ grid-template-columns: 1fr; }}
    }}
    .upload-form {{
      display: grid;
      gap: 8px;
    }}
    .upload-form input[type="file"] {{
      max-width: 100%;
    }}
    .action-note {{
      color: var(--muted);
      font-size: 12px;
      white-space: nowrap;
    }}
  </style>
</head>
<body data-stream-revision="{escape(snapshot.stream_revision, quote=True)}" data-job-revision="{escape(snapshot.job_revision, quote=True)}">
  <header>
    <h1>ONLYSAVEmeVODS Status</h1>
    <div class="summary">
      <div class="metric"><strong id="metric-total">{total}</strong><span>Total</span></div>
      <div class="metric"><strong id="metric-downloading">{active}</strong><span>Downloading</span></div>
      <div class="metric"><strong id="metric-checking">{checking}</strong><span>Checking</span></div>
      <div class="metric"><strong id="metric-attention">{len(attention_streams)}</strong><span>Attention</span></div>
      <div class="metric"><strong id="metric-streamers">{len(snapshot.streamer_stats)}</strong><span>Streamers</span></div>
      <div class="metric"><strong id="metric-jobs">{active_job_count(snapshot.jobs)}</strong><span>Active Jobs</span></div>
      <div class="metric"><strong id="metric-logs">{len(snapshot.recent_logs)}</strong><span>Logs</span></div>
      <div class="metric"><strong id="metric-storage">{escape(format_bytes(snapshot.total_bytes))}</strong><span>Storage</span></div>
      <div class="metric"><strong id="metric-partial">{escape(format_bytes(snapshot.part_bytes))}</strong><span>Partial</span></div>
    </div>
    <div class="toolbar">
      <span id="updated-at">Updated {escape(generated)}</span>
      <a href="/status.json">JSON</a>
      <span id="refresh-state">Refresh 15s</span>
    </div>
    {status_counts}
  </header>
  <main>
    <input class="tab-radio" type="radio" id="tab-streamers" name="dashboard-tab" checked>
    <input class="tab-radio" type="radio" id="tab-powerchat" name="dashboard-tab">
    <input class="tab-radio" type="radio" id="tab-jobs" name="dashboard-tab">
    <input class="tab-radio" type="radio" id="tab-logs" name="dashboard-tab">
    <input class="tab-radio" type="radio" id="tab-about" name="dashboard-tab">
    <input class="tab-radio" type="radio" id="tab-config" name="dashboard-tab">
    <div class="tabs">
      <label for="tab-streamers">Streamers</label>
      <label for="tab-powerchat">Powerchat</label>
      <label for="tab-jobs">Jobs</label>
      <label for="tab-logs">Logs</label>
      <label for="tab-about">About</label>
      <label for="tab-config">Config</label>
    </div>
    <section class="tab-panel streamers-panel">
      <div class="dashboard-grid">
        <section class="panel">
          <h2>Storage</h2>
          <dl>
            <dt>Final</dt><dd id="storage-final">{escape(format_bytes(snapshot.final_bytes))}</dd>
            <dt>Partial</dt><dd id="storage-part">{escape(format_bytes(snapshot.part_bytes))}</dd>
            <dt>Chat</dt><dd id="storage-chat">{escape(format_bytes(snapshot.chat_bytes))}</dd>
            <dt>Fragments</dt><dd id="storage-fragment">{escape(format_bytes(snapshot.fragment_bytes))}</dd>
            <dt>State files</dt><dd id="storage-state">{escape(format_bytes(snapshot.state_bytes))}</dd>
            <dt>Temporary</dt><dd id="storage-temporary">{escape(format_bytes(snapshot.temporary_bytes))}</dd>
          </dl>
        </section>
        <section class="panel">
          <h2>Runtime</h2>
          <dl>
            <dt>Download dir</dt><dd id="runtime-download-dir">{escape(snapshot.download_dir)}</dd>
            <dt>State DB</dt><dd id="runtime-state-db">{escape(snapshot.state_db)}</dd>
            <dt>Stream limit</dt><dd id="runtime-stream-limit">{snapshot.stream_limit}</dd>
            <dt>File limit</dt><dd>{FILE_LIMIT_PER_STREAM} per stream</dd>
          </dl>
        </section>
        {watermark_detection}
      </div>
      {streamer_toolbar}
      {streamer_wizard}
      <div class="streamer-list" id="streamer-list">{streamer_cards}</div>
    </section>
    <section class="tab-panel powerchat-panel">
      {powerchat_dashboard}
    </section>
    <section class="tab-panel jobs-panel">
      <section class="panel">
        <h2>Jobs</h2>
        <div class="file-meta">Showing up to {snapshot.job_limit} dashboard jobs and watermark copy jobs.</div>
        <div class="table-wrap">
          <table class="jobs">
            <thead><tr><th>Status</th><th>Progress</th><th>Job</th><th>Phase</th><th>Video</th><th>Item</th><th>Detail</th><th>Started</th><th>Updated</th><th>Duration</th><th>Message</th></tr></thead>
            <tbody id="job-rows">{job_rows}</tbody>
          </table>
        </div>
      </section>
    </section>
    <section class="tab-panel logs-panel">
      <section class="panel">
        <h2>Recent Logs</h2>
        <div class="file-meta">Showing up to {snapshot.log_limit} in-process log entries since this service started.</div>
        <div class="table-wrap">
          <table class="logs">
            <thead><tr><th>Time</th><th>Level</th><th>Logger</th><th>Message</th></tr></thead>
            <tbody id="log-rows">{log_rows}</tbody>
          </table>
        </div>
      </section>
    </section>
    <section class="tab-panel about-panel">
      {about_panel}
    </section>
    <section class="tab-panel config-panel">
      <h2>Current Configuration</h2>
      <div class="file-meta">Sensitive yt-dlp arguments are redacted before display.</div>
      {app_config_form}
      {content_event_rules_panel}
      {voice_detection_panel}
      {speaker_labels_panel}
      <div class="config-stack" id="config-sections">
        {config_sections}
      </div>
    </section>
  </main>
  <script type="application/json" id="powerchat-stats-json">{powerchat_stats_json}</script>
  {script}
</body>
</html>
"""
    log_perf(
        "render-html",
        perf_elapsed(started_at),
        WEB_SLOW_STEP_SECONDS,
        streams=len(snapshot.streams),
        files=sum(stream.file_count for stream in snapshot.streams),
        bytes=len(body.encode("utf-8")),
    )
    return body


def dashboard_script() -> str:
    return """<script>
(() => {
  const tabKey = "onlysavemevods.dashboardTab";
  const collapsedKey = "onlysavemevods.collapsedStreams";
  const expandedKey = "onlysavemevods.expandedStreams";
  const collapsedStreamerKey = "onlysavemevods.collapsedStreamers";
  const expandedStreamerKey = "onlysavemevods.expandedStreamers";
  const openStreamerSettingsKey = "onlysavemevods.openStreamerSettings";
  const streamTabKey = "onlysavemevods.streamTabs";
  const streamerStreamFilterKey = "onlysavemevods.streamerStreamFilters";
  const streamerStreamPageSizeDefault = 5;
  const streamerStreamPageSizeOptions = [5, 10, 25, 50];
  const powerchatPageSizeDefault = 50;
  const powerchatPageSizeOptions = [25, 50, 100];
  let powerchatPage = 1;
  let latestPowerchatStats = null;
  const tabs = ["tab-streamers", "tab-powerchat", "tab-jobs", "tab-logs", "tab-about", "tab-config"];
  const statusLabels = {
    checking_after_exit: "checking after exit",
    detected: "detected",
    downloading: "downloading",
    ended: "ended",
    interrupted: "interrupted",
    waiting_retry: "waiting retry",
  };
  const attentionStatuses = new Set(["checking_after_exit", "interrupted", "waiting_retry"]);

  const byId = (id) => document.getElementById(id);
  const setText = (id, value) => {
    const element = byId(id);
    if (element) element.textContent = value;
  };
  const readInitialPowerchatStats = () => {
    const element = byId("powerchat-stats-json");
    if (!element) return null;
    try { return JSON.parse(element.textContent || "{}"); } catch (_) { return null; }
  };
  const escapeHtml = (value) => String(value ?? "").replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  }[char]));
  const escapeAttr = escapeHtml;
  const statusLabel = (status) => statusLabels[status] || String(status || "").replaceAll("_", " ");
  const formatOptionalInt = (value) => value === null || value === undefined ? "-" : String(value);
  const formatBytes = (value) => {
    const units = ["B", "KiB", "MiB", "GiB", "TiB"];
    let size = Number(value || 0);
    for (const unit of units) {
      if (size < 1024 || unit === units[units.length - 1]) {
        return unit === "B" ? `${Math.trunc(size)} ${unit}` : `${size.toFixed(1)} ${unit}`;
      }
      size = size / 1024;
    }
    return "0 B";
  };
  const formatDuration = (seconds) => {
    seconds = Math.max(0, Math.trunc(seconds));
    if (seconds < 60) return `${seconds}s`;
    let minutes = Math.trunc(seconds / 60);
    seconds = seconds % 60;
    if (minutes < 60) return `${minutes}m ${String(seconds).padStart(2, "0")}s`;
    let hours = Math.trunc(minutes / 60);
    minutes = minutes % 60;
    if (hours < 48) return `${hours}h ${String(minutes).padStart(2, "0")}m`;
    const days = Math.trunc(hours / 24);
    hours = hours % 24;
    return `${days}d ${String(hours).padStart(2, "0")}h`;
  };
  const formatEpoch = (value) => {
    if (value === null || value === undefined) return "-";
    const date = new Date(Number(value) * 1000);
    if (Number.isNaN(date.getTime())) return "-";
    return date.toLocaleString();
  };
  const formatEpochAge = (value) => {
    if (value === null || value === undefined) return "";
    const age = (Date.now() / 1000) - Number(value);
    return `(${formatDuration(age)} ago)`;
  };
  const formatIso = (value) => {
    if (!value) return "-";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value);
    return date.toLocaleString();
  };
  const formatKindCounts = (counts) => {
    const entries = Object.entries(counts || {}).sort(([a], [b]) => a.localeCompare(b));
    if (!entries.length) return "-";
    return entries.map(([kind, count]) => `${kind} ${count}`).join(", ");
  };
  const formatConfigValue = (value) => {
    if (value === true) return "true";
    if (value === false) return "false";
    if (value === null || value === undefined) return "-";
    if (Array.isArray(value)) return value.length ? value.map(String).join("\\n") : "-";
    return String(value);
  };
  const sourcePlatformLabels = {
    youtube: "YouTube",
    twitch: "Twitch",
    kick: "Kick",
    rumble: "Rumble",
    unknown: "Unknown",
  };
  const sourcePlatformInitials = {
    youtube: "Y",
    twitch: "T",
    kick: "K",
    rumble: "R",
    unknown: "?",
  };
  const sourcePlatformIconUrls = {
    youtube: "/assets/platforms/youtube.svg",
    kick: "/assets/platforms/kick.svg",
    rumble: "/assets/platforms/rumble.svg",
  };
  const sourcePlatforms = new Set(["youtube", "twitch", "kick", "rumble"]);
  const splitSourceValues = (value) => String(value || "")
    .split(/\\r?\\n/)
    .map((source) => source.trim())
    .filter(Boolean);
  const sourcePlatformFromHost = (host) => {
    host = String(host || "").toLowerCase().replace(/^www\\./, "");
    if (host === "youtu.be" || host === "youtube.com" || host.endsWith(".youtube.com")) return "youtube";
    if (host === "twitch.tv" || host.endsWith(".twitch.tv")) return "twitch";
    if (host === "kick.com" || host.endsWith(".kick.com")) return "kick";
    if (host === "rumble.com" || host.endsWith(".rumble.com")) return "rumble";
    return "unknown";
  };
  const detectSourcePlatform = (value, selected = "auto") => {
    selected = String(selected || "auto").toLowerCase();
    if (selected !== "auto" && sourcePlatforms.has(selected)) return selected;
    value = String(value || "").trim();
    const prefix = value.match(/^([A-Za-z][A-Za-z0-9_-]*):(.+)$/);
    if (prefix) {
      const platform = prefix[1].toLowerCase().replaceAll("_", "-");
      if (sourcePlatforms.has(platform)) return platform;
    }
    if (/^https?:\\/\\//i.test(value)) {
      try { return sourcePlatformFromHost(new URL(value).hostname); } catch (_) { return "unknown"; }
    }
    return "youtube";
  };
  const sourceDisplayName = (source) => {
    let value = String(source || "").trim().replace(/\\/+$/, "");
    if (!value) return "Unknown source";
    if (/^https?:\\/\\//i.test(value)) {
      try {
        const url = new URL(value);
        const path = url.pathname.replace(/^\\/+|\\/+$/g, "");
        return path ? path.split("/").pop() : url.hostname.replace(/^www\\./, "");
      } catch (_) {}
    }
    const prefix = value.match(/^([A-Za-z][A-Za-z0-9_-]*):(.+)$/);
    if (prefix && sourcePlatforms.has(prefix[1].toLowerCase().replaceAll("_", "-"))) {
      value = prefix[2].trim().replace(/\\/+$/, "");
    }
    if (value.includes("/")) value = value.split("/").filter(Boolean).pop() || value;
    return value.replace(/^@+/, "") || source;
  };
  const sourceUrlPath = (value) => {
    try { return new URL(value).pathname.replace(/^\\/+|\\/+$/g, ""); } catch (_) { return ""; }
  };
  const normalizeSourceValue = (value, platform = "auto") => {
    value = String(value || "").trim();
    if (!value) return "";
    const detected = detectSourcePlatform(value, platform);
    if (/^https?:\\/\\//i.test(value)) {
      const path = sourceUrlPath(value);
      if (detected === "youtube" && path.startsWith("@")) return path.split("/")[0];
      if (["twitch", "kick"].includes(detected)) {
        const channel = path.split("/").filter(Boolean)[0] || "";
        return channel ? `${detected}:${channel}` : value;
      }
      if (detected === "rumble") return path ? `rumble:${path}` : value;
      return value;
    }
    const prefix = value.match(/^([A-Za-z][A-Za-z0-9_-]*):(.+)$/);
    if (prefix && sourcePlatforms.has(prefix[1].toLowerCase().replaceAll("_", "-"))) return value;
    const clean = value.replace(/^@+/, "").replace(/^\\/+|\\/+$/g, "");
    if (detected === "youtube") return value.startsWith("@") ? value : `@${clean}`;
    if (sourcePlatforms.has(detected)) return `${detected}:${clean}`;
    return value;
  };
  const renderPlatformIcon = (platform, label, initial) => {
    const url = sourcePlatformIconUrls[platform] || "";
    const image = url ? `<img src="${escapeAttr(url)}" alt="" loading="lazy" onerror="this.remove()">` : "";
    return `<span class="source-platform-icon ${escapeAttr(platform)}" title="${escapeAttr(label)}" aria-label="${escapeAttr(label)}">${image}<span class="source-platform-initial">${escapeHtml(initial)}</span></span>`;
  };
  const renderSourceList = (sources) => {
    sources = sources || [];
    if (!sources.length) return '<div class="source-list" data-source-list><div class="source-list-empty file-meta">No sources configured.</div></div>';
    const rows = sources.map((source) => {
      const platform = detectSourcePlatform(source);
      const label = sourcePlatformLabels[platform] || sourcePlatformLabels.unknown;
      const initial = sourcePlatformInitials[platform] || sourcePlatformInitials.unknown;
      return `<div class="source-list-row" data-source-row>
  ${renderPlatformIcon(platform, label, initial)}
  <div><div class="source-name">${escapeHtml(sourceDisplayName(source))}</div><div class="source-raw file-meta">${escapeHtml(source)}</div></div>
  <span class="badge">${escapeHtml(label)}</span>
  <button class="download action-button" type="button" data-remove-source="${escapeAttr(source)}">Remove</button>
</div>`;
    }).join("");
    return `<div class="source-list" data-source-list>${rows}</div>`;
  };
  const renderSourceBuilder = (sources) => `<div class="source-builder" data-source-builder>
  <textarea name="sources" data-source-values hidden>${escapeHtml((sources || []).join("\\n"))}</textarea>
  ${renderSourceList(sources || [])}
  <div class="source-builder-actions"><button class="download action-button" type="button" data-open-source-popover>Add Source</button></div>
  <div class="source-popover" data-source-popover hidden>
    <div class="source-popover-head"><strong>Add Source</strong><button class="download action-button" type="button" data-close-source-popover>Close</button></div>
    <div class="source-popover-fields">
      <label>Website <select data-source-platform><option value="auto">Auto-detect</option><option value="youtube">YouTube</option><option value="twitch">Twitch</option><option value="kick">Kick</option><option value="rumble">Rumble</option></select></label>
      <label>Channel or URL <input data-source-input placeholder="Paste URL or channel name"></label>
      <button class="download action-button" type="button" data-add-source>Add Source</button>
    </div>
    <div class="file-meta">Paste a YouTube, Twitch, Kick, or Rumble URL to auto-detect the website, or choose one for a plain channel name.</div>
  </div>
</div>`;
  const sourceValuesForBuilder = (builder) => splitSourceValues((builder.querySelector("[data-source-values]") || {}).value || "");
  const markStreamerFormDirty = (element) => {
    const form = element ? element.closest("form.streamer-form") : null;
    if (form) form.setAttribute("data-dirty", "true");
  };
  const updateSourceBuilder = (builder, sources) => {
    sources = [...new Set((sources || []).map((source) => String(source || "").trim()).filter(Boolean))];
    const values = builder.querySelector("[data-source-values]");
    if (values) values.value = sources.join("\\n");
    const list = builder.querySelector("[data-source-list]");
    if (list) list.outerHTML = renderSourceList(sources);
  };
  const streamerListIsEditing = (streamerList) => {
    if (!streamerList) return false;
    const activeElement = document.activeElement;
    return Boolean(
      (activeElement && streamerList.contains(activeElement))
      || streamerList.querySelector("form.streamer-form[data-dirty='true']")
      || streamerList.querySelector("[data-source-popover]:not([hidden])")
    );
  };
  const streamNeedsAttention = (stream) => attentionStatuses.has(stream.status) || Boolean(stream.has_mixed_formats);
  const readLocalStorageValue = (key) => {
    try {
      const value = localStorage.getItem(key);
      if (value !== null) return value;
    } catch (_) {}
    return "";
  };
  const readStreamSet = (key) => {
    try {
      const parsed = JSON.parse(readLocalStorageValue(key) || "[]");
      return new Set(Array.isArray(parsed) ? parsed.map(String) : []);
    } catch (_) {
      return new Set();
    }
  };
  const readStreamTabState = () => {
    try {
      const parsed = JSON.parse(readLocalStorageValue(streamTabKey) || "{}");
      return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
    } catch (_) {
      return {};
    }
  };
  const readStreamerStreamFilters = () => {
    try {
      const parsed = JSON.parse(readLocalStorageValue(streamerStreamFilterKey) || "{}");
      return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
    } catch (_) {
      return {};
    }
  };
  const collapsedStreams = readStreamSet(collapsedKey);
  const expandedStreams = readStreamSet(expandedKey);
  const collapsedStreamers = readStreamSet(collapsedStreamerKey);
  const expandedStreamers = readStreamSet(expandedStreamerKey);
  const openStreamerSettings = readStreamSet(openStreamerSettingsKey);
  const selectedStreamTabs = readStreamTabState();
  const streamerStreamFilters = readStreamerStreamFilters();
  const writeCollapsedStreams = () => {
    try { localStorage.setItem(collapsedKey, JSON.stringify([...collapsedStreams])); } catch (_) {}
  };
  const writeExpandedStreams = () => {
    try { localStorage.setItem(expandedKey, JSON.stringify([...expandedStreams])); } catch (_) {}
  };
  const writeCollapsedStreamers = () => {
    try { localStorage.setItem(collapsedStreamerKey, JSON.stringify([...collapsedStreamers])); } catch (_) {}
  };
  const writeExpandedStreamers = () => {
    try { localStorage.setItem(expandedStreamerKey, JSON.stringify([...expandedStreamers])); } catch (_) {}
  };
  const writeOpenStreamerSettings = () => {
    try { localStorage.setItem(openStreamerSettingsKey, JSON.stringify([...openStreamerSettings])); } catch (_) {}
  };
  const writeSelectedStreamTabs = () => {
    try { localStorage.setItem(streamTabKey, JSON.stringify(selectedStreamTabs)); } catch (_) {}
  };
  const writeStreamerStreamFilters = () => {
    try { localStorage.setItem(streamerStreamFilterKey, JSON.stringify(streamerStreamFilters)); } catch (_) {}
  };
  const streamIsCollapsed = (videoId, status) => (
    collapsedStreams.has(videoId) || (status === "ended" && !expandedStreams.has(videoId))
  );
  const streamerExpandsByDefault = (streamer) => (
    Boolean(streamer && streamer.needs_grouping)
    || Number(streamer && streamer.active_count || 0) > 0
    || Number(streamer && streamer.attention_count || 0) > 0
    || (streamerActiveJobCount(streamer) > 0)
  );
  const streamerCardExpandsByDefault = (card) => (
    card && (
      card.getAttribute("data-streamer-needs-grouping") === "true"
      || Number(card.getAttribute("data-streamer-active") || 0) > 0
      || Number(card.getAttribute("data-streamer-attention") || 0) > 0
      || Number(card.getAttribute("data-streamer-active-jobs") || 0) > 0
    )
  );
  const streamerIsCollapsed = (key, expandsDefault) => (
    collapsedStreamers.has(key) || (!expandsDefault && !expandedStreamers.has(key))
  );

  const defaultStreamerStreamFilter = () => ({ platform: "all", search: "", from: "", to: "", page: 1, page_size: streamerStreamPageSizeDefault });
  const normalizeStreamerStreamFilter = (value) => {
    const defaults = defaultStreamerStreamFilter();
    const state = value && typeof value === "object" && !Array.isArray(value) ? value : {};
    const pageSize = Number(state.page_size || defaults.page_size);
    return {
      platform: String(state.platform || defaults.platform).toLowerCase() || "all",
      search: String(state.search || ""),
      from: String(state.from || ""),
      to: String(state.to || ""),
      page: Math.max(1, Math.trunc(Number(state.page || defaults.page)) || 1),
      page_size: streamerStreamPageSizeOptions.includes(pageSize) ? pageSize : defaults.page_size,
    };
  };
  const streamerStreamFilterFor = (key) => {
    key = String(key || "");
    const normalized = normalizeStreamerStreamFilter(streamerStreamFilters[key]);
    streamerStreamFilters[key] = normalized;
    return normalized;
  };
  const streamDateValue = (stream) => {
    const iso = String((stream && (stream.last_started_at || stream.updated_at)) || "");
    if (iso.length >= 10) return iso.slice(0, 10);
    const epoch = Number(stream && stream.latest_file_modified_at);
    if (epoch > 0) return new Date(epoch * 1000).toISOString().slice(0, 10);
    return "";
  };
  const streamCardMatchesFilter = (card, state) => {
    const platform = String(card.getAttribute("data-stream-platform") || "unknown").toLowerCase();
    if (state.platform !== "all" && platform !== state.platform) return false;
    const query = state.search.trim().toLowerCase();
    if (query) {
      const haystack = [
        card.getAttribute("data-stream-title") || "",
        card.getAttribute("data-video-id") || "",
      ].join(" ").toLowerCase();
      if (!haystack.includes(query)) return false;
    }
    const date = String(card.getAttribute("data-stream-date") || "");
    if (state.from && (!date || date < state.from)) return false;
    if (state.to && (!date || date > state.to)) return false;
    return true;
  };
  const setStreamerBrowserControls = (browser, state) => {
    const platform = browser.querySelector("[data-stream-filter-platform]");
    const search = browser.querySelector("[data-stream-filter-search]");
    const from = browser.querySelector("[data-stream-filter-from]");
    const to = browser.querySelector("[data-stream-filter-to]");
    const pageSize = browser.querySelector("[data-stream-page-size]");
    if (platform) platform.value = state.platform;
    if (search) search.value = state.search;
    if (from) from.value = state.from;
    if (to) to.value = state.to;
    if (pageSize) pageSize.value = String(state.page_size);
  };
  const readStreamerBrowserControls = (browser, state) => {
    const platform = browser.querySelector("[data-stream-filter-platform]");
    const search = browser.querySelector("[data-stream-filter-search]");
    const from = browser.querySelector("[data-stream-filter-from]");
    const to = browser.querySelector("[data-stream-filter-to]");
    const pageSize = browser.querySelector("[data-stream-page-size]");
    const size = Number(pageSize ? pageSize.value : state.page_size);
    state.platform = String(platform ? platform.value : state.platform || "all").toLowerCase() || "all";
    state.search = String(search ? search.value : state.search || "");
    state.from = String(from ? from.value : state.from || "");
    state.to = String(to ? to.value : state.to || "");
    state.page_size = streamerStreamPageSizeOptions.includes(size) ? size : streamerStreamPageSizeDefault;
    state.page = Math.max(1, Math.trunc(Number(state.page || 1)) || 1);
    return state;
  };
  const applyStreamerStreamBrowser = (browser) => {
    const key = browser.getAttribute("data-streamer-key") || "";
    const state = streamerStreamFilterFor(key);
    if (browser.getAttribute("data-stream-browser-ready") !== "true") {
      setStreamerBrowserControls(browser, state);
      browser.setAttribute("data-stream-browser-ready", "true");
    } else {
      readStreamerBrowserControls(browser, state);
    }
    const cards = Array.from(browser.querySelectorAll(".stream[data-video-id]"));
    const matches = cards.filter((card) => streamCardMatchesFilter(card, state));
    const pageCount = Math.max(1, Math.ceil(matches.length / state.page_size));
    state.page = Math.min(Math.max(1, state.page), pageCount);
    const start = (state.page - 1) * state.page_size;
    const visible = new Set(matches.slice(start, start + state.page_size));
    cards.forEach((card) => { card.hidden = !visible.has(card); });
    const stateText = browser.querySelector("[data-stream-browser-state]");
    if (stateText) {
      if (!matches.length) {
        stateText.textContent = `No streams match ${cards.length} total stream${cards.length === 1 ? "" : "s"}.`;
      } else {
        const first = start + 1;
        const last = Math.min(start + state.page_size, matches.length);
        stateText.textContent = `Showing ${first}-${last} of ${matches.length} stream${matches.length === 1 ? "" : "s"} · page ${state.page} of ${pageCount}`;
      }
    }
    const prev = browser.querySelector("[data-stream-page-prev]");
    const next = browser.querySelector("[data-stream-page-next]");
    if (prev) prev.disabled = state.page <= 1;
    if (next) next.disabled = state.page >= pageCount;
    writeStreamerStreamFilters();
  };
  const applyStreamerStreamBrowsers = (root) => {
    for (const browser of root.querySelectorAll("[data-stream-browser]")) {
      applyStreamerStreamBrowser(browser);
    }
  };

  const normalizeTabId = (id) => ({
    "tab-streams": "tab-streamers",
    "tab-channels": "tab-streamers",
    "tab-streamer-groups": "tab-streamers",
  }[id] || id);

  const selectTab = (id, updateHash) => {
    id = normalizeTabId(id);
    if (!tabs.includes(id)) return;
    const tab = byId(id);
    if (!tab) return;
    tab.checked = true;
    try { localStorage.setItem(tabKey, id); } catch (_) {}
    if (updateHash && history.replaceState) {
      history.replaceState(null, "", "#" + id.replace("tab-", ""));
    }
  };

  let stored = "";
  stored = normalizeTabId(readLocalStorageValue(tabKey));
  const hashTab = location.hash ? normalizeTabId("tab-" + location.hash.slice(1)) : "";
  selectTab(tabs.includes(hashTab) ? hashTab : stored, false);

  for (const id of tabs) {
    const tab = byId(id);
    if (!tab) continue;
    tab.addEventListener("change", () => {
      if (tab.checked) selectTab(id, true);
    });
  }

  document.addEventListener("click", (event) => {
    const openSource = event.target.closest("[data-open-source-popover]");
    if (openSource) {
      event.preventDefault();
      const builder = openSource.closest("[data-source-builder]");
      const popover = builder ? builder.querySelector("[data-source-popover]") : null;
      if (popover) popover.hidden = false;
      const input = builder ? builder.querySelector("[data-source-input]") : null;
      if (input) input.focus();
      return;
    }
    const closeSource = event.target.closest("[data-close-source-popover]");
    if (closeSource) {
      event.preventDefault();
      const popover = closeSource.closest("[data-source-popover]");
      if (popover) popover.hidden = true;
      return;
    }
    const addSource = event.target.closest("[data-add-source]");
    if (addSource) {
      event.preventDefault();
      const builder = addSource.closest("[data-source-builder]");
      if (!builder) return;
      const input = builder.querySelector("[data-source-input]");
      const select = builder.querySelector("[data-source-platform]");
      const source = normalizeSourceValue(input ? input.value : "", select ? select.value : "auto");
      if (!source) return;
      updateSourceBuilder(builder, [...sourceValuesForBuilder(builder), source]);
      markStreamerFormDirty(builder);
      if (input) input.value = "";
      const popover = builder.querySelector("[data-source-popover]");
      if (popover) popover.hidden = true;
      const form = builder.closest("form.streamer-form");
      if (form) {
        const state = byId("refresh-state");
        if (state) state.textContent = "Saving source...";
        if (typeof form.requestSubmit === "function") form.requestSubmit();
        else form.submit();
      }
      return;
    }
    const removeSource = event.target.closest("[data-remove-source]");
    if (removeSource) {
      event.preventDefault();
      const builder = removeSource.closest("[data-source-builder]");
      if (!builder) return;
      const source = removeSource.getAttribute("data-remove-source") || "";
      updateSourceBuilder(builder, sourceValuesForBuilder(builder).filter((value) => value !== source));
      markStreamerFormDirty(builder);
    }
  });

  document.addEventListener("input", (event) => {
    const target = event.target;
    const powerchatFilter = target && target.closest ? target.closest("[data-powerchat-filter-control]") : null;
    if (powerchatFilter) {
      powerchatPage = 1;
      renderPowerchatDashboard(latestPowerchatStats || readInitialPowerchatStats() || {});
      return;
    }
    const streamFilter = target && target.closest ? target.closest("[data-stream-filter-control]") : null;
    if (streamFilter) {
      const browser = streamFilter.closest("[data-stream-browser]");
      if (browser) {
        const key = browser.getAttribute("data-streamer-key") || "";
        const state = streamerStreamFilterFor(key);
        state.page = 1;
        browser.setAttribute("data-stream-browser-ready", "true");
        readStreamerBrowserControls(browser, state);
        applyStreamerStreamBrowser(browser);
      }
      return;
    }
    if (target && target.closest && target.closest("form.streamer-form")) {
      markStreamerFormDirty(target);
    }
  });

  document.addEventListener("change", (event) => {
    const target = event.target;
    const powerchatFilter = target && target.closest ? target.closest("[data-powerchat-filter-control]") : null;
    if (powerchatFilter) {
      powerchatPage = 1;
      renderPowerchatDashboard(latestPowerchatStats || readInitialPowerchatStats() || {});
      return;
    }
    const streamFilter = target && target.closest ? target.closest("[data-stream-filter-control]") : null;
    if (streamFilter) {
      const browser = streamFilter.closest("[data-stream-browser]");
      if (browser) {
        const key = browser.getAttribute("data-streamer-key") || "";
        const state = streamerStreamFilterFor(key);
        state.page = 1;
        browser.setAttribute("data-stream-browser-ready", "true");
        readStreamerBrowserControls(browser, state);
        applyStreamerStreamBrowser(browser);
      }
      return;
    }
    if (target && target.closest && target.closest("form.streamer-form")) {
      markStreamerFormDirty(target);
    }
  });

  const applyStreamerCollapsedState = (root) => {
    for (const card of root.querySelectorAll(".streamer-section[data-streamer-key]")) {
      const key = card.getAttribute("data-streamer-key") || "";
      const button = card.querySelector("[data-streamer-toggle]");
      if (!key || !button) continue;
      const collapsed = streamerIsCollapsed(key, streamerCardExpandsByDefault(card));
      card.classList.toggle("collapsed", collapsed);
      button.textContent = collapsed ? "Expand" : "Collapse";
      button.setAttribute("aria-expanded", collapsed ? "false" : "true");
    }
  };

  const applyStreamerSettingsState = (root) => {
    for (const card of root.querySelectorAll(".streamer-section[data-streamer-key]")) {
      const key = card.getAttribute("data-streamer-key") || "";
      const button = card.querySelector("[data-streamer-settings-toggle]");
      const panel = card.querySelector("[data-streamer-settings-panel]");
      if (!key || !button || !panel) continue;
      const open = openStreamerSettings.has(key);
      panel.hidden = !open;
      button.textContent = open ? "Hide Settings" : "Settings";
      button.setAttribute("aria-expanded", open ? "true" : "false");
    }
  };

  const applyStreamTabState = (root) => {
    for (const card of root.querySelectorAll(".stream[data-video-id]")) {
      const videoId = card.getAttribute("data-video-id") || "";
      const selected = selectedStreamTabs[videoId];
      if (!["files", "events", "powerchat", "speakers", "log", "jobs"].includes(selected)) continue;
      const input = card.querySelector(`[data-stream-tab="${selected}"]`);
      if (input) input.checked = true;
    }
  };

  const applyCollapsedState = (root) => {
    applyStreamerCollapsedState(root);
    applyStreamerSettingsState(root);
    applyStreamTabState(root);
    for (const card of root.querySelectorAll(".stream[data-video-id]")) {
      const videoId = card.getAttribute("data-video-id");
      const status = card.getAttribute("data-stream-status") || "";
      const button = card.querySelector("[data-stream-toggle]");
      if (!videoId || !button) continue;
      const collapsed = streamIsCollapsed(videoId, status);
      card.classList.toggle("collapsed", collapsed);
      button.textContent = collapsed ? "Expand" : "Collapse";
      button.setAttribute("aria-expanded", collapsed ? "false" : "true");
    }
    applyStreamerStreamBrowsers(root);
  };

  document.addEventListener("change", (event) => {
    const input = event.target.closest("[data-stream-tab][data-video-id]");
    if (!input || !input.checked) return;
    selectedStreamTabs[input.getAttribute("data-video-id") || ""] = input.getAttribute("data-stream-tab") || "files";
    writeSelectedStreamTabs();
  });

  document.addEventListener("click", (event) => {
    const streamSpeakersButton = event.target.closest("[data-load-stream-speakers]");
    if (streamSpeakersButton) {
      event.preventDefault();
      loadStreamSpeakers(streamSpeakersButton);
      return;
    }

    const voiceDetailsButton = event.target.closest("[data-load-voice-details]");
    if (voiceDetailsButton) {
      event.preventDefault();
      loadVoiceDetails(voiceDetailsButton);
      return;
    }

    const streamPageButton = event.target.closest("[data-stream-page-prev], [data-stream-page-next]");
    if (streamPageButton) {
      event.preventDefault();
      const browser = streamPageButton.closest("[data-stream-browser]");
      if (!browser) return;
      const key = browser.getAttribute("data-streamer-key") || "";
      const state = streamerStreamFilterFor(key);
      browser.setAttribute("data-stream-browser-ready", "true");
      readStreamerBrowserControls(browser, state);
      state.page += streamPageButton.hasAttribute("data-stream-page-next") ? 1 : -1;
      applyStreamerStreamBrowser(browser);
      return;
    }

    const powerchatPageButton = event.target.closest("[data-powerchat-page-prev], [data-powerchat-page-next]");
    if (powerchatPageButton) {
      event.preventDefault();
      powerchatPage += powerchatPageButton.hasAttribute("data-powerchat-page-next") ? 1 : -1;
      renderPowerchatDashboard(latestPowerchatStats || readInitialPowerchatStats() || {});
      return;
    }

    const streamerJobPageButton = event.target.closest("[data-streamer-job-page-button]");
    if (streamerJobPageButton) {
      event.preventDefault();
      const jobsRoot = streamerJobPageButton.closest("[data-streamer-jobs]");
      const page = streamerJobPageButton.getAttribute("data-streamer-job-page-button") || "1";
      if (!jobsRoot) return;
      jobsRoot.querySelectorAll("[data-streamer-job-page]").forEach((panel) => {
        const active = panel.getAttribute("data-streamer-job-page") === page;
        panel.hidden = !active;
        panel.classList.toggle("is-active", active);
      });
      jobsRoot.querySelectorAll("[data-streamer-job-page-button]").forEach((button) => {
        const active = button.getAttribute("data-streamer-job-page-button") === page;
        button.setAttribute("aria-current", active ? "page" : "false");
      });
      const state = jobsRoot.querySelector("[data-streamer-job-page-state]");
      if (state) {
        const total = jobsRoot.querySelectorAll("[data-streamer-job-page]").length;
        state.textContent = `Page ${page} of ${total}`;
      }
      return;
    }

    const settingsButton = event.target.closest("[data-streamer-settings-toggle]");
    if (settingsButton) {
      const key = settingsButton.getAttribute("data-streamer-settings-toggle") || "";
      const card = settingsButton.closest(".streamer-section");
      if (!key || !card) return;
      const opening = !openStreamerSettings.has(key);
      if (opening) {
        openStreamerSettings.add(key);
        collapsedStreamers.delete(key);
        expandedStreamers.add(key);
        writeCollapsedStreamers();
        writeExpandedStreamers();
      } else {
        openStreamerSettings.delete(key);
      }
      writeOpenStreamerSettings();
      applyCollapsedState(card.parentElement || document);
      return;
    }

    const streamerButton = event.target.closest("[data-streamer-toggle]");
    if (streamerButton) {
      const key = streamerButton.getAttribute("data-streamer-toggle") || "";
      const card = streamerButton.closest(".streamer-section");
      if (!key || !card) return;
      const currentlyCollapsed = streamerIsCollapsed(key, streamerCardExpandsByDefault(card));
      if (currentlyCollapsed) {
        collapsedStreamers.delete(key);
        expandedStreamers.add(key);
      } else {
        collapsedStreamers.add(key);
        expandedStreamers.delete(key);
      }
      writeCollapsedStreamers();
      writeExpandedStreamers();
      applyStreamerCollapsedState(card.parentElement || document);
      return;
    }

    const button = event.target.closest("[data-stream-toggle]");
    if (!button) return;
    const videoId = button.getAttribute("data-stream-toggle");
    if (!videoId) return;
    const card = button.closest(".stream");
    const status = card ? card.getAttribute("data-stream-status") || "" : "";
    const currentlyCollapsed = streamIsCollapsed(videoId, status);
    if (currentlyCollapsed) {
      collapsedStreams.delete(videoId);
      expandedStreams.add(videoId);
    } else {
      collapsedStreams.add(videoId);
      expandedStreams.delete(videoId);
    }
    writeCollapsedStreams();
    writeExpandedStreams();
    if (card) {
      const collapsed = streamIsCollapsed(videoId, status);
      card.classList.toggle("collapsed", collapsed);
      button.textContent = collapsed ? "Expand" : "Collapse";
      button.setAttribute("aria-expanded", collapsed ? "false" : "true");
    }
  });

  applyCollapsedState(document);

  const wizard = byId("streamer-wizard");
  const wizardForm = byId("streamer-wizard-form");
  let wizardStepIndex = 0;
  const wizardSteps = () => wizard ? Array.from(wizard.querySelectorAll("[data-wizard-step]")) : [];
  const wizardStepTabs = () => wizard ? Array.from(wizard.querySelectorAll("[data-wizard-step-tab]")) : [];
  const setWizardStep = (index) => {
    const steps = wizardSteps();
    if (!steps.length) return;
    wizardStepIndex = Math.max(0, Math.min(index, steps.length - 1));
    steps.forEach((step, stepIndex) => { step.hidden = stepIndex !== wizardStepIndex; });
    wizardStepTabs().forEach((tab, stepIndex) => tab.classList.toggle("active", stepIndex === wizardStepIndex));
    const back = wizard.querySelector("[data-wizard-back]");
    const next = wizard.querySelector("[data-wizard-next]");
    const submit = wizard.querySelector("[data-wizard-submit]");
    const progress = wizard.querySelector("[data-wizard-progress]");
    if (back) back.hidden = wizardStepIndex === 0;
    if (next) next.hidden = wizardStepIndex === steps.length - 1;
    if (submit) submit.hidden = wizardStepIndex !== steps.length - 1;
    if (progress) progress.textContent = `Step ${wizardStepIndex + 1} of ${steps.length}`;
  };
  const addWizardSpeakerRow = (label = "", name = "") => {
    const rows = byId("streamer-wizard-speaker-rows");
    if (!rows) return;
    rows.insertAdjacentHTML("beforeend", `<div class="speaker-label-pair">
        <input name="speaker_label" value="${escapeAttr(label)}" placeholder="SPEAKER_00">
        <input name="speaker_name" value="${escapeAttr(name)}" placeholder="Name">
      </div>`);
  };
  const resetWizardSpeakerRows = () => {
    const rows = byId("streamer-wizard-speaker-rows");
    if (!rows) return;
    rows.innerHTML = "";
    addWizardSpeakerRow();
  };
  const openStreamerWizard = (button) => {
    if (!wizard || !wizardForm) return;
    wizardForm.reset();
    resetWizardSpeakerRows();
    setWizardStep(0);
    const name = button ? (button.getAttribute("data-streamer-name") || "") : "";
    const sources = button ? (button.getAttribute("data-streamer-sources") || "") : "";
    const nameInput = byId("streamer-wizard-name");
    const sourcesInput = byId("streamer-wizard-sources");
    const downloadDirInput = byId("streamer-wizard-download-dir");
    if (nameInput) nameInput.value = name;
    if (sourcesInput) sourcesInput.value = sources;
    if (downloadDirInput) downloadDirInput.value = "";
    if (typeof wizard.showModal === "function") wizard.showModal();
    else wizard.setAttribute("open", "");
    if (nameInput) nameInput.focus();
  };

  document.addEventListener("click", (event) => {
    const openButton = event.target.closest("[data-open-streamer-wizard]");
    if (openButton) {
      event.preventDefault();
      if (!openButton.disabled) openStreamerWizard(openButton);
      return;
    }
    if (event.target.closest("[data-close-streamer-wizard]")) {
      event.preventDefault();
      if (wizard) wizard.close();
      return;
    }
    if (event.target.closest("[data-wizard-next]")) {
      event.preventDefault();
      if (wizardStepIndex === 0 && wizardForm && !wizardForm.reportValidity()) return;
      setWizardStep(wizardStepIndex + 1);
      return;
    }
    if (event.target.closest("[data-wizard-back]")) {
      event.preventDefault();
      setWizardStep(wizardStepIndex - 1);
      return;
    }
    if (event.target.closest("[data-add-wizard-speaker]")) {
      event.preventDefault();
      addWizardSpeakerRow();
    }
  });

  if (wizard) {
    wizard.addEventListener("click", (event) => {
      if (event.target === wizard) wizard.close();
    });
  }

  const renderStatusCounts = (counts) => {
    const entries = Object.entries(counts || {}).sort(([a], [b]) => a.localeCompare(b));
    if (!entries.length) return "";
    return entries.map(([status, count]) => (
      `<span class="badge ${escapeAttr(status)}">${escapeHtml(statusLabel(status))}: ${escapeHtml(count)}</span>`
    )).join("");
  };

  const renderStreamSignals = (stream) => {
    const signals = [];
    if (stream.has_mixed_formats) signals.push("mixed finalized and partial formats");
    if (stream.status === "checking_after_exit") signals.push("post-exit checks running");
    if (stream.status === "waiting_retry") signals.push("waiting for retry");
    if (stream.status === "interrupted") signals.push("interrupted before clean exit");
    if (stream.has_part_files && stream.status !== "downloading") signals.push("partial files present");
    return signals.length ? `<div class="signals">${escapeHtml(signals.join(" / "))}</div>` : "";
  };

  const renderStreamEvents = (events) => {
    if (!events || !events.length) return '<div class="file-meta">No stream log entries yet.</div>';
    const rows = [...events].reverse().map((event) => {
      const segment = event.segment_index ? `seg ${String(event.segment_index).padStart(3, "0")}` : "-";
      const rawLevel = String(event.level || "info").toLowerCase();
      const level = ["debug", "info", "warning", "error"].includes(rawLevel) ? rawLevel : "info";
      return `<div class="stream-event ${escapeAttr(level)}">
        <div class="stream-event-time">${escapeHtml(formatIso(event.created_at))}</div>
        <div class="stream-event-level">${escapeHtml(level.toUpperCase())}</div>
        <div class="stream-event-segment">${escapeHtml(segment)}</div>
        <div class="stream-event-message">${escapeHtml(event.message || "")}</div>
      </div>`;
    }).join("");
    return `<div class="stream-events">${rows}</div>`;
  };

  const formatEventOffset = (seconds) => {
    seconds = Math.max(0, Math.trunc(Number(seconds) || 0));
    const hours = Math.trunc(seconds / 3600);
    const minutes = Math.trunc((seconds % 3600) / 60);
    const secs = seconds % 60;
    if (hours) return `${hours}:${String(minutes).padStart(2, "0")}:${String(secs).padStart(2, "0")}`;
    return `${minutes}:${String(secs).padStart(2, "0")}`;
  };

  const renderContentEvents = (events) => {
    events = events || [];
    if (!events.length) return '<div class="file-meta">No content events detected yet.</div>';
    const rows = events.slice().sort((a, b) => Number(a.start || 0) - Number(b.start || 0)).slice(0, 50).map((event) => {
      const labels = (event.labels || []).slice(0, 3).map((item) => `${item.label || ""} ${Math.round(Number(item.score || 0) * 100)}%`.trim()).filter(Boolean).join(", ") || "-";
      const keywords = (event.keywords || []).join(", ") || "-";
      const loudness = event.loudness_dbfs === null || event.loudness_dbfs === undefined ? "-" : `${Number(event.loudness_dbfs).toFixed(1)} dBFS`;
      const start = formatEventOffset(event.start);
      const end = formatEventOffset(event.end);
      return `<div class="content-event ${escapeAttr(event.severity || "info")}">
        <div class="content-event-time"><span>${escapeHtml(start)}</span><span class="content-event-end">to ${escapeHtml(end)}</span></div>
        <div class="content-event-main"><strong>${escapeHtml(event.rule || "Event")}</strong><span class="file-meta">${escapeHtml(formatDuration(event.duration || 0))} &middot; ${escapeHtml(Math.round(Number(event.score || 0) * 100))}% &middot; ${escapeHtml(loudness)}</span><div>${escapeHtml(event.text || labels)}</div></div>
        <div class="content-event-meta"><span><b>Labels</b> ${escapeHtml(labels)}</span><span><b>Keywords</b> ${escapeHtml(keywords)}</span></div>
      </div>`;
    }).join("");
    return `<div class="content-events">${rows}</div>`;
  };

  const formatPowerchatNumber = (value, decimals = 0) => {
    value = Number(value || 0);
    if (!decimals && Number.isInteger(value)) return String(value);
    return decimals ? value.toFixed(decimals) : String(value);
  };

  const formatPowerchatSummary = (moneyTotals, unitTotals) => {
    const parts = [];
    (moneyTotals || []).forEach((total) => {
      const currency = String(total.currency || "").toUpperCase();
      if (currency && total.amount !== undefined && total.amount !== null) {
        parts.push(`${currency} ${formatPowerchatNumber(total.amount, 2)}`);
      }
    });
    (unitTotals || []).forEach((total) => {
      const platform = String(total.platform || "").trim();
      const unit = String(total.unit || "").trim();
      if (unit && total.amount !== undefined && total.amount !== null) {
        const formatted = `${formatPowerchatNumber(total.amount)} ${unit}`;
        parts.push(platform ? `${platform}: ${formatted}` : formatted);
      }
    });
    return parts.join(", ");
  };

  const powerchatEventAmountText = (event) => {
    if (event.kind === "money" && event.money_amount !== null && event.money_amount !== undefined && event.money_currency) {
      return `${String(event.money_currency).toUpperCase()} ${formatPowerchatNumber(event.money_amount, 2)}`;
    }
    if (event.kind === "unit" && event.unit_amount !== null && event.unit_amount !== undefined && event.unit) {
      const formatted = `${formatPowerchatNumber(event.unit_amount)} ${event.unit}`;
      return event.platform ? `${event.platform}: ${formatted}` : formatted;
    }
    return "";
  };

  const renderPowerchatEvents = (stream) => {
    const events = (stream && stream.powerchat_events) || [];
    if (!events.length) return '<div class="file-meta">No Powerchat support events captured yet.</div>';
    const stats = buildStreamPowerchatStats(stream);
    const videoId = String((stream && stream.video_id) || "");
    const rows = events.slice(0, 100).map((event) => {
      const kind = ["money", "unit", "unknown"].includes(String(event.kind || "")) ? String(event.kind || "unknown") : "unknown";
      const timestamp = event.offset_seconds === null || event.offset_seconds === undefined ? formatIso(event.received_at) : formatEventOffset(event.offset_seconds);
      const donor = event.donor || "Unknown donor";
      const platform = event.platform || "Powerchat";
      const meta = [event.source, platform, event.kind].filter(Boolean).join(" / ");
      return `<div class="powerchat-event ${escapeAttr(kind)}">
        <div class="content-event-time"><span>${escapeHtml(timestamp)}</span></div>
        <div class="content-event-main"><strong>${escapeHtml(donor)}</strong><div>${escapeHtml(event.message || "-")}</div><span class="file-meta">${escapeHtml(meta)}</span></div>
        <div class="powerchat-event-amount">${escapeHtml(powerchatEventAmountText(event) || "-")}</div>
      </div>`;
    }).join("");
    return `<div class="stream-powerchat-dashboard">
      <div class="powerchat-export-actions">
        <a class="download action-button" href="${escapeAttr(powerchatExportUrl("json", { video_id: videoId }))}">Download JSON</a>
        <a class="download action-button" href="${escapeAttr(powerchatExportUrl("csv", { video_id: videoId }))}">Download CSV</a>
      </div>
      <div class="powerchat-summary-grid">${renderStreamPowerchatSummaryCards(stats)}</div>
      <div class="powerchat-dashboard-section">
        <h4>Donations Per Hour</h4>
        <div class="table-wrap"><table><thead><tr><th>Stream Hour</th><th>Events</th><th>Total</th><th>Average</th></tr></thead><tbody>${renderPowerchatHourlyRows(stats.hourly_totals || [])}</tbody></table></div>
      </div>
      <div class="powerchat-dashboard-section">
        <h4>Events</h4>
        <div class="powerchat-events">${rows}</div>
      </div>
    </div>`;
  };

  const formatPowerchatRates = (rates) => (rates || []).map((rate) => {
    const currency = String(rate.currency || "").toUpperCase();
    if (!currency || rate.amount_per_hour === undefined || rate.amount_per_hour === null) return "";
    return `${currency} ${formatPowerchatNumber(rate.amount_per_hour, 2)}/hr`;
  }).filter(Boolean).join(", ");

  const newPowerchatAccumulator = () => ({ event_count: 0, money: {}, money_event_counts: {}, units: {} });
  const addPowerchatEventToAccumulator = (accumulator, event) => {
    accumulator.event_count += 1;
    if (event.kind === "money" && event.money_amount !== null && event.money_amount !== undefined && event.money_currency) {
      const currency = String(event.money_currency || "").toUpperCase();
      accumulator.money[currency] = Number(accumulator.money[currency] || 0) + Number(event.money_amount || 0);
      accumulator.money_event_counts[currency] = Number(accumulator.money_event_counts[currency] || 0) + 1;
    } else if (event.kind === "unit" && event.unit_amount !== null && event.unit_amount !== undefined && event.unit) {
      const key = `${event.platform || ""}\u0000${event.unit || ""}`;
      accumulator.units[key] = Number(accumulator.units[key] || 0) + Number(event.unit_amount || 0);
    }
  };
  const powerchatMoneyTotalsFromAccumulator = (accumulator) => Object.entries(accumulator.money || {})
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([currency, amount]) => ({ currency, amount: Math.round(Number(amount || 0) * 100) / 100 }));
  const powerchatUnitTotalsFromAccumulator = (accumulator) => Object.entries(accumulator.units || {})
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([key, amount]) => {
      const [platform, unit] = String(key).split("\u0000");
      return { platform: platform || "", unit: unit || "", amount: Math.round(Number(amount || 0) * 100) / 100 };
    });
  const powerchatAveragesFromAccumulator = (accumulator) => Object.entries(accumulator.money || {})
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([currency, amount]) => {
      const count = Number((accumulator.money_event_counts || {})[currency] || 0);
      return count > 0 ? { currency, amount: Math.round((Number(amount || 0) / count) * 100) / 100 } : null;
    }).filter(Boolean);
  const powerchatRatesFromAccumulator = (accumulator, durationSeconds) => {
    durationSeconds = Number(durationSeconds || 0);
    if (durationSeconds <= 0) return [];
    const durationHours = durationSeconds / 3600;
    return powerchatMoneyTotalsFromAccumulator(accumulator).map((row) => ({
      currency: row.currency,
      amount: row.amount,
      duration_hours: Math.round(durationHours * 1000) / 1000,
      amount_per_hour: Math.round((Number(row.amount || 0) / durationHours) * 100) / 100,
    }));
  };
  const powerchatSortAmount = (accumulator) => Object.values(accumulator.money || {}).reduce((total, amount) => total + Number(amount || 0), 0);
  const powerchatHourLabel = (hourIndex) => hourIndex === null || hourIndex === undefined ? "No stream offset" : `${hourIndex}:00-${hourIndex}:59`;
  const powerchatEventTime = (event) => event.offset_seconds === null || event.offset_seconds === undefined ? formatIso(event.received_at) : formatEventOffset(event.offset_seconds);
  const powerchatExportUrl = (format, filters = {}) => {
    const params = new URLSearchParams();
    params.set("format", format);
    ["streamer", "video_id", "platform", "kind", "from", "to", "search"].forEach((key) => {
      const value = String(filters[key] || "").trim();
      if (value && value !== "all") params.set(key, value);
    });
    return `/powerchat-events?${params.toString()}`;
  };
  const updatePowerchatExportLinks = (filters) => {
    document.querySelectorAll("[data-powerchat-export]").forEach((link) => {
      const format = link.getAttribute("data-powerchat-export") || "json";
      link.setAttribute("href", powerchatExportUrl(format, filters));
    });
  };
  const streamPowerchatDurationSeconds = (stream, events) => {
    const started = Date.parse((stream && (stream.last_started_at || stream.first_seen_at)) || "");
    const ended = Date.parse((stream && stream.last_exit_at) || "");
    if (!Number.isNaN(started) && !Number.isNaN(ended) && ended > started) return Math.max(0, (ended - started) / 1000);
    const activeStatuses = new Set(["detected", "downloading", "checking_after_exit", "waiting_retry"]);
    if (!Number.isNaN(started) && activeStatuses.has(String((stream && stream.status) || ""))) {
      const updated = Date.parse((stream && stream.updated_at) || "") || Date.now();
      if (!Number.isNaN(updated) && updated > started) return Math.max(0, (updated - started) / 1000);
    }
    const offsets = (events || []).map((event) => event.offset_seconds).filter((value) => value !== null && value !== undefined).map(Number).filter((value) => !Number.isNaN(value));
    if (offsets.length >= 2) return Math.max(0, Math.max(...offsets) - Math.min(...offsets));
    if (offsets.length === 1) return Math.max(0, offsets[0]);
    return 0;
  };
  const renderStreamPowerchatSummaryCards = (stats) => {
    const topDonors = stats.top_donors || [];
    const topDonor = topDonors.length ? topDonors[0].donor : "-";
    const cards = [
      ["Total", formatPowerchatSummary(stats.money_totals || [], stats.unit_totals || []) || "-"],
      ["Per hour", formatPowerchatRates(stats.money_rates || []) || "-"],
      ["Events", String(stats.event_count || 0)],
      ["Duration", formatDuration(stats.duration_seconds || 0)],
      ["Top donor", topDonor || "-"],
      ["No offset", String(stats.events_without_offset || 0)],
    ];
    return cards.map(([label, value]) => `<div class="powerchat-summary-card"><strong>${escapeHtml(value)}</strong><span class="muted">${escapeHtml(label)}</span></div>`).join("");
  };
  const buildStreamPowerchatStats = (stream) => {
    const events = (stream && stream.powerchat_events) || [];
    const totals = newPowerchatAccumulator();
    const donors = new Map();
    const hours = new Map();
    let eventsWithoutOffset = 0;
    events.forEach((event) => {
      addPowerchatEventToAccumulator(totals, event);
      const donorName = event.donor || "Unknown donor";
      if (!donors.has(donorName)) donors.set(donorName, { donor: donorName, event_count: 0, latest_received_at: "", accumulator: newPowerchatAccumulator() });
      const donor = donors.get(donorName);
      donor.event_count += 1;
      if (event.received_at && event.received_at > donor.latest_received_at) donor.latest_received_at = event.received_at;
      addPowerchatEventToAccumulator(donor.accumulator, event);
      if (event.offset_seconds === null || event.offset_seconds === undefined) {
        eventsWithoutOffset += 1;
        return;
      }
      const hourIndex = Math.max(0, Math.floor(Number(event.offset_seconds || 0) / 3600));
      if (!hours.has(hourIndex)) hours.set(hourIndex, { hour_index: hourIndex, hour_label: powerchatHourLabel(hourIndex), event_count: 0, accumulator: newPowerchatAccumulator() });
      const hour = hours.get(hourIndex);
      hour.event_count += 1;
      addPowerchatEventToAccumulator(hour.accumulator, event);
    });
    const durationSeconds = streamPowerchatDurationSeconds(stream, events);
    const donorRows = [...donors.values()].map((donor) => ({
      donor: donor.donor,
      event_count: donor.event_count,
      latest_received_at: donor.latest_received_at,
      money_totals: powerchatMoneyTotalsFromAccumulator(donor.accumulator),
      unit_totals: powerchatUnitTotalsFromAccumulator(donor.accumulator),
      sort_amount: powerchatSortAmount(donor.accumulator),
    })).sort((a, b) => (Number(b.sort_amount || 0) - Number(a.sort_amount || 0)) || (Number(b.event_count || 0) - Number(a.event_count || 0)) || String(a.donor || "").localeCompare(String(b.donor || "")));
    const hourlyRows = [...hours.values()].sort((a, b) => Number(a.hour_index || 0) - Number(b.hour_index || 0)).map((hour) => ({
      hour_index: hour.hour_index,
      hour_label: hour.hour_label,
      event_count: hour.event_count,
      money_totals: powerchatMoneyTotalsFromAccumulator(hour.accumulator),
      unit_totals: powerchatUnitTotalsFromAccumulator(hour.accumulator),
      average_money: powerchatAveragesFromAccumulator(hour.accumulator),
      sort_amount: powerchatSortAmount(hour.accumulator),
    }));
    return {
      event_count: totals.event_count,
      duration_seconds: durationSeconds,
      duration_hours: durationSeconds > 0 ? Math.round((durationSeconds / 3600) * 1000) / 1000 : 0,
      events_without_offset: eventsWithoutOffset,
      money_totals: powerchatMoneyTotalsFromAccumulator(totals),
      unit_totals: powerchatUnitTotalsFromAccumulator(totals),
      money_rates: powerchatRatesFromAccumulator(totals, durationSeconds),
      top_donors: donorRows.slice(0, 10),
      hourly_totals: hourlyRows,
    };
  };
  const powerchatEventDate = (event) => String(event.received_at || "").slice(0, 10);
  const powerchatDashboardEventMatches = (event, filters) => {
    if (filters.streamer !== "all" && event.streamer !== filters.streamer) return false;
    if (filters.platform !== "all" && event.platform !== filters.platform) return false;
    if (filters.kind !== "all" && event.kind !== filters.kind) return false;
    const date = powerchatEventDate(event);
    if (filters.from && date && date < filters.from) return false;
    if (filters.to && date && date > filters.to) return false;
    const query = filters.search.trim().toLowerCase();
    if (query) {
      const haystack = [event.donor, event.message, event.stream_title, event.streamer, event.video_id, event.platform].join(" ").toLowerCase();
      if (!haystack.includes(query)) return false;
    }
    return true;
  };
  const powerchatControlValue = (selector, fallback = "") => {
    const element = document.querySelector(selector);
    return element ? String(element.value || fallback) : fallback;
  };
  const readPowerchatFilters = () => ({
    streamer: powerchatControlValue("[data-powerchat-filter-streamer]", "all"),
    platform: powerchatControlValue("[data-powerchat-filter-platform]", "all"),
    kind: powerchatControlValue("[data-powerchat-filter-kind]", "all"),
    from: powerchatControlValue("[data-powerchat-filter-from]", ""),
    to: powerchatControlValue("[data-powerchat-filter-to]", ""),
    search: powerchatControlValue("[data-powerchat-filter-search]", ""),
    page_size: powerchatPageSizeOptions.includes(Number(powerchatControlValue("[data-powerchat-page-size]", powerchatPageSizeDefault))) ? Number(powerchatControlValue("[data-powerchat-page-size]", powerchatPageSizeDefault)) : powerchatPageSizeDefault,
  });
  const setPowerchatSelectOptions = (selector, values, allLabel) => {
    const select = document.querySelector(selector);
    if (!select) return;
    const current = select.value || "all";
    const options = [`<option value="all">${escapeHtml(allLabel)}</option>`]
      .concat([...values].sort((a, b) => String(a).localeCompare(String(b))).map((value) => `<option value="${escapeAttr(value)}">${escapeHtml(value)}</option>`));
    select.innerHTML = options.join("");
    select.value = values.has(current) ? current : "all";
  };
  const updatePowerchatFilterOptions = (events) => {
    setPowerchatSelectOptions("[data-powerchat-filter-streamer]", new Set(events.map((event) => event.streamer).filter(Boolean)), "All streamers");
    setPowerchatSelectOptions("[data-powerchat-filter-platform]", new Set(events.map((event) => event.platform).filter(Boolean)), "All platforms");
    setPowerchatSelectOptions("[data-powerchat-filter-kind]", new Set(events.map((event) => event.kind).filter(Boolean)), "All kinds");
  };
  const buildPowerchatStreamerDashboards = (events, streamDurations) => {
    const streamers = new Map();
    events.forEach((event) => {
      const streamerName = event.streamer || "Unknown streamer";
      if (!streamers.has(streamerName)) {
        streamers.set(streamerName, {
          streamer: streamerName,
          event_count: 0,
          events_without_offset: 0,
          accumulator: newPowerchatAccumulator(),
          donors: new Map(),
          hours: new Map(),
          streams: new Map(),
        });
      }
      const streamer = streamers.get(streamerName);
      streamer.event_count += 1;
      addPowerchatEventToAccumulator(streamer.accumulator, event);

      const donorName = event.donor || "Unknown donor";
      if (!streamer.donors.has(donorName)) streamer.donors.set(donorName, { donor: donorName, event_count: 0, latest_received_at: "", accumulator: newPowerchatAccumulator() });
      const donor = streamer.donors.get(donorName);
      donor.event_count += 1;
      if (event.received_at && event.received_at > donor.latest_received_at) donor.latest_received_at = event.received_at;
      addPowerchatEventToAccumulator(donor.accumulator, event);

      const streamKey = event.video_id || event.stream_title || "unknown";
      if (!streamer.streams.has(streamKey)) {
        streamer.streams.set(streamKey, {
          streamer: streamerName,
          video_id: event.video_id || "",
          title: event.stream_title || "-",
          event_count: 0,
          duration_seconds: streamDurations.get(event.video_id) || 0,
          accumulator: newPowerchatAccumulator(),
        });
      }
      const stream = streamer.streams.get(streamKey);
      stream.event_count += 1;
      addPowerchatEventToAccumulator(stream.accumulator, event);

      if (event.hour_index === null || event.hour_index === undefined) {
        streamer.events_without_offset += 1;
        return;
      }
      const hourIndex = Number(event.hour_index || 0);
      if (!streamer.hours.has(hourIndex)) streamer.hours.set(hourIndex, { hour_index: hourIndex, hour_label: powerchatHourLabel(hourIndex), event_count: 0, accumulator: newPowerchatAccumulator() });
      const hour = streamer.hours.get(hourIndex);
      hour.event_count += 1;
      addPowerchatEventToAccumulator(hour.accumulator, event);
    });
    return [...streamers.values()].map((streamer) => {
      const streamRows = [...streamer.streams.values()].map((stream) => ({
        streamer: stream.streamer,
        video_id: stream.video_id,
        title: stream.title,
        event_count: stream.event_count,
        duration_seconds: stream.duration_seconds,
        money_totals: powerchatMoneyTotalsFromAccumulator(stream.accumulator),
        unit_totals: powerchatUnitTotalsFromAccumulator(stream.accumulator),
        money_rates: powerchatRatesFromAccumulator(stream.accumulator, stream.duration_seconds),
        sort_amount: powerchatSortAmount(stream.accumulator),
      })).sort((a, b) => (Number(b.sort_amount || 0) - Number(a.sort_amount || 0)) || (Number(b.event_count || 0) - Number(a.event_count || 0)) || String(a.title || "").localeCompare(String(b.title || "")));
      const durationSeconds = streamRows.reduce((total, stream) => total + Number(stream.duration_seconds || 0), 0);
      const donorRows = [...streamer.donors.values()].map((donor) => ({
        donor: donor.donor,
        event_count: donor.event_count,
        latest_received_at: donor.latest_received_at,
        money_totals: powerchatMoneyTotalsFromAccumulator(donor.accumulator),
        unit_totals: powerchatUnitTotalsFromAccumulator(donor.accumulator),
        sort_amount: powerchatSortAmount(donor.accumulator),
      })).sort((a, b) => (Number(b.sort_amount || 0) - Number(a.sort_amount || 0)) || (Number(b.event_count || 0) - Number(a.event_count || 0)) || String(a.donor || "").localeCompare(String(b.donor || "")));
      const hourlyRows = [...streamer.hours.values()].map((hour) => ({
        hour_index: hour.hour_index,
        hour_label: hour.hour_label,
        event_count: hour.event_count,
        money_totals: powerchatMoneyTotalsFromAccumulator(hour.accumulator),
        unit_totals: powerchatUnitTotalsFromAccumulator(hour.accumulator),
        average_money: powerchatAveragesFromAccumulator(hour.accumulator),
        sort_amount: powerchatSortAmount(hour.accumulator),
      })).sort((a, b) => Number(a.hour_index || 0) - Number(b.hour_index || 0));
      return {
        streamer: streamer.streamer,
        event_count: streamer.event_count,
        stream_count: streamRows.length,
        duration_seconds: durationSeconds,
        duration_hours: durationSeconds > 0 ? Math.round((durationSeconds / 3600) * 1000) / 1000 : 0,
        events_without_offset: streamer.events_without_offset,
        money_totals: powerchatMoneyTotalsFromAccumulator(streamer.accumulator),
        unit_totals: powerchatUnitTotalsFromAccumulator(streamer.accumulator),
        money_rates: powerchatRatesFromAccumulator(streamer.accumulator, durationSeconds),
        top_donors: donorRows.slice(0, 10),
        hourly_totals: hourlyRows,
        stream_totals: streamRows,
        sort_amount: powerchatSortAmount(streamer.accumulator),
      };
    }).sort((a, b) => (Number(b.sort_amount || 0) - Number(a.sort_amount || 0)) || (Number(b.event_count || 0) - Number(a.event_count || 0)) || String(a.streamer || "").localeCompare(String(b.streamer || "")));
  };
  const aggregatePowerchatEvents = (events, stats) => {
    const totals = newPowerchatAccumulator();
    const donors = new Map();
    const streams = new Map();
    const hours = new Map();
    const streamDurations = new Map((stats.stream_totals || []).map((stream) => [stream.video_id, Number(stream.duration_seconds || 0)]));
    let eventsWithoutOffset = 0;
    events.forEach((event) => {
      addPowerchatEventToAccumulator(totals, event);
      const donorName = event.donor || "Unknown donor";
      if (!donors.has(donorName)) donors.set(donorName, { donor: donorName, event_count: 0, latest_received_at: "", accumulator: newPowerchatAccumulator() });
      const donor = donors.get(donorName);
      donor.event_count += 1;
      if (event.received_at && event.received_at > donor.latest_received_at) donor.latest_received_at = event.received_at;
      addPowerchatEventToAccumulator(donor.accumulator, event);
      const streamKey = event.video_id || event.stream_title || "unknown";
      if (!streams.has(streamKey)) streams.set(streamKey, { video_id: event.video_id || "", streamer: event.streamer || "", title: event.stream_title || "-", event_count: 0, duration_seconds: streamDurations.get(event.video_id) || 0, accumulator: newPowerchatAccumulator() });
      const stream = streams.get(streamKey);
      stream.event_count += 1;
      addPowerchatEventToAccumulator(stream.accumulator, event);
      if (event.hour_index === null || event.hour_index === undefined) {
        eventsWithoutOffset += 1;
      } else {
        const hourIndex = Number(event.hour_index || 0);
        if (!hours.has(hourIndex)) hours.set(hourIndex, { hour_index: hourIndex, hour_label: powerchatHourLabel(hourIndex), event_count: 0, accumulator: newPowerchatAccumulator() });
        const hour = hours.get(hourIndex);
        hour.event_count += 1;
        addPowerchatEventToAccumulator(hour.accumulator, event);
      }
    });
    const durationSeconds = [...streams.values()].reduce((total, stream) => total + Number(stream.duration_seconds || 0), 0);
    const streamerDashboards = buildPowerchatStreamerDashboards(events, streamDurations);
    return {
      event_count: events.length,
      streams_with_powerchat: streams.size,
      duration_seconds: durationSeconds,
      events_without_offset: eventsWithoutOffset,
      money_totals: powerchatMoneyTotalsFromAccumulator(totals),
      unit_totals: powerchatUnitTotalsFromAccumulator(totals),
      money_rates: powerchatRatesFromAccumulator(totals, durationSeconds),
      top_donors: [...donors.values()].map((donor) => ({ donor: donor.donor, event_count: donor.event_count, latest_received_at: donor.latest_received_at, money_totals: powerchatMoneyTotalsFromAccumulator(donor.accumulator), unit_totals: powerchatUnitTotalsFromAccumulator(donor.accumulator), sort_amount: powerchatSortAmount(donor.accumulator) })).sort((a, b) => (b.sort_amount - a.sort_amount) || (b.event_count - a.event_count)).slice(0, 25),
      streamer_totals: streamerDashboards.map((streamer) => ({ streamer: streamer.streamer, event_count: streamer.event_count, stream_count: streamer.stream_count, duration_seconds: streamer.duration_seconds, duration_hours: streamer.duration_hours, money_totals: streamer.money_totals, unit_totals: streamer.unit_totals, money_rates: streamer.money_rates, sort_amount: streamer.sort_amount })),
      streamer_dashboards: streamerDashboards,
      stream_totals: [...streams.values()].map((stream) => ({ streamer: stream.streamer, video_id: stream.video_id, title: stream.title, event_count: stream.event_count, duration_seconds: stream.duration_seconds, money_totals: powerchatMoneyTotalsFromAccumulator(stream.accumulator), unit_totals: powerchatUnitTotalsFromAccumulator(stream.accumulator), money_rates: powerchatRatesFromAccumulator(stream.accumulator, stream.duration_seconds), sort_amount: powerchatSortAmount(stream.accumulator) })).sort((a, b) => (b.sort_amount - a.sort_amount) || (b.event_count - a.event_count)),
      hourly_totals: [...hours.values()].map((hour) => ({ hour_index: hour.hour_index, hour_label: hour.hour_label, event_count: hour.event_count, money_totals: powerchatMoneyTotalsFromAccumulator(hour.accumulator), unit_totals: powerchatUnitTotalsFromAccumulator(hour.accumulator), average_money: powerchatAveragesFromAccumulator(hour.accumulator) })).sort((a, b) => a.hour_index - b.hour_index),
    };
  };
  const renderPowerchatSummaryCards = (stats) => {
    const topDonor = (stats.top_donors || [])[0];
    const cards = [
      ["Total", formatPowerchatSummary(stats.money_totals || [], stats.unit_totals || []) || "-"],
      ["Per hour", formatPowerchatRates(stats.money_rates || []) || "-"],
      ["Events", String(stats.event_count || 0)],
      ["Top donor", topDonor ? topDonor.donor : "-"],
      ["Streams", String(stats.streams_with_powerchat || 0)],
      ["No offset", String(stats.events_without_offset || 0)],
    ];
    return cards.map(([label, value]) => `<div class="powerchat-summary-card"><strong>${escapeHtml(value)}</strong><span class="muted">${escapeHtml(label)}</span></div>`).join("");
  };
  const renderPowerchatStreamerSummaryCards = (stats) => {
    const topDonor = (stats.top_donors || [])[0];
    const cards = [
      ["Total", formatPowerchatSummary(stats.money_totals || [], stats.unit_totals || []) || "-"],
      ["Per hour", formatPowerchatRates(stats.money_rates || []) || "-"],
      ["Events", String(stats.event_count || 0)],
      ["Streams", String(stats.stream_count || 0)],
      ["Top donor", topDonor ? topDonor.donor : "-"],
      ["No offset", String(stats.events_without_offset || 0)],
    ];
    return cards.map(([label, value]) => `<div class="powerchat-summary-card"><strong>${escapeHtml(value)}</strong><span class="muted">${escapeHtml(label)}</span></div>`).join("");
  };
  const renderPowerchatStreamerDashboards = (rows) => {
    if (!rows || !rows.length) return '<div class="file-meta">No streamers with Powerchat events yet.</div>';
    return rows.map((row, index) => {
      const streamer = row.streamer || "Unknown streamer";
      const summary = formatPowerchatSummary(row.money_totals || [], row.unit_totals || []) || "-";
      const rate = formatPowerchatRates(row.money_rates || []) || "-";
      const open = index === 0 ? " open" : "";
      return `<details class="powerchat-streamer-card"${open}>
        <summary>
          <strong>${escapeHtml(streamer)}</strong>
          <span>Total: ${escapeHtml(summary)}</span>
          <span>Rate: ${escapeHtml(rate)}</span>
          <span>${escapeHtml(row.stream_count || 0)} streams</span>
          <span>${escapeHtml(row.event_count || 0)} events</span>
        </summary>
        <div class="powerchat-streamer-card-body">
          <div class="powerchat-export-actions">
            <a class="download action-button" href="${escapeAttr(powerchatExportUrl("json", { streamer }))}">Download JSON</a>
            <a class="download action-button" href="${escapeAttr(powerchatExportUrl("csv", { streamer }))}">Download CSV</a>
          </div>
          <div class="powerchat-summary-grid">${renderPowerchatStreamerSummaryCards(row)}</div>
          <div class="powerchat-dashboard-section">
            <h4>Donations Per Hour</h4>
            <div class="table-wrap"><table><thead><tr><th>Stream Hour</th><th>Events</th><th>Total</th><th>Average</th></tr></thead><tbody>${renderPowerchatHourlyRows(row.hourly_totals || [])}</tbody></table></div>
          </div>
          <div class="powerchat-dashboard-section">
            <h4>Streams</h4>
            <div class="table-wrap"><table><thead><tr><th>Streamer</th><th>Stream</th><th>Events</th><th>Total</th><th>Duration</th><th>Per hour</th></tr></thead><tbody>${renderPowerchatDashboardStreamRows(row.stream_totals || [])}</tbody></table></div>
          </div>
          <div class="powerchat-dashboard-section">
            <h4>Top Donors</h4>
            <div class="table-wrap"><table><thead><tr><th>Donor</th><th>Events</th><th>Total</th><th>Latest</th></tr></thead><tbody>${renderPowerchatDonorRows(row.top_donors || [])}</tbody></table></div>
          </div>
        </div>
      </details>`;
    }).join("");
  };
  const renderPowerchatHourlyRows = (rows) => {
    if (!rows || !rows.length) return '<tr><td colspan="4" class="file-meta">No hourly Powerchat events captured yet</td></tr>';
    return rows.map((row) => `<tr><td>${escapeHtml(row.hour_label || "-")}</td><td>${escapeHtml(row.event_count || 0)}</td><td>${escapeHtml(formatPowerchatSummary(row.money_totals || [], row.unit_totals || []) || "-")}</td><td>${escapeHtml(formatPowerchatSummary(row.average_money || [], []) || "-")}</td></tr>`).join("");
  };
  const renderPowerchatDashboardStreamRows = (rows) => {
    if (!rows || !rows.length) return '<tr><td colspan="6" class="file-meta">No streams with Powerchat events yet</td></tr>';
    return rows.slice(0, 50).map((row) => `<tr><td>${escapeHtml(row.streamer || "-")}</td><td class="file-name">${escapeHtml(row.title || row.video_id || "-")}</td><td>${escapeHtml(row.event_count || 0)}</td><td>${escapeHtml(formatPowerchatSummary(row.money_totals || [], row.unit_totals || []) || "-")}</td><td>${escapeHtml(formatDuration(row.duration_seconds || 0))}</td><td>${escapeHtml(formatPowerchatRates(row.money_rates || []) || "-")}</td></tr>`).join("");
  };
  const renderPowerchatDonorRows = (rows) => {
    if (!rows || !rows.length) return '<tr><td colspan="4" class="file-meta">No Powerchat donors yet</td></tr>';
    return rows.slice(0, 25).map((row) => `<tr><td>${escapeHtml(row.donor || "Unknown donor")}</td><td>${escapeHtml(row.event_count || 0)}</td><td>${escapeHtml(formatPowerchatSummary(row.money_totals || [], row.unit_totals || []) || "-")}</td><td>${escapeHtml(formatIso(row.latest_received_at || ""))}</td></tr>`).join("");
  };
  const renderPowerchatLedgerRows = (events) => {
    if (!events || !events.length) return '<tr><td colspan="7" class="file-meta">No Powerchat events captured yet</td></tr>';
    return events.map((event) => `<tr><td>${escapeHtml(powerchatEventTime(event))}</td><td>${escapeHtml(event.streamer || "-")}</td><td class="file-name">${escapeHtml(event.stream_title || event.video_id || "-")}</td><td>${escapeHtml(event.donor || "Unknown donor")}</td><td>${escapeHtml(powerchatEventAmountText(event) || "-")}</td><td>${escapeHtml(event.platform || "Powerchat")}</td><td class="log-message">${escapeHtml(event.message || "-")}</td></tr>`).join("");
  };
  const renderPowerchatDashboard = (stats) => {
    stats = stats || { events: [] };
    const allEvents = stats.events || [];
    updatePowerchatFilterOptions(allEvents);
    const filters = readPowerchatFilters();
    const filteredEvents = allEvents.filter((event) => powerchatDashboardEventMatches(event, filters));
    const filteredStats = aggregatePowerchatEvents(filteredEvents, stats);
    updatePowerchatExportLinks(filters);
    const maxPage = Math.max(1, Math.ceil(filteredEvents.length / filters.page_size));
    powerchatPage = Math.max(1, Math.min(powerchatPage, maxPage));
    const start = (powerchatPage - 1) * filters.page_size;
    const pageEvents = filteredEvents.slice(start, start + filters.page_size);
    const cards = byId("powerchat-summary-cards");
    if (cards) cards.innerHTML = renderPowerchatSummaryCards(filteredStats);
    const streamerRows = byId("powerchat-streamer-rows");
    if (streamerRows) streamerRows.innerHTML = renderPowerchatStreamerDashboards(filteredStats.streamer_dashboards || []);
    const hourly = byId("powerchat-hourly-rows");
    if (hourly) hourly.innerHTML = renderPowerchatHourlyRows(filteredStats.hourly_totals || []);
    const streams = byId("powerchat-stream-rows");
    if (streams) streams.innerHTML = renderPowerchatDashboardStreamRows(filteredStats.stream_totals || []);
    const donors = byId("powerchat-donor-rows");
    if (donors) donors.innerHTML = renderPowerchatDonorRows(filteredStats.top_donors || []);
    const ledger = byId("powerchat-ledger-rows");
    if (ledger) ledger.innerHTML = renderPowerchatLedgerRows(pageEvents);
    const state = byId("powerchat-ledger-state");
    if (state) state.textContent = filteredEvents.length ? `Showing ${start + 1}-${Math.min(start + pageEvents.length, filteredEvents.length)} of ${filteredEvents.length} events` : "Showing 0 events";
  };

  const renderFileAction = (file) => {
    const actions = [];
    if (file.download_url) {
      actions.push(`<a class="download" href="${escapeAttr(file.download_url)}">Download</a>`);
    }
    if (file.watermark_copy_id) {
      const recipient = file.watermark_recipient_label || file.watermark_copy_id;
      actions.push(`<span class="action-note" title="${escapeAttr(file.watermark_copy_id)}">Watermark: ${escapeHtml(recipient)}</span>`);
    }
    if (file.watermark_delete_url && file.watermark_copy_id) {
      actions.push(`<form class="inline-form" method="post" action="${escapeAttr(file.watermark_delete_url)}">
        <input type="hidden" name="copy_id" value="${escapeAttr(file.watermark_copy_id)}">
        <button class="download action-button" type="submit">Delete</button>
      </form>`);
    }
    if (file.refresh_chat_status === "running") {
      actions.push('<span class="action-note">Refreshing chat</span>');
    } else if (file.refresh_chat_url) {
      const label = file.refresh_chat_status === "failed"
        ? "Retry refresh"
        : (file.refresh_chat_status === "download" ? "Download chat replay" : "Refresh chat");
      actions.push(`<form class="inline-form" method="post" action="${escapeAttr(file.refresh_chat_url)}"><button class="download action-button" type="submit" title="Redownload chat replay or sync the recorded chat sidecar">${label}</button></form>`);
      if (file.refresh_chat_status === "failed" && file.refresh_chat_message) {
        actions.push(`<span class="action-note" title="${escapeAttr(file.refresh_chat_message)}">Refresh failed</span>`);
      }
    }
    if (file.render_chat_output_url) {
      actions.push(`<a class="download" href="${escapeAttr(file.render_chat_output_url)}">Chat video</a>`);
    } else if (file.render_chat_status === "rendering") {
      actions.push('<span class="action-note">Rendering chat</span>');
    } else if (file.render_chat_url) {
      const label = file.render_chat_status === "failed"
        ? "Retry chat"
        : (file.render_chat_status === "rendered" ? "Regenerate chat video" : "Render chat");
      const title = file.render_chat_status === "rendered"
        ? ' title="Re-render and replace the existing chat video"'
        : "";
      actions.push(`<form class="inline-form" method="post" action="${escapeAttr(file.render_chat_url)}"><button class="download action-button" type="submit"${title}>${label}</button></form>`);
      if (file.render_chat_status === "failed" && file.render_chat_message) {
        const message = String(file.render_chat_message || "").trim();
        const shortMessage = message.length > 140 ? `${message.slice(0, 137)}...` : message;
        actions.push(`<span class="action-note" title="${escapeAttr(message)}">Failed: ${escapeHtml(shortMessage)}</span>`);
      }
    }
    if (file.transcription_status === "running") {
      actions.push('<span class="action-note">Transcribing</span>');
    } else if (file.transcription_url) {
      const label = file.transcription_status === "failed"
        ? "Retry transcript"
        : (file.transcription_status === "transcribed" ? "Retranscribe" : "Transcribe");
      const title = file.transcription_status === "transcribed"
        ? ' title="Run WhisperX again and replace existing subtitle sidecars"'
        : "";
      actions.push(`<form class="inline-form" method="post" action="${escapeAttr(file.transcription_url)}"><button class="download action-button" type="submit"${title}>${label}</button></form>`);
      if (file.transcription_status === "failed" && file.transcription_message) {
        actions.push(`<span class="action-note" title="${escapeAttr(file.transcription_message)}">Transcript failed</span>`);
      }
    }
    if (file.event_detection_status === "running") {
      actions.push('<span class="action-note">Detecting events</span>');
    } else if (file.event_detection_url) {
      const label = file.event_detection_status === "failed"
        ? "Retry events"
        : (file.event_detection_status === "detected" ? "Redetect events" : "Detect events");
      const title = file.event_detection_status === "detected"
        ? ' title="Run content event detection again and replace the sidecar"'
        : "";
      actions.push(`<form class="inline-form" method="post" action="${escapeAttr(file.event_detection_url)}"><button class="download action-button" type="submit"${title}>${label}</button></form>`);
      if (file.event_detection_status === "failed" && file.event_detection_message) {
        actions.push(`<span class="action-note" title="${escapeAttr(file.event_detection_message)}">Event detection failed</span>`);
      }
    }
    if (file.watermark_url) {
      actions.push(`<form class="inline-form watermark-form" method="post" action="${escapeAttr(file.watermark_url)}">
        <input type="hidden" name="video_id" value="${escapeAttr(file.video_id)}">
        <input type="hidden" name="name" value="${escapeAttr(file.name)}">
        <input class="recipient-input" name="recipient_label" placeholder="Recipient" required maxlength="160">
        <button class="download action-button" type="submit">Watermark</button>
      </form>`);
    }
    for (const copy of (file.watermark_copies || []).slice(0, 5)) {
      if (copy.status === "done" && copy.download_url) {
        actions.push(`<a class="download" href="${escapeAttr(copy.download_url)}" title="${escapeAttr(copy.copy_id)}">${escapeHtml(copy.recipient_label)}</a>`);
      } else {
        const note = `${copy.recipient_label}: ${copy.status}`;
        actions.push(`<span class="action-note" title="${escapeAttr(copy.error || copy.message || copy.status)}">${escapeHtml(note)}</span>`);
      }
      if (copy.delete_url) {
        actions.push(`<form class="inline-form" method="post" action="${escapeAttr(copy.delete_url)}">
          <input type="hidden" name="copy_id" value="${escapeAttr(copy.copy_id)}">
          <button class="download action-button" type="submit">Delete copy</button>
        </form>`);
      }
    }
    return actions.length ? `<div class="actions">${actions.join("")}</div>` : "-";
  };

  const renderFileRow = (file) => {
    const action = renderFileAction(file);
    return [
      "<tr>",
      `<td class="file-name">${escapeHtml(file.name)}</td>`,
      `<td>${escapeHtml(file.segment || "-")}</td>`,
      `<td>${escapeHtml(file.format_id || "-")}</td>`,
      `<td class="kind">${escapeHtml(file.kind)}</td>`,
      `<td>${escapeHtml(formatEpoch(file.modified_at))}</td>`,
      `<td>${escapeHtml(formatBytes(file.size_bytes))}</td>`,
      `<td>${action}</td>`,
      "</tr>",
    ].join("");
  };

  const renderCleanupFragmentsAction = (stream) => {
    const videoId = String((stream && stream.video_id) || "");
    const count = Number(((stream && stream.file_kind_counts) || {}).fragment || 0);
    const blockedStatuses = new Set(["checking_after_exit", "downloading", "waiting_retry"]);
    if (!videoId || count <= 0 || blockedStatuses.has(String(stream.status || ""))) {
      return "";
    }
    const url = `/cleanup-fragments?video_id=${encodeURIComponent(videoId)}`;
    const label = count === 1 ? "Clean fragment" : `Clean fragments (${count})`;
    return `<form class="inline-form" method="post" action="${escapeAttr(url)}"><button class="download action-button" type="submit" title="Remove saved .part-Frag files for this stream">${escapeHtml(label)}</button></form>`;
  };

  const renderDeleteStreamAction = (stream) => {
    const videoId = String((stream && stream.video_id) || "");
    const blockedStatuses = new Set(["checking_after_exit", "detected", "downloading", "waiting_retry"]);
    if (!videoId || blockedStatuses.has(String(stream.status || ""))) {
      return "";
    }
    return `<form class="inline-form stream-delete-form" method="post" action="/delete-stream" onsubmit="return confirm('Delete this stream and all downloaded files? This cannot be undone.');">
      <input type="hidden" name="video_id" value="${escapeAttr(videoId)}">
      <input type="hidden" name="confirm_delete" value="delete_stream">
      <button class="download action-button danger-action" type="submit" title="Delete this stream record and its downloaded files">Delete stream</button>
    </form>`;
  };

  const jobDetailElapsed = (job) => {
    const details = job.details || {};
    if (details.elapsed) return String(details.elapsed);
    if (details.elapsed_seconds !== null && details.elapsed_seconds !== undefined && !Number.isNaN(Number(details.elapsed_seconds))) {
      return formatDuration(Number(details.elapsed_seconds));
    }
    if (job.started_at === null || job.started_at === undefined) return "-";
    const end = job.finished_at === null || job.finished_at === undefined ? Date.now() / 1000 : Number(job.finished_at);
    return formatDuration(Math.max(0, end - Number(job.started_at)));
  };

  const jobCurrentSize = (details) => {
    details = details || {};
    if (details.current_size) return String(details.current_size);
    if (details.current_size_bytes !== null && details.current_size_bytes !== undefined && !Number.isNaN(Number(details.current_size_bytes))) {
      return formatBytes(Number(details.current_size_bytes));
    }
    return "";
  };

  const renderJobCompactMeta = (job) => {
    const details = job.details || {};
    const parts = [];
    const elapsed = jobDetailElapsed(job);
    if (elapsed && elapsed !== "-") parts.push(`elapsed ${elapsed}`);
    const currentSize = jobCurrentSize(details);
    const currentLabel = String(details.current_label || "").trim();
    if (currentSize) parts.push(`${currentLabel || "output"} ${currentSize}`);
    const outputName = String(details.output_name || job.item || "").trim();
    if (outputName) parts.push(`output ${outputName}`);
    const extraDetail = String(job.detail || "").trim();
    if (extraDetail && !new Set([outputName, job.item, job.video_id]).has(extraDetail)) parts.push(extraDetail);
    if (!parts.length) parts.push(job.item || job.detail || job.video_id || "-");
    return parts.map((part) => `<span>${escapeHtml(part)}</span>`).join("");
  };

  const renderJobDetailToggle = (job) => {
    const details = job.details || {};
    const fields = [
      ["Media", details.media_name],
      ["Chat", details.chat_name],
      ["Output", details.output_name || job.item],
      ["Current file", details.current_name],
      ["Current size", jobCurrentSize(details)],
      ["Elapsed", jobDetailElapsed(job)],
      ["Updated", formatEpoch(job.updated_at)],
      ["Message", details.diagnostic || job.message],
    ];
    const rows = fields
      .filter(([_label, value]) => value !== null && value !== undefined && String(value).trim() && String(value).trim() !== "-")
      .map(([label, value]) => `<dt>${escapeHtml(label)}</dt><dd>${escapeHtml(value)}</dd>`)
      .join("");
    if (!rows) return "";
    return `<details class="streamer-job-details file-meta"><summary>Details</summary><dl class="streamer-job-details-grid">${rows}</dl></details>`;
  };

  const renderStreamJobRow = (job) => {
    return `<div class="streamer-job-row">
  <span class="badge ${escapeAttr(job.status)}">${escapeHtml(job.status || "-")}</span>
  <div class="streamer-job-body">
    <div class="streamer-job-heading"><span class="streamer-job-kind">${escapeHtml(job.kind || "Job")}</span><span class="streamer-job-phase muted">${escapeHtml(job.phase || job.message || "-")}</span></div>
    <div class="streamer-job-progress">${renderJobProgress(job.progress)}</div>
    <div class="streamer-job-detail streamer-job-meta file-meta">${renderJobCompactMeta(job)}</div>
    ${renderJobDetailToggle(job)}
  </div>
</div>`;
  };

  const renderStreamJobs = (jobs) => {
    jobs = jobs || [];
    if (!jobs.length) return '<div class="file-meta">No jobs have been seen for this stream.</div>';
    return `<div class="stream-job-list">${jobs.slice(0, 8).map(renderStreamJobRow).join("")}</div>`;
  };

  const renderStreamSpeakersPanel = (streamer, stream) => {
    const streamerName = String((streamer && streamer.name) || "");
    const videoId = String((stream && stream.video_id) || "");
    if (!streamerName || !(streamer && streamer.configured)) {
      return '<div class="file-meta">Create a streamer entry before making voice samples from detected speakers.</div>';
    }
    return `<div class="voice-lazy-panel stream-speakers-panel" data-stream-speakers data-streamer-name="${escapeAttr(streamerName)}" data-video-id="${escapeAttr(videoId)}">
  <button class="download action-button" type="button" data-load-stream-speakers>Load detected speakers</button>
  <span class="file-meta" data-stream-speakers-state>Detected transcript speakers are loaded only for this stream.</span>
</div>`;
  };

  const streamPlatform = (stream) => {
    const platform = String(stream && stream.platform || "").toLowerCase();
    if (sourcePlatforms.has(platform)) return platform;
    return detectSourcePlatform((stream && (stream.source || stream.url)) || "");
  };

  const renderStreamSourceMeta = (stream) => {
    const source = String((stream && stream.source) || "").trim();
    const channel = escapeHtml((stream && stream.channel) || "unknown channel");
    const videoId = escapeHtml((stream && stream.video_id) || "-");
    const url = escapeAttr((stream && stream.url) || "#");
    const prefix = source ? `${escapeHtml(source)} - ` : "";
    return `${prefix}${channel} - <a href="${url}">${videoId}</a>`;
  };

  const renderStreamVodRedownloadForm = (stream) => {
    const videoId = String((stream && stream.video_id) || "");
    const blockedStatuses = new Set(["detected", "downloading", "checking_after_exit", "waiting_retry"]);
    if (!videoId || blockedStatuses.has(String((stream && stream.status) || ""))) return "";
    const rawUrl = String((stream && stream.url) || "");
    const defaultUrl = rawUrl.startsWith("http://") || rawUrl.startsWith("https://") ? rawUrl : "";
    return `<details class="vod-download-box">
  <summary class="download action-button">Redownload from VOD</summary>
  <form class="vod-download-form" method="post" action="/vod-download">
    <input type="hidden" name="action" value="redownload">
    <input type="hidden" name="video_id" value="${escapeAttr(videoId)}">
    <label class="settings-field wide">VOD URL
      <input name="vod_url" value="${escapeAttr(defaultUrl)}" placeholder="Paste the VOD URL" required>
    </label>
    <button class="download action-button" type="submit">Download VOD Copy</button>
  </form>
</details>`;
  };

  const renderStreamCard = (stream, streamer) => {
    const title = stream.title || stream.video_id;
    const platform = streamPlatform(stream);
    const platformLabel = sourcePlatformLabels[platform] || sourcePlatformLabels.unknown;
    const platformInitial = sourcePlatformInitials[platform] || sourcePlatformInitials.unknown;
    const mixed = stream.has_mixed_formats ? "yes" : "no";
    const videoId = String(stream.video_id);
    const collapsed = streamIsCollapsed(videoId, stream.status);
    const collapsedClass = collapsed ? " collapsed" : "";
    const toggleLabel = collapsed ? "Expand" : "Collapse";
    const expanded = collapsed ? "false" : "true";
    const files = (stream.files || []).slice(0, 20).map(renderFileRow).join("")
      || '<tr><td colspan="7" class="file-meta">No files found</td></tr>';
    const filesTabId = `stream-tab-${videoId}-files`;
    const eventsTabId = `stream-tab-${videoId}-events`;
    const powerchatTabId = `stream-tab-${videoId}-powerchat`;
    const speakersTabId = `stream-tab-${videoId}-speakers`;
    const logTabId = `stream-tab-${videoId}-log`;
    const jobsTabId = `stream-tab-${videoId}-jobs`;
    const tabName = `stream-tabs-${videoId}`;
    const vodRedownload = renderStreamVodRedownloadForm(stream);
    const powerchatSummary = formatPowerchatSummary(stream.powerchat_money_totals || [], stream.powerchat_unit_totals || []);
    const dateValue = streamDateValue(stream);
    return `<section class="stream${collapsedClass}" data-video-id="${escapeAttr(videoId)}" data-stream-status="${escapeAttr(stream.status)}" data-stream-platform="${escapeAttr(platform)}" data-stream-title="${escapeAttr(title)}" data-stream-date="${escapeAttr(dateValue)}">
  <div class="stream-head">
    <div class="stream-title-block">
      ${renderPlatformIcon(platform, platformLabel, platformInitial)}
      <div class="stream-title-text">
        <div class="title">${escapeHtml(title)}</div>
        <div class="file-meta">${renderStreamSourceMeta(stream)}</div>
      </div>
    </div>
    <div class="stream-actions">
      ${renderCleanupFragmentsAction(stream)}
      ${renderDeleteStreamAction(stream)}
      <button class="download stream-toggle" type="button" data-stream-toggle="${escapeAttr(videoId)}" aria-expanded="${expanded}">${toggleLabel}</button>
      <span class="badge ${escapeAttr(stream.status)}">${escapeHtml(statusLabel(stream.status))}</span>
    </div>
  </div>
  <div class="stream-body">
    ${renderStreamSignals(stream)}
    <div class="meta">
      <div>Segment: ${String(stream.segment_index).padStart(3, "0")}</div>
      <div>Files: ${escapeHtml(stream.file_count)}</div>
      <div>Total size: ${escapeHtml(formatBytes(stream.total_bytes))}</div>
      <div>Partial size: ${escapeHtml(formatBytes(stream.part_bytes))}</div>
      <div>Final size: ${escapeHtml(formatBytes(stream.final_bytes))}</div>
      <div>Chat size: ${escapeHtml(formatBytes(stream.chat_bytes))}</div>
      <div>Fragment size: ${escapeHtml(formatBytes(stream.fragment_bytes))}</div>
      <div>Content events: ${escapeHtml(stream.content_event_count || 0)}</div>
      <div>Powerchat: ${escapeHtml(stream.powerchat_event_count || 0)} <span class="muted">${escapeHtml(powerchatSummary)}</span></div>
      <div>Mixed formats: ${mixed}</div>
      <div>Exit code: ${escapeHtml(formatOptionalInt(stream.exit_code))}</div>
      <div>Started: ${escapeHtml(formatIso(stream.last_started_at))}</div>
      <div>Exited: ${escapeHtml(formatIso(stream.last_exit_at))}</div>
      <div>Updated: ${escapeHtml(formatIso(stream.updated_at))}</div>
      <div>Latest file: ${escapeHtml(formatEpoch(stream.latest_file_modified_at))} <span class="muted">${escapeHtml(formatEpochAge(stream.latest_file_modified_at))}</span></div>
      <div class="wide">Kinds: ${escapeHtml(formatKindCounts(stream.file_kind_counts))}</div>
      <div class="wide">Directory: ${escapeHtml(stream.directory)}</div>
    </div>
    ${vodRedownload}
    <div class="stream-detail-tabs">
      <input class="stream-tab-radio stream-tab-files-toggle" type="radio" name="${escapeAttr(tabName)}" id="${escapeAttr(filesTabId)}" data-stream-tab="files" data-video-id="${escapeAttr(videoId)}" checked>
      <input class="stream-tab-radio stream-tab-events-toggle" type="radio" name="${escapeAttr(tabName)}" id="${escapeAttr(eventsTabId)}" data-stream-tab="events" data-video-id="${escapeAttr(videoId)}">
      <input class="stream-tab-radio stream-tab-powerchat-toggle" type="radio" name="${escapeAttr(tabName)}" id="${escapeAttr(powerchatTabId)}" data-stream-tab="powerchat" data-video-id="${escapeAttr(videoId)}">
      <input class="stream-tab-radio stream-tab-speakers-toggle" type="radio" name="${escapeAttr(tabName)}" id="${escapeAttr(speakersTabId)}" data-stream-tab="speakers" data-video-id="${escapeAttr(videoId)}">
      <input class="stream-tab-radio stream-tab-jobs-toggle" type="radio" name="${escapeAttr(tabName)}" id="${escapeAttr(jobsTabId)}" data-stream-tab="jobs" data-video-id="${escapeAttr(videoId)}">
      <input class="stream-tab-radio stream-tab-log-toggle" type="radio" name="${escapeAttr(tabName)}" id="${escapeAttr(logTabId)}" data-stream-tab="log" data-video-id="${escapeAttr(videoId)}">
      <div class="stream-tab-labels">
        <label class="stream-tab-files-label" for="${escapeAttr(filesTabId)}">Files</label>
        <label class="stream-tab-events-label" for="${escapeAttr(eventsTabId)}">Content Events</label>
        <label class="stream-tab-powerchat-label" for="${escapeAttr(powerchatTabId)}">Powerchat</label>
        <label class="stream-tab-speakers-label" for="${escapeAttr(speakersTabId)}">Detected Speakers</label>
        <label class="stream-tab-jobs-label" for="${escapeAttr(jobsTabId)}">Jobs</label>
        <label class="stream-tab-log-label" for="${escapeAttr(logTabId)}">Stream Log</label>
      </div>
      <div class="stream-tab-panels">
        <section class="stream-tab-panel stream-tab-files">
          <div class="table-wrap">
            <table class="files">
              <thead><tr><th>File</th><th>Segment</th><th>Format</th><th>Kind</th><th>Modified</th><th>Size</th><th>Action</th></tr></thead>
              <tbody>${files}</tbody>
            </table>
          </div>
        </section>
        <section class="stream-tab-panel stream-tab-events">${renderContentEvents(stream.content_events || [])}</section>
        <section class="stream-tab-panel stream-tab-powerchat">${renderPowerchatEvents(stream)}</section>
        <section class="stream-tab-panel stream-tab-speakers">${renderStreamSpeakersPanel(streamer, stream)}</section>
        <section class="stream-tab-panel stream-tab-jobs">${renderStreamJobs(stream.jobs || [])}</section>
        <section class="stream-tab-panel stream-tab-log">${renderStreamEvents(stream.events || [])}</section>
      </div>
    </div>
  </div>
</section>`;
  };

  const snapshotConfigPath = (snapshot) => String((((snapshot || {}).configuration || {}).Paths || {}).config_path || "-");

  const renderSourceChips = (sources) => {
    sources = sources || [];
    if (!sources.length) return '<div class="source-chips"><span class="source-chip">No configured sources</span></div>';
    return `<div class="source-chips">${sources.map((source) => {
      const platform = detectSourcePlatform(source);
      const label = sourcePlatformLabels[platform] || sourcePlatformLabels.unknown;
      const initial = sourcePlatformInitials[platform] || sourcePlatformInitials.unknown;
      return `<span class="source-chip">${renderPlatformIcon(platform, label, initial)}<span>${escapeHtml(source)}</span></span>`;
    }).join("")}</div>`;
  };

  const renderManualVodForm = (streamer) => {
    const name = String((streamer && streamer.name) || "");
    if (!name || !(streamer && streamer.configured)) return "";
    return `<section class="manual-vod-panel">
  <h4>Add VOD</h4>
  <form class="vod-download-form" method="post" action="/vod-download">
    <input type="hidden" name="action" value="manual">
    <input type="hidden" name="streamer_name" value="${escapeAttr(name)}">
    <label class="settings-field wide">VOD URL
      <input name="vod_url" placeholder="Paste a YouTube, Twitch, Kick, or Rumble VOD URL" required>
    </label>
    <button class="download action-button" type="submit">Add VOD</button>
  </form>
</section>`;
  };

  const renderStreamerForm = (streamer) => {
    const isExisting = Boolean(streamer && streamer.configured);
    const name = isExisting ? String(streamer.name || "") : "";
    const sources = isExisting ? (streamer.sources || []) : [];
    const downloadDirName = isExisting ? String(streamer.download_dir_name || "") : "";
    const powerchatEnabled = Boolean(streamer && streamer.powerchat_enabled);
    const powerchatUsername = isExisting ? String(streamer.powerchat_username || "") : "";
    const readonly = isExisting ? " readonly" : "";
    const deleteButton = isExisting
      ? '<button class="download action-button" name="action" value="delete" type="submit">Delete</button>'
      : "";
    const saveLabel = isExisting ? "Save Streamer" : "Add Streamer";
    const meta = isExisting
      ? `<span class="streamer-meta">Voice ${escapeHtml(streamer.voice_detection || "default")}; labels ${escapeHtml(streamer.speaker_label_count || 0)}</span>`
      : '<span class="streamer-meta">&nbsp;</span>';
    return `<form class="streamer-form" method="post" action="/streamers">
  <label class="settings-field">Name
    <input name="streamer_name" value="${escapeAttr(name)}"${readonly}>
  </label>
  <label class="settings-field">Download Dir Name
    <input name="download_dir_name" value="${escapeAttr(downloadDirName)}">
  </label>
  <label class="settings-field checkbox-field">Powerchat
    <span><input name="powerchat_enabled" type="checkbox" value="true"${powerchatEnabled ? " checked" : ""}> Listen for live support events</span>
  </label>
  <label class="settings-field">Powerchat Username
    <input name="powerchat_username" value="${escapeAttr(powerchatUsername)}" placeholder="Powerchat username">
  </label>
  ${meta}
  <div class="settings-field wide"><span>Sources</span>${renderSourceBuilder(sources)}</div>
  <div class="settings-actions">
    <button class="download action-button" name="action" value="save" type="submit">${saveLabel}</button>
    ${deleteButton}
  </div>
</form>`;
  };

  const renderSelect = (name, selected, options) => `<select name="${escapeAttr(name)}">${options.map((option) => `<option value="${escapeAttr(option)}"${String(option) === String(selected) ? " selected" : ""}>${escapeHtml(option)}</option>`).join("")}</select>`;

  const renderEventRuleSummary = (rule) => {
    rule = rule || {};
    const parts = [];
    if (Array.isArray(rule.labels) && rule.labels.length) parts.push(`labels: ${rule.labels.slice(0, 3).join(", ")}`);
    if (Array.isArray(rule.keywords) && rule.keywords.length) parts.push(`keywords: ${rule.keywords.slice(0, 3).join(", ")}`);
    if (rule.voice) parts.push(`voice: ${rule.voice}`);
    if (rule.min_loudness_dbfs !== null && rule.min_loudness_dbfs !== undefined) parts.push(`loudness >= ${rule.min_loudness_dbfs} dBFS`);
    if (rule.min_duration_seconds || rule.max_duration_seconds) parts.push(`duration ${rule.min_duration_seconds || 0}-${rule.max_duration_seconds || "any"}s`);
    return parts.length ? parts.join("; ") : "configure labels, keywords, or loudness";
  };

  const renderVoiceRuleControl = (selected, voices) => {
    const value = String(selected || "");
    const names = (voices || []).map((voice) => String(voice.name || voice || "").trim()).filter(Boolean);
    if (!names.length) {
      return `<input name="rule_voice" value="${escapeAttr(value)}" placeholder="Any voice">`;
    }
    const known = new Set(names);
    const options = [`<option value=""${value ? "" : " selected"}>Any voice</option>`];
    names.forEach((name) => {
      options.push(`<option value="${escapeAttr(name)}"${name === value ? " selected" : ""}>${escapeHtml(name)}</option>`);
    });
    if (value && !known.has(value)) {
      options.push(`<option value="${escapeAttr(value)}" selected>${escapeHtml(value)}</option>`);
    }
    return `<select name="rule_voice">${options.join("")}</select>`;
  };

  const renderEventRuleRow = (rule, index, isNew = false, voices = []) => {
    rule = rule || {};
    const labels = Array.isArray(rule.labels) ? rule.labels.join(", ") : "";
    const keywords = Array.isArray(rule.keywords) ? rule.keywords.join(", ") : "";
    const loudness = rule.min_loudness_dbfs === null || rule.min_loudness_dbfs === undefined ? "" : String(rule.min_loudness_dbfs);
    const minDuration = rule.min_duration_seconds ? String(rule.min_duration_seconds) : "";
    const maxDuration = rule.max_duration_seconds ? String(rule.max_duration_seconds) : "";
    const enabled = rule.enabled === false ? "false" : "true";
    const title = rule.name || "New content event";
    const deleteButton = isNew ? "" : `<button class="download action-button" name="rule_delete_${index}" value="true" type="submit">Delete</button>`;
    return `<details class="event-rule-card${!isNew && enabled === "false" ? " disabled" : ""}${isNew ? " event-rule-add" : ""}">
  <summary><span class="event-rule-title">${escapeHtml(title)}</span><span class="event-rule-summary">${escapeHtml(renderEventRuleSummary(rule))}</span><span class="event-rule-action">${isNew ? "Add" : "Edit"}</span></summary>
  <div class="event-rule-editor">
    <div class="event-rule-primary">
      <label class="settings-field">Name <input name="rule_name" value="${escapeAttr(rule.name || "")}" placeholder="Hype moment"></label>
      <label class="settings-field">Enabled ${renderSelect("rule_enabled", enabled, ["true", "false"])}</label>
      <label class="settings-field">Voice ${renderVoiceRuleControl(rule.voice || "", voices)}</label>
      <label class="settings-field">Severity <input name="rule_severity" value="${escapeAttr(rule.severity || "info")}" placeholder="info"></label>
    </div>
    <div class="event-rule-criteria">
      <label class="settings-field">Audio labels <input name="rule_labels" value="${escapeAttr(labels)}" placeholder="Laughter, Cheering"></label>
      <label class="settings-field">Transcript keywords <input name="rule_keywords" value="${escapeAttr(keywords)}" placeholder="keyword, phrase"></label>
      <label class="settings-field">Min loudness <input name="rule_min_loudness_dbfs" type="number" step="0.1" value="${escapeAttr(loudness)}" placeholder="dBFS"></label>
      <label class="settings-field">Min duration <input name="rule_min_duration_seconds" type="number" step="0.1" min="0" value="${escapeAttr(minDuration)}" placeholder="seconds"></label>
      <label class="settings-field">Max duration <input name="rule_max_duration_seconds" type="number" step="0.1" min="0" value="${escapeAttr(maxDuration)}" placeholder="seconds"></label>
    </div>
    <div class="settings-actions">${deleteButton}</div>
  </div>
</details>`;
  };

  const renderEventRuleRows = (rules, voices = []) => {
    rules = rules || [];
    const existing = rules.length
      ? rules.map((rule, index) => renderEventRuleRow(rule, index, false, voices)).join("")
      : '<div class="event-rule-empty">No content events configured yet.</div>';
    return `<div class="event-rule-list">${existing}${renderEventRuleRow({}, rules.length, true, voices)}</div>`;
  };

  const renderStreamerEventSettings = (streamer) => {
    const detection = streamer.stream_event_detection || {};
    const enabled = detection.enabled === null || detection.enabled === undefined ? "inherit" : (detection.enabled ? "true" : "false");
    const minConfidence = detection.min_confidence === null || detection.min_confidence === undefined || Number(detection.min_confidence) < 0 ? "" : String(detection.min_confidence);
    const rules = streamer.stream_event_rules || [];
    const voices = streamer.voices || [];
    const ruleCount = rules.length;
    return `<form class="event-rules-form" method="post" action="/stream-event-rules">
  <input type="hidden" name="scope" value="streamer">
  <input type="hidden" name="streamer_name" value="${escapeAttr(streamer.name || "")}">
  <fieldset class="event-settings-box">
    <legend>Detection</legend>
    <div class="settings-grid compact-grid">
      <label class="settings-field">Detection ${renderSelect("event_enabled", enabled, ["inherit", "true", "false"])}</label>
      <label class="settings-field">Model <input name="event_model" value="${escapeAttr(detection.model || "")}" placeholder="inherit"></label>
      <label class="settings-field">Device <input name="event_device" value="${escapeAttr(detection.device || "")}" placeholder="inherit"></label>
      <label class="settings-field">Window seconds <input name="event_window_seconds" type="number" step="0.001" min="0" value="${escapeAttr(detection.window_seconds || "")}" placeholder="inherit"></label>
      <label class="settings-field">Hop seconds <input name="event_hop_seconds" type="number" step="0.001" min="0" value="${escapeAttr(detection.hop_seconds || "")}" placeholder="inherit"></label>
      <label class="settings-field">Confidence <input name="event_min_confidence" type="number" step="0.001" min="0" max="1" value="${escapeAttr(minConfidence)}" placeholder="inherit"></label>
      <label class="settings-field">Max events <input name="event_max_events_per_media" type="number" min="1" value="${escapeAttr(detection.max_events_per_media || "")}" placeholder="inherit"></label>
    </div>
  </fieldset>
  <fieldset class="event-settings-box">
    <legend>Content Events</legend>
    <div class="event-rule-toolbar"><strong>Current Events</strong><span class="file-meta">${escapeHtml(ruleCount)} configured event${ruleCount === 1 ? "" : "s"}</span></div>
    ${renderEventRuleRows(rules, voices)}
  </fieldset>
  <div class="settings-actions"><button class="download action-button" type="submit">Save Content Events</button></div>
</form>`;
  };

  const renderStreamerSettingsArea = (streamer, snapshot) => {
    if (snapshotConfigPath(snapshot) === "-") {
      return `<div class="streamer-jobs">
  <h3>Settings</h3>
  <div class="file-meta">Config file path is not available.</div>
</div>`;
    }
    if (streamer.configured) {
      const rawKey = String(streamer.name || "streamer");
      const settingsKey = encodeURIComponent(rawKey).replace(/%/g, "-") || "streamer";
      const tabName = `streamer-settings-${settingsKey}`;
      const mainTabId = `${tabName}-main`;
      const voicesTabId = `${tabName}-voices`;
      const eventsTabId = `${tabName}-events`;
      return `<div class="streamer-settings">
  <h3>Settings</h3>
  <div class="streamer-settings-tabs">
    <input class="streamer-settings-main-toggle" type="radio" name="${escapeAttr(tabName)}" id="${escapeAttr(mainTabId)}" checked>
    <input class="streamer-settings-voices-toggle" type="radio" name="${escapeAttr(tabName)}" id="${escapeAttr(voicesTabId)}">
    <input class="streamer-settings-events-toggle" type="radio" name="${escapeAttr(tabName)}" id="${escapeAttr(eventsTabId)}">
    <div class="streamer-settings-tab-labels">
      <label class="streamer-settings-main-label" for="${escapeAttr(mainTabId)}">Streamer</label>
      <label class="streamer-settings-voices-label" for="${escapeAttr(voicesTabId)}">Voices</label>
      <label class="streamer-settings-events-label" for="${escapeAttr(eventsTabId)}">Content Events</label>
    </div>
    <div class="streamer-settings-panels">
      <section class="streamer-settings-panel streamer-settings-main">${renderStreamerForm(streamer)}${renderManualVodForm(streamer)}</section>
      <section class="streamer-settings-panel streamer-settings-voices">${renderStreamerVoiceSettings(streamer, snapshot)}</section>
      <section class="streamer-settings-panel streamer-settings-events">${renderStreamerEventSettings(streamer)}</section>
    </div>
  </div>
</div>`;
    }
    return `<div class="streamer-jobs">
  <h3>Settings</h3>
  <div class="file-meta">Create a streamer entry for these sources to share settings and voices.</div>
</div>`;
  };

  const renderStreamerJobRow = (job) => {
    return `<div class="streamer-job-row">
  <span class="badge ${escapeAttr(job.status)}">${escapeHtml(job.status || "-")}</span>
  <div class="streamer-job-body">
    <div class="streamer-job-heading"><span class="streamer-job-kind">${escapeHtml(job.kind || "Job")}</span><span class="streamer-job-phase muted">${escapeHtml(job.phase || job.message || "-")}</span></div>
    <div class="streamer-job-progress">${renderJobProgress(job.progress)}</div>
    <div class="streamer-job-detail streamer-job-meta file-meta">${renderJobCompactMeta(job)}</div>
    ${renderJobDetailToggle(job)}
  </div>
</div>`;
  };

  const renderStreamerJobsSummary = (jobs) => {
    jobs = jobs || [];
    const pageSize = Number(5);
    let body = '<div class="file-meta">No active or recent jobs for this streamer.</div>';
    if (jobs.length) {
      const pages = [];
      for (let index = 0; index < jobs.length; index += pageSize) pages.push(jobs.slice(index, index + pageSize));
      const panels = pages.map((pageJobs, index) => `<div class="streamer-job-page${index === 0 ? " is-active" : ""}" data-streamer-job-page="${index + 1}"${index === 0 ? "" : " hidden"}>${pageJobs.map(renderStreamerJobRow).join("")}</div>`).join("");
      const pager = pages.length > 1
        ? `<div class="streamer-job-pager"><span class="file-meta" data-streamer-job-page-state>Page 1 of ${pages.length}</span>${pages.map((_page, index) => `<button class="download action-button streamer-job-page-button" type="button" data-streamer-job-page-button="${index + 1}" aria-current="${index === 0 ? "page" : "false"}">${index + 1}</button>`).join("")}</div>`
        : "";
      body = `<div class="streamer-job-pages">${panels}</div>${pager}`;
    }
    return `<div class="streamer-jobs" data-streamer-jobs>
  <h3>Jobs</h3>
  ${body}
</div>`;
  };

  const renderStreamerStreamPlatformOptions = (streams, selected) => {
    const platforms = new Set((streams || []).map(streamPlatform).filter(Boolean));
    if (selected && selected !== "all") platforms.add(selected);
    const ordered = ["youtube", "twitch", "kick", "rumble", "unknown"].filter((platform) => platforms.has(platform));
    return [`<option value="all"${selected === "all" ? " selected" : ""}>All platforms</option>`]
      .concat(ordered.map((platform) => `<option value="${escapeAttr(platform)}"${selected === platform ? " selected" : ""}>${escapeHtml(sourcePlatformLabels[platform] || platform)}</option>`))
      .join("");
  };

  const renderStreamerStreamPageSizeOptions = (selected) => streamerStreamPageSizeOptions
    .map((size) => `<option value="${size}"${Number(selected) === size ? " selected" : ""}>${size}</option>`)
    .join("");

  const renderStreamerStreamBrowser = (streamer, streams) => {
    const key = String((streamer && streamer.name) || "");
    const state = streamerStreamFilterFor(key);
    const controls = `<div class="stream-browser-controls">
      <label>Platform <select data-stream-filter-control data-stream-filter-platform>${renderStreamerStreamPlatformOptions(streams, state.platform)}</select></label>
      <label>Search title <input data-stream-filter-control data-stream-filter-search value="${escapeAttr(state.search)}" placeholder="Search title or stream id"></label>
      <label>From <input data-stream-filter-control data-stream-filter-from type="date" value="${escapeAttr(state.from)}"></label>
      <label>To <input data-stream-filter-control data-stream-filter-to type="date" value="${escapeAttr(state.to)}"></label>
      <label>Per page <select data-stream-filter-control data-stream-page-size>${renderStreamerStreamPageSizeOptions(state.page_size)}</select></label>
    </div>`;
    const cards = streams.map((stream) => renderStreamCard(stream, streamer)).join("");
    return `<div class="stream-browser" data-stream-browser data-streamer-key="${escapeAttr(key)}">
      ${controls}
      <div class="stream-browser-footer">
        <span class="file-meta" data-stream-browser-state>Showing streams...</span>
        <div class="stream-browser-pager"><button class="download action-button" type="button" data-stream-page-prev>Previous</button><button class="download action-button" type="button" data-stream-page-next>Next</button></div>
      </div>
      <div class="stream-browser-list" data-stream-list>${cards}</div>
    </div>`;
  };

  const renderStreamerStreams = (streamer) => {
    const streams = (streamer && streamer.streams) || [];
    if (!streams.length) return '<section class="empty">No streams have been seen for this streamer.</section>';
    return renderStreamerStreamBrowser(streamer, streams);
  };

  const renderStreamerGroupingAction = (streamer, snapshot) => {
    if (!streamer.needs_grouping) return "";
    const disabled = snapshotConfigPath(snapshot) === "-" ? " disabled" : "";
    const sources = (streamer.sources || []).join(String.fromCharCode(10));
    return `<button class="download action-button" type="button" data-open-streamer-wizard data-streamer-name="${escapeAttr(streamer.name || "")}" data-streamer-sources="${escapeAttr(sources)}"${disabled}>Create Streamer</button>`;
  };

  const streamerDomId = (value) => String(value || "streamer").trim().replace(/[^A-Za-z0-9_-]+/g, "-").replace(/^-+|-+$/g, "") || "streamer";

  const renderVoiceAddMenu = (streamer) => {
    return `<details class="voice-add-menu">
  <summary class="download action-button">Add Voice</summary>
  <div class="voice-add-popover">
    <form class="voice-profile-form voice-task-form" method="post" action="/streamer-voices" enctype="multipart/form-data">
      <h3 class="voice-task-title">Add Voice</h3>
      <input type="hidden" name="streamer_name" value="${escapeAttr(streamer.name || "")}">
      <input type="hidden" name="action" value="save">
      <label>Voice name <input name="voice_name" required placeholder="Host"></label>
      <label>Matching <select name="enabled"><option value="true" selected>On</option><option value="false">Off</option></select></label>
      <label>Match threshold <input name="threshold" type="number" step="0.001" min="0" placeholder="default"></label>
      <label class="wide">Optional sample <input name="media" type="file" accept="audio/*,video/*"></label>
      <label class="wide">Notes <textarea name="notes" rows="2"></textarea></label>
      <button class="download action-button" type="submit">Add Voice</button>
    </form>
  </div>
</details>`;
  };

  const renderVoiceProfileForm = (streamer, voice) => {
    const samples = Array.isArray(voice.samples) ? voice.samples.join(String.fromCharCode(10)) : "";
    const enabled = voice.enabled ? "true" : "false";
    const status = voice.enabled ? "On" : "Off";
    const statusClass = voice.enabled ? "running" : "ended";
    return `<details class="voice-card">
  <summary><span class="voice-card-name">${escapeHtml(voice.name || "Voice")}</span><span class="badge ${statusClass}">${status}</span><span class="file-meta">${escapeHtml(voice.sample_count || 0)} samples</span><span class="voice-card-action">Edit</span></summary>
  <div class="voice-card-body">
    <form class="voice-profile-form" method="post" action="/streamer-voices">
      <input type="hidden" name="streamer_name" value="${escapeAttr(streamer.name || "")}">
      <input type="hidden" name="voice_name" value="${escapeAttr(voice.name || "")}">
      <div class="voice-profile-title"><strong>Edit Voice</strong></div>
      <label>Matching <select name="enabled"><option value="true"${enabled === "true" ? " selected" : ""}>On</option><option value="false"${enabled === "false" ? " selected" : ""}>Off</option></select></label>
      <label>Match threshold <input name="threshold" type="number" step="0.001" min="0" value="${escapeAttr(voice.threshold || "")}" placeholder="default"></label>
      <details class="voice-advanced wide"><summary>Advanced sample paths</summary><label>Sample files <textarea name="samples" rows="3">${escapeHtml(samples)}</textarea></label></details>
      <label class="wide">Notes <textarea name="notes" rows="2">${escapeHtml(voice.notes || "")}</textarea></label>
      <div class="settings-actions"><button class="download action-button" name="action" value="save" type="submit">Save Voice</button><button class="download action-button" name="action" value="delete" type="submit">Delete</button></div>
    </form>
    <form class="voice-profile-form voice-sample-form" method="post" action="/streamer-voice-samples" enctype="multipart/form-data">
      <input type="hidden" name="streamer_name" value="${escapeAttr(streamer.name || "")}">
      <input type="hidden" name="voice_name" value="${escapeAttr(voice.name || "")}">
      <div class="voice-profile-title"><strong>Upload Sample</strong><span class="file-meta">audio or video clip</span></div>
      <label>Sample file <input name="media" type="file" accept="audio/*,video/*" required></label>
      <button class="download action-button" type="submit">Upload Sample</button>
    </form>
  </div>
</details>`;
  };

  const renderVoiceLazyPanel = (streamer, message) => `<div class="voice-lazy-panel" data-voice-details data-streamer-name="${escapeAttr(streamer.name || "")}">
    <button class="download action-button" type="button" data-load-voice-details>Load voice details</button>
    <span class="file-meta" data-voice-details-state>${escapeHtml(message)}</span>
  </div>`;

  const loadStreamSpeakers = async (button) => {
    const panel = button.closest("[data-stream-speakers]");
    const streamerName = (panel && panel.getAttribute("data-streamer-name")) || "";
    const videoId = (panel && panel.getAttribute("data-video-id")) || "";
    const state = panel ? panel.querySelector("[data-stream-speakers-state]") : null;
    if (!panel || !streamerName || !videoId) return;
    if (state) state.textContent = "Loading detected speakers...";
    button.disabled = true;
    try {
      const response = await fetch(`/stream-voice-speakers?streamer=${encodeURIComponent(streamerName)}&video_id=${encodeURIComponent(videoId)}`, { cache: "no-store" });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const payload = await response.json();
      panel.innerHTML = payload.speakers || '<div class="file-meta">No diarized transcript speakers found yet. Transcribe this stream with voice detection first.</div>';
    } catch (error) {
      if (state) state.textContent = `Unable to load detected speakers: ${error.message || error}`;
      button.disabled = false;
    }
  };

  const loadVoiceDetails = async (button) => {
    const root = button.closest(".voice-settings");
    const panel = button.closest("[data-voice-details]");
    const streamerName = (panel && panel.getAttribute("data-streamer-name")) || "";
    const state = root ? root.querySelector("[data-voice-details-state]") : null;
    if (!root || !streamerName) return;
    if (state) state.textContent = "Loading voice details...";
    button.disabled = true;
    try {
      const response = await fetch(`/streamer-voice-details?streamer=${encodeURIComponent(streamerName)}`, { cache: "no-store" });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const payload = await response.json();
      const review = root.querySelector("[data-voice-review]");
      if (review) review.innerHTML = payload.review || '<div class="file-meta">No voice matches found.</div>';
      if (state) state.textContent = "Voice details loaded.";
    } catch (error) {
      if (state) state.textContent = `Unable to load voice details: ${error.message || error}`;
      button.disabled = false;
    }
  };

  const renderStreamerVoiceSettings = (streamer, snapshot) => {
    if (!streamer.configured) return '<div class="file-meta">Create a streamer entry before adding voices.</div>';
    const tabId = `voice-settings-${streamerDomId(streamer.name)}`;
    const backend = (((snapshot || {}).configuration || {}).Transcription || {}).voice_match_backend || {};
    const voices = streamer.voices || [];
    const profiles = voices.length ? `<div class="voice-list">${voices.map((voice) => renderVoiceProfileForm(streamer, voice)).join("")}</div>` : '<div class="file-meta">No known voices yet.</div>';
    const addMenu = renderVoiceAddMenu(streamer);
    const review = renderVoiceLazyPanel(streamer, "Voice match review rows are loaded only when requested.");
    return `<div class="voice-settings">
  <div class="voice-manager-head"><h2>${escapeHtml(streamer.name || "Streamer")} Voices</h2><div class="voice-manager-actions">${addMenu}</div></div>
  <div class="voice-manager-note file-meta">${escapeHtml(backend.message || "")}</div>
  <div class="voice-tabs">
    <input id="${escapeAttr(tabId)}-known" name="${escapeAttr(tabId)}-tab" type="radio" checked><label for="${escapeAttr(tabId)}-known">Known Voices</label><section>${profiles}</section>
    <input id="${escapeAttr(tabId)}-review" name="${escapeAttr(tabId)}-tab" type="radio"><label for="${escapeAttr(tabId)}-review">Review Matches</label><section data-voice-review>${review}</section>
  </div>
</div>`;
  };

  const streamerActiveJobCount = (streamer) => (streamer && streamer.jobs || [])
    .filter((job) => ["queued", "running"].includes(job.status)).length;

  const renderStreamerCard = (streamer, snapshot) => {
    const stateLabel = streamer.needs_grouping ? "Needs Grouping" : "Configured";
    const stateClass = streamer.needs_grouping ? "waiting_retry" : "done";
    const activeJobs = streamerActiveJobCount(streamer);
    const expandsDefault = streamerExpandsByDefault(streamer);
    const collapsedClass = expandsDefault ? "" : " collapsed";
    const toggleLabel = expandsDefault ? "Collapse" : "Expand";
    const toggleExpanded = expandsDefault ? "true" : "false";
    const needsClass = streamer.needs_grouping ? " needs-grouping" : "";
    const warning = streamer.needs_grouping ? '<div class="signals">Needs streamer group</div>' : "";
    const groupAction = renderStreamerGroupingAction(streamer, snapshot);
    const latestAge = formatEpochAge(streamer.latest_activity_at);
    return `<section class="streamer-section${needsClass}${collapsedClass}" data-streamer-key="${escapeAttr(streamer.name || "")}" data-streamer-name="${escapeAttr(streamer.name || "")}" data-streamer-active="${escapeAttr(streamer.active_count || 0)}" data-streamer-attention="${escapeAttr(streamer.attention_count || 0)}" data-streamer-active-jobs="${escapeAttr(activeJobs)}" data-streamer-needs-grouping="${streamer.needs_grouping ? "true" : "false"}">
  <div class="streamer-head">
    <div class="streamer-title">
      <h2>${escapeHtml(streamer.name || "unknown streamer")}</h2>
      ${renderSourceChips(streamer.sources || [])}
    </div>
    <div class="streamer-badges">
      <span class="badge ${stateClass}">${stateLabel}</span>
      <span class="badge downloading">Active ${escapeHtml(streamer.active_count || 0)}</span>
      <span class="badge checking_after_exit">Attention ${escapeHtml(streamer.attention_count || 0)}</span>
      <span class="badge">Storage ${escapeHtml(formatBytes(streamer.total_bytes))}</span>
      <span class="badge">Latest ${escapeHtml(latestAge || "-")}</span>
      ${groupAction}
      <button class="download streamer-settings-toggle" type="button" data-streamer-settings-toggle="${escapeAttr(streamer.name || "")}" aria-expanded="false">Settings</button>
      <button class="download streamer-toggle" type="button" data-streamer-toggle="${escapeAttr(streamer.name || "")}" aria-expanded="${toggleExpanded}">${toggleLabel}</button>
    </div>
  </div>
  <div class="streamer-details">
    ${warning}
    <div class="streamer-stat-grid">
      <div><strong>${escapeHtml(streamer.stream_count || 0)}</strong><br><span class="muted">Streams</span></div>
      <div><strong>${escapeHtml(streamer.file_count || 0)}</strong><br><span class="muted">Files</span></div>
      <div><strong>${escapeHtml(formatBytes(streamer.total_bytes))}</strong><br><span class="muted">Storage</span></div>
      <div><strong>${escapeHtml(formatBytes(streamer.final_bytes))}</strong><br><span class="muted">Final</span></div>
      <div><strong>${escapeHtml(formatBytes(streamer.part_bytes))}</strong><br><span class="muted">Partial</span></div>
      <div><strong>${escapeHtml(formatBytes(streamer.chat_bytes))}</strong><br><span class="muted">Chat</span></div>
      <div><strong>${escapeHtml(formatEpoch(streamer.latest_activity_at))}</strong><br><span class="muted">Latest ${escapeHtml(latestAge)}</span></div>
      <div><strong>${escapeHtml(streamer.download_dir_name || "-")}</strong><br><span class="muted">Download dir</span></div>
      <div><strong>${escapeHtml(streamer.voice_detection || "default")}</strong><br><span class="muted">Voice</span></div>
      <div><strong>${escapeHtml(streamer.speaker_label_count || 0)}</strong><br><span class="muted">Speaker labels</span></div>
      <div><strong>${escapeHtml((streamer.voices || []).length)}</strong><br><span class="muted">Known voices</span></div>
    </div>
    <div class="streamer-settings-panel" data-streamer-settings-panel hidden>
      ${renderStreamerSettingsArea(streamer, snapshot)}
    </div>
    <div class="streamer-body-grid">
      ${renderStreamerJobsSummary(streamer.jobs || [])}
    </div>
    <div class="streamer-streams">
      <h3>Streams</h3>
      ${renderStreamerStreams(streamer)}
    </div>
  </div>
</section>`;
  };

  const renderStreamerList = (snapshot) => {
    const streamers = snapshot.streamer_stats || [];
    return streamers.length
      ? streamers.map((streamer) => renderStreamerCard(streamer, snapshot)).join("")
      : '<section class="empty">No streamers or streams have been seen yet.</section>';
  };

  const activeJobCount = (jobs) => (jobs || []).filter((job) => ["queued", "running"].includes(job.status)).length;

  const renderJobProgress = (value) => {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
    const percent = Math.max(0, Math.min(100, Math.round(Number(value) * 100)));
    return `<div class="job-progress"><progress max="100" value="${percent}"></progress><span>${percent}%</span></div>`;
  };

  const renderJobRows = (jobs) => {
    if (!jobs || !jobs.length) {
      return '<tr><td colspan="11" class="file-meta">No dashboard jobs have been seen yet</td></tr>';
    }
    return jobs.map((job) => {
      const started = job.started_at === null || job.started_at === undefined ? "-" : formatEpoch(job.started_at);
      const updated = job.updated_at === null || job.updated_at === undefined ? "-" : formatEpoch(job.updated_at);
      const end = job.finished_at === null || job.finished_at === undefined ? Date.now() / 1000 : Number(job.finished_at);
      const duration = job.started_at === null || job.started_at === undefined ? "-" : formatDuration(Math.max(0, end - Number(job.started_at)));
      return [
        "<tr>",
        `<td><span class="badge ${escapeAttr(job.status)}">${escapeHtml(job.status || "-")}</span></td>`,
        `<td>${renderJobProgress(job.progress)}</td>`,
        `<td>${escapeHtml(job.kind || "-")}</td>`,
        `<td class="file-name">${escapeHtml(job.phase || "-")}</td>`,
        `<td class="file-name">${escapeHtml(job.video_id || "-")}</td>`,
        `<td class="file-name">${escapeHtml(job.item || "-")}</td>`,
        `<td class="file-name"><div class="streamer-job-meta file-meta">${renderJobCompactMeta(job)}</div>${renderJobDetailToggle(job)}</td>`,
        `<td>${escapeHtml(started)}</td>`,
        `<td>${escapeHtml(updated)}</td>`,
        `<td>${escapeHtml(duration)}</td>`,
        `<td class="log-message">${escapeHtml(job.message || "-")}</td>`,
        "</tr>",
      ].join("");
    }).join("");
  };

  const renderLogRows = (logs) => {
    if (!logs || !logs.length) {
      return '<tr><td colspan="4" class="file-meta">No in-process logs captured yet</td></tr>';
    }
    return [...logs].reverse().map((entry) => [
      "<tr>",
      `<td>${escapeHtml(formatEpoch(entry.created_at))}</td>`,
      `<td class="level-${escapeAttr(entry.level)}">${escapeHtml(entry.level)}</td>`,
      `<td class="file-name">${escapeHtml(entry.logger)}</td>`,
      `<td class="log-message">${escapeHtml(entry.message)}</td>`,
      "</tr>",
    ].join("")).join("");
  };

  const renderConfigSections = (configuration) => Object.entries(configuration || {}).map(([section, values]) => {
    const rows = Object.entries(values || {}).map(([name, value]) => (
      `<tr><td>${escapeHtml(name)}</td><td class="config-value">${escapeHtml(formatConfigValue(value))}</td></tr>`
    )).join("");
    return `<section class="panel">
  <h2>${escapeHtml(section)}</h2>
  <div class="table-wrap">
    <table class="config-table">
      <thead><tr><th>Setting</th><th>Value</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>
  </div>
</section>`;
  }).join("");

  const applyAbout = (app) => {
    app = app || {};
    setText("about-app-name", app.name || "ONLYSAVEmeVODS");
    setText("about-app-version", app.version || "-");
    setText("about-python-version", app.python_version || "-");
    setText("about-platform", app.platform || "-");
    setText("about-executable", app.executable || "-");
  };

  let lastStreamRevision = document.body ? String(document.body.getAttribute("data-stream-revision") || "") : "";
  let lastJobRevision = document.body ? String(document.body.getAttribute("data-job-revision") || "") : "";
  let refreshPollCount = 0;
  const fullRefreshEvery = 8;

  const applySnapshot = (snapshot) => {
    const isLite = snapshot.detail === "lite";
    const streams = snapshot.streams || [];
    const counts = snapshot.counts || {};
    const streamCount = isLite ? Number(snapshot.stream_count || 0) : streams.length;
    const attentionCount = isLite ? Number(snapshot.attention_count || 0) : streams.filter(streamNeedsAttention).length;
    setText("metric-total", streamCount);
    setText("metric-downloading", counts.downloading || 0);
    setText("metric-checking", counts.checking_after_exit || 0);
    setText("metric-attention", attentionCount);
    setText("metric-streamers", isLite ? Number(snapshot.streamer_count || 0) : (snapshot.streamer_stats || []).length);
    setText("metric-jobs", activeJobCount(snapshot.jobs || []));
    setText("metric-logs", (snapshot.recent_logs || []).length);
    setText("updated-at", `Updated ${formatEpoch(snapshot.generated_at)}`);
    setText("refresh-state", isLite ? "Refresh 15s lite" : "Refresh 15s");

    if (!isLite) {
      lastStreamRevision = String(snapshot.stream_revision || "");
      lastJobRevision = String(snapshot.job_revision || "");
      setText("metric-storage", formatBytes(snapshot.total_bytes));
      setText("metric-partial", formatBytes(snapshot.part_bytes));
      setText("storage-final", formatBytes(snapshot.final_bytes));
      setText("storage-part", formatBytes(snapshot.part_bytes));
      setText("storage-chat", formatBytes(snapshot.chat_bytes));
      setText("storage-fragment", formatBytes(snapshot.fragment_bytes));
      setText("storage-state", formatBytes(snapshot.state_bytes));
      setText("storage-temporary", formatBytes(snapshot.temporary_bytes));
      setText("runtime-download-dir", snapshot.download_dir || "-");
      setText("runtime-state-db", snapshot.state_db || "-");
      setText("runtime-stream-limit", snapshot.stream_limit);

      const statusCounts = byId("status-counts");
      if (statusCounts) statusCounts.innerHTML = renderStatusCounts(counts);
      const streamerList = byId("streamer-list");
      if (streamerList) {
        if (!streamerListIsEditing(streamerList)) {
          streamerList.innerHTML = renderStreamerList(snapshot);
          applyCollapsedState(streamerList);
        }
      }
      const configSections = byId("config-sections");
      if (configSections) configSections.innerHTML = renderConfigSections(snapshot.configuration || {});
      latestPowerchatStats = snapshot.powerchat_stats || { events: [] };
      renderPowerchatDashboard(latestPowerchatStats);
    }

    if (isLite) {
      lastJobRevision = String(snapshot.job_revision || "");
    }
    const jobRows = byId("job-rows");
    if (jobRows) jobRows.innerHTML = renderJobRows(snapshot.jobs || []);
    const logRows = byId("log-rows");
    if (logRows) logRows.innerHTML = renderLogRows(snapshot.recent_logs || []);
    applyAbout(snapshot.app || {});
  };

  const shouldFetchFullSnapshot = (liteSnapshot) => {
    refreshPollCount += 1;
    const revision = String(liteSnapshot.stream_revision || "");
    const jobRevision = String(liteSnapshot.job_revision || "");
    if (lastStreamRevision && revision && revision !== lastStreamRevision) return true;
    if (lastJobRevision && jobRevision && jobRevision !== lastJobRevision) return true;
    if (refreshPollCount % fullRefreshEvery !== 0) return false;
    const streamerList = byId("streamer-list");
    return !streamerList || !streamerListIsEditing(streamerList);
  };

  const refreshStatus = async () => {
    try {
      const response = await fetch("/status.json?lite=1", { cache: "no-store" });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const snapshot = await response.json();
      if (snapshot.detail === "lite" && shouldFetchFullSnapshot(snapshot)) {
        const fullResponse = await fetch("/status.json?dashboard=1", { cache: "no-store" });
        if (!fullResponse.ok) throw new Error(`HTTP ${fullResponse.status}`);
        applySnapshot(await fullResponse.json());
        return;
      }
      applySnapshot(snapshot);
    } catch (error) {
      setText("refresh-state", `Refresh failed: ${error.message || error}`);
    }
  };

  latestPowerchatStats = readInitialPowerchatStats() || { events: [] };
  renderPowerchatDashboard(latestPowerchatStats);
  window.setInterval(refreshStatus, 15000);
})();
</script>"""


def render_about_panel(snapshot: StatusSnapshot) -> str:
    generated = time.strftime(
        "%Y-%m-%d %H:%M:%S %Z",
        time.localtime(snapshot.generated_at),
    )
    return f"""<section class="panel">
  <div class="about-heading">
    <img class="about-icon" src="/Favicon.png?v={escape(APP_VERSION, quote=True)}" alt="" loading="lazy">
    <div class="about-title">
      <h2>About</h2>
      <div class="file-meta">ONLYSAVEmeVODS dashboard</div>
    </div>
  </div>
  <dl>
    <dt>Application</dt><dd id="about-app-name">{escape(snapshot.app.name)}</dd>
    <dt>Version</dt><dd id="about-app-version">{escape(snapshot.app.version)}</dd>
    <dt>Python</dt><dd id="about-python-version">{escape(snapshot.app.python_version)}</dd>
    <dt>Platform</dt><dd id="about-platform">{escape(snapshot.app.platform)}</dd>
    <dt>Executable</dt><dd id="about-executable">{escape(snapshot.app.executable)}</dd>
    <dt>Status generated</dt><dd>{escape(generated)}</dd>
  </dl>
</section>"""


def voice_detection_overrides_for_summary(config: BotConfig) -> dict[str, str]:
    overrides = {
        channel: voice_detection_config_summary(override)
        for channel, override in sorted(config.channel_voice_detection.items())
    }
    for streamer_name, streamer in sorted(config.streamers.items()):
        if streamer.voice_detection is not None and streamer_name not in overrides:
            overrides[streamer_name] = (
                "streamer default: "
                f"{voice_detection_config_summary(streamer.voice_detection)}"
            )
    return overrides


def streamer_summary_for_config(config: BotConfig) -> dict[str, Any]:
    streamers = [
        f"{name}: {', '.join(streamer.sources)}"
        for name, streamer in sorted(config.streamers.items())
    ]
    voice_detection = [
        f"{name}: {voice_detection_config_summary(streamer.voice_detection)}"
        for name, streamer in sorted(config.streamers.items())
        if streamer.voice_detection is not None
    ]
    speaker_labels = [
        f"{name}: {len(streamer.speaker_labels)} labels"
        for name, streamer in sorted(config.streamers.items())
        if streamer.speaker_labels
    ]
    return {
        "count": len(config.streamers),
        "streamers": streamers,
        "shared_voice_detection": voice_detection,
        "shared_speaker_labels": speaker_labels,
    }


def voice_detection_config_summary(override: VoiceDetectionConfig) -> str:
    if override.mode == "off":
        return "off"
    if override.mode == "auto":
        return "auto"
    if override.mode == "fixed":
        return f"fixed, exactly {override.min_speakers}"
    if override.min_speakers and override.max_speakers:
        speakers = f"{override.min_speakers}-{override.max_speakers}"
    elif override.min_speakers:
        speakers = f"at least {override.min_speakers}"
    else:
        speakers = f"up to {override.max_speakers}"
    return f"range, {speakers}"



def render_streamer_dashboard(snapshot: StatusSnapshot) -> str:
    if not snapshot.streamer_stats:
        return '<section class="empty">No streamers or streams have been seen yet.</section>'
    return "\n".join(
        render_streamer_card(streamer, snapshot)
        for streamer in snapshot.streamer_stats
    )


def render_streamer_toolbar(snapshot: StatusSnapshot) -> str:
    disabled = ' disabled' if snapshot_config_path(snapshot) == "-" else ""
    note = (
        '<span class="file-meta">Config file path is not available.</span>'
        if disabled
        else ""
    )
    return f"""<div class="streamer-toolbar">
  <button class="download action-button primary-action" type="button" data-open-streamer-wizard{disabled}>Add Streamer</button>
  {note}
</div>"""


def render_streamer_wizard(snapshot: StatusSnapshot) -> str:
    if snapshot_config_path(snapshot) == "-":
        return ""
    return f"""<dialog class="streamer-wizard" id="streamer-wizard">
  <form class="streamer-wizard-form" id="streamer-wizard-form" method="post" action="/streamers">
    <input type="hidden" name="form_kind" value="streamer_wizard">
    <input type="hidden" name="action" value="save">
    <div class="wizard-head">
      <h2>Add Streamer</h2>
      <button class="download action-button" type="button" data-close-streamer-wizard>Close</button>
    </div>
    <div class="wizard-steps" aria-hidden="true">
      <span class="wizard-step-tab active" data-wizard-step-tab="0">1 Streamer</span>
      <span class="wizard-step-tab" data-wizard-step-tab="1">2 Voice</span>
      <span class="wizard-step-tab" data-wizard-step-tab="2">3 Speakers</span>
    </div>
    <section class="wizard-step" data-wizard-step="0">
      <div class="settings-grid">
        <label class="settings-field">Name
          <input id="streamer-wizard-name" name="streamer_name" required>
        </label>
        <label class="settings-field">Download Dir Name
          <input id="streamer-wizard-download-dir" name="download_dir_name">
        </label>
        <label class="settings-field wide">Sources
          <textarea id="streamer-wizard-sources" name="sources" rows="4" required></textarea>
        </label>
      </div>
    </section>
    <section class="wizard-step" data-wizard-step="1" hidden>
      <div class="voice-form voice-default-form">
        <label>Mode {render_voice_detection_mode_select("mode", "inherit", include_inherit=True)}</label>
        <label>Speakers <input class="small-input" name="speakers" type="number" min="1" placeholder="fixed"></label>
        <label>Min <input class="small-input" name="min_speakers" type="number" min="1" placeholder="range"></label>
        <label>Max <input class="small-input" name="max_speakers" type="number" min="1" placeholder="range"></label>
        <label>Token env <input class="env-input" name="hf_token_env" placeholder="HF_TOKEN"></label>
      </div>
    </section>
    <section class="wizard-step" data-wizard-step="2" hidden>
      <div class="wizard-speaker-rows" id="streamer-wizard-speaker-rows">
        {render_speaker_label_pair("", "", readonly=False)}
      </div>
      <button class="download action-button" type="button" data-add-wizard-speaker>Add Speaker Row</button>
    </section>
    <div class="wizard-footer">
      <button class="download action-button" type="button" data-wizard-back hidden>Back</button>
      <span class="file-meta" data-wizard-progress>Step 1 of 3</span>
      <div class="actions">
        <button class="download action-button" type="button" data-wizard-next>Next</button>
        <button class="download action-button primary-action" type="submit" data-wizard-submit hidden>Create Streamer</button>
      </div>
    </div>
  </form>
</dialog>"""


def streamer_active_job_count(streamer: StreamerStatStatus) -> int:
    return sum(1 for job in streamer.jobs if job.status in {"queued", "running"})


def streamer_expands_by_default(streamer: StreamerStatStatus) -> bool:
    return (
        streamer.needs_grouping
        or streamer.active_count > 0
        or streamer.attention_count > 0
        or streamer_active_job_count(streamer) > 0
    )


def render_streamer_card(streamer: StreamerStatStatus, snapshot: StatusSnapshot) -> str:
    state_label = "Needs Grouping" if streamer.needs_grouping else "Configured"
    state_class = "waiting_retry" if streamer.needs_grouping else "done"
    active_jobs = streamer_active_job_count(streamer)
    expands_default = streamer_expands_by_default(streamer)
    needs_class = " needs-grouping" if streamer.needs_grouping else ""
    collapsed_class = "" if expands_default else " collapsed"
    toggle_label = "Collapse" if expands_default else "Expand"
    toggle_expanded = "true" if expands_default else "false"
    warning = (
        '<div class="signals">Needs streamer group</div>'
        if streamer.needs_grouping
        else ""
    )
    group_action = render_needs_grouping_action(streamer, snapshot)
    settings = render_streamer_settings_area(streamer, snapshot)
    jobs = render_streamer_jobs_summary(streamer.jobs)
    streams = render_streamer_streams(streamer)
    latest_activity = format_optional_epoch(streamer.latest_activity_at)
    latest_activity_age = format_epoch_age(streamer.latest_activity_at)
    streamer_key = escape(streamer.name, quote=True)
    return f"""<section class="streamer-section{needs_class}{collapsed_class}" data-streamer-key="{streamer_key}" data-streamer-name="{streamer_key}" data-streamer-active="{streamer.active_count}" data-streamer-attention="{streamer.attention_count}" data-streamer-active-jobs="{active_jobs}" data-streamer-needs-grouping="{'true' if streamer.needs_grouping else 'false'}">
  <div class="streamer-head">
    <div class="streamer-title">
      <h2>{escape(streamer.name)}</h2>
      {render_source_chips(streamer.sources)}
    </div>
    <div class="streamer-badges">
      <span class="badge {state_class}">{escape(state_label)}</span>
      <span class="badge downloading">Active {streamer.active_count}</span>
      <span class="badge checking_after_exit">Attention {streamer.attention_count}</span>
      <span class="badge">Storage {escape(format_bytes(streamer.total_bytes))}</span>
      <span class="badge">Latest {escape(latest_activity_age or '-')}</span>
      {group_action}
      <button class="download streamer-settings-toggle" type="button" data-streamer-settings-toggle="{streamer_key}" aria-expanded="false">Settings</button>
      <button class="download streamer-toggle" type="button" data-streamer-toggle="{streamer_key}" aria-expanded="{toggle_expanded}">{toggle_label}</button>
    </div>
  </div>
  <div class="streamer-details">
    {warning}
    <div class="streamer-stat-grid">
      <div><strong>{streamer.stream_count}</strong><br><span class="muted">Streams</span></div>
      <div><strong>{streamer.file_count}</strong><br><span class="muted">Files</span></div>
      <div><strong>{escape(format_bytes(streamer.total_bytes))}</strong><br><span class="muted">Storage</span></div>
      <div><strong>{escape(format_bytes(streamer.final_bytes))}</strong><br><span class="muted">Final</span></div>
      <div><strong>{escape(format_bytes(streamer.part_bytes))}</strong><br><span class="muted">Partial</span></div>
      <div><strong>{escape(format_bytes(streamer.chat_bytes))}</strong><br><span class="muted">Chat</span></div>
      <div><strong>{escape(latest_activity)}</strong><br><span class="muted">Latest {escape(latest_activity_age)}</span></div>
      <div><strong>{escape(streamer.download_dir_name or '-')}</strong><br><span class="muted">Download dir</span></div>
      <div><strong>{escape(streamer.voice_detection)}</strong><br><span class="muted">Voice</span></div>
      <div><strong>{streamer.speaker_label_count}</strong><br><span class="muted">Speaker labels</span></div>
      <div><strong>{len(streamer.voices)}</strong><br><span class="muted">Known voices</span></div>
      <div><strong>{escape(streamer.powerchat_username or '-')}</strong><br><span class="muted">Powerchat {'on' if streamer.powerchat_enabled else 'off'}</span></div>
    </div>
    <div class="streamer-settings-panel" data-streamer-settings-panel hidden>
      {settings}
    </div>
    <div class="streamer-body-grid">
      {jobs}
    </div>
    <div class="streamer-streams">
      <h3>Streams</h3>
      {streams}
    </div>
  </div>
</section>"""


def streamer_dom_id(value: str) -> str:
    key = re.sub(r"[^A-Za-z0-9_-]+", "-", value.strip()).strip("-")
    return key or "streamer"


def build_streamer_voice_details_payload(
    config: BotConfig,
    streamer_name: str,
) -> dict[str, Any]:
    if not streamer_name:
        raise ConfigError("streamer is required")
    snapshot = build_status_snapshot(config)
    streamer = next(
        (item for item in snapshot.streamer_stats if item.name == streamer_name),
        None,
    )
    if streamer is None or not streamer.configured:
        raise ConfigError(f"streamer is not configured: {streamer_name}")
    return {
        "detected": "",
        "review": render_voice_review_rows(streamer),
    }


def build_stream_voice_speakers_payload(
    config: BotConfig,
    streamer_name: str,
    video_id: str,
) -> dict[str, Any]:
    if not streamer_name:
        raise ConfigError("streamer is required")
    if not video_id:
        raise ConfigError("video_id is required")
    snapshot = build_status_snapshot(config)
    streamer = next(
        (item for item in snapshot.streamer_stats if item.name == streamer_name),
        None,
    )
    if streamer is None or not streamer.configured:
        raise ConfigError(f"streamer is not configured: {streamer_name}")
    stream = next((item for item in streamer.streams if item.video_id == video_id), None)
    if stream is None:
        raise ConfigError(f"stream is not available for streamer: {video_id}")
    return {"speakers": render_stream_voice_transcript_sample_forms(streamer, stream)}


def render_streamer_voice_settings(
    streamer: StreamerStatStatus,
    snapshot: StatusSnapshot,
) -> str:
    backend = snapshot.configuration.get("Transcription", {}).get("voice_match_backend", {})
    backend_message = ""
    if isinstance(backend, dict):
        backend_message = str(backend.get("message") or "")
    tab_key = f"voice-settings-{streamer_dom_id(streamer.name)}"
    profiles = render_voice_profile_forms(streamer)
    add_menu = render_voice_add_menu(streamer)
    review_rows = render_voice_lazy_panel(
        streamer,
        "Voice match review rows are loaded only when requested.",
    )
    return f"""<div class="voice-settings">
  <div class="voice-manager-head">
    <h2>{escape(streamer.name)} Voices</h2>
    <div class="voice-manager-actions">{add_menu}</div>
  </div>
  <div class="voice-manager-note file-meta">{escape(backend_message)}</div>
  <div class="voice-tabs">
    <input id="{escape(tab_key, quote=True)}-known" name="{escape(tab_key, quote=True)}-tab" type="radio" checked>
    <label for="{escape(tab_key, quote=True)}-known">Known Voices</label>
    <section>{profiles}</section>
    <input id="{escape(tab_key, quote=True)}-review" name="{escape(tab_key, quote=True)}-tab" type="radio">
    <label for="{escape(tab_key, quote=True)}-review">Review Matches</label>
    <section data-voice-review>{review_rows}</section>
  </div>
</div>"""

def render_voice_profile_forms(streamer: StreamerStatStatus) -> str:
    if not streamer.voices:
        return '<div class="file-meta">No known voices yet.</div>'
    rows = "".join(render_voice_profile_form(streamer.name, profile) for profile in streamer.voices)
    return f'<div class="voice-list">{rows}</div>'


def render_voice_profile_form(streamer_name: str, profile: VoiceProfileStatus) -> str:
    samples = "\n".join(profile.samples)
    enabled = "true" if profile.enabled else "false"
    status = "On" if profile.enabled else "Off"
    status_class = "running" if profile.enabled else "ended"
    return f"""<details class="voice-card">
  <summary>
    <span class="voice-card-name">{escape(profile.name)}</span>
    <span class="badge {status_class}">{status}</span>
    <span class="file-meta">{profile.sample_count} samples</span>
    <span class="voice-card-action">Edit</span>
  </summary>
  <div class="voice-card-body">
    <form class="voice-profile-form" method="post" action="/streamer-voices">
      <input type="hidden" name="streamer_name" value="{escape(streamer_name, quote=True)}">
      <input type="hidden" name="voice_name" value="{escape(profile.name, quote=True)}">
      <div class="voice-profile-title"><strong>Edit Voice</strong></div>
      <label>Matching <select name="enabled"><option value="true"{' selected' if enabled == 'true' else ''}>On</option><option value="false"{' selected' if enabled == 'false' else ''}>Off</option></select></label>
      <label>Match threshold <input name="threshold" type="number" step="0.001" min="0" value="{escape(str(profile.threshold or ''), quote=True)}" placeholder="default"></label>
      <details class="voice-advanced wide"><summary>Advanced sample paths</summary><label>Sample files <textarea name="samples" rows="3">{escape(samples)}</textarea></label></details>
      <label class="wide">Notes <textarea name="notes" rows="2">{escape(profile.notes)}</textarea></label>
      <div class="settings-actions">
        <button class="download action-button" name="action" value="save" type="submit">Save Voice</button>
        <button class="download action-button" name="action" value="delete" type="submit">Delete</button>
      </div>
    </form>
    <form class="voice-profile-form voice-sample-form" method="post" action="/streamer-voice-samples" enctype="multipart/form-data">
      <input type="hidden" name="streamer_name" value="{escape(streamer_name, quote=True)}">
      <input type="hidden" name="voice_name" value="{escape(profile.name, quote=True)}">
      <div class="voice-profile-title"><strong>Upload Sample</strong><span class="file-meta">audio or video clip</span></div>
      <label>Sample file <input name="media" type="file" accept="audio/*,video/*" required></label>
      <button class="download action-button" type="submit">Upload Sample</button>
    </form>
  </div>
</details>"""


def render_voice_add_menu(streamer: StreamerStatStatus) -> str:
    streamer_name = escape(streamer.name, quote=True)
    return f"""<details class="voice-add-menu">
  <summary class="download action-button">Add Voice</summary>
  <div class="voice-add-popover">
  <form class="voice-profile-form voice-task-form" method="post" action="/streamer-voices" enctype="multipart/form-data">
    <h3 class="voice-task-title">Add Voice</h3>
    <input type="hidden" name="streamer_name" value="{streamer_name}">
    <input type="hidden" name="action" value="save">
    <label>Voice name <input name="voice_name" required placeholder="Host"></label>
    <label>Matching <select name="enabled"><option value="true" selected>On</option><option value="false">Off</option></select></label>
    <label>Match threshold <input name="threshold" type="number" step="0.001" min="0" placeholder="default"></label>
    <label class="wide">Optional sample <input name="media" type="file" accept="audio/*,video/*"></label>
    <label class="wide">Notes <textarea name="notes" rows="2"></textarea></label>
    <button class="download action-button" type="submit">Add Voice</button>
  </form>
</div>
</details>"""


def render_voice_lazy_panel(streamer: StreamerStatStatus, message: str) -> str:
    return f"""<div class="voice-lazy-panel" data-voice-details data-streamer-name="{escape(streamer.name, quote=True)}">
  <button class="download action-button" type="button" data-load-voice-details>Load voice details</button>
  <span class="file-meta" data-voice-details-state>{escape(message)}</span>
</div>"""

def render_voice_name_options(streamer: StreamerStatStatus) -> str:
    options = "".join(
        f'<option value="{escape(profile.name, quote=True)}"></option>'
        for profile in streamer.voices
    )
    return f'<datalist id="voices-{escape(streamer_dom_id(streamer.name), quote=True)}">{options}</datalist>'


def render_voice_transcript_sample_forms(streamer: StreamerStatStatus) -> str:
    options = streamer_transcript_voice_options(streamer)
    if not options:
        return '<div class="file-meta">No diarized transcript speakers found yet. Transcribe a stream with voice detection first.</div>'
    return "".join(render_voice_transcript_sample_form(streamer, option) for option in options[:25])


def render_stream_voice_transcript_sample_forms(
    streamer: StreamerStatStatus,
    stream: StreamStatus,
) -> str:
    options = stream_transcript_voice_options(stream)
    if not options:
        return '<div class="file-meta">No diarized transcript speakers found yet. Transcribe this stream with voice detection first.</div>'
    return "".join(render_voice_transcript_sample_form(streamer, option) for option in options[:25])


def streamer_transcript_voice_options(streamer: StreamerStatStatus) -> list[dict[str, str]]:
    options: list[dict[str, str]] = []
    for stream in streamer.streams:
        options.extend(stream_transcript_voice_options(stream))
    return options


def stream_transcript_voice_options(stream: StreamStatus) -> list[dict[str, str]]:
    options: list[dict[str, str]] = []
    directory = Path(stream.directory)
    for json_file in transcript_json_files(directory):
        media_file = media_file_for_transcript_json(json_file)
        if media_file is None:
            continue
        labels = speaker_labels_in_segments(load_transcript_segments(json_file, logger=LOGGER))
        for label in labels:
            options.append({
                "video_id": stream.video_id,
                "media_name": media_file.name,
                "speaker_label": label,
                "title": stream.title,
            })
    return options


def media_file_for_transcript_json(json_file: Path) -> Path | None:
    for suffix in (".mp4", ".mkv", ".webm", ".m4a", ".mp3", ".wav", ".flac", ".ogg"):
        candidate = json_file.with_suffix(suffix)
        if candidate.is_file() and is_transcribable_media_file(candidate.name):
            return candidate
    return None


def render_voice_transcript_sample_form(streamer: StreamerStatStatus, option: dict[str, str]) -> str:
    voice_options = render_voice_name_options(streamer)
    label = option["speaker_label"]
    return f"""<form class="voice-sample-row" method="post" action="/streamer-voice-samples/from-transcript">
  <input type="hidden" name="streamer_name" value="{escape(streamer.name, quote=True)}">
  <input type="hidden" name="video_id" value="{escape(option['video_id'], quote=True)}">
  <input type="hidden" name="media_name" value="{escape(option['media_name'], quote=True)}">
  <input type="hidden" name="speaker_label" value="{escape(label, quote=True)}">
  <div class="file-name"><strong>{escape(label)}</strong><br><span class="muted">{escape(option['title'])}</span></div>
  <label>Voice <input name="voice_name" list="voices-{escape(streamer_dom_id(streamer.name), quote=True)}" required placeholder="Name"></label>
  {voice_options}
  <button class="download action-button" type="submit">Add Sample</button>
</form>"""


def render_voice_review_rows(streamer: StreamerStatStatus) -> str:
    rows: list[str] = []
    for stream in streamer.streams:
        directory = Path(stream.directory)
        for file in stream.files:
            if not is_transcribable_media_file(file.name):
                continue
            media_file = directory / file.name
            if voice_attribution_file(media_file).is_file():
                rows.extend(render_voice_review_row(stream, media_file, row) for row in voice_match_rows_for_media(media_file))
            if transcription_outputs_exist(media_file):
                rows.append(render_voice_rematch_row(streamer, stream, media_file))
    if not rows:
        return '<div class="file-meta">No voice matches have been generated yet.</div>'
    return "".join(rows[:40])


def render_voice_review_row(stream: StreamStatus, media_file: Path, row: dict[str, Any]) -> str:
    distance = row.get("distance")
    distance_text = "-" if distance is None else f"{float(distance):.3f}"
    speaker = str(row.get("speaker") or "")
    voice = str(row.get("voice") or "")
    status = str(row.get("status") or "")
    return f"""<form class="voice-match-row" method="post" action="/streamer-voice-attributions">
  <input type="hidden" name="video_id" value="{escape(stream.video_id, quote=True)}">
  <input type="hidden" name="media_name" value="{escape(media_file.name, quote=True)}">
  <input type="hidden" name="speaker_label" value="{escape(speaker, quote=True)}">
  <input type="hidden" name="voice_name" value="{escape(voice, quote=True)}">
  <span class="badge {escape(status, quote=True)}">{escape(status or '-')}</span>
  <div class="file-name"><strong>{escape(speaker)}</strong> -> {escape(voice or '-')}<br><span class="muted">{escape(media_file.name)} distance {escape(distance_text)}</span></div>
  <button class="download action-button" name="action" value="approve" type="submit">Approve</button>
  <button class="download action-button" name="action" value="reject" type="submit">Reject</button>
</form>"""


def render_voice_rematch_row(streamer: StreamerStatStatus, stream: StreamStatus, media_file: Path) -> str:
    disabled = "" if streamer.voices else " disabled"
    return f"""<form class="voice-match-row" method="post" action="/streamer-voice-attributions">
  <input type="hidden" name="video_id" value="{escape(stream.video_id, quote=True)}">
  <input type="hidden" name="media_name" value="{escape(media_file.name, quote=True)}">
  <span class="badge">Rematch</span>
  <div class="file-name">{escape(media_file.name)}<br><span class="muted">Run known-voice matching for this transcript</span></div>
  <button class="download action-button" name="action" value="rematch" type="submit"{disabled}>Match Voices</button>
</form>"""


def render_needs_grouping_action(
    streamer: StreamerStatStatus,
    snapshot: StatusSnapshot,
) -> str:
    if not streamer.needs_grouping:
        return ""
    disabled = ' disabled' if snapshot_config_path(snapshot) == "-" else ""
    sources = "\n".join(streamer.sources)
    return (
        '<button class="download action-button" type="button" '
        'data-open-streamer-wizard '
        f'data-streamer-name="{escape(streamer.name, quote=True)}" '
        f'data-streamer-sources="{escape(sources, quote=True)}"'
        f'{disabled}>Create Streamer</button>'
    )



def render_source_chips(sources: list[str]) -> str:
    if not sources:
        return '<div class="source-chips"><span class="source-chip">No configured sources</span></div>'
    chips = "".join(render_source_chip(source) for source in sources)
    return f'<div class="source-chips">{chips}</div>'


def platform_icon_url(platform: str) -> str:
    route = PLATFORM_ICON_URLS.get(platform)
    if not route:
        return ""
    return f"{route}?v={quote(APP_VERSION)}"


def render_platform_icon(platform: str, label: str, initial: str) -> str:
    url = platform_icon_url(platform)
    image = (
        f'<img src="{escape(url, quote=True)}" alt="" loading="lazy" onerror="this.remove()">'
        if url
        else ""
    )
    return (
        f'<span class="source-platform-icon {escape(platform, quote=True)}" '
        f'title="{escape(label, quote=True)}" aria-label="{escape(label, quote=True)}">'
        f'{image}<span class="source-platform-initial">{escape(initial)}</span></span>'
    )


def render_source_chip(source: str) -> str:
    platform, label, initial, _name = source_display_details(source)
    return (
        '<span class="source-chip">'
        f'{render_platform_icon(platform, label, initial)}'
        f'<span>{escape(source)}</span>'
        '</span>'
    )


def source_display_details(source: str) -> tuple[str, str, str, str]:
    try:
        spec = resolve_source(source)
        platform = spec.platform
        name = spec.display_name or channel_display_name(source)
    except SourceError:
        platform = "unknown"
        name = channel_display_name(source)
    if platform not in SOURCE_PLATFORM_LABELS:
        platform = "unknown"
    label = SOURCE_PLATFORM_LABELS[platform]
    initial = SOURCE_PLATFORM_INITIALS[platform]
    return platform, label, initial, name


def render_source_list_items(sources: list[str]) -> str:
    if not sources:
        return '<div class="source-list-empty file-meta">No sources configured.</div>'
    rows: list[str] = []
    for source in sources:
        platform, label, initial, name = source_display_details(source)
        rows.append(
            '<div class="source-list-row" data-source-row>'
            f'{render_platform_icon(platform, label, initial)}'
            '<div>'
            f'<div class="source-name">{escape(name)}</div>'
            f'<div class="source-raw file-meta">{escape(source)}</div>'
            '</div>'
            f'<span class="badge">{escape(label)}</span>'
            '<button class="download action-button" type="button" '
            f'data-remove-source="{escape(source, quote=True)}">Remove</button>'
            '</div>'
        )
    return "".join(rows)


def render_source_editor(sources: list[str]) -> str:
    values = "\n".join(sources)
    return f"""<div class="source-builder" data-source-builder>
  <textarea name="sources" data-source-values hidden>{escape(values)}</textarea>
  <div class="source-list" data-source-list>{render_source_list_items(sources)}</div>
  <div class="source-builder-actions"><button class="download action-button" type="button" data-open-source-popover>Add Source</button></div>
  <div class="source-popover" data-source-popover hidden>
    <div class="source-popover-head"><strong>Add Source</strong><button class="download action-button" type="button" data-close-source-popover>Close</button></div>
    <div class="source-popover-fields">
      <label>Website <select data-source-platform><option value="auto">Auto-detect</option><option value="youtube">YouTube</option><option value="twitch">Twitch</option><option value="kick">Kick</option><option value="rumble">Rumble</option></select></label>
      <label>Channel or URL <input data-source-input placeholder="Paste URL or channel name"></label>
      <button class="download action-button" type="button" data-add-source>Add Source</button>
    </div>
    <div class="file-meta">Paste a YouTube, Twitch, Kick, or Rumble URL to auto-detect the website, or choose one for a plain channel name.</div>
  </div>
</div>"""


def render_streamer_settings_area(
    streamer: StreamerStatStatus,
    snapshot: StatusSnapshot,
) -> str:
    if snapshot_config_path(snapshot) == "-":
        return """<div class="streamer-jobs">
  <h3>Settings</h3>
  <div class="file-meta">Config file path is not available.</div>
</div>"""
    if streamer.configured:
        tab_key = re.sub(r"[^A-Za-z0-9_-]+", "-", streamer.name).strip("-") or "streamer"
        main_tab_id = f"streamer-settings-{tab_key}-main"
        voices_tab_id = f"streamer-settings-{tab_key}-voices"
        events_tab_id = f"streamer-settings-{tab_key}-events"
        tab_name = f"streamer-settings-{tab_key}"
        return f"""<div class="streamer-settings">
  <h3>Settings</h3>
  <div class="streamer-settings-tabs">
    <input class="streamer-settings-main-toggle" type="radio" name="{escape(tab_name, quote=True)}" id="{escape(main_tab_id, quote=True)}" checked>
    <input class="streamer-settings-voices-toggle" type="radio" name="{escape(tab_name, quote=True)}" id="{escape(voices_tab_id, quote=True)}">
    <input class="streamer-settings-events-toggle" type="radio" name="{escape(tab_name, quote=True)}" id="{escape(events_tab_id, quote=True)}">
    <div class="streamer-settings-tab-labels">
      <label class="streamer-settings-main-label" for="{escape(main_tab_id, quote=True)}">Streamer</label>
      <label class="streamer-settings-voices-label" for="{escape(voices_tab_id, quote=True)}">Voices</label>
      <label class="streamer-settings-events-label" for="{escape(events_tab_id, quote=True)}">Content Events</label>
    </div>
    <div class="streamer-settings-panels">
      <section class="streamer-settings-panel streamer-settings-main">{render_streamer_group_form(streamer)}{render_manual_vod_form(streamer)}</section>
      <section class="streamer-settings-panel streamer-settings-voices">{render_streamer_voice_settings(streamer, snapshot)}</section>
      <section class="streamer-settings-panel streamer-settings-events">{render_streamer_event_settings_form(streamer)}</section>
    </div>
  </div>
</div>"""
    return """<div class="streamer-jobs">
  <h3>Settings</h3>
  <div class="file-meta">Create a streamer entry for these sources to share settings and voices.</div>
</div>"""


def render_streamer_jobs_summary(jobs: list[JobStatus]) -> str:
    if not jobs:
        body = '<div class="file-meta">No active or recent jobs for this streamer.</div>'
    else:
        pages = [jobs[index:index + STREAMER_JOB_PAGE_SIZE] for index in range(0, len(jobs), STREAMER_JOB_PAGE_SIZE)]
        panels = []
        for index, page_jobs in enumerate(pages, start=1):
            active = index == 1
            hidden = "" if active else " hidden"
            active_class = " is-active" if active else ""
            rows = "".join(render_streamer_job_row(job) for job in page_jobs)
            panels.append(
                f'<div class="streamer-job-page{active_class}" data-streamer-job-page="{index}"{hidden}>{rows}</div>'
            )
        pager = ""
        if len(pages) > 1:
            buttons = "".join(
                '<button class="download action-button streamer-job-page-button" type="button" '
                f'data-streamer-job-page-button="{index}" aria-current="{"page" if index == 1 else "false"}">{index}</button>'
                for index in range(1, len(pages) + 1)
            )
            pager = (
                '<div class="streamer-job-pager">'
                f'<span class="file-meta" data-streamer-job-page-state>Page 1 of {len(pages)}</span>'
                f'{buttons}'
                '</div>'
            )
        body = f'<div class="streamer-job-pages">{"".join(panels)}</div>{pager}'
    return f"""<div class="streamer-jobs" data-streamer-jobs>
  <h3>Jobs</h3>
  {body}
</div>"""


def render_streamer_job_row(job: JobStatus) -> str:
    return (
        '<div class="streamer-job-row">'
        f'<span class="badge {escape(job.status, quote=True)}">{escape(job.status or "-")}</span>'
        '<div class="streamer-job-body">'
        '<div class="streamer-job-heading">'
        f'<span class="streamer-job-kind">{escape(job.kind or "Job")}</span>'
        f'<span class="streamer-job-phase muted">{escape(job.phase or job.message or "-")}</span>'
        '</div>'
        f'<div class="streamer-job-progress">{render_job_progress(job.progress)}</div>'
        f'<div class="streamer-job-detail streamer-job-meta file-meta">{render_job_compact_meta(job)}</div>'
        f'{render_job_detail_toggle(job)}'
        '</div>'
        '</div>'
    )


def render_job_compact_meta(job: JobStatus) -> str:
    details = job.details or {}
    parts: list[str] = []
    elapsed = job_detail_elapsed(job)
    if elapsed:
        parts.append(f"elapsed {elapsed}")
    current_size = job_detail_current_size(details)
    current_label = str(details.get("current_label") or "").strip()
    if current_size:
        parts.append(f"{current_label or 'output'} {current_size}")
    output_name = str(details.get("output_name") or job.item or "").strip()
    if output_name:
        parts.append(f"output {output_name}")
    extra_detail = str(job.detail or "").strip()
    if extra_detail and extra_detail not in {output_name, job.item, job.video_id}:
        parts.append(extra_detail)
    if not parts:
        fallback = job.item or job.detail or job.video_id or "-"
        parts.append(fallback)
    return '<span>' + '</span><span>'.join(escape(part) for part in parts) + '</span>'


def render_job_detail_toggle(job: JobStatus) -> str:
    rows = render_job_detail_rows(job)
    if not rows:
        return ""
    return (
        '<details class="streamer-job-details file-meta">'
        '<summary>Details</summary>'
        f'<dl class="streamer-job-details-grid">{rows}</dl>'
        '</details>'
    )


def render_job_detail_rows(job: JobStatus) -> str:
    details = job.details or {}
    fields = [
        ("Media", details.get("media_name")),
        ("Chat", details.get("chat_name")),
        ("Output", details.get("output_name") or job.item),
        ("Current file", details.get("current_name")),
        ("Current size", job_detail_current_size(details)),
        ("Elapsed", job_detail_elapsed(job)),
        ("Updated", format_optional_epoch(job.updated_at)),
        ("Message", details.get("diagnostic") or job.message),
    ]
    rows: list[str] = []
    for label, value in fields:
        if value in (None, "", "-"):
            continue
        rows.append(f'<dt>{escape(label)}</dt><dd>{escape(str(value))}</dd>')
    return "".join(rows)


def job_detail_elapsed(job: JobStatus) -> str:
    details = job.details or {}
    elapsed = details.get("elapsed")
    if elapsed:
        return str(elapsed)
    elapsed_seconds = optional_float(details.get("elapsed_seconds"))
    if elapsed_seconds is not None:
        return format_duration(max(0, int(elapsed_seconds)))
    return format_job_duration(job)


def job_detail_current_size(details: dict[str, Any]) -> str:
    current_size = details.get("current_size")
    if current_size:
        return str(current_size)
    size_bytes = optional_int(details.get("current_size_bytes"))
    if size_bytes is None:
        return ""
    return format_bytes(size_bytes)


def stream_platform_details(stream: StreamStatus) -> tuple[str, str, str]:
    platform = (stream.platform or "").casefold()
    if platform not in SOURCE_PLATFORM_LABELS:
        platform, label, initial, _name = source_display_details(stream.source or stream.url)
        return platform, label, initial
    return platform, SOURCE_PLATFORM_LABELS[platform], SOURCE_PLATFORM_INITIALS[platform]


def stream_filter_date_value(stream: StreamStatus) -> str:
    for value in (stream.last_started_at, stream.updated_at):
        if value and len(value) >= 10:
            return value[:10]
    if stream.latest_file_modified_at is not None:
        return time.strftime("%Y-%m-%d", time.localtime(stream.latest_file_modified_at))
    return ""


def render_stream_source_meta(stream: StreamStatus) -> str:
    prefix = f"{escape(stream.source)} - " if stream.source else ""
    return (
        f'{prefix}{escape(stream.channel or "unknown channel")} - '
        f'<a href="{escape(stream.url, quote=True)}">{escape(stream.video_id)}</a>'
    )


def render_streamer_streams(streamer: StreamerStatStatus) -> str:
    if not streamer.streams:
        return '<section class="empty">No streams have been seen for this streamer.</section>'
    return render_streamer_stream_browser(streamer, streamer.streams)


def render_streamer_stream_browser(
    streamer: StreamerStatStatus,
    streams: list[StreamStatus],
) -> str:
    controls = render_streamer_stream_controls(streams)
    cards = "\n".join(render_stream_card(stream, streamer) for stream in streams)
    return f"""<div class="stream-browser" data-stream-browser data-streamer-key="{escape(streamer.name, quote=True)}">
      {controls}
      <div class="stream-browser-footer">
        <span class="file-meta" data-stream-browser-state>Showing streams...</span>
        <div class="stream-browser-pager">
          <button class="download action-button" type="button" data-stream-page-prev>Previous</button>
          <button class="download action-button" type="button" data-stream-page-next>Next</button>
        </div>
      </div>
      <div class="stream-browser-list" data-stream-list>{cards}</div>
    </div>"""


def render_streamer_stream_controls(streams: list[StreamStatus]) -> str:
    platform_options = render_streamer_stream_platform_options(streams)
    page_size_options = "".join(
        f'<option value="{size}"{" selected" if size == STREAMER_STREAM_PAGE_SIZE else ""}>{size}</option>'
        for size in STREAMER_STREAM_PAGE_SIZE_OPTIONS
    )
    return f"""<div class="stream-browser-controls">
      <label>Platform <select data-stream-filter-control data-stream-filter-platform>{platform_options}</select></label>
      <label>Search title <input data-stream-filter-control data-stream-filter-search placeholder="Search title or stream id"></label>
      <label>From <input data-stream-filter-control data-stream-filter-from type="date"></label>
      <label>To <input data-stream-filter-control data-stream-filter-to type="date"></label>
      <label>Per page <select data-stream-filter-control data-stream-page-size>{page_size_options}</select></label>
    </div>"""


def render_streamer_stream_platform_options(streams: list[StreamStatus]) -> str:
    platforms = {
        stream_platform_details(stream)[0]
        for stream in streams
    }
    ordered = [
        platform
        for platform in ("youtube", "twitch", "kick", "rumble", "unknown")
        if platform in platforms
    ]
    options = ['<option value="all" selected>All platforms</option>']
    options.extend(
        f'<option value="{escape(platform, quote=True)}">{escape(SOURCE_PLATFORM_LABELS.get(platform, platform))}</option>'
        for platform in ordered
    )
    return "".join(options)


def snapshot_config_path(snapshot: StatusSnapshot) -> str:
    return str(snapshot.configuration.get("Paths", {}).get("config_path", "-"))



def render_manual_vod_form(streamer: StreamerStatStatus) -> str:
    return f"""<section class="manual-vod-panel">
  <h4>Add VOD</h4>
  <form class="vod-download-form" method="post" action="/vod-download">
    <input type="hidden" name="action" value="manual">
    <input type="hidden" name="streamer_name" value="{escape(streamer.name, quote=True)}">
    <label class="settings-field wide">VOD URL
      <input name="vod_url" placeholder="Paste a YouTube, Twitch, Kick, or Rumble VOD URL" required>
    </label>
    <button class="download action-button" type="submit">Add VOD</button>
  </form>
</section>"""


def render_streamer_group_form(streamer: StreamerStatus | StreamerStatStatus | None) -> str:
    is_existing = streamer is not None
    name = streamer.name if streamer is not None else ""
    sources = "\n".join(streamer.sources) if streamer is not None else ""
    download_dir_name = streamer.download_dir_name if streamer is not None else ""
    powerchat_enabled = streamer.powerchat_enabled if streamer is not None else False
    powerchat_username = streamer.powerchat_username if streamer is not None else ""
    voice_detection = streamer.voice_detection if streamer is not None else "default"
    speaker_label_count = streamer.speaker_label_count if streamer is not None else 0
    readonly = " readonly" if is_existing else ""
    delete_button = (
        '<button class="download action-button" name="action" value="delete" type="submit">Delete</button>'
        if is_existing
        else ""
    )
    save_label = "Save Streamer" if is_existing else "Add Streamer"
    meta = (
        f'<span class="streamer-meta">Voice {escape(voice_detection)}; '
        f'labels {speaker_label_count}</span>'
        if is_existing
        else '<span class="streamer-meta">&nbsp;</span>'
    )
    return f"""<form class="streamer-form" method="post" action="/streamers">
  <label class="settings-field">Name
    <input name="streamer_name" value="{escape(name, quote=True)}"{readonly}>
  </label>
  <label class="settings-field">Download Dir Name
    <input name="download_dir_name" value="{escape(download_dir_name, quote=True)}">
  </label>
  <label class="settings-field checkbox-field">Powerchat
    <span><input name="powerchat_enabled" type="checkbox" value="true"{' checked' if powerchat_enabled else ''}> Listen for live support events</span>
  </label>
  <label class="settings-field">Powerchat Username
    <input name="powerchat_username" value="{escape(powerchat_username, quote=True)}" placeholder="Powerchat username">
  </label>
  {meta}
  <div class="settings-field wide"><span>Sources</span>{render_source_editor(streamer.sources if streamer is not None else [])}</div>
  <div class="settings-actions">
    <button class="download action-button" name="action" value="save" type="submit">{save_label}</button>
    {delete_button}
  </div>
</form>"""


def render_app_config_form(snapshot: StatusSnapshot) -> str:
    config_path = str(
        snapshot.configuration.get("Paths", {}).get("config_path", "-")
    )
    if config_path == "-":
        return """<section class="panel">
  <h2>App Settings</h2>
  <div class="file-meta">Config file path is not available.</div>
</section>"""

    fieldsets: list[str] = []
    for section in CONFIG_FORM_SECTIONS:
        fields = [field for field in CONFIG_FORM_FIELDS if field.section == section]
        rendered_fields = "\n".join(
            render_app_config_field(snapshot, field)
            for field in fields
        )
        fieldsets.append(
            f"""<fieldset class="settings-fieldset">
  <legend>{escape(section)}</legend>
  <div class="settings-grid">{rendered_fields}</div>
</fieldset>"""
        )
    return f"""<section class="panel">
  <h2>App Settings</h2>
  <form class="settings-form" method="post" action="/config">
    {''.join(fieldsets)}
    <div class="settings-actions">
      <button class="download action-button" type="submit">Save App Settings</button>
      <span class="file-meta">{escape(config_path)}</span>
    </div>
  </form>
</section>"""


def render_content_event_rules_panel(snapshot: StatusSnapshot) -> str:
    content = snapshot.configuration.get("Content Events", {})
    backend = content.get("backend", {})
    backend_message = str(backend.get("message") if isinstance(backend, dict) else "")
    rules = [rule for rule in content.get("rules", []) if isinstance(rule, dict)]
    disabled = " disabled" if snapshot_config_path(snapshot) == "-" else ""
    return f"""<section class="panel content-event-rules-panel">
  <h3>Content Event Rules</h3>
  <div class="file-meta">{escape(backend_message)}</div>
  <form class="event-rules-form" method="post" action="/stream-event-rules">
    <input type="hidden" name="scope" value="global">
    <fieldset class="event-settings-box">
      <legend>Content Events</legend>
      <div class="event-rule-toolbar"><strong>Current Events</strong><span class="file-meta">{len(rules)} configured event{'s' if len(rules) != 1 else ''}</span></div>
      {render_stream_event_rule_rows(rules)}
    </fieldset>
    <div class="settings-actions"><button class="download action-button" type="submit"{disabled}>Save Event Rules</button></div>
  </form>
</section>"""


def render_streamer_event_settings_form(streamer: StreamerStatStatus) -> str:
    detection = streamer.stream_event_detection or {}
    enabled = detection.get("enabled")
    enabled_value = "inherit" if enabled is None else ("true" if enabled else "false")
    min_confidence = detection.get("min_confidence")
    min_confidence_value = "" if min_confidence in (None, -1.0) else str(min_confidence)
    rule_count = len(streamer.stream_event_rules)
    rule_label = "event" if rule_count == 1 else "events"
    return f"""<form class="event-rules-form" method="post" action="/stream-event-rules">
  <input type="hidden" name="scope" value="streamer">
  <input type="hidden" name="streamer_name" value="{escape(streamer.name, quote=True)}">
  <fieldset class="event-settings-box">
    <legend>Detection</legend>
    <div class="settings-grid compact-grid">
      <label class="settings-field">Detection {render_form_select("event_enabled", enabled_value, ("inherit", "true", "false"))}</label>
      <label class="settings-field">Model <input name="event_model" value="{escape(str(detection.get('model') or ''), quote=True)}" placeholder="inherit"></label>
      <label class="settings-field">Device <input name="event_device" value="{escape(str(detection.get('device') or ''), quote=True)}" placeholder="inherit"></label>
      <label class="settings-field">Window seconds <input name="event_window_seconds" type="number" step="0.001" min="0" value="{escape(str(detection.get('window_seconds') or ''), quote=True)}" placeholder="inherit"></label>
      <label class="settings-field">Hop seconds <input name="event_hop_seconds" type="number" step="0.001" min="0" value="{escape(str(detection.get('hop_seconds') or ''), quote=True)}" placeholder="inherit"></label>
      <label class="settings-field">Confidence <input name="event_min_confidence" type="number" step="0.001" min="0" max="1" value="{escape(min_confidence_value, quote=True)}" placeholder="inherit"></label>
      <label class="settings-field">Max events <input name="event_max_events_per_media" type="number" min="1" value="{escape(str(detection.get('max_events_per_media') or ''), quote=True)}" placeholder="inherit"></label>
    </div>
  </fieldset>
  <fieldset class="event-settings-box">
    <legend>Content Events</legend>
    <div class="event-rule-toolbar"><strong>Current Events</strong><span class="file-meta">{rule_count} configured {rule_label}</span></div>
    {render_stream_event_rule_rows(streamer.stream_event_rules, streamer.voices)}
  </fieldset>
  <div class="settings-actions"><button class="download action-button" type="submit">Save Content Events</button></div>
</form>"""


def render_stream_event_rule_rows(
    rules: list[dict[str, Any]],
    voices: list[VoiceProfileStatus] | None = None,
) -> str:
    voice_options = voices or []
    existing = "".join(
        render_stream_event_rule_row(rule, index=index, voices=voice_options)
        for index, rule in enumerate(rules)
    )
    if not existing:
        existing = '<div class="event-rule-empty">No content events configured yet.</div>'
    add_index = len(rules)
    add = render_stream_event_rule_row(
        {},
        index=add_index,
        is_new=True,
        voices=voice_options,
    )
    return f"""<div class="event-rule-list">
  {existing}
  {add}
</div>"""


def render_stream_event_rule_row(
    rule: dict[str, Any],
    *,
    index: int,
    is_new: bool = False,
    voices: list[VoiceProfileStatus] | None = None,
) -> str:
    enabled = "true" if rule.get("enabled", True) else "false"
    labels = ", ".join(str(item) for item in rule.get("labels", []) if str(item).strip())
    keywords = ", ".join(str(item) for item in rule.get("keywords", []) if str(item).strip())
    loudness = "" if rule.get("min_loudness_dbfs") is None else str(rule.get("min_loudness_dbfs"))
    min_duration = "" if not rule.get("min_duration_seconds") else str(rule.get("min_duration_seconds"))
    max_duration = "" if not rule.get("max_duration_seconds") else str(rule.get("max_duration_seconds"))
    severity = str(rule.get("severity") or "info")
    voice_control = render_stream_event_voice_control(str(rule.get("voice") or ""), voices or [])
    title = str(rule.get("name") or "New content event")
    summary = render_stream_event_rule_summary(rule)
    disabled_class = " disabled" if not is_new and enabled == "false" else ""
    add_class = " event-rule-add" if is_new else ""
    action_label = "Add" if is_new else "Edit"
    delete_button = ""
    if not is_new:
        delete_button = (
            f'<button class="download action-button" name="rule_delete_{index}" '
            'value="true" type="submit">Delete</button>'
        )
    return f"""<details class="event-rule-card{disabled_class}{add_class}">
  <summary><span class="event-rule-title">{escape(title)}</span><span class="event-rule-summary">{summary}</span><span class="event-rule-action">{action_label}</span></summary>
  <div class="event-rule-editor">
    <div class="event-rule-primary">
      <label class="settings-field">Name <input name="rule_name" value="{escape(str(rule.get('name') or ''), quote=True)}" placeholder="Hype moment"></label>
      <label class="settings-field">Enabled {render_form_select("rule_enabled", enabled, ("true", "false"))}</label>
      <label class="settings-field">Voice {voice_control}</label>
      <label class="settings-field">Severity <input name="rule_severity" value="{escape(severity, quote=True)}" placeholder="info"></label>
    </div>
    <div class="event-rule-criteria">
      <label class="settings-field">Audio labels <input name="rule_labels" value="{escape(labels, quote=True)}" placeholder="Laughter, Cheering"></label>
      <label class="settings-field">Transcript keywords <input name="rule_keywords" value="{escape(keywords, quote=True)}" placeholder="keyword, phrase"></label>
      <label class="settings-field">Min loudness <input name="rule_min_loudness_dbfs" type="number" step="0.1" value="{escape(loudness, quote=True)}" placeholder="dBFS"></label>
      <label class="settings-field">Min duration <input name="rule_min_duration_seconds" type="number" step="0.1" min="0" value="{escape(min_duration, quote=True)}" placeholder="seconds"></label>
      <label class="settings-field">Max duration <input name="rule_max_duration_seconds" type="number" step="0.1" min="0" value="{escape(max_duration, quote=True)}" placeholder="seconds"></label>
    </div>
    <div class="settings-actions">{delete_button}</div>
  </div>
</details>"""


def render_stream_event_voice_control(
    selected: str,
    voices: list[VoiceProfileStatus],
) -> str:
    value = selected.strip()
    voice_names = [voice.name for voice in voices if voice.name.strip()]
    if not voice_names:
        return (
            f'<input name="rule_voice" value="{escape(value, quote=True)}" '
            'placeholder="Any voice">'
        )
    options = [
        '<option value=""' + ('' if value else ' selected') + '>Any voice</option>'
    ]
    known = set(voice_names)
    for voice_name in voice_names:
        selected_attr = ' selected' if voice_name == value else ''
        options.append(
            f'<option value="{escape(voice_name, quote=True)}"{selected_attr}>'
            f'{escape(voice_name)}</option>'
        )
    if value and value not in known:
        options.append(
            f'<option value="{escape(value, quote=True)}" selected>'
            f'{escape(value)}</option>'
        )
    return f'<select name="rule_voice">{"".join(options)}</select>'


def render_stream_event_rule_summary(rule: dict[str, Any]) -> str:
    labels = [str(item) for item in rule.get("labels", []) if str(item).strip()]
    keywords = [str(item) for item in rule.get("keywords", []) if str(item).strip()]
    parts: list[str] = []
    if labels:
        parts.append("labels: " + ", ".join(labels[:3]))
    if keywords:
        parts.append("keywords: " + ", ".join(keywords[:3]))
    if rule.get("voice"):
        parts.append("voice: " + str(rule.get("voice")))
    if rule.get("min_loudness_dbfs") is not None:
        parts.append(f"loudness >= {rule.get('min_loudness_dbfs')} dBFS")
    if rule.get("min_duration_seconds") or rule.get("max_duration_seconds"):
        start = rule.get("min_duration_seconds") or 0
        end = rule.get("max_duration_seconds") or "any"
        parts.append(f"duration {start}-{end}s")
    if not parts:
        parts.append("configure labels, keywords, or loudness")
    return escape("; ".join(parts))


def render_app_config_field(snapshot: StatusSnapshot, field: ConfigFormField) -> str:
    value = app_config_field_value(snapshot, field)
    wide = " wide" if field.kind in {"str_list", "int_list", "extra_args"} else ""
    control = render_app_config_control(field, value)
    return (
        f'<label class="settings-field{wide}">'
        f"{escape(field.key)}"
        f"{control}"
        "</label>"
    )


def render_app_config_control(field: ConfigFormField, value: Any) -> str:
    name = escape(field.key, quote=True)
    if field.kind == "bool":
        selected = "true" if bool(value) else "false"
        return render_form_select(name, selected, ("true", "false"))
    if field.kind == "choice":
        return render_form_select(name, str(value), field.options)
    if field.kind == "int":
        min_attr = f' min="{field.minimum}"' if field.minimum is not None else ""
        return (
            f'<input name="{name}" type="number"{min_attr} '
            f'value="{escape(str(value), quote=True)}">'
        )
    if field.kind == "float":
        min_attr = f' min="{field.minimum}"' if field.minimum is not None else ""
        return (
            f'<input name="{name}" type="number" step="0.001"{min_attr} '
            f'value="{escape(str(value), quote=True)}">'
        )
    if field.kind == "int_list":
        text = ", ".join(str(item) for item in value) if isinstance(value, list) else str(value)
        return (
            f'<textarea name="{name}" rows="{field.rows}">'
            f"{escape(text)}"
            "</textarea>"
        )
    if field.kind == "str_list":
        if isinstance(value, list):
            text = "\n".join(str(item) for item in value)
        else:
            text = str(value)
        return (
            f'<textarea name="{name}" rows="{field.rows}">'
            f"{escape(text)}"
            "</textarea>"
        )
    if field.kind == "extra_args":
        configured = "configured" if app_config_extra_args_configured(value) else "empty"
        return (
            '<select name="extra_yt_dlp_args_mode">'
            '<option value="keep" selected>keep current</option>'
            '<option value="replace">replace</option>'
            '<option value="clear">clear</option>'
            '</select>'
            f'<textarea name="{name}" rows="{field.rows}" '
            f'placeholder="{escape(configured, quote=True)}"></textarea>'
        )
    input_type = "text"
    return (
        f'<input name="{name}" type="{input_type}" '
        f'value="{escape(str(value), quote=True)}">'
    )


def render_form_select(name: str, selected: str, options: tuple[str, ...]) -> str:
    rendered: list[str] = []
    for option in options:
        selected_attr = " selected" if option == selected else ""
        rendered.append(
            f'<option value="{escape(option, quote=True)}"{selected_attr}>'
            f"{escape(option)}</option>"
        )
    return f'<select name="{name}">{"".join(rendered)}</select>'


def app_config_field_value(snapshot: StatusSnapshot, field: ConfigFormField) -> Any:
    if field.key == "channels":
        return list(snapshot.configuration.get("Channels", {}).get("channels", []))
    if field.key == "extra_yt_dlp_args":
        return snapshot.configuration.get("Download", {}).get(
            "extra_yt_dlp_args_configured",
            False,
        )
    if field.key == "chat_render_nvenc_devices":
        return snapshot.configuration.get("Live Chat", {}).get(
            "chat_render_nvenc_device_values",
            [],
        )
    if field.key == "whisperx_language":
        return snapshot.configuration.get("Transcription", {}).get(
            "whisperx_language_value",
            "",
        )
    values = snapshot.configuration.get(field.section, {})
    return values.get(field.key, "")


def app_config_extra_args_configured(value: Any) -> bool:
    return bool(value)

def render_voice_detection_panel(snapshot: StatusSnapshot) -> str:
    transcription = snapshot.configuration.get("Transcription", {})
    token_env = transcription.get("whisperx_hf_token_env", "HF_TOKEN")
    min_speakers = transcription.get("whisperx_min_speakers", "")
    max_speakers = transcription.get("whisperx_max_speakers", "")
    min_value = "" if min_speakers == "-" else str(min_speakers)
    max_value = "" if max_speakers == "-" else str(max_speakers)
    fixed_value = min_value if min_value and min_value == max_value else ""
    overrides = transcription.get("channel_overrides", {})
    if not isinstance(overrides, dict):
        overrides = {}
    rows = "\n".join(
        render_voice_detection_channel_row(channel, overrides)
        for channel in streamer_first_channel_stats(snapshot, overrides)
    )
    if not rows:
        rows = '<tr><td colspan="4" class="file-meta">No streamers, source overrides, or stream history found</td></tr>'
    return f"""<section class="panel voice-detection-panel">
  <h2>Voice Detection</h2>
  <form class="voice-form voice-default-form" method="post" action="/voice-detection">
    <input type="hidden" name="scope" value="global">
    <label>Default mode {render_voice_detection_mode_select("mode", str(transcription.get("voice_detection", "auto")), include_inherit=False)}</label>
    <label>Speakers <input class="small-input" name="speakers" type="number" min="1" value="{escape(fixed_value, quote=True)}" placeholder="fixed"></label>
    <label>Min <input class="small-input" name="min_speakers" type="number" min="1" value="{escape(min_value, quote=True)}" placeholder="range"></label>
    <label>Max <input class="small-input" name="max_speakers" type="number" min="1" value="{escape(max_value, quote=True)}" placeholder="range"></label>
    <label>Token env <input class="env-input" name="hf_token_env" value="{escape(str(token_env), quote=True)}"></label>
    <button class="download action-button" type="submit">Save Default</button>
  </form>
  <div class="table-wrap">
    <table class="voice-table">
      <thead><tr><th>Streamer / Source</th><th>Sources</th><th>Current Override</th><th>Update Override</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </div>
</section>"""


def streamer_first_channel_stats(
    snapshot: StatusSnapshot,
    overrides: dict[str, str],
) -> list[ChannelStatus]:
    statuses_by_key = {
        channel_group_key(status.name): status
        for status in snapshot.channel_stats
    }
    for name in overrides:
        key = channel_group_key(name)
        statuses_by_key.setdefault(
            key,
            ChannelStatus(
                name=name,
                configured_sources=[],
                stream_count=0,
                active_count=0,
                checking_count=0,
                ended_count=0,
                attention_count=0,
                file_count=0,
                downloadable_count=0,
                total_bytes=0,
                part_bytes=0,
                final_bytes=0,
                chat_bytes=0,
                fragment_bytes=0,
                latest_updated_at=None,
                latest_file_modified_at=None,
            ),
        )
    streamer_keys = {
        channel_group_key(streamer.name)
        for streamer in snapshot.streamer_stats
        if streamer.configured
    }
    return sorted(
        statuses_by_key.values(),
        key=lambda status: (
            channel_group_key(status.name) not in streamer_keys,
            status.name.lower(),
        ),
    )


def render_voice_detection_channel_row(
    channel: ChannelStatus,
    overrides: dict[str, str],
) -> str:
    configured_as = ", ".join(channel.configured_sources) or "-"
    key = channel.name
    summary = overrides.get(channel.name, "Use default")
    return f"""<tr>
  <td class="file-name">{escape(channel.name)}</td>
  <td class="file-name">{escape(configured_as)}</td>
  <td>{escape(summary)}</td>
  <td>
    <form class="voice-form voice-channel-form" method="post" action="/voice-detection">
      <input type="hidden" name="scope" value="channel">
      <input type="hidden" name="channel" value="{escape(key, quote=True)}">
      {render_voice_detection_mode_select("mode", "inherit", include_inherit=True)}
      <input class="small-input" name="speakers" type="number" min="1" placeholder="fixed">
      <input class="small-input" name="min_speakers" type="number" min="1" placeholder="min">
      <input class="small-input" name="max_speakers" type="number" min="1" placeholder="max">
      <input class="env-input" name="hf_token_env" placeholder="token env">
      <button class="download action-button" type="submit">Save</button>
    </form>
  </td>
</tr>"""


def render_speaker_labels_panel(snapshot: StatusSnapshot) -> str:
    rows = "\n".join(
        render_speaker_label_row(status)
        for status in snapshot.speaker_labels
    )
    if not rows:
        rows = '<tr><td colspan="4" class="file-meta">No configured channels, streamers, or diarized transcripts found</td></tr>'
    return f"""<section class="panel speaker-labels-panel">
  <h2>Speaker Names</h2>
  <div class="table-wrap">
    <table class="speaker-table">
      <thead><tr><th>Streamer / Source</th><th>Sources</th><th>Detected Labels</th><th>Speaker Names</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </div>
</section>"""


def render_speaker_label_row(status: SpeakerLabelStatus) -> str:
    sources = ", ".join(status.configured_sources) or "-"
    detected = ", ".join(status.detected_labels) or "-"
    return f"""<tr>
  <td class="file-name">{escape(status.channel)}</td>
  <td class="file-name">{escape(sources)}</td>
  <td class="file-name">{escape(detected)}</td>
  <td>{render_speaker_label_form(status)}</td>
</tr>"""


def render_speaker_label_form(status: SpeakerLabelStatus) -> str:
    labels = sorted(set(status.detected_labels) | set(status.labels))
    fields = "".join(
        render_speaker_label_pair(label, status.labels.get(label, ""), readonly=True)
        for label in labels
    )
    fields += render_speaker_label_pair("", "", readonly=False)
    return f"""<form class="speaker-label-form" method="post" action="/speaker-labels">
      <input type="hidden" name="channel" value="{escape(status.channel, quote=True)}">
      {fields}
      <div class="speaker-label-actions"><button class="download action-button" type="submit">Save Names</button></div>
    </form>"""


def render_speaker_label_pair(label: str, name: str, *, readonly: bool) -> str:
    readonly_attr = " readonly" if readonly else ""
    label_placeholder = "SPEAKER_00" if not label else ""
    return f"""<div class="speaker-label-pair">
        <input name="speaker_label" value="{escape(label, quote=True)}" placeholder="{label_placeholder}"{readonly_attr}>
        <input name="speaker_name" value="{escape(name, quote=True)}" placeholder="Name">
      </div>"""


def render_voice_detection_mode_select(
    name: str,
    selected: str,
    *,
    include_inherit: bool,
) -> str:
    options = ["inherit"] if include_inherit else []
    options.extend(["off", "auto", "range", "fixed"])
    rendered = []
    for option in options:
        selected_attr = ' selected' if option == selected else ''
        label = "Use default" if option == "inherit" else option
        rendered.append(
            f'<option value="{escape(option, quote=True)}"{selected_attr}>{escape(label)}</option>'
        )
    return f'<select name="{escape(name, quote=True)}">{"".join(rendered)}</select>'


def render_config_sections(configuration: dict[str, dict[str, Any]]) -> str:
    sections: list[str] = []
    for section, values in configuration.items():
        rows = "\n".join(
            render_config_row(name, value)
            for name, value in values.items()
        )
        sections.append(
            f"""<section class="panel">
  <h2>{escape(section)}</h2>
  <div class="table-wrap">
    <table class="config-table">
      <thead><tr><th>Setting</th><th>Value</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </div>
</section>"""
        )
    return "\n".join(sections)


def render_watermark_detection_panel(configuration: dict[str, dict[str, Any]]) -> str:
    watermark = configuration.get("Watermark", {})
    enabled = bool(watermark.get("watermark_enabled"))
    secret_configured = bool(watermark.get("watermark_secret_configured"))
    if not enabled:
        body = '<div class="file-meta">Watermarking is disabled.</div>'
    elif not secret_configured:
        body = '<div class="file-meta">Watermark secret is not configured.</div>'
    else:
        body = """<form class="upload-form" method="post" action="/detect-watermark" enctype="multipart/form-data">
            <input type="file" name="media" accept="video/*" required>
            <button class="download action-button" type="submit">Detect watermark</button>
          </form>
          <div class="file-meta">Use a video slice when possible; screenshots are not enough for confident attribution.</div>"""
    return f"""<section class="panel">
          <h2>Watermark Detection</h2>
          {body}
        </section>"""


def render_watermark_detection_result(result: Any) -> str:
    payload = detection_result_to_dict(result)
    best = payload.get("best") or {}
    rows = [
        ("Result", payload.get("message", "")),
        ("Confidence", payload.get("confidence", "")),
        ("Score", f"{float(payload.get('score') or 0.0):.5f}"),
        ("Margin", f"{float(payload.get('margin') or 0.0):.5f}"),
        ("Frames", str(payload.get("frames_analyzed") or 0)),
    ]
    if best:
        prefix = "" if payload.get("matched") else "Best "
        rows.extend(
            [
                (f"{prefix}Recipient", str(best.get("recipient_label", ""))),
                (f"{prefix}Copy ID", str(best.get("copy_id", ""))),
                (f"{prefix}Video ID", str(best.get("video_id", ""))),
                (f"{prefix}Source", str(best.get("source_name", ""))),
            ]
        )
        if best.get("variant"):
            rows.append((f"{prefix}Geometry", str(best.get("variant", ""))))
    rendered_rows = "".join(
        f"<tr><td>{escape(name)}</td><td>{escape(value)}</td></tr>"
        for name, value in rows
    )
    return f"""<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Watermark Detection</title></head>
<body>
  <h1>Watermark Detection</h1>
  <table>{rendered_rows}</table>
  <p><a href="/#streamers">Back to dashboard</a></p>
</body>
</html>
"""


def render_watermark_detection_error(message: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Watermark Detection</title></head>
<body>
  <h1>Watermark Detection</h1>
  <p>{escape(message)}</p>
  <p><a href="/#streamers">Back to dashboard</a></p>
</body>
</html>
"""


def render_config_row(name: str, value: Any) -> str:
    return (
        "<tr>"
        f"<td>{escape(name)}</td>"
        f'<td class="config-value">{escape(format_config_value(value))}</td>'
        "</tr>"
    )


def format_config_value(value: Any) -> str:
    if value is True:
        return "true"
    if value is False:
        return "false"
    if value is None:
        return "-"
    if isinstance(value, list):
        if not value:
            return "-"
        return "\n".join(str(item) for item in value)
    return str(value)


def render_stream_jobs(jobs: list[JobStatus]) -> str:
    if not jobs:
        return '<div class="file-meta">No jobs have been seen for this stream.</div>'
    rows = "".join(render_streamer_job_row(job) for job in jobs[:8])
    return f'<div class="stream-job-list">{rows}</div>'


def render_content_events(events: list[ContentEventStatus]) -> str:
    if not events:
        return '<div class="file-meta">No content events detected yet.</div>'
    ordered = sorted(events, key=lambda event: event.start)[:50]
    rows = "".join(render_content_event(event) for event in ordered)
    return f'<div class="content-events">{rows}</div>'


def render_content_event(event: ContentEventStatus) -> str:
    labels = ", ".join(
        f"{label.get('label', '')} {round(float(label.get('score') or 0.0) * 100)}%".strip()
        for label in event.labels[:3]
        if label.get("label")
    ) or "-"
    keywords = ", ".join(event.keywords) or "-"
    voice = event.voice or "-"
    loudness = "-" if event.loudness_dbfs is None else f"{event.loudness_dbfs:.1f} dBFS"
    start = format_event_offset(event.start)
    end = format_event_offset(event.end)
    return (
        f'<div class="content-event {escape(event.severity or "info", quote=True)}">'
        '<div class="content-event-time">'
        f'<span>{escape(start)}</span><span class="content-event-end">to {escape(end)}</span>'
        '</div>'
        '<div class="content-event-main">'
        f'<strong>{escape(event.rule or "Event")}</strong>'
        f'<span class="file-meta">{escape(format_duration(int(event.duration)))} &middot; '
        f'{round(event.score * 100)}% &middot; {escape(loudness)}</span>'
        f'<div>{escape(event.text or labels)}</div>'
        '</div>'
        '<div class="content-event-meta">'
        f'<span><b>Voice</b> {escape(voice)}</span><span><b>Labels</b> {escape(labels)}</span><span><b>Keywords</b> {escape(keywords)}</span>'
        '</div></div>'
    )


def render_powerchat_dashboard(stats: dict[str, Any]) -> str:
    return f"""<section class="panel powerchat-dashboard" id="powerchat-dashboard">
  <h2>Powerchat</h2>
  <div class="powerchat-summary-grid" id="powerchat-summary-cards">{render_powerchat_summary_cards(stats)}</div>
  <div class="powerchat-dashboard-controls">
    <label>Streamer <select data-powerchat-filter-control data-powerchat-filter-streamer>{render_powerchat_filter_options(powerchat_filter_values(stats, "streamer"), "All streamers")}</select></label>
    <label>Platform <select data-powerchat-filter-control data-powerchat-filter-platform>{render_powerchat_filter_options(powerchat_filter_values(stats, "platform"), "All platforms")}</select></label>
    <label>Kind <select data-powerchat-filter-control data-powerchat-filter-kind>{render_powerchat_filter_options(powerchat_filter_values(stats, "kind"), "All kinds")}</select></label>
    <label>From <input data-powerchat-filter-control data-powerchat-filter-from type="date"></label>
    <label>To <input data-powerchat-filter-control data-powerchat-filter-to type="date"></label>
    <label>Search <input data-powerchat-filter-control data-powerchat-filter-search placeholder="Donor, message, title"></label>
    <label>Per page <select data-powerchat-filter-control data-powerchat-page-size><option>25</option><option selected>50</option><option>100</option></select></label>
  </div>
  <div class="powerchat-export-actions">
    <a class="download action-button" id="powerchat-export-json" data-powerchat-export="json" href="{escape(powerchat_export_url("json"), quote=True)}">Download JSON</a>
    <a class="download action-button" id="powerchat-export-csv" data-powerchat-export="csv" href="{escape(powerchat_export_url("csv"), quote=True)}">Download CSV</a>
  </div>
  <div class="powerchat-dashboard-section">
    <h3>By Streamer</h3>
    <div class="powerchat-streamer-list" id="powerchat-streamer-rows">{render_powerchat_streamer_dashboards(stats.get("streamer_dashboards", []))}</div>
  </div>
  <details class="powerchat-overall-breakdown">
    <summary><strong>Overall Breakdown</strong><span class="muted">All streamers combined</span></summary>
    <div class="powerchat-overall-body">
      <div class="powerchat-dashboard-section">
        <h3>Donations Per Hour</h3>
        <div class="table-wrap"><table><thead><tr><th>Stream Hour</th><th>Events</th><th>Total</th><th>Average</th></tr></thead><tbody id="powerchat-hourly-rows">{render_powerchat_hourly_rows(stats.get("hourly_totals", []))}</tbody></table></div>
      </div>
      <div class="powerchat-dashboard-section">
        <h3>Streams</h3>
        <div class="table-wrap"><table><thead><tr><th>Streamer</th><th>Stream</th><th>Events</th><th>Total</th><th>Duration</th><th>Per hour</th></tr></thead><tbody id="powerchat-stream-rows">{render_powerchat_dashboard_stream_rows(stats.get("stream_totals", []))}</tbody></table></div>
      </div>
      <div class="powerchat-dashboard-section">
        <h3>Top Donors</h3>
        <div class="table-wrap"><table><thead><tr><th>Donor</th><th>Events</th><th>Total</th><th>Latest</th></tr></thead><tbody id="powerchat-donor-rows">{render_powerchat_donor_rows(stats.get("top_donors", []))}</tbody></table></div>
      </div>
    </div>
  </details>
  <div class="powerchat-dashboard-section">
    <h3>Event Ledger</h3>
    <div class="table-wrap"><table><thead><tr><th>Time</th><th>Streamer</th><th>Stream</th><th>Donor</th><th>Amount</th><th>Platform</th><th>Message</th></tr></thead><tbody id="powerchat-ledger-rows">{render_powerchat_ledger_rows(stats.get("events", [])[:50])}</tbody></table></div>
    <div class="powerchat-ledger-footer"><span class="file-meta" id="powerchat-ledger-state">Showing {min(len(stats.get("events", [])), 50)} of {len(stats.get("events", []))} events</span><div class="stream-browser-pager"><button class="download action-button" type="button" data-powerchat-page-prev>Previous</button><button class="download action-button" type="button" data-powerchat-page-next>Next</button></div></div>
  </div>
</section>"""


def render_powerchat_summary_cards(stats: dict[str, Any]) -> str:
    top_donors = stats.get("top_donors") or []
    top_donor = top_donors[0].get("donor") if top_donors else "-"
    cards = [
        ("Total", format_powerchat_summary(stats.get("money_totals", []), stats.get("unit_totals", [])) or "-"),
        ("Per hour", format_powerchat_rates(stats.get("money_rates", [])) or "-"),
        ("Events", str(stats.get("event_count") or 0)),
        ("Top donor", str(top_donor or "-")),
        ("Streams", str(stats.get("streams_with_powerchat") or 0)),
        ("No offset", str(stats.get("events_without_offset") or 0)),
    ]
    return "".join(
        f'<div class="powerchat-summary-card"><strong>{escape(value)}</strong><span class="muted">{escape(label)}</span></div>'
        for label, value in cards
    )


def render_powerchat_streamer_dashboards(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<div class="file-meta">No streamers with Powerchat events yet.</div>'
    rendered: list[str] = []
    for index, row in enumerate(rows):
        streamer = str(row.get("streamer") or "Unknown streamer")
        summary = format_powerchat_summary(row.get("money_totals", []), row.get("unit_totals", [])) or "-"
        rate = format_powerchat_rates(row.get("money_rates", [])) or "-"
        open_attr = " open" if index == 0 else ""
        rendered.append(
            f"""<details class="powerchat-streamer-card"{open_attr}>
  <summary>
    <strong>{escape(streamer)}</strong>
    <span>Total: {escape(summary)}</span>
    <span>Rate: {escape(rate)}</span>
    <span>{escape(str(row.get("stream_count") or 0))} streams</span>
    <span>{escape(str(row.get("event_count") or 0))} events</span>
  </summary>
  <div class="powerchat-streamer-card-body">
    <div class="powerchat-export-actions">
      <a class="download action-button" href="{escape(powerchat_export_url("json", streamer=streamer), quote=True)}">Download JSON</a>
      <a class="download action-button" href="{escape(powerchat_export_url("csv", streamer=streamer), quote=True)}">Download CSV</a>
    </div>
    <div class="powerchat-summary-grid">{render_powerchat_streamer_summary_cards(row)}</div>
    <div class="powerchat-dashboard-section">
      <h4>Donations Per Hour</h4>
      <div class="table-wrap"><table><thead><tr><th>Stream Hour</th><th>Events</th><th>Total</th><th>Average</th></tr></thead><tbody>{render_powerchat_hourly_rows(row.get("hourly_totals", []))}</tbody></table></div>
    </div>
    <div class="powerchat-dashboard-section">
      <h4>Streams</h4>
      <div class="table-wrap"><table><thead><tr><th>Streamer</th><th>Stream</th><th>Events</th><th>Total</th><th>Duration</th><th>Per hour</th></tr></thead><tbody>{render_powerchat_dashboard_stream_rows(row.get("stream_totals", []))}</tbody></table></div>
    </div>
    <div class="powerchat-dashboard-section">
      <h4>Top Donors</h4>
      <div class="table-wrap"><table><thead><tr><th>Donor</th><th>Events</th><th>Total</th><th>Latest</th></tr></thead><tbody>{render_powerchat_donor_rows(row.get("top_donors", []))}</tbody></table></div>
    </div>
  </div>
</details>"""
        )
    return "".join(rendered)


def render_powerchat_streamer_summary_cards(stats: dict[str, Any]) -> str:
    top_donors = stats.get("top_donors") or []
    top_donor = top_donors[0].get("donor") if top_donors else "-"
    cards = [
        ("Total", format_powerchat_summary(stats.get("money_totals", []), stats.get("unit_totals", [])) or "-"),
        ("Per hour", format_powerchat_rates(stats.get("money_rates", [])) or "-"),
        ("Events", str(stats.get("event_count") or 0)),
        ("Streams", str(stats.get("stream_count") or 0)),
        ("Top donor", str(top_donor or "-")),
        ("No offset", str(stats.get("events_without_offset") or 0)),
    ]
    return "".join(
        f'<div class="powerchat-summary-card"><strong>{escape(value)}</strong><span class="muted">{escape(label)}</span></div>'
        for label, value in cards
    )


def powerchat_filter_values(stats: dict[str, Any], key: str) -> list[str]:
    return sorted(
        {
            str(event.get(key) or "").strip()
            for event in stats.get("events", [])
            if str(event.get(key) or "").strip()
        },
        key=str.casefold,
    )


def render_powerchat_filter_options(values: list[str], all_label: str) -> str:
    options = [f'<option value="all" selected>{escape(all_label)}</option>']
    options.extend(
        f'<option value="{escape(value, quote=True)}">{escape(value)}</option>'
        for value in values
    )
    return "".join(options)


def render_powerchat_hourly_rows(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<tr><td colspan="4" class="file-meta">No hourly Powerchat events captured yet</td></tr>'
    return "".join(
        "<tr>"
        f'<td>{escape(str(row.get("hour_label") or "-"))}</td>'
        f'<td>{escape(str(row.get("event_count") or 0))}</td>'
        f'<td>{escape(format_powerchat_summary(row.get("money_totals", []), row.get("unit_totals", [])) or "-")}</td>'
        f'<td>{escape(format_powerchat_summary(row.get("average_money", []), []) or "-")}</td>'
        "</tr>"
        for row in rows
    )


def render_powerchat_dashboard_stream_rows(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<tr><td colspan="6" class="file-meta">No streams with Powerchat events yet</td></tr>'
    return "".join(
        "<tr>"
        f'<td>{escape(str(row.get("streamer") or "-"))}</td>'
        f'<td class="file-name">{escape(str(row.get("title") or row.get("video_id") or "-"))}</td>'
        f'<td>{escape(str(row.get("event_count") or 0))}</td>'
        f'<td>{escape(format_powerchat_summary(row.get("money_totals", []), row.get("unit_totals", [])) or "-")}</td>'
        f'<td>{escape(format_duration(int(float(row.get("duration_seconds") or 0))))}</td>'
        f'<td>{escape(format_powerchat_rates(row.get("money_rates", [])) or "-")}</td>'
        "</tr>"
        for row in rows[:50]
    )


def render_powerchat_donor_rows(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<tr><td colspan="4" class="file-meta">No Powerchat donors yet</td></tr>'
    return "".join(
        "<tr>"
        f'<td>{escape(str(row.get("donor") or "Unknown donor"))}</td>'
        f'<td>{escape(str(row.get("event_count") or 0))}</td>'
        f'<td>{escape(format_powerchat_summary(row.get("money_totals", []), row.get("unit_totals", [])) or "-")}</td>'
        f'<td>{escape(format_optional_iso(str(row.get("latest_received_at") or "")))}</td>'
        "</tr>"
        for row in rows[:25]
    )


def render_powerchat_ledger_rows(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<tr><td colspan="7" class="file-meta">No Powerchat events captured yet</td></tr>'
    return "".join(
        "<tr>"
        f'<td>{escape(powerchat_dashboard_event_time(row))}</td>'
        f'<td>{escape(str(row.get("streamer") or "-"))}</td>'
        f'<td class="file-name">{escape(str(row.get("stream_title") or row.get("video_id") or "-"))}</td>'
        f'<td>{escape(str(row.get("donor") or "Unknown donor"))}</td>'
        f'<td>{escape(powerchat_dashboard_event_amount_text(row) or "-")}</td>'
        f'<td>{escape(str(row.get("platform") or "Powerchat"))}</td>'
        f'<td class="log-message">{escape(str(row.get("message") or "-"))}</td>'
        "</tr>"
        for row in rows
    )


def powerchat_dashboard_event_time(row: dict[str, Any]) -> str:
    if row.get("offset_seconds") is not None:
        return format_event_offset(float(row.get("offset_seconds") or 0.0))
    return format_optional_iso(str(row.get("received_at") or ""))


def powerchat_dashboard_event_amount_text(row: dict[str, Any]) -> str:
    kind = str(row.get("kind") or "")
    if kind == "money" and row.get("money_amount") is not None and row.get("money_currency"):
        return f'{str(row.get("money_currency") or "").upper()} {format_powerchat_number(float(row.get("money_amount") or 0.0), decimals=2)}'
    if kind == "unit" and row.get("unit_amount") is not None and row.get("unit"):
        amount = f'{format_powerchat_number(float(row.get("unit_amount") or 0.0))} {row.get("unit")}'
        platform = str(row.get("platform") or "")
        return f"{platform}: {amount}" if platform else amount
    return ""


def format_powerchat_rates(rates: list[dict[str, Any]]) -> str:
    parts: list[str] = []
    for rate in rates:
        currency = str(rate.get("currency") or "").upper()
        amount = rate.get("amount_per_hour")
        if currency and amount is not None:
            parts.append(f"{currency} {format_powerchat_number(float(amount), decimals=2)}/hr")
    return ", ".join(parts)


def render_stream_powerchat_summary_cards(stats: dict[str, Any]) -> str:
    top_donors = stats.get("top_donors") or []
    top_donor = top_donors[0].get("donor") if top_donors else "-"
    cards = [
        ("Total", format_powerchat_summary(stats.get("money_totals", []), stats.get("unit_totals", [])) or "-"),
        ("Per hour", format_powerchat_rates(stats.get("money_rates", [])) or "-"),
        ("Events", str(stats.get("event_count") or 0)),
        ("Duration", format_duration(int(float(stats.get("duration_seconds") or 0)))),
        ("Top donor", str(top_donor or "-")),
        ("No offset", str(stats.get("events_without_offset") or 0)),
    ]
    return "".join(
        f'<div class="powerchat-summary-card"><strong>{escape(value)}</strong><span class="muted">{escape(label)}</span></div>'
        for label, value in cards
    )


def render_powerchat_events(stream: StreamStatus) -> str:
    if not stream.powerchat_events:
        return '<div class="file-meta">No Powerchat support events captured yet.</div>'
    stats = build_stream_powerchat_stats(stream)
    rows = "".join(render_powerchat_event(event) for event in stream.powerchat_events[:100])
    return (
        '<div class="stream-powerchat-dashboard">'
        '<div class="powerchat-export-actions">'
        f'<a class="download action-button" href="{escape(powerchat_export_url("json", video_id=stream.video_id), quote=True)}">Download JSON</a>'
        f'<a class="download action-button" href="{escape(powerchat_export_url("csv", video_id=stream.video_id), quote=True)}">Download CSV</a>'
        '</div>'
        f'<div class="powerchat-summary-grid">{render_stream_powerchat_summary_cards(stats)}</div>'
        '<div class="powerchat-dashboard-section">'
        '<h4>Donations Per Hour</h4>'
        '<div class="table-wrap"><table><thead><tr><th>Stream Hour</th><th>Events</th><th>Total</th><th>Average</th></tr></thead>'
        f'<tbody>{render_powerchat_hourly_rows(stats.get("hourly_totals", []))}</tbody></table></div>'
        '</div>'
        '<div class="powerchat-dashboard-section">'
        '<h4>Events</h4>'
        f'<div class="powerchat-events">{rows}</div>'
        '</div>'
        '</div>'
    )


def render_powerchat_event(event: PowerchatEventStatus) -> str:
    amount = powerchat_event_amount_text(event)
    timestamp = (
        format_event_offset(event.offset_seconds)
        if event.offset_seconds is not None
        else format_optional_iso(event.received_at)
    )
    kind = event.kind if event.kind in {"money", "unit", "unknown"} else "unknown"
    platform = event.platform or "Powerchat"
    donor = event.donor or "Unknown donor"
    message = event.message or "-"
    meta = " / ".join(part for part in [event.source, platform, event.kind] if part)
    return (
        f'<div class="powerchat-event {escape(kind, quote=True)}">'
        f'<div class="content-event-time"><span>{escape(timestamp)}</span></div>'
        '<div class="content-event-main">'
        f'<strong>{escape(donor)}</strong>'
        f'<div>{escape(message)}</div>'
        f'<span class="file-meta">{escape(meta)}</span>'
        '</div>'
        f'<div class="powerchat-event-amount">{escape(amount or "-")}</div>'
        '</div>'
    )


def powerchat_event_amount_text(event: PowerchatEventStatus) -> str:
    if event.kind == "money" and event.money_amount is not None and event.money_currency:
        return f"{event.money_currency} {format_powerchat_number(event.money_amount, decimals=2)}"
    if event.kind == "unit" and event.unit_amount is not None and event.unit:
        amount = f"{format_powerchat_number(event.unit_amount)} {event.unit}"
        return f"{event.platform}: {amount}" if event.platform else amount
    return ""


def format_powerchat_summary(
    money_totals: list[dict[str, Any]],
    unit_totals: list[dict[str, Any]],
) -> str:
    parts: list[str] = []
    for total in money_totals:
        currency = str(total.get("currency") or "").upper()
        amount = total.get("amount")
        if currency and amount is not None:
            parts.append(f"{currency} {format_powerchat_number(float(amount), decimals=2)}")
    for total in unit_totals:
        platform = str(total.get("platform") or "").strip()
        unit = str(total.get("unit") or "").strip()
        amount = total.get("amount")
        if unit and amount is not None:
            formatted = f"{format_powerchat_number(float(amount))} {unit}"
            parts.append(f"{platform}: {formatted}" if platform else formatted)
    return ", ".join(parts)


def format_powerchat_number(value: float, *, decimals: int = 0) -> str:
    if decimals <= 0 and float(value).is_integer():
        return str(int(value))
    return f"{value:.{decimals}f}" if decimals > 0 else f"{value:g}"


def render_cleanup_fragments_action(stream: StreamStatus) -> str:
    count = stream.file_kind_counts.get("fragment", 0)
    if count <= 0 or stream.status in FRAGMENT_CLEANUP_BLOCKED_STATUSES:
        return ""
    url = f"/cleanup-fragments?{urlencode({'video_id': stream.video_id})}"
    label = "Clean fragment" if count == 1 else f"Clean fragments ({count})"
    return (
        '<form class="inline-form" method="post" '
        f'action="{escape(url, quote=True)}">'
        '<button class="download action-button" type="submit" '
        'title="Remove saved .part-Frag files for this stream">'
        f"{escape(label)}</button>"
        "</form>"
    )


def render_delete_stream_action(stream: StreamStatus) -> str:
    if stream.status in STREAM_DELETE_BLOCKED_STATUSES:
        return ""
    return (
        '<form class="inline-form stream-delete-form" method="post" action="/delete-stream" '
        'onsubmit="return confirm(\'Delete this stream and all downloaded files? This cannot be undone.\');">'
        f'<input type="hidden" name="video_id" value="{escape(stream.video_id, quote=True)}">'
        f'<input type="hidden" name="confirm_delete" value="{STREAM_DELETE_CONFIRM_VALUE}">'
        '<button class="download action-button danger-action" type="submit" '
        'title="Delete this stream record and its downloaded files">Delete stream</button>'
        '</form>'
    )


def render_stream_vod_redownload_form(stream: StreamStatus) -> str:
    if stream.status in VOD_DOWNLOAD_BLOCKED_STATUSES:
        return ""
    default_url = stream.url if stream.url.startswith(("http://", "https://")) else ""
    return f"""<details class="vod-download-box">
  <summary class="download action-button">Redownload from VOD</summary>
  <form class="vod-download-form" method="post" action="/vod-download">
    <input type="hidden" name="action" value="redownload">
    <input type="hidden" name="video_id" value="{escape(stream.video_id, quote=True)}">
    <label class="settings-field wide">VOD URL
      <input name="vod_url" value="{escape(default_url, quote=True)}" placeholder="Paste the VOD URL" required>
    </label>
    <button class="download action-button" type="submit">Download VOD Copy</button>
  </form>
</details>"""


def render_stream_speakers_panel(
    streamer: StreamerStatStatus | None,
    stream: StreamStatus,
) -> str:
    if streamer is None or not streamer.configured:
        return '<div class="file-meta">Create a streamer entry before making voice samples from detected speakers.</div>'
    return (
        '<div class="voice-lazy-panel stream-speakers-panel" data-stream-speakers '
        f'data-streamer-name="{escape(streamer.name, quote=True)}" '
        f'data-video-id="{escape(stream.video_id, quote=True)}">'
        '<button class="download action-button" type="button" data-load-stream-speakers>'
        'Load detected speakers</button>'
        '<span class="file-meta" data-stream-speakers-state>'
        'Detected transcript speakers are loaded only for this stream.</span>'
        '</div>'
    )


def render_stream_card(stream: StreamStatus, streamer: StreamerStatStatus | None = None) -> str:
    title = stream.title or stream.video_id
    platform, platform_label, platform_initial = stream_platform_details(stream)
    mixed = "yes" if stream.has_mixed_formats else "no"
    latest_file = format_optional_epoch(stream.latest_file_modified_at)
    latest_age = format_epoch_age(stream.latest_file_modified_at)
    status_label = STATUS_LABELS.get(stream.status, stream.status.replace("_", " "))
    signals = render_stream_signals(stream)
    collapsed = stream.status == "ended"
    collapsed_class = " collapsed" if collapsed else ""
    toggle_label = "Expand" if collapsed else "Collapse"
    expanded = "false" if collapsed else "true"
    files = "\n".join(render_file_row(file) for file in stream.files[:20])
    if not files:
        files = '<tr><td colspan="7" class="file-meta">No files found</td></tr>'
    events = render_stream_event_timeline(stream.events)
    content_events = render_content_events(stream.content_events)
    powerchat = render_powerchat_events(stream)
    powerchat_summary = format_powerchat_summary(
        stream.powerchat_money_totals,
        stream.powerchat_unit_totals,
    )
    date_value = stream_filter_date_value(stream)
    speakers = render_stream_speakers_panel(streamer, stream)
    jobs = render_stream_jobs(stream.jobs)
    vod_redownload = render_stream_vod_redownload_form(stream)
    tab_key = escape(stream.video_id, quote=True)
    files_tab_id = f"stream-tab-{tab_key}-files"
    content_events_tab_id = f"stream-tab-{tab_key}-events"
    powerchat_tab_id = f"stream-tab-{tab_key}-powerchat"
    speakers_tab_id = f"stream-tab-{tab_key}-speakers"
    log_tab_id = f"stream-tab-{tab_key}-log"
    jobs_tab_id = f"stream-tab-{tab_key}-jobs"
    tab_name = f"stream-tabs-{tab_key}"

    return f"""<section class="stream{collapsed_class}" data-video-id="{escape(stream.video_id, quote=True)}" data-stream-status="{escape(stream.status, quote=True)}" data-stream-platform="{escape(platform, quote=True)}" data-stream-title="{escape(title, quote=True)}" data-stream-date="{escape(date_value, quote=True)}">
  <div class="stream-head">
    <div class="stream-title-block">
      {render_platform_icon(platform, platform_label, platform_initial)}
      <div class="stream-title-text">
        <div class="title">{escape(title)}</div>
        <div class="file-meta">{render_stream_source_meta(stream)}</div>
      </div>
    </div>
    <div class="stream-actions">
      {render_cleanup_fragments_action(stream)}
      {render_delete_stream_action(stream)}
      <button class="download stream-toggle" type="button" data-stream-toggle="{escape(stream.video_id, quote=True)}" aria-expanded="{expanded}">{toggle_label}</button>
      <span class="badge {escape(stream.status)}">{escape(status_label)}</span>
    </div>
  </div>
  <div class="stream-body">
    {signals}
    <div class="meta">
      <div>Segment: {stream.segment_index:03d}</div>
      <div>Files: {stream.file_count}</div>
      <div>Total size: {escape(format_bytes(stream.total_bytes))}</div>
      <div>Partial size: {escape(format_bytes(stream.part_bytes))}</div>
      <div>Final size: {escape(format_bytes(stream.final_bytes))}</div>
      <div>Chat size: {escape(format_bytes(stream.chat_bytes))}</div>
      <div>Fragment size: {escape(format_bytes(stream.fragment_bytes))}</div>
      <div>Content events: {stream.content_event_count}</div>
      <div>Powerchat: {stream.powerchat_event_count} <span class="muted">{escape(powerchat_summary)}</span></div>
      <div>Mixed formats: {mixed}</div>
      <div>Exit code: {escape(format_optional_int(stream.exit_code))}</div>
      <div>Started: {escape(format_optional_iso(stream.last_started_at))}</div>
      <div>Exited: {escape(format_optional_iso(stream.last_exit_at))}</div>
      <div>Updated: {escape(format_optional_iso(stream.updated_at))}</div>
      <div>Latest file: {escape(latest_file)} <span class="muted">{escape(latest_age)}</span></div>
      <div class="wide">Kinds: {escape(format_kind_counts(stream.file_kind_counts))}</div>
      <div class="wide">Directory: {escape(stream.directory)}</div>
    </div>
    {vod_redownload}
    <div class="stream-detail-tabs">
      <input class="stream-tab-radio stream-tab-files-toggle" type="radio" name="{tab_name}" id="{files_tab_id}" data-stream-tab="files" data-video-id="{tab_key}" checked>
      <input class="stream-tab-radio stream-tab-events-toggle" type="radio" name="{tab_name}" id="{content_events_tab_id}" data-stream-tab="events" data-video-id="{tab_key}">
      <input class="stream-tab-radio stream-tab-powerchat-toggle" type="radio" name="{tab_name}" id="{powerchat_tab_id}" data-stream-tab="powerchat" data-video-id="{tab_key}">
      <input class="stream-tab-radio stream-tab-speakers-toggle" type="radio" name="{tab_name}" id="{speakers_tab_id}" data-stream-tab="speakers" data-video-id="{tab_key}">
      <input class="stream-tab-radio stream-tab-jobs-toggle" type="radio" name="{tab_name}" id="{jobs_tab_id}" data-stream-tab="jobs" data-video-id="{tab_key}">
      <input class="stream-tab-radio stream-tab-log-toggle" type="radio" name="{tab_name}" id="{log_tab_id}" data-stream-tab="log" data-video-id="{tab_key}">
      <div class="stream-tab-labels">
        <label class="stream-tab-files-label" for="{files_tab_id}">Files</label>
        <label class="stream-tab-events-label" for="{content_events_tab_id}">Content Events</label>
        <label class="stream-tab-powerchat-label" for="{powerchat_tab_id}">Powerchat</label>
        <label class="stream-tab-speakers-label" for="{speakers_tab_id}">Detected Speakers</label>
        <label class="stream-tab-jobs-label" for="{jobs_tab_id}">Jobs</label>
        <label class="stream-tab-log-label" for="{log_tab_id}">Stream Log</label>
      </div>
      <div class="stream-tab-panels">
        <section class="stream-tab-panel stream-tab-files">
          <div class="table-wrap">
            <table class="files">
              <thead><tr><th>File</th><th>Segment</th><th>Format</th><th>Kind</th><th>Modified</th><th>Size</th><th>Action</th></tr></thead>
              <tbody>{files}</tbody>
            </table>
          </div>
        </section>
        <section class="stream-tab-panel stream-tab-events">{content_events}</section>
        <section class="stream-tab-panel stream-tab-powerchat">{powerchat}</section>
        <section class="stream-tab-panel stream-tab-speakers">{speakers}</section>
        <section class="stream-tab-panel stream-tab-jobs">{jobs}</section>
        <section class="stream-tab-panel stream-tab-log">{events}</section>
      </div>
    </div>
  </div>
</section>"""


def render_stream_event_timeline(events: list[StreamEventStatus]) -> str:
    if not events:
        return '<div class="file-meta">No stream log entries yet.</div>'
    rows = "".join(render_stream_event(event) for event in reversed(events))
    return f'<div class="stream-events">{rows}</div>'


def render_stream_event(event: StreamEventStatus) -> str:
    segment = f"seg {event.segment_index:03d}" if event.segment_index else "-"
    level = event.level if event.level in {"debug", "info", "warning", "error"} else "info"
    return (
        f'<div class="stream-event {escape(level, quote=True)}">'
        f'<div class="stream-event-time">{escape(format_optional_iso(event.created_at))}</div>'
        f'<div class="stream-event-level">{escape(level.upper())}</div>'
        f'<div class="stream-event-segment">{escape(segment)}</div>'
        f'<div class="stream-event-message">{escape(event.message)}</div>'
        "</div>"
    )


def render_file_row(file: FileStatus) -> str:
    action = render_file_action(file)
    return (
        "<tr>"
        f'<td class="file-name">{escape(file.name)}</td>'
        f"<td>{escape(file.segment or '-')}</td>"
        f"<td>{escape(file.format_id or '-')}</td>"
        f'<td class="kind">{escape(file.kind)}</td>'
        f"<td>{escape(format_optional_epoch(file.modified_at))}</td>"
        f"<td>{escape(format_bytes(file.size_bytes))}</td>"
        f"<td>{action}</td>"
        "</tr>"
    )


def short_action_message(message: str, limit: int = 140) -> str:
    stripped = message.strip()
    if len(stripped) <= limit:
        return stripped
    return stripped[: max(0, limit - 3)] + "..."


def render_file_action(file: FileStatus) -> str:
    actions: list[str] = []
    if file.download_url:
        actions.append(
            f'<a class="download" href="{escape(file.download_url, quote=True)}">'
            "Download</a>"
        )
    if file.watermark_copy_id:
        recipient = file.watermark_recipient_label or file.watermark_copy_id
        actions.append(
            '<span class="action-note" '
            f'title="{escape(file.watermark_copy_id, quote=True)}">'
            f"Watermark: {escape(recipient)}</span>"
        )
    if file.watermark_delete_url and file.watermark_copy_id:
        actions.append(
            '<form class="inline-form" method="post" '
            f'action="{escape(file.watermark_delete_url, quote=True)}">'
            f'<input type="hidden" name="copy_id" value="{escape(file.watermark_copy_id, quote=True)}">'
            '<button class="download action-button" type="submit">Delete</button>'
            '</form>'
        )
    if file.refresh_chat_status == "running":
        actions.append('<span class="action-note">Refreshing chat</span>')
    elif file.refresh_chat_url:
        if file.refresh_chat_status == "failed":
            label = "Retry refresh"
        elif file.refresh_chat_status == "download":
            label = "Download chat replay"
        else:
            label = "Refresh chat"
        title = ' title="Redownload chat replay or sync the recorded chat sidecar"'
        actions.append(
            '<form class="inline-form" method="post" '
            f'action="{escape(file.refresh_chat_url, quote=True)}">'
            f'<button class="download action-button" type="submit"{title}>'
            f"{escape(label)}</button>"
            "</form>"
        )
        if file.refresh_chat_status == "failed" and file.refresh_chat_message:
            actions.append(
                '<span class="action-note" '
                f'title="{escape(file.refresh_chat_message, quote=True)}">'
                "Refresh failed</span>"
            )
    if file.render_chat_output_url:
        actions.append(
            '<a class="download" '
            f'href="{escape(file.render_chat_output_url, quote=True)}">'
            "Chat video</a>"
        )
    elif file.render_chat_status == "rendering":
        actions.append('<span class="action-note">Rendering chat</span>')
    elif file.render_chat_url:
        if file.render_chat_status == "failed":
            label = "Retry chat"
        elif file.render_chat_status == "rendered":
            label = "Regenerate chat video"
        else:
            label = "Render chat"
        title = (
            ' title="Re-render and replace the existing chat video"'
            if file.render_chat_status == "rendered"
            else ""
        )
        actions.append(
            '<form class="inline-form" method="post" '
            f'action="{escape(file.render_chat_url, quote=True)}">'
            f'<button class="download action-button" type="submit"{title}>'
            f"{escape(label)}</button>"
            "</form>"
        )
        if file.render_chat_status == "failed" and file.render_chat_message:
            message = short_action_message(file.render_chat_message)
            actions.append(
                '<span class="action-note" '
                f'title="{escape(file.render_chat_message, quote=True)}">'
                f"Failed: {escape(message)}</span>"
            )
    if file.transcription_status == "running":
        actions.append('<span class="action-note">Transcribing</span>')
    elif file.transcription_url:
        if file.transcription_status == "failed":
            label = "Retry transcript"
        elif file.transcription_status == "transcribed":
            label = "Retranscribe"
        else:
            label = "Transcribe"
        title = (
            ' title="Run WhisperX again and replace existing subtitle sidecars"'
            if file.transcription_status == "transcribed"
            else ""
        )
        actions.append(
            '<form class="inline-form" method="post" '
            f'action="{escape(file.transcription_url, quote=True)}">'
            f'<button class="download action-button" type="submit"{title}>'
            f"{escape(label)}</button>"
            "</form>"
        )
        if file.transcription_status == "failed" and file.transcription_message:
            actions.append(
                '<span class="action-note" '
                f'title="{escape(file.transcription_message, quote=True)}">'
                "Transcript failed</span>"
            )
    if file.event_detection_status == "running":
        actions.append('<span class="action-note">Detecting events</span>')
    elif file.event_detection_url:
        if file.event_detection_status == "failed":
            label = "Retry events"
        elif file.event_detection_status == "detected":
            label = "Redetect events"
        else:
            label = "Detect events"
        title = (
            ' title="Run content event detection again and replace the sidecar"'
            if file.event_detection_status == "detected"
            else ""
        )
        actions.append(
            '<form class="inline-form" method="post" '
            f'action="{escape(file.event_detection_url, quote=True)}">'
            f'<button class="download action-button" type="submit"{title}>'
            f"{escape(label)}</button>"
            "</form>"
        )
        if file.event_detection_status == "failed" and file.event_detection_message:
            actions.append(
                '<span class="action-note" '
                f'title="{escape(file.event_detection_message, quote=True)}">'
                "Event detection failed</span>"
            )
    if file.watermark_url:
        actions.append(
            '<form class="inline-form watermark-form" method="post" '
            f'action="{escape(file.watermark_url, quote=True)}">'
            f'<input type="hidden" name="video_id" value="{escape(file.video_id, quote=True)}">'
            f'<input type="hidden" name="name" value="{escape(file.name, quote=True)}">'
            '<input class="recipient-input" name="recipient_label" '
            'placeholder="Recipient" required maxlength="160">'
            '<button class="download action-button" type="submit">Watermark</button>'
            "</form>"
        )
    for copy in file.watermark_copies[:5]:
        label = f"{copy.recipient_label}: {copy.status}"
        if copy.status == WATERMARK_STATUS_DONE and copy.download_url:
            actions.append(
                f'<a class="download" href="{escape(copy.download_url, quote=True)}" '
                f'title="{escape(copy.copy_id, quote=True)}">'
                f"{escape(copy.recipient_label)}</a>"
            )
        else:
            message = copy.error or copy.message or copy.status
            actions.append(
                '<span class="action-note" '
                f'title="{escape(message, quote=True)}">{escape(label)}</span>'
            )
        if copy.delete_url:
            actions.append(
                '<form class="inline-form" method="post" '
                f'action="{escape(copy.delete_url, quote=True)}">'
                f'<input type="hidden" name="copy_id" value="{escape(copy.copy_id, quote=True)}">'
                '<button class="download action-button" type="submit">Delete copy</button>'
                '</form>'
            )
    if not actions:
        return "-"
    return f'<div class="actions">{"".join(actions)}</div>'




def active_job_count(jobs: list[JobStatus]) -> int:
    return sum(1 for job in jobs if job.status in {"queued", "running"})

def render_job_rows(jobs: list[JobStatus]) -> str:
    if not jobs:
        return '<tr><td colspan="11" class="file-meta">No dashboard jobs have been seen yet</td></tr>'
    return "\n".join(render_job_row(job) for job in jobs)


def render_job_row(job: JobStatus) -> str:
    started = format_optional_epoch(job.started_at)
    updated = format_optional_epoch(job.updated_at)
    duration = format_job_duration(job)
    return (
        "<tr>"
        f"<td><span class=\"badge {escape(job.status, quote=True)}\">"
        f"{escape(job.status or '-')}</span></td>"
        f"<td>{render_job_progress(job.progress)}</td>"
        f"<td>{escape(job.kind or '-')}</td>"
        f"<td class=\"file-name\">{escape(job.phase or '-')}</td>"
        f"<td class=\"file-name\">{escape(job.video_id or '-')}</td>"
        f"<td class=\"file-name\">{escape(job.item or '-')}</td>"
        f"<td class=\"file-name\"><div class=\"streamer-job-meta file-meta\">{render_job_compact_meta(job)}</div>{render_job_detail_toggle(job)}</td>"
        f"<td>{escape(started)}</td>"
        f"<td>{escape(updated)}</td>"
        f"<td>{escape(duration)}</td>"
        f"<td class=\"log-message\">{escape(job.message or '-')}</td>"
        "</tr>"
    )


def render_job_progress(value: float | None) -> str:
    if value is None:
        return "-"
    percent = max(0, min(100, round(value * 100)))
    return (
        "<div class=\"job-progress\">"
        f"<progress max=\"100\" value=\"{percent}\"></progress>"
        f"<span>{percent}%</span>"
        "</div>"
    )


def format_job_duration(job: JobStatus) -> str:
    if job.started_at is None:
        return "-"
    ended_at = job.finished_at if job.finished_at is not None else time.time()
    return format_duration(max(0, int(ended_at - job.started_at)))


def render_log_rows(logs: list[LogEntry]) -> str:
    if not logs:
        return '<tr><td colspan="4" class="file-meta">No in-process logs captured yet</td></tr>'
    return "\n".join(render_log_row(entry) for entry in reversed(logs))


def render_log_row(entry: LogEntry) -> str:
    level_class = f"level-{entry.level}"
    return (
        "<tr>"
        f"<td>{escape(format_optional_epoch(entry.created_at))}</td>"
        f'<td class="{escape(level_class)}">{escape(entry.level)}</td>'
        f'<td class="file-name">{escape(entry.logger)}</td>'
        f'<td class="log-message">{escape(entry.message)}</td>'
        "</tr>"
    )


def render_status_counts(counts: dict[str, int]) -> str:
    if not counts:
        return '<div id="status-counts" class="status-counts"></div>'
    badges = []
    for status, count in sorted(counts.items()):
        label = STATUS_LABELS.get(status, status.replace("_", " "))
        badges.append(
            f'<span class="badge {escape(status)}">{escape(label)}: {count}</span>'
        )
    return f'<div id="status-counts" class="status-counts">{"".join(badges)}</div>'


def render_stream_signals(stream: StreamStatus) -> str:
    signals: list[str] = []
    if stream.has_mixed_formats:
        signals.append("mixed finalized and partial formats")
    if stream.status == "checking_after_exit":
        signals.append("post-exit checks running")
    if stream.status == "waiting_retry":
        signals.append("waiting for retry")
    if stream.status == "interrupted":
        signals.append("interrupted before clean exit")
    if stream.has_part_files and stream.status != "downloading":
        signals.append("partial files present")
    if not signals:
        return ""
    return f'<div class="signals">{escape(" / ".join(signals))}</div>'


def stream_needs_attention(stream: StreamStatus) -> bool:
    return stream.status in ATTENTION_STATUSES or stream.has_mixed_formats


def format_optional_int(value: int | None) -> str:
    return "-" if value is None else str(value)


def format_optional_iso(value: str | None) -> str:
    if not value:
        return "-"
    try:
        timestamp = datetime.fromisoformat(value).timestamp()
    except ValueError:
        return value
    return time.strftime("%Y-%m-%d %H:%M:%S %Z", time.localtime(timestamp))


def format_optional_epoch(value: float | None) -> str:
    if value is None:
        return "-"
    return time.strftime("%Y-%m-%d %H:%M:%S %Z", time.localtime(value))


def format_epoch_age(value: float | None) -> str:
    if value is None:
        return ""
    age = max(0, int(time.time() - value))
    return f"({format_duration(age)} ago)"


def format_duration(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}s"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {seconds:02d}s"
    hours, minutes = divmod(minutes, 60)
    if hours < 48:
        return f"{hours}h {minutes:02d}m"
    days, hours = divmod(hours, 24)
    return f"{days}d {hours:02d}h"


def format_event_offset(seconds: float) -> str:
    total = max(0, int(seconds))
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes}:{secs:02d}"


def format_kind_counts(counts: dict[str, int]) -> str:
    if not counts:
        return "-"
    return ", ".join(
        f"{kind} {count}"
        for kind, count in sorted(counts.items())
    )


def format_bytes(value: int) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    size = float(value)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024
