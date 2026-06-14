from __future__ import annotations

from dataclasses import asdict, dataclass
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
from .chat_refresh import refresh_chat_sidecar
from .chat_timing import is_chat_timing_file
from .config import (
    BotConfig,
    ConfigError,
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
    update_streamer_config,
    update_streamer_speaker_labels_config,
    update_streamer_voice_detection_config,
    update_streamer_voice_profile_config,
    sanitize_voice_sample_filename,
    validate_voice_name,
    voice_sample_dir,
)
from .downloader import (
    command_for_log,
    is_live_chat_file,
    is_yt_dlp_temporary_file,
    log_process_output,
    segment_directory,
    segment_has_mixed_format_files,
    segment_part_files,
)
from .log_buffer import LogEntry, get_recent_log_entries
from .sources import SourceError, resolve_source
from .state import StateStore, StreamEventRecord, StreamRecord, WatermarkCopyRecord
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
FAVICON_ROUTES = {
    "/favicon.ico": "favicon.ico",
    "/favicon-16x16.png": "favicon-16x16.png",
    "/favicon-32x32.png": "favicon-32x32.png",
    "/apple-touch-icon.png": "apple-touch-icon.png",
    "/android-chrome-192x192.png": "android-chrome-192x192.png",
    "/android-chrome-512x512.png": "android-chrome-512x512.png",
    "/Favicon.png": "Favicon.png",
}
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
STREAM_LIMIT = 100
FILE_LIMIT_PER_STREAM = 80
STREAM_EVENT_LIMIT = 8
LOG_LIMIT = 200
JOB_LIMIT = 200
CHAT_RENDER_PROGRESS_POLL_SECONDS = 2.0
SEGMENT_NAME_RE = re.compile(
    r"^(?P<segment>segment-\d{3})(?:\.f(?P<format_id>\d+))?"
)
LIVE_CHAT_SUFFIX = ".live_chat.json"
CHAT_RENDER_MEDIA_SUFFIXES = (".mp4", ".mkv", ".webm", ".mov")
CHAT_RENDER_OUTPUT_SUFFIX = " - chat.mp4"
ATTENTION_STATUSES = {"checking_after_exit", "interrupted", "waiting_retry"}
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
    watermark_url: str | None
    watermark_copies: list[WatermarkCopyStatus]


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


@dataclass(frozen=True, slots=True)
class StreamEventStatus:
    event_id: int
    level: str
    message: str
    segment_index: int | None
    created_at: str


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
    voice_detection: str
    speaker_label_count: int
    voices: list[VoiceProfileStatus]


@dataclass(frozen=True, slots=True)
class StreamerStatStatus:
    name: str
    sources: list[str]
    download_dir_name: str
    configured: bool
    needs_grouping: bool
    voice_detection: str
    speaker_label_count: int
    voices: list[VoiceProfileStatus]
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
            parts = urlsplit(self.path)
            path = parts.path
            if path in ("", "/", "/status"):
                self._send_html(render_status_html(build_status_snapshot(config)))
                return
            if path == "/status.json":
                self._send_json(snapshot_to_dict(build_status_snapshot(config)))
                return
            if path == "/healthz":
                self._send_text("ok\n", "text/plain; charset=utf-8")
                return
            if path in FAVICON_ROUTES:
                self._send_package_asset(FAVICON_ROUTES[path])
                return
            if path == "/download":
                self._send_download(parts.query)
                return
            if path == "/download-watermark":
                self._send_watermark_download(parts.query)
                return
            self.send_error(HTTPStatus.NOT_FOUND)

        def do_POST(self) -> None:
            parts = urlsplit(self.path)
            if parts.path == "/render-chat":
                self._start_render_chat(parts.query)
                return
            if parts.path == "/refresh-chat":
                self._start_refresh_chat(parts.query)
                return
            if parts.path == "/transcribe":
                self._start_transcription(parts.query)
                return
            if parts.path == "/voice-detection":
                self._update_voice_detection()
                return
            if parts.path == "/speaker-labels":
                self._update_speaker_labels()
                return
            if parts.path == "/streamer-voices":
                self._update_streamer_voice()
                return
            if parts.path == "/streamer-voice-samples":
                self._upload_streamer_voice_sample()
                return
            if parts.path == "/streamer-voice-samples/from-transcript":
                self._create_streamer_voice_sample_from_transcript()
                return
            if parts.path == "/streamer-voice-attributions":
                self._update_streamer_voice_attribution()
                return
            if parts.path == "/streamers":
                self._update_streamers()
                return
            if parts.path == "/config":
                self._update_config()
                return
            if parts.path == "/watermark":
                self._start_watermark()
                return
            if parts.path == "/detect-watermark":
                self._detect_watermark_upload()
                return
            self.send_error(HTTPStatus.NOT_FOUND)

        def log_message(self, fmt: str, *args: Any) -> None:
            LOGGER.debug("status web: " + fmt, *args)

        def _send_html(self, body: str) -> None:
            self._send_text(body, "text/html; charset=utf-8")

        def _send_json(self, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, indent=2, sort_keys=True) + "\n"
            self._send_text(body, "application/json; charset=utf-8")

        def _send_package_asset(self, filename: str) -> None:
            if filename not in FAVICON_ROUTES.values():
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            path = PACKAGE_DIR / filename
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


def build_status_snapshot(config: BotConfig) -> StatusSnapshot:
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

    watermarks_by_video: dict[str, list[WatermarkCopyRecord]] = {}
    for watermark_record in watermark_records:
        watermarks_by_video.setdefault(watermark_record.video_id, []).append(
            watermark_record
        )

    jobs = build_job_statuses(watermark_records)
    jobs_by_video: dict[str, list[JobStatus]] = {}
    for job in jobs:
        jobs_by_video.setdefault(job.video_id, []).append(job)

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
    counts: dict[str, int] = {}
    for stream in streams:
        counts[stream.status] = counts.get(stream.status, 0) + 1

    channel_stats = build_channel_stats(streams, config)
    streamer_stats = build_streamer_stats(config, streams, jobs)
    speaker_labels = build_speaker_label_statuses(config, streams, channel_stats)
    return StatusSnapshot(
        generated_at=time.time(),
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

    return sorted(
        jobs,
        key=lambda job: job.updated_at or job.started_at or 0.0,
        reverse=True,
    )[:JOB_LIMIT]


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


def build_streamer_statuses(config: BotConfig) -> list[StreamerStatus]:
    return [
        StreamerStatus(
            name=name,
            sources=list(streamer.sources),
            download_dir_name=streamer.download_dir_name,
            voice_detection=(
                voice_detection_config_summary(streamer.voice_detection)
                if streamer.voice_detection is not None
                else "default"
            ),
            speaker_label_count=len(streamer.speaker_labels),
            voices=voice_profile_statuses(streamer.voices),
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
                voice_detection=voice_detection,
                speaker_label_count=len(streamer.speaker_labels),
                voices=voice_profile_statuses(streamer.voices),
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
                voice_detection=voice_detection_summary_for_source_group(config, name, sources),
                speaker_label_count=speaker_label_count_for_source_group(config, name, sources),
                voices=[],
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
                voice_detection=voice_detection_summary_for_source_group(config, name, []),
                speaker_label_count=speaker_label_count_for_source_group(config, name, []),
                voices=[],
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
    voice_detection: str,
    speaker_label_count: int,
    voices: list[VoiceProfileStatus],
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
        configured=configured,
        needs_grouping=needs_grouping,
        voice_detection=voice_detection,
        speaker_label_count=speaker_label_count,
        voices=list(voices),
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
        and not is_live_chat_file(path.name)
        and not is_chat_timing_file(path.name)
    )


def stream_status_from_record(
    config: BotConfig,
    record: StreamRecord,
    watermark_records: list[WatermarkCopyRecord] | None = None,
    event_records: list[StreamEventRecord] | None = None,
    job_records: list[JobStatus] | None = None,
) -> StreamStatus:
    directory = segment_directory(config, record.video_id, record.channel)
    files = summarize_files(
        config,
        directory,
        record.video_id,
        config.watermark_enabled and bool(watermark_secret(config)),
        watermark_records or [],
        platform=record.platform,
    )
    total_bytes = sum(file.size_bytes for file in files)
    bytes_by_kind = summarize_bytes_by_kind(files)
    counts_by_kind = summarize_counts_by_kind(files)
    latest_file_modified_at = max((file.modified_at for file in files), default=None)
    return StreamStatus(
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
        file_count=len(files),
        total_bytes=total_bytes,
        part_bytes=bytes_by_kind.get("part", 0),
        final_bytes=bytes_by_kind.get("final", 0),
        chat_bytes=bytes_by_kind.get("chat", 0),
        fragment_bytes=bytes_by_kind.get("fragment", 0),
        state_bytes=bytes_by_kind.get("state", 0),
        temporary_bytes=bytes_by_kind.get("temporary", 0),
        file_kind_counts=counts_by_kind,
        latest_file_modified_at=latest_file_modified_at,
        has_part_files=bool(
            segment_part_files(
                config,
                record.video_id,
                record.segment_index,
                record.channel,
            )
        ),
        has_mixed_formats=segment_has_mixed_format_files(
            config,
            record.video_id,
            record.segment_index,
            record.channel,
        ),
        events=[stream_event_status(event) for event in event_records or []],
        jobs=list(job_records or []),
        files=files[:FILE_LIMIT_PER_STREAM],
    )


def stream_event_status(event: StreamEventRecord) -> StreamEventStatus:
    return StreamEventStatus(
        event_id=event.event_id,
        level=event.level,
        message=event.message,
        segment_index=event.segment_index,
        created_at=event.created_at,
    )


def summarize_files(
    config: BotConfig,
    directory: Path,
    video_id: str,
    watermark_enabled: bool = False,
    watermark_records: list[WatermarkCopyRecord] | None = None,
    platform: str = "youtube",
) -> list[FileStatus]:
    if not directory.exists():
        return []

    watermarks_by_source: dict[str, list[WatermarkCopyRecord]] = {}
    for record in watermark_records or []:
        watermarks_by_source.setdefault(record.source_name, []).append(record)

    files: list[FileStatus] = []
    for path in sorted(directory.iterdir(), key=lambda item: item.name):
        if not path.is_file():
            continue
        try:
            stat = path.stat()
        except OSError:
            continue
        details = file_details(path.name)
        chat_actions_enabled = platform == "youtube"
        render_chat_url, render_chat_output_url, render_chat_status, render_chat_message = (
            chat_render_action_for_file(directory, video_id, path.name)
            if chat_actions_enabled
            else (None, None, None, None)
        )
        refresh_chat_url, refresh_chat_status, refresh_chat_message = (
            chat_refresh_action_for_file(config, directory, video_id, path.name)
            if chat_actions_enabled
            else (None, None, None)
        )
        transcription_url, transcription_status, transcription_message = (
            transcription_action_for_file(directory, video_id, path.name)
        )
        watermark_copies = [
            watermark_copy_status(copy)
            for copy in watermarks_by_source.get(path.name, [])
            if copy.status in WATERMARK_JOB_STATUSES
        ]
        files.append(
            FileStatus(
                video_id=video_id,
                name=path.name,
                size_bytes=stat.st_size,
                modified_at=stat.st_mtime,
                kind=file_kind(path.name),
                segment=details[0],
                format_id=details[1],
                download_url=download_url_for(video_id, path.name)
                if is_downloadable_file(path.name)
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
                watermark_url=watermark_url_for(video_id, path.name)
                if watermark_enabled and is_watermarkable_media_file(path.name)
                else None,
                watermark_copies=watermark_copies,
            )
        )
    return files


def download_url_for(video_id: str, filename: str) -> str:
    return "/download?" + urlencode({"video_id": video_id, "name": filename})


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


def watermark_url_for(video_id: str, filename: str) -> str:
    return "/watermark"


def watermark_download_url_for(copy_id: str) -> str:
    return "/download-watermark?" + urlencode({"copy_id": copy_id})


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
    )


def first_query_value(params: dict[str, list[str]], key: str) -> str:
    values = params.get(key) or []
    return values[0] if values else ""


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
    if not is_renderable_media_file(name):
        return False
    return Path(name).suffix.lower() in {".mp4", ".mkv", ".mov", ".webm"}


def is_transcribable_media_file(name: str) -> bool:
    return is_watermarkable_media_file(name)


def chat_render_action_for_file(
    directory: Path,
    video_id: str,
    filename: str,
) -> tuple[str | None, str | None, str | None, str | None]:
    if not is_live_chat_file(filename):
        return None, None, None, None

    media_file = chat_media_file_for_chat_file(directory, filename)
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
) -> tuple[str | None, str | None, str | None]:
    if not is_live_chat_file(filename):
        return None, None, None
    if chat_media_file_for_chat_file(directory, filename) is None:
        return None, None, None
    if resolve_refresh_chat_files(config, video_id, filename) is None:
        return None, None, None

    job = refresh_chat_job_for(video_id, filename)
    if job is not None and job.status == "running":
        return None, "running", job.message
    if job is not None and job.status == "failed":
        return refresh_chat_url_for(video_id, filename), "failed", job.message
    return refresh_chat_url_for(video_id, filename), "ready", None


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
            message="Refreshing chat",
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
        phase="Refreshing chat replay",
        progress=0.2,
        message="Refreshing chat replay",
        updated_at=time.time(),
    )
    try:
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
    command = build_render_chat_file_process_command(
        sys.executable,
        config.config_path,
        media_file,
        chat_file,
        output_file,
        overwrite=regenerate,
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
        )
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
            update_isolated_render_chat_progress(key, output_file, started_at)

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
        )
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
    )
    record_stream_event(
        config,
        video_id_from_job_key(key),
        f"Chat video rendered: {output_file.name}",
    )


def update_isolated_render_chat_progress(
    key: str,
    output_file: Path,
    started_at: float,
) -> None:
    now = time.time()
    phase = isolated_render_chat_progress_phase(output_file, now - started_at)
    update_render_chat_job(
        key,
        phase=phase,
        progress=None,
        message=phase,
        updated_at=now,
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
            progress_callback=lambda phase, value: update_render_chat_job(
                key,
                phase=phase,
                progress=value,
                message=phase,
                updated_at=time.time(),
            ),
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
    update_streamer_config(
        config.config_path,
        streamer_name,
        sources,
        download_dir_name,
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
    if is_chat_timing_file(name) or name.endswith(".voice-attribution.json"):
        return "state"
    if is_live_chat_file(name):
        return "chat"
    if is_yt_dlp_temporary_file(name) or is_rendering_temporary_file(name):
        return "temporary"
    return "final"


def snapshot_to_dict(snapshot: StatusSnapshot) -> dict[str, Any]:
    return asdict(snapshot)


def render_status_html(snapshot: StatusSnapshot) -> str:
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
    voice_detection_panel = render_voice_detection_panel(snapshot)
    speaker_labels_panel = render_speaker_labels_panel(snapshot)
    app_config_form = render_app_config_form(snapshot)
    log_rows = render_log_rows(snapshot.recent_logs)
    watermark_detection = render_watermark_detection_panel(snapshot.configuration)
    about_panel = render_about_panel(snapshot)
    script = dashboard_script()

    return f"""<!doctype html>
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
    #tab-jobs:checked ~ .tabs label[for="tab-jobs"],
    #tab-logs:checked ~ .tabs label[for="tab-logs"],
    #tab-about:checked ~ .tabs label[for="tab-about"],
    #tab-config:checked ~ .tabs label[for="tab-config"] {{
      color: var(--text);
      background: var(--panel-strong);
      font-weight: 650;
    }}
    #tab-streamers:checked ~ .streamers-panel,
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
      padding: 2px 8px;
      color: var(--muted);
      background: var(--panel-strong);
      overflow-wrap: anywhere;
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
      color: white;
      background: var(--muted);
      font-size: 12px;
      font-weight: 750;
      line-height: 1;
    }}
    .source-platform-icon.youtube {{ background: #cc0000; }}
    .source-platform-icon.twitch {{ background: #6441a5; }}
    .source-platform-icon.kick {{ background: #15803d; }}
    .source-platform-icon.rumble {{ background: #2563eb; }}
    .source-platform-icon.unknown {{ background: var(--muted); }}
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
    .streamer-jobs {{
      display: grid;
      gap: 8px;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
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
    .streamer-job-row .job-progress {{ width: min(420px, 100%); min-width: 0; }}
    .stream-head {{
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: start;
    }}
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
    .stream-tab-log-toggle:checked ~ .stream-tab-labels .stream-tab-log-label,
    .stream-tab-jobs-toggle:checked ~ .stream-tab-labels .stream-tab-jobs-label {{
      color: var(--text);
      background: var(--panel-strong);
      font-weight: 650;
    }}
    .stream-tab-files-toggle:checked ~ .stream-tab-panels .stream-tab-files,
    .stream-tab-log-toggle:checked ~ .stream-tab-panels .stream-tab-log,
    .stream-tab-jobs-toggle:checked ~ .stream-tab-panels .stream-tab-jobs {{ display: block; }}
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
      .stream-event {{ grid-template-columns: 1fr; }}
      .stream-event-time, .stream-event-level, .stream-event-segment {{ justify-self: start; }}
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
      grid-template-columns: repeat(3, max-content) 1fr;
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
<body>
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
    <input class="tab-radio" type="radio" id="tab-jobs" name="dashboard-tab">
    <input class="tab-radio" type="radio" id="tab-logs" name="dashboard-tab">
    <input class="tab-radio" type="radio" id="tab-about" name="dashboard-tab">
    <input class="tab-radio" type="radio" id="tab-config" name="dashboard-tab">
    <div class="tabs">
      <label for="tab-streamers">Streamers</label>
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
      {voice_detection_panel}
      {speaker_labels_panel}
      <div class="config-stack" id="config-sections">
        {config_sections}
      </div>
    </section>
  </main>
  {script}
</body>
</html>
"""


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
  const tabs = ["tab-streamers", "tab-jobs", "tab-logs", "tab-about", "tab-config"];
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
  const normalizeSourceValue = (value, platform = "auto") => {
    value = String(value || "").trim();
    if (!value) return "";
    const detected = detectSourcePlatform(value, platform);
    if (/^https?:\\/\\//i.test(value)) return value;
    const prefix = value.match(/^([A-Za-z][A-Za-z0-9_-]*):(.+)$/);
    if (prefix && sourcePlatforms.has(prefix[1].toLowerCase().replaceAll("_", "-"))) return value;
    const clean = value.replace(/^@+/, "").replace(/^\\/+|\\/+$/g, "");
    if (detected === "youtube") return value.startsWith("@") ? value : `@${clean}`;
    if (sourcePlatforms.has(detected)) return `${detected}:${clean}`;
    return value;
  };
  const renderSourceList = (sources) => {
    sources = sources || [];
    if (!sources.length) return '<div class="source-list" data-source-list><div class="source-list-empty file-meta">No sources configured.</div></div>';
    const rows = sources.map((source) => {
      const platform = detectSourcePlatform(source);
      const label = sourcePlatformLabels[platform] || sourcePlatformLabels.unknown;
      const initial = sourcePlatformInitials[platform] || sourcePlatformInitials.unknown;
      return `<div class="source-list-row" data-source-row>
  <span class="source-platform-icon ${escapeAttr(platform)}" title="${escapeAttr(label)}" aria-label="${escapeAttr(label)}">${escapeHtml(initial)}</span>
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
  const updateSourceBuilder = (builder, sources) => {
    sources = [...new Set((sources || []).map((source) => String(source || "").trim()).filter(Boolean))];
    const values = builder.querySelector("[data-source-values]");
    if (values) values.value = sources.join("\\n");
    const list = builder.querySelector("[data-source-list]");
    if (list) list.outerHTML = renderSourceList(sources);
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
  const collapsedStreams = readStreamSet(collapsedKey);
  const expandedStreams = readStreamSet(expandedKey);
  const collapsedStreamers = readStreamSet(collapsedStreamerKey);
  const expandedStreamers = readStreamSet(expandedStreamerKey);
  const openStreamerSettings = readStreamSet(openStreamerSettingsKey);
  const selectedStreamTabs = readStreamTabState();
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
      if (input) input.value = "";
      const popover = builder.querySelector("[data-source-popover]");
      if (popover) popover.hidden = true;
      return;
    }
    const removeSource = event.target.closest("[data-remove-source]");
    if (removeSource) {
      event.preventDefault();
      const builder = removeSource.closest("[data-source-builder]");
      if (!builder) return;
      const source = removeSource.getAttribute("data-remove-source") || "";
      updateSourceBuilder(builder, sourceValuesForBuilder(builder).filter((value) => value !== source));
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
      if (!["files", "log", "jobs"].includes(selected)) continue;
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
  };

  document.addEventListener("change", (event) => {
    const input = event.target.closest("[data-stream-tab][data-video-id]");
    if (!input || !input.checked) return;
    selectedStreamTabs[input.getAttribute("data-video-id") || ""] = input.getAttribute("data-stream-tab") || "files";
    writeSelectedStreamTabs();
  });

  document.addEventListener("click", (event) => {
    const voiceButton = event.target.closest("[data-open-voice-manager]");
    if (voiceButton) {
      event.preventDefault();
      const id = voiceButton.getAttribute("data-open-voice-manager") || "";
      const dialog = id ? byId(id) : null;
      if (dialog) {
        if (typeof dialog.showModal === "function") dialog.showModal();
        else dialog.setAttribute("open", "");
      }
      return;
    }
    const closeVoiceButton = event.target.closest("[data-close-voice-manager]");
    if (closeVoiceButton) {
      event.preventDefault();
      const dialog = closeVoiceButton.closest("dialog");
      if (dialog && typeof dialog.close === "function") dialog.close();
      else if (dialog) dialog.removeAttribute("open");
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

  document.addEventListener("click", (event) => {
    const dialog = event.target.closest ? event.target.closest("dialog.voice-manager") : null;
    if (dialog && event.target === dialog && typeof dialog.close === "function") {
      dialog.close();
    }
  });

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

  const renderFileAction = (file) => {
    const actions = [];
    if (file.download_url) {
      actions.push(`<a class="download" href="${escapeAttr(file.download_url)}">Download</a>`);
    }
    if (file.refresh_chat_status === "running") {
      actions.push('<span class="action-note">Refreshing chat</span>');
    } else if (file.refresh_chat_url) {
      const label = file.refresh_chat_status === "failed" ? "Retry refresh" : "Refresh chat";
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

  const renderStreamJobRow = (job) => {
    const detail = job.item || job.detail || job.video_id || "-";
    return `<div class="streamer-job-row">
  <span class="badge ${escapeAttr(job.status)}">${escapeHtml(job.status || "-")}</span>
  <div class="streamer-job-body">
    <div class="streamer-job-heading"><span class="streamer-job-kind">${escapeHtml(job.kind || "Job")}</span><span class="streamer-job-phase muted">${escapeHtml(job.phase || job.message || "-")}</span></div>
    <div class="streamer-job-progress">${renderJobProgress(job.progress)}</div>
    <div class="streamer-job-detail file-meta">${escapeHtml(detail)}</div>
  </div>
</div>`;
  };

  const renderStreamJobs = (jobs) => {
    jobs = jobs || [];
    if (!jobs.length) return '<div class="file-meta">No jobs have been seen for this stream.</div>';
    return `<div class="stream-job-list">${jobs.slice(0, 8).map(renderStreamJobRow).join("")}</div>`;
  };

  const renderStreamCard = (stream) => {
    const title = stream.title || stream.video_id;
    const mixed = stream.has_mixed_formats ? "yes" : "no";
    const videoId = String(stream.video_id);
    const collapsed = streamIsCollapsed(videoId, stream.status);
    const collapsedClass = collapsed ? " collapsed" : "";
    const toggleLabel = collapsed ? "Expand" : "Collapse";
    const expanded = collapsed ? "false" : "true";
    const files = (stream.files || []).slice(0, 20).map(renderFileRow).join("")
      || '<tr><td colspan="7" class="file-meta">No files found</td></tr>';
    const filesTabId = `stream-tab-${videoId}-files`;
    const logTabId = `stream-tab-${videoId}-log`;
    const jobsTabId = `stream-tab-${videoId}-jobs`;
    const tabName = `stream-tabs-${videoId}`;
    return `<section class="stream${collapsedClass}" data-video-id="${escapeAttr(videoId)}" data-stream-status="${escapeAttr(stream.status)}">
  <div class="stream-head">
    <div>
      <div class="title">${escapeHtml(title)}</div>
      <div class="file-meta">${escapeHtml(stream.channel || "unknown channel")} - <a href="${escapeAttr(stream.url)}">${escapeHtml(stream.video_id)}</a></div>
    </div>
    <div class="stream-actions">
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
      <div>Mixed formats: ${mixed}</div>
      <div>Exit code: ${escapeHtml(formatOptionalInt(stream.exit_code))}</div>
      <div>Started: ${escapeHtml(formatIso(stream.last_started_at))}</div>
      <div>Exited: ${escapeHtml(formatIso(stream.last_exit_at))}</div>
      <div>Updated: ${escapeHtml(formatIso(stream.updated_at))}</div>
      <div>Latest file: ${escapeHtml(formatEpoch(stream.latest_file_modified_at))} <span class="muted">${escapeHtml(formatEpochAge(stream.latest_file_modified_at))}</span></div>
      <div class="wide">Kinds: ${escapeHtml(formatKindCounts(stream.file_kind_counts))}</div>
      <div class="wide">Directory: ${escapeHtml(stream.directory)}</div>
    </div>
    <div class="stream-detail-tabs">
      <input class="stream-tab-radio stream-tab-files-toggle" type="radio" name="${escapeAttr(tabName)}" id="${escapeAttr(filesTabId)}" data-stream-tab="files" data-video-id="${escapeAttr(videoId)}" checked>
      <input class="stream-tab-radio stream-tab-jobs-toggle" type="radio" name="${escapeAttr(tabName)}" id="${escapeAttr(jobsTabId)}" data-stream-tab="jobs" data-video-id="${escapeAttr(videoId)}">
      <input class="stream-tab-radio stream-tab-log-toggle" type="radio" name="${escapeAttr(tabName)}" id="${escapeAttr(logTabId)}" data-stream-tab="log" data-video-id="${escapeAttr(videoId)}">
      <div class="stream-tab-labels">
        <label class="stream-tab-files-label" for="${escapeAttr(filesTabId)}">Files</label>
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
    return `<div class="source-chips">${sources.map((source) => `<span class="source-chip">${escapeHtml(source)}</span>`).join("")}</div>`;
  };

  const renderStreamerForm = (streamer) => {
    const isExisting = Boolean(streamer && streamer.configured);
    const name = isExisting ? String(streamer.name || "") : "";
    const sources = isExisting ? (streamer.sources || []) : [];
    const downloadDirName = isExisting ? String(streamer.download_dir_name || "") : "";
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
  ${meta}
  <div class="settings-field wide"><span>Sources</span>${renderSourceBuilder(sources)}</div>
  <div class="settings-actions">
    <button class="download action-button" name="action" value="save" type="submit">${saveLabel}</button>
    ${deleteButton}
  </div>
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
      return `<div class="streamer-settings">
  <h3>Settings</h3>
  ${renderStreamerForm(streamer)}
</div>`;
    }
    return `<div class="streamer-jobs">
  <h3>Settings</h3>
  <div class="file-meta">Create a streamer entry for these sources to share settings and voices.</div>
</div>`;
  };

  const renderStreamerJobRow = (job) => {
    const detail = job.item || job.detail || job.video_id || "-";
    return `<div class="streamer-job-row">
  <span class="badge ${escapeAttr(job.status)}">${escapeHtml(job.status || "-")}</span>
  <div class="streamer-job-body">
    <div class="streamer-job-heading"><span class="streamer-job-kind">${escapeHtml(job.kind || "Job")}</span><span class="streamer-job-phase muted">${escapeHtml(job.phase || job.message || "-")}</span></div>
    <div class="streamer-job-progress">${renderJobProgress(job.progress)}</div>
    <div class="streamer-job-detail file-meta">${escapeHtml(detail)}</div>
  </div>
</div>`;
  };

  const renderStreamerJobsSummary = (jobs) => {
    jobs = jobs || [];
    const body = jobs.length
      ? jobs.slice(0, 8).map(renderStreamerJobRow).join("")
      : '<div class="file-meta">No active or recent jobs for this streamer.</div>';
    return `<div class="streamer-jobs">
  <h3>Jobs</h3>
  ${body}
</div>`;
  };

  const renderStreamerStreams = (streams) => {
    streams = streams || [];
    if (!streams.length) return '<section class="empty">No streams have been seen for this streamer.</section>';
    return streams.map(renderStreamCard).join("");
  };

  const renderStreamerGroupingAction = (streamer, snapshot) => {
    if (!streamer.needs_grouping) return "";
    const disabled = snapshotConfigPath(snapshot) === "-" ? " disabled" : "";
    const sources = (streamer.sources || []).join(String.fromCharCode(10));
    return `<button class="download action-button" type="button" data-open-streamer-wizard data-streamer-name="${escapeAttr(streamer.name || "")}" data-streamer-sources="${escapeAttr(sources)}"${disabled}>Create Streamer</button>`;
  };

  const streamerDomId = (value) => String(value || "streamer").trim().replace(/[^A-Za-z0-9_-]+/g, "-").replace(/^-+|-+$/g, "") || "streamer";

  const renderStreamerVoiceAction = (streamer, snapshot) => {
    if (!streamer.configured) return "";
    const disabled = snapshotConfigPath(snapshot) === "-" ? " disabled" : "";
    const dialogId = `voice-manager-${streamerDomId(streamer.name)}`;
    return `<button class="download action-button" type="button" data-open-voice-manager="${escapeAttr(dialogId)}"${disabled}>Voices</button>`;
  };

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

  const renderVoiceManager = (streamer, snapshot) => {
    if (!streamer.configured || snapshotConfigPath(snapshot) === "-") return "";
    const dialogId = `voice-manager-${streamerDomId(streamer.name)}`;
    const backend = (((snapshot || {}).configuration || {}).Transcription || {}).voice_match_backend || {};
    const voices = streamer.voices || [];
    const profiles = voices.length ? `<div class="voice-list">${voices.map((voice) => renderVoiceProfileForm(streamer, voice)).join("")}</div>` : '<div class="file-meta">No known voices yet.</div>';
    const addMenu = renderVoiceAddMenu(streamer);
    return `<dialog class="voice-manager" id="${escapeAttr(dialogId)}">
  <div class="voice-manager-head"><h2>${escapeHtml(streamer.name || "Streamer")} Voices</h2><div class="voice-manager-actions">${addMenu}<button class="download action-button" type="button" data-close-voice-manager>Close</button></div></div>
  <div class="voice-manager-note file-meta">${escapeHtml(backend.message || "")}</div>
  <div class="voice-tabs">
    <input id="${escapeAttr(dialogId)}-known" name="${escapeAttr(dialogId)}-tab" type="radio" checked><label for="${escapeAttr(dialogId)}-known">Known Voices</label><section>${profiles}</section>
    <input id="${escapeAttr(dialogId)}-detected" name="${escapeAttr(dialogId)}-tab" type="radio"><label for="${escapeAttr(dialogId)}-detected">Detected Speakers</label><section><div class="file-meta">Refresh the page for transcript sample rows.</div></section>
    <input id="${escapeAttr(dialogId)}-review" name="${escapeAttr(dialogId)}-tab" type="radio"><label for="${escapeAttr(dialogId)}-review">Review Matches</label><section><div class="file-meta">Refresh the page for transcript sample and match review rows.</div></section>
  </div>
</dialog>`;
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
    const voiceAction = renderStreamerVoiceAction(streamer, snapshot);
    const voiceManager = renderVoiceManager(streamer, snapshot);
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
      ${voiceAction}
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
      ${renderStreamerStreams(streamer.streams || [])}
    </div>
  </div>
  ${voiceManager}
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
        `<td class="file-name">${escapeHtml(job.detail || "-")}</td>`,
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

  const applySnapshot = (snapshot) => {
    const streams = snapshot.streams || [];
    const counts = snapshot.counts || {};
    setText("metric-total", streams.length);
    setText("metric-downloading", counts.downloading || 0);
    setText("metric-checking", counts.checking_after_exit || 0);
    setText("metric-attention", streams.filter(streamNeedsAttention).length);
    setText("metric-streamers", (snapshot.streamer_stats || []).length);
    setText("metric-jobs", activeJobCount(snapshot.jobs || []));
    setText("metric-logs", (snapshot.recent_logs || []).length);
    setText("metric-storage", formatBytes(snapshot.total_bytes));
    setText("metric-partial", formatBytes(snapshot.part_bytes));
    setText("updated-at", `Updated ${formatEpoch(snapshot.generated_at)}`);
    setText("refresh-state", "Refresh 15s");
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
      const activeElement = document.activeElement;
      if (!activeElement || !streamerList.contains(activeElement)) {
        streamerList.innerHTML = renderStreamerList(snapshot);
        applyCollapsedState(streamerList);
      }
    }
    const jobRows = byId("job-rows");
    if (jobRows) jobRows.innerHTML = renderJobRows(snapshot.jobs || []);
    const logRows = byId("log-rows");
    if (logRows) logRows.innerHTML = renderLogRows(snapshot.recent_logs || []);
    const configSections = byId("config-sections");
    if (configSections) configSections.innerHTML = renderConfigSections(snapshot.configuration || {});
    applyAbout(snapshot.app || {});
  };

  const refreshStatus = async () => {
    try {
      const response = await fetch("/status.json", { cache: "no-store" });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      applySnapshot(await response.json());
    } catch (error) {
      setText("refresh-state", `Refresh failed: ${error.message || error}`);
    }
  };

  window.setInterval(refreshStatus, 15000);
})();
</script>"""


def render_about_panel(snapshot: StatusSnapshot) -> str:
    generated = time.strftime(
        "%Y-%m-%d %H:%M:%S %Z",
        time.localtime(snapshot.generated_at),
    )
    return f"""<section class="panel">
  <h2>About</h2>
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
    voice_action = render_streamer_voice_action(streamer, snapshot)
    voice_manager = render_streamer_voice_manager(streamer, snapshot)
    settings = render_streamer_settings_area(streamer, snapshot)
    jobs = render_streamer_jobs_summary(streamer.jobs)
    streams = render_streamer_streams(streamer.streams)
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
      {voice_action}
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
  {voice_manager}
</section>"""


def streamer_dom_id(value: str) -> str:
    key = re.sub(r"[^A-Za-z0-9_-]+", "-", value.strip()).strip("-")
    return key or "streamer"


def render_streamer_voice_action(
    streamer: StreamerStatStatus,
    snapshot: StatusSnapshot,
) -> str:
    if not streamer.configured:
        return ""
    disabled = ' disabled' if snapshot_config_path(snapshot) == "-" else ""
    dialog_id = f"voice-manager-{streamer_dom_id(streamer.name)}"
    return (
        f'<button class="download action-button" type="button" '
        f'data-open-voice-manager="{escape(dialog_id, quote=True)}"{disabled}>Voices</button>'
    )


def render_streamer_voice_manager(
    streamer: StreamerStatStatus,
    snapshot: StatusSnapshot,
) -> str:
    if not streamer.configured or snapshot_config_path(snapshot) == "-":
        return ""
    dialog_id = f"voice-manager-{streamer_dom_id(streamer.name)}"
    backend = snapshot.configuration.get("Transcription", {}).get("voice_match_backend", {})
    backend_message = ""
    if isinstance(backend, dict):
        backend_message = str(backend.get("message") or "")
    profiles = render_voice_profile_forms(streamer)
    add_menu = render_voice_add_menu(streamer)
    transcript_samples = render_voice_transcript_sample_forms(streamer)
    review_rows = render_voice_review_rows(streamer)
    return f"""<dialog class="voice-manager" id="{escape(dialog_id, quote=True)}">
  <div class="voice-manager-head">
    <h2>{escape(streamer.name)} Voices</h2>
    <div class="voice-manager-actions">
      {add_menu}
      <button class="download action-button" type="button" data-close-voice-manager>Close</button>
    </div>
  </div>
  <div class="voice-manager-note file-meta">{escape(backend_message)}</div>
  <div class="voice-tabs">
    <input id="{escape(dialog_id, quote=True)}-known" name="{escape(dialog_id, quote=True)}-tab" type="radio" checked>
    <label for="{escape(dialog_id, quote=True)}-known">Known Voices</label>
    <section>{profiles}</section>
    <input id="{escape(dialog_id, quote=True)}-detected" name="{escape(dialog_id, quote=True)}-tab" type="radio">
    <label for="{escape(dialog_id, quote=True)}-detected">Detected Speakers</label>
    <section>{transcript_samples}</section>
    <input id="{escape(dialog_id, quote=True)}-review" name="{escape(dialog_id, quote=True)}-tab" type="radio">
    <label for="{escape(dialog_id, quote=True)}-review">Review Matches</label>
    <section>{review_rows}</section>
  </div>
</dialog>"""


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


def streamer_transcript_voice_options(streamer: StreamerStatStatus) -> list[dict[str, str]]:
    options: list[dict[str, str]] = []
    for stream in streamer.streams:
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
    chips = "".join(
        f'<span class="source-chip">{escape(source)}</span>'
        for source in sources
    )
    return f'<div class="source-chips">{chips}</div>'


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
            f'<span class="source-platform-icon {escape(platform, quote=True)}" '
            f'title="{escape(label, quote=True)}" aria-label="{escape(label, quote=True)}">'
            f'{escape(initial)}</span>'
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
        return f"""<div class="streamer-settings">
  <h3>Settings</h3>
  {render_streamer_group_form(streamer)}
</div>"""
    return """<div class="streamer-jobs">
  <h3>Settings</h3>
  <div class="file-meta">Create a streamer entry for these sources to share settings and voices.</div>
</div>"""


def render_streamer_jobs_summary(jobs: list[JobStatus]) -> str:
    if not jobs:
        body = '<div class="file-meta">No active or recent jobs for this streamer.</div>'
    else:
        body = "".join(render_streamer_job_row(job) for job in jobs[:8])
    return f"""<div class="streamer-jobs">
  <h3>Jobs</h3>
  {body}
</div>"""


def render_streamer_job_row(job: JobStatus) -> str:
    detail = job.item or job.detail or job.video_id or "-"
    return (
        '<div class="streamer-job-row">'
        f'<span class="badge {escape(job.status, quote=True)}">{escape(job.status or "-")}</span>'
        '<div class="streamer-job-body">'
        '<div class="streamer-job-heading">'
        f'<span class="streamer-job-kind">{escape(job.kind or "Job")}</span>'
        f'<span class="streamer-job-phase muted">{escape(job.phase or job.message or "-")}</span>'
        '</div>'
        f'<div class="streamer-job-progress">{render_job_progress(job.progress)}</div>'
        f'<div class="streamer-job-detail file-meta">{escape(detail)}</div>'
        '</div>'
        '</div>'
    )


def render_streamer_streams(streams: list[StreamStatus]) -> str:
    if not streams:
        return '<section class="empty">No streams have been seen for this streamer.</section>'
    return "\n".join(render_stream_card(stream) for stream in streams)


def snapshot_config_path(snapshot: StatusSnapshot) -> str:
    return str(snapshot.configuration.get("Paths", {}).get("config_path", "-"))



def render_streamer_group_form(streamer: StreamerStatus | StreamerStatStatus | None) -> str:
    is_existing = streamer is not None
    name = streamer.name if streamer is not None else ""
    sources = "\n".join(streamer.sources) if streamer is not None else ""
    download_dir_name = streamer.download_dir_name if streamer is not None else ""
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
    if payload.get("matched"):
        rows.extend(
            [
                ("Recipient", str(best.get("recipient_label", ""))),
                ("Copy ID", str(best.get("copy_id", ""))),
                ("Video ID", str(best.get("video_id", ""))),
                ("Source", str(best.get("source_name", ""))),
            ]
        )
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


def render_stream_card(stream: StreamStatus) -> str:
    title = stream.title or stream.video_id
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
    jobs = render_stream_jobs(stream.jobs)
    tab_key = escape(stream.video_id, quote=True)
    files_tab_id = f"stream-tab-{tab_key}-files"
    log_tab_id = f"stream-tab-{tab_key}-log"
    jobs_tab_id = f"stream-tab-{tab_key}-jobs"
    tab_name = f"stream-tabs-{tab_key}"

    return f"""<section class="stream{collapsed_class}" data-video-id="{escape(stream.video_id, quote=True)}" data-stream-status="{escape(stream.status, quote=True)}">
  <div class="stream-head">
    <div>
      <div class="title">{escape(title)}</div>
      <div class="file-meta">{escape(stream.channel or "unknown channel")} - <a href="{escape(stream.url, quote=True)}">{escape(stream.video_id)}</a></div>
    </div>
    <div class="stream-actions">
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
      <div>Mixed formats: {mixed}</div>
      <div>Exit code: {escape(format_optional_int(stream.exit_code))}</div>
      <div>Started: {escape(format_optional_iso(stream.last_started_at))}</div>
      <div>Exited: {escape(format_optional_iso(stream.last_exit_at))}</div>
      <div>Updated: {escape(format_optional_iso(stream.updated_at))}</div>
      <div>Latest file: {escape(latest_file)} <span class="muted">{escape(latest_age)}</span></div>
      <div class="wide">Kinds: {escape(format_kind_counts(stream.file_kind_counts))}</div>
      <div class="wide">Directory: {escape(stream.directory)}</div>
    </div>
    <div class="stream-detail-tabs">
      <input class="stream-tab-radio stream-tab-files-toggle" type="radio" name="{tab_name}" id="{files_tab_id}" data-stream-tab="files" data-video-id="{tab_key}" checked>
      <input class="stream-tab-radio stream-tab-jobs-toggle" type="radio" name="{tab_name}" id="{jobs_tab_id}" data-stream-tab="jobs" data-video-id="{tab_key}">
      <input class="stream-tab-radio stream-tab-log-toggle" type="radio" name="{tab_name}" id="{log_tab_id}" data-stream-tab="log" data-video-id="{tab_key}">
      <div class="stream-tab-labels">
        <label class="stream-tab-files-label" for="{files_tab_id}">Files</label>
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
    if file.refresh_chat_status == "running":
        actions.append('<span class="action-note">Refreshing chat</span>')
    elif file.refresh_chat_url:
        label = "Retry refresh" if file.refresh_chat_status == "failed" else "Refresh chat"
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
        f"<td class=\"file-name\">{escape(job.detail or '-')}</td>"
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
