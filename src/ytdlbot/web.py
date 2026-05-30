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
import re
import shutil
import subprocess
import sys
import tempfile
import time

from .chat_render import (
    build_render_chat_file_process_command,
    chat_video_output_file,
    choose_chat_render_nvenc_device,
    detect_nvidia_devices,
    render_chat_video_file,
)
from .config import BotConfig
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
from .state import StateStore, StreamRecord, WatermarkCopyRecord
from .transcription import (
    transcribe_media_file,
    transcription_outputs_exist,
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
STREAM_LIMIT = 100
FILE_LIMIT_PER_STREAM = 80
LOG_LIMIT = 200
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


@dataclass(frozen=True, slots=True)
class TranscriptionJob:
    video_id: str
    media_name: str
    status: str
    message: str
    started_at: float
    finished_at: float | None = None


@dataclass(frozen=True, slots=True)
class StreamStatus:
    video_id: str
    title: str
    channel: str
    url: str
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
class StatusSnapshot:
    generated_at: float
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
    configuration: dict[str, dict[str, Any]]
    channel_stats: list[ChannelStatus]
    recent_logs: list[LogEntry]
    log_limit: int
    streams: list[StreamStatus]


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
        thread = Thread(target=server.serve_forever, name="ytdlbot-web", daemon=True)
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
        server_version = "YTDLBotStatus/1.0"

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
            if parts.path == "/transcribe":
                self._start_transcription(parts.query)
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
            self.send_header("Location", "/#streams")
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
            self.send_header("Location", "/#streams")
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
            self.send_header("Location", "/#streams")
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
                    prefix="ytdlbot-watermark-",
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
    finally:
        state.close()

    watermarks_by_video: dict[str, list[WatermarkCopyRecord]] = {}
    for watermark_record in watermark_records:
        watermarks_by_video.setdefault(watermark_record.video_id, []).append(
            watermark_record
        )

    streams = [
        stream_status_from_record(
            config,
            record,
            watermarks_by_video.get(record.video_id, []),
        )
        for record in records
    ]
    counts: dict[str, int] = {}
    for stream in streams:
        counts[stream.status] = counts.get(stream.status, 0) + 1

    channel_stats = build_channel_stats(streams, config.channels)
    return StatusSnapshot(
        generated_at=time.time(),
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
        configuration=build_config_summary(config),
        channel_stats=channel_stats,
        recent_logs=get_recent_log_entries(LOG_LIMIT),
        log_limit=LOG_LIMIT,
        streams=streams,
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
        },
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
        },
        "Live Chat": {
            "record_live_chat": config.record_live_chat,
            "render_live_chat_video": config.render_live_chat_video,
            "chat_render_panel_workers": config.chat_render_panel_workers,
            "chat_render_use_nvenc": config.chat_render_use_nvenc,
            "chat_render_nvenc_devices": nvenc_devices_for_config_summary(
                config.chat_render_nvenc_devices
            ),
        },
        "Transcription": {
            "transcribe_subtitles": config.transcribe_subtitles,
            "transcription_max_concurrent": config.transcription_max_concurrent,
            "whisperx_path": config.whisperx_path,
            "whisperx_model": config.whisperx_model,
            "whisperx_device": config.whisperx_device,
            "whisperx_compute_type": config.whisperx_compute_type,
            "whisperx_batch_size": config.whisperx_batch_size,
            "whisperx_language": config.whisperx_language or "auto",
            "whisperx_diarize": config.whisperx_diarize,
            "whisperx_hf_token_configured": bool(
                config.whisperx_hf_token_env
                and os_environ_has(config.whisperx_hf_token_env)
            ),
            "whisperx_min_speakers": config.whisperx_min_speakers or "-",
            "whisperx_max_speakers": config.whisperx_max_speakers or "-",
        },
        "Watermark": {
            "watermark_enabled": config.watermark_enabled,
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
    configured_channels: list[str],
) -> list[ChannelStatus]:
    groups: dict[str, list[StreamStatus]] = {}
    configured: dict[str, list[str]] = {}
    display_names: dict[str, str] = {}

    for channel in configured_channels:
        name = channel_display_name(channel)
        key = channel_group_key(name)
        configured.setdefault(key, []).append(channel)
        groups.setdefault(key, [])
        display_names.setdefault(key, name)

    for stream in streams:
        name = stream.channel or "unknown channel"
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


def stream_status_from_record(
    config: BotConfig,
    record: StreamRecord,
    watermark_records: list[WatermarkCopyRecord] | None = None,
) -> StreamStatus:
    directory = segment_directory(config, record.video_id, record.channel)
    files = summarize_files(
        directory,
        record.video_id,
        config.watermark_enabled and bool(watermark_secret(config)),
        watermark_records or [],
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
        files=files[:FILE_LIMIT_PER_STREAM],
    )


def summarize_files(
    directory: Path,
    video_id: str,
    watermark_enabled: bool = False,
    watermark_records: list[WatermarkCopyRecord] | None = None,
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
        render_chat_url, render_chat_output_url, render_chat_status, render_chat_message = (
            chat_render_action_for_file(directory, video_id, path.name)
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

    media_file = chat_media_file_for_chat_file(directory_path, chat_path.name)
    if media_file is None:
        return None
    output_file = chat_video_output_file(media_file)
    return media_file, chat_path, output_file


def chat_media_file_for_chat_file(directory: Path, chat_filename: str) -> Path | None:
    if not is_live_chat_file(chat_filename):
        return None
    if not (directory / chat_filename).is_file():
        return None
    stem = chat_filename.removesuffix(LIVE_CHAT_SUFFIX)
    for suffix in CHAT_RENDER_MEDIA_SUFFIXES:
        candidate = directory / f"{stem}{suffix}"
        if candidate.is_file() and is_renderable_media_file(candidate.name):
            return candidate
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
    if Path(name).suffix.lower() not in CHAT_RENDER_MEDIA_SUFFIXES:
        return False
    return is_downloadable_file(name)


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
        )

    thread = Thread(
        target=run_render_chat_job,
        args=(config, key, media_file, chat_file, output_file, regenerate),
        name=f"ytdlbot-chat-render-{video_id}",
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
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=False,
            check=False,
        )
    except OSError as exc:
        LOGGER.exception("Unable to start isolated manual chat render process")
        update_render_chat_job(
            key,
            status="failed",
            message=str(exc) or exc.__class__.__name__,
            finished_at=time.time(),
        )
        return

    log_process_output(
        LOGGER,
        "isolated manual chat render",
        result.stdout or b"",
        result.stderr or b"",
        failed=result.returncode != 0,
    )
    if result.returncode != 0:
        message = process_failure_message(result.stdout or b"", result.stderr or b"")
        update_render_chat_job(
            key,
            status="failed",
            message=message or f"Chat render exited with code {result.returncode}",
            finished_at=time.time(),
        )
        return

    LOGGER.info("Manual chat render completed: %s", output_file)
    update_render_chat_job(
        key,
        status="done",
        message="Rendered chat video",
        finished_at=time.time(),
    )


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
            use_nvenc=config.chat_render_use_nvenc,
            nvenc_device=nvenc_device,
        )
    except Exception as exc:  # noqa: BLE001 - web job should capture renderer failures.
        LOGGER.exception(
            "Manual chat render failed for media=%s chat=%s",
            media_file,
            chat_file,
        )
        update_render_chat_job(
            key,
            status="failed",
            message=str(exc) or exc.__class__.__name__,
            finished_at=time.time(),
        )
        return

    LOGGER.info("Manual chat render completed: %s", output_file)
    update_render_chat_job(
        key,
        status="done",
        message="Rendered chat video",
        finished_at=time.time(),
    )


def chat_render_job_key(video_id: str, chat_filename: str) -> str:
    return f"{video_id}\0{chat_filename}"


def chat_render_job_for(video_id: str, chat_filename: str) -> RenderChatJob | None:
    with CHAT_RENDER_JOBS_LOCK:
        return CHAT_RENDER_JOBS.get(chat_render_job_key(video_id, chat_filename))


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
        )


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

    _record, media_file = resolved
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
        )

    thread = Thread(
        target=run_transcription_job,
        args=(config, key, media_file, regenerate),
        name=f"ytdlbot-transcribe-{video_id}",
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
) -> None:
    try:
        ok = asyncio.run(
            transcribe_media_file(
                config,
                media_file,
                overwrite=regenerate,
                logger=LOGGER,
            )
        )
    except Exception as exc:  # noqa: BLE001 - background job must capture failures.
        LOGGER.exception("Manual transcription failed for media=%s", media_file)
        update_transcription_job(
            key,
            status="failed",
            message=str(exc) or exc.__class__.__name__,
            finished_at=time.time(),
        )
        return

    if not ok:
        update_transcription_job(
            key,
            status="failed",
            message="WhisperX did not produce both .srt and .vtt outputs",
            finished_at=time.time(),
        )
        return

    LOGGER.info("Manual transcription completed: %s", media_file)
    update_transcription_job(
        key,
        status="done",
        message="Transcribed subtitles",
        finished_at=time.time(),
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
    finally:
        state.close()

    thread = Thread(
        target=run_watermark_job,
        args=(config, copy_id),
        name=f"ytdlbot-watermark-{video_id}",
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
        )
    except Exception as exc:  # noqa: BLE001 - background job must capture failures.
        LOGGER.exception("Watermark render failed copy_id=%s", copy_id)
        state.update_watermark_copy(
            copy_id,
            status=WATERMARK_STATUS_FAILED,
            message="Watermark render failed",
            error=str(exc) or exc.__class__.__name__,
            finished=True,
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
    if is_live_chat_file(name):
        return "chat"
    if is_yt_dlp_temporary_file(name):
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
    rows = "\n".join(render_stream_card(stream) for stream in snapshot.streams)
    if not rows:
        rows = '<section class="empty">No streams have been seen yet.</section>'
    channel_rows = render_channel_rows(snapshot.channel_stats)
    config_sections = render_config_sections(snapshot.configuration)
    log_rows = render_log_rows(snapshot.recent_logs)
    watermark_detection = render_watermark_detection_panel(snapshot.configuration)
    script = dashboard_script()

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>YTDLBot Status</title>
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
    .summary {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      gap: 10px;
    }}
    .metric, .stream, .empty, .panel {{
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
    #tab-streams:checked ~ .tabs label[for="tab-streams"],
    #tab-channels:checked ~ .tabs label[for="tab-channels"],
    #tab-logs:checked ~ .tabs label[for="tab-logs"],
    #tab-config:checked ~ .tabs label[for="tab-config"] {{
      color: var(--text);
      background: var(--panel-strong);
      font-weight: 650;
    }}
    #tab-streams:checked ~ .streams-panel,
    #tab-channels:checked ~ .channels-panel,
    #tab-logs:checked ~ .logs-panel,
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
    .stream-toggle {{
      color: inherit;
      cursor: pointer;
      font: inherit;
    }}
    .stream.collapsed .stream-body {{ display: none; }}
    .title {{ font-weight: 650; overflow-wrap: anywhere; }}
    .badge {{
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 3px 8px;
      white-space: nowrap;
      color: var(--muted);
      background: var(--panel-strong);
    }}
    .badge.downloading {{ color: var(--active); border-color: color-mix(in srgb, var(--active), transparent 55%); }}
    .badge.checking_after_exit, .badge.waiting_retry, .badge.interrupted {{ color: var(--warn); }}
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
    .files, .channels, .logs, .config-table {{
      width: 100%;
      border-collapse: collapse;
      margin-top: 8px;
      min-width: 860px;
    }}
    .channels {{ min-width: 980px; }}
    .logs {{ min-width: 860px; }}
    .config-table {{ table-layout: fixed; }}
    .config-table th:first-child, .config-table td:first-child {{ width: 32%; }}
    .config-table th:last-child, .config-table td:last-child {{ width: 68%; }}
    .files th, .files td, .channels th, .channels td, .logs th, .logs td,
    .config-table th, .config-table td {{
      border-top: 1px solid var(--line);
      padding: 7px 6px;
      text-align: left;
      vertical-align: top;
    }}
    .files th:last-child, .files td:last-child,
    .channels th:last-child, .channels td:last-child {{ text-align: right; }}
    .logs th:last-child, .logs td:last-child {{ text-align: left; }}
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
    <h1>YTDLBot Status</h1>
    <div class="summary">
      <div class="metric"><strong id="metric-total">{total}</strong><span>Total</span></div>
      <div class="metric"><strong id="metric-downloading">{active}</strong><span>Downloading</span></div>
      <div class="metric"><strong id="metric-checking">{checking}</strong><span>Checking</span></div>
      <div class="metric"><strong id="metric-attention">{len(attention_streams)}</strong><span>Attention</span></div>
      <div class="metric"><strong id="metric-channels">{len(snapshot.channel_stats)}</strong><span>Channels</span></div>
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
    <input class="tab-radio" type="radio" id="tab-streams" name="dashboard-tab" checked>
    <input class="tab-radio" type="radio" id="tab-channels" name="dashboard-tab">
    <input class="tab-radio" type="radio" id="tab-logs" name="dashboard-tab">
    <input class="tab-radio" type="radio" id="tab-config" name="dashboard-tab">
    <div class="tabs">
      <label for="tab-streams">Streams</label>
      <label for="tab-channels">Channels</label>
      <label for="tab-logs">Logs</label>
      <label for="tab-config">Config</label>
    </div>
    <section class="tab-panel streams-panel">
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
      <div id="streams-list">{rows}</div>
    </section>
    <section class="tab-panel channels-panel">
      <section class="panel">
        <h2>Channels</h2>
        <div class="table-wrap">
          <table class="channels">
            <thead><tr><th>Channel</th><th>Configured As</th><th>Streams</th><th>Downloading</th><th>Checking</th><th>Ended</th><th>Attention</th><th>Clips</th><th>Files</th><th>Final</th><th>Partial</th><th>Chat</th><th>Total</th><th>Latest Update</th><th>Latest File</th></tr></thead>
            <tbody id="channel-rows">{channel_rows}</tbody>
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
    <section class="tab-panel config-panel">
      <h2>Current Configuration</h2>
      <div class="file-meta">Sensitive yt-dlp arguments are redacted before display.</div>
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
  const tabKey = "ytdlbot.dashboardTab";
  const collapsedKey = "ytdlbot.collapsedStreams";
  const expandedKey = "ytdlbot.expandedStreams";
  const tabs = ["tab-streams", "tab-channels", "tab-logs", "tab-config"];
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
  const streamNeedsAttention = (stream) => attentionStatuses.has(stream.status) || Boolean(stream.has_mixed_formats);
  const readStreamSet = (key) => {
    try {
      const parsed = JSON.parse(localStorage.getItem(key) || "[]");
      return new Set(Array.isArray(parsed) ? parsed.map(String) : []);
    } catch (_) {
      return new Set();
    }
  };
  const collapsedStreams = readStreamSet(collapsedKey);
  const expandedStreams = readStreamSet(expandedKey);
  const writeCollapsedStreams = () => {
    try { localStorage.setItem(collapsedKey, JSON.stringify([...collapsedStreams])); } catch (_) {}
  };
  const writeExpandedStreams = () => {
    try { localStorage.setItem(expandedKey, JSON.stringify([...expandedStreams])); } catch (_) {}
  };
  const streamIsCollapsed = (videoId, status) => (
    collapsedStreams.has(videoId) || (status === "ended" && !expandedStreams.has(videoId))
  );

  const selectTab = (id, updateHash) => {
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
  try { stored = localStorage.getItem(tabKey) || ""; } catch (_) {}
  const hashTab = location.hash ? "tab-" + location.hash.slice(1) : "";
  selectTab(tabs.includes(hashTab) ? hashTab : stored, false);

  for (const id of tabs) {
    const tab = byId(id);
    if (!tab) continue;
    tab.addEventListener("change", () => {
      if (tab.checked) selectTab(id, true);
    });
  }

  const applyCollapsedState = (root) => {
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

  document.addEventListener("click", (event) => {
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

  const renderFileAction = (file) => {
    const actions = [];
    if (file.download_url) {
      actions.push(`<a class="download" href="${escapeAttr(file.download_url)}">Download</a>`);
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
        actions.push(`<span class="action-note" title="${escapeAttr(file.render_chat_message)}">Failed</span>`);
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
    <div class="table-wrap">
      <table class="files">
        <thead><tr><th>File</th><th>Segment</th><th>Format</th><th>Kind</th><th>Modified</th><th>Size</th><th>Action</th></tr></thead>
        <tbody>${files}</tbody>
      </table>
    </div>
  </div>
</section>`;
  };

  const renderChannelRows = (channels) => {
    if (!channels || !channels.length) {
      return '<tr><td colspan="15" class="file-meta">No configured channels or stream history found</td></tr>';
    }
    return channels.map((channel) => {
      const configuredAs = (channel.configured_sources || []).join(", ") || "-";
      const latestFileAge = formatEpochAge(channel.latest_file_modified_at);
      return [
        "<tr>",
        `<td class="file-name">${escapeHtml(channel.name)}</td>`,
        `<td class="file-name">${escapeHtml(configuredAs)}</td>`,
        `<td>${escapeHtml(channel.stream_count)}</td>`,
        `<td>${escapeHtml(channel.active_count)}</td>`,
        `<td>${escapeHtml(channel.checking_count)}</td>`,
        `<td>${escapeHtml(channel.ended_count)}</td>`,
        `<td>${escapeHtml(channel.attention_count)}</td>`,
        `<td>${escapeHtml(channel.downloadable_count)}</td>`,
        `<td>${escapeHtml(channel.file_count)}</td>`,
        `<td>${escapeHtml(formatBytes(channel.final_bytes))}</td>`,
        `<td>${escapeHtml(formatBytes(channel.part_bytes))}</td>`,
        `<td>${escapeHtml(formatBytes(channel.chat_bytes))}</td>`,
        `<td>${escapeHtml(formatBytes(channel.total_bytes))}</td>`,
        `<td>${escapeHtml(formatIso(channel.latest_updated_at))}</td>`,
        `<td>${escapeHtml(formatEpoch(channel.latest_file_modified_at))} <span class="muted">${escapeHtml(latestFileAge)}</span></td>`,
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

  const applySnapshot = (snapshot) => {
    const streams = snapshot.streams || [];
    const counts = snapshot.counts || {};
    setText("metric-total", streams.length);
    setText("metric-downloading", counts.downloading || 0);
    setText("metric-checking", counts.checking_after_exit || 0);
    setText("metric-attention", streams.filter(streamNeedsAttention).length);
    setText("metric-channels", (snapshot.channel_stats || []).length);
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
    const streamList = byId("streams-list");
    if (streamList) {
      streamList.innerHTML = streams.length
        ? streams.map(renderStreamCard).join("")
        : '<section class="empty">No streams have been seen yet.</section>';
    }
    const channelRows = byId("channel-rows");
    if (channelRows) channelRows.innerHTML = renderChannelRows(snapshot.channel_stats || []);
    const logRows = byId("log-rows");
    if (logRows) logRows.innerHTML = renderLogRows(snapshot.recent_logs || []);
    const configSections = byId("config-sections");
    if (configSections) configSections.innerHTML = renderConfigSections(snapshot.configuration || {});
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
  <p><a href="/#streams">Back to dashboard</a></p>
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
  <p><a href="/#streams">Back to dashboard</a></p>
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
    <div class="table-wrap">
      <table class="files">
        <thead><tr><th>File</th><th>Segment</th><th>Format</th><th>Kind</th><th>Modified</th><th>Size</th><th>Action</th></tr></thead>
        <tbody>{files}</tbody>
      </table>
    </div>
  </div>
</section>"""


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


def render_file_action(file: FileStatus) -> str:
    actions: list[str] = []
    if file.download_url:
        actions.append(
            f'<a class="download" href="{escape(file.download_url, quote=True)}">'
            "Download</a>"
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
            actions.append(
                '<span class="action-note" '
                f'title="{escape(file.render_chat_message, quote=True)}">Failed</span>'
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


def render_channel_rows(channels: list[ChannelStatus]) -> str:
    if not channels:
        return '<tr><td colspan="15" class="file-meta">No configured channels or stream history found</td></tr>'
    return "\n".join(render_channel_row(channel) for channel in channels)


def render_channel_row(channel: ChannelStatus) -> str:
    configured_as = ", ".join(channel.configured_sources) or "-"
    latest_update = format_optional_iso(channel.latest_updated_at)
    latest_file = format_optional_epoch(channel.latest_file_modified_at)
    latest_file_age = format_epoch_age(channel.latest_file_modified_at)
    return (
        "<tr>"
        f'<td class="file-name">{escape(channel.name)}</td>'
        f'<td class="file-name">{escape(configured_as)}</td>'
        f"<td>{channel.stream_count}</td>"
        f"<td>{channel.active_count}</td>"
        f"<td>{channel.checking_count}</td>"
        f"<td>{channel.ended_count}</td>"
        f"<td>{channel.attention_count}</td>"
        f"<td>{channel.downloadable_count}</td>"
        f"<td>{channel.file_count}</td>"
        f"<td>{escape(format_bytes(channel.final_bytes))}</td>"
        f"<td>{escape(format_bytes(channel.part_bytes))}</td>"
        f"<td>{escape(format_bytes(channel.chat_bytes))}</td>"
        f"<td>{escape(format_bytes(channel.total_bytes))}</td>"
        f"<td>{escape(latest_update)}</td>"
        f'<td>{escape(latest_file)} <span class="muted">{escape(latest_file_age)}</span></td>'
        "</tr>"
    )


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
