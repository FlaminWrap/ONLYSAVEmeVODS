from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Awaitable, Callable, Literal, Mapping, Protocol
import asyncio
import json
import logging
import re
import shlex
import subprocess
import sys
import time

from .chat_render import (
    ChatPanelRenderError,
    DEFAULT_CHAT_TIME_ZONE,
    VideoProbeError,
    build_render_chat_file_process_command,
    build_chat_panel_merge_command,
    build_chat_video_command,
    chat_layout_for_video,
    chat_render_fps_for_platform,
    chat_video_output_file,
    choose_chat_render_nvenc_device,
    ffprobe_path_for,
    log_chat_media_sync_diagnostics,
    parse_live_chat_file,
    probe_video_dimensions,
    probe_video_duration,
    render_chat_panel_video,
    run_ffmpeg_command_with_output_progress,
    write_chat_ass_file,
)
from .chat_refresh import refresh_chat_sidecar
from .chat_timing import (
    CHAT_TIMING_SUFFIX,
    chat_timing_file_for_chat_file,
    is_chat_timing_file,
    stream_start_iso,
    update_chat_timing,
    utc_now_iso,
)
from .config import (
    BotConfig,
    automatic_processing_window_wait_seconds,
    download_group_name_for_channel,
    post_stream_config_for_channel,
    streamer_for_channel,
)
from .content_events import (
    ContentEventDetectorUnavailable,
    detect_content_events_for_media,
    load_content_events,
)
from .job_tracker import finish_tracked_job, start_tracked_job, update_tracked_job
from .models import LiveStream
from .powerchat import (
    PowerchatRecorder,
    copy_powerchat_segment_sidecar,
    is_powerchat_event_file,
    run_powerchat_listener,
)
from .state import StateStore, StreamRecord
from .transcription import transcribe_media_file, transcription_config_for_channel
from .twitch_ad_repair import repair_twitch_ads_for_media
from .youtube import TerminalVideoUnavailableError, YouTubeLiveEdge


LOGGER = logging.getLogger(__name__)
RECONNECT_STOP_TIMEOUT_SECONDS = 20
FINALIZE_MUX_TIMEOUT_SECONDS = 60 * 60
FINALIZE_DURATION_TOLERANCE_SECONDS = 5.0
CATCHUP_FRAGMENT_MARGIN = 2
MIXED_SEGMENT_WATCH_SECONDS = 10
MIXED_SEGMENT_CONFIRM_SECONDS = 120
STALE_LIVE_WATCH_INTERVAL_SECONDS = 30
STALE_LIVE_CONFIRM_SECONDS = 30
PROCESSING_WINDOW_RECHECK_SECONDS = 60
DEFAULT_MEDIA_FORMAT = "bestvideo*+bestaudio/best"
FORMAT_OPTIONS = {"-f", "--format"}
YOUTUBE_FORMAT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")
SENSITIVE_COMMAND_OPTIONS = {
    "--add-header",
    "--ap-password",
    "--ap-username",
    "--cookies",
    "--cookies-from-browser",
    "--netrc-cmd",
    "--password",
    "--proxy",
    "--username",
    "--video-password",
    "--videopassword",
}
FRAGMENT_PROGRESS_RE = re.compile(
    r"(?:(?P<context>\d+):\s*)?\[download\].*?"
    r"\(frag\s+(?P<fragment>\d+)\s*/\s*(?P<count>\d+)\)"
)
KEPT_FRAGMENT_RE = re.compile(r"-Frag(?P<fragment>\d+)$")
CHANNEL_SOURCE_POST_EXIT_PLATFORMS = {"kick", "twitch"}


def post_exit_probe_target(stream: LiveStream) -> str:
    platform = stream.platform.strip().casefold()
    if platform in CHANNEL_SOURCE_POST_EXIT_PLATFORMS and stream.source:
        return stream.source
    return stream.url


def powerchat_settings_for_stream(
    config: BotConfig,
    stream: LiveStream,
) -> tuple[str, str] | None:
    for target in (stream.source, stream.channel):
        match = streamer_for_channel(config, target)
        if match is None:
            continue
        streamer_name, streamer = match
        username = streamer.powerchat_username.strip()
        if streamer.powerchat_enabled and username:
            return streamer_name, username
    return None


def chat_render_timezone_for_stream(config: BotConfig, stream: LiveStream) -> str:
    for target in (stream.source, stream.channel):
        match = streamer_for_channel(config, target)
        if match is not None:
            return match[1].timezone
    return DEFAULT_CHAT_TIME_ZONE


def powerchat_segment_sidecar_file(
    config: BotConfig,
    stream: LiveStream,
    segment_index: int,
) -> Path:
    directory = segment_directory(config, stream.video_id, stream.channel)
    return directory / f"segment-{segment_index:03d}.powerchat-events.json"


SleepFunc = Callable[[float], Awaitable[None]]
ProbeVideoFunc = Callable[[str], Awaitable[LiveStream]]
ProbeYouTubeLiveEdgeFunc = Callable[[str], Awaitable[YouTubeLiveEdge]]
MonotonicFunc = Callable[[], float]
LocalNowFunc = Callable[[], datetime]


def server_local_now() -> datetime:
    return datetime.now().astimezone()


ProgressCallback = Callable[[float], None]
YouTubeRestartDecision = Literal["restart", "defer", "stalled"]


class StreamProbe(Protocol):
    def probe_video(self, url: str) -> LiveStream:
        ...


@dataclass(slots=True)
class ActiveDownload:
    stream: LiveStream
    process: asyncio.subprocess.Process
    segment_index: int
    output_template: Path
    task: asyncio.Task[None]
    reconnect_task: asyncio.Task[None] | None = None
    output_task: asyncio.Task[None] | None = None
    mixed_segment_task: asyncio.Task[None] | None = None
    stale_live_task: asyncio.Task[None] | None = None
    chat_process: asyncio.subprocess.Process | None = None
    chat_task: asyncio.Task[None] | None = None
    chat_output_task: asyncio.Task[None] | None = None
    powerchat_task: asyncio.Task[None] | None = None
    powerchat_recorder: PowerchatRecorder | None = None


@dataclass(slots=True)
class FinalizePlan:
    output_file: Path
    input_files: list[Path]
    cleanup_files: list[Path]
    mixed_inputs: bool = False


@dataclass(frozen=True, slots=True)
class FinalizeMediaStream:
    path: Path
    input_index: int
    stream_index: int
    codec_type: str
    duration: float
    size: int
    partial: bool


@dataclass(frozen=True, slots=True)
class FinalizeOutputValidation:
    duration: float
    audio_streams: int
    video_streams: int


@dataclass(frozen=True, slots=True)
class SegmentRecoveryResult:
    output_file: Path
    duration: float
    size: int


@dataclass(slots=True)
class FinalizedSegmentFiles:
    segment_index: int
    channel: str
    media_file: Path | None
    chat_file: Path | None
    timing_file: Path | None


@dataclass(frozen=True, slots=True)
class YouTubeVideoFormatChoice:
    format_id: str
    codec: str
    selector: str
    audio_format_id: str
    height: int
    fps: float


class CatchupTracker:
    def __init__(
        self,
        ready_event: asyncio.Event,
        *,
        monotonic_func: MonotonicFunc = time.monotonic,
        initial_progress_at: float | None = None,
        progress_callback: ProgressCallback | None = None,
    ) -> None:
        self.ready_event = ready_event
        self.fragments: dict[str, tuple[int, int]] = {}
        self.has_prefixed_context = False
        self.monotonic = monotonic_func
        self.progress_callback = progress_callback
        self.last_fragment_progress_at = (
            self.monotonic()
            if initial_progress_at is None
            else initial_progress_at
        )

    def update(self, line: str) -> None:
        match = FRAGMENT_PROGRESS_RE.search(line)
        if not match:
            return

        context = match.group("context") or "0"
        self.has_prefixed_context = self.has_prefixed_context or context != "0"
        progress = (
            int(match.group("fragment")),
            int(match.group("count")),
        )
        previous = self.fragments.get(context)
        self.fragments[context] = progress
        if (
            previous is None
            or progress[0] > previous[0]
            or progress[1] > previous[1]
        ):
            self._note_progress()
        if self.caught_up:
            self.ready_event.set()

    def inactive_seconds(self) -> float:
        return max(0.0, self.monotonic() - self.last_fragment_progress_at)

    def note_external_progress(self) -> None:
        self._note_progress()

    def _note_progress(self) -> None:
        self.last_fragment_progress_at = self.monotonic()
        if self.progress_callback is not None:
            self.progress_callback(self.last_fragment_progress_at)

    @property
    def caught_up(self) -> bool:
        if not self.fragments:
            return False
        if self.has_prefixed_context and len(self.fragments) < 2:
            return False
        return all(
            fragment_count > 0
            and fragment_index >= fragment_count - CATCHUP_FRAGMENT_MARGIN
            for fragment_index, fragment_count in self.fragments.values()
        )


def youtube_live_edge_advanced(
    previous: YouTubeLiveEdge,
    current: YouTubeLiveEdge,
) -> bool:
    timestamp_advanced = (
        current.newest_segment_at is not None
        and previous.newest_segment_at is not None
        and current.newest_segment_at > previous.newest_segment_at
    )
    sequence_advanced = (
        current.media_sequence is not None
        and previous.media_sequence is not None
        and current.media_sequence > previous.media_sequence
    )
    return timestamp_advanced or sequence_advanced


def youtube_live_edge_is_fresh(
    edge: YouTubeLiveEdge,
    *,
    stale_after_seconds: int,
    now: datetime,
) -> bool:
    if edge.newest_segment_at is None:
        return False
    return (now - edge.newest_segment_at).total_seconds() < stale_after_seconds


def youtube_live_edge_is_confirmed_stale(
    previous: YouTubeLiveEdge,
    current: YouTubeLiveEdge,
    *,
    stale_after_seconds: int,
    now: datetime,
) -> bool:
    if previous.has_endlist and current.has_endlist:
        return True
    if (
        previous.media_sequence is None
        or current.media_sequence is None
        or previous.media_sequence != current.media_sequence
        or previous.newest_segment_at is None
        or current.newest_segment_at is None
        or current.newest_segment_at > previous.newest_segment_at
    ):
        return False
    return (now - current.newest_segment_at).total_seconds() >= stale_after_seconds


def youtube_live_edge_advanced_from_record(
    record: StreamRecord,
    edge: YouTubeLiveEdge,
) -> bool:
    if edge.has_endlist:
        return False
    recorded_edge = parse_youtube_live_edge_time(record.youtube_stale_edge_at)
    if recorded_edge is not None and edge.newest_segment_at is not None:
        return edge.newest_segment_at > recorded_edge
    return (
        record.youtube_stale_media_sequence is not None
        and edge.media_sequence is not None
        and edge.media_sequence > record.youtube_stale_media_sequence
    )


def format_youtube_live_edge_time(edge: YouTubeLiveEdge) -> str:
    if edge.newest_segment_at is None:
        return ""
    return edge.newest_segment_at.astimezone(timezone.utc).isoformat(
        timespec="milliseconds"
    )


def parse_youtube_live_edge_time(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


class DownloadManager:
    def __init__(
        self,
        config: BotConfig,
        state: StateStore,
        probe: StreamProbe,
        *,
        sleep_func: SleepFunc = asyncio.sleep,
        probe_video_func: ProbeVideoFunc | None = None,
        probe_youtube_live_edge_func: ProbeYouTubeLiveEdgeFunc | None = None,
        monotonic_func: MonotonicFunc = time.monotonic,
        local_now_func: LocalNowFunc = server_local_now,
        logger: logging.Logger = LOGGER,
    ) -> None:
        self.config = config
        self.state = state
        self.probe = probe
        self.sleep = sleep_func
        self.probe_video = probe_video_func or self._probe_video_in_thread
        self.probe_youtube_live_edge = (
            probe_youtube_live_edge_func or self._probe_youtube_live_edge_in_thread
        )
        self.monotonic = monotonic_func
        self.local_now = local_now_func
        self.logger = logger
        self.active: dict[str, ActiveDownload] = {}
        self._youtube_fragment_progress_at: dict[str, float] = {}
        self._youtube_restart_checks: set[str] = set()
        self._stalled_youtube_monitors: set[str] = set()
        self._post_exit_tasks: set[asyncio.Task[None]] = set()
        self._planned_reconnects: set[str] = set()
        self._spawn_failures: dict[str, int] = {}
        self._transcription_semaphore = asyncio.Semaphore(
            config.transcription_max_concurrent
        )
        self._stopping = False

    async def _wait_for_automatic_processing_window(
        self,
        stream: LiveStream,
        action: str,
        segment_index: int,
    ) -> bool:
        delayed = False
        while not self._stopping:
            wait_seconds = automatic_processing_window_wait_seconds(
                self.config,
                self.local_now(),
            )
            if wait_seconds <= 0:
                if delayed:
                    message = (
                        "Quiet-hours processing window opened; starting automatic "
                        f"{action}"
                    )
                    self.logger.info("%s video_id=%s", message, stream.video_id)
                    self.state.add_stream_event(
                        stream.video_id,
                        message,
                        segment_index=segment_index,
                    )
                return True
            if not delayed:
                message = (
                    f"Automatic {action} waiting for quiet-hours window "
                    f"{self.config.processing_quiet_hours_start}–"
                    f"{self.config.processing_quiet_hours_end} server local time"
                )
                self.logger.info("%s video_id=%s", message, stream.video_id)
                self.state.add_stream_event(
                    stream.video_id,
                    message,
                    segment_index=segment_index,
                )
                delayed = True
            await self.sleep(min(wait_seconds, PROCESSING_WINDOW_RECHECK_SECONDS))
        return False

    async def _probe_video_in_thread(self, url: str) -> LiveStream:
        return await asyncio.to_thread(self.probe.probe_video, url)

    async def _probe_youtube_live_edge_in_thread(self, url: str) -> YouTubeLiveEdge:
        probe = getattr(self.probe, "probe_youtube_live_edge", None)
        if not callable(probe):
            raise RuntimeError("stream probe does not support YouTube live-edge checks")
        return await asyncio.to_thread(probe, url)

    def _catchup_tracker_for_stream(
        self,
        stream: LiveStream,
        ready_event: asyncio.Event,
    ) -> CatchupTracker:
        if stream.platform.casefold() != "youtube":
            return CatchupTracker(
                ready_event,
                monotonic_func=self.monotonic,
            )

        initial_progress_at = self._youtube_fragment_progress_at.get(stream.video_id)
        if initial_progress_at is None:
            initial_progress_at = self.monotonic()
            self._youtube_fragment_progress_at[stream.video_id] = initial_progress_at

        def remember_progress(progress_at: float) -> None:
            self._youtube_fragment_progress_at[stream.video_id] = progress_at

        return CatchupTracker(
            ready_event,
            monotonic_func=self.monotonic,
            initial_progress_at=initial_progress_at,
            progress_callback=remember_progress,
        )

    def _remember_youtube_edge_progress(self, video_id: str) -> None:
        self._youtube_fragment_progress_at[video_id] = self.monotonic()

    async def start_stream(
        self,
        stream: LiveStream,
        *,
        segment_index: int | None = None,
    ) -> bool:
        if self._stopping:
            return False
        if stream.video_id in self.active:
            return False
        if stream.video_id in self._youtube_restart_checks:
            self.logger.debug(
                "Deferring %s while its YouTube live edge is checked before restart",
                stream.video_id,
            )
            return False
        if not await self._youtube_stalled_state_allows_start(stream):
            return False
        if len(self.active) >= self.config.max_concurrent_downloads:
            self.logger.info(
                "Concurrency limit reached; deferring %s (%s)",
                stream.video_id,
                stream.title,
            )
            self.state.upsert_detected(stream)
            return False

        self.state.upsert_detected(stream)
        if segment_index is None:
            record = self.state.get_stream(stream.video_id)
            segment_index = record.segment_index if record else 1
            restart_segment = choose_restart_segment(
                self.config,
                stream.video_id,
                segment_index,
                stream.channel,
            )
            if restart_segment != segment_index:
                segment_index = restart_segment
                self.state.set_segment_index(stream.video_id, segment_index)

        previous_format = self.state.get_stream(stream.video_id)
        youtube_video_format_selector = self._youtube_video_format_selector(stream)
        selected_format = self.state.get_stream(stream.video_id)
        selected_segment_index = youtube_format_segment_index(
            self.config,
            stream,
            segment_index,
            previous_format_id=(
                previous_format.youtube_video_format_id
                if previous_format is not None
                else ""
            ),
            previous_codec=(
                previous_format.youtube_video_codec
                if previous_format is not None
                else ""
            ),
            previous_selector=(
                previous_format.youtube_video_format_selector
                if previous_format is not None
                else ""
            ),
            selected_format_id=(
                selected_format.youtube_video_format_id
                if selected_format is not None
                else ""
            ),
            selected_codec=(
                selected_format.youtube_video_codec
                if selected_format is not None
                else ""
            ),
            selected_selector=(
                selected_format.youtube_video_format_selector
                if selected_format is not None
                else ""
            ),
        )
        if (
            previous_format is not None
            and selected_format is not None
            and selected_segment_index != segment_index
        ):
            previous_segment = segment_index
            segment_index = selected_segment_index
            self.state.set_segment_index(stream.video_id, segment_index)
            message = (
                "Started a new segment after YouTube format changed "
                f"id={previous_format.youtube_video_format_id}"
                f"->{selected_format.youtube_video_format_id} "
                f"codec={previous_format.youtube_video_codec}"
                f"->{selected_format.youtube_video_codec} "
                f"selector={previous_format.youtube_video_format_selector}"
                f"->{selected_format.youtube_video_format_selector}"
            )
            self.state.add_stream_event(
                stream.video_id,
                message,
                level="warning",
                segment_index=segment_index,
            )
            self.logger.warning(
                "%s video_id=%s previous_segment=%03d new_segment=%03d",
                message,
                stream.video_id,
                previous_segment,
                segment_index,
            )

        output_template = output_template_for(self.config, stream, segment_index)
        output_template.parent.mkdir(parents=True, exist_ok=True)
        command = build_download_command(
            self.config,
            stream,
            segment_index,
            youtube_video_format_selector=youtube_video_format_selector,
        )
        self.logger.debug(
            "Download output template for %s segment=%03d: %s",
            stream.video_id,
            segment_index,
            output_template,
        )
        self.logger.debug(
            "yt-dlp download command for %s segment=%03d: %s",
            stream.video_id,
            segment_index,
            command_for_log(command),
        )

        self.logger.info(
            "Starting download video_id=%s segment=%03d title=%r",
            stream.video_id,
            segment_index,
            stream.title,
        )
        self.state.mark_downloading(stream, segment_index)

        reconnect_ready = asyncio.Event()
        if not self.config.live_from_start:
            reconnect_ready.set()
        catchup_tracker = self._catchup_tracker_for_stream(stream, reconnect_ready)

        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            media_started_at = utc_now_iso()
        except FileNotFoundError:
            self.logger.exception("Unable to start yt-dlp; binary not found")
            self.state.mark_waiting_retry(stream.video_id)
            await self._schedule_spawn_retry(stream)
            return False
        except OSError:
            self.logger.exception("Unable to start yt-dlp for %s", stream.video_id)
            self.state.mark_waiting_retry(stream.video_id)
            await self._schedule_spawn_retry(stream)
            return False

        record_chat = should_record_chat_for_stream(self.config, stream)
        if record_chat:
            self.write_segment_timing_started(
                stream,
                segment_index,
                media_started_at=media_started_at,
            )
        task = asyncio.create_task(self._watch_process(stream, process, segment_index))
        output_task = None
        if process.stdout is not None:
            output_task = asyncio.create_task(
                self._monitor_process_output(
                    stream.video_id,
                    process.stdout,
                    catchup_tracker,
                )
            )
            output_task.add_done_callback(discard_task_exception)
        reconnect_task = None
        if self.config.reconnect_interval_seconds > 0:
            reconnect_task = asyncio.create_task(
                self._planned_reconnect_timer(
                    stream.video_id,
                    process,
                    reconnect_ready,
                )
            )
            reconnect_task.add_done_callback(discard_task_exception)
        mixed_segment_task = asyncio.create_task(
            self._mixed_segment_watchdog(stream, process, segment_index)
        )
        mixed_segment_task.add_done_callback(discard_task_exception)
        stale_live_task = None
        if (
            stream.platform.casefold() == "youtube"
            and self.config.youtube_stale_live_timeout_seconds > 0
        ):
            stale_live_task = asyncio.create_task(
                self._stale_youtube_live_watchdog(
                    stream,
                    process,
                    catchup_tracker,
                )
            )
            stale_live_task.add_done_callback(discard_task_exception)
        active = ActiveDownload(
            stream=stream,
            process=process,
            segment_index=segment_index,
            output_template=output_template,
            task=task,
            reconnect_task=reconnect_task,
            output_task=output_task,
            mixed_segment_task=mixed_segment_task,
            stale_live_task=stale_live_task,
        )
        self.active[stream.video_id] = active
        powerchat_recorder, powerchat_task = self._start_powerchat_listener(
            stream,
            segment_index,
            media_started_at=stream_start_iso(stream.raw) or media_started_at,
        )
        active.powerchat_recorder = powerchat_recorder
        active.powerchat_task = powerchat_task
        if record_chat:
            chat_process, chat_task, chat_output_task = await self._start_chat_recorder(
                stream,
                segment_index,
            )
            if self.active.get(stream.video_id) is active and process.returncode is None:
                active.chat_process = chat_process
                active.chat_task = chat_task
                active.chat_output_task = chat_output_task
            elif chat_process is not None:
                orphaned = ActiveDownload(
                    stream=stream,
                    process=process,
                    segment_index=segment_index,
                    output_template=output_template,
                    task=task,
                    chat_process=chat_process,
                    chat_task=chat_task,
                    chat_output_task=chat_output_task,
                )
                await self._stop_chat_recorder(orphaned)
        self._spawn_failures.pop(stream.video_id, None)
        return True

    def _youtube_video_format_selector(self, stream: LiveStream) -> str:
        if (
            stream.platform.casefold() != "youtube"
            or yt_dlp_args_include_format(self.config.extra_yt_dlp_args)
        ):
            return ""

        record = self.state.get_stream(stream.video_id)
        if record is not None and record.youtube_video_format_selector:
            choice = choose_youtube_video_format(
                stream,
                record.youtube_video_codec,
                preferred_format_id=record.youtube_video_format_id,
                preferred_audio_format_id=youtube_audio_format_id(
                    record.youtube_video_format_selector
                ),
            )
            if choice is None:
                self.logger.warning(
                    "Unable to refresh preferred YouTube format for %s; "
                    "reusing recorded format id=%s codec=%s",
                    stream.video_id,
                    record.youtube_video_format_id,
                    record.youtube_video_codec,
                )
                return record.youtube_video_format_selector

            if (
                choice.format_id != record.youtube_video_format_id
                or choice.codec != record.youtube_video_codec
                or choice.selector != record.youtube_video_format_selector
            ):
                previous_format_id = record.youtube_video_format_id
                previous_codec = record.youtube_video_codec
                record = self.state.update_youtube_video_format(
                    stream.video_id,
                    format_id=choice.format_id,
                    codec=choice.codec,
                    selector=choice.selector,
                )
                self.logger.warning(
                    "Changed YouTube format selection from current availability "
                    "video_id=%s "
                    "format_id=%s->%s codec=%s->%s selector=%s",
                    stream.video_id,
                    previous_format_id,
                    choice.format_id,
                    previous_codec,
                    choice.codec,
                    record.youtube_video_format_selector,
                )
            self.logger.info(
                "Reusing preferred YouTube video format video_id=%s format_id=%s "
                "codec=%s selector=%s",
                stream.video_id,
                record.youtube_video_format_id,
                record.youtube_video_codec,
                record.youtube_video_format_selector,
            )
            return record.youtube_video_format_selector

        choice = choose_youtube_video_format(
            stream,
            self.config.youtube_preferred_video_codec,
        )
        if choice is None:
            self.logger.warning(
                "Unable to lock a YouTube video format for %s; metadata contained "
                "no usable video formats",
                stream.video_id,
            )
            return ""

        record = self.state.lock_youtube_video_format(
            stream.video_id,
            format_id=choice.format_id,
            codec=choice.codec,
            selector=choice.selector,
        )
        self.logger.info(
            "Preferred YouTube video format video_id=%s format_id=%s codec=%s "
            "height=%s fps=%s selector=%s",
            stream.video_id,
            choice.format_id,
            choice.codec,
            choice.height or "unknown",
            choice.fps or "unknown",
            record.youtube_video_format_selector,
        )
        return record.youtube_video_format_selector

    async def _start_chat_recorder(
        self,
        stream: LiveStream,
        segment_index: int,
    ) -> tuple[
        asyncio.subprocess.Process | None,
        asyncio.Task[None] | None,
        asyncio.Task[None] | None,
    ]:
        command = build_chat_download_command(self.config, stream, segment_index)
        self.logger.debug(
            "yt-dlp live chat command for %s segment=%03d: %s",
            stream.video_id,
            segment_index,
            command_for_log(command),
        )
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            chat_started_at = utc_now_iso()
        except FileNotFoundError:
            self.logger.warning("Unable to start live chat recorder; yt-dlp not found")
            return None, None, None
        except OSError:
            self.logger.exception(
                "Unable to start live chat recorder for %s",
                stream.video_id,
            )
            return None, None, None

        self.logger.info(
            "Started live chat recorder video_id=%s segment=%03d",
            stream.video_id,
            segment_index,
        )
        self.update_segment_timing(
            stream,
            segment_index,
            chat_started_at=chat_started_at,
        )
        task = asyncio.create_task(self._watch_chat_process(stream.video_id, process))
        task.add_done_callback(discard_task_exception)
        output_task = None
        if process.stdout is not None:
            output_task = asyncio.create_task(
                self._monitor_sidecar_output(
                    stream.video_id,
                    "live-chat",
                    process.stdout,
                )
            )
            output_task.add_done_callback(discard_task_exception)
        return process, task, output_task

    def _start_powerchat_listener(
        self,
        stream: LiveStream,
        segment_index: int,
        *,
        media_started_at: str,
    ) -> tuple[PowerchatRecorder | None, asyncio.Task[None] | None]:
        settings = powerchat_settings_for_stream(self.config, stream)
        if settings is None:
            return None, None
        streamer_name, username = settings
        sidecar_path = powerchat_segment_sidecar_file(
            self.config,
            stream,
            segment_index,
        )
        recorder = PowerchatRecorder(
            sidecar_path=sidecar_path,
            streamer_name=streamer_name,
            username=username,
            video_id=stream.video_id,
            segment_index=segment_index,
            stream_started_at=media_started_at,
        )

        def status_callback(message: str, level: str) -> None:
            self.state.add_stream_event(
                stream.video_id,
                message,
                level=level,
                segment_index=segment_index,
            )

        task = asyncio.create_task(
            run_powerchat_listener(
                username,
                recorder,
                logger=self.logger,
                status_callback=status_callback,
            )
        )
        task.add_done_callback(discard_task_exception)
        self.state.add_stream_event(
            stream.video_id,
            f"Powerchat listener starting username={username}",
            segment_index=segment_index,
        )
        return recorder, task

    async def _youtube_stalled_state_allows_start(
        self,
        stream: LiveStream,
    ) -> bool:
        if stream.platform.casefold() != "youtube":
            return True
        record = self.state.get_stream(stream.video_id)
        if record is None or not record.youtube_stale_detected_at:
            return True

        self.logger.debug(
            "Deferring stalled YouTube stream while its monitor checks the edge "
            "video_id=%s sequence=%s edge=%s",
            stream.video_id,
            record.youtube_stale_media_sequence,
            record.youtube_stale_edge_at or "unknown",
        )
        return False

    async def _check_youtube_before_restart(
        self,
        stream: LiveStream,
    ) -> YouTubeRestartDecision:
        timeout_seconds = self.config.youtube_stale_live_timeout_seconds
        if stream.platform.casefold() != "youtube" or timeout_seconds <= 0:
            return "restart"

        check_started_at = self.monotonic()
        self._youtube_restart_checks.add(stream.video_id)
        try:
            try:
                previous = await self.probe_youtube_live_edge(stream.url)
            except Exception as exc:
                self.logger.warning(
                    "Unable to inspect YouTube live edge before restarting %s: %s",
                    stream.video_id,
                    exc,
                )
                return "restart"

            if (
                previous.media_sequence is None
                and previous.newest_segment_at is None
                and not previous.has_endlist
            ):
                return "restart"

            while True:
                await self.sleep(STALE_LIVE_CONFIRM_SECONDS)
                if self._stopping:
                    return "defer"

                record = self.state.get_stream(stream.video_id)
                if (
                    stream.video_id in self.active
                    or record is None
                    or record.status != "checking_after_exit"
                ):
                    return "defer"

                try:
                    current = await self.probe_youtube_live_edge(stream.url)
                except Exception as exc:
                    self.logger.warning(
                        "Unable to confirm YouTube live edge before restarting %s: %s",
                        stream.video_id,
                        exc,
                    )
                    return "restart"

                now = datetime.now(timezone.utc)
                if youtube_live_edge_is_confirmed_stale(
                    previous,
                    current,
                    stale_after_seconds=timeout_seconds,
                    now=now,
                ):
                    edge_at = format_youtube_live_edge_time(current)
                    self.logger.warning(
                        "Confirmed frozen YouTube live edge before restart; "
                        "marking stalled video_id=%s sequence=%s edge=%s",
                        stream.video_id,
                        current.media_sequence,
                        edge_at or "unknown",
                    )
                    self.state.mark_youtube_stale_live(
                        stream.video_id,
                        media_sequence=current.media_sequence,
                        edge_at=edge_at,
                    )
                    return "stalled"

                if youtube_live_edge_advanced(previous, current):
                    self._remember_youtube_edge_progress(stream.video_id)
                    return "restart"

                if current.newest_segment_at is None and not current.has_endlist:
                    return "restart"

                if (
                    self.monotonic() - check_started_at
                    >= timeout_seconds + STALE_LIVE_CONFIRM_SECONDS
                ):
                    self.logger.warning(
                        "YouTube live edge remained inconclusive for %s; "
                        "allowing restart after %ss",
                        stream.video_id,
                        timeout_seconds + STALE_LIVE_CONFIRM_SECONDS,
                    )
                    return "restart"

                previous = current
        finally:
            self._youtube_restart_checks.discard(stream.video_id)

    async def monitor_stalled_youtube(
        self,
        stream: LiveStream,
        segment_index: int,
    ) -> None:
        if stream.video_id in self._stalled_youtube_monitors:
            return

        self._stalled_youtube_monitors.add(stream.video_id)
        resume_stream: LiveStream | None = None
        resume_segment = segment_index
        observed_stream = stream
        consecutive_non_live_probes = 0
        try:
            while not self._stopping:
                record = self.state.get_stream(stream.video_id)
                if (
                    record is None
                    or record.status != "stalled"
                    or not record.youtube_stale_detected_at
                ):
                    return
                segment_index = record.segment_index

                try:
                    latest = await self.probe_video(post_exit_probe_target(observed_stream))
                except TerminalVideoUnavailableError as exc:
                    self.logger.info(
                        "Stalled YouTube stream %s is terminally unavailable: %s",
                        stream.video_id,
                        exc,
                    )
                    if self._stream_status_matches(stream.video_id, "stalled"):
                        await self.finish_ended_stream(observed_stream, segment_index)
                    return
                except Exception as exc:
                    latest = None
                    self.logger.warning(
                        "Unable to check stalled YouTube metadata for %s: %s",
                        stream.video_id,
                        exc,
                    )

                if latest is not None:
                    observed_stream = latest
                    if not latest.is_live:
                        consecutive_non_live_probes += 1
                        if consecutive_non_live_probes >= 2:
                            self.logger.info(
                                "Stalled YouTube stream %s repeatedly reports ended; "
                                "finalizing",
                                stream.video_id,
                            )
                            if self._stream_status_matches(
                                stream.video_id,
                                "stalled",
                            ):
                                await self.finish_ended_stream(latest, segment_index)
                            return
                    else:
                        consecutive_non_live_probes = 0

                try:
                    edge = await self.probe_youtube_live_edge(observed_stream.url)
                except Exception as exc:
                    self.logger.warning(
                        "Unable to inspect stalled YouTube live edge for %s: %s",
                        stream.video_id,
                        exc,
                    )
                else:
                    record = self.state.get_stream(stream.video_id)
                    if (
                        record is None
                        or record.status != "stalled"
                        or not record.youtube_stale_detected_at
                    ):
                        return
                    if edge.has_endlist:
                        self.logger.info(
                            "Stalled YouTube stream %s published an HLS end marker; "
                            "finalizing",
                            stream.video_id,
                        )
                        await self.finish_ended_stream(observed_stream, segment_index)
                        return
                    if youtube_live_edge_advanced_from_record(record, edge):
                        self.logger.info(
                            "Stalled YouTube live edge advanced; resuming %s",
                            stream.video_id,
                        )
                        resume_segment = await self.choose_live_restart_segment(
                            observed_stream,
                            segment_index,
                            stream.channel,
                        )
                        if resume_segment != segment_index:
                            self.state.set_segment_index(
                                stream.video_id,
                                resume_segment,
                            )
                        self._remember_youtube_edge_progress(stream.video_id)
                        self.state.clear_youtube_stale_live(stream.video_id)
                        resume_stream = observed_stream
                        break

                await self.sleep(max(1, self.config.poll_interval_seconds))
        finally:
            self._stalled_youtube_monitors.discard(stream.video_id)

        if resume_stream is not None and not self._stopping:
            await self.start_stream(resume_stream, segment_index=resume_segment)

    async def _stale_youtube_live_watchdog(
        self,
        stream: LiveStream,
        process: asyncio.subprocess.Process,
        tracker: CatchupTracker,
    ) -> None:
        timeout_seconds = self.config.youtube_stale_live_timeout_seconds
        watch_interval = min(STALE_LIVE_WATCH_INTERVAL_SECONDS, timeout_seconds)
        try:
            while not self._stopping and process.returncode is None:
                await self.sleep(watch_interval)
                if self._stopping or process.returncode is not None:
                    return
                if tracker.inactive_seconds() < timeout_seconds:
                    continue

                progress_checkpoint = tracker.last_fragment_progress_at
                try:
                    first = await self.probe_youtube_live_edge(stream.url)
                except Exception as exc:
                    self.logger.warning(
                        "Unable to inspect inactive YouTube live edge for %s: %s",
                        stream.video_id,
                        exc,
                    )
                    continue

                await self.sleep(STALE_LIVE_CONFIRM_SECONDS)
                if self._stopping or process.returncode is not None:
                    return
                if tracker.last_fragment_progress_at > progress_checkpoint:
                    continue

                try:
                    second = await self.probe_youtube_live_edge(stream.url)
                except Exception as exc:
                    self.logger.warning(
                        "Unable to confirm inactive YouTube live edge for %s: %s",
                        stream.video_id,
                        exc,
                    )
                    continue

                if tracker.last_fragment_progress_at > progress_checkpoint:
                    continue
                now = datetime.now(timezone.utc)
                if youtube_live_edge_is_confirmed_stale(
                    first,
                    second,
                    stale_after_seconds=timeout_seconds,
                    now=now,
                ):
                    edge_at = format_youtube_live_edge_time(second)
                    self.logger.warning(
                        "Confirmed frozen YouTube live edge; pausing recording "
                        "video_id=%s sequence=%s edge=%s inactive=%ss",
                        stream.video_id,
                        second.media_sequence,
                        edge_at or "unknown",
                        int(tracker.inactive_seconds()),
                    )
                    self.state.mark_youtube_stale_live(
                        stream.video_id,
                        media_sequence=second.media_sequence,
                        edge_at=edge_at,
                    )
                    await self._stop_stale_live_process(stream.video_id, process)
                    return

                if youtube_live_edge_advanced(first, second) or youtube_live_edge_is_fresh(
                    second,
                    stale_after_seconds=timeout_seconds,
                    now=now,
                ):
                    tracker.note_external_progress()
        except asyncio.CancelledError:
            raise
        except ProcessLookupError:
            return

    async def _stop_stale_live_process(
        self,
        video_id: str,
        process: asyncio.subprocess.Process,
    ) -> None:
        process.terminate()
        try:
            await asyncio.wait_for(
                process.wait(),
                timeout=RECONNECT_STOP_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            if process.returncode is None:
                self.logger.warning(
                    "yt-dlp did not stop after stale-live detection for %s "
                    "within %ss; killing",
                    video_id,
                    RECONNECT_STOP_TIMEOUT_SECONDS,
                )
                process.kill()
                await process.wait()

    async def _planned_reconnect_timer(
        self,
        video_id: str,
        process: asyncio.subprocess.Process,
        reconnect_ready: asyncio.Event,
    ) -> None:
        try:
            self.logger.debug(
                "Waiting for catch-up before planned reconnect timer for %s",
                video_id,
            )
            await reconnect_ready.wait()
            if self._stopping or process.returncode is not None:
                return

            self.logger.info(
                "Stream %s has caught up; planned reconnect timer started for %ss",
                video_id,
                self.config.reconnect_interval_seconds,
            )
            await self.sleep(self.config.reconnect_interval_seconds)
            if self._stopping or process.returncode is not None:
                return

            self.logger.info(
                "Planned reconnect for %s after %ss",
                video_id,
                self.config.reconnect_interval_seconds,
            )
            await self._request_process_reconnect(video_id, process)
        except asyncio.CancelledError:
            raise
        except ProcessLookupError:
            return

    async def _mixed_segment_watchdog(
        self,
        stream: LiveStream,
        process: asyncio.subprocess.Process,
        segment_index: int,
    ) -> None:
        try:
            while not self._stopping and process.returncode is None:
                await self.sleep(MIXED_SEGMENT_WATCH_SECONDS)
                if self._stopping or process.returncode is not None:
                    return
                if not segment_has_mixed_format_files(
                    self.config,
                    stream.video_id,
                    segment_index,
                    stream.channel,
                ):
                    continue

                self.logger.info(
                    "Detected split finalized/partial tracks for %s segment=%03d; "
                    "waiting %ss to allow yt-dlp to finish or recover",
                    stream.video_id,
                    segment_index,
                    MIXED_SEGMENT_CONFIRM_SECONDS,
                )
                await self.sleep(MIXED_SEGMENT_CONFIRM_SECONDS)
                if self._stopping or process.returncode is not None:
                    return
                if not segment_has_mixed_format_files(
                    self.config,
                    stream.video_id,
                    segment_index,
                    stream.channel,
                ):
                    continue

                self.logger.warning(
                    "Split finalized/partial tracks persisted for %s segment=%03d; "
                    "reconnecting to restore both tracks from kept fragments",
                    stream.video_id,
                    segment_index,
                )
                await self._request_process_reconnect(stream.video_id, process)
                return
        except asyncio.CancelledError:
            raise
        except ProcessLookupError:
            return

    async def _request_process_reconnect(
        self,
        video_id: str,
        process: asyncio.subprocess.Process,
    ) -> None:
        self._planned_reconnects.add(video_id)
        process.terminate()
        try:
            await asyncio.wait_for(
                process.wait(),
                timeout=RECONNECT_STOP_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            if process.returncode is None:
                self.logger.warning(
                    "yt-dlp did not stop for reconnect of %s within %ss; killing",
                    video_id,
                    RECONNECT_STOP_TIMEOUT_SECONDS,
                )
                try:
                    process.kill()
                except ProcessLookupError:
                    return

    async def _monitor_process_output(
        self,
        video_id: str,
        stream: asyncio.StreamReader,
        catchup_tracker: CatchupTracker,
    ) -> None:
        buffer = ""
        while not stream.at_eof():
            chunk = await stream.read(4096)
            if not chunk:
                break

            buffer += chunk.decode("utf-8", "replace").replace("\r", "\n")
            lines = buffer.split("\n")
            buffer = lines.pop()
            for line in lines:
                self._handle_process_output_line(video_id, line, catchup_tracker)

        if buffer:
            self._handle_process_output_line(video_id, buffer, catchup_tracker)

    async def _monitor_sidecar_output(
        self,
        video_id: str,
        label: str,
        stream: asyncio.StreamReader,
    ) -> None:
        buffer = ""
        while not stream.at_eof():
            chunk = await stream.read(4096)
            if not chunk:
                break

            buffer += chunk.decode("utf-8", "replace").replace("\r", "\n")
            lines = buffer.split("\n")
            buffer = lines.pop()
            for line in lines:
                self._handle_sidecar_output_line(video_id, label, line)

        if buffer:
            self._handle_sidecar_output_line(video_id, label, buffer)

    def _handle_process_output_line(
        self,
        video_id: str,
        line: str,
        catchup_tracker: CatchupTracker,
    ) -> None:
        line = line.strip()
        if not line:
            return

        catchup_tracker.update(line)
        self.logger.debug("yt-dlp %s: %s", video_id, line)

    def _handle_sidecar_output_line(
        self,
        video_id: str,
        label: str,
        line: str,
    ) -> None:
        line = line.strip()
        if not line:
            return

        self.logger.debug("yt-dlp %s %s: %s", label, video_id, line)

    async def _watch_chat_process(
        self,
        video_id: str,
        process: asyncio.subprocess.Process,
    ) -> None:
        exit_code = await process.wait()
        if self._stopping:
            self.logger.info("Live chat recorder stopped for %s during shutdown", video_id)
            return
        if exit_code == 0:
            self.logger.info("Live chat recorder exited video_id=%s", video_id)
            return
        self.logger.warning(
            "Live chat recorder exited video_id=%s exit_code=%s",
            video_id,
            exit_code,
        )

    async def _finish_output_task(self, output_task: asyncio.Task[None]) -> None:
        try:
            await asyncio.wait_for(output_task, timeout=5)
        except asyncio.TimeoutError:
            output_task.cancel()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.logger.debug("yt-dlp output monitor exited with error: %s", exc)

    async def _watch_process(
        self,
        stream: LiveStream,
        process: asyncio.subprocess.Process,
        segment_index: int,
    ) -> None:
        exit_code = await process.wait()
        active = self.active.get(stream.video_id)
        if active and active.process is process:
            self.active.pop(stream.video_id, None)
            if active.reconnect_task and active.reconnect_task is not asyncio.current_task():
                active.reconnect_task.cancel()
            if (
                active.mixed_segment_task
                and active.mixed_segment_task is not asyncio.current_task()
            ):
                active.mixed_segment_task.cancel()
            if (
                active.stale_live_task
                and active.stale_live_task is not asyncio.current_task()
            ):
                active.stale_live_task.cancel()
            if active.output_task:
                await self._finish_output_task(active.output_task)
            await self._stop_powerchat_listener(active)
            await self._stop_chat_recorder(active)

        if self._stopping:
            self.logger.info("Downloader stopped for %s during shutdown", stream.video_id)
            return

        self.logger.info(
            "yt-dlp exited video_id=%s segment=%03d exit_code=%s",
            stream.video_id,
            segment_index,
            exit_code,
        )
        self.state.mark_exited(stream.video_id, int(exit_code))
        record = self.state.get_stream(stream.video_id)
        self.update_segment_timing_if_exists(
            stream,
            segment_index,
            last_exit_at=record.last_exit_at if record else utc_now_iso(),
        )
        planned_reconnect = stream.video_id in self._planned_reconnects
        if planned_reconnect:
            self._planned_reconnects.discard(stream.video_id)
        if planned_reconnect and not (
            record is not None and record.status == "stalled"
        ):
            task = asyncio.create_task(self.handle_planned_reconnect(stream, segment_index))
            self._post_exit_tasks.add(task)
            task.add_done_callback(self._post_exit_tasks.discard)
            return

        task = asyncio.create_task(self.handle_post_exit(stream, segment_index))
        self._post_exit_tasks.add(task)
        task.add_done_callback(self._post_exit_tasks.discard)

    async def _stop_chat_recorder(self, active: ActiveDownload) -> None:
        process = active.chat_process
        if process is None:
            return

        if process.returncode is None:
            self.logger.info(
                "Terminating live chat recorder for %s",
                active.stream.video_id,
            )
            try:
                process.terminate()
            except ProcessLookupError:
                pass

        try:
            await asyncio.wait_for(
                process.wait(),
                timeout=RECONNECT_STOP_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            if process.returncode is None:
                self.logger.warning(
                    "Live chat recorder did not stop for %s within %ss; killing",
                    active.stream.video_id,
                    RECONNECT_STOP_TIMEOUT_SECONDS,
                )
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
                await process.wait()

        if active.chat_output_task:
            await self._finish_output_task(active.chat_output_task)
        if (
            active.chat_task
            and active.chat_task is not asyncio.current_task()
            and not active.chat_task.done()
        ):
            try:
                await asyncio.wait_for(active.chat_task, timeout=5)
            except asyncio.TimeoutError:
                active.chat_task.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.debug("live chat watcher exited with error: %s", exc)

    async def _stop_powerchat_listener(self, active: ActiveDownload) -> None:
        task = active.powerchat_task
        recorder = active.powerchat_recorder
        if task is not None and not task.done():
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=5)
            except asyncio.TimeoutError:
                self.logger.debug(
                    "Powerchat listener did not stop promptly for %s",
                    active.stream.video_id,
                )
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                self.logger.debug("Powerchat listener exited with error: %s", exc)
        if recorder is not None:
            try:
                recorder.write()
            except OSError as exc:
                self.logger.warning(
                    "Unable to flush Powerchat sidecar for %s: %s",
                    active.stream.video_id,
                    exc,
                )

    async def handle_planned_reconnect(
        self,
        stream: LiveStream,
        segment_index: int,
    ) -> None:
        try:
            latest = await self.probe_video(post_exit_probe_target(stream))
        except TerminalVideoUnavailableError as exc:
            await self._mark_terminal_unavailable(stream, segment_index, exc)
            return
        except Exception as exc:
            self.logger.warning(
                "Planned reconnect probe failed for %s: %s; using post-exit schedule",
                stream.video_id,
                exc,
            )
            await self.handle_post_exit(stream, segment_index)
            return

        if latest.is_live:
            restart_decision = (
                await self._check_youtube_before_restart(latest)
            )
            if self._stopping:
                return
            if restart_decision == "stalled":
                await self.monitor_stalled_youtube(stream, segment_index)
                return
            if restart_decision != "restart" or stream.video_id in self.active:
                return
            next_segment = await self.choose_live_restart_segment(
                latest,
                segment_index,
                stream.channel,
            )
            if next_segment != segment_index:
                self.state.set_segment_index(stream.video_id, next_segment)

            self.logger.info(
                "Stream %s still live during planned reconnect; restarting segment=%03d",
                stream.video_id,
                next_segment,
            )
            await self.start_stream(latest, segment_index=next_segment)
            return

        self.logger.info(
            "Stream %s not live during planned reconnect; using post-exit schedule",
            stream.video_id,
        )
        await self.handle_post_exit(stream, segment_index)

    def resume_post_exit_check(
        self,
        stream: LiveStream,
        segment_index: int,
        *,
        elapsed_since_exit_seconds: float = 0.0,
    ) -> None:
        if self._stopping:
            return
        task = asyncio.create_task(
            self.recover_post_exit_check(
                stream,
                segment_index,
                elapsed_since_exit_seconds=elapsed_since_exit_seconds,
            )
        )
        self._post_exit_tasks.add(task)
        task.add_done_callback(self._post_exit_tasks.discard)

    def resume_stalled_youtube_check(
        self,
        stream: LiveStream,
        segment_index: int,
    ) -> None:
        if self._stopping:
            return
        task = asyncio.create_task(
            self.monitor_stalled_youtube(stream, segment_index)
        )
        self._post_exit_tasks.add(task)
        task.add_done_callback(self._post_exit_tasks.discard)

    async def recover_post_exit_check(
        self,
        stream: LiveStream,
        segment_index: int,
        *,
        elapsed_since_exit_seconds: float = 0.0,
    ) -> None:
        if not self._stream_status_matches(stream.video_id, "checking_after_exit"):
            return
        self.logger.warning(
            "Recovering post-exit checks after service restart video_id=%s segment=%03d elapsed=%.1fs",
            stream.video_id,
            segment_index,
            max(0.0, elapsed_since_exit_seconds),
        )
        self.state.add_stream_event(
            stream.video_id,
            "Resuming post-exit checks after service restart",
            level="warning",
            segment_index=segment_index,
        )
        await self.handle_post_exit(
            stream,
            segment_index,
            elapsed_since_exit_seconds=elapsed_since_exit_seconds,
            expected_status="checking_after_exit",
        )

    def _stream_status_matches(self, video_id: str, expected_status: str | None) -> bool:
        if expected_status is None:
            return True
        record = self.state.get_stream(video_id)
        return record is not None and record.status == expected_status

    async def handle_post_exit(
        self,
        stream: LiveStream,
        segment_index: int,
        *,
        elapsed_since_exit_seconds: float = 0.0,
        expected_status: str | None = None,
    ) -> None:
        record = self.state.get_stream(stream.video_id)
        if record is not None and record.youtube_stale_detected_at:
            if not self._stream_status_matches(stream.video_id, expected_status):
                return
            self.logger.info(
                "Monitoring stalled YouTube stream %s until it resumes or ends",
                stream.video_id,
            )
            await self.monitor_stalled_youtube(stream, segment_index)
            return

        previous_offset = 0.0
        elapsed_since_exit_seconds = max(0.0, elapsed_since_exit_seconds)
        self.logger.debug(
            "Starting post-exit checks for %s segment=%03d schedule=%s elapsed=%.1fs",
            stream.video_id,
            segment_index,
            self.config.post_exit_check_seconds,
            elapsed_since_exit_seconds,
        )
        for offset in self.config.post_exit_check_seconds:
            if self._stopping:
                return
            if not self._stream_status_matches(stream.video_id, expected_status):
                self.logger.info(
                    "Stopping recovered post-exit checks for %s; stream status changed",
                    stream.video_id,
                )
                return

            delay = 0.0
            if elapsed_since_exit_seconds < offset:
                delay = max(0.0, offset - max(previous_offset, elapsed_since_exit_seconds))
            previous_offset = float(offset)
            if delay:
                self.logger.debug(
                    "Waiting %ss before post-exit probe for %s at +%ss",
                    delay,
                    stream.video_id,
                    offset,
                )
                await self.sleep(delay)
                if self._stopping:
                    return
                if not self._stream_status_matches(stream.video_id, expected_status):
                    self.logger.info(
                        "Stopping recovered post-exit checks for %s; stream status changed",
                        stream.video_id,
                    )
                    return

            try:
                self.logger.debug(
                    "Running post-exit probe for %s at +%ss",
                    stream.video_id,
                    offset,
                )
                latest = await self.probe_video(post_exit_probe_target(stream))
            except TerminalVideoUnavailableError as exc:
                if self._stream_status_matches(stream.video_id, expected_status):
                    await self._mark_terminal_unavailable(stream, segment_index, exc)
                return
            except Exception as exc:
                self.logger.warning(
                    "Post-exit probe failed for %s at +%ss: %s",
                    stream.video_id,
                    offset,
                    exc,
                )
                continue

            if latest.is_live:
                if not self._stream_status_matches(stream.video_id, expected_status):
                    return
                restart_decision = (
                    await self._check_youtube_before_restart(latest)
                )
                if self._stopping:
                    return
                if restart_decision == "stalled":
                    await self.monitor_stalled_youtube(stream, segment_index)
                    return
                if restart_decision != "restart" or stream.video_id in self.active:
                    return
                next_segment = await self.choose_live_restart_segment(
                    latest,
                    segment_index,
                    stream.channel,
                )
                if next_segment != segment_index:
                    self.state.set_segment_index(stream.video_id, next_segment)

                self.logger.info(
                    "Stream %s is still live at +%ss; restarting segment=%03d",
                    stream.video_id,
                    offset,
                    next_segment,
                )
                await self.start_stream(latest, segment_index=next_segment)
                return

            self.logger.info(
                "Stream %s not live at +%ss; continuing post-exit checks",
                stream.video_id,
                offset,
            )

        if not self._stream_status_matches(stream.video_id, expected_status):
            return
        self.logger.info(
            "Stream %s did not return live during post-exit window; marking ended",
            stream.video_id,
        )
        await self.finish_ended_stream(stream, segment_index)

    async def _mark_terminal_unavailable(
        self,
        stream: LiveStream,
        segment_index: int,
        exc: Exception,
    ) -> None:
        self.logger.info(
            "Stream %s is terminally unavailable; ending checks: %s",
            stream.video_id,
            exc,
        )
        await self.finish_ended_stream(stream, segment_index)

    async def finish_ended_stream(
        self,
        stream: LiveStream,
        segment_index: int,
    ) -> None:
        await self.finalize_ended_segment(
            stream.video_id,
            segment_index,
            stream.channel,
        )
        finalized_files = self.rename_finalized_segments(stream, segment_index)
        self.finalize_powerchat_sidecars(stream, finalized_files)
        if should_record_chat_for_stream(self.config, stream):
            await self.refresh_finalized_chat_files(stream, finalized_files)
        self.state.mark_ended(stream.video_id)
        self._youtube_fragment_progress_at.pop(stream.video_id, None)
        post_stream_config = post_stream_config_for_channel(self.config, stream.channel)
        if (
            post_stream_config.twitch_ad_repair_enabled
            and stream.platform == "twitch"
            and await self._wait_for_automatic_processing_window(
                stream,
                "Twitch ad repair",
                segment_index,
            )
        ):
            await self.repair_finalized_twitch_ads(stream, finalized_files)
        if (
            post_stream_config.transcribe_subtitles
            and await self._wait_for_automatic_processing_window(
                stream,
                "subtitle transcription",
                segment_index,
            )
        ):
            await self.transcribe_finalized_media(
                stream,
                finalized_files,
                post_stream_config=post_stream_config,
            )
        if (
            post_stream_config.stream_event_detection_enabled
            and await self._wait_for_automatic_processing_window(
                stream,
                "content event detection",
                segment_index,
            )
        ):
            await self.detect_finalized_content_events(
                stream,
                finalized_files,
                post_stream_config=post_stream_config,
            )
        if (
            post_stream_config.render_live_chat_video
            and stream.platform == "youtube"
            and await self._wait_for_automatic_processing_window(
                stream,
                "chat video rendering",
                segment_index,
            )
        ):
            await self.render_finalized_chat_videos(stream, finalized_files)

    def rename_finalized_segments(
        self,
        stream: LiveStream,
        segment_index: int,
    ) -> list[FinalizedSegmentFiles]:
        finalized_files: list[FinalizedSegmentFiles] = []
        for index in range(1, segment_index + 1):
            media_file = rename_finalized_segment_file(
                self.config,
                stream,
                index,
                self.logger,
            )
            chat_file = rename_segment_chat_file(
                self.config,
                stream,
                index,
                self.logger,
            )
            timing_file = rename_segment_timing_file(
                self.config,
                stream,
                index,
                self.logger,
            )
            finalized_files.append(
                FinalizedSegmentFiles(
                    segment_index=index,
                    channel=stream.channel,
                    media_file=media_file,
                    chat_file=chat_file,
                    timing_file=timing_file,
                )
            )
        return finalized_files

    def finalize_powerchat_sidecars(
        self,
        stream: LiveStream,
        finalized_files: list[FinalizedSegmentFiles],
    ) -> None:
        settings = powerchat_settings_for_stream(self.config, stream)
        streamer_name = settings[0] if settings else ""
        username = settings[1] if settings else ""
        for files in finalized_files:
            if files.media_file is None:
                continue
            source = powerchat_segment_sidecar_file(
                self.config,
                stream,
                files.segment_index,
            )
            target = copy_powerchat_segment_sidecar(
                source,
                files.media_file,
                streamer_name=streamer_name,
                username=username,
                video_id=stream.video_id,
                segment_index=files.segment_index,
                logger=self.logger,
            )
            if target is None:
                continue
            self.state.add_stream_event(
                stream.video_id,
                f"Powerchat events saved for {files.media_file.name}",
                segment_index=files.segment_index,
            )

    async def render_finalized_chat_videos(
        self,
        stream: LiveStream,
        finalized_files: list[FinalizedSegmentFiles],
    ) -> None:
        for files in finalized_files:
            if files.media_file is None or files.chat_file is None:
                continue
            await self.render_live_chat_video(
                stream,
                files.media_file,
                files.chat_file,
                files.segment_index,
            )

    async def refresh_finalized_chat_files(
        self,
        stream: LiveStream,
        finalized_files: list[FinalizedSegmentFiles],
    ) -> None:
        record = self.state.get_stream(stream.video_id)
        last_exit_at = record.last_exit_at if record else None
        for files in finalized_files:
            if files.media_file is None or files.chat_file is None:
                continue
            result = await asyncio.to_thread(
                refresh_chat_sidecar,
                self.config,
                video_url=stream.url,
                media_file=files.media_file,
                chat_file=files.chat_file,
                last_exit_at=last_exit_at,
                stream_metadata=stream.raw,
                timing_file=files.timing_file,
                logger=self.logger,
            )
            if result.ok:
                self.logger.info(
                    "Chat refresh completed segment=%03d source=%s message=%s",
                    files.segment_index,
                    result.source,
                    result.message,
                )
            else:
                self.logger.warning(
                    "Chat refresh unavailable segment=%03d message=%s",
                    files.segment_index,
                    result.message,
                )

    def write_segment_timing_started(
        self,
        stream: LiveStream,
        segment_index: int,
        *,
        media_started_at: str,
    ) -> None:
        self.update_segment_timing(
            stream,
            segment_index,
            stream_started_at=stream_start_iso(stream.raw),
            media_started_at=media_started_at,
            media_live_from_start=self.config.live_from_start,
        )

    def update_segment_timing(
        self,
        stream: LiveStream,
        segment_index: int,
        **changes: object,
    ) -> None:
        timing_file = segment_timing_file(
            self.config,
            stream.video_id,
            segment_index,
            stream.channel,
        )
        payload = {
            "video_id": stream.video_id,
            "segment_index": segment_index,
            **changes,
        }
        try:
            update_chat_timing(timing_file, **payload)
        except OSError:
            self.logger.warning(
                "Unable to write live chat timing sidecar %s",
                timing_file,
            )

    def update_segment_timing_if_exists(
        self,
        stream: LiveStream,
        segment_index: int,
        **changes: object,
    ) -> None:
        timing_file = segment_timing_file(
            self.config,
            stream.video_id,
            segment_index,
            stream.channel,
        )
        if not timing_file.is_file():
            return
        self.update_segment_timing(stream, segment_index, **changes)

    async def transcribe_finalized_media(
        self,
        stream: LiveStream,
        finalized_files: list[FinalizedSegmentFiles],
        *,
        post_stream_config: BotConfig | None = None,
    ) -> None:
        processing_config = post_stream_config or self.config
        for files in finalized_files:
            if files.media_file is None:
                continue
            job_id = auto_job_id("transcription", stream.video_id, files.media_file.name)
            start_tracked_job(
                job_id,
                kind="Transcription",
                video_id=stream.video_id,
                item=files.media_file.name,
                detail="Automatic post-finalize transcription",
                phase="Queued",
                message="Queued automatic transcription",
                progress=0.0,
            )
            try:
                async with self._transcription_semaphore:
                    update_tracked_job(
                        job_id,
                        phase="Running WhisperX",
                        progress=0.1,
                        message="Running automatic transcription",
                    )
                    await transcribe_media_file(
                        transcription_config_for_channel(processing_config, files.channel),
                        files.media_file,
                        logger=self.logger,
                        channel=files.channel,
                    )
            except Exception as exc:  # noqa: BLE001 - post-processing must not break finalization.
                self.logger.exception("Automatic transcription failed for %s", files.media_file)
                finish_tracked_job(
                    job_id,
                    status="failed",
                    phase="Failed",
                    message=str(exc) or exc.__class__.__name__,
                    progress=None,
                )
                self.state.add_stream_event(
                    stream.video_id,
                    f"Transcription failed for {files.media_file.name}: {exc}",
                    level="error",
                    segment_index=files.segment_index,
                )
                continue
            finish_tracked_job(
                job_id,
                message="Automatic transcription completed",
            )

    async def repair_finalized_twitch_ads(
        self,
        stream: LiveStream,
        finalized_files: list[FinalizedSegmentFiles],
    ) -> None:
        record = self.state.get_stream(stream.video_id)
        started_at = record.last_started_at if record else None
        for files in finalized_files:
            if files.media_file is None:
                continue
            job_id = auto_job_id("twitch-ad-repair", stream.video_id, files.media_file.name)
            start_tracked_job(
                job_id,
                kind="Twitch ad repair",
                video_id=stream.video_id,
                item=files.media_file.name,
                detail="Automatic Twitch commercial break detection and repair",
                phase="Queued",
                message="Queued Twitch ad repair",
                progress=0.0,
            )

            def report_progress(phase: str, value: float | None) -> None:
                update_tracked_job(
                    job_id,
                    phase=phase,
                    progress=value,
                    message=phase,
                )

            try:
                result = await asyncio.to_thread(
                    repair_twitch_ads_for_media,
                    self.config,
                    stream,
                    files.media_file,
                    started_at=started_at,
                    progress_callback=report_progress,
                    logger=self.logger,
                )
            except Exception as exc:  # noqa: BLE001 - post-processing must not break finalization.
                self.logger.exception("Twitch ad repair failed for %s", files.media_file)
                finish_tracked_job(
                    job_id,
                    status="failed",
                    phase="Failed",
                    message=str(exc) or exc.__class__.__name__,
                    progress=None,
                )
                self.state.add_stream_event(
                    stream.video_id,
                    f"Twitch ad repair failed for {files.media_file.name}: {exc}",
                    level="error",
                    segment_index=files.segment_index,
                )
                continue

            if result.repaired and result.output_file:
                files.media_file = Path(result.output_file)
                finish_tracked_job(
                    job_id,
                    message=result.message,
                )
                self.state.add_stream_event(
                    stream.video_id,
                    f"Twitch ad repair completed for {files.media_file.name}: {result.message}",
                    segment_index=files.segment_index,
                )
                continue

            finish_tracked_job(
                job_id,
                status="done",
                phase="Complete",
                message=result.message,
            )
            self.state.add_stream_event(
                stream.video_id,
                f"Twitch ad repair skipped for {files.media_file.name}: {result.message}",
                level="info",
                segment_index=files.segment_index,
            )

    async def detect_finalized_content_events(
        self,
        stream: LiveStream,
        finalized_files: list[FinalizedSegmentFiles],
        *,
        post_stream_config: BotConfig | None = None,
    ) -> None:
        processing_config = post_stream_config or self.config
        for files in finalized_files:
            if files.media_file is None:
                continue
            job_id = auto_job_id("content-events", stream.video_id, files.media_file.name)
            start_tracked_job(
                job_id,
                kind="Event detection",
                video_id=stream.video_id,
                item=files.media_file.name,
                detail="Automatic post-finalize content event detection",
                phase="Queued",
                message="Queued automatic content event detection",
                progress=0.0,
            )
            try:
                update_tracked_job(
                    job_id,
                    phase="Detecting content events",
                    progress=0.1,
                    message="Detecting content events",
                )
                ok = await asyncio.to_thread(
                    detect_content_events_for_media,
                    processing_config,
                    files.media_file,
                    logger=self.logger,
                    channel=files.channel,
                )
            except ContentEventDetectorUnavailable as exc:
                self.logger.warning(
                    "Content event detection unavailable for %s: %s",
                    files.media_file,
                    exc,
                )
                finish_tracked_job(
                    job_id,
                    status="failed",
                    phase="Unavailable",
                    message=str(exc),
                    progress=None,
                )
                self.state.add_stream_event(
                    stream.video_id,
                    f"Content event detection unavailable for {files.media_file.name}: {exc}",
                    level="warning",
                    segment_index=files.segment_index,
                )
                continue
            except Exception as exc:  # noqa: BLE001 - post-processing must not break finalization.
                self.logger.exception(
                    "Content event detection failed for %s",
                    files.media_file,
                )
                finish_tracked_job(
                    job_id,
                    status="failed",
                    phase="Failed",
                    message=str(exc) or exc.__class__.__name__,
                    progress=None,
                )
                self.state.add_stream_event(
                    stream.video_id,
                    f"Content event detection failed for {files.media_file.name}: {exc}",
                    level="error",
                    segment_index=files.segment_index,
                )
                continue
            if ok:
                event_count = len(load_content_events(files.media_file))
                finish_tracked_job(
                    job_id,
                    message=f"Detected {event_count} content event(s)",
                )
                self.state.add_stream_event(
                    stream.video_id,
                    f"Content event detection completed for {files.media_file.name}: {event_count} event(s)",
                    segment_index=files.segment_index,
                )
            else:
                finish_tracked_job(
                    job_id,
                    status="done",
                    phase="Complete",
                    message="No content events detected",
                )

    async def render_live_chat_video(
        self,
        stream: LiveStream,
        media_file: Path,
        chat_file: Path,
        segment_index: int,
    ) -> bool:
        output_file = chat_video_output_file(media_file)
        temp_output = output_file.with_name(
            f"{output_file.stem}.rendering{output_file.suffix}"
        )
        ass_file = output_file.with_name(f"{output_file.stem}.ass")
        timezone_name = chat_render_timezone_for_stream(self.config, stream)
        if output_file.exists():
            self.logger.info(
                "Chat render output already exists for segment=%03d: %s",
                segment_index,
                output_file,
            )
            return True

        job_id = auto_job_id("chat-render", stream.video_id, chat_file.name)
        start_tracked_job(
            job_id,
            kind="Chat render",
            video_id=stream.video_id,
            item=output_file.name,
            detail=f"{media_file.name} + {chat_file.name}",
            phase="Queued",
            message="Queued automatic chat render",
            progress=0.0,
        )

        if self.config.config_path is not None:
            update_tracked_job(
                job_id,
                phase="Starting isolated renderer",
                progress=0.05,
                message="Starting isolated chat renderer",
            )
        subprocess_result = await self.render_live_chat_video_process(
            media_file,
            chat_file,
            output_file,
            segment_index,
            stream.platform,
            timezone_name,
        )
        if subprocess_result is not None:
            finish_tracked_job(
                job_id,
                status="done" if subprocess_result else "failed",
                phase="Complete" if subprocess_result else "Failed",
                message="Automatic chat render completed" if subprocess_result else "Automatic chat render failed",
                progress=1.0 if subprocess_result else None,
            )
            return subprocess_result

        started_at = time.monotonic()
        update_tracked_job(
            job_id,
            phase="Preparing chat render",
            progress=0.05,
            message="Preparing automatic chat render",
        )
        self.logger.info(
            "Preparing chat render segment=%03d media=%s chat=%s output=%s",
            segment_index,
            media_file,
            chat_file,
            output_file,
        )
        nvenc_device = choose_chat_render_nvenc_device(
            self.config.chat_render_nvenc_devices,
            segment_index - 1,
        )
        frame_rate = chat_render_fps_for_platform(stream.platform)
        if self.config.chat_render_use_nvenc:
            self.logger.info(
                "Selected NVENC device for chat render segment=%03d device=%s",
                segment_index,
                nvenc_device or "default",
            )
        try:
            entries = parse_live_chat_file(chat_file)
        except OSError as exc:
            self.logger.exception("Unable to read live chat file %s", chat_file)
            finish_tracked_job(
                job_id,
                status="failed",
                phase="Failed",
                message=str(exc) or exc.__class__.__name__,
                progress=None,
            )
            return False

        if not entries:
            self.logger.info("No live chat messages found in %s", chat_file)
            finish_tracked_job(
                job_id,
                status="failed",
                phase="No chat messages",
                message="No live chat messages found",
                progress=None,
            )
            return False
        self.logger.info(
            "Parsed live chat for render segment=%03d entries=%d "
            "first_offset=%.2fs last_offset=%.2fs",
            segment_index,
            len(entries),
            entries[0].offset_seconds,
            entries[-1].offset_seconds,
        )

        try:
            dimensions = probe_video_dimensions(
                media_file,
                ffprobe_path_for(self.config.ffmpeg_path),
            )
            duration = probe_video_duration(
                media_file,
                ffprobe_path_for(self.config.ffmpeg_path),
            )
            layout = chat_layout_for_video(dimensions.width, dimensions.height)
            self.logger.info(
                "Probed media for chat render segment=%03d video=%sx%s "
                "duration=%.2fs output=%sx%s panel_width=%s",
                segment_index,
                dimensions.width,
                dimensions.height,
                duration,
                layout.output_width,
                layout.output_height,
                layout.panel_width,
            )
            log_chat_media_sync_diagnostics(
                entries,
                duration,
                media_file=media_file,
                chat_file=chat_file,
                logger=self.logger,
            )
        except VideoProbeError:
            layout = None
            duration = 0.0
            self.logger.exception(
                "Unable to probe video size for chat render; using fallback layout for %s",
                media_file,
            )

        temp_output.unlink(missing_ok=True)
        panel_file = output_file.with_name(f"{output_file.stem}.panel.mp4")
        panel_file.unlink(missing_ok=True)
        if layout is not None and duration > 0:
            try:
                self.logger.info(
                    "Rendering image chat panel segment=%03d panel=%s",
                    segment_index,
                    panel_file,
                )
                update_tracked_job(
                    job_id,
                    phase="Rendering chat panel",
                    progress=0.35,
                    message="Rendering chat panel",
                )
                await asyncio.to_thread(
                    render_chat_panel_video,
                    entries,
                    layout,
                    panel_file,
                    duration,
                    self.config.ffmpeg_path,
                    self.config.chat_emoji_cache_dir,
                    self.config.chat_render_panel_workers,
                    self.config.chat_render_use_nvenc,
                    nvenc_device,
                    frame_rate,
                    timezone_name=timezone_name,
                )
                command = build_chat_panel_merge_command(
                    self.config.ffmpeg_path,
                    media_file,
                    panel_file,
                    temp_output,
                    layout,
                    use_nvenc=self.config.chat_render_use_nvenc,
                    nvenc_device=nvenc_device,
                    frame_rate=frame_rate,
                )
                self.logger.info(
                    "Merging rendered chat panel segment=%03d media=%s panel=%s",
                    segment_index,
                    media_file,
                    panel_file,
                )
                update_tracked_job(
                    job_id,
                    phase="Merging chat video",
                    progress=0.75,
                    message="Merging rendered chat panel",
                )
            except ChatPanelRenderError:
                panel_file.unlink(missing_ok=True)
                self.logger.exception(
                    "Unable to render image chat panel; falling back to subtitle renderer"
                )
                try:
                    write_chat_ass_file(
                        ass_file,
                        entries,
                        layout,
                        timezone_name,
                    )
                except OSError as exc:
                    self.logger.exception(
                        "Unable to write live chat subtitle file %s",
                        ass_file,
                    )
                    finish_tracked_job(
                        job_id,
                        status="failed",
                        phase="Failed",
                        message=str(exc) or exc.__class__.__name__,
                        progress=None,
                    )
                    return False
                self.logger.info(
                    "Using subtitle fallback for chat render segment=%03d ass=%s",
                    segment_index,
                    ass_file,
                )
                command = build_chat_video_command(
                    self.config.ffmpeg_path,
                    media_file,
                    ass_file,
                    temp_output,
                    layout,
                    use_nvenc=self.config.chat_render_use_nvenc,
                    nvenc_device=nvenc_device,
                    frame_rate=frame_rate,
                )
        else:
            try:
                write_chat_ass_file(
                    ass_file,
                    entries,
                    layout,
                    timezone_name,
                )
            except OSError as exc:
                self.logger.exception("Unable to write live chat subtitle file %s", ass_file)
                finish_tracked_job(
                    job_id,
                    status="failed",
                    phase="Failed",
                    message=str(exc) or exc.__class__.__name__,
                    progress=None,
                )
                return False
            self.logger.info(
                "Using subtitle fallback for chat render segment=%03d ass=%s",
                segment_index,
                ass_file,
            )
            command = build_chat_video_command(
                self.config.ffmpeg_path,
                media_file,
                ass_file,
                temp_output,
                layout,
                use_nvenc=self.config.chat_render_use_nvenc,
                nvenc_device=nvenc_device,
                frame_rate=frame_rate,
            )
        self.logger.info(
            "Rendering chat video for segment=%03d as %s",
            segment_index,
            output_file,
        )
        self.logger.debug("ffmpeg chat render command: %s", command_for_log(command))

        try:
            update_tracked_job(
                job_id,
                phase="Rendering chat video",
                progress=0.8,
                message="Rendering chat video with ffmpeg",
            )
            result, ffmpeg_elapsed = await asyncio.to_thread(
                run_ffmpeg_command_with_output_progress,
                command,
                temp_output,
                chat_render_timeout_seconds(self.config),
            )
        except FileNotFoundError:
            self.logger.warning(
                "Unable to render chat video; ffmpeg not found: %s",
                self.config.ffmpeg_path,
            )
            ass_file.unlink(missing_ok=True)
            panel_file.unlink(missing_ok=True)
            finish_tracked_job(
                job_id,
                status="failed",
                phase="Failed",
                message=f"ffmpeg not found: {self.config.ffmpeg_path}",
                progress=None,
            )
            return False
        except OSError as exc:
            self.logger.exception("Unable to start ffmpeg for chat video render")
            ass_file.unlink(missing_ok=True)
            panel_file.unlink(missing_ok=True)
            finish_tracked_job(
                job_id,
                status="failed",
                phase="Failed",
                message=str(exc) or exc.__class__.__name__,
                progress=None,
            )
            return False
        except subprocess.TimeoutExpired:
            temp_output.unlink(missing_ok=True)
            ass_file.unlink(missing_ok=True)
            panel_file.unlink(missing_ok=True)
            self.logger.warning(
                "ffmpeg made no output progress while rendering chat video"
            )
            finish_tracked_job(
                job_id,
                status="failed",
                phase="Timed out",
                message="ffmpeg made no output progress while rendering chat video",
                progress=None,
            )
            return False
        finally:
            ass_file.unlink(missing_ok=True)
            panel_file.unlink(missing_ok=True)

        if result.returncode != 0:
            temp_output.unlink(missing_ok=True)
            message = (result.stderr or result.stdout).decode("utf-8", "replace").strip()
            self.logger.warning("ffmpeg failed while rendering chat video: %s", message)
            finish_tracked_job(
                job_id,
                status="failed",
                phase="Failed",
                message=message or f"ffmpeg exited with code {result.returncode}",
                progress=None,
            )
            return False

        temp_output.rename(output_file)
        output_size = output_file.stat().st_size if output_file.exists() else 0
        self.logger.info(
            "Rendered chat video %s size=%s elapsed=%.1fs ffmpeg_elapsed=%.1fs",
            output_file,
            output_size,
            time.monotonic() - started_at,
            ffmpeg_elapsed,
        )
        finish_tracked_job(
            job_id,
            message=f"Rendered chat video {format_byte_count(output_size)}",
        )
        return True

    async def render_live_chat_video_process(
        self,
        media_file: Path,
        chat_file: Path,
        output_file: Path,
        segment_index: int,
        platform: str = "",
        timezone_name: str = DEFAULT_CHAT_TIME_ZONE,
    ) -> bool | None:
        if self.config.config_path is None:
            return None

        command = build_render_chat_file_process_command(
            sys.executable,
            self.config.config_path,
            media_file,
            chat_file,
            output_file,
            platform=platform,
            timezone_name=timezone_name,
        )
        self.logger.info(
            "Starting isolated chat render process segment=%03d media=%s chat=%s "
            "output=%s",
            segment_index,
            media_file,
            chat_file,
            output_file,
        )
        self.logger.debug("isolated chat render command: %s", command_for_log(command))
        started_at = time.monotonic()
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except OSError:
            self.logger.exception("Unable to start isolated chat render process")
            return False

        stdout, stderr = await process.communicate()

        log_process_output(
            self.logger,
            "isolated chat render",
            stdout,
            stderr,
            failed=process.returncode != 0,
        )
        if process.returncode != 0:
            self.logger.warning(
                "Isolated chat render process failed segment=%03d exit_code=%s",
                segment_index,
                process.returncode,
            )
            return False

        output_size = output_file.stat().st_size if output_file.exists() else 0
        self.logger.info(
            "Isolated chat render process completed segment=%03d output=%s "
            "size=%s elapsed=%.1fs",
            segment_index,
            output_file,
            output_size,
            time.monotonic() - started_at,
        )
        return True

    async def choose_live_restart_segment(
        self,
        stream: LiveStream,
        segment_index: int,
        fallback_channel: str = "",
    ) -> int:
        channel = stream.channel or fallback_channel
        self.logger.debug(
            "Choosing restart segment for %s current_segment=%03d channel=%r",
            stream.video_id,
            segment_index,
            channel,
        )
        if segment_has_mixed_format_files(
            self.config,
            stream.video_id,
            segment_index,
            channel,
        ):
            try:
                restored = restore_mixed_segment_for_resume(
                    self.config,
                    stream.video_id,
                    segment_index,
                    channel,
                )
            except OSError:
                restored = False
                self.logger.exception(
                    "Unable to restore split-track segment for %s segment=%03d",
                    stream.video_id,
                    segment_index,
                )

            if restored:
                self.logger.info(
                    "Prepared split-track segment for resumed live download of %s "
                    "segment=%03d",
                    stream.video_id,
                    segment_index,
                )
                self.state.add_stream_event(
                    stream.video_id,
                    f"Restored split-track segment={segment_index:03d} from kept fragments",
                    segment_index=segment_index,
                )
                return segment_index

            self.logger.warning(
                "Unable to restore split-track segment for exact resume of %s "
                "segment=%03d; finalizing this segment before starting a new "
                "live-from-start segment",
                stream.video_id,
                segment_index,
            )
            if not await self.finalize_ended_segment(
                stream.video_id,
                segment_index,
                channel,
            ):
                self.logger.warning(
                    "Unable to finalize split-track segment for %s segment=%03d; "
                    "continuing with the next segment",
                    stream.video_id,
                    segment_index,
            )
            return segment_index + 1

        if segment_final_format_files(
            self.config,
            stream.video_id,
            segment_index,
            channel,
        ):
            try:
                restored = restore_mixed_segment_for_resume(
                    self.config,
                    stream.video_id,
                    segment_index,
                    channel,
                )
            except OSError:
                restored = False
                self.logger.exception(
                    "Unable to restore finalized segment for %s segment=%03d",
                    stream.video_id,
                    segment_index,
                )

            if restored:
                self.logger.info(
                    "Prepared finalized segment for resumed live download of %s segment=%03d",
                    stream.video_id,
                    segment_index,
                )
                self.state.add_stream_event(
                    stream.video_id,
                    f"Restored finalized segment={segment_index:03d} from kept fragments",
                    segment_index=segment_index,
                )
                return segment_index

        next_segment = choose_restart_segment(
            self.config,
            stream.video_id,
            segment_index,
            channel,
        )
        self.logger.debug(
            "Restart segment decision for %s: current=%03d next=%03d",
            stream.video_id,
            segment_index,
            next_segment,
        )
        return next_segment

    async def finalize_ended_segment(
        self,
        video_id: str,
        segment_index: int,
        channel: str = "",
    ) -> bool:
        try:
            plan = prepare_finalize_plan(self.config, video_id, segment_index, channel)
        except OSError:
            self.logger.exception(
                "Unable to prepare partial segment finalization for %s segment=%03d",
                video_id,
                segment_index,
            )
            return False

        if plan is None:
            self.logger.debug(
                "No partial segment files to finalize for %s segment=%03d",
                video_id,
                segment_index,
            )
            return True
        self.logger.debug(
            "Finalize plan for %s segment=%03d output=%s inputs=%s cleanup=%s "
            "mixed_inputs=%s",
            video_id,
            segment_index,
            plan.output_file,
            [str(path) for path in plan.input_files],
            [str(path) for path in plan.cleanup_files],
            plan.mixed_inputs,
        )

        if len(plan.input_files) == 1:
            if not await self._finalize_single_input(plan):
                return False
            cleanup_files(plan.cleanup_files, self.logger)
            self.logger.info(
                "Finalized partial segment for %s segment=%03d as %s",
                video_id,
                segment_index,
                plan.output_file,
            )
            return True

        if await self._mux_finalize_inputs(plan):
            cleanup_files([*plan.input_files, *plan.cleanup_files], self.logger)
            self.logger.info(
                "Muxed partial segment for %s segment=%03d as %s",
                video_id,
                segment_index,
                plan.output_file,
            )
            return True

        return False

    async def _finalize_single_input(self, plan: FinalizePlan) -> bool:
        input_file = plan.input_files[0]
        if input_file == plan.output_file:
            return True
        if plan.output_file.exists():
            self.logger.warning(
                "Final output already exists; leaving partial input in place: %s",
                plan.output_file,
            )
            return False
        input_file.rename(plan.output_file)
        return True

    async def _mux_finalize_inputs(self, plan: FinalizePlan) -> bool:
        if plan.output_file.exists():
            self.logger.warning(
                "Final output already exists; leaving partial inputs in place: %s",
                plan.output_file,
            )
            return False

        ffprobe_path = ffprobe_path_for(self.config.ffmpeg_path)
        try:
            media_streams = probe_finalize_media_streams(
                plan.input_files,
                ffprobe_path,
            )
            selected_streams = select_finalize_media_streams(media_streams)
        except VideoProbeError as exc:
            self.logger.warning(
                "Unable to safely select partial segment inputs; preserving all "
                "source files: %s",
                exc,
            )
            return False

        self.logger.info(
            "Selected finalization streams: %s",
            [
                {
                    "type": stream.codec_type,
                    "path": str(stream.path),
                    "stream": stream.stream_index,
                    "duration": round(stream.duration, 3),
                    "partial": stream.partial,
                }
                for stream in selected_streams
            ],
        )
        temp_output = plan.output_file.with_name(
            f"{plan.output_file.stem}.muxing{plan.output_file.suffix}"
        )
        temp_output.unlink(missing_ok=True)

        command = [
            self.config.ffmpeg_path,
            "-hide_banner",
            "-loglevel",
            "warning",
            "-y",
        ]
        for input_file in plan.input_files:
            command.extend(["-i", str(input_file)])
        for stream in selected_streams:
            command.extend(["-map", f"{stream.input_index}:{stream.stream_index}"])
        command.extend(["-c", "copy"])
        if len({stream.codec_type for stream in selected_streams}) > 1:
            command.append("-shortest")
        command.append(str(temp_output))
        self.logger.debug("ffmpeg finalize command: %s", command_for_log(command))

        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError:
            self.logger.warning(
                "Unable to finalize partial segment; ffmpeg not found: %s",
                self.config.ffmpeg_path,
            )
            return False
        except OSError:
            self.logger.exception("Unable to start ffmpeg for partial segment finalization")
            return False

        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=FINALIZE_MUX_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()
            temp_output.unlink(missing_ok=True)
            self.logger.warning("ffmpeg timed out while finalizing partial segment")
            return False

        if process.returncode != 0:
            temp_output.unlink(missing_ok=True)
            message = (stderr or stdout).decode("utf-8", "replace").strip()
            self.logger.warning(
                "ffmpeg failed while finalizing partial segment: %s",
                message,
            )
            return False

        try:
            validation = validate_finalize_output(
                temp_output,
                selected_streams,
                ffprobe_path,
            )
        except VideoProbeError as exc:
            temp_output.unlink(missing_ok=True)
            self.logger.warning(
                "Finalized output failed validation; preserving all source files: %s",
                exc,
            )
            return False

        self.logger.info(
            "Validated finalized output duration=%.3fs video_streams=%s "
            "audio_streams=%s output=%s",
            validation.duration,
            validation.video_streams,
            validation.audio_streams,
            plan.output_file,
        )
        temp_output.rename(plan.output_file)
        return True

    async def _schedule_spawn_retry(self, stream: LiveStream) -> None:
        failures = self._spawn_failures.get(stream.video_id, 0)
        delay = self.config.retry_backoff_seconds[
            min(failures, len(self.config.retry_backoff_seconds) - 1)
        ]
        self._spawn_failures[stream.video_id] = failures + 1
        self.logger.info("Retrying start for %s in %ss", stream.video_id, delay)
        await self.sleep(delay)
        try:
            latest = await self.probe_video(post_exit_probe_target(stream))
        except TerminalVideoUnavailableError as exc:
            self.logger.info(
                "Retry probe found %s terminally unavailable: %s",
                stream.video_id,
                exc,
            )
            self.state.mark_ended(stream.video_id)
            self._youtube_fragment_progress_at.pop(stream.video_id, None)
            return
        except Exception as exc:
            self.logger.warning("Retry probe failed for %s: %s", stream.video_id, exc)
            return
        if latest.is_live:
            await self.start_stream(latest)

    async def stop_all(self) -> None:
        self._stopping = True
        for active in list(self.active.values()):
            if active.reconnect_task:
                active.reconnect_task.cancel()
            if active.mixed_segment_task:
                active.mixed_segment_task.cancel()
            if active.stale_live_task:
                active.stale_live_task.cancel()
            if active.chat_process and active.chat_process.returncode is None:
                self.logger.info(
                    "Terminating live chat recorder for %s",
                    active.stream.video_id,
                )
                active.chat_process.terminate()
            if active.process.returncode is None:
                self.logger.info("Terminating yt-dlp for %s", active.stream.video_id)
                active.process.terminate()

        for active in list(self.active.values()):
            try:
                await asyncio.wait_for(active.process.wait(), timeout=30)
            except asyncio.TimeoutError:
                self.logger.warning("Killing yt-dlp for %s", active.stream.video_id)
                active.process.kill()
                await active.process.wait()
            if active.output_task:
                await self._finish_output_task(active.output_task)
            await self._stop_powerchat_listener(active)
            await self._stop_chat_recorder(active)

        for task in list(self._post_exit_tasks):
            task.cancel()
        if self._post_exit_tasks:
            await asyncio.gather(*self._post_exit_tasks, return_exceptions=True)




def format_byte_count(value: int) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    size = float(value)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{value} B"

def auto_job_id(kind: str, video_id: str, item: str) -> str:
    return f"auto-{kind}:{video_id}:{item}"


def youtube_video_codec(value: object) -> str:
    codec = str(value or "").strip().casefold()
    if codec.startswith(("vp9", "vp09")):
        return "vp9"
    if codec.startswith(("av1", "av01")):
        return "av1"
    if codec.startswith(("h264", "avc1", "avc3")):
        return "h264"
    return codec.partition(".")[0]


def choose_youtube_video_format(
    stream: LiveStream,
    preferred_codec: str = "vp9",
    *,
    preferred_format_id: str = "",
    preferred_audio_format_id: str = "",
) -> YouTubeVideoFormatChoice | None:
    if stream.platform.casefold() != "youtube":
        return None
    formats = stream.raw.get("formats")
    if not isinstance(formats, list):
        return None

    audio_format_id = choose_youtube_audio_format_id(
        formats,
        preferred_audio_format_id=preferred_audio_format_id,
    )
    candidates: list[tuple[tuple[float, ...], YouTubeVideoFormatChoice]] = []
    preferred = preferred_codec.strip().casefold()
    for position, raw_format in enumerate(formats):
        if not isinstance(raw_format, Mapping) or raw_format.get("has_drm"):
            continue
        format_id = str(raw_format.get("format_id") or "").strip()
        if not YOUTUBE_FORMAT_ID_RE.fullmatch(format_id):
            continue
        raw_codec = str(raw_format.get("vcodec") or "").strip()
        codec = youtube_video_codec(raw_codec)
        if not codec or codec == "none":
            continue

        acodec = str(raw_format.get("acodec") or "").strip().casefold()
        audio_ext = str(raw_format.get("audio_ext") or "").strip().casefold()
        has_audio = (
            acodec not in {"", "none"}
            or audio_ext not in {"", "none"}
        )
        selector = (
            format_id
            if has_audio
            else f"{format_id}+{audio_format_id or 'bestaudio'}"
        )
        height = format_number(raw_format.get("height"), integer=True)
        fps = format_number(raw_format.get("fps"))
        choice = YouTubeVideoFormatChoice(
            format_id=format_id,
            codec=codec,
            selector=selector,
            audio_format_id="" if has_audio else audio_format_id,
            height=int(height),
            fps=fps,
        )
        preferred_id_match = float(
            bool(preferred_format_id)
            and format_id == preferred_format_id
            and (preferred == "auto" or codec == preferred)
        )
        preferred_match = float(preferred != "auto" and codec == preferred)
        rank = (
            preferred_id_match,
            preferred_match,
            height,
            format_number(raw_format.get("width"), integer=True),
            fps,
            format_number(raw_format.get("quality")),
            format_number(raw_format.get("tbr")),
            format_number(raw_format.get("vbr")),
            float(not has_audio),
            format_number(
                raw_format.get("filesize")
                or raw_format.get("filesize_approx")
            ),
            float(position),
        )
        candidates.append((rank, choice))

    if not candidates:
        return None
    return max(candidates, key=lambda candidate: candidate[0])[1]


def choose_youtube_audio_format_id(
    formats: list[object],
    *,
    preferred_audio_format_id: str = "",
) -> str:
    candidates: list[tuple[tuple[float, ...], str]] = []
    for position, raw_format in enumerate(formats):
        if not isinstance(raw_format, Mapping) or raw_format.get("has_drm"):
            continue
        format_id = str(raw_format.get("format_id") or "").strip()
        if not YOUTUBE_FORMAT_ID_RE.fullmatch(format_id):
            continue
        vcodec = str(raw_format.get("vcodec") or "").strip().casefold()
        acodec = str(raw_format.get("acodec") or "").strip().casefold()
        if vcodec not in {"", "none"} or acodec in {"", "none"}:
            continue
        rank = (
            float(
                bool(preferred_audio_format_id)
                and format_id == preferred_audio_format_id
            ),
            format_number(raw_format.get("quality")),
            format_number(raw_format.get("abr")),
            format_number(raw_format.get("tbr")),
            format_number(raw_format.get("asr")),
            format_number(raw_format.get("audio_channels")),
            format_number(
                raw_format.get("filesize")
                or raw_format.get("filesize_approx")
            ),
            float(position),
        )
        candidates.append((rank, format_id))
    if not candidates:
        return ""
    return max(candidates, key=lambda candidate: candidate[0])[1]


def youtube_audio_format_id(selector: str) -> str:
    exact_selector = selector.strip().partition("/")[0]
    _video_format_id, separator, audio_format_id = exact_selector.partition("+")
    if not separator or audio_format_id == "bestaudio":
        return ""
    return audio_format_id


def format_number(value: object, *, integer: bool = False) -> float:
    if isinstance(value, bool) or value is None:
        return 0.0
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    if number != number or number < 0:
        return 0.0
    return float(int(number)) if integer else number


def build_download_command(
    config: BotConfig,
    stream: LiveStream,
    segment_index: int,
    *,
    youtube_video_format_selector: str = "",
) -> list[str]:
    output_template = output_template_for(config, stream, segment_index)
    command = [config.yt_dlp_path, *config.extra_yt_dlp_args]
    record_chat = should_record_chat_for_stream(config, stream)
    if not yt_dlp_args_include_format(config.extra_yt_dlp_args):
        if stream.platform.casefold() == "youtube" and youtube_video_format_selector:
            command.extend(["--format", youtube_video_format_selector])
        elif record_chat:
            command.extend(["--format", DEFAULT_MEDIA_FORMAT])
    if config.live_from_start and stream.platform == "youtube":
        command.append("--live-from-start")
    if config.keep_fragments_for_resume:
        command.append("--keep-fragments")
    command.extend(
        [
            "--continue",
            "--part",
            "--progress",
            "--newline",
            "--progress-delta",
            "5",
            "--no-playlist",
            "-o",
            str(output_template),
            stream.url,
        ]
    )
    return command


def build_chat_download_command(
    config: BotConfig,
    stream: LiveStream,
    segment_index: int,
) -> list[str]:
    if stream.platform != "youtube":
        raise ValueError("live chat recording is currently YouTube-only")
    output_template = output_template_for(config, stream, segment_index)
    command = [config.yt_dlp_path, *config.extra_yt_dlp_args]
    if config.live_from_start and stream.platform == "youtube":
        command.append("--live-from-start")
    command.extend(
        [
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
            stream.url,
        ]
    )
    return command


def should_record_chat(config: BotConfig) -> bool:
    return config.record_live_chat or config.render_live_chat_video


def should_record_chat_for_stream(config: BotConfig, stream: LiveStream) -> bool:
    processing_config = post_stream_config_for_channel(config, stream.channel)
    return stream.platform == "youtube" and should_record_chat(processing_config)


def chat_render_timeout_seconds(config: BotConfig) -> float | None:
    if config.chat_render_timeout_seconds <= 0:
        return None
    return float(config.chat_render_timeout_seconds)


def yt_dlp_args_include_format(args: list[str]) -> bool:
    return any(arg.partition("=")[0] in FORMAT_OPTIONS for arg in args)


def output_template_for(
    config: BotConfig,
    stream_or_video_id: LiveStream | str,
    segment_index: int,
    channel: str = "",
) -> Path:
    if isinstance(stream_or_video_id, LiveStream):
        video_id = stream_or_video_id.video_id
        channel = stream_or_video_id.channel
    else:
        video_id = stream_or_video_id
    directory = segment_directory(config, video_id, channel)
    return directory / f"{segment_file_stem(segment_index)}.%(ext)s"


def choose_restart_segment(
    config: BotConfig,
    video_id: str,
    segment_index: int,
    channel: str = "",
) -> int:
    if segment_has_part_files(config, video_id, segment_index, channel):
        return segment_index
    if segment_has_final_files(config, video_id, segment_index, channel):
        return segment_index + 1
    return segment_index


def youtube_format_segment_index(
    config: BotConfig,
    stream: LiveStream,
    segment_index: int,
    *,
    previous_format_id: str,
    previous_codec: str,
    previous_selector: str,
    selected_format_id: str,
    selected_codec: str,
    selected_selector: str,
) -> int:
    if (
        not previous_format_id
        or (
            previous_format_id == selected_format_id
            and previous_codec == selected_codec
            and previous_selector == selected_selector
        )
        or not segment_media_input_files(
            config,
            stream.video_id,
            segment_index,
            stream.channel,
        )
    ):
        return segment_index
    return segment_index + 1


def segment_has_part_files(
    config: BotConfig,
    video_id: str,
    segment_index: int,
    channel: str = "",
) -> bool:
    return bool(segment_part_files(config, video_id, segment_index, channel))


def segment_has_mixed_format_files(
    config: BotConfig,
    video_id: str,
    segment_index: int,
    channel: str = "",
) -> bool:
    return bool(
        segment_part_files(config, video_id, segment_index, channel)
        and segment_final_format_files(config, video_id, segment_index, channel)
    )


def segment_has_final_files(
    config: BotConfig,
    video_id: str,
    segment_index: int,
    channel: str = "",
) -> bool:
    directory = segment_directory(config, video_id, channel)
    for path in directory.glob(f"{segment_file_stem(segment_index)}*"):
        if (
            path.is_file()
            and not is_yt_dlp_temporary_file(path.name)
            and not is_live_chat_file(path.name)
            and not is_chat_timing_file(path.name)
        ):
            return True
    return False


def restore_mixed_segment_for_resume(
    config: BotConfig,
    video_id: str,
    segment_index: int,
    channel: str = "",
) -> bool:
    if not config.keep_fragments_for_resume:
        return False

    final_files = segment_final_format_files(config, video_id, segment_index, channel)
    restore_plan: list[tuple[Path, Path, int]] = []
    for final_file in final_files:
        part_file = final_file.with_name(f"{final_file.name}.part")
        if part_file.exists():
            return False

        fragment_index = latest_kept_fragment_index(part_file)
        if fragment_index is None:
            return False

        restore_plan.append((final_file, part_file, fragment_index))

    restored_files: list[tuple[Path, Path]] = []
    written_ytdl_files: list[Path] = []
    try:
        for final_file, part_file, fragment_index in restore_plan:
            final_file.rename(part_file)
            restored_files.append((final_file, part_file))

            ytdl_file = ytdl_state_file_for(final_file)
            write_ytdl_fragment_state(ytdl_file, fragment_index)
            written_ytdl_files.append(ytdl_file)
    except OSError:
        for ytdl_file in written_ytdl_files:
            ytdl_file.unlink(missing_ok=True)
        for final_file, part_file in reversed(restored_files):
            if part_file.exists() and not final_file.exists():
                part_file.rename(final_file)
        raise

    return bool(restored_files)


def is_yt_dlp_temporary_file(name: str) -> bool:
    return name.endswith(".part") or name.endswith(".ytdl") or ".part-Frag" in name


def is_live_chat_file(name: str) -> bool:
    return name.endswith(".live_chat.json")


def is_live_chat_related_file(name: str) -> bool:
    return ".live_chat.json" in name


def segment_timing_file(
    config: BotConfig,
    video_id: str,
    segment_index: int,
    channel: str = "",
) -> Path:
    directory = segment_directory(config, video_id, channel)
    return directory / f"{segment_file_stem(segment_index)}{CHAT_TIMING_SUFFIX}"


def probe_finalize_media_streams(
    input_files: list[Path],
    ffprobe_path: str = "ffprobe",
) -> list[FinalizeMediaStream]:
    media_streams: list[FinalizeMediaStream] = []
    for input_index, path in enumerate(input_files):
        command = [
            ffprobe_path,
            "-v",
            "error",
            "-show_entries",
            (
                "format=duration:"
                "stream=index,codec_type,duration,duration_ts,time_base:"
                "stream_tags=DURATION"
            ),
            "-of",
            "json",
            str(path),
        ]
        try:
            completed = subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True,
            )
            payload = json.loads(completed.stdout)
            raw_streams = payload.get("streams")
            raw_format = payload.get("format")
            if not isinstance(raw_streams, list):
                raise ValueError("ffprobe did not return streams")
            format_duration = media_duration_value(
                raw_format.get("duration")
                if isinstance(raw_format, Mapping)
                else None
            )
            path_size = path.stat().st_size
        except (
            OSError,
            subprocess.SubprocessError,
            json.JSONDecodeError,
            ValueError,
            TypeError,
        ) as exc:
            raise VideoProbeError(f"Unable to inspect finalization input {path}") from exc

        found_media = False
        for raw_stream in raw_streams:
            if not isinstance(raw_stream, Mapping):
                continue
            codec_type = str(raw_stream.get("codec_type") or "").casefold()
            if codec_type not in {"audio", "video"}:
                continue
            found_media = True
            try:
                stream_index = int(raw_stream.get("index"))
            except (TypeError, ValueError) as exc:
                raise VideoProbeError(
                    f"Invalid stream index while inspecting {path}"
                ) from exc
            duration = finalize_stream_duration(raw_stream, format_duration)
            if duration <= 0:
                duration = probe_packet_duration(path, stream_index, ffprobe_path)
            if duration <= 0:
                raise VideoProbeError(
                    f"Unable to determine {codec_type} duration for {path}"
                )
            media_streams.append(
                FinalizeMediaStream(
                    path=path,
                    input_index=input_index,
                    stream_index=stream_index,
                    codec_type=codec_type,
                    duration=duration,
                    size=path_size,
                    partial=path.name.endswith(".part"),
                )
            )
        if not found_media:
            raise VideoProbeError(f"No audio or video streams found in {path}")

    return media_streams


def probe_packet_duration(
    path: Path,
    stream_index: int,
    ffprobe_path: str = "ffprobe",
) -> float:
    command = [
        ffprobe_path,
        "-v",
        "error",
        "-select_streams",
        str(stream_index),
        "-show_entries",
        "packet=pts_time,duration_time",
        "-of",
        "csv=p=0",
        str(path),
    ]
    try:
        completed = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.SubprocessError):
        return 0.0

    last_end = 0.0
    for line in completed.stdout.splitlines():
        pts_text, separator, duration_text = line.strip().partition(",")
        if not separator:
            continue
        try:
            pts = float(pts_text)
            packet_duration = float(duration_text)
        except ValueError:
            continue
        if pts >= 0 and packet_duration >= 0:
            last_end = max(last_end, pts + packet_duration)
    return last_end


def finalize_stream_duration(
    raw_stream: Mapping[object, object],
    format_duration: float,
) -> float:
    duration = media_duration_value(raw_stream.get("duration"))
    if duration > 0:
        return duration

    duration_ts = media_duration_value(raw_stream.get("duration_ts"))
    time_base = rational_value(raw_stream.get("time_base"))
    if duration_ts > 0 and time_base > 0:
        return duration_ts * time_base

    tags = raw_stream.get("tags")
    if isinstance(tags, Mapping):
        duration = media_duration_value(tags.get("DURATION"))
        if duration > 0:
            return duration
    return format_duration


def media_duration_value(value: object) -> float:
    if isinstance(value, bool) or value is None:
        return 0.0
    text = str(value).strip()
    if not text or text.casefold() in {"n/a", "nan", "inf", "-inf"}:
        return 0.0
    if ":" not in text:
        try:
            duration = float(text)
        except ValueError:
            return 0.0
        return duration if duration > 0 else 0.0

    parts = text.split(":")
    if len(parts) != 3:
        return 0.0
    try:
        hours, minutes, seconds = (
            float(parts[0]),
            float(parts[1]),
            float(parts[2]),
        )
    except ValueError:
        return 0.0
    duration = hours * 3600 + minutes * 60 + seconds
    return duration if duration > 0 else 0.0


def rational_value(value: object) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    numerator_text, separator, denominator_text = text.partition("/")
    try:
        numerator = float(numerator_text)
        denominator = float(denominator_text) if separator else 1.0
    except ValueError:
        return 0.0
    if denominator == 0:
        return 0.0
    result = numerator / denominator
    return result if result > 0 else 0.0


def select_finalize_media_streams(
    media_streams: list[FinalizeMediaStream],
) -> list[FinalizeMediaStream]:
    selected: list[FinalizeMediaStream] = []
    for codec_type in ("video", "audio"):
        candidates = [
            stream
            for stream in media_streams
            if stream.codec_type == codec_type
        ]
        if not candidates:
            continue
        selected.append(
            max(
                candidates,
                key=lambda stream: (
                    stream.duration,
                    stream.partial,
                    stream.size,
                    -stream.input_index,
                    -stream.stream_index,
                ),
            )
        )
    if not selected:
        raise VideoProbeError("No usable audio or video streams were available")
    return selected


def validate_finalize_output(
    output_file: Path,
    selected_streams: list[FinalizeMediaStream],
    ffprobe_path: str = "ffprobe",
) -> FinalizeOutputValidation:
    output_streams = probe_finalize_media_streams([output_file], ffprobe_path)
    expected_types = {stream.codec_type for stream in selected_streams}
    audio_streams = sum(
        stream.codec_type == "audio"
        for stream in output_streams
    )
    video_streams = sum(
        stream.codec_type == "video"
        for stream in output_streams
    )
    actual_counts = {
        "audio": audio_streams,
        "video": video_streams,
    }
    if any(actual_counts[codec_type] != 1 for codec_type in expected_types):
        raise VideoProbeError(
            "Finalized output did not contain exactly one selected stream "
            f"for each media type: {actual_counts}"
        )
    if any(
        actual_counts[codec_type]
        for codec_type in {"audio", "video"} - expected_types
    ):
        raise VideoProbeError(
            f"Finalized output contained unexpected media streams: {actual_counts}"
        )

    expected_duration = min(stream.duration for stream in selected_streams)
    output_durations = [
        stream.duration
        for stream in output_streams
        if stream.codec_type in expected_types
    ]
    actual_duration = min(output_durations)
    tolerance = min(
        FINALIZE_DURATION_TOLERANCE_SECONDS,
        max(0.25, expected_duration * 0.01),
    )
    if actual_duration + tolerance < expected_duration:
        raise VideoProbeError(
            "Finalized output was shorter than the selected recoverable media: "
            f"expected={expected_duration:.3f}s actual={actual_duration:.3f}s "
            f"tolerance={tolerance:.3f}s"
        )
    return FinalizeOutputValidation(
        duration=actual_duration,
        audio_streams=audio_streams,
        video_streams=video_streams,
    )


def prepare_finalize_plan(
    config: BotConfig,
    video_id: str,
    segment_index: int,
    channel: str = "",
) -> FinalizePlan | None:
    input_files = segment_media_input_files(config, video_id, segment_index, channel)
    if not input_files:
        return None

    output_file = finalized_output_file(config, video_id, segment_index, input_files, channel)
    cleanup = [
        *segment_ytdl_files(config, video_id, segment_index, channel),
        *segment_fragment_files(config, video_id, segment_index, channel),
    ]

    return FinalizePlan(
        output_file=output_file,
        input_files=input_files,
        cleanup_files=cleanup,
        mixed_inputs=segment_has_mixed_format_files(
            config,
            video_id,
            segment_index,
            channel,
        ),
    )


def finalized_output_file(
    config: BotConfig,
    video_id: str,
    segment_index: int,
    input_files: list[Path],
    channel: str = "",
) -> Path:
    directory = segment_directory(config, video_id, channel)
    segment_name = segment_file_stem(segment_index)
    suffixes = {
        normalize_part_file(path).suffix.lower()
        for path in input_files
        if normalize_part_file(path).suffix
    }
    suffix = next(iter(suffixes)) if len(suffixes) == 1 else ".mkv"
    return directory / f"{segment_name}{suffix}"


def segment_part_files(
    config: BotConfig,
    video_id: str,
    segment_index: int,
    channel: str = "",
) -> list[Path]:
    directory = segment_directory(config, video_id, channel)
    return sorted(
        path
        for path in directory.glob(f"{segment_file_stem(segment_index)}*.part")
        if path.is_file()
        and ".part-Frag" not in path.name
        and not is_live_chat_related_file(path.name)
    )


def segment_final_format_files(
    config: BotConfig,
    video_id: str,
    segment_index: int,
    channel: str = "",
) -> list[Path]:
    directory = segment_directory(config, video_id, channel)
    segment_name = segment_file_stem(segment_index)
    return sorted(
        path
        for path in directory.glob(f"{segment_name}.*")
        if path.is_file()
        and not is_yt_dlp_temporary_file(path.name)
        and not is_segment_recovery_file(path.name)
        and not is_live_chat_file(path.name)
        and not is_chat_timing_file(path.name)
        and not is_powerchat_event_file(path.name)
        and path.stem != segment_name
    )


def segment_media_input_files(
    config: BotConfig,
    video_id: str,
    segment_index: int,
    channel: str = "",
) -> list[Path]:
    return sorted(
        [
            *segment_final_format_files(config, video_id, segment_index, channel),
            *segment_part_files(config, video_id, segment_index, channel),
        ]
    )


def recovered_segment_output_file(
    config: BotConfig,
    video_id: str,
    segment_index: int,
    channel: str = "",
) -> Path:
    directory = segment_directory(config, video_id, channel)
    return directory / f"{segment_file_stem(segment_index)}-recovered.mkv"


def is_segment_recovery_file(name: str) -> bool:
    return bool(
        re.fullmatch(
            r"segment-\d{3}-recovered(?:\.recovering)?\.mkv",
            name,
            flags=re.IGNORECASE,
        )
    )


def recover_segment_from_fragments(
    config: BotConfig,
    video_id: str,
    segment_index: int,
    channel: str = "",
    *,
    overwrite: bool = False,
    progress_callback: Callable[[str, float], None] | None = None,
    logger: logging.Logger = LOGGER,
) -> SegmentRecoveryResult:
    def progress(phase: str, value: float) -> None:
        if progress_callback is not None:
            progress_callback(phase, value)

    input_files = segment_media_input_files(
        config,
        video_id,
        segment_index,
        channel,
    )
    if not input_files:
        raise VideoProbeError("No recoverable media tracks were found")

    output_file = recovered_segment_output_file(
        config,
        video_id,
        segment_index,
        channel,
    )
    if output_file.exists() and not overwrite:
        raise FileExistsError(f"Recovered video already exists: {output_file.name}")

    temporary_output = output_file.with_name(
        f"{output_file.stem}.recovering{output_file.suffix}"
    )
    temporary_output.unlink(missing_ok=True)
    ffprobe_path = ffprobe_path_for(config.ffmpeg_path)

    try:
        progress("Inspecting recoverable tracks", 0.15)
        media_streams = probe_finalize_media_streams(input_files, ffprobe_path)
        selected_streams = select_finalize_media_streams(media_streams)
        selected_types = {stream.codec_type for stream in selected_streams}
        if selected_types != {"audio", "video"}:
            raise VideoProbeError(
                "Recovery requires both a readable video track and audio track"
            )

        command = [
            config.ffmpeg_path,
            "-hide_banner",
            "-loglevel",
            "warning",
            "-y",
        ]
        for input_file in input_files:
            command.extend(["-i", str(input_file)])
        for stream in selected_streams:
            command.extend(["-map", f"{stream.input_index}:{stream.stream_index}"])
        command.extend(["-c", "copy", "-shortest", str(temporary_output)])

        progress("Remuxing recovered video", 0.45)
        logger.debug("ffmpeg recovery command: %s", command_for_log(command))
        try:
            completed = subprocess.run(
                command,
                capture_output=True,
                timeout=FINALIZE_MUX_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired as exc:
            raise VideoProbeError("Video recovery timed out") from exc
        except OSError as exc:
            raise VideoProbeError("Unable to start ffmpeg for video recovery") from exc
        if completed.returncode != 0:
            message = (completed.stderr or completed.stdout).decode(
                "utf-8",
                "replace",
            ).strip()
            raise VideoProbeError(
                f"ffmpeg failed while recovering the video: {message or 'unknown error'}"
            )

        progress("Validating recovered video", 0.85)
        validation = validate_finalize_output(
            temporary_output,
            selected_streams,
            ffprobe_path,
        )
        temporary_output.replace(output_file)
        progress("Recovery complete", 1.0)
        return SegmentRecoveryResult(
            output_file=output_file,
            duration=validation.duration,
            size=output_file.stat().st_size,
        )
    except Exception:
        temporary_output.unlink(missing_ok=True)
        raise


def segment_ytdl_files(
    config: BotConfig,
    video_id: str,
    segment_index: int,
    channel: str = "",
) -> list[Path]:
    directory = segment_directory(config, video_id, channel)
    return sorted(directory.glob(f"{segment_file_stem(segment_index)}*.ytdl"))


def segment_fragment_files(
    config: BotConfig,
    video_id: str,
    segment_index: int,
    channel: str = "",
) -> list[Path]:
    directory = segment_directory(config, video_id, channel)
    return sorted(
        path
        for path in directory.glob(f"{segment_file_stem(segment_index)}*.part-Frag*")
        if not is_live_chat_related_file(path.name)
    )


def latest_kept_fragment_index(part_file: Path) -> int | None:
    fragment_indexes: list[int] = []
    for fragment_file in part_file.parent.glob(f"{part_file.name}-Frag*"):
        match = KEPT_FRAGMENT_RE.search(fragment_file.name)
        if match and fragment_file.is_file():
            fragment_indexes.append(int(match.group("fragment")))
    if not fragment_indexes:
        return None
    return max(fragment_indexes)


def ytdl_state_file_for(final_file: Path) -> Path:
    return final_file.with_name(f"{final_file.name}.ytdl")


def write_ytdl_fragment_state(path: Path, fragment_index: int) -> None:
    path.write_text(
        json.dumps(
            {
                "downloader": {
                    "current_fragment": {
                        "index": fragment_index,
                    },
                },
            }
        ),
        encoding="utf-8",
    )


def rename_finalized_segment_file(
    config: BotConfig,
    stream: LiveStream,
    segment_index: int,
    logger: logging.Logger,
) -> Path | None:
    source = finalized_segment_file(config, stream.video_id, segment_index, stream.channel)
    if source is None:
        return None

    target = named_finalized_output_file(config, stream, segment_index, source.suffix)
    if source == target:
        return target
    if target.exists():
        logger.warning(
            "Final named output already exists; leaving %s in place",
            source,
        )
        return source

    try:
        source.rename(target)
    except OSError:
        logger.warning("Unable to rename finalized segment %s to %s", source, target)
        return source
    return target


def rename_segment_chat_file(
    config: BotConfig,
    stream: LiveStream,
    segment_index: int,
    logger: logging.Logger,
) -> Path | None:
    source = finalized_segment_chat_file(
        config,
        stream.video_id,
        segment_index,
        stream.channel,
        logger,
    )
    if source is None:
        return None

    target = named_segment_chat_file(config, stream, segment_index)
    if source == target:
        return target
    if target.exists():
        logger.warning(
            "Final named chat output already exists; leaving %s in place",
            source,
        )
        return source

    try:
        source.rename(target)
    except OSError:
        logger.warning("Unable to rename live chat file %s to %s", source, target)
        return source
    return target


def rename_segment_timing_file(
    config: BotConfig,
    stream: LiveStream,
    segment_index: int,
    logger: logging.Logger,
) -> Path | None:
    source = segment_timing_file(config, stream.video_id, segment_index, stream.channel)
    if not source.is_file():
        return None

    target = named_segment_timing_file(config, stream, segment_index)
    if source == target:
        return target
    if target.exists():
        logger.warning(
            "Final chat timing sidecar already exists; leaving %s in place",
            source,
        )
        return source

    try:
        source.rename(target)
    except OSError:
        logger.warning("Unable to rename chat timing sidecar %s to %s", source, target)
        return source
    return target


def finalized_segment_chat_file(
    config: BotConfig,
    video_id: str,
    segment_index: int,
    channel: str,
    logger: logging.Logger,
) -> Path | None:
    final_chat = segment_chat_file(config, video_id, segment_index, channel)
    if final_chat is not None:
        cleanup_files(
            segment_chat_fragment_files(config, video_id, segment_index, channel),
            logger,
        )
        return final_chat

    part_file = segment_chat_part_file(config, video_id, segment_index, channel)
    if part_file is None:
        return None

    target = part_file.with_name(part_file.name.removesuffix(".part"))
    if target.exists():
        logger.warning(
            "Final live chat output already exists; leaving %s in place",
            part_file,
        )
        return target

    try:
        part_file.rename(target)
    except OSError:
        logger.warning("Unable to finalize live chat file %s to %s", part_file, target)
        return part_file

    cleanup_files(
        segment_chat_fragment_files(config, video_id, segment_index, channel),
        logger,
    )
    return target


def finalized_segment_file(
    config: BotConfig,
    video_id: str,
    segment_index: int,
    channel: str = "",
) -> Path | None:
    directory = segment_directory(config, video_id, channel)
    stem = segment_file_stem(segment_index)
    matches = sorted(
        path
        for path in directory.glob(f"{stem}.*")
        if path.is_file()
        and not is_yt_dlp_temporary_file(path.name)
        and not is_powerchat_event_file(path.name)
        and path.stem == stem
    )
    return next(iter(matches), None)


def segment_chat_file(
    config: BotConfig,
    video_id: str,
    segment_index: int,
    channel: str = "",
) -> Path | None:
    directory = segment_directory(config, video_id, channel)
    stem = segment_file_stem(segment_index)
    matches = sorted(
        path
        for path in directory.glob(f"{stem}.live_chat.json")
        if path.is_file()
    )
    return next(iter(matches), None)


def segment_chat_part_file(
    config: BotConfig,
    video_id: str,
    segment_index: int,
    channel: str = "",
) -> Path | None:
    directory = segment_directory(config, video_id, channel)
    stem = segment_file_stem(segment_index)
    matches = sorted(
        path
        for path in directory.glob(f"{stem}.live_chat.json.part")
        if path.is_file()
    )
    return next(iter(matches), None)


def segment_chat_fragment_files(
    config: BotConfig,
    video_id: str,
    segment_index: int,
    channel: str = "",
) -> list[Path]:
    directory = segment_directory(config, video_id, channel)
    stem = segment_file_stem(segment_index)
    return sorted(directory.glob(f"{stem}.live_chat.json.part-Frag*"))


def named_finalized_output_file(
    config: BotConfig,
    stream: LiveStream,
    segment_index: int,
    suffix: str,
) -> Path:
    directory = segment_directory(config, stream.video_id, stream.channel)
    return directory / f"{named_segment_file_stem(stream.title, stream.video_id, segment_index)}{suffix}"


def named_segment_chat_file(
    config: BotConfig,
    stream: LiveStream,
    segment_index: int,
) -> Path:
    directory = segment_directory(config, stream.video_id, stream.channel)
    return directory / (
        f"{named_segment_file_stem(stream.title, stream.video_id, segment_index)}"
        ".live_chat.json"
    )


def named_segment_timing_file(
    config: BotConfig,
    stream: LiveStream,
    segment_index: int,
) -> Path:
    return chat_timing_file_for_chat_file(
        named_segment_chat_file(config, stream, segment_index)
    )


def normalize_part_file(path: Path) -> Path:
    return path.with_name(path.name.removesuffix(".part"))


def cleanup_files(paths: list[Path], logger: logging.Logger) -> None:
    for path in paths:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            logger.warning("Unable to remove temporary file %s", path)


def command_for_log(command: list[str]) -> str:
    redacted: list[str] = []
    redact_next = False
    for arg in command:
        if redact_next:
            redacted.append("<redacted>")
            redact_next = False
            continue

        option, separator, value = arg.partition("=")
        if separator and option in SENSITIVE_COMMAND_OPTIONS:
            redacted.append(f"{option}=<redacted>")
            continue

        redacted.append(arg)
        if arg in SENSITIVE_COMMAND_OPTIONS:
            redact_next = True

    return shlex.join(redacted)


def log_process_output(
    logger: logging.Logger,
    label: str,
    stdout: bytes,
    stderr: bytes,
    *,
    failed: bool = False,
) -> None:
    output = (stderr or stdout).decode("utf-8", "replace").strip()
    if not output:
        return
    if len(output) > 4000:
        output = output[-4000:]
    if failed:
        logger.warning("%s output:\n%s", label, output)
    else:
        logger.debug("%s output:\n%s", label, output)


def discard_task_exception(task: asyncio.Task[None]) -> None:
    if task.cancelled():
        return
    try:
        task.exception()
    except asyncio.CancelledError:
        return


def segment_directory(config: BotConfig, video_id: str, channel: str = "") -> Path:
    legacy_directory = config.download_dir / safe_path_component(video_id)
    directory_component = stream_directory_component(video_id)
    group_name = download_group_name_for_channel(config, channel)
    if not group_name:
        stream_directory = config.download_dir / directory_component
        if legacy_directory.exists() and not stream_directory.exists():
            return legacy_directory
        return stream_directory

    group_directory = config.download_dir / safe_path_component(group_name)
    channel_directory = group_directory / directory_component
    legacy_channel_directory = group_directory / safe_path_component(video_id)
    if legacy_directory.exists() and not channel_directory.exists():
        return legacy_directory
    if (
        legacy_channel_directory != channel_directory
        and legacy_channel_directory.exists()
        and not channel_directory.exists()
    ):
        return legacy_channel_directory
    return channel_directory


def stream_directory_component(video_id: str) -> str:
    platform, separator, raw_id = video_id.partition(":")
    if separator and platform in {"kick", "twitch", "rumble"}:
        return safe_filename_stem(f"{platform}_{raw_id}")
    return safe_path_component(video_id)


def named_segment_file_stem(title: str, video_id: str, segment_index: int) -> str:
    title_stem = safe_filename_stem(title)
    id_label = video_id_filename_label(video_id, title_stem)
    stem = f"{title_stem} [{id_label}]"
    if segment_index > 1:
        stem = f"{stem} - part {segment_index:03d}"
    return stem


def video_id_filename_label(video_id: str, title_stem: str) -> str:
    platform, separator, raw_id = video_id.partition(":")
    if separator and platform in {"kick", "twitch", "rumble"}:
        raw_label = safe_filename_stem(raw_id)
        if raw_label == title_stem:
            return platform
        return safe_filename_stem(video_id)
    return video_id


def segment_file_stem(segment_index: int) -> str:
    return f"segment-{segment_index:03d}"


def safe_filename_stem(value: str) -> str:
    cleaned = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "_", value.strip())
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = cleaned.strip(" ._")
    if len(cleaned) > 180:
        cleaned = cleaned[:180].rstrip(" ._")
    return cleaned or "video"


def safe_path_component(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip())
    return cleaned.strip("._") or "unknown"
