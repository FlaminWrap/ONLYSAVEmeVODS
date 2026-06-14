from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Awaitable, Callable, Protocol
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
    VideoProbeError,
    build_render_chat_file_process_command,
    build_chat_panel_merge_command,
    build_chat_video_command,
    chat_layout_for_video,
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
from .config import BotConfig, download_group_name_for_channel
from .models import LiveStream
from .state import StateStore
from .transcription import transcribe_media_file, transcription_config_for_channel
from .youtube import TerminalVideoUnavailableError


LOGGER = logging.getLogger(__name__)
RECONNECT_STOP_TIMEOUT_SECONDS = 20
FINALIZE_MUX_TIMEOUT_SECONDS = 60 * 60
CATCHUP_FRAGMENT_MARGIN = 2
MIXED_SEGMENT_WATCH_SECONDS = 10
DEFAULT_MEDIA_FORMAT = "bestvideo*+bestaudio/best"
FORMAT_OPTIONS = {"-f", "--format"}
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


SleepFunc = Callable[[float], Awaitable[None]]
ProbeVideoFunc = Callable[[str], Awaitable[LiveStream]]


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
    chat_process: asyncio.subprocess.Process | None = None
    chat_task: asyncio.Task[None] | None = None
    chat_output_task: asyncio.Task[None] | None = None


@dataclass(slots=True)
class FinalizePlan:
    output_file: Path
    input_files: list[Path]
    cleanup_files: list[Path]
    shortest: bool = False


@dataclass(slots=True)
class FinalizedSegmentFiles:
    segment_index: int
    channel: str
    media_file: Path | None
    chat_file: Path | None
    timing_file: Path | None


class CatchupTracker:
    def __init__(self, ready_event: asyncio.Event) -> None:
        self.ready_event = ready_event
        self.fragments: dict[str, tuple[int, int]] = {}
        self.has_prefixed_context = False

    def update(self, line: str) -> None:
        match = FRAGMENT_PROGRESS_RE.search(line)
        if not match:
            return

        context = match.group("context") or "0"
        self.has_prefixed_context = self.has_prefixed_context or context != "0"
        self.fragments[context] = (
            int(match.group("fragment")),
            int(match.group("count")),
        )
        if self.caught_up:
            self.ready_event.set()

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


class DownloadManager:
    def __init__(
        self,
        config: BotConfig,
        state: StateStore,
        probe: StreamProbe,
        *,
        sleep_func: SleepFunc = asyncio.sleep,
        probe_video_func: ProbeVideoFunc | None = None,
        logger: logging.Logger = LOGGER,
    ) -> None:
        self.config = config
        self.state = state
        self.probe = probe
        self.sleep = sleep_func
        self.probe_video = probe_video_func or self._probe_video_in_thread
        self.logger = logger
        self.active: dict[str, ActiveDownload] = {}
        self._post_exit_tasks: set[asyncio.Task[None]] = set()
        self._planned_reconnects: set[str] = set()
        self._spawn_failures: dict[str, int] = {}
        self._transcription_semaphore = asyncio.Semaphore(
            config.transcription_max_concurrent
        )
        self._stopping = False

    async def _probe_video_in_thread(self, url: str) -> LiveStream:
        return await asyncio.to_thread(self.probe.probe_video, url)

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

        output_template = output_template_for(self.config, stream, segment_index)
        output_template.parent.mkdir(parents=True, exist_ok=True)
        command = build_download_command(self.config, stream, segment_index)
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
        catchup_tracker = CatchupTracker(reconnect_ready)

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
        active = ActiveDownload(
            stream=stream,
            process=process,
            segment_index=segment_index,
            output_template=output_template,
            task=task,
            reconnect_task=reconnect_task,
            output_task=output_task,
            mixed_segment_task=mixed_segment_task,
        )
        self.active[stream.video_id] = active
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

                self.logger.warning(
                    "Detected mixed finalized/partial formats for %s segment=%03d; "
                    "reconnecting before video continues without audio",
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
            if active.output_task:
                await self._finish_output_task(active.output_task)
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
        if stream.video_id in self._planned_reconnects:
            self._planned_reconnects.discard(stream.video_id)
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

    async def handle_planned_reconnect(
        self,
        stream: LiveStream,
        segment_index: int,
    ) -> None:
        try:
            latest = await self.probe_video(stream.url)
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

    async def handle_post_exit(self, stream: LiveStream, segment_index: int) -> None:
        previous_offset = 0
        self.logger.debug(
            "Starting post-exit checks for %s segment=%03d schedule=%s",
            stream.video_id,
            segment_index,
            self.config.post_exit_check_seconds,
        )
        for offset in self.config.post_exit_check_seconds:
            if self._stopping:
                return

            delay = max(0, offset - previous_offset)
            previous_offset = offset
            if delay:
                self.logger.debug(
                    "Waiting %ss before post-exit probe for %s at +%ss",
                    delay,
                    stream.video_id,
                    offset,
                )
                await self.sleep(delay)

            try:
                self.logger.debug(
                    "Running post-exit probe for %s at +%ss",
                    stream.video_id,
                    offset,
                )
                latest = await self.probe_video(stream.url)
            except TerminalVideoUnavailableError as exc:
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
        if should_record_chat_for_stream(self.config, stream):
            await self.refresh_finalized_chat_files(stream, finalized_files)
        self.state.mark_ended(stream.video_id)
        if self.config.transcribe_subtitles:
            await self.transcribe_finalized_media(finalized_files)
        if self.config.render_live_chat_video and stream.platform == "youtube":
            await self.render_finalized_chat_videos(finalized_files)

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

    async def render_finalized_chat_videos(
        self,
        finalized_files: list[FinalizedSegmentFiles],
    ) -> None:
        for files in finalized_files:
            if files.media_file is None or files.chat_file is None:
                continue
            await self.render_live_chat_video(
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
        finalized_files: list[FinalizedSegmentFiles],
    ) -> None:
        for files in finalized_files:
            if files.media_file is None:
                continue
            async with self._transcription_semaphore:
                await transcribe_media_file(
                    transcription_config_for_channel(self.config, files.channel),
                    files.media_file,
                    logger=self.logger,
                    channel=files.channel,
                )

    async def render_live_chat_video(
        self,
        media_file: Path,
        chat_file: Path,
        segment_index: int,
    ) -> bool:
        output_file = chat_video_output_file(media_file)
        temp_output = output_file.with_name(
            f"{output_file.stem}.rendering{output_file.suffix}"
        )
        ass_file = output_file.with_name(f"{output_file.stem}.ass")
        if output_file.exists():
            self.logger.info(
                "Chat render output already exists for segment=%03d: %s",
                segment_index,
                output_file,
            )
            return True

        subprocess_result = await self.render_live_chat_video_process(
            media_file,
            chat_file,
            output_file,
            segment_index,
        )
        if subprocess_result is not None:
            return subprocess_result

        started_at = time.monotonic()
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
        if self.config.chat_render_use_nvenc:
            self.logger.info(
                "Selected NVENC device for chat render segment=%03d device=%s",
                segment_index,
                nvenc_device or "default",
            )
        try:
            entries = parse_live_chat_file(chat_file)
        except OSError:
            self.logger.exception("Unable to read live chat file %s", chat_file)
            return False

        if not entries:
            self.logger.info("No live chat messages found in %s", chat_file)
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
                await asyncio.to_thread(
                    render_chat_panel_video,
                    entries,
                    layout,
                    panel_file,
                    duration,
                    self.config.ffmpeg_path,
                    output_file.parent / ".emoji-cache",
                    self.config.chat_render_panel_workers,
                    self.config.chat_render_use_nvenc,
                    nvenc_device,
                )
                command = build_chat_panel_merge_command(
                    self.config.ffmpeg_path,
                    media_file,
                    panel_file,
                    temp_output,
                    layout,
                    use_nvenc=self.config.chat_render_use_nvenc,
                    nvenc_device=nvenc_device,
                )
                self.logger.info(
                    "Merging rendered chat panel segment=%03d media=%s panel=%s",
                    segment_index,
                    media_file,
                    panel_file,
                )
            except ChatPanelRenderError:
                panel_file.unlink(missing_ok=True)
                self.logger.exception(
                    "Unable to render image chat panel; falling back to subtitle renderer"
                )
                try:
                    write_chat_ass_file(ass_file, entries, layout)
                except OSError:
                    self.logger.exception(
                        "Unable to write live chat subtitle file %s",
                        ass_file,
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
                )
        else:
            try:
                write_chat_ass_file(ass_file, entries, layout)
            except OSError:
                self.logger.exception("Unable to write live chat subtitle file %s", ass_file)
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
            )
        self.logger.info(
            "Rendering chat video for segment=%03d as %s",
            segment_index,
            output_file,
        )
        self.logger.debug("ffmpeg chat render command: %s", command_for_log(command))

        try:
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
            return False
        except OSError:
            self.logger.exception("Unable to start ffmpeg for chat video render")
            ass_file.unlink(missing_ok=True)
            panel_file.unlink(missing_ok=True)
            return False
        except subprocess.TimeoutExpired:
            temp_output.unlink(missing_ok=True)
            ass_file.unlink(missing_ok=True)
            panel_file.unlink(missing_ok=True)
            self.logger.warning(
                "ffmpeg made no output progress while rendering chat video"
            )
            return False
        finally:
            ass_file.unlink(missing_ok=True)
            panel_file.unlink(missing_ok=True)

        if result.returncode != 0:
            temp_output.unlink(missing_ok=True)
            message = (result.stderr or result.stdout).decode("utf-8", "replace").strip()
            self.logger.warning("ffmpeg failed while rendering chat video: %s", message)
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
        return True

    async def render_live_chat_video_process(
        self,
        media_file: Path,
        chat_file: Path,
        output_file: Path,
        segment_index: int,
    ) -> bool | None:
        if self.config.config_path is None:
            return None

        command = build_render_chat_file_process_command(
            sys.executable,
            self.config.config_path,
            media_file,
            chat_file,
            output_file,
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
                    "Unable to restore mixed segment for %s segment=%03d",
                    stream.video_id,
                    segment_index,
                )

            if restored:
                self.logger.info(
                    "Prepared mixed segment for resumed live download of %s segment=%03d",
                    stream.video_id,
                    segment_index,
                )
                self.state.add_stream_event(
                    stream.video_id,
                    f"Restored mixed segment={segment_index:03d} from kept fragments",
                    segment_index=segment_index,
                )
                return segment_index

            self.logger.warning(
                "Unable to restore mixed segment for exact resume of %s segment=%03d; "
                "finalizing this segment before starting a new live-from-start segment",
                stream.video_id,
                segment_index,
            )
            if not await self.finalize_ended_segment(
                stream.video_id,
                segment_index,
                channel,
            ):
                self.logger.warning(
                    "Unable to finalize mixed segment for %s segment=%03d; "
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
            "shortest=%s",
            video_id,
            segment_index,
            plan.output_file,
            [str(path) for path in plan.input_files],
            [str(path) for path in plan.cleanup_files],
            plan.shortest,
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
        for input_index in range(len(plan.input_files)):
            command.extend(["-map", str(input_index)])
        command.extend(["-c", "copy"])
        if plan.shortest:
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
            latest = await self.probe_video(stream.url)
        except TerminalVideoUnavailableError as exc:
            self.logger.info(
                "Retry probe found %s terminally unavailable: %s",
                stream.video_id,
                exc,
            )
            self.state.mark_ended(stream.video_id)
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
            await self._stop_chat_recorder(active)

        for task in list(self._post_exit_tasks):
            task.cancel()
        if self._post_exit_tasks:
            await asyncio.gather(*self._post_exit_tasks, return_exceptions=True)


def build_download_command(
    config: BotConfig,
    stream: LiveStream,
    segment_index: int,
) -> list[str]:
    output_template = output_template_for(config, stream, segment_index)
    command = [config.yt_dlp_path, *config.extra_yt_dlp_args]
    record_chat = should_record_chat_for_stream(config, stream)
    if record_chat and not yt_dlp_args_include_format(config.extra_yt_dlp_args):
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
    return stream.platform == "youtube" and should_record_chat(config)


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
        shortest=segment_has_mixed_format_files(config, video_id, segment_index, channel),
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
        and not is_live_chat_file(path.name)
        and not is_chat_timing_file(path.name)
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
    group_name = download_group_name_for_channel(config, channel)
    if not group_name:
        return legacy_directory

    channel_directory = (
        config.download_dir
        / safe_path_component(group_name)
        / safe_path_component(video_id)
    )
    if legacy_directory.exists() and not channel_directory.exists():
        return legacy_directory
    return channel_directory


def named_segment_file_stem(title: str, video_id: str, segment_index: int) -> str:
    stem = f"{safe_filename_stem(title)} [{video_id}]"
    if segment_index > 1:
        stem = f"{stem} - part {segment_index:03d}"
    return stem


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
