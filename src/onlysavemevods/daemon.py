from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone

from .chat_render import log_nvenc_environment
from .config import (
    BotConfig,
    ensure_config_dirs,
    monitored_sources,
    post_stream_setting_enabled_anywhere,
)
from .downloader import DownloadManager
from .models import LiveStream
from .sources import SourceMonitor
from .state import StateStore, StreamRecord
from .web import StatusWebServer, cleanup_expired_stream_fragments
from .youtube import YtDlpRunner


LOGGER = logging.getLogger(__name__)
FRAGMENT_CLEANUP_INTERVAL_SECONDS = 15 * 60


class OnlySaveMeVodsDaemon:
    def __init__(self, config: BotConfig) -> None:
        self.config = config
        ensure_config_dirs(config)
        self.state = StateStore(config.db_path)
        self.sources = SourceMonitor(
            YtDlpRunner(config.yt_dlp_path),
            channel_scan_limit=config.channel_scan_limit,
            discovery_probe_concurrency=config.discovery_probe_concurrency,
            youtube_live_from_start=config.live_from_start,
        )
        self.downloads = DownloadManager(config, self.state, self.sources)
        self.web = StatusWebServer(config) if config.web_enabled else None
        self._stop_event = asyncio.Event()
        self._last_fragment_cleanup_monotonic: float | None = None

    async def run(self) -> None:
        self.state.reconcile_stale_downloads()
        requeued_post_processing = (
            self.state.requeue_incomplete_post_processing_jobs()
        )
        stale_post_exit_records = self.state.list_streams_by_status(
            ["checking_after_exit"]
        )
        stalled_youtube_records = self.state.list_streams_by_status(["stalled"])
        self.state.mark_stale_watermarks_interrupted()
        sources = monitored_sources(self.config)
        LOGGER.info(
            "ONLYSAVEmeVODS daemon started sources=%s poll_interval=%ss download_dir=%s",
            len(sources),
            self.config.poll_interval_seconds,
            self.config.download_dir,
        )
        LOGGER.debug(
            "Daemon config: channel_scan_limit=%s discovery_probe_concurrency=%s "
            "live_from_start=%s youtube_preferred_video_codec=%s "
            "keep_fragments_for_resume=%s "
            "fragment_retention_hours=%s "
            "reconnect_interval_seconds=%s "
            "youtube_stale_live_timeout_seconds=%s post_exit_check_seconds=%s "
            "render_live_chat_video=%s chat_render_use_nvenc=%s "
            "chat_render_nvenc_devices=%s transcribe_subtitles=%s "
            "processing_quiet_hours=%s processing_window=%s-%s "
            "whisperx_model=%s whisperx_diarize=%s watermark_enabled=%s "
            "watermark_strength=%s web_enabled=%s web_bind=%s:%s",
            self.config.channel_scan_limit,
            self.config.discovery_probe_concurrency,
            self.config.live_from_start,
            self.config.youtube_preferred_video_codec,
            self.config.keep_fragments_for_resume,
            self.config.fragment_retention_hours,
            self.config.reconnect_interval_seconds,
            self.config.youtube_stale_live_timeout_seconds,
            self.config.post_exit_check_seconds,
            self.config.render_live_chat_video,
            self.config.chat_render_use_nvenc,
            self.config.chat_render_nvenc_devices,
            self.config.transcribe_subtitles,
            self.config.processing_quiet_hours_enabled,
            self.config.processing_quiet_hours_start,
            self.config.processing_quiet_hours_end,
            self.config.whisperx_model,
            self.config.whisperx_diarize,
            self.config.watermark_enabled,
            self.config.watermark_strength,
            self.config.web_enabled,
            self.config.web_host,
            self.config.web_port,
        )
        if (
            post_stream_setting_enabled_anywhere(
                self.config,
                "render_live_chat_video",
            )
            or self.config.chat_render_use_nvenc
        ):
            await asyncio.to_thread(
                log_nvenc_environment,
                self.config.ffmpeg_path,
                self.config.chat_render_use_nvenc,
            )
        if self.web:
            try:
                self.web.start()
            except OSError as exc:
                LOGGER.warning(
                    "Unable to start administration dashboard on %s:%s: %s",
                    self.config.web_host,
                    self.config.web_port,
                    exc,
                )
        else:
            LOGGER.info("Administration dashboard disabled by config")
        if not monitored_sources(self.config):
            LOGGER.warning("No sources configured; edit config.toml to add channels or streamers")
        self.resume_stale_post_exit_checks(stale_post_exit_records)
        self.resume_stalled_youtube_checks(stalled_youtube_records)
        if requeued_post_processing:
            LOGGER.warning(
                "Requeued %s interrupted post-processing job(s) after service restart",
                requeued_post_processing,
            )
        self.downloads.resume_pending_post_processing_jobs()

        try:
            while not self._stop_event.is_set():
                await self.poll_once()
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self.config.poll_interval_seconds,
                    )
                except asyncio.TimeoutError:
                    pass
        finally:
            if self.web:
                self.web.stop()
            await self.downloads.stop_all()
            self.state.close()
            LOGGER.info("ONLYSAVEmeVODS daemon stopped")

    def stop(self) -> None:
        self._stop_event.set()

    def resume_stale_post_exit_checks(self, records: list[StreamRecord]) -> None:
        if not records:
            return
        LOGGER.warning(
            "Resuming %s interrupted post-exit check(s) after service restart",
            len(records),
        )
        for record in records:
            self.downloads.resume_post_exit_check(
                stream_from_record(record),
                record.segment_index,
                elapsed_since_exit_seconds=seconds_since_iso(record.last_exit_at),
            )

    def resume_stalled_youtube_checks(self, records: list[StreamRecord]) -> None:
        if not records:
            return
        LOGGER.warning(
            "Resuming monitoring for %s stalled YouTube stream(s) after service restart",
            len(records),
        )
        for record in records:
            self.downloads.resume_stalled_youtube_check(
                stream_from_record(record),
                record.segment_index,
            )

    async def poll_once(self) -> None:
        self.downloads.resume_pending_post_processing_jobs()
        await self.cleanup_expired_fragments()
        for source in monitored_sources(self.config):
            LOGGER.info("Checking source %s", source)
            try:
                streams = await asyncio.to_thread(
                    self.sources.discover_live_streams,
                    source,
                )
            except Exception as exc:
                LOGGER.warning("Failed to check source %s: %s", source, exc)
                continue

            if not streams:
                LOGGER.info("No live streams detected for %s", source)
                continue

            for stream in streams:
                LOGGER.info(
                    "Live stream found for %s platform=%s video_id=%s title=%r",
                    source,
                    stream.platform,
                    stream.video_id,
                    stream.title,
                )
                try:
                    await self._start_stream(stream)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001 - isolate one stream startup.
                    LOGGER.exception(
                        "Unable to start stream video_id=%s source=%s; "
                        "continuing source checks",
                        stream.video_id,
                        source,
                    )
                    try:
                        self.state.add_stream_event(
                            stream.video_id,
                            "Unable to start recording: "
                            f"{str(exc) or exc.__class__.__name__}",
                            level="error",
                        )
                    except Exception:  # noqa: BLE001 - failure reporting is best effort.
                        LOGGER.debug(
                            "Unable to record stream startup failure for %s",
                            stream.video_id,
                            exc_info=True,
                        )

    async def cleanup_expired_fragments(self) -> None:
        if self.config.fragment_retention_hours <= 0:
            return
        current_monotonic = time.monotonic()
        if (
            self._last_fragment_cleanup_monotonic is not None
            and current_monotonic - self._last_fragment_cleanup_monotonic
            < FRAGMENT_CLEANUP_INTERVAL_SECONDS
        ):
            return
        self._last_fragment_cleanup_monotonic = current_monotonic
        try:
            streams, files, bytes_removed = await asyncio.to_thread(
                cleanup_expired_stream_fragments,
                self.config,
            )
        except Exception:
            LOGGER.exception("Automatic fragment cleanup failed")
            return
        if files:
            LOGGER.info(
                "Automatic fragment cleanup removed streams=%s files=%s bytes=%s",
                streams,
                files,
                bytes_removed,
            )

    async def _start_stream(self, stream: LiveStream) -> None:
        self.state.upsert_detected(stream)
        await self.downloads.start_stream(stream)


def stream_from_record(record: StreamRecord) -> LiveStream:
    return LiveStream(
        video_id=record.video_id,
        url=record.url,
        title=record.title,
        channel=record.channel,
        is_live=False,
        platform=record.platform,
        source=record.source,
    )


def seconds_since_iso(value: str | None) -> float:
    if not value:
        return 0.0
    try:
        timestamp = datetime.fromisoformat(value)
    except ValueError:
        return 0.0
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return max(0.0, (datetime.now(timezone.utc) - timestamp).total_seconds())
