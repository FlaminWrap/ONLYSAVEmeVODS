from __future__ import annotations

import asyncio
import logging

from .chat_render import log_nvenc_environment
from .config import BotConfig, ensure_config_dirs, monitored_sources
from .downloader import DownloadManager
from .models import LiveStream
from .sources import SourceMonitor
from .state import StateStore
from .web import StatusWebServer
from .youtube import YtDlpRunner


LOGGER = logging.getLogger(__name__)


class OnlySaveMeVodsDaemon:
    def __init__(self, config: BotConfig) -> None:
        self.config = config
        ensure_config_dirs(config)
        self.state = StateStore(config.db_path)
        self.sources = SourceMonitor(
            YtDlpRunner(config.yt_dlp_path),
            channel_scan_limit=config.channel_scan_limit,
            discovery_probe_concurrency=config.discovery_probe_concurrency,
        )
        self.downloads = DownloadManager(config, self.state, self.sources)
        self.web = StatusWebServer(config) if config.web_enabled else None
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        self.state.mark_stale_downloads_interrupted()
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
            "live_from_start=%s keep_fragments_for_resume=%s "
            "reconnect_interval_seconds=%s post_exit_check_seconds=%s "
            "render_live_chat_video=%s chat_render_use_nvenc=%s "
            "chat_render_nvenc_devices=%s transcribe_subtitles=%s "
            "whisperx_model=%s whisperx_diarize=%s watermark_enabled=%s "
            "watermark_strength=%s web_enabled=%s web_bind=%s:%s",
            self.config.channel_scan_limit,
            self.config.discovery_probe_concurrency,
            self.config.live_from_start,
            self.config.keep_fragments_for_resume,
            self.config.reconnect_interval_seconds,
            self.config.post_exit_check_seconds,
            self.config.render_live_chat_video,
            self.config.chat_render_use_nvenc,
            self.config.chat_render_nvenc_devices,
            self.config.transcribe_subtitles,
            self.config.whisperx_model,
            self.config.whisperx_diarize,
            self.config.watermark_enabled,
            self.config.watermark_strength,
            self.config.web_enabled,
            self.config.web_host,
            self.config.web_port,
        )
        if self.config.render_live_chat_video or self.config.chat_render_use_nvenc:
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
                    "Unable to start status web interface on %s:%s: %s",
                    self.config.web_host,
                    self.config.web_port,
                    exc,
                )
        else:
            LOGGER.info("Status web interface disabled by config")
        if not monitored_sources(self.config):
            LOGGER.warning("No sources configured; edit config.toml to add channels or streamers")

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

    async def poll_once(self) -> None:
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
                await self._start_stream(stream)

    async def _start_stream(self, stream: LiveStream) -> None:
        self.state.upsert_detected(stream)
        await self.downloads.start_stream(stream)
