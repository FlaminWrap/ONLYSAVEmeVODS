from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, patch
import json
import logging
import subprocess
import unittest

from onlysavemevods import __version__
from onlysavemevods.config import BotConfig, ConfigError, StreamerConfig, VoiceDetectionConfig, VoiceProfileConfig, load_config
from onlysavemevods.chat_refresh import ChatRefreshResult
from onlysavemevods.log_buffer import RingBufferLogHandler, clear_log_buffer
from onlysavemevods.models import LiveStream, video_url
from onlysavemevods.state import StateStore
from onlysavemevods.web import (
    build_config_summary,
    build_status_snapshot,
    dashboard_script,
    chat_media_file_for_chat_file,
    file_kind,
    format_bytes,
    is_watermarkable_media_file,
    render_file_action,
    render_status_html,
    update_app_config_from_form,
    update_speaker_labels_from_form,
    update_streamer_voice_from_form,
    update_streamer_voice_with_optional_sample,
    store_streamer_voice_sample_upload,
    create_streamer_voice_sample_from_transcript_form,
    update_streamer_from_form,
    update_voice_detection_from_form,
    resolve_refresh_chat_files,
    resolve_watermark_download_file,
    resolve_transcription_source_file,
    resolve_render_chat_files,
    resolve_download_file,
    run_refresh_chat_job,
    run_render_chat_in_process_job,
    update_render_chat_job,
    run_render_chat_process_job,
    run_transcription_job,
    refresh_chat_job_key,
    RefreshChatJob,
    CHAT_REFRESH_JOBS,
    CHAT_REFRESH_JOBS_LOCK,
    RenderChatJob,
    CHAT_RENDER_JOBS,
    CHAT_RENDER_JOBS_LOCK,
    FAVICON_ROUTES,
    PLATFORM_ICON_ROUTES,
    snapshot_to_dict,
    transcription_job_key,
    TranscriptionJob,
    TRANSCRIPTION_JOBS,
    TRANSCRIPTION_JOBS_LOCK,
    StatusWebServer,
    StreamEventStatus,
    render_stream_event_timeline,
)


def app_config_form_params(**overrides: str) -> dict[str, list[str]]:
    values = {
        "channels": "@Example\n@Second",
        "download_dir": "downloads",
        "state_dir": "state",
        "poll_interval_seconds": "60",
        "channel_scan_limit": "10",
        "discovery_probe_concurrency": "4",
        "max_concurrent_downloads": "4",
        "live_from_start": "true",
        "keep_fragments_for_resume": "true",
        "reconnect_interval_seconds": "0",
        "post_exit_check_seconds": "30, 60, 90",
        "retry_backoff_seconds": "30, 60, 120",
        "extra_yt_dlp_args_mode": "keep",
        "extra_yt_dlp_args": "",
        "record_live_chat": "false",
        "render_live_chat_video": "false",
        "chat_render_panel_workers": "0",
        "chat_render_timeout_seconds": "3600",
        "chat_render_use_nvenc": "false",
        "chat_render_nvenc_devices": "",
        "transcribe_subtitles": "false",
        "transcription_max_concurrent": "1",
        "whisperx_path": "whisperx",
        "whisperx_model": "large-v3",
        "whisperx_device": "cuda",
        "whisperx_compute_type": "float16",
        "whisperx_batch_size": "16",
        "whisperx_language": "",
        "voice_match_enabled": "true",
        "voice_match_model": "pyannote/embedding",
        "voice_match_threshold": "0.35",
        "voice_match_min_margin": "0.05",
        "voice_sample_max_bytes": "104857600",
        "web_enabled": "true",
        "web_host": "127.0.0.1",
        "web_port": "8080",
        "log_level": "INFO",
        "yt_dlp_path": "yt-dlp",
        "ffmpeg_path": "ffmpeg",
        "watermark_enabled": "false",
        "watermark_secret_env": "ONLYSAVEMEVODS_WATERMARK_SECRET",
        "watermark_strength": "invisible",
        "watermark_detect_upload_max_bytes": "2147483648",
    }
    values.update(overrides)
    return {key: [value] for key, value in values.items()}


class WebStatusTests(unittest.TestCase):
    def test_status_snapshot_reads_state_and_download_files(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                channels=["@ExampleChannel", "@Unused"],
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Live Status",
                channel="Example Channel",
            )
            state = StateStore(config.db_path)
            state.mark_downloading(stream, 1)
            state.add_stream_event(
                stream.video_id,
                "Post-exit check saw stream live",
                segment_index=1,
            )
            state.close()

            segment_dir = config.download_dir / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            (segment_dir / "segment-001.f140.mp4.part").write_text(
                "audio",
                encoding="utf-8",
            )
            (segment_dir / "segment-001.f137.mp4").write_text(
                "video",
                encoding="utf-8",
            )
            (segment_dir / "Live Status [LIVEVIDEO01].mp4").write_text(
                "final",
                encoding="utf-8",
            )
            (segment_dir / "segment-001.f137.mp4.part-Frag1").write_text(
                "fragment",
                encoding="utf-8",
            )
            (segment_dir / "segment-001.f140.mp4.ytdl").write_text(
                "{}",
                encoding="utf-8",
            )
            (segment_dir / "segment-001.live_chat.json").write_text(
                "chat",
                encoding="utf-8",
            )

            snapshot = build_status_snapshot(config)

        self.assertEqual(snapshot.counts["downloading"], 1)
        self.assertEqual(len(snapshot.streams), 1)
        self.assertGreater(snapshot.total_bytes, 0)
        self.assertGreater(snapshot.part_bytes, 0)
        self.assertGreater(snapshot.final_bytes, 0)
        self.assertGreater(snapshot.chat_bytes, 0)
        self.assertGreater(snapshot.fragment_bytes, 0)
        self.assertGreater(snapshot.state_bytes, 0)
        stream_status = snapshot.streams[0]
        self.assertEqual(stream_status.video_id, "LIVEVIDEO01")
        self.assertTrue(stream_status.has_part_files)
        self.assertTrue(stream_status.has_mixed_formats)
        self.assertGreater(stream_status.total_bytes, 0)
        self.assertEqual(stream_status.file_kind_counts["part"], 1)
        self.assertEqual(stream_status.file_kind_counts["final"], 2)
        self.assertEqual(stream_status.file_kind_counts["chat"], 1)
        self.assertEqual(stream_status.file_kind_counts["fragment"], 1)
        self.assertEqual(stream_status.file_kind_counts["state"], 1)
        self.assertGreater(stream_status.chat_bytes, 0)
        self.assertIsNotNone(stream_status.latest_file_modified_at)
        self.assertIn("segment-001.f140.mp4.part", [file.name for file in stream_status.files])
        part_file = next(
            file
            for file in stream_status.files
            if file.name == "segment-001.f140.mp4.part"
        )
        self.assertEqual(part_file.segment, "segment-001")
        self.assertEqual(part_file.format_id, "140")
        self.assertIsNone(part_file.download_url)
        final_file = next(
            file
            for file in stream_status.files
            if file.name == "Live Status [LIVEVIDEO01].mp4"
        )
        self.assertIsNotNone(final_file.download_url)
        assert final_file.download_url is not None
        self.assertIn("/download?", final_file.download_url)
        self.assertIn("video_id=LIVEVIDEO01", final_file.download_url)
        format_file = next(
            file
            for file in stream_status.files
            if file.name == "segment-001.f137.mp4"
        )
        self.assertIsNone(format_file.download_url)
        chat_file = next(
            file
            for file in stream_status.files
            if file.name == "segment-001.live_chat.json"
        )
        self.assertIsNotNone(chat_file.download_url)
        example_channel = next(
            channel
            for channel in snapshot.channel_stats
            if channel.name == "Example Channel"
        )
        self.assertEqual(example_channel.configured_sources, ["@ExampleChannel"])
        self.assertEqual(example_channel.stream_count, 1)
        self.assertEqual(example_channel.active_count, 1)
        self.assertEqual(example_channel.downloadable_count, 2)
        self.assertGreater(example_channel.total_bytes, 0)
        self.assertGreater(example_channel.chat_bytes, 0)
        self.assertEqual(
            [
                channel.configured_sources
                for channel in snapshot.channel_stats
            ].count(["@ExampleChannel"]),
            1,
        )
        unused_channel = next(
            channel
            for channel in snapshot.channel_stats
            if channel.name == "@Unused"
        )
        self.assertEqual(unused_channel.configured_sources, ["@Unused"])
        self.assertEqual(unused_channel.stream_count, 0)

    def test_status_snapshot_groups_streamer_sources(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                streamers={
                    "OUMB3rd": StreamerConfig(
                        sources=["@OUMB3rd", "@OUMB3rdVODS"],
                    )
                },
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Live Status",
                channel="OUMB3rd VODS",
            )
            state = StateStore(config.db_path)
            state.mark_downloading(stream, 1)
            state.close()
            segment_dir = config.download_dir / "OUMB3rd" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            (segment_dir / "Live Status [LIVEVIDEO01].mp4").write_text(
                "final",
                encoding="utf-8",
            )

            snapshot = build_status_snapshot(config)

        streamer = next(
            channel
            for channel in snapshot.channel_stats
            if channel.name == "OUMB3rd"
        )
        self.assertEqual(streamer.configured_sources, ["@OUMB3rd", "@OUMB3rdVODS"])
        self.assertEqual(streamer.stream_count, 1)
        self.assertEqual(streamer.active_count, 1)
        streamer_stat = next(
            item
            for item in snapshot.streamer_stats
            if item.name == "OUMB3rd"
        )
        self.assertTrue(streamer_stat.configured)
        self.assertFalse(streamer_stat.needs_grouping)
        self.assertEqual(streamer_stat.sources, ["@OUMB3rd", "@OUMB3rdVODS"])
        self.assertEqual(streamer_stat.download_dir_name, "OUMB3rd")
        self.assertEqual(streamer_stat.stream_count, 1)
        self.assertEqual(streamer_stat.active_count, 1)
        self.assertEqual(streamer_stat.streams[0].video_id, "LIVEVIDEO01")
        self.assertEqual(snapshot.configuration["Streamers"]["count"], 1)
        self.assertEqual(snapshot.configuration["Channels"]["monitored_source_count"], 2)

    def test_status_snapshot_marks_top_level_channels_as_needs_grouping(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                channels=["@ExampleChannel", "@Unused"],
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Live Status",
                channel="Example Channel",
            )
            state = StateStore(config.db_path)
            state.mark_downloading(stream, 1)
            state.close()

            snapshot = build_status_snapshot(config)

        grouped = next(
            item
            for item in snapshot.streamer_stats
            if item.name == "Example Channel"
        )
        self.assertFalse(grouped.configured)
        self.assertTrue(grouped.needs_grouping)
        self.assertEqual(grouped.sources, ["@ExampleChannel"])
        self.assertEqual(grouped.streams[0].video_id, "LIVEVIDEO01")
        unused = next(
            item
            for item in snapshot.streamer_stats
            if item.name == "@Unused"
        )
        self.assertTrue(unused.needs_grouping)
        self.assertEqual(unused.sources, ["@Unused"])
        self.assertEqual(unused.stream_count, 0)

    def test_status_html_and_json_are_renderable(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                record_live_chat=True,
                render_live_chat_video=True,
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="<Live Status>",
                channel="Example Channel",
            )
            state = StateStore(config.db_path)
            state.mark_downloading(stream, 1)
            state.add_stream_event(
                stream.video_id,
                "Post-exit check saw stream live",
                segment_index=1,
            )
            state.close()
            segment_dir = config.download_dir / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            (segment_dir / "Live Status [LIVEVIDEO01].mp4").write_text(
                "final",
                encoding="utf-8",
            )
            (segment_dir / "Live Status [LIVEVIDEO01].live_chat.json").write_text(
                "chat",
                encoding="utf-8",
            )
            clear_log_buffer()
            handler = RingBufferLogHandler()
            handler.setFormatter(
                logging.Formatter("%(levelname)s %(name)s: %(message)s")
            )
            record = logging.LogRecord(
                "tests.web",
                logging.WARNING,
                __file__,
                1,
                "dashboard warning %s",
                ("line",),
                None,
            )
            handler.handle(record)

            snapshot = build_status_snapshot(config)
            html = render_status_html(snapshot)
            payload = snapshot_to_dict(snapshot)
            clear_log_buffer()

        self.assertIn("&lt;Live Status&gt;", html)
        self.assertIn("/status.json", html)
        self.assertIn("Storage", html)
        self.assertIn("Chat size", html)
        self.assertIn("Runtime", html)
        self.assertIn("Latest file", html)
        self.assertIn("Download", html)
        self.assertIn("Render chat", html)
        self.assertIn("/render-chat?", html)
        self.assertIn("/download?", html)
        self.assertIn("Streamers", html)
        self.assertIn('id="tab-streamers"', html)
        self.assertIn('for="tab-streamers"', html)
        self.assertIn('id="streamer-list"', html)
        self.assertIn("Needs Grouping", html)
        self.assertIn("renderStreamerList", html)
        self.assertNotIn('for="tab-streams"', html)
        self.assertNotIn('for="tab-channels"', html)
        self.assertNotIn('for="tab-streamer-groups"', html)
        self.assertIn("Jobs", html)
        self.assertIn("job-rows", html)
        self.assertIn("Recent Logs", html)
        self.assertIn("About", html)
        self.assertIn("Version", html)
        self.assertIn(__version__, html)
        self.assertIn("Current Configuration", html)
        self.assertIn("record_live_chat", html)
        self.assertIn("render_live_chat_video", html)
        self.assertIn("tab-config", html)
        self.assertIn("tab-about", html)
        self.assertIn("config-stack", html)
        self.assertIn("onlysavemevods.dashboardTab", html)
        self.assertIn("normalizeTabId", html)
        self.assertIn('"tab-streams": "tab-streamers"', html)
        self.assertIn('"tab-channels": "tab-streamers"', html)
        self.assertIn('"tab-streamer-groups": "tab-streamers"', html)
        self.assertIn("onlysavemevods.collapsedStreams", html)
        self.assertIn("onlysavemevods.expandedStreams", html)
        self.assertIn("onlysavemevods.collapsedStreamers", html)
        self.assertIn("onlysavemevods.expandedStreamers", html)
        self.assertIn("onlysavemevods.openStreamerSettings", html)
        self.assertIn("onlysavemevods.streamTabs", html)
        self.assertIn("data-streamer-toggle", html)
        self.assertIn("data-streamer-settings-toggle", html)
        self.assertIn("data-streamer-settings-panel", html)
        self.assertIn("streamer-details", html)
        self.assertIn("applyStreamerCollapsedState", html)
        self.assertIn("applyStreamerSettingsState", html)
        self.assertIn("applyStreamTabState", html)
        self.assertIn("stream-detail-tabs", html)
        self.assertIn('data-stream-tab="files"', html)
        self.assertIn('data-stream-tab="log"', html)
        self.assertIn('data-stream-tab="jobs"', html)
        self.assertIn("stream-tab-panel stream-tab-files", html)
        self.assertIn("stream-tab-panel stream-tab-log", html)
        self.assertIn("stream-tab-panel stream-tab-jobs", html)
        self.assertIn("Stream Log", html)
        stream_tabs = html[html.index('<div class="stream-tab-labels">'):]
        self.assertLess(
            stream_tabs.index('class="stream-tab-jobs-label"'),
            stream_tabs.index('class="stream-tab-log-label"'),
        )
        self.assertLess(
            stream_tabs.index('stream-tab-panel stream-tab-jobs'),
            stream_tabs.index('stream-tab-panel stream-tab-log'),
        )
        self.assertIn("stream-events", html)
        self.assertIn("stream-event-level", html)
        self.assertIn(">INFO</div>", html)
        self.assertIn("Post-exit check saw stream live", html)
        self.assertIn("seg 001", html)
        self.assertIn("data-stream-toggle", html)
        self.assertIn("stream-body", html)
        self.assertIn("Collapse", html)
        self.assertIn('fetch("/status.json"', html)
        self.assertIn("applySnapshot", html)
        self.assertIn("const rows = [...events].reverse().map", html)
        self.assertIn("window.setInterval(refreshStatus, 15000)", html)
        self.assertNotIn("window.location.reload", html)
        self.assertNotIn('http-equiv="refresh"', html)
        self.assertIn("#\" + id.replace(\"tab-\", \"\")", html)
        self.assertIn("dashboard warning line", html)
        self.assertEqual(payload["streams"][0]["video_id"], "LIVEVIDEO01")
        self.assertEqual(
            payload["streams"][0]["events"][-1]["message"],
            "Post-exit check saw stream live",
        )
        self.assertEqual(payload["streams"][0]["events"][-1]["segment_index"], 1)
        self.assertIn("file_kind_counts", payload["streams"][0])
        self.assertIn("total_bytes", payload)
        self.assertIn("chat_bytes", payload)
        self.assertIn("configured_channels", payload)
        self.assertIn("app", payload)
        self.assertEqual(payload["app"]["name"], "ONLYSAVEmeVODS")
        self.assertEqual(payload["app"]["version"], __version__)
        self.assertIn("python_version", payload["app"])
        self.assertIn("configuration", payload)
        self.assertTrue(payload["configuration"]["Live Chat"]["record_live_chat"])
        self.assertTrue(payload["configuration"]["Live Chat"]["render_live_chat_video"])
        self.assertEqual(payload["configuration"]["Live Chat"]["chat_render_panel_workers"], 0)
        self.assertEqual(payload["configuration"]["Live Chat"]["chat_render_timeout_seconds"], 3600)
        self.assertFalse(payload["configuration"]["Live Chat"]["chat_render_use_nvenc"])
        self.assertEqual(payload["configuration"]["Live Chat"]["chat_render_nvenc_devices"], [])
        self.assertIn("Transcription", payload["configuration"])
        self.assertFalse(payload["configuration"]["Transcription"]["transcribe_subtitles"])
        self.assertEqual(payload["configuration"]["Transcription"]["voice_detection"], "auto")
        self.assertEqual(payload["configuration"]["Transcription"]["voice_detection_speakers"], "auto")
        self.assertEqual(payload["configuration"]["Transcription"]["whisperx_model"], "large-v3")
        self.assertIn("streamer_stats", payload)
        self.assertIn("channel_stats", payload)
        self.assertIn("streamer_groups", payload)
        self.assertEqual(payload["streamer_stats"][0]["name"], "Example Channel")
        self.assertTrue(payload["streamer_stats"][0]["needs_grouping"])
        self.assertEqual(
            payload["streamer_stats"][0]["streams"][0]["video_id"],
            "LIVEVIDEO01",
        )
        self.assertIn("jobs", payload)
        self.assertIn("job_limit", payload)
        self.assertIn("recent_logs", payload)
        self.assertEqual(payload["recent_logs"][0]["level"], "WARNING")
        self.assertIn("download_url", payload["streams"][0]["files"][0])
        chat_payload = next(
            file
            for file in payload["streams"][0]["files"]
            if file["name"] == "Live Status [LIVEVIDEO01].live_chat.json"
        )
        self.assertIn("/render-chat?", chat_payload["render_chat_url"])
        self.assertEqual(chat_payload["render_chat_status"], "ready")
        json.dumps(payload)

    def test_stream_event_timeline_renders_newest_first(self) -> None:
        html = render_stream_event_timeline(
            [
                StreamEventStatus(
                    event_id=1,
                    level="info",
                    message="Older event",
                    segment_index=None,
                    created_at="2026-06-14T11:00:00Z",
                ),
                StreamEventStatus(
                    event_id=2,
                    level="warning",
                    message="Newer event",
                    segment_index=2,
                    created_at="2026-06-14T12:00:00Z",
                ),
            ]
        )

        self.assertLess(html.index("Newer event"), html.index("Older event"))
        self.assertIn("seg 002", html)


    def test_stream_detail_tabs_include_matching_jobs(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Live Status",
                channel="Example Channel",
            )
            state = StateStore(config.db_path)
            state.mark_downloading(stream, 1)
            state.close()
            key = "LIVEVIDEO01\0Live Status [LIVEVIDEO01].live_chat.json"
            with CHAT_RENDER_JOBS_LOCK:
                CHAT_RENDER_JOBS[key] = RenderChatJob(
                    video_id="LIVEVIDEO01",
                    chat_name="Live Status [LIVEVIDEO01].live_chat.json",
                    media_name="Live Status [LIVEVIDEO01].mp4",
                    output_name="Live Status [LIVEVIDEO01] - chat.mp4",
                    status="running",
                    message="Rendering chat",
                    phase="Starting isolated renderer",
                    progress=0.05,
                    started_at=0.0,
                )
            try:
                snapshot = build_status_snapshot(config)
                html = render_status_html(snapshot)
                payload = snapshot_to_dict(snapshot)
            finally:
                with CHAT_RENDER_JOBS_LOCK:
                    CHAT_RENDER_JOBS.pop(key, None)

        self.assertEqual(snapshot.streams[0].jobs[0].kind, "Chat render")
        self.assertEqual(payload["streams"][0]["jobs"][0]["kind"], "Chat render")
        self.assertIn('data-stream-tab="jobs"', html)
        self.assertIn('stream-tab-panel stream-tab-jobs', html)
        self.assertIn("Chat render", html)
        self.assertIn("Starting isolated renderer", html)
        self.assertIn("streamer-job-body", html)
        self.assertIn("streamer-job-heading", html)
        self.assertIn("streamer-job-progress", html)
        self.assertIn("streamer-job-detail", html)

    def test_isolated_chat_render_reports_output_file_growth(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.toml"
            config_path.write_text(
                'download_dir = "downloads"\n'
                'state_dir = "state"\n',
                encoding="utf-8",
            )
            config = load_config(config_path)
            media_file = config.download_dir / "Live Status [LIVEVIDEO01].mp4"
            chat_file = config.download_dir / "Live Status [LIVEVIDEO01].live_chat.json"
            output_file = config.download_dir / "Live Status [LIVEVIDEO01] - chat.mp4"
            panel_file = output_file.with_name(f"{output_file.stem}.panel{output_file.suffix}")
            media_file.parent.mkdir(parents=True)
            media_file.write_text("media", encoding="utf-8")
            chat_file.write_text("chat", encoding="utf-8")
            key = "LIVEVIDEO01\0Live Status [LIVEVIDEO01].live_chat.json"
            with CHAT_RENDER_JOBS_LOCK:
                CHAT_RENDER_JOBS[key] = RenderChatJob(
                    video_id="LIVEVIDEO01",
                    chat_name=chat_file.name,
                    media_name=media_file.name,
                    output_name=output_file.name,
                    status="running",
                    message="Rendering chat video",
                    started_at=0.0,
                )

            class FakeProcess:
                returncode: int | None = None

                def __init__(self, *_args: object, **_kwargs: object) -> None:
                    self.calls = 0

                def communicate(self, timeout: float | None = None) -> tuple[bytes, bytes]:
                    self.calls += 1
                    if self.calls == 1:
                        panel_file.write_bytes(b"x" * 2048)
                        raise subprocess.TimeoutExpired(cmd="fake", timeout=timeout)
                    self.returncode = 0
                    output_file.write_bytes(b"done")
                    return b"", b""

            updates: list[dict[str, object]] = []

            def spy_update(job_key: str, **changes: object) -> None:
                updates.append(dict(changes))
                update_render_chat_job(job_key, **changes)

            try:
                with (
                    patch("onlysavemevods.web.subprocess.Popen", FakeProcess),
                    patch("onlysavemevods.web.update_render_chat_job", side_effect=spy_update),
                ):
                    run_render_chat_process_job(
                        config,
                        key,
                        media_file,
                        chat_file,
                        output_file,
                    )
                with CHAT_RENDER_JOBS_LOCK:
                    job = CHAT_RENDER_JOBS[key]
            finally:
                with CHAT_RENDER_JOBS_LOCK:
                    CHAT_RENDER_JOBS.pop(key, None)

        progress_updates = [
            update
            for update in updates
            if "Rendering in isolated process" in str(update.get("phase", ""))
        ]
        self.assertTrue(progress_updates)
        self.assertIn("chat panel 2.0 KiB", str(progress_updates[0]["phase"]))
        self.assertIsNone(progress_updates[0]["progress"])
        self.assertEqual(job.status, "done")
        self.assertEqual(job.progress, 1.0)

    def test_manual_chat_render_uses_configured_timeout(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                chat_render_timeout_seconds=7200,
            )
            media_file = config.download_dir / "Live Status [LIVEVIDEO01].mp4"
            chat_file = config.download_dir / "Live Status [LIVEVIDEO01].live_chat.json"
            output_file = config.download_dir / "Live Status [LIVEVIDEO01] - chat.mp4"
            media_file.parent.mkdir(parents=True)
            media_file.write_text("media", encoding="utf-8")
            chat_file.write_text("chat", encoding="utf-8")
            key = "LIVEVIDEO01\0Live Status [LIVEVIDEO01].live_chat.json"
            with CHAT_RENDER_JOBS_LOCK:
                CHAT_RENDER_JOBS[key] = RenderChatJob(
                    video_id="LIVEVIDEO01",
                    chat_name=chat_file.name,
                    media_name=media_file.name,
                    output_name=output_file.name,
                    status="running",
                    message="Rendering chat video",
                    started_at=0.0,
                )

            try:
                with patch("onlysavemevods.web.render_chat_video_file") as render:
                    run_render_chat_in_process_job(
                        config,
                        key,
                        media_file,
                        chat_file,
                        output_file,
                    )
                with CHAT_RENDER_JOBS_LOCK:
                    job = CHAT_RENDER_JOBS[key]
            finally:
                with CHAT_RENDER_JOBS_LOCK:
                    CHAT_RENDER_JOBS.pop(key, None)

        self.assertEqual(job.status, "done")
        render.assert_called_once()
        self.assertEqual(render.call_args.kwargs["timeout_seconds"], 7200.0)

    def test_inactive_configured_streamers_render_collapsed_by_default(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.toml"
            config_path.write_text(
                'download_dir = "downloads"\n'
                'state_dir = "state"\n'
                '[streamers.OUMB3rd]\n'
                'sources = ["@OUMB3rd"]\n',
                encoding="utf-8",
            )
            config = load_config(config_path)

            html = render_status_html(build_status_snapshot(config))

        self.assertIn('class="streamer-section collapsed"', html)
        self.assertIn('data-streamer-key="OUMB3rd"', html)
        self.assertIn('data-streamer-active="0"', html)
        self.assertIn('data-streamer-attention="0"', html)
        self.assertIn('data-streamer-active-jobs="0"', html)
        self.assertIn('data-streamer-needs-grouping="false"', html)
        self.assertIn('data-streamer-settings-toggle="OUMB3rd" aria-expanded="false">Settings</button>', html)
        self.assertIn('<div class="streamer-settings-panel" data-streamer-settings-panel hidden>', html)
        self.assertIn('data-streamer-toggle="OUMB3rd" aria-expanded="false">Expand</button>', html)

    def test_status_html_renders_streamer_wizard_and_prefill_buttons(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.toml"
            config_path.write_text(
                'channels = ["@ExampleChannel"]\n'
                'download_dir = "downloads"\n'
                'state_dir = "state"\n',
                encoding="utf-8",
            )
            config = load_config(config_path)
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Live Status",
                channel="Example Channel",
            )
            state = StateStore(config.db_path)
            state.mark_downloading(stream, 1)
            state.close()

            html = render_status_html(build_status_snapshot(config))

        self.assertIn('data-open-streamer-wizard', html)
        self.assertIn('id="streamer-wizard"', html)
        self.assertIn('name="form_kind" value="streamer_wizard"', html)
        self.assertIn('data-wizard-next', html)
        self.assertIn('data-add-wizard-speaker', html)
        self.assertIn('Create Streamer', html)
        self.assertIn('data-streamer-name="Example Channel"', html)
        self.assertIn('data-streamer-sources="@ExampleChannel"', html)
        self.assertIn('renderStreamerGroupingAction', html)
        self.assertNotIn('streamer-create-panel', html)
        self.assertNotIn('renderStreamerCreatePanel', html)

    def test_streamer_settings_include_source_builder(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.toml"
            config_path.write_text(
                'download_dir = "downloads"\n'
                'state_dir = "state"\n'
                '[streamers."OUMB3rd"]\n'
                'sources = ["@OUMB3rd"]\n',
                encoding="utf-8",
            )
            config = load_config(config_path)

            html = render_status_html(build_status_snapshot(config))

        self.assertIn('data-source-builder', html)
        self.assertIn('data-source-platform', html)
        self.assertIn('data-source-input', html)
        self.assertIn('data-add-source', html)
        self.assertIn('data-close-source-popover', html)
        self.assertIn('data-source-list', html)
        self.assertNotIn('data-source-unsaved', html)
        self.assertNotIn('Unsaved source changes', html)
        self.assertIn('source-platform-icon youtube', html)
        self.assertIn('src="/assets/platforms/youtube.svg?v=', html)
        self.assertIn('data-remove-source="@OUMB3rd"', html)
        self.assertIn('OUMB3rd', html)
        self.assertIn('<option value="twitch">Twitch</option>', html)
        self.assertIn('detectSourcePlatform', html)
        self.assertIn('normalizeSourceValue', html)
        self.assertIn('sourceUrlPath', html)
        self.assertIn('form.requestSubmit', html)
        self.assertIn('renderSourceList', html)
        self.assertIn('markStreamerFormDirty', html)
        self.assertIn('streamerListIsEditing(streamerList)', html)

    def test_status_html_links_package_favicons(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            html = render_status_html(build_status_snapshot(config))

        self.assertIn('href="/favicon.ico?v=', html)
        self.assertIn('href="/favicon-32x32.png?v=', html)
        self.assertIn('href="/favicon-16x16.png?v=', html)
        self.assertIn('href="/apple-touch-icon.png?v=', html)
        self.assertIn('href="/android-chrome-192x192.png?v=', html)
        for path in FAVICON_ROUTES.values():
            self.assertTrue(Path(path).is_file(), path)
            self.assertIn("assets/favicons", Path(path).as_posix())
        self.assertIn("/assets/platforms/youtube.svg", PLATFORM_ICON_ROUTES)
        self.assertTrue(Path(PLATFORM_ICON_ROUTES["/assets/platforms/youtube.svg"]).is_file())

    def test_status_html_renders_streamer_voice_manager(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.toml"
            config_path.write_text(
                'download_dir = "downloads"\n'
                'state_dir = "state"\n'
                '[streamers."OUMB3rd"]\n'
                'sources = ["@OUMB3rd"]\n'
                '[streamers."OUMB3rd".voices."Host"]\n'
                'enabled = true\n'
                'samples = ["host.wav"]\n',
                encoding="utf-8",
            )
            config = load_config(config_path)
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Live Status",
                channel="OUMB3rd",
            )
            state = StateStore(config.db_path)
            state.mark_downloading(stream, 1)
            state.close()
            segment_dir = config.download_dir / "OUMB3rd" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            media_file = segment_dir / "Live Status [LIVEVIDEO01].mp4"
            media_file.write_text("media", encoding="utf-8")
            media_file.with_suffix(".json").write_text(
                json.dumps(
                    {
                        "segments": [
                            {
                                "start": 0.0,
                                "end": 1.5,
                                "speaker": "SPEAKER_00",
                                "text": "hello",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            media_file.with_suffix(".srt").write_text("1\n", encoding="utf-8")
            media_file.with_suffix(".vtt").write_text("WEBVTT\n", encoding="utf-8")
            media_file.with_suffix(".voice-attribution.json").write_text(
                json.dumps(
                    {
                        "matches": {
                            "SPEAKER_00": {
                                "voice": "Host",
                                "status": "suggested",
                                "distance": 0.2,
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )

            html = render_status_html(build_status_snapshot(config))

        self.assertIn('data-open-voice-manager="voice-manager-OUMB3rd"', html)
        self.assertIn('id="voice-manager-OUMB3rd"', html)
        self.assertIn("Known Voices", html)
        self.assertIn("Add Voice", html)
        self.assertIn("voice-manager-actions", html)
        self.assertIn("voice-add-menu", html)
        self.assertIn("voice-add-popover", html)
        self.assertIn("Optional sample", html)
        self.assertIn("voice-list", html)
        self.assertIn("voice-card", html)
        self.assertIn("voice-card-action", html)
        self.assertIn("Edit Voice", html)
        self.assertIn("Voice name", html)
        self.assertIn("Sample files", html)
        self.assertIn("Upload Sample", html)
        self.assertIn("Detected Speakers", html)
        self.assertIn("Review Matches", html)
        self.assertIn("Host", html)
        self.assertIn("Known voices", html)
        self.assertIn('action="/streamer-voices"', html)
        self.assertIn('action="/streamer-voice-samples"', html)
        self.assertIn('action="/streamer-voice-samples/from-transcript"', html)
        self.assertIn('action="/streamer-voice-attributions"', html)
        self.assertIn("SPEAKER_00", html)
        self.assertIn("Approve", html)
        self.assertIn("Reject", html)
        self.assertIn("Match Voices", html)

    def test_dashboard_script_does_not_emit_literal_newlines_in_js_strings(self) -> None:
        script = dashboard_script()

        self.assertIn("join(String.fromCharCode(10))", script)
        self.assertNotIn('join("' + "\n" + '")', script)

    def test_jobs_tab_shows_dashboard_and_watermark_jobs(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            state = StateStore(config.db_path)
            state.create_watermark_copy(
                copy_id="wm-copy001",
                video_id="LIVEVIDEO01",
                source_name="Live Status [LIVEVIDEO01].mp4",
                output_name=".watermarks/Live Status [LIVEVIDEO01] - wm-copy001.mp4",
                recipient_label="Recipient A",
                message="Queued watermark render",
            )
            state.close()
            key = "LIVEVIDEO01\0Live Status [LIVEVIDEO01].live_chat.json"
            with CHAT_RENDER_JOBS_LOCK:
                CHAT_RENDER_JOBS[key] = RenderChatJob(
                    video_id="LIVEVIDEO01",
                    chat_name="Live Status [LIVEVIDEO01].live_chat.json",
                    media_name="Live Status [LIVEVIDEO01].mp4",
                    output_name="Live Status [LIVEVIDEO01] - chat.mp4",
                    status="running",
                    message="Rendering chat video",
                    started_at=123.0,
                    phase="Rendering panel frames",
                    progress=0.42,
                    updated_at=130.0,
                )

            try:
                snapshot = build_status_snapshot(config)
                payload = snapshot_to_dict(snapshot)
                html = render_status_html(snapshot)
            finally:
                with CHAT_RENDER_JOBS_LOCK:
                    CHAT_RENDER_JOBS.pop(key, None)

        self.assertIn('for="tab-jobs"', html)
        self.assertIn('id="job-rows"', html)
        self.assertIn("Chat render", html)
        self.assertIn("Rendering chat video", html)
        self.assertIn("Rendering panel frames", html)
        self.assertIn("42%", html)
        self.assertIn("<progress", html)
        self.assertIn("Watermark", html)
        self.assertIn("Recipient A", html)
        self.assertIn("metric-jobs", html)
        self.assertEqual(payload["jobs"][0]["kind"], "Watermark")
        self.assertTrue(any(job["kind"] == "Chat render" for job in payload["jobs"]))
        self.assertTrue(any(job["progress"] == 0.42 for job in payload["jobs"]))
        self.assertTrue(any(job["status"] == "queued" for job in payload["jobs"]))
        self.assertEqual(payload["job_limit"], 200)

    def test_streamer_stats_include_matching_jobs(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                streamers={
                    "OUMB3rd": StreamerConfig(sources=["@OUMB3rdVODS"]),
                },
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Live Status",
                channel="OUMB3rd VODS",
            )
            state = StateStore(config.db_path)
            state.mark_downloading(stream, 1)
            state.close()
            key = "LIVEVIDEO01\0Live Status [LIVEVIDEO01].live_chat.json"
            with CHAT_RENDER_JOBS_LOCK:
                CHAT_RENDER_JOBS[key] = RenderChatJob(
                    video_id="LIVEVIDEO01",
                    chat_name="Live Status [LIVEVIDEO01].live_chat.json",
                    media_name="Live Status [LIVEVIDEO01].mp4",
                    output_name="Live Status [LIVEVIDEO01] - chat.mp4",
                    status="running",
                    message="Rendering chat video",
                    started_at=123.0,
                    phase="Rendering panel frames",
                    progress=0.42,
                    updated_at=130.0,
                )

            try:
                snapshot = build_status_snapshot(config)
                payload = snapshot_to_dict(snapshot)
            finally:
                with CHAT_RENDER_JOBS_LOCK:
                    CHAT_RENDER_JOBS.pop(key, None)

        streamer = next(
            item
            for item in snapshot.streamer_stats
            if item.name == "OUMB3rd"
        )
        self.assertEqual(streamer.jobs[0].kind, "Chat render")
        self.assertEqual(streamer.jobs[0].progress, 0.42)
        payload_streamer = next(
            item
            for item in payload["streamer_stats"]
            if item["name"] == "OUMB3rd"
        )
        self.assertEqual(payload_streamer["jobs"][0]["kind"], "Chat render")
        self.assertEqual(payload_streamer["jobs"][0]["video_id"], "LIVEVIDEO01")

    def test_download_resolver_serves_only_final_files_for_known_stream(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Live Status",
                channel="Example Channel",
            )
            state = StateStore(config.db_path)
            state.mark_downloading(stream, 1)
            state.close()

            segment_dir = config.download_dir / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            final_file = segment_dir / "Live Status [LIVEVIDEO01].mp4"
            format_file = segment_dir / "segment-001.f137.mp4"
            part_file = segment_dir / "segment-001.f140.mp4.part"
            chat_file = segment_dir / "Live Status [LIVEVIDEO01].live_chat.json"
            final_file.write_text("final", encoding="utf-8")
            format_file.write_text("video only", encoding="utf-8")
            part_file.write_text("part", encoding="utf-8")
            chat_file.write_text("chat", encoding="utf-8")

            resolved = resolve_download_file(
                config,
                "LIVEVIDEO01",
                final_file.name,
            )
            rejected_part = resolve_download_file(
                config,
                "LIVEVIDEO01",
                part_file.name,
            )
            resolved_chat = resolve_download_file(
                config,
                "LIVEVIDEO01",
                chat_file.name,
            )
            rejected_format = resolve_download_file(
                config,
                "LIVEVIDEO01",
                format_file.name,
            )
            rejected_unknown = resolve_download_file(
                config,
                "UNKNOWNID01",
                final_file.name,
            )
            rejected_traversal = resolve_download_file(
                config,
                "LIVEVIDEO01",
                "../config.toml",
            )

        self.assertEqual(resolved, final_file.resolve())
        self.assertEqual(resolved_chat, chat_file.resolve())
        self.assertIsNone(rejected_part)
        self.assertIsNone(rejected_format)
        self.assertIsNone(rejected_unknown)
        self.assertIsNone(rejected_traversal)

    def test_ended_streams_are_collapsed_by_default(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Live Status",
                channel="Example Channel",
            )
            state = StateStore(config.db_path)
            state.mark_downloading(stream, 1)
            state.mark_ended(stream.video_id)
            state.close()

            snapshot = build_status_snapshot(config)
            html = render_status_html(snapshot)

        self.assertIn('class="stream collapsed"', html)
        self.assertIn('data-stream-status="ended"', html)
        self.assertIn('aria-expanded="false"', html)
        self.assertIn(">Expand</button>", html)

    def test_chat_render_action_requires_matching_final_media(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Live Status",
                channel="Example Channel",
            )
            state = StateStore(config.db_path)
            state.mark_downloading(stream, 1)
            state.mark_ended(stream.video_id)
            state.close()

            segment_dir = config.download_dir / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            media_file = segment_dir / "Live Status [LIVEVIDEO01].mp4"
            chat_file = segment_dir / "Live Status [LIVEVIDEO01].live_chat.json"
            output_file = segment_dir / "Live Status [LIVEVIDEO01] - chat.mp4"
            format_file = segment_dir / "segment-001.f136.mp4"
            media_file.write_text("media", encoding="utf-8")
            chat_file.write_text("chat", encoding="utf-8")
            output_file.write_text("rendered", encoding="utf-8")
            format_file.write_text("video only", encoding="utf-8")

            media_match = chat_media_file_for_chat_file(
                segment_dir,
                chat_file.name,
            )
            ignored_format = chat_media_file_for_chat_file(
                segment_dir,
                "segment-001.live_chat.json",
            )
            resolved = resolve_render_chat_files(
                config,
                "LIVEVIDEO01",
                chat_file.name,
            )
            snapshot = build_status_snapshot(config)

        self.assertEqual(media_match, media_file)
        self.assertIsNone(ignored_format)
        self.assertEqual(resolved, (media_file.resolve(), chat_file.resolve(), output_file.resolve()))
        chat_status = next(
            file
            for file in snapshot.streams[0].files
            if file.name == chat_file.name
        )
        self.assertEqual(chat_status.render_chat_status, "rendered")
        self.assertIsNotNone(chat_status.render_chat_url)
        self.assertIn("regenerate=1", chat_status.render_chat_url or "")
        self.assertIsNone(chat_status.render_chat_output_url)
        action = render_file_action(chat_status)
        self.assertIn("Regenerate chat video", action)
        self.assertIn("Re-render and replace the existing chat video", action)
        self.assertNotIn("Chat video", action)

    def test_non_youtube_streams_do_not_offer_youtube_chat_actions(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                record_live_chat=True,
                render_live_chat_video=True,
            )
            stream = LiveStream(
                video_id="twitch:OUMB3rd",
                url="https://www.twitch.tv/OUMB3rd",
                title="Live on Twitch",
                channel="OUMB3rd",
                platform="twitch",
                source="twitch:OUMB3rd",
            )
            state = StateStore(config.db_path)
            state.mark_downloading(stream, 1)
            state.mark_ended(stream.video_id)
            state.close()

            segment_dir = config.download_dir / "OUMB3rd" / "twitch_OUMB3rd"
            segment_dir.mkdir(parents=True)
            media_file = segment_dir / "Live on Twitch [twitch_OUMB3rd].mp4"
            chat_file = segment_dir / "Live on Twitch [twitch_OUMB3rd].live_chat.json"
            media_file.write_text("media", encoding="utf-8")
            chat_file.write_text("chat", encoding="utf-8")

            snapshot = build_status_snapshot(config)
            html = render_status_html(snapshot)

        chat_status = next(
            file
            for file in snapshot.streams[0].files
            if file.name == chat_file.name
        )
        self.assertIsNone(chat_status.render_chat_url)
        self.assertIsNone(chat_status.refresh_chat_url)
        self.assertIn("twitch", snapshot_to_dict(snapshot)["streams"][0]["platform"])
        self.assertIn('class="stream-title-block"', html)
        self.assertIn('source-platform-icon twitch', html)
        self.assertIn('twitch:OUMB3rd', html)
        self.assertIn('renderStreamSourceMeta', html)

    def test_chat_refresh_action_is_available_for_finalized_chat(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Live Status",
                channel="Example Channel",
            )
            state = StateStore(config.db_path)
            state.mark_downloading(stream, 1)
            state.mark_ended(stream.video_id)
            state.close()

            segment_dir = config.download_dir / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            media_file = segment_dir / "Live Status [LIVEVIDEO01].mp4"
            chat_file = segment_dir / "Live Status [LIVEVIDEO01].live_chat.json"
            output_file = segment_dir / "Live Status [LIVEVIDEO01] - chat.mp4"
            media_file.write_text("media", encoding="utf-8")
            chat_file.write_text("chat", encoding="utf-8")
            output_file.write_text("rendered", encoding="utf-8")

            snapshot = build_status_snapshot(config)
            resolved_bad = resolve_refresh_chat_files(
                config,
                "LIVEVIDEO01",
                "../Live Status [LIVEVIDEO01].live_chat.json",
            )
            unmatched_chat = segment_dir / "Different [LIVEVIDEO01].live_chat.json"
            unmatched_chat.write_text("chat", encoding="utf-8")
            resolved_unmatched = resolve_refresh_chat_files(
                config,
                "LIVEVIDEO01",
                unmatched_chat.name,
            )
            rendered_text = output_file.read_text(encoding="utf-8")

        chat_status = next(
            file
            for file in snapshot.streams[0].files
            if file.name == chat_file.name
        )
        self.assertIsNone(resolved_bad)
        self.assertIsNone(resolved_unmatched)
        self.assertIsNotNone(chat_status.refresh_chat_url)
        self.assertIn("/refresh-chat?", chat_status.refresh_chat_url or "")
        action = render_file_action(chat_status)
        self.assertIn("Refresh chat", action)
        self.assertIn("Regenerate chat video", action)
        self.assertEqual(rendered_text, "rendered")

    def test_chat_refresh_action_is_hidden_until_stream_is_ended(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Live Status",
                channel="Example Channel",
            )
            state = StateStore(config.db_path)
            state.mark_downloading(stream, 1)
            state.close()

            segment_dir = config.download_dir / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            media_file = segment_dir / "Live Status [LIVEVIDEO01].mp4"
            chat_file = segment_dir / "Live Status [LIVEVIDEO01].live_chat.json"
            media_file.write_text("media", encoding="utf-8")
            chat_file.write_text("chat", encoding="utf-8")

            snapshot = build_status_snapshot(config)
            resolved = resolve_refresh_chat_files(config, "LIVEVIDEO01", chat_file.name)

        chat_status = next(
            file
            for file in snapshot.streams[0].files
            if file.name == chat_file.name
        )
        self.assertIsNone(resolved)
        self.assertIsNone(chat_status.refresh_chat_url)

    def test_chat_refresh_action_shows_running_state(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Live Status",
                channel="Example Channel",
            )
            state = StateStore(config.db_path)
            state.mark_downloading(stream, 1)
            state.mark_ended(stream.video_id)
            state.close()

            segment_dir = config.download_dir / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            media_file = segment_dir / "Live Status [LIVEVIDEO01].mp4"
            chat_file = segment_dir / "Live Status [LIVEVIDEO01].live_chat.json"
            media_file.write_text("media", encoding="utf-8")
            chat_file.write_text("chat", encoding="utf-8")
            key = refresh_chat_job_key("LIVEVIDEO01", chat_file.name)
            with CHAT_REFRESH_JOBS_LOCK:
                CHAT_REFRESH_JOBS[key] = RefreshChatJob(
                    video_id="LIVEVIDEO01",
                    chat_name=chat_file.name,
                    media_name=media_file.name,
                    status="running",
                    message="Refreshing chat",
                    started_at=0,
                )

            try:
                snapshot = build_status_snapshot(config)
            finally:
                with CHAT_REFRESH_JOBS_LOCK:
                    CHAT_REFRESH_JOBS.pop(key, None)

        chat_status = next(
            file
            for file in snapshot.streams[0].files
            if file.name == chat_file.name
        )
        self.assertEqual(chat_status.refresh_chat_status, "running")
        self.assertIn("Refreshing chat", render_file_action(chat_status))

    def test_manual_chat_refresh_does_not_overwrite_rendered_chat_video(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Live Status",
                channel="Example Channel",
            )
            state = StateStore(config.db_path)
            state.mark_downloading(stream, 1)
            state.mark_exited(stream.video_id, 0)
            state.mark_ended(stream.video_id)
            record = state.get_stream(stream.video_id)
            state.close()
            assert record is not None

            segment_dir = config.download_dir / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            media_file = segment_dir / "Live Status [LIVEVIDEO01].mp4"
            chat_file = segment_dir / "Live Status [LIVEVIDEO01].live_chat.json"
            output_file = segment_dir / "Live Status [LIVEVIDEO01] - chat.mp4"
            media_file.write_text("media", encoding="utf-8")
            chat_file.write_text("old chat", encoding="utf-8")
            output_file.write_text("rendered", encoding="utf-8")
            key = refresh_chat_job_key(stream.video_id, chat_file.name)
            with CHAT_REFRESH_JOBS_LOCK:
                CHAT_REFRESH_JOBS[key] = RefreshChatJob(
                    video_id=stream.video_id,
                    chat_name=chat_file.name,
                    media_name=media_file.name,
                    status="running",
                    message="Refreshing chat",
                    started_at=0,
                )

            def fake_refresh(*_args: object, **_kwargs: object) -> ChatRefreshResult:
                chat_file.write_text("new chat", encoding="utf-8")
                return ChatRefreshResult(
                    ok=True,
                    changed=True,
                    source="replay",
                    message="Refreshed",
                )

            try:
                with patch("onlysavemevods.web.refresh_chat_sidecar", fake_refresh):
                    run_refresh_chat_job(config, key, record, media_file, chat_file)
                chat_text = chat_file.read_text(encoding="utf-8")
                output_text = output_file.read_text(encoding="utf-8")
            finally:
                with CHAT_REFRESH_JOBS_LOCK:
                    CHAT_REFRESH_JOBS.pop(key, None)

        self.assertEqual(chat_text, "new chat")
        self.assertEqual(output_text, "rendered")


    def test_app_config_form_renders_editable_settings_without_sensitive_args(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                'channels = ["@Example"]\n'
                'extra_yt_dlp_args = ["--cookies", "/secret/cookies.txt", "--format", "best"]\n',
                encoding="utf-8",
            )
            config = load_config(config_path)

            html = render_status_html(build_status_snapshot(config))

        self.assertIn('action="/config"', html)
        self.assertIn('name="channels"', html)
        self.assertIn('name="download_dir"', html)
        self.assertIn('name="web_port"', html)
        self.assertIn('name="chat_render_timeout_seconds"', html)
        self.assertIn('name="watermark_strength"', html)
        self.assertIn('name="whisperx_language" type="text" value=""', html)
        self.assertIn('name="extra_yt_dlp_args_mode"', html)
        self.assertIn('value="keep" selected', html)
        self.assertIn("Save App Settings", html)
        self.assertIn("&lt;redacted&gt;", html)
        self.assertNotIn("/secret/cookies.txt", html)

    def test_streamer_group_form_updates_file_and_running_config(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text("channels = []\n", encoding="utf-8")
            config = load_config(config_path)

            update_streamer_from_form(
                config,
                {
                    "action": ["save"],
                    "streamer_name": ["OUMB3rd"],
                    "sources": ["@OUMB3rd\n@OUMB3rdVODS"],
                    "download_dir_name": ["OUMB3rd Shared"],
                },
            )
            created = load_config(config_path)
            html = render_status_html(build_status_snapshot(config))
            update_streamer_from_form(
                config,
                {
                    "action": ["delete"],
                    "streamer_name": ["OUMB3rd"],
                },
            )
            removed = load_config(config_path)

        self.assertEqual(created.streamers["OUMB3rd"].sources, ["@OUMB3rd", "@OUMB3rdVODS"])
        self.assertEqual(created.streamers["OUMB3rd"].download_dir_name, "OUMB3rd Shared")
        self.assertEqual(config.streamers, {})
        self.assertEqual(removed.streamers, {})
        self.assertIn('action="/streamers"', html)
        self.assertIn("Streamers", html)
        self.assertIn("Save Streamer", html)

    def test_streamer_group_form_accepts_platform_urls(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text("channels = []\n", encoding="utf-8")
            config = load_config(config_path)

            update_streamer_from_form(
                config,
                {
                    "action": ["save"],
                    "streamer_name": ["OUMB"],
                    "sources": ["https://kick.com/oumb\nhttps://rumble.com/user/OUMB2"],
                    "download_dir_name": [""],
                },
            )
            updated = load_config(config_path)

        self.assertEqual(
            updated.streamers["OUMB"].sources,
            ["kick:oumb", "rumble:user/OUMB2"],
        )
        self.assertEqual(config.streamers["OUMB"].sources, updated.streamers["OUMB"].sources)

    def test_streamer_wizard_form_creates_full_config_and_rewrites_subtitles(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.toml"
            config_path.write_text(
                'download_dir = "downloads"\n'
                'state_dir = "state"\n',
                encoding="utf-8",
            )
            config = load_config(config_path)
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Live Status",
                channel="Example Channel",
            )
            state = StateStore(config.db_path)
            state.upsert_detected(stream)
            state.mark_ended(stream.video_id)
            state.close()
            segment_dir = config.download_dir / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            media_file = segment_dir / "Live Status [LIVEVIDEO01].mp4"
            media_file.write_text("media", encoding="utf-8")
            media_file.with_suffix(".json").write_text(
                json.dumps(
                    {
                        "segments": [
                            {
                                "start": 0.0,
                                "end": 1.0,
                                "text": "hello",
                                "speaker": "SPEAKER_00",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            media_file.with_suffix(".srt").write_text("old", encoding="utf-8")
            media_file.with_suffix(".vtt").write_text("old", encoding="utf-8")

            update_streamer_from_form(
                config,
                {
                    "form_kind": ["streamer_wizard"],
                    "action": ["save"],
                    "streamer_name": ["Example Channel"],
                    "sources": ["@ExampleChannel"],
                    "download_dir_name": [""],
                    "mode": ["fixed"],
                    "speakers": ["2"],
                    "speaker_label": ["SPEAKER_00", ""],
                    "speaker_name": ["OUMB3rd", ""],
                },
            )
            updated = load_config(config_path)
            srt_text = media_file.with_suffix(".srt").read_text(encoding="utf-8")

        streamer = updated.streamers["Example Channel"]
        self.assertEqual(streamer.sources, ["@ExampleChannel"])
        self.assertEqual(streamer.download_dir_name, "")
        self.assertIsNotNone(streamer.voice_detection)
        assert streamer.voice_detection is not None
        self.assertEqual(streamer.voice_detection.mode, "fixed")
        self.assertEqual(streamer.voice_detection.min_speakers, 2)
        self.assertEqual(streamer.speaker_labels, {"SPEAKER_00": "OUMB3rd"})
        self.assertEqual(config.streamers["Example Channel"].speaker_labels, {"SPEAKER_00": "OUMB3rd"})
        self.assertIn("OUMB3rd: hello", srt_text)

    def test_app_config_form_updates_file_and_running_config(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                'channels = ["@Old"]\n'
                'extra_yt_dlp_args = ["--cookies", "/secret/cookies.txt"]\n',
                encoding="utf-8",
            )
            config = load_config(config_path)

            update_app_config_from_form(
                config,
                app_config_form_params(
                    channels="@New\n@Second",
                    poll_interval_seconds="45",
                    max_concurrent_downloads="2",
                    record_live_chat="true",
                    render_live_chat_video="true",
                    chat_render_timeout_seconds="7200",
                    chat_render_use_nvenc="true",
                    chat_render_nvenc_devices="0\n1",
                    transcribe_subtitles="true",
                    whisperx_language="en",
                    web_port="9090",
                    log_level="DEBUG",
                    watermark_enabled="true",
                    watermark_secret_env="TEST_WATERMARK_SECRET",
                    watermark_strength="balanced",
                    watermark_detect_upload_max_bytes="123456",
                    extra_yt_dlp_args_mode="keep",
                ),
            )
            updated = load_config(config_path)

        self.assertEqual(updated.channels, ["@New", "@Second"])
        self.assertEqual(updated.poll_interval_seconds, 45)
        self.assertEqual(updated.max_concurrent_downloads, 2)
        self.assertTrue(updated.record_live_chat)
        self.assertTrue(updated.render_live_chat_video)
        self.assertEqual(updated.chat_render_timeout_seconds, 7200)
        self.assertTrue(updated.chat_render_use_nvenc)
        self.assertEqual(updated.chat_render_nvenc_devices, ["0", "1"])
        self.assertTrue(updated.transcribe_subtitles)
        self.assertEqual(updated.whisperx_language, "en")
        self.assertEqual(updated.web_port, 9090)
        self.assertEqual(updated.log_level, "DEBUG")
        self.assertTrue(updated.watermark_enabled)
        self.assertEqual(updated.watermark_secret_env, "TEST_WATERMARK_SECRET")
        self.assertEqual(updated.watermark_strength, "balanced")
        self.assertEqual(updated.watermark_detect_upload_max_bytes, 123456)
        self.assertEqual(updated.extra_yt_dlp_args, ["--cookies", "/secret/cookies.txt"])
        self.assertEqual(config.channels, ["@New", "@Second"])
        self.assertEqual(config.web_port, 9090)
        self.assertEqual(config.extra_yt_dlp_args, ["--cookies", "/secret/cookies.txt"])

    def test_app_config_form_can_replace_or_clear_extra_yt_dlp_args(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                'extra_yt_dlp_args = ["--cookies", "/secret/cookies.txt"]\n',
                encoding="utf-8",
            )
            config = load_config(config_path)

            update_app_config_from_form(
                config,
                app_config_form_params(
                    extra_yt_dlp_args_mode="replace",
                    extra_yt_dlp_args="--format\nbestvideo+bestaudio/best",
                ),
            )
            replaced = load_config(config_path)
            update_app_config_from_form(
                config,
                app_config_form_params(extra_yt_dlp_args_mode="clear"),
            )
            cleared = load_config(config_path)

        self.assertEqual(
            replaced.extra_yt_dlp_args,
            ["--format", "bestvideo+bestaudio/best"],
        )
        self.assertEqual(cleared.extra_yt_dlp_args, [])
        self.assertEqual(config.extra_yt_dlp_args, [])

    def test_app_config_form_rejects_invalid_values_without_writing(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text('web_port = 8080\n', encoding="utf-8")
            original = config_path.read_text(encoding="utf-8")
            config = load_config(config_path)

            with self.assertRaisesRegex(ConfigError, "web_port"):
                update_app_config_from_form(
                    config,
                    app_config_form_params(web_port="70000"),
                )

            unchanged = config_path.read_text(encoding="utf-8")

        self.assertEqual(unchanged, original)
        self.assertEqual(config.web_port, 8080)

    def test_voice_detection_panel_renders_global_and_channel_forms(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                channels=["@ExampleChannel"],
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                channel_voice_detection={
                    "Example Channel": VoiceDetectionConfig(
                        mode="fixed",
                        min_speakers=2,
                        max_speakers=2,
                    )
                },
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Live Status",
                channel="Example Channel",
            )
            state = StateStore(config.db_path)
            state.mark_downloading(stream, 1)
            state.close()

            html = render_status_html(build_status_snapshot(config))

        self.assertIn('action="/voice-detection"', html)
        self.assertIn('class="voice-table"', html)
        self.assertNotIn('config-table voice-table', html)
        self.assertIn('name="scope" value="global"', html)
        self.assertIn('name="scope" value="channel"', html)
        self.assertIn("fixed, exactly 2", html)

    def test_voice_detection_form_updates_running_config(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text('whisperx_diarize = true\n', encoding="utf-8")
            config = load_config(config_path)

            update_voice_detection_from_form(
                config,
                {
                    "scope": ["global"],
                    "mode": ["fixed"],
                    "speakers": ["2"],
                },
            )
            update_voice_detection_from_form(
                config,
                {
                    "scope": ["channel"],
                    "channel": ["Example Channel"],
                    "mode": ["off"],
                },
            )

        self.assertTrue(config.whisperx_diarize)
        self.assertEqual(config.whisperx_min_speakers, 2)
        self.assertEqual(config.whisperx_max_speakers, 2)
        self.assertEqual(config.channel_voice_detection["Example Channel"].mode, "off")

    def test_voice_detection_form_updates_streamer_shared_override(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                '[streamers."OUMB3rd"]\n'
                'sources = ["@ExampleChannel"]\n',
                encoding="utf-8",
            )
            config = load_config(config_path)

            update_voice_detection_from_form(
                config,
                {
                    "scope": ["channel"],
                    "channel": ["OUMB3rd"],
                    "mode": ["fixed"],
                    "speakers": ["2"],
                },
            )
            updated = load_config(config_path)

        self.assertEqual(updated.channel_voice_detection, {})
        self.assertIsNotNone(updated.streamers["OUMB3rd"].voice_detection)
        assert updated.streamers["OUMB3rd"].voice_detection is not None
        self.assertEqual(updated.streamers["OUMB3rd"].voice_detection.mode, "fixed")
        self.assertEqual(updated.streamers["OUMB3rd"].voice_detection.min_speakers, 2)

    def test_streamer_voice_form_updates_profile_config(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                '[streamers."OUMB3rd"]\n'
                'sources = ["@OUMB3rd"]\n',
                encoding="utf-8",
            )
            config = load_config(config_path)

            update_streamer_voice_from_form(
                config,
                {
                    "streamer_name": ["OUMB3rd"],
                    "voice_name": ["Host"],
                    "enabled": ["false"],
                    "threshold": ["0.2"],
                    "samples": ["host.wav\nsecond.wav"],
                    "notes": ["main voice"],
                    "action": ["save"],
                },
            )
            updated = load_config(config_path)

        profile = updated.streamers["OUMB3rd"].voices["Host"]
        self.assertFalse(profile.enabled)
        self.assertEqual(profile.threshold, 0.2)
        self.assertEqual(profile.samples, ["host.wav", "second.wav"])
        self.assertEqual(profile.notes, "main voice")
        self.assertIn("Host", config.streamers["OUMB3rd"].voices)

    def test_streamer_voice_upload_stores_sample_and_updates_config(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.toml"
            config_path.write_text(
                f'state_dir = "{(root / "state").as_posix()}"\n'
                '[streamers."OUMB3rd"]\n'
                'sources = ["@OUMB3rd"]\n',
                encoding="utf-8",
            )
            config = load_config(config_path)

            store_streamer_voice_sample_upload(
                config,
                {"streamer_name": ["OUMB3rd"], "voice_name": ["Host"]},
                {"media": ("../Host Voice!!.mp3", b"sample-data")},
            )
            updated = load_config(config_path)
            sample_path = (
                updated.state_dir
                / "voice_samples"
                / "OUMB3rd"
                / "Host"
                / "Host_Voice.mp3"
            )
            sample_exists = sample_path.is_file()
            sample_bytes = sample_path.read_bytes()

        self.assertTrue(sample_exists)
        self.assertEqual(sample_bytes, b"sample-data")
        self.assertEqual(updated.streamers["OUMB3rd"].voices["Host"].samples, ["Host_Voice.mp3"])
        self.assertEqual(config.streamers["OUMB3rd"].voices["Host"].samples, ["Host_Voice.mp3"])

    def test_streamer_voice_add_can_include_optional_sample_upload(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.toml"
            config_path.write_text(
                f'state_dir = "{(root / "state").as_posix()}"\n'
                '[streamers."OUMB3rd"]\n'
                'sources = ["@OUMB3rd"]\n',
                encoding="utf-8",
            )
            config = load_config(config_path)

            update_streamer_voice_with_optional_sample(
                config,
                {
                    "streamer_name": ["OUMB3rd"],
                    "voice_name": ["Blade"],
                    "enabled": ["true"],
                    "threshold": ["0.25"],
                    "notes": ["first pass"],
                    "action": ["save"],
                },
                {"media": ("Blade Intro.wav", b"sample-data")},
            )
            updated = load_config(config_path)
            profile = updated.streamers["OUMB3rd"].voices["Blade"]
            sample_path = (
                updated.state_dir
                / "voice_samples"
                / "OUMB3rd"
                / "Blade"
                / "Blade_Intro.wav"
            )
            sample_bytes = sample_path.read_bytes()

        self.assertTrue(profile.enabled)
        self.assertEqual(profile.threshold, 0.25)
        self.assertEqual(profile.notes, "first pass")
        self.assertEqual(profile.samples, ["Blade_Intro.wav"])
        self.assertEqual(sample_bytes, b"sample-data")

    def test_streamer_voice_sample_can_be_created_from_transcript_speaker(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.toml"
            config_path.write_text(
                'download_dir = "downloads"\n'
                'state_dir = "state"\n'
                '[streamers."OUMB3rd"]\n'
                'sources = ["@OUMB3rd"]\n',
                encoding="utf-8",
            )
            config = load_config(config_path)
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Live Status",
                channel="OUMB3rd",
            )
            state = StateStore(config.db_path)
            state.mark_downloading(stream, 1)
            state.close()
            segment_dir = config.download_dir / "OUMB3rd" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            media_file = segment_dir / "Live Status [LIVEVIDEO01].mp4"
            media_file.write_text("media", encoding="utf-8")
            media_file.with_suffix(".json").write_text(
                json.dumps(
                    {
                        "segments": [
                            {
                                "start": 1.0,
                                "end": 3.0,
                                "speaker": "SPEAKER_00",
                                "text": "sample me",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            create_streamer_voice_sample_from_transcript_form(
                config,
                {
                    "streamer_name": ["OUMB3rd"],
                    "voice_name": ["Host"],
                    "video_id": ["LIVEVIDEO01"],
                    "media_name": ["Live Status [LIVEVIDEO01].mp4"],
                    "speaker_label": ["SPEAKER_00"],
                },
            )
            updated = load_config(config_path)
            samples = updated.streamers["OUMB3rd"].voices["Host"].samples
            sample_path = updated.state_dir / "voice_samples" / "OUMB3rd" / "Host" / samples[0]
            payload = json.loads(sample_path.read_text(encoding="utf-8"))

        self.assertEqual(len(samples), 1)
        self.assertEqual(payload["kind"], "transcript-segments")
        self.assertEqual(payload["speaker_label"], "SPEAKER_00")
        self.assertEqual(payload["ranges"], [[1.0, 3.0]])

    def test_voice_detection_form_ignores_irrelevant_prefilled_values(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                "whisperx_diarize = true\n"
                "whisperx_min_speakers = 2\n"
                "whisperx_max_speakers = 2\n",
                encoding="utf-8",
            )
            config = load_config(config_path)

            update_voice_detection_from_form(
                config,
                {
                    "scope": ["global"],
                    "mode": ["auto"],
                    "speakers": ["2"],
                    "min_speakers": ["2"],
                    "max_speakers": ["2"],
                },
            )

        self.assertTrue(config.whisperx_diarize)
        self.assertEqual(config.whisperx_min_speakers, 0)
        self.assertEqual(config.whisperx_max_speakers, 0)

    def test_voice_detection_form_updates_channel_override(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text('channels = ["@Example"]\n', encoding="utf-8")
            config = load_config(config_path)

            update_voice_detection_from_form(
                config,
                {
                    "scope": ["channel"],
                    "channel": ["Example Channel"],
                    "mode": ["range"],
                    "min_speakers": ["2"],
                    "max_speakers": ["4"],
                },
            )
            updated = load_config(config_path)

        override = updated.channel_voice_detection["Example Channel"]
        self.assertEqual(override.mode, "range")
        self.assertEqual(override.min_speakers, 2)
        self.assertEqual(override.max_speakers, 4)

    def test_speaker_labels_panel_detects_transcript_speakers(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                channels=["@ExampleChannel"],
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                channel_speaker_labels={"Example Channel": {"SPEAKER_00": "OUMB3rd"}},
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Live Status",
                channel="Example Channel",
            )
            state = StateStore(config.db_path)
            state.upsert_detected(stream)
            state.mark_ended(stream.video_id)
            state.close()
            segment_dir = config.download_dir / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            (segment_dir / "Live Status [LIVEVIDEO01].json").write_text(
                json.dumps(
                    {
                        "segments": [
                            {
                                "start": 0.0,
                                "end": 1.0,
                                "text": "hello",
                                "speaker": "SPEAKER_00",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            html = render_status_html(build_status_snapshot(config))

        self.assertIn('action="/speaker-labels"', html)
        self.assertIn("Speaker Names", html)
        self.assertIn("SPEAKER_00", html)
        self.assertIn("OUMB3rd", html)

    def test_speaker_labels_form_updates_config_and_existing_subtitles(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.toml"
            config_path.write_text(
                'channels = ["@ExampleChannel"]\n'
                'download_dir = "downloads"\n'
                'state_dir = "state"\n',
                encoding="utf-8",
            )
            config = load_config(config_path)
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Live Status",
                channel="Example Channel",
            )
            state = StateStore(config.db_path)
            state.upsert_detected(stream)
            state.mark_ended(stream.video_id)
            state.close()
            segment_dir = config.download_dir / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            media_file = segment_dir / "Live Status [LIVEVIDEO01].mp4"
            media_file.write_text("media", encoding="utf-8")
            media_file.with_suffix(".json").write_text(
                json.dumps(
                    {
                        "segments": [
                            {
                                "start": 0.0,
                                "end": 1.0,
                                "text": "hello",
                                "speaker": "SPEAKER_00",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            media_file.with_suffix(".srt").write_text("old", encoding="utf-8")
            media_file.with_suffix(".vtt").write_text("old", encoding="utf-8")

            update_speaker_labels_from_form(
                config,
                {
                    "channel": ["Example Channel"],
                    "speaker_label": ["SPEAKER_00", ""],
                    "speaker_name": ["OUMB3rd", ""],
                },
            )
            updated = load_config(config_path)
            srt_text = media_file.with_suffix(".srt").read_text(encoding="utf-8")

        self.assertEqual(
            updated.channel_speaker_labels["Example Channel"],
            {"SPEAKER_00": "OUMB3rd"},
        )
        self.assertIn("OUMB3rd: hello", srt_text)

    def test_speaker_labels_form_updates_streamer_shared_config(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.toml"
            config_path.write_text(
                'download_dir = "downloads"\n'
                'state_dir = "state"\n'
                '[streamers."OUMB3rd"]\n'
                'sources = ["@ExampleChannel"]\n',
                encoding="utf-8",
            )
            config = load_config(config_path)
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Live Status",
                channel="Example Channel",
            )
            state = StateStore(config.db_path)
            state.upsert_detected(stream)
            state.mark_ended(stream.video_id)
            state.close()
            segment_dir = config.download_dir / "OUMB3rd" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            media_file = segment_dir / "Live Status [LIVEVIDEO01].mp4"
            media_file.write_text("media", encoding="utf-8")
            media_file.with_suffix(".json").write_text(
                json.dumps(
                    {
                        "segments": [
                            {
                                "start": 0.0,
                                "end": 1.0,
                                "text": "hello",
                                "speaker": "SPEAKER_00",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            media_file.with_suffix(".srt").write_text("old", encoding="utf-8")
            media_file.with_suffix(".vtt").write_text("old", encoding="utf-8")

            update_speaker_labels_from_form(
                config,
                {
                    "channel": ["OUMB3rd"],
                    "speaker_label": ["SPEAKER_00"],
                    "speaker_name": ["OUMB3rd"],
                },
            )
            updated = load_config(config_path)
            srt_text = media_file.with_suffix(".srt").read_text(encoding="utf-8")

        self.assertEqual(updated.channel_speaker_labels, {})
        self.assertEqual(
            updated.streamers["OUMB3rd"].speaker_labels,
            {"SPEAKER_00": "OUMB3rd"},
        )
        self.assertIn("OUMB3rd: hello", srt_text)

    def test_transcription_action_can_retranscribe_existing_sidecars(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Live Status",
                channel="Example Channel",
            )
            state = StateStore(config.db_path)
            state.mark_downloading(stream, 1)
            state.mark_ended(stream.video_id)
            state.close()

            segment_dir = config.download_dir / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            media_file = segment_dir / "Live Status [LIVEVIDEO01].mp4"
            media_file.write_text("media", encoding="utf-8")
            media_file.with_suffix(".srt").write_text("subtitle", encoding="utf-8")
            media_file.with_suffix(".vtt").write_text("subtitle", encoding="utf-8")
            media_path = media_file.resolve()

            resolved = resolve_transcription_source_file(
                config,
                "LIVEVIDEO01",
                media_file.name,
            )
            snapshot = build_status_snapshot(config)

        self.assertIsNotNone(resolved)
        assert resolved is not None
        self.assertEqual(resolved[1], media_path)
        media_status = next(
            file
            for file in snapshot.streams[0].files
            if file.name == media_file.name
        )
        self.assertEqual(media_status.transcription_status, "transcribed")
        self.assertIsNotNone(media_status.transcription_url)
        self.assertIn("regenerate=1", media_status.transcription_url or "")
        action = render_file_action(media_status)
        self.assertIn("Retranscribe", action)
        self.assertIn("Run WhisperX again", action)

    def test_manual_transcription_job_passes_overwrite_for_retranscribe(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            media_file = config.download_dir / "Live Status [LIVEVIDEO01].mp4"
            media_file.parent.mkdir(parents=True)
            media_file.write_text("media", encoding="utf-8")
            key = transcription_job_key("LIVEVIDEO01", media_file.name)
            with TRANSCRIPTION_JOBS_LOCK:
                TRANSCRIPTION_JOBS[key] = TranscriptionJob(
                    video_id="LIVEVIDEO01",
                    media_name=media_file.name,
                    status="running",
                    message="Retranscribing subtitles",
                    started_at=0.0,
                )
            transcribe = AsyncMock(return_value=True)

            try:
                with patch("onlysavemevods.web.transcribe_media_file", transcribe):
                    run_transcription_job(config, key, media_file, regenerate=True)
                with TRANSCRIPTION_JOBS_LOCK:
                    job = TRANSCRIPTION_JOBS[key]
            finally:
                with TRANSCRIPTION_JOBS_LOCK:
                    TRANSCRIPTION_JOBS.pop(key, None)

        self.assertEqual(job.status, "done")
        transcribe.assert_awaited_once()
        self.assertEqual(transcribe.await_args.args[0], config)
        self.assertEqual(transcribe.await_args.args[1], media_file)
        self.assertTrue(transcribe.await_args.kwargs["overwrite"])

    def test_watermark_status_and_downloads_are_separate_from_originals(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                watermark_enabled=True,
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Live Status",
                channel="Example Channel",
            )
            state = StateStore(config.db_path)
            state.mark_downloading(stream, 1)
            state.mark_ended(stream.video_id)
            segment_dir = config.download_dir / "Example_Channel" / "LIVEVIDEO01"
            watermark_dir = segment_dir / ".watermarks"
            watermark_dir.mkdir(parents=True)
            media_file = segment_dir / "Live Status [LIVEVIDEO01].mp4"
            output_name = ".watermarks/Live Status [LIVEVIDEO01] - wm-copy001.mp4"
            output_file = segment_dir / output_name
            media_file.write_text("media", encoding="utf-8")
            output_file.write_text("watermarked", encoding="utf-8")
            state.create_watermark_copy(
                copy_id="wm_copy001",
                video_id=stream.video_id,
                source_name=media_file.name,
                output_name=output_name,
                recipient_label="Recipient A",
            )
            state.update_watermark_copy(
                "wm_copy001",
                status="done",
                message="Completed",
                finished=True,
            )
            state.close()

            with patch.dict("os.environ", {"ONLYSAVEMEVODS_WATERMARK_SECRET": "secret"}):
                snapshot = build_status_snapshot(config)
                html = render_status_html(snapshot)
            resolved = resolve_watermark_download_file(config, "wm_copy001")

        self.assertTrue(is_watermarkable_media_file(media_file.name))
        self.assertFalse(is_watermarkable_media_file("Live Status [LIVEVIDEO01].live_chat.json"))
        self.assertFalse(is_watermarkable_media_file("Live Status [LIVEVIDEO01] - chat.rendering.mp4"))
        stream_status = snapshot.streams[0]
        final_file = next(file for file in stream_status.files if file.name == media_file.name)
        self.assertIsNotNone(final_file.watermark_url)
        self.assertEqual(final_file.watermark_copies[0].recipient_label, "Recipient A")
        self.assertIn("/download-watermark?", final_file.watermark_copies[0].download_url or "")
        self.assertEqual(resolved, output_file.resolve())
        self.assertIn("Watermark", html)
        self.assertIn("Recipient A", html)
        self.assertIn("/download-watermark?", html)

    def test_file_kind_and_byte_formatting(self) -> None:
        self.assertEqual(file_kind("segment-001.mp4"), "final")
        self.assertEqual(file_kind("segment-001.mp4.part"), "part")
        self.assertEqual(file_kind("segment-001.mp4.ytdl"), "state")
        self.assertEqual(file_kind("segment-001.mp4.part-Frag1"), "fragment")
        self.assertEqual(file_kind("segment-001.live_chat.json"), "chat")
        self.assertEqual(file_kind("segment-001.timing.json"), "state")
        self.assertEqual(file_kind("Live Status [LIVEVIDEO01] - chat.rendering.mp4"), "temporary")
        self.assertEqual(format_bytes(1536), "1.5 KiB")

    def test_config_summary_redacts_sensitive_extra_args(self) -> None:
        config = BotConfig(
            extra_yt_dlp_args=[
                "--cookies",
                "/secret/cookies.txt",
                "--add-header=Authorization: Bearer secret-token",
                "--format",
                "bestvideo+bestaudio/best",
            ],
        )

        summary = build_config_summary(config)
        rendered = json.dumps(summary)

        self.assertIn("<redacted>", rendered)
        self.assertIn("--format", rendered)
        self.assertIn("bestvideo+bestaudio/best", rendered)
        self.assertNotIn("/secret/cookies.txt", rendered)
        self.assertNotIn("secret-token", rendered)

    def test_config_summary_labels_configured_nvenc_devices(self) -> None:
        config = BotConfig(chat_render_nvenc_devices=["0", "1", "2"])

        with patch(
            "onlysavemevods.web.detect_nvidia_devices",
            return_value=["0: NVIDIA A2", "1: NVIDIA A2"],
        ):
            summary = build_config_summary(config)

        self.assertEqual(
            summary["Live Chat"]["chat_render_nvenc_devices"],
            ["0: NVIDIA A2", "1: NVIDIA A2", "2: not detected"],
        )

    def test_config_summary_redacts_watermark_secret_value(self) -> None:
        config = BotConfig(
            watermark_enabled=True,
            watermark_secret_env="TEST_WATERMARK_SECRET",
        )

        with patch.dict("os.environ", {"TEST_WATERMARK_SECRET": "super-secret"}):
            summary = build_config_summary(config)

        rendered = json.dumps(summary)
        self.assertTrue(summary["Watermark"]["watermark_secret_configured"])
        self.assertNotIn("super-secret", rendered)

    def test_web_server_uses_configured_bind_address(self) -> None:
        config = BotConfig(web_host="0.0.0.0", web_port=8079)

        server = StatusWebServer(config)

        self.assertEqual(server.host, "0.0.0.0")
        self.assertEqual(server.port, 8079)
