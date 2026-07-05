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
from onlysavemevods.kick_chat import KickChatReplayResult
from onlysavemevods.job_tracker import clear_tracked_jobs, list_tracked_jobs, start_tracked_job
from onlysavemevods.log_buffer import RingBufferLogHandler, clear_log_buffer
from onlysavemevods.models import LiveStream, video_url
from onlysavemevods.powerchat import normalize_powerchat_payload, write_powerchat_sidecar
from onlysavemevods.state import StateStore
from onlysavemevods.web import (
    build_config_summary,
    build_status_snapshot,
    build_lite_status_payload,
    build_vod_chat_download_command,
    build_vod_download_command,
    build_streamer_voice_details_payload,
    build_stream_voice_speakers_payload,
    dashboard_script,
    chat_media_file_for_chat_file,
    file_kind,
    summarize_files,
    FILE_LIMIT_PER_STREAM,
    FILE_SCAN_CACHE,
    FILE_SCAN_CACHE_LOCK,
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
    update_stream_event_rules_from_form,
    update_voice_detection_from_form,
    resolve_refresh_chat_files,
    resolve_kick_chat_replay_files,
    cleanup_stream_fragments,
    delete_stream,
    resolve_watermark_download_file,
    start_vod_redownload_job,
    vod_download_progress_from_line,
    vod_output_template_for,
    resolve_watermark_source_file,
    delete_watermark_copy,
    resolve_transcription_source_file,
    resolve_render_chat_files,
    resolve_download_file,
    run_refresh_chat_job,
    run_render_chat_in_process_job,
    update_render_chat_job,
    run_render_chat_process_job,
    run_vod_download_job,
    queue_vod_post_processing_jobs,
    run_transcription_job,
    event_detection_job_key,
    run_event_detection_job,
    refresh_chat_job_key,
    RefreshChatJob,
    CHAT_REFRESH_JOBS,
    CHAT_REFRESH_JOBS_LOCK,
    RenderChatJob,
    CHAT_RENDER_JOBS,
    CHAT_RENDER_JOBS_LOCK,
    FAVICON_ROUTES,
    JobStatus,
    PLATFORM_ICON_ROUTES,
    snapshot_to_dict,
    transcription_job_key,
    TranscriptionJob,
    TRANSCRIPTION_JOBS,
    TRANSCRIPTION_JOBS_LOCK,
    EventDetectionJob,
    EVENT_DETECTION_JOBS,
    EVENT_DETECTION_JOBS_LOCK,
    StatusWebServer,
    StreamEventStatus,
    render_stream_event_timeline,
    render_streamer_jobs_summary,
    STREAM_DELETE_CONFIRM_VALUE,
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
        "stream_event_detection_enabled": "false",
        "stream_event_model": "MIT/ast-finetuned-audioset-10-10-0.4593",
        "stream_event_device": "auto",
        "stream_event_window_seconds": "10.0",
        "stream_event_hop_seconds": "5.0",
        "stream_event_min_confidence": "0.35",
        "stream_event_max_events_per_media": "100",
        "twitch_ad_repair_enabled": "true",
        "twitch_ad_repair_tesseract_path": "tesseract",
        "twitch_ad_repair_scan_seconds": "300",
        "twitch_ad_repair_sample_seconds": "2",
        "twitch_ad_repair_max_seconds": "180",
        "twitch_ad_repair_vod_search_limit": "5",
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
    def setUp(self) -> None:
        clear_tracked_jobs()

    def tearDown(self) -> None:
        clear_tracked_jobs()

    def test_vod_download_helpers_build_command_template_and_progress(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                yt_dlp_path="yt-dlp",
                extra_yt_dlp_args=["--cookies", "cookies.txt"],
            )
            stream = LiveStream(
                video_id="kick:Hungover 2026-07-05 06:18",
                url="https://kick.com/oumb/videos/123",
                title="Hungover 2026-07-05 06:18",
                channel="OUMB3rd",
                platform="kick",
                source="kick:oumb",
                is_live=False,
            )

            template = vod_output_template_for(config, stream, force_copy=False)
            command = build_vod_download_command(
                config,
                "https://kick.com/oumb/videos/123",
                template,
            )
            chat_command = build_vod_chat_download_command(
                config,
                "https://www.youtube.com/watch?v=LIVEVIDEO01",
                template,
            )

        self.assertEqual(
            template,
            Path(tmp)
            / "downloads"
            / "OUMB3rd"
            / "kick_Hungover 2026-07-05 06_18"
            / "Hungover 2026-07-05 06_18 [kick].%(ext)s",
        )
        self.assertIn("--no-playlist", command)
        self.assertIn("--cookies", command)
        self.assertIn("cookies.txt", command)
        self.assertEqual(command[-1], "https://kick.com/oumb/videos/123")
        self.assertIn("--skip-download", chat_command)
        self.assertIn("--write-subs", chat_command)
        self.assertIn("live_chat", chat_command)
        self.assertNotIn("--live-from-start", chat_command)
        self.assertEqual(chat_command[chat_command.index("-o") + 1], str(template))
        self.assertEqual(chat_command[-1], "https://www.youtube.com/watch?v=LIVEVIDEO01")
        self.assertEqual(vod_download_progress_from_line("[download]  42.5% of 1.00GiB"), 0.425)
        self.assertIsNone(vod_download_progress_from_line("[download] Destination: file.mp4"))

    def test_youtube_vod_download_warns_when_chat_replay_is_unavailable(self) -> None:
        class FakeProcess:
            def __init__(self, lines: list[str], return_code: int) -> None:
                self.stdout = iter(lines)
                self.return_code = return_code

            def wait(self) -> int:
                return self.return_code

        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                yt_dlp_path="yt-dlp",
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Live Status",
                channel="Example Channel",
                platform="youtube",
                source="@Example",
                is_live=False,
            )
            state = StateStore(config.db_path)
            state.mark_vod_downloading(stream, message="Started VOD download")
            state.close()
            output_template = vod_output_template_for(config, stream, force_copy=False)
            output_template.parent.mkdir(parents=True, exist_ok=True)
            job_id = "vod-download:LIVEVIDEO01:test"
            start_tracked_job(
                job_id,
                kind="VOD download",
                video_id=stream.video_id,
                item="Live Status.media",
                detail=stream.url,
                phase="Queued",
                message="Queued VOD download",
                progress=0.0,
            )

            with patch(
                "onlysavemevods.web.subprocess.Popen",
                side_effect=[
                    FakeProcess(["[download] 100.0% of 1.00GiB"], 0),
                    FakeProcess(["ERROR: no live chat replay available"], 1),
                ],
            ) as popen:
                run_vod_download_job(
                    config,
                    job_id,
                    stream,
                    stream.url,
                    output_template,
                    previous_status="ended",
                )

            state = StateStore(config.db_path)
            record = state.get_stream(stream.video_id)
            events = state.list_stream_events([stream.video_id], limit_per_stream=8)
            state.close()
            jobs = list_tracked_jobs()

        self.assertEqual(popen.call_count, 2)
        chat_command = popen.call_args_list[1].args[0]
        self.assertIn("--write-subs", chat_command)
        self.assertIn("live_chat", chat_command)
        self.assertNotIn("--live-from-start", chat_command)
        self.assertEqual(chat_command[chat_command.index("-o") + 1], str(output_template))
        self.assertEqual(chat_command[-1], stream.url)
        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(record.status, "ended")
        self.assertEqual(record.exit_code, 0)
        event_text = "\n".join(event.message for event in events[stream.video_id])
        self.assertIn("YouTube VOD live chat replay unavailable", event_text)
        self.assertIn("VOD download completed from", event_text)
        self.assertTrue(any(job.job_id == job_id and job.status == "done" for job in jobs))
        self.assertTrue(any("live chat replay unavailable" in job.message for job in jobs))

    def test_kick_vod_download_attempts_chat_replay_after_media_download(self) -> None:
        class FakeProcess:
            def __init__(self, lines: list[str], return_code: int) -> None:
                self.stdout = iter(lines)
                self.return_code = return_code

            def wait(self) -> int:
                return self.return_code

        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                yt_dlp_path="yt-dlp",
            )
            stream = LiveStream(
                video_id="kick:Hungover 2026-07-05 06:18",
                url="https://kick.com/oumb/videos/voduuid",
                title="Hungover 2026-07-05 06:18",
                channel="OUMB3rd",
                platform="kick",
                source="kick:oumb",
                is_live=False,
            )
            state = StateStore(config.db_path)
            state.mark_vod_downloading(stream, message="Started VOD download")
            state.close()
            output_template = vod_output_template_for(config, stream, force_copy=False)
            output_template.parent.mkdir(parents=True, exist_ok=True)
            chat_file = output_template.with_name(
                output_template.name.replace("%(ext)s", "live_chat.json")
            )
            job_id = "vod-download:kick:test"
            start_tracked_job(
                job_id,
                kind="VOD download",
                video_id=stream.video_id,
                item="Hungover.media",
                detail=stream.url,
                phase="Queued",
                message="Queued VOD download",
                progress=0.0,
            )

            with (
                patch("onlysavemevods.web.subprocess.Popen", return_value=FakeProcess(["[download] 100.0%"], 0)),
                patch(
                    "onlysavemevods.web.download_kick_vod_chat_replay",
                    return_value=KickChatReplayResult(
                        True,
                        "Kick chat replay downloaded",
                        chat_file=chat_file,
                        messages=3,
                    ),
                ) as replay,
            ):
                run_vod_download_job(
                    config,
                    job_id,
                    stream,
                    stream.url,
                    output_template,
                    previous_status="ended",
                )

            state = StateStore(config.db_path)
            record = state.get_stream(stream.video_id)
            events = state.list_stream_events([stream.video_id], limit_per_stream=8)
            state.close()
            jobs = list_tracked_jobs()

        replay.assert_called_once()
        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(record.status, "ended")
        event_text = "\n".join(event.message for event in events[stream.video_id])
        self.assertIn("Kick VOD chat replay downloaded", event_text)
        self.assertTrue(any(job.job_id == job_id and job.status == "done" for job in jobs))
        self.assertTrue(any("Kick chat replay" in job.message for job in jobs))

    def test_vod_download_queues_enabled_post_processing_jobs(self) -> None:
        class FakeProcess:
            def __init__(self, lines: list[str], return_code: int, output: Path) -> None:
                self.stdout = iter(lines)
                self.return_code = return_code
                self.output = output

            def wait(self) -> int:
                self.output.write_text("media", encoding="utf-8")
                return self.return_code

        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                transcribe_subtitles=True,
                stream_event_detection_enabled=True,
            )
            stream = LiveStream(
                video_id="rumble:vod-123",
                url="https://rumble.com/vod-123.html",
                title="Rumble VOD",
                channel="OUMB3rd",
                platform="rumble",
                source="rumble:user/OUMB3rd",
                is_live=False,
            )
            state = StateStore(config.db_path)
            state.mark_vod_downloading(stream, message="Started VOD download")
            state.close()
            output_template = vod_output_template_for(config, stream, force_copy=False)
            output_template.parent.mkdir(parents=True, exist_ok=True)
            media_file = output_template.with_name(output_template.name.replace("%(ext)s", "mp4"))
            job_id = "vod-download:rumble:test"
            start_tracked_job(
                job_id,
                kind="VOD download",
                video_id=stream.video_id,
                item="Rumble VOD.media",
                detail=stream.url,
                phase="Queued",
                message="Queued VOD download",
                progress=0.0,
            )

            with (
                patch(
                    "onlysavemevods.web.subprocess.Popen",
                    return_value=FakeProcess(["[download] 100.0%"], 0, media_file),
                ),
                patch("onlysavemevods.web.Thread") as thread_cls,
            ):
                run_vod_download_job(
                    config,
                    job_id,
                    stream,
                    stream.url,
                    output_template,
                    previous_status="ended",
                )

            jobs = list_tracked_jobs(limit=20)
            with TRANSCRIPTION_JOBS_LOCK:
                transcription_jobs = list(TRANSCRIPTION_JOBS.values())
                TRANSCRIPTION_JOBS.clear()
            with EVENT_DETECTION_JOBS_LOCK:
                event_jobs = list(EVENT_DETECTION_JOBS.values())
                EVENT_DETECTION_JOBS.clear()

        self.assertTrue(thread_cls.return_value.start.called)
        self.assertTrue(any(job.kind == "VOD download" and job.status == "done" for job in jobs))
        self.assertTrue(any(job.status == "running" for job in transcription_jobs))
        self.assertTrue(any(job.status == "running" for job in event_jobs))

    def test_vod_post_processing_queues_chat_render_and_twitch_ad_repair(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                render_live_chat_video=True,
                twitch_ad_repair_enabled=True,
            )
            kick_stream = LiveStream(
                video_id="kick:Hungover 2026-07-05 06:18",
                url="https://kick.com/oumb/videos/voduuid",
                title="Hungover 2026-07-05 06:18",
                channel="OUMB3rd",
                platform="kick",
                source="kick:oumb",
                is_live=False,
            )
            twitch_stream = LiveStream(
                video_id="twitch:Live 2026-07-05 06:18",
                url="https://www.twitch.tv/videos/123",
                title="Live 2026-07-05 06:18",
                channel="OUMB3rd",
                platform="twitch",
                source="twitch:oumb",
                is_live=False,
            )
            state = StateStore(config.db_path)
            for stream in (kick_stream, twitch_stream):
                state.mark_vod_downloading(stream, message="Started VOD download")
                state.mark_vod_download_finished(stream.video_id)
            state.close()

            kick_template = vod_output_template_for(config, kick_stream, force_copy=False)
            kick_template.parent.mkdir(parents=True, exist_ok=True)
            kick_media = kick_template.with_name(kick_template.name.replace("%(ext)s", "mp4"))
            kick_chat = kick_template.with_name(kick_template.name.replace("%(ext)s", "live_chat.json"))
            kick_media.write_text("media", encoding="utf-8")
            kick_chat.write_text(json.dumps({"platform": "kick", "messages": []}), encoding="utf-8")

            twitch_template = vod_output_template_for(config, twitch_stream, force_copy=False)
            twitch_template.parent.mkdir(parents=True, exist_ok=True)
            twitch_media = twitch_template.with_name(twitch_template.name.replace("%(ext)s", "mp4"))
            twitch_media.write_text("media", encoding="utf-8")

            with patch("onlysavemevods.web.Thread") as thread_cls:
                queue_vod_post_processing_jobs(config, kick_stream, kick_template)
                queue_vod_post_processing_jobs(config, twitch_stream, twitch_template)

            jobs = list_tracked_jobs(limit=20)
            with CHAT_RENDER_JOBS_LOCK:
                render_jobs = list(CHAT_RENDER_JOBS.values())
                CHAT_RENDER_JOBS.clear()

        self.assertGreaterEqual(thread_cls.return_value.start.call_count, 2)
        self.assertTrue(any(job.status == "running" for job in render_jobs))
        self.assertTrue(any(job.kind == "Twitch ad repair" for job in jobs))

    def test_start_vod_redownload_job_marks_stream_and_tracks_job(self) -> None:
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
            state.upsert_detected(stream)
            state.mark_ended(stream.video_id)
            state.close()

            with patch("onlysavemevods.web.Thread") as thread_cls:
                ok, message = start_vod_redownload_job(
                    config,
                    stream.video_id,
                    video_url("LIVEVIDEO01"),
                )

            state = StateStore(config.db_path)
            record = state.get_stream(stream.video_id)
            events = state.list_stream_events([stream.video_id], limit_per_stream=8)
            state.close()

        self.assertTrue(ok, message)
        self.assertEqual(message, "VOD redownload queued")
        thread_cls.return_value.start.assert_called_once()
        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(record.status, "downloading")
        self.assertEqual(record.url, video_url("LIVEVIDEO01"))
        self.assertTrue(any(job.kind == "VOD download" for job in list_tracked_jobs()))
        self.assertIn(
            "Started VOD redownload",
            "\n".join(event.message for event in events[stream.video_id]),
        )

    def test_streamer_ui_shows_manual_and_redownload_vod_controls(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                streamers={"OUMB3rd": StreamerConfig(sources=["@OUMB3rd"])},
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Live Status",
                channel="OUMB3rd",
            )
            state = StateStore(config.db_path)
            state.upsert_detected(stream)
            state.mark_ended(stream.video_id)
            state.close()

            html = render_status_html(build_status_snapshot(config, include_speaker_scan=False))

        self.assertIn("Add VOD", html)
        self.assertIn("Redownload from VOD", html)
        self.assertIn('/vod-download', html)
        self.assertIn("Download VOD Copy", html)

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
            (segment_dir / "Live Status [LIVEVIDEO01].stream-events.json").write_text(
                json.dumps(
                    {
                        "media": "Live Status [LIVEVIDEO01].mp4",
                        "events": [
                            {
                                "start": 10.0,
                                "end": 14.0,
                                "duration": 4.0,
                                "rule": "Laughter",
                                "severity": "high",
                                "score": 0.92,
                                "loudness_dbfs": -8.2,
                                "labels": [{"label": "Laughter", "score": 0.92}],
                                "keywords": ["haha"],
                                "text": "big laugh",
                            }
                        ],
                    }
                ),
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
        self.assertEqual(stream_status.file_kind_counts["state"], 2)
        self.assertGreater(stream_status.chat_bytes, 0)
        self.assertEqual(stream_status.content_event_count, 1)
        self.assertEqual(stream_status.content_events[0].rule, "Laughter")
        self.assertEqual(stream_status.content_events[0].media_name, "Live Status [LIVEVIDEO01].mp4")
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

    def test_status_snapshot_and_html_include_powerchat_events(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                channels=["kick:oumb"],
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            stream = LiveStream(
                video_id="kick:oumb",
                url="https://kick.com/oumb",
                title="Kick Stream",
                channel="oumb",
                platform="kick",
                source="kick:oumb",
            )
            state = StateStore(config.db_path)
            state.mark_ended(stream.video_id)
            state.mark_downloading(stream, 1)
            state.mark_ended(stream.video_id)
            state.close()

            segment_dir = config.download_dir / "oumb" / "kick_oumb"
            segment_dir.mkdir(parents=True)
            media = segment_dir / "Kick Stream [kick_oumb].mp4"
            media.write_text("media", encoding="utf-8")
            event = normalize_powerchat_payload(
                "KDrizzy69 just gifted 50 Kicks on Kick",
                source="tts",
                received_at="2026-07-05T10:00:30+00:00",
                stream_started_at="2026-07-05T10:00:00+00:00",
            )
            assert event is not None
            write_powerchat_sidecar(
                segment_dir / "Kick Stream [kick_oumb].powerchat-events.json",
                events=[event],
                streamer_name="OUMB3rd",
                username="oumb",
                video_id="kick:oumb",
                segment_index=1,
                stream_started_at="2026-07-05T10:00:00+00:00",
            )

            snapshot = build_status_snapshot(config)
            html = render_status_html(snapshot)
            payload = snapshot_to_dict(snapshot)

        stream_status = snapshot.streams[0]
        self.assertEqual(stream_status.powerchat_event_count, 1)
        self.assertEqual(stream_status.powerchat_unit_totals, [
            {"platform": "Kick", "unit": "Kicks", "amount": 50.0}
        ])
        self.assertEqual(stream_status.powerchat_events[0].donor, "KDrizzy69")
        self.assertEqual(file_kind("Kick Stream [kick_oumb].powerchat-events.json"), "state")
        self.assertIn("Powerchat", html)
        self.assertIn("KDrizzy69", html)
        self.assertIn("Kick: 50 Kicks", html)
        self.assertEqual(payload["streams"][0]["powerchat_event_count"], 1)
        self.assertEqual(payload["streams"][0]["powerchat_events"][0]["platform"], "Kick")

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
            (segment_dir / "Live Status [LIVEVIDEO01].stream-events.json").write_text(
                json.dumps(
                    {
                        "media": "Live Status [LIVEVIDEO01].mp4",
                        "events": [
                            {
                                "start": 10.0,
                                "end": 14.0,
                                "duration": 4.0,
                                "rule": "Laughter",
                                "severity": "high",
                                "score": 0.92,
                                "loudness_dbfs": -8.2,
                                "labels": [{"label": "Laughter", "score": 0.92}],
                                "keywords": ["haha"],
                                "text": "big laugh",
                            }
                        ],
                    }
                ),
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
        self.assertIn("Content Event Rules", html)
        self.assertIn('action="/stream-event-rules"', html)
        self.assertIn("event-settings-box", html)
        self.assertIn("event-rule-card", html)
        self.assertIn("event-rule-add", html)
        self.assertIn("Current Events", html)
        self.assertIn("Audio labels", html)
        self.assertIn("Transcript keywords", html)
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
        self.assertIn("onlysavemevods.streamerStreamFilters", html)
        self.assertIn("data-streamer-toggle", html)
        self.assertIn("data-streamer-settings-toggle", html)
        self.assertIn("data-streamer-settings-panel", html)
        self.assertIn("data-stream-browser", html)
        self.assertIn("data-stream-filter-platform", html)
        self.assertIn("data-stream-filter-search", html)
        self.assertIn("data-stream-filter-from", html)
        self.assertIn("data-stream-filter-to", html)
        self.assertIn("data-stream-page-size", html)
        self.assertIn("streamer-details", html)
        self.assertIn(".streamer-settings-tabs .streamer-settings-panel", html)
        self.assertNotIn("\n    .streamer-settings-panel { display: none", html)
        self.assertIn("applyStreamerCollapsedState", html)
        self.assertIn("applyStreamerSettingsState", html)
        self.assertIn("applyStreamTabState", html)
        self.assertIn("stream-detail-tabs", html)
        self.assertIn('data-stream-tab="files"', html)
        self.assertIn('data-stream-tab="log"', html)
        self.assertIn('data-stream-tab="jobs"', html)
        self.assertIn('data-stream-tab="events"', html)
        self.assertIn('data-stream-tab="speakers"', html)
        self.assertIn("stream-tab-panel stream-tab-files", html)
        self.assertIn("stream-tab-panel stream-tab-log", html)
        self.assertIn("stream-tab-panel stream-tab-jobs", html)
        self.assertIn("stream-tab-panel stream-tab-events", html)
        self.assertIn("stream-tab-panel stream-tab-speakers", html)
        self.assertIn("Content Events", html)
        self.assertIn("Detected Speakers", html)
        self.assertIn("Stream Log", html)
        self.assertIn("Laughter", html)
        self.assertIn("big laugh", html)
        self.assertIn('class="content-event-time"', html)
        self.assertIn('class="content-event-end"', html)
        self.assertIn("to 0:14", html)
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
        self.assertIn('fetch("/status.json?lite=1"', html)
        self.assertIn('fetch("/status.json?dashboard=1"', html)
        self.assertIn("lastStreamRevision", html)
        self.assertIn("lastJobRevision", html)
        self.assertIn("data-job-revision", html)
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

    def test_status_file_scan_caps_detail_rows_but_counts_all_files(self) -> None:
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
            for index in range(FILE_LIMIT_PER_STREAM + 5):
                (segment_dir / f"segment-001.f136.mp4.part-Frag{index}").write_text(
                    "fragment",
                    encoding="utf-8",
                )
            (segment_dir / "segment-001.f136.mp4.part").write_text("part", encoding="utf-8")
            (segment_dir / "segment-001.f136.mp4").write_text("final", encoding="utf-8")

            snapshot = build_status_snapshot(config)

        status = snapshot.streams[0]
        self.assertEqual(status.file_count, FILE_LIMIT_PER_STREAM + 7)
        self.assertLessEqual(len(status.files), FILE_LIMIT_PER_STREAM)
        self.assertEqual(status.file_kind_counts["fragment"], FILE_LIMIT_PER_STREAM + 5)
        self.assertTrue(status.has_part_files)
        self.assertTrue(status.has_mixed_formats)

    def test_file_scan_cache_reuses_unchanged_directory(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            directory = Path(tmp) / "stream"
            directory.mkdir()
            (directory / "video.mp4").write_text("media", encoding="utf-8")
            with FILE_SCAN_CACHE_LOCK:
                FILE_SCAN_CACHE.clear()

            first = summarize_files(config, directory, "LIVEVIDEO01", cache_ttl_seconds=60.0)
            with patch(
                "onlysavemevods.web.scan_directory_uncached",
                side_effect=AssertionError("cache missed"),
            ):
                second = summarize_files(config, directory, "LIVEVIDEO01", cache_ttl_seconds=60.0)
            with FILE_SCAN_CACHE_LOCK:
                FILE_SCAN_CACHE.clear()

        self.assertEqual(first.file_count, 1)
        self.assertEqual(second.file_count, 1)
        self.assertEqual(second.files[0].name, "video.mp4")

    def test_status_payloads_include_tracked_auto_jobs(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            start_tracked_job(
                "auto-transcription:LIVEVIDEO01:Live.mp4",
                kind="Transcription",
                video_id="LIVEVIDEO01",
                item="Live.mp4",
                detail="Automatic post-finalize transcription",
                phase="Running WhisperX",
                message="Running automatic transcription",
                progress=0.25,
            )

            snapshot = build_status_snapshot(config)
            lite = build_lite_status_payload(config)
            html = render_status_html(snapshot)

        self.assertEqual(snapshot.jobs[0].kind, "Transcription")
        self.assertEqual(snapshot.jobs[0].progress, 0.25)
        self.assertTrue(snapshot.job_revision.startswith("active:"))
        self.assertEqual(lite["jobs"][0]["kind"], "Transcription")
        self.assertEqual(lite["jobs"][0]["progress"], 0.25)
        self.assertEqual(lite["job_revision"], snapshot.job_revision)
        self.assertIn('data-job-revision="active:', html)
        self.assertIn("Running automatic transcription", html)

    def test_lite_status_payload_avoids_full_stream_payload(self) -> None:
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

            payload = build_lite_status_payload(config)

        self.assertEqual(payload["detail"], "lite")
        self.assertEqual(payload["stream_count"], 1)
        self.assertEqual(payload["counts"]["downloading"], 1)
        self.assertIn("stream_revision", payload)
        self.assertIn("jobs", payload)
        self.assertIn("recent_logs", payload)
        self.assertNotIn("streams", payload)
        self.assertNotIn("streamer_stats", payload)
        self.assertNotIn("configuration", payload)

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
        self.assertEqual(
            payload["streams"][0]["jobs"][0]["details"]["output_name"],
            "Live Status [LIVEVIDEO01] - chat.mp4",
        )
        self.assertIn('data-stream-tab="jobs"', html)
        self.assertIn('stream-tab-panel stream-tab-jobs', html)
        self.assertIn("Chat render", html)
        self.assertIn("Starting isolated renderer", html)
        self.assertIn("Details", html)
        self.assertIn("Media", html)
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

    def test_isolated_chat_render_uses_progress_sidecar(self) -> None:
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

                def __init__(self, command: list[str], *_args: object, **_kwargs: object) -> None:
                    self.calls = 0
                    self.progress_file = Path(command[command.index("--progress-file") + 1])

                def communicate(self, timeout: float | None = None) -> tuple[bytes, bytes]:
                    self.calls += 1
                    if self.calls == 1:
                        self.progress_file.write_text(
                            json.dumps(
                                {
                                    "phase": "Rendering panel frames 42/100",
                                    "progress": 0.42,
                                    "updated_at": 1234.0,
                                    "elapsed_seconds": 12.0,
                                    "media_name": media_file.name,
                                    "chat_name": chat_file.name,
                                    "output_name": output_file.name,
                                    "outputs": {
                                        "panel": {
                                            "name": f"{output_file.stem}.panel{output_file.suffix}",
                                            "size_bytes": 4096,
                                        }
                                    },
                                }
                            ),
                            encoding="utf-8",
                        )
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
            if update.get("phase") == "Rendering panel frames 42/100"
        ]
        self.assertTrue(progress_updates)
        self.assertEqual(progress_updates[0]["progress"], 0.42)
        details = progress_updates[0]["details"]
        self.assertEqual(details["current_label"], "chat panel")
        self.assertEqual(details["current_size_bytes"], 4096)
        self.assertEqual(details["output_name"], output_file.name)
        self.assertEqual(job.status, "done")
        self.assertEqual(job.progress, 1.0)
        self.assertEqual(job.details["output_name"], output_file.name)

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
                'sources = ["@OUMB3rd"]\n'
                '[streamers."OUMB3rd".voices."Host"]\n'
                'samples = []\n'
                '[[streamers."OUMB3rd".stream_event_rules]]\n'
                'name = "Hype"\n'
                'keywords = ["lets go"]\n'
                'voice = "Host"\n',
                encoding="utf-8",
            )
            config = load_config(config_path)

            html = render_status_html(build_status_snapshot(config))

        self.assertIn('streamer-settings-tabs', html)
        self.assertIn('streamer-settings-main-label', html)
        self.assertIn('streamer-settings-voices-label', html)
        self.assertIn('streamer-settings-events-label', html)
        self.assertIn('streamer-settings-panel streamer-settings-voices', html)
        self.assertIn('streamer-settings-panel streamer-settings-events', html)
        self.assertIn('Current Events', html)
        self.assertIn('Hype', html)
        self.assertIn('keywords: lets go', html)
        self.assertIn('voice: Host', html)
        self.assertIn('name="rule_voice"', html)
        self.assertIn('<option value="Host" selected>Host</option>', html)
        self.assertIn('name="rule_delete_0"', html)
        self.assertIn('event-rule-add', html)
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
        self.assertIn('class="about-icon" src="/Favicon.png?v=', html)
        for path in FAVICON_ROUTES.values():
            self.assertTrue(Path(path).is_file(), path)
            self.assertIn("assets/favicons", Path(path).as_posix())
        self.assertIn("/assets/platforms/youtube.svg", PLATFORM_ICON_ROUTES)
        self.assertTrue(Path(PLATFORM_ICON_ROUTES["/assets/platforms/youtube.svg"]).is_file())

    def test_status_html_renders_streamer_voice_settings_tab(self) -> None:
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
            details = build_streamer_voice_details_payload(config, "OUMB3rd")
            speakers = build_stream_voice_speakers_payload(config, "OUMB3rd", "LIVEVIDEO01")

        self.assertIn('streamer-settings-voices-label', html)
        self.assertIn('streamer-settings-panel streamer-settings-voices', html)
        self.assertIn('id="voice-settings-OUMB3rd-known"', html)
        self.assertNotIn('data-open-voice-manager', html)
        self.assertNotIn('id="voice-manager-OUMB3rd"', html)
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
        self.assertIn('data-load-voice-details', html)
        self.assertIn('data-load-stream-speakers', html)
        self.assertIn('data-stream-speakers', html)
        self.assertIn('data-voice-review', html)
        self.assertNotIn('id="voice-settings-OUMB3rd-detected"', html)
        self.assertNotIn('data-voice-detected', html)
        self.assertIn("loaded only when requested", html)
        self.assertNotIn('action="/streamer-voice-samples/from-transcript"', html)
        self.assertNotIn('action="/streamer-voice-attributions"', html)
        self.assertNotIn("SPEAKER_00</strong> -> Host", html)
        self.assertNotIn("distance 0.200", html)
        self.assertNotIn("Approve", html)
        self.assertNotIn("Reject", html)
        self.assertNotIn("Match Voices", html)
        self.assertEqual("", details["detected"])
        self.assertIn('action="/streamer-voice-samples/from-transcript"', speakers["speakers"])
        self.assertIn("SPEAKER_00", speakers["speakers"])
        self.assertIn('action="/streamer-voice-attributions"', details["review"])
        self.assertIn("SPEAKER_00</strong> -> Host", details["review"])
        self.assertIn("distance 0.200", details["review"])
        self.assertIn("Approve", details["review"])
        self.assertIn("Reject", details["review"])
        self.assertIn("Match Voices", details["review"])

    def test_dashboard_script_does_not_emit_literal_newlines_in_js_strings(self) -> None:
        script = dashboard_script()

        self.assertIn("join(String.fromCharCode(10))", script)
        self.assertIn("renderStreamerVoiceSettings", script)
        self.assertIn("/streamer-voice-details?streamer=", script)
        self.assertIn("/stream-voice-speakers?streamer=", script)
        self.assertIn("data-load-voice-details", script)
        self.assertIn("data-load-stream-speakers", script)
        self.assertIn("data-streamer-job-page-button", script)
        self.assertIn("data-streamer-job-page-state", script)
        self.assertIn("onlysavemevods.streamerStreamFilters", script)
        self.assertIn("applyStreamerStreamBrowser", script)
        self.assertIn("data-stream-filter-platform", script)
        self.assertIn("data-stream-filter-search", script)
        self.assertIn("data-stream-filter-from", script)
        self.assertIn("data-stream-filter-to", script)
        self.assertIn("data-stream-page-next", script)
        self.assertNotIn("data-open-voice-manager", script)
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

    def test_streamer_jobs_summary_paginates_many_jobs(self) -> None:
        jobs = [
            JobStatus(
                job_id=f"job-{index}",
                kind="Chat render",
                status="running",
                phase=f"Part {index}",
                progress=0.1 * index,
                video_id="LIVEVIDEO01",
                item=f"part-{index}.live_chat.json",
                detail="",
                message="Rendering",
                started_at=float(index),
                updated_at=float(index),
                finished_at=None,
            )
            for index in range(1, 7)
        ]

        html = render_streamer_jobs_summary(jobs)

        self.assertIn('data-streamer-jobs', html)
        self.assertIn('data-streamer-job-page="1"', html)
        self.assertIn('data-streamer-job-page="2" hidden', html)
        self.assertIn('data-streamer-job-page-button="2"', html)
        self.assertIn('data-streamer-job-page-state>Page 1 of 2', html)
        self.assertIn('part-1.live_chat.json', html)
        self.assertIn('part-6.live_chat.json', html)

    def test_streamer_streams_have_filter_controls_and_metadata(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                streamers={
                    "OUMB3rd": StreamerConfig(
                        sources=["@OUMB3rd", "twitch:OUMB3rd", "kick:oumb", "rumble:user/OUMB3rd"],
                    )
                },
            )
            state = StateStore(config.db_path)
            streams = [
                LiveStream(
                    video_id="youtube:YOUTUBE01",
                    url="https://www.youtube.com/watch?v=YOUTUBE01",
                    title="YouTube Stream",
                    channel="OUMB3rd",
                    platform="youtube",
                    source="@OUMB3rd",
                ),
                LiveStream(
                    video_id="twitch:OUMB3rd",
                    url="https://www.twitch.tv/OUMB3rd",
                    title="Twitch Stream",
                    channel="OUMB3rd",
                    platform="twitch",
                    source="twitch:OUMB3rd",
                ),
                LiveStream(
                    video_id="kick:oumb",
                    url="https://kick.com/oumb",
                    title="Kick Stream",
                    channel="oumb",
                    platform="kick",
                    source="kick:oumb",
                ),
                LiveStream(
                    video_id="rumble:OUMB3rd",
                    url="https://rumble.com/user/OUMB3rd",
                    title="Rumble Stream",
                    channel="OUMB3rd",
                    platform="rumble",
                    source="rumble:user/OUMB3rd",
                ),
            ]
            for stream in streams:
                state.mark_downloading(stream, 1)
                state.mark_ended(stream.video_id)
            state.close()

            html = render_status_html(build_status_snapshot(config))

        self.assertIn('data-stream-browser data-streamer-key="OUMB3rd"', html)
        self.assertIn('data-stream-filter-platform', html)
        self.assertIn('data-stream-filter-search', html)
        self.assertIn('data-stream-filter-from', html)
        self.assertIn('data-stream-filter-to', html)
        self.assertIn('data-stream-page-size', html)
        self.assertIn('data-stream-page-prev', html)
        self.assertIn('data-stream-page-next', html)
        self.assertIn('<option value="twitch">Twitch</option>', html)
        self.assertIn('<option value="kick">Kick</option>', html)
        self.assertIn('<option value="rumble">Rumble</option>', html)
        self.assertIn('data-stream-platform="kick"', html)
        self.assertIn('data-stream-title="Kick Stream"', html)

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

    def test_cleanup_stream_fragments_removes_only_fragment_files(self) -> None:
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
            media_fragment = segment_dir / "segment-001.f137.mp4.part-Frag1"
            chat_fragment = segment_dir / "segment-001.live_chat.json.part-Frag2"
            part_file = segment_dir / "segment-001.f140.mp4.part"
            state_file = segment_dir / "segment-001.f140.mp4.ytdl"
            final_file = segment_dir / "Live Status [LIVEVIDEO01].mp4"
            media_fragment.write_text("fragment", encoding="utf-8")
            chat_fragment.write_text("chatfrag", encoding="utf-8")
            part_file.write_text("part", encoding="utf-8")
            state_file.write_text("{}", encoding="utf-8")
            final_file.write_text("final", encoding="utf-8")

            html = render_status_html(build_status_snapshot(config))
            count, bytes_removed = cleanup_stream_fragments(config, stream.video_id)
            state = StateStore(config.db_path)
            try:
                events = state.list_stream_events([stream.video_id], limit_per_stream=8)
            finally:
                state.close()
            media_fragment_exists = media_fragment.exists()
            chat_fragment_exists = chat_fragment.exists()
            part_file_exists = part_file.exists()
            state_file_exists = state_file.exists()
            final_file_exists = final_file.exists()

        self.assertIn("/cleanup-fragments?video_id=LIVEVIDEO01", html)
        self.assertIn("Clean fragments (2)", html)
        self.assertEqual(count, 2)
        self.assertEqual(bytes_removed, len("fragment") + len("chatfrag"))
        self.assertFalse(media_fragment_exists)
        self.assertFalse(chat_fragment_exists)
        self.assertTrue(part_file_exists)
        self.assertTrue(state_file_exists)
        self.assertTrue(final_file_exists)
        self.assertTrue(
            any(
                "Cleaned 2 fragment file(s)" in event.message
                for event in events[stream.video_id]
            )
        )

    def test_cleanup_stream_fragments_rejects_active_download(self) -> None:
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
            fragment = segment_dir / "segment-001.f137.mp4.part-Frag1"
            fragment.write_text("fragment", encoding="utf-8")

            html = render_status_html(build_status_snapshot(config))
            with self.assertRaisesRegex(ConfigError, "may still resume"):
                cleanup_stream_fragments(config, stream.video_id)
            fragment_exists = fragment.exists()

        self.assertNotIn("/cleanup-fragments?video_id=LIVEVIDEO01", html)
        self.assertTrue(fragment_exists)

    def test_delete_stream_removes_directory_and_state_after_confirmation(self) -> None:
        clear_tracked_jobs()
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
            state.create_watermark_copy(
                copy_id="wm_copy001",
                video_id=stream.video_id,
                source_name="Live Status [LIVEVIDEO01].mp4",
                output_name=".watermarks/Live Status [LIVEVIDEO01] - wm-copy001.mp4",
                recipient_label="Recipient A",
            )
            state.update_watermark_copy("wm_copy001", status="done", finished=True)
            state.close()

            segment_dir = config.download_dir / "Example_Channel" / "LIVEVIDEO01"
            watermark_dir = segment_dir / ".watermarks"
            watermark_dir.mkdir(parents=True)
            final_file = segment_dir / "Live Status [LIVEVIDEO01].mp4"
            watermark_file = watermark_dir / "Live Status [LIVEVIDEO01] - wm-copy001.mp4"
            final_file.write_text("final", encoding="utf-8")
            watermark_file.write_text("watermark", encoding="utf-8")

            html = render_status_html(build_status_snapshot(config))
            rendered_body = html.split("<script>", 1)[0]
            ok, message = delete_stream(
                config,
                stream.video_id,
                STREAM_DELETE_CONFIRM_VALUE,
            )
            state = StateStore(config.db_path)
            try:
                record = state.get_stream(stream.video_id)
                events = state.list_stream_events([stream.video_id], limit_per_stream=8)
                watermarks = state.list_watermark_copies(video_id=stream.video_id)
            finally:
                state.close()
            directory_exists = segment_dir.exists()

        self.assertIn('/delete-stream', rendered_body)
        self.assertIn('Delete stream', rendered_body)
        self.assertIn('return confirm', rendered_body)
        self.assertTrue(ok, message)
        self.assertIn("Stream deleted", message)
        self.assertFalse(directory_exists)
        self.assertIsNone(record)
        self.assertEqual(events[stream.video_id], [])
        self.assertEqual(watermarks, [])

    def test_delete_stream_requires_confirmation(self) -> None:
        clear_tracked_jobs()
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
            (segment_dir / "Live Status [LIVEVIDEO01].mp4").write_text("final", encoding="utf-8")

            ok, message = delete_stream(config, stream.video_id, "")
            state = StateStore(config.db_path)
            try:
                record = state.get_stream(stream.video_id)
            finally:
                state.close()
            directory_exists = segment_dir.exists()

        self.assertFalse(ok)
        self.assertIn("not confirmed", message)
        self.assertTrue(directory_exists)
        self.assertIsNotNone(record)

    def test_delete_stream_rejects_active_download(self) -> None:
        clear_tracked_jobs()
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
            (segment_dir / "segment-001.mp4.part").write_text("part", encoding="utf-8")

            html = render_status_html(build_status_snapshot(config))
            rendered_body = html.split("<script>", 1)[0]
            ok, message = delete_stream(
                config,
                stream.video_id,
                STREAM_DELETE_CONFIRM_VALUE,
            )
            state = StateStore(config.db_path)
            try:
                record = state.get_stream(stream.video_id)
            finally:
                state.close()
            directory_exists = segment_dir.exists()

        self.assertNotIn('/delete-stream', rendered_body)
        self.assertFalse(ok)
        self.assertIn("may still be active", message)
        self.assertTrue(directory_exists)
        self.assertIsNotNone(record)

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

    def test_status_chat_actions_do_not_use_single_media_fallback(self) -> None:
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
            chat_file = segment_dir / "orphan.live_chat.json"
            media_file.write_text("media", encoding="utf-8")
            chat_file.write_text("chat", encoding="utf-8")

            fallback_match = chat_media_file_for_chat_file(segment_dir, chat_file.name)
            snapshot = build_status_snapshot(config)

        self.assertEqual(fallback_match, media_file)
        chat_status = next(
            file
            for file in snapshot.streams[0].files
            if file.name == chat_file.name
        )
        self.assertIsNone(chat_status.render_chat_url)
        self.assertIsNone(chat_status.render_chat_status)
        self.assertIsNone(chat_status.refresh_chat_url)
        self.assertIsNone(chat_status.refresh_chat_status)

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

    def test_kick_media_offers_chat_replay_download_when_sidecar_is_missing(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                record_live_chat=True,
                render_live_chat_video=True,
            )
            stream = LiveStream(
                video_id="kick:Hungover 2026-07-05 06:18",
                url="https://kick.com/oumb/videos/voduuid",
                title="Hungover 2026-07-05 06:18",
                channel="OUMB3rd",
                platform="kick",
                source="kick:oumb",
                is_live=False,
            )
            state = StateStore(config.db_path)
            state.mark_vod_downloading(stream, message="Started VOD download")
            state.mark_ended(stream.video_id)
            state.close()

            output_template = vod_output_template_for(config, stream, force_copy=False)
            output_template.parent.mkdir(parents=True, exist_ok=True)
            media_file = output_template.with_name(
                output_template.name.replace("%(ext)s", "mp4")
            )
            media_file.write_text("media", encoding="utf-8")
            chat_name = f"{media_file.stem}.live_chat.json"

            snapshot = build_status_snapshot(config)
            resolved = resolve_kick_chat_replay_files(config, stream.video_id, chat_name)

        media_status = next(
            file
            for file in snapshot.streams[0].files
            if file.name == media_file.name
        )
        self.assertEqual(resolved[1], media_file.resolve() if resolved else None)
        self.assertEqual(media_status.refresh_chat_status, "download")
        self.assertIn("/refresh-chat?", media_status.refresh_chat_url or "")
        self.assertIn("Download chat replay", render_file_action(media_status))

    def test_kick_chat_sidecar_offers_render_and_refresh_actions(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                record_live_chat=True,
                render_live_chat_video=True,
            )
            stream = LiveStream(
                video_id="kick:Hungover 2026-07-05 06:18",
                url="https://kick.com/oumb/videos/voduuid",
                title="Hungover 2026-07-05 06:18",
                channel="OUMB3rd",
                platform="kick",
                source="kick:oumb",
                is_live=False,
            )
            state = StateStore(config.db_path)
            state.mark_vod_downloading(stream, message="Started VOD download")
            state.mark_ended(stream.video_id)
            state.close()

            output_template = vod_output_template_for(config, stream, force_copy=False)
            output_template.parent.mkdir(parents=True, exist_ok=True)
            media_file = output_template.with_name(
                output_template.name.replace("%(ext)s", "mp4")
            )
            chat_file = output_template.with_name(
                output_template.name.replace("%(ext)s", "live_chat.json")
            )
            media_file.write_text("media", encoding="utf-8")
            chat_file.write_text(
                json.dumps({"platform": "kick", "messages": []}),
                encoding="utf-8",
            )

            snapshot = build_status_snapshot(config)

        chat_status = next(
            file
            for file in snapshot.streams[0].files
            if file.name == chat_file.name
        )
        self.assertIsNotNone(chat_status.render_chat_url)
        self.assertIsNotNone(chat_status.refresh_chat_url)
        action = render_file_action(chat_status)
        self.assertIn("Render chat", action)
        self.assertIn("Refresh chat", action)

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
        self.assertIn('name="twitch_ad_repair_enabled"', html)
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
                    twitch_ad_repair_enabled="false",
                    twitch_ad_repair_tesseract_path="/usr/bin/tesseract",
                    twitch_ad_repair_scan_seconds="0",
                    twitch_ad_repair_sample_seconds="5",
                    twitch_ad_repair_max_seconds="120",
                    twitch_ad_repair_vod_search_limit="3",
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
        self.assertFalse(updated.twitch_ad_repair_enabled)
        self.assertEqual(updated.twitch_ad_repair_tesseract_path, "/usr/bin/tesseract")
        self.assertEqual(updated.twitch_ad_repair_scan_seconds, 0)
        self.assertEqual(updated.twitch_ad_repair_sample_seconds, 5)
        self.assertEqual(updated.twitch_ad_repair_max_seconds, 120)
        self.assertEqual(updated.twitch_ad_repair_vod_search_limit, 3)
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

    def test_stream_event_rules_form_updates_streamer_config(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                '[streamers."OUMB3rd"]\n'
                'sources = ["@OUMB3rd"]\n',
                encoding="utf-8",
            )
            config = load_config(config_path)

            update_stream_event_rules_from_form(
                config,
                {
                    "scope": ["streamer"],
                    "streamer_name": ["OUMB3rd"],
                    "event_enabled": ["true"],
                    "event_model": ["custom/ast"],
                    "event_device": ["cpu"],
                    "event_window_seconds": ["8"],
                    "event_hop_seconds": ["2"],
                    "event_min_confidence": ["0.6"],
                    "event_max_events_per_media": ["25"],
                    "rule_name": ["Hype"],
                    "rule_enabled": ["true"],
                    "rule_labels": ["Cheering"],
                    "rule_keywords": ["lets go"],
                    "rule_voice": ["Host"],
                    "rule_min_loudness_dbfs": ["-30"],
                    "rule_min_duration_seconds": ["1"],
                    "rule_max_duration_seconds": ["20"],
                    "rule_severity": ["warning"],
                },
            )

            updated = load_config(config_path)

        streamer = updated.streamers["OUMB3rd"]
        self.assertIsNotNone(streamer.stream_event_detection)
        assert streamer.stream_event_detection is not None
        self.assertTrue(streamer.stream_event_detection.enabled)
        self.assertEqual(streamer.stream_event_detection.model, "custom/ast")
        self.assertEqual(streamer.stream_event_detection.device, "cpu")
        self.assertEqual(streamer.stream_event_detection.window_seconds, 8.0)
        self.assertEqual(streamer.stream_event_detection.hop_seconds, 2.0)
        self.assertEqual(streamer.stream_event_detection.min_confidence, 0.6)
        self.assertEqual(streamer.stream_event_detection.max_events_per_media, 25)
        self.assertEqual(len(streamer.stream_event_rules), 1)
        self.assertEqual(streamer.stream_event_rules[0].name, "Hype")
        self.assertEqual(streamer.stream_event_rules[0].labels, ["Cheering"])
        self.assertEqual(streamer.stream_event_rules[0].keywords, ["lets go"])
        self.assertEqual(streamer.stream_event_rules[0].voice, "Host")

    def test_stream_event_rules_form_deletes_marked_rule(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                '[streamers."OUMB3rd"]\n'
                'sources = ["@OUMB3rd"]\n'
                '[[streamers."OUMB3rd".stream_event_rules]]\n'
                'name = "Hype"\n'
                'keywords = ["lets go"]\n'
                'voice = "Host"\n'
                '[[streamers."OUMB3rd".stream_event_rules]]\n'
                'name = "Laugh"\n'
                'labels = ["Laughter"]\n'
                'voice = "Guest"\n',
                encoding="utf-8",
            )
            config = load_config(config_path)

            update_stream_event_rules_from_form(
                config,
                {
                    "scope": ["streamer"],
                    "streamer_name": ["OUMB3rd"],
                    "event_enabled": ["inherit"],
                    "rule_name": ["Hype", "Laugh"],
                    "rule_enabled": ["true", "true"],
                    "rule_labels": ["", "Laughter"],
                    "rule_keywords": ["lets go", ""],
                    "rule_voice": ["Host", "Guest"],
                    "rule_min_loudness_dbfs": ["", ""],
                    "rule_min_duration_seconds": ["", ""],
                    "rule_max_duration_seconds": ["", ""],
                    "rule_severity": ["info", "info"],
                    "rule_delete_0": ["true"],
                },
            )
            updated = load_config(config_path)

        self.assertEqual([rule.name for rule in updated.streamers["OUMB3rd"].stream_event_rules], ["Laugh"])
        self.assertEqual(updated.streamers["OUMB3rd"].stream_event_rules[0].labels, ["Laughter"])
        self.assertEqual(updated.streamers["OUMB3rd"].stream_event_rules[0].voice, "Guest")

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

    def test_event_detection_action_can_redetect_existing_sidecars(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                stream_event_detection_enabled=True,
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
            media_file.with_suffix(".stream-events.json").write_text(
                json.dumps({"media": media_file.name, "events": []}),
                encoding="utf-8",
            )

            snapshot = build_status_snapshot(config)

        media_status = next(
            file
            for file in snapshot.streams[0].files
            if file.name == media_file.name
        )
        self.assertEqual(media_status.event_detection_status, "detected")
        self.assertIsNotNone(media_status.event_detection_url)
        self.assertIn("regenerate=1", media_status.event_detection_url or "")
        action = render_file_action(media_status)
        self.assertIn("Redetect events", action)
        self.assertIn("Run content event detection again", action)

    def test_manual_event_detection_job_passes_regenerate_and_updates_status(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                stream_event_detection_enabled=True,
            )
            media_file = config.download_dir / "Live Status [LIVEVIDEO01].mp4"
            media_file.parent.mkdir(parents=True)
            media_file.write_text("media", encoding="utf-8")
            key = event_detection_job_key("LIVEVIDEO01", media_file.name)
            with EVENT_DETECTION_JOBS_LOCK:
                EVENT_DETECTION_JOBS[key] = EventDetectionJob(
                    video_id="LIVEVIDEO01",
                    media_name=media_file.name,
                    status="running",
                    message="Detecting content events",
                    started_at=0.0,
                )

            try:
                with patch("onlysavemevods.web.detect_content_events_for_media", return_value=True) as detect:
                    with patch("onlysavemevods.web.load_content_events", return_value=[{"rule": "Laugh"}]):
                        run_event_detection_job(
                            config,
                            key,
                            media_file,
                            regenerate=True,
                            channel="Example Channel",
                        )
                with EVENT_DETECTION_JOBS_LOCK:
                    job = EVENT_DETECTION_JOBS[key]
            finally:
                with EVENT_DETECTION_JOBS_LOCK:
                    EVENT_DETECTION_JOBS.pop(key, None)

        self.assertEqual(job.status, "done")
        self.assertEqual(job.progress, 1.0)
        self.assertIn("1 content event", job.message)
        detect.assert_called_once()
        self.assertEqual(detect.call_args.args[0], config)
        self.assertEqual(detect.call_args.args[1], media_file)
        self.assertTrue(detect.call_args.kwargs["overwrite"])
        self.assertEqual(detect.call_args.kwargs["channel"], "Example Channel")

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
            chat_video_file = segment_dir / "Live Status [LIVEVIDEO01] - chat.mp4"
            output_name = ".watermarks/Live Status [LIVEVIDEO01] - wm-copy001.mp4"
            output_file = segment_dir / output_name
            media_file.write_text("media", encoding="utf-8")
            chat_video_file.write_text("chat video", encoding="utf-8")
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
            resolved_chat_source = resolve_watermark_source_file(
                config,
                stream.video_id,
                chat_video_file.name,
            )

        self.assertTrue(is_watermarkable_media_file(media_file.name))
        self.assertTrue(is_watermarkable_media_file(chat_video_file.name))
        self.assertFalse(is_watermarkable_media_file("Live Status [LIVEVIDEO01].live_chat.json"))
        self.assertFalse(is_watermarkable_media_file("Live Status [LIVEVIDEO01] - chat.rendering.mp4"))
        stream_status = snapshot.streams[0]
        final_file = next(file for file in stream_status.files if file.name == media_file.name)
        chat_video_status = next(file for file in stream_status.files if file.name == chat_video_file.name)
        watermark_file = next(file for file in stream_status.files if file.name == output_name)
        self.assertIsNotNone(final_file.watermark_url)
        self.assertIsNotNone(chat_video_status.watermark_url)
        self.assertIsNone(chat_video_status.transcription_url)
        self.assertIsNotNone(resolved_chat_source)
        assert resolved_chat_source is not None
        self.assertEqual(resolved_chat_source[1], chat_video_file.resolve())
        self.assertEqual(final_file.watermark_copies[0].recipient_label, "Recipient A")
        self.assertIn("/download-watermark?", final_file.watermark_copies[0].download_url or "")
        self.assertEqual(final_file.watermark_copies[0].delete_url, "/delete-watermark")
        self.assertEqual(watermark_file.kind, "watermark")
        self.assertEqual(watermark_file.watermark_copy_id, "wm_copy001")
        self.assertEqual(watermark_file.watermark_recipient_label, "Recipient A")
        self.assertIn("/download-watermark?", watermark_file.download_url or "")
        self.assertEqual(watermark_file.watermark_delete_url, "/delete-watermark")
        self.assertEqual(resolved, output_file.resolve())
        self.assertIn("Watermark", html)
        self.assertIn("Recipient A", html)
        self.assertIn("/download-watermark?", html)
        self.assertIn("/delete-watermark", html)

    def test_delete_watermark_copy_removes_file_and_record(self) -> None:
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
            output_name = ".watermarks/Live Status [LIVEVIDEO01] - wm-copy001.mp4"
            output_file = segment_dir / output_name
            output_file.write_text("watermarked", encoding="utf-8")
            state.create_watermark_copy(
                copy_id="wm_copy001",
                video_id=stream.video_id,
                source_name="Live Status [LIVEVIDEO01].mp4",
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

            ok, message = delete_watermark_copy(config, "wm_copy001")
            state = StateStore(config.db_path)
            fetched = state.get_watermark_copy("wm_copy001")
            events = state.list_stream_events([stream.video_id])[stream.video_id]
            state.close()

        self.assertTrue(ok, message)
        self.assertFalse(output_file.exists())
        self.assertIsNone(fetched)
        self.assertTrue(any("Deleted watermark copy" in event.message for event in events))

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
