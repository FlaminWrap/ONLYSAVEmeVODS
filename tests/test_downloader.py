from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, patch
import asyncio
import json
import unittest

from onlysavemevods.config import BotConfig, DEFAULT_POST_EXIT_CHECK_SECONDS
from onlysavemevods.chat_refresh import ChatRefreshResult
from onlysavemevods.chat_timing import read_chat_timing
from onlysavemevods.downloader import (
    DownloadManager,
    build_chat_download_command,
    build_download_command,
    choose_restart_segment,
    CatchupTracker,
    command_for_log,
    output_template_for,
    prepare_finalize_plan,
    rename_finalized_segment_file,
    rename_segment_chat_file,
    rename_segment_timing_file,
    restore_mixed_segment_for_resume,
    segment_has_final_files,
    segment_part_files,
    segment_timing_file,
)
from onlysavemevods.models import LiveStream, video_url
from onlysavemevods.state import StateStore


class NullLogger:
    def warning(self, *args, **kwargs) -> None:
        return None


NULL_LOGGER = NullLogger()


class DownloaderCommandTests(unittest.TestCase):
    def test_download_command_includes_live_resume_flags_without_keep_video(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(download_dir=Path(tmp) / "downloads")
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Live",
                channel="Example Channel",
            )

            command = build_download_command(config, stream, 1)

        expected_output = (
            Path(tmp)
            / "downloads"
            / "Example_Channel"
            / "LIVEVIDEO01"
            / "segment-001.%(ext)s"
        )
        self.assertIn("--live-from-start", command)
        self.assertIn("--continue", command)
        self.assertIn("--part", command)
        self.assertIn("--keep-fragments", command)
        self.assertIn("--progress", command)
        self.assertIn("--newline", command)
        self.assertIn("--no-playlist", command)
        self.assertIn(str(expected_output), command)
        self.assertNotIn("--write-subs", command)
        self.assertNotIn("live_chat", command)
        self.assertNotIn("-k", command)
        self.assertNotIn("--keep-video", command)
        self.assertNotIn("--download-archive", command)

    def test_keep_video_can_be_supplied_as_extra_arg(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(download_dir=Path(tmp), extra_yt_dlp_args=["-k"])
            stream = LiveStream(video_id="LIVEVIDEO01", url=video_url("LIVEVIDEO01"))

            command = build_download_command(config, stream, 1)

        self.assertIn("-k", command)

    def test_download_command_log_redacts_sensitive_args(self) -> None:
        logged = command_for_log(
            [
                "yt-dlp",
                "--cookies",
                "/secret/cookies.txt",
                "--add-header=Authorization: Bearer token",
                "https://example.test",
            ]
        )

        self.assertIn("--cookies '<redacted>'", logged)
        self.assertIn("'--add-header=<redacted>'", logged)
        self.assertNotIn("/secret/cookies.txt", logged)
        self.assertNotIn("Bearer token", logged)

    def test_continuation_segment_uses_live_from_start_for_gapless_resume(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(download_dir=Path(tmp))
            stream = LiveStream(video_id="LIVEVIDEO01", url=video_url("LIVEVIDEO01"))

            command = build_download_command(config, stream, 2)

        self.assertIn("--live-from-start", command)
        self.assertIn("--continue", command)

    def test_keep_fragments_can_be_disabled(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp),
                keep_fragments_for_resume=False,
            )
            stream = LiveStream(video_id="LIVEVIDEO01", url=video_url("LIVEVIDEO01"))

            command = build_download_command(config, stream, 1)

        self.assertNotIn("--keep-fragments", command)

    def test_live_chat_recording_keeps_media_command_video_audio_only(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp),
                record_live_chat=True,
            )
            stream = LiveStream(video_id="LIVEVIDEO01", url=video_url("LIVEVIDEO01"))

            command = build_download_command(config, stream, 1)

        self.assertNotIn("--write-subs", command)
        self.assertNotIn("--sub-langs", command)
        self.assertNotIn("live_chat", command)
        self.assertNotIn("--skip-download", command)
        format_index = command.index("--format")
        self.assertEqual(command[format_index + 1], "bestvideo*+bestaudio/best")

    def test_chat_video_rendering_keeps_media_command_video_audio_only(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp),
                render_live_chat_video=True,
            )
            stream = LiveStream(video_id="LIVEVIDEO01", url=video_url("LIVEVIDEO01"))

            command = build_download_command(config, stream, 1)

        self.assertNotIn("--write-subs", command)
        self.assertNotIn("--sub-langs", command)
        self.assertNotIn("live_chat", command)
        self.assertNotIn("--skip-download", command)
        format_index = command.index("--format")
        self.assertEqual(command[format_index + 1], "bestvideo*+bestaudio/best")

    def test_live_chat_command_downloads_only_chat_sidecar(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp),
                record_live_chat=True,
            )
            stream = LiveStream(video_id="LIVEVIDEO01", url=video_url("LIVEVIDEO01"))

            command = build_chat_download_command(config, stream, 1)

        self.assertIn("--skip-download", command)
        self.assertIn("--write-subs", command)
        sub_langs_index = command.index("--sub-langs")
        self.assertEqual(command[sub_langs_index + 1], "live_chat")
        self.assertNotIn("--keep-fragments", command)

    def test_user_format_is_not_overridden_when_recording_live_chat(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp),
                record_live_chat=True,
                extra_yt_dlp_args=["--format", "f136+f140/best"],
            )
            stream = LiveStream(video_id="LIVEVIDEO01", url=video_url("LIVEVIDEO01"))

            command = build_download_command(config, stream, 1)

        format_indexes = [
            index
            for index, arg in enumerate(command)
            if arg == "--format"
        ]
        self.assertEqual(len(format_indexes), 1)
        self.assertEqual(command[format_indexes[0] + 1], "f136+f140/best")
        self.assertNotIn("bestvideo*+bestaudio/best", command)
        self.assertNotIn("--write-subs", command)

    def test_output_template_groups_by_channel_video_id_and_segment(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(download_dir=Path(tmp))
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Late Night Stream",
                channel="Example Channel",
            )

            template = output_template_for(config, stream, 2)

        self.assertEqual(
            template,
            Path(tmp) / "Example_Channel" / "LIVEVIDEO01" / "segment-002.%(ext)s",
        )

    def test_finalized_segment_is_renamed_to_title_and_video_id(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(download_dir=Path(tmp))
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title='Late/Night: "Stream"',
                channel="Example Channel",
            )
            segment_dir = Path(tmp) / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            source = segment_dir / "segment-001.mp4"
            source.write_text("media", encoding="utf-8")

            renamed = rename_finalized_segment_file(
                config,
                stream,
                1,
                NULL_LOGGER,
            )

            target = segment_dir / 'Late_Night_ _Stream [LIVEVIDEO01].mp4'
            self.assertEqual(renamed, target)
            self.assertFalse(source.exists())
            self.assertEqual(target.read_text(encoding="utf-8"), "media")

    def test_continuation_finalized_segment_gets_part_suffix(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(download_dir=Path(tmp))
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Late Night Stream",
                channel="Example Channel",
            )
            segment_dir = Path(tmp) / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            source = segment_dir / "segment-002.mp4"
            source.write_text("media", encoding="utf-8")

            renamed = rename_finalized_segment_file(
                config,
                stream,
                2,
                NULL_LOGGER,
            )

            self.assertEqual(
                renamed,
                segment_dir / "Late Night Stream [LIVEVIDEO01] - part 002.mp4",
            )

    def test_live_chat_file_is_renamed_to_title_and_video_id(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(download_dir=Path(tmp))
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Late Night Stream",
                channel="Example Channel",
            )
            segment_dir = Path(tmp) / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            source = segment_dir / "segment-001.live_chat.json"
            source.write_text('{"replayChatItemAction":{}}', encoding="utf-8")

            renamed = rename_segment_chat_file(config, stream, 1, NULL_LOGGER)

            target = segment_dir / "Late Night Stream [LIVEVIDEO01].live_chat.json"
            self.assertEqual(renamed, target)
            self.assertFalse(source.exists())
            self.assertEqual(
                target.read_text(encoding="utf-8"),
                '{"replayChatItemAction":{}}',
            )

    def test_live_chat_part_file_is_finalized_and_renamed(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(download_dir=Path(tmp))
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Late Night Stream",
                channel="Example Channel",
            )
            segment_dir = Path(tmp) / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            source = segment_dir / "segment-001.live_chat.json.part"
            fragment = segment_dir / "segment-001.live_chat.json.part-Frag1"
            source.write_text('{"replayChatItemAction":{}}', encoding="utf-8")
            fragment.write_text("fragment", encoding="utf-8")

            renamed = rename_segment_chat_file(config, stream, 1, NULL_LOGGER)

            target = segment_dir / "Late Night Stream [LIVEVIDEO01].live_chat.json"
            self.assertEqual(renamed, target)
            self.assertFalse(source.exists())
            self.assertFalse(fragment.exists())
            self.assertEqual(
                target.read_text(encoding="utf-8"),
                '{"replayChatItemAction":{}}',
            )

    def test_timing_sidecar_is_renamed_to_title_and_video_id(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(download_dir=Path(tmp))
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Late Night Stream",
                channel="Example Channel",
            )
            segment_dir = Path(tmp) / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            source = segment_dir / "segment-001.timing.json"
            source.write_text('{"video_id":"LIVEVIDEO01","segment_index":1}', encoding="utf-8")

            renamed = rename_segment_timing_file(config, stream, 1, NULL_LOGGER)

            target = segment_dir / "Late Night Stream [LIVEVIDEO01].timing.json"
            self.assertEqual(renamed, target)
            self.assertTrue(target.exists())
            self.assertFalse(source.exists())

    def test_timing_sidecar_does_not_count_as_final_media(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(download_dir=Path(tmp))
            segment_dir = Path(tmp) / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            (segment_dir / "segment-001.timing.json").write_text(
                '{"video_id":"LIVEVIDEO01","segment_index":1}',
                encoding="utf-8",
            )

            restart_segment = choose_restart_segment(
                config,
                "LIVEVIDEO01",
                1,
                "Example Channel",
            )
            has_final_files = segment_has_final_files(
                config,
                "LIVEVIDEO01",
                1,
                "Example Channel",
            )

        self.assertFalse(has_final_files)
        self.assertEqual(restart_segment, 1)

    def test_existing_legacy_video_folder_is_reused_for_resume(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(download_dir=Path(tmp))
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                channel="Example Channel",
            )
            segment_dir = Path(tmp) / "LIVEVIDEO01"
            segment_dir.mkdir()
            (segment_dir / "segment-001.mp4.part").write_text("", encoding="utf-8")

            template = output_template_for(config, stream, 1)
            next_segment = choose_restart_segment(
                config,
                "LIVEVIDEO01",
                1,
                "Example Channel",
            )

        self.assertEqual(template, Path(tmp) / "LIVEVIDEO01" / "segment-001.%(ext)s")
        self.assertEqual(next_segment, 1)

    def test_partial_file_reuses_same_segment(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(download_dir=Path(tmp))
            segment_dir = Path(tmp) / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            (segment_dir / "segment-001.mp4.part").write_text("", encoding="utf-8")

            next_segment = choose_restart_segment(
                config,
                "LIVEVIDEO01",
                1,
                "Example Channel",
            )

        self.assertEqual(next_segment, 1)

    def test_finalized_file_uses_continuation_segment(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(download_dir=Path(tmp))
            segment_dir = Path(tmp) / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            (segment_dir / "segment-001.mp4").write_text("", encoding="utf-8")

            next_segment = choose_restart_segment(
                config,
                "LIVEVIDEO01",
                1,
                "Example Channel",
            )

        self.assertEqual(next_segment, 2)

    def test_ytdl_sidecar_file_reuses_same_segment(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(download_dir=Path(tmp))
            segment_dir = Path(tmp) / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            (segment_dir / "segment-001.mp4.ytdl").write_text("{}", encoding="utf-8")

            next_segment = choose_restart_segment(
                config,
                "LIVEVIDEO01",
                1,
                "Example Channel",
            )

        self.assertEqual(next_segment, 1)

    def test_live_chat_part_file_does_not_count_as_media_part(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(download_dir=Path(tmp))
            segment_dir = Path(tmp) / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            (segment_dir / "segment-001.live_chat.json.part").write_text(
                "chat",
                encoding="utf-8",
            )

            parts = segment_part_files(config, "LIVEVIDEO01", 1, "Example Channel")
            next_segment = choose_restart_segment(
                config,
                "LIVEVIDEO01",
                1,
                "Example Channel",
            )

        self.assertEqual(parts, [])
        self.assertEqual(next_segment, 1)

    def test_live_chat_part_does_not_block_next_segment_after_media_final(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(download_dir=Path(tmp))
            segment_dir = Path(tmp) / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            (segment_dir / "segment-001.mp4").write_text("media", encoding="utf-8")
            (segment_dir / "segment-001.live_chat.json.part").write_text(
                "chat",
                encoding="utf-8",
            )

            next_segment = choose_restart_segment(
                config,
                "LIVEVIDEO01",
                1,
                "Example Channel",
            )

        self.assertEqual(next_segment, 2)

    def test_fragment_intermediate_file_reuses_same_segment(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(download_dir=Path(tmp))
            segment_dir = Path(tmp) / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            (segment_dir / "segment-001.mp4.part-Frag1").write_text("", encoding="utf-8")

            next_segment = choose_restart_segment(
                config,
                "LIVEVIDEO01",
                1,
                "Example Channel",
            )

        self.assertEqual(next_segment, 1)

    def test_prepare_finalize_plan_uses_part_inputs_and_cleans_sidecars(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(download_dir=Path(tmp))
            segment_dir = Path(tmp) / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            audio_part = segment_dir / "segment-001.f140.mp4.part"
            video_part = segment_dir / "segment-001.f299.mp4.part"
            audio_part.write_text("audio", encoding="utf-8")
            video_part.write_text("video", encoding="utf-8")
            ytdl_file = segment_dir / "segment-001.f140.mp4.ytdl"
            fragment_file = segment_dir / "segment-001.f299.mp4.part-Frag2727.part"
            chat_file = segment_dir / "segment-001.live_chat.json"
            ytdl_file.write_text("{}", encoding="utf-8")
            fragment_file.write_text("", encoding="utf-8")
            chat_file.write_text("chat", encoding="utf-8")

            plan = prepare_finalize_plan(config, "LIVEVIDEO01", 1, "Example Channel")

            self.assertIsNotNone(plan)
            assert plan is not None
            self.assertEqual(plan.output_file, segment_dir / "segment-001.mp4")
            self.assertEqual(
                plan.input_files,
                [
                    segment_dir / "segment-001.f140.mp4.part",
                    segment_dir / "segment-001.f299.mp4.part",
                ],
            )
            self.assertIn(ytdl_file, plan.cleanup_files)
            self.assertIn(fragment_file, plan.cleanup_files)
            self.assertNotIn(chat_file, plan.input_files)
            self.assertNotIn(chat_file, plan.cleanup_files)
            self.assertFalse(plan.shortest)
            self.assertTrue(audio_part.exists())
            self.assertTrue(video_part.exists())

    def test_prepare_finalize_plan_accepts_mixed_final_and_part_inputs(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(download_dir=Path(tmp))
            segment_dir = Path(tmp) / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            audio_file = segment_dir / "segment-001.f140.mp4"
            video_part = segment_dir / "segment-001.f137.mp4.part"
            audio_file.write_text("audio", encoding="utf-8")
            video_part.write_text("video", encoding="utf-8")

            plan = prepare_finalize_plan(config, "LIVEVIDEO01", 1, "Example Channel")

            self.assertIsNotNone(plan)
            assert plan is not None
            self.assertEqual(plan.output_file, segment_dir / "segment-001.mp4")
            self.assertEqual(
                plan.input_files,
                [
                    segment_dir / "segment-001.f137.mp4.part",
                    segment_dir / "segment-001.f140.mp4",
                ],
            )
            self.assertTrue(plan.shortest)

    def test_restore_mixed_segment_moves_final_format_back_to_resumable_part(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(download_dir=Path(tmp))
            segment_dir = Path(tmp) / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            audio_file = segment_dir / "segment-001.f140.mp4"
            audio_part = segment_dir / "segment-001.f140.mp4.part"
            video_part = segment_dir / "segment-001.f137.mp4.part"
            audio_file.write_text("audio", encoding="utf-8")
            video_part.write_text("video", encoding="utf-8")
            (segment_dir / "segment-001.f140.mp4.part-Frag1").write_text(
                "a1",
                encoding="utf-8",
            )
            (segment_dir / "segment-001.f140.mp4.part-Frag2").write_text(
                "a2",
                encoding="utf-8",
            )
            (segment_dir / "segment-001.f140.mp4.part-Frag3.part").write_text(
                "unfinished",
                encoding="utf-8",
            )

            restored = restore_mixed_segment_for_resume(
                config,
                "LIVEVIDEO01",
                1,
                "Example Channel",
            )

            state = json.loads(
                (segment_dir / "segment-001.f140.mp4.ytdl").read_text(
                    encoding="utf-8"
                )
            )

            self.assertTrue(restored)
            self.assertFalse(audio_file.exists())
            self.assertTrue(audio_part.exists())
            self.assertEqual(state["downloader"]["current_fragment"]["index"], 2)

    def test_restore_mixed_segment_requires_kept_fragments(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(download_dir=Path(tmp))
            segment_dir = Path(tmp) / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            audio_file = segment_dir / "segment-001.f140.mp4"
            video_part = segment_dir / "segment-001.f137.mp4.part"
            audio_file.write_text("audio", encoding="utf-8")
            video_part.write_text("video", encoding="utf-8")

            restored = restore_mixed_segment_for_resume(
                config,
                "LIVEVIDEO01",
                1,
                "Example Channel",
            )

            self.assertFalse(restored)
            self.assertTrue(audio_file.exists())

    def test_default_post_exit_schedule_is_ten_minutes_every_thirty_seconds(self) -> None:
        self.assertEqual(DEFAULT_POST_EXIT_CHECK_SECONDS, list(range(30, 601, 30)))

    def test_catchup_tracker_waits_for_both_prefixed_formats(self) -> None:
        event = asyncio.Event()
        tracker = CatchupTracker(event)

        tracker.update("1: [download] 5.34GiB at 2.29MiB/s (frag 4924/4924)")
        self.assertFalse(event.is_set())

        tracker.update("2: [download] 385.90MiB at 136.86KiB/s (frag 2175/4924)")
        self.assertFalse(event.is_set())

        tracker.update("2: [download] 400.00MiB at 200.00KiB/s (frag 4923/4924)")
        self.assertTrue(event.is_set())

    def test_catchup_tracker_handles_single_unprefixed_format(self) -> None:
        event = asyncio.Event()
        tracker = CatchupTracker(event)

        tracker.update("[download] 100.00MiB at 1.00MiB/s (frag 99/100)")

        self.assertTrue(event.is_set())


class DownloadManagerRestartTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_restart_restores_finalized_formats_with_kept_fragments(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Late Night Stream",
                channel="Example Channel",
            )
            segment_dir = config.download_dir / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            audio_file = segment_dir / "segment-001.f140.mp4"
            video_file = segment_dir / "segment-001.f137.mp4"
            audio_file.write_text("audio", encoding="utf-8")
            video_file.write_text("video", encoding="utf-8")
            (segment_dir / "segment-001.f140.mp4.part-Frag2").write_text(
                "audio fragment",
                encoding="utf-8",
            )
            (segment_dir / "segment-001.f137.mp4.part-Frag5").write_text(
                "video fragment",
                encoding="utf-8",
            )
            state = StateStore(config.db_path)
            manager = DownloadManager(config, state, probe=None)  # type: ignore[arg-type]

            try:
                next_segment = await manager.choose_live_restart_segment(stream, 1)
                events = state.list_stream_events([stream.video_id], limit_per_stream=4)[
                    stream.video_id
                ]
            finally:
                state.close()

            audio_state = json.loads(
                (segment_dir / "segment-001.f140.mp4.ytdl").read_text(
                    encoding="utf-8"
                )
            )
            video_state = json.loads(
                (segment_dir / "segment-001.f137.mp4.ytdl").read_text(
                    encoding="utf-8"
                )
            )
            audio_file_exists = audio_file.exists()
            video_file_exists = video_file.exists()
            audio_part_exists = (segment_dir / "segment-001.f140.mp4.part").exists()
            video_part_exists = (segment_dir / "segment-001.f137.mp4.part").exists()

        self.assertEqual(next_segment, 1)
        self.assertFalse(audio_file_exists)
        self.assertFalse(video_file_exists)
        self.assertTrue(audio_part_exists)
        self.assertTrue(video_part_exists)
        self.assertEqual(audio_state["downloader"]["current_fragment"]["index"], 2)
        self.assertEqual(video_state["downloader"]["current_fragment"]["index"], 5)
        self.assertIn(
            "Restored finalized segment=001 from kept fragments",
            [event.message for event in events],
        )


class DownloadManagerTranscriptionTests(unittest.IsolatedAsyncioTestCase):
    async def test_segment_timing_sidecar_records_capture_anchors(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                live_from_start=True,
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Late Night Stream",
                channel="Example Channel",
                raw={"actual_start_timestamp": "2026-05-17T21:45:00+00:00"},
            )
            state = StateStore(config.db_path)
            manager = DownloadManager(config, state, probe=None)  # type: ignore[arg-type]

            manager.write_segment_timing_started(
                stream,
                1,
                media_started_at="2026-05-17T21:45:05+00:00",
            )
            manager.update_segment_timing(
                stream,
                1,
                chat_started_at="2026-05-17T21:46:05+00:00",
                last_exit_at="2026-05-17T21:50:00+00:00",
            )
            timing = read_chat_timing(
                segment_timing_file(config, stream.video_id, 1, stream.channel)
            )
            state.close()

        self.assertIsNotNone(timing)
        assert timing is not None
        self.assertEqual(timing.video_id, stream.video_id)
        self.assertEqual(timing.segment_index, 1)
        self.assertEqual(timing.stream_started_at, "2026-05-17T21:45:00+00:00")
        self.assertEqual(timing.media_started_at, "2026-05-17T21:45:05+00:00")
        self.assertEqual(timing.chat_started_at, "2026-05-17T21:46:05+00:00")
        self.assertTrue(timing.media_live_from_start)
        self.assertEqual(timing.last_exit_at, "2026-05-17T21:50:00+00:00")

    async def test_finish_ended_stream_refreshes_chat_before_optional_render(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                record_live_chat=True,
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Late Night Stream",
                channel="Example Channel",
            )
            segment_dir = config.download_dir / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            (segment_dir / "segment-001.mp4").write_text("media", encoding="utf-8")
            (segment_dir / "segment-001.live_chat.json").write_text("chat", encoding="utf-8")
            (segment_dir / "segment-001.timing.json").write_text(
                '{"video_id":"LIVEVIDEO01","segment_index":1}',
                encoding="utf-8",
            )
            state = StateStore(config.db_path)
            state.mark_downloading(stream, 1)
            state.mark_exited(stream.video_id, 0)
            manager = DownloadManager(config, state, probe=None)  # type: ignore[arg-type]
            calls: list[tuple[Path, Path, str | None, Path | None]] = []

            def fake_refresh(
                _config: BotConfig,
                *,
                video_url: str,
                media_file: Path,
                chat_file: Path,
                last_exit_at: str | None,
                timing_file: Path | None,
                **_kwargs: object,
            ) -> ChatRefreshResult:
                calls.append((media_file, chat_file, last_exit_at, timing_file))
                return ChatRefreshResult(
                    ok=True,
                    changed=True,
                    source="replay",
                    message="Refreshed",
                )

            async def fake_to_thread(func: object, *args: object, **kwargs: object) -> object:
                assert callable(func)
                return func(*args, **kwargs)

            try:
                with (
                    patch("onlysavemevods.downloader.refresh_chat_sidecar", fake_refresh),
                    patch("onlysavemevods.downloader.asyncio.to_thread", fake_to_thread),
                ):
                    await manager.finish_ended_stream(stream, 1)
            finally:
                state.close()

            media_file = segment_dir / "Late Night Stream [LIVEVIDEO01].mp4"
            chat_file = segment_dir / "Late Night Stream [LIVEVIDEO01].live_chat.json"
            timing_file = segment_dir / "Late Night Stream [LIVEVIDEO01].timing.json"

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], media_file)
        self.assertEqual(calls[0][1], chat_file)
        self.assertIsNotNone(calls[0][2])
        self.assertEqual(calls[0][3], timing_file)

    async def test_finish_ended_stream_transcribes_named_media_file(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                transcribe_subtitles=True,
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                title="Late Night Stream",
                channel="Example Channel",
            )
            segment_dir = config.download_dir / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            (segment_dir / "segment-001.mp4").write_text("media", encoding="utf-8")
            state = StateStore(config.db_path)
            state.mark_downloading(stream, 1)
            manager = DownloadManager(config, state, probe=None)  # type: ignore[arg-type]
            transcribe = AsyncMock(return_value=True)

            try:
                with patch("onlysavemevods.downloader.transcribe_media_file", transcribe):
                    await manager.finish_ended_stream(stream, 1)
            finally:
                state.close()

            target = segment_dir / "Late Night Stream [LIVEVIDEO01].mp4"
            self.assertTrue(target.exists())
            transcribe.assert_awaited_once()
            self.assertEqual(transcribe.await_args.args[0], config)
            self.assertEqual(transcribe.await_args.args[1], target)
