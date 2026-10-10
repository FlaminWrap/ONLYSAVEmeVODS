import asyncio
from dataclasses import replace
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, Mock, patch

from onlysavemevods.config import BotConfig
from onlysavemevods.daemon import OnlySaveMeVodsDaemon
from onlysavemevods.downloader import (
    DownloadManager,
    FinalizeMediaStream,
    segment_directory,
)
from onlysavemevods.job_tracker import clear_tracked_jobs
from onlysavemevods.models import LiveStream, video_url
from onlysavemevods.state import StateStore


class FinalizationLifecycleTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        clear_tracked_jobs()
        self.tmp = TemporaryDirectory()
        root = Path(self.tmp.name)
        self.config = BotConfig(
            download_dir=root / "downloads",
            state_dir=root / "state",
            web_enabled=False,
        )
        self.state = StateStore(self.config.db_path)
        self.stream = LiveStream(
            video_id="youtube:6WKBYU9Rg-c",
            url=video_url("6WKBYU9Rg-c"),
            channel="OGGEEZERLIVE",
            platform="youtube",
            is_live=True,
        )
        self.state.mark_downloading(self.stream, 1)
        self.state.lock_youtube_video_format(
            self.stream.video_id, format_id="299", codec="h264", selector="299+140"
        )
        self.state.mark_exited(self.stream.video_id, 1)
        self.retry_permits: asyncio.Queue[None] = asyncio.Queue()

        async def controlled_sleep(_delay: float) -> None:
            await self.retry_permits.get()

        logger = logging.Logger("finalization-lifecycle-test")
        logger.addHandler(logging.NullHandler())
        self.manager = DownloadManager(
            self.config,
            self.state,
            probe=Mock(),
            sleep_func=controlled_sleep,
            probe_video_func=AsyncMock(
                return_value=replace(self.stream, is_live=False)
            ),
            logger=logger,
        )

    async def asyncTearDown(self) -> None:
        await self.manager.stop_all()
        self.state.close()
        clear_tracked_jobs()
        self.tmp.cleanup()

    def mock_successful_outputs(self) -> None:
        self.manager.rename_finalized_segments = Mock(return_value=[])
        self.manager.finalize_powerchat_sidecars = Mock()
        self.manager.enqueue_finalized_post_processing = Mock(return_value=[])
        self.manager.process_pending_post_processing = AsyncMock()

    async def test_missing_video_records_failure_and_preserves_every_source(self) -> None:
        directory = segment_directory(
            self.config, self.stream.video_id, self.stream.channel
        )
        directory.mkdir(parents=True)
        audio = directory / "segment-001.f140.m4a"
        saved_files = {
            audio: b"saved audio track",
            directory / "segment-001.f140.m4a.part-Frag1": b"saved audio fragment",
            directory / "segment-001.f140.m4a.ytdl": b"saved resume state",
            directory / "segment-001.f299.mp4.part-Frag1": b"saved video fragment",
        }
        for path, content in saved_files.items():
            path.write_bytes(content)
        audio_metadata = FinalizeMediaStream(
            path=audio,
            input_index=0,
            stream_index=0,
            codec_type="audio",
            duration=60,
            size=audio.stat().st_size,
            partial=False,
        )
        with (
            patch(
                "onlysavemevods.downloader.probe_finalize_media_streams",
                return_value=[audio_metadata],
            ),
            patch(
                "onlysavemevods.downloader.asyncio.create_subprocess_exec",
                new=AsyncMock(),
            ) as spawn,
        ):
            await self.manager.finish_ended_stream(
                self.stream,
                1,
                expected_status="checking_after_exit",
                end_confirmed=True,
            )

        self.assertEqual(
            self.state.get_stream(self.stream.video_id).status, "finalization_failed"
        )
        events = self.state.list_stream_events([self.stream.video_id])[
            self.stream.video_id
        ]
        self.assertTrue(
            any(
                "Source confirmed ended" in event.message
                and "Missing video" in event.message
                for event in events
            )
        )
        self.assertEqual(set(directory.iterdir()), set(saved_files))
        for path, content in saved_files.items():
            self.assertEqual(path.read_bytes(), content)
        spawn.assert_not_awaited()
        self.assertIn(self.stream.video_id, self.manager._finalization_retry_tasks)

    async def test_retry_reconfirms_end_and_completes_after_media_is_restored(self) -> None:
        self.mock_successful_outputs()
        self.manager.finalize_ended_segment = AsyncMock(side_effect=[False, True])
        await self.manager.finish_ended_stream(
            self.stream, 1, expected_status="checking_after_exit", end_confirmed=True
        )
        self.assertEqual(
            self.state.get_stream(self.stream.video_id).status, "finalization_failed"
        )
        self.manager.probe_video.assert_not_awaited()
        retry = self.manager._finalization_retry_tasks[self.stream.video_id]
        self.retry_permits.put_nowait(None)
        await asyncio.wait_for(retry, timeout=1)

        self.manager.probe_video.assert_awaited_once_with(self.stream.url)
        self.assertEqual(self.manager.finalize_ended_segment.await_count, 2)
        self.assertEqual(self.state.get_stream(self.stream.video_id).status, "ended")
        self.assertNotIn(self.stream.video_id, self.manager._finalization_retry_tasks)

    async def test_failed_merge_returns_to_live_checks_if_source_resumes(self) -> None:
        self.state.mark_finalization_failed(self.stream.video_id)
        self.manager.probe_video = AsyncMock(return_value=self.stream)
        self.manager._youtube_endlist_confirmed = AsyncMock(return_value=False)
        self.manager.handle_post_exit = AsyncMock()
        self.manager.finalize_ended_segment = AsyncMock()

        self.manager.resume_finalization_retry(self.stream, 1)
        retry = self.manager._finalization_retry_tasks[self.stream.video_id]
        self.retry_permits.put_nowait(None)
        await asyncio.wait_for(retry, timeout=1)

        self.assertEqual(
            self.state.get_stream(self.stream.video_id).status, "checking_after_exit"
        )
        self.manager.handle_post_exit.assert_awaited_once_with(
            self.stream, 1, expected_status="checking_after_exit"
        )
        self.manager.finalize_ended_segment.assert_not_awaited()

    async def test_service_restart_recovers_interrupted_merge_and_resumes_retry(self) -> None:
        self.assertTrue(
            self.state.compare_and_set_stream_status(
                self.stream.video_id,
                expected_status="checking_after_exit",
                new_status="finalizing",
            )
        )
        daemon = OnlySaveMeVodsDaemon(self.config)
        daemon.downloads.resume_finalization_retry = Mock()
        daemon.downloads.resume_pending_post_processing_jobs = Mock()
        daemon.downloads.stop_all = AsyncMock()
        daemon.stop()

        await daemon.run()

        self.assertEqual(
            self.state.get_stream(self.stream.video_id).status, "finalization_failed"
        )
        daemon.downloads.resume_finalization_retry.assert_called_once()
        recovered, segment = daemon.downloads.resume_finalization_retry.call_args.args
        self.assertEqual(recovered.video_id, self.stream.video_id)
        self.assertEqual(segment, 1)

    async def test_background_merge_crash_releases_claim_and_schedules_new_retry(self) -> None:
        self.mock_successful_outputs()
        self.state.mark_finalization_failed(self.stream.video_id)
        self.manager.finalize_ended_segment = AsyncMock(
            side_effect=[RuntimeError("mux worker crashed"), True]
        )
        self.manager.resume_finalization_retry(self.stream, 1)
        first_retry = self.manager._finalization_retry_tasks[self.stream.video_id]
        self.retry_permits.put_nowait(None)
        with self.assertRaisesRegex(RuntimeError, "mux worker crashed"):
            await asyncio.wait_for(first_retry, timeout=1)
        await asyncio.sleep(0)

        self.assertEqual(
            self.state.get_stream(self.stream.video_id).status, "finalization_failed"
        )
        replacement = self.manager._finalization_retry_tasks[self.stream.video_id]
        self.assertIsNot(replacement, first_retry)
        self.assertNotIn(self.stream.video_id, self.manager._finalizing_video_ids)
        events = self.state.list_stream_events([self.stream.video_id])[
            self.stream.video_id
        ]
        self.assertTrue(any("mux worker crashed" in event.message for event in events))

        self.retry_permits.put_nowait(None)
        await asyncio.wait_for(replacement, timeout=1)
        self.assertEqual(self.state.get_stream(self.stream.video_id).status, "ended")
        self.assertEqual(self.manager.probe_video.await_count, 2)
