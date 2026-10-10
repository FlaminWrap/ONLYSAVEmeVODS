import asyncio
from dataclasses import replace
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import AsyncMock, Mock, patch

from onlysavemevods.config import BotConfig
from onlysavemevods.downloader import DownloadManager, segment_directory
from onlysavemevods.job_tracker import clear_tracked_jobs, list_tracked_jobs
from onlysavemevods.models import LiveStream, video_url
from onlysavemevods.state import StateStore
from onlysavemevods import web
from onlysavemevods.youtube import ConfirmedVideoRemovalError, YtDlpError


class AutomaticVodRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        clear_tracked_jobs()
        self.tmp = TemporaryDirectory()
        root = Path(self.tmp.name)
        self.config = BotConfig(
            download_dir=root / "downloads",
            state_dir=root / "state",
            auto_redownload_failed_finalization=True,
            extra_yt_dlp_args=["--cookies", "/private/test-cookies.txt"],
        )
        self.state = StateStore(self.config.db_path)
        self.stream = LiveStream(
            video_id="youtube:6WKBYU9Rg-c",
            url=video_url("6WKBYU9Rg-c"),
            title="Saved live recording",
            channel="OGGEEZERLIVE",
            source="@OGGEEZERLIVE",
            platform="youtube",
            is_live=True,
        )
        self.state.mark_downloading(self.stream, 2)
        self.state.mark_exited(self.stream.video_id, 1)
        self.state.mark_finalization_failed(self.stream.video_id)
        self.info = {
            "id": "6WKBYU9Rg-c",
            "title": "Published VOD",
            "webpage_url": self.stream.url,
            "channel": "OGGEEZERLIVE",
            "is_live": False,
            "live_status": "was_live",
            "duration": 8833,
        }

    def tearDown(self) -> None:
        self.state.close()
        clear_tracked_jobs()
        self.tmp.cleanup()

    def attempt(self, *, info=None, error=None, now=1000, **kwargs):
        with (
            patch("onlysavemevods.web.time.time", return_value=now),
            patch(
                "onlysavemevods.web.YtDlpRunner.run_json",
                return_value=self.info if info is None else info,
                side_effect=error,
            ) as probe,
            patch("onlysavemevods.web.Thread") as thread,
            patch(
                "onlysavemevods.web.queue_vod_download_job",
                wraps=web.queue_vod_download_job,
            ) as queue,
        ):
            started = web.start_automatic_vod_redownload_job(
                self.config, self.stream.video_id, **kwargs
            )
        return started, probe, thread, queue

    def test_confirmed_ended_vod_queues_copy_and_preserves_original_recording(self) -> None:
        directory = segment_directory(
            self.config, self.stream.video_id, self.stream.channel
        )
        directory.mkdir(parents=True)
        original = directory / "segment-002.f299.mp4.part-Frag7"
        original.write_bytes(b"saved video fragment")
        before = self.state.get_stream(self.stream.video_id)

        started, probe, thread, queue = self.attempt()

        self.assertTrue(started)
        probe.assert_called_once()
        command = probe.call_args.args[0]
        self.assertIn(self.stream.url, command)
        self.assertIn("--cookies", command)
        self.assertIn("/private/test-cookies.txt", command)
        thread.return_value.start.assert_called_once()
        self.assertTrue(queue.call_args.kwargs["automatic"])
        self.assertEqual(queue.call_args.kwargs["previous_status"], "finalization_failed")
        output_template = queue.call_args.args[4]
        self.assertEqual(output_template.parent, directory)
        self.assertNotEqual(output_template.name, "segment-002.%(ext)s")
        after = self.state.get_stream(self.stream.video_id)
        self.assertEqual(after.status, "downloading")
        self.assertEqual(after.segment_index, before.segment_index)
        self.assertEqual(after.last_started_at, before.last_started_at)
        self.assertEqual(after.last_exit_at, before.last_exit_at)
        self.assertEqual(after.url, before.url)
        self.assertEqual(after.title, before.title)
        self.assertEqual(original.read_bytes(), b"saved video fragment")

    def test_disabled_option_does_not_probe_or_queue(self) -> None:
        self.config.auto_redownload_failed_finalization = False

        started, probe, thread, queue = self.attempt()

        self.assertFalse(started)
        probe.assert_not_called()
        queue.assert_not_called()
        thread.assert_not_called()

    def test_other_recording_statuses_do_not_probe_or_queue(self) -> None:
        statuses = (
            "downloading", "checking_after_exit", "stalled", "finalizing", "ended"
        )
        for status in statuses:
            with self.subTest(status=status):
                current = self.state.get_stream(self.stream.video_id).status
                self.assertTrue(self.state.compare_and_set_stream_status(
                    self.stream.video_id,
                    expected_status=current,
                    new_status=status,
                ))
                started, probe, thread, queue = self.attempt()
                self.assertFalse(started)
                probe.assert_not_called()
                queue.assert_not_called()
                thread.assert_not_called()

    def test_active_processing_job_delays_automatic_download(self) -> None:
        with patch(
            "onlysavemevods.web.active_dashboard_job_kinds", return_value=["Transcription"]
        ):
            started, probe, thread, queue = self.attempt()

        self.assertFalse(started)
        probe.assert_not_called()
        queue.assert_not_called()
        thread.assert_not_called()

    def test_unavailable_vod_retries_after_cooldown(self) -> None:
        started, probe, thread, queue = self.attempt(
            error=YtDlpError("temporary metadata request failure"), now=1000
        )
        self.assertFalse(started)
        probe.assert_called_once()
        queue.assert_not_called()
        thread.assert_not_called()
        self.assertEqual(
            self.state.get_stream(self.stream.video_id).status, "finalization_failed"
        )

        started, probe, thread, queue = self.attempt(now=1299)
        self.assertFalse(started)
        probe.assert_not_called()
        queue.assert_not_called()

        started, probe, thread, queue = self.attempt(now=1300)
        self.assertTrue(started)
        probe.assert_called_once()
        thread.return_value.start.assert_called_once()

    def assert_metadata_is_not_queued(self, **changes) -> None:
        started, probe, thread, queue = self.attempt(info={**self.info, **changes})
        self.assertFalse(started)
        probe.assert_called_once()
        queue.assert_not_called()
        thread.assert_not_called()
        self.assertEqual(
            self.state.get_stream(self.stream.video_id).status, "finalization_failed"
        )

    def test_live_broadcast_is_not_downloaded_as_a_vod(self) -> None:
        self.assert_metadata_is_not_queued(is_live=True, live_status="is_live")

    def test_upcoming_broadcast_is_not_downloaded_as_a_vod(self) -> None:
        self.assert_metadata_is_not_queued(live_status="is_upcoming")

    def test_platform_processing_vod_is_retried_later(self) -> None:
        self.assert_metadata_is_not_queued(live_status="post_live")

    def test_different_video_id_is_not_downloaded(self) -> None:
        self.assert_metadata_is_not_queued(
            id="ANOTHERID01", webpage_url=video_url("ANOTHERID01")
        )

    def test_deletion_error_does_not_queue_a_copy(self) -> None:
        started, probe, thread, queue = self.attempt(error=ConfirmedVideoRemovalError(
            "ERROR: [youtube] 6WKBYU9Rg-c: This video has been removed "
            "for violating YouTube's Terms of Service"
        ))

        self.assertFalse(started)
        probe.assert_called_once()
        queue.assert_not_called()
        thread.assert_not_called()
        self.assertTrue(
            self.state.get_automatic_vod_recovery(self.stream.video_id).blocked
        )

        started, probe, thread, queue = self.attempt(now=10000)
        self.assertFalse(started)
        probe.assert_not_called()
        queue.assert_not_called()

    def test_disabling_automatic_recovery_during_probe_prevents_queueing(self) -> None:
        def disable(_command, **_kwargs):
            self.config.auto_redownload_failed_finalization = False
            return self.info

        started, probe, thread, queue = self.attempt(error=disable)

        self.assertFalse(started)
        self.assertFalse(self.config.auto_redownload_failed_finalization)
        probe.assert_called_once()
        queue.assert_not_called()
        thread.assert_not_called()

    def test_failed_download_restores_failure_and_reuses_copy_path_after_restart(self) -> None:
        started, _, _, queue = self.attempt()
        self.assertTrue(started)
        original_template = queue.call_args.args[4]
        process = Mock(stdout=["ERROR: transient download failure"], wait=Mock(return_value=1))
        with patch("onlysavemevods.web.subprocess.Popen", return_value=process):
            web.run_automatic_vod_download_job(
                *queue.call_args.args, previous_status="finalization_failed"
            )

        self.assertEqual(
            self.state.get_stream(self.stream.video_id).status, "finalization_failed"
        )
        recovery = self.state.get_automatic_vod_recovery(self.stream.video_id)
        self.assertFalse(recovery.in_progress)
        self.assertFalse(recovery.completed)
        self.state.close()
        clear_tracked_jobs()
        self.state = StateStore(self.config.db_path)

        started, _, _, queue = self.attempt(now=1300)

        self.assertTrue(started)
        self.assertEqual(queue.call_args.args[4], original_template)
        self.assertEqual(
            self.state.get_automatic_vod_recovery(self.stream.video_id).attempts, 2
        )

    def test_unexpected_worker_crash_releases_claim_and_preserves_failure(self) -> None:
        started, _, _, queue = self.attempt()
        self.assertTrue(started)
        with patch(
            "onlysavemevods.web.subprocess.Popen", side_effect=RuntimeError("worker crashed")
        ):
            web.run_automatic_vod_download_job(
                *queue.call_args.args, previous_status="finalization_failed"
            )

        self.assertEqual(
            self.state.get_stream(self.stream.video_id).status, "finalization_failed"
        )
        self.assertFalse(
            self.state.get_automatic_vod_recovery(self.stream.video_id).in_progress
        )
        self.assertEqual(list_tracked_jobs()[0].status, "failed")

    def test_copy_without_video_does_not_complete_recovery(self) -> None:
        started, _, _, queue = self.attempt()
        self.assertTrue(started)
        output_template = queue.call_args.args[4]
        media = Path(str(output_template).replace("%(ext)s", "mp4"))
        media.write_bytes(b"downloaded audio-only media")
        process = Mock(stdout=[], wait=Mock(return_value=0))
        with (
            patch("onlysavemevods.web.subprocess.Popen", return_value=process),
            patch(
                "onlysavemevods.web.probe_finalize_media_streams",
                return_value=[Mock(codec_type="audio")],
            ),
            patch("onlysavemevods.web.run_youtube_vod_chat_download_job") as chat,
        ):
            web.run_automatic_vod_download_job(
                *queue.call_args.args, previous_status="finalization_failed"
            )

        chat.assert_not_called()
        self.assertEqual(
            self.state.get_stream(self.stream.video_id).status, "finalization_failed"
        )
        self.assertFalse(
            self.state.get_automatic_vod_recovery(self.stream.video_id).completed
        )
        self.assertEqual(list_tracked_jobs()[0].status, "failed")
        self.assertEqual(media.read_bytes(), b"downloaded audio-only media")

    def test_valid_copy_completes_recovery_without_removing_saved_fragments(self) -> None:
        started, _, _, queue = self.attempt()
        self.assertTrue(started)
        output_template = queue.call_args.args[4]
        media = Path(str(output_template).replace("%(ext)s", "mp4"))
        media.write_bytes(b"valid replacement VOD")
        original = output_template.parent / "segment-002.f299.mp4.part-Frag7"
        original.write_bytes(b"original fragment")
        process = Mock(stdout=[], wait=Mock(return_value=0))
        with (
            patch("onlysavemevods.web.subprocess.Popen", return_value=process),
            patch(
                "onlysavemevods.web.probe_finalize_media_streams",
                return_value=[Mock(codec_type="video"), Mock(codec_type="audio")],
            ),
            patch(
                "onlysavemevods.web.run_youtube_vod_chat_download_job",
                return_value=(False, "chat unavailable"),
            ),
        ):
            web.run_automatic_vod_download_job(
                *queue.call_args.args, previous_status="finalization_failed"
            )

        self.assertEqual(self.state.get_stream(self.stream.video_id).status, "ended")
        recovery = self.state.get_automatic_vod_recovery(self.stream.video_id)
        self.assertTrue(recovery.completed)
        self.assertFalse(recovery.in_progress)
        self.assertEqual(list_tracked_jobs()[0].status, "done")
        self.assertEqual(original.read_bytes(), b"original fragment")
        self.assertTrue(self.state.compare_and_set_stream_status(
            self.stream.video_id,
            expected_status="ended",
            new_status="finalization_failed",
        ))
        started, probe, thread, queue = self.attempt(now=10000)
        self.assertFalse(started)
        probe.assert_not_called()
        queue.assert_not_called()

    def test_service_stop_during_probe_prevents_queueing(self) -> None:
        can_start = Mock(side_effect=[True, False])
        started, probe, thread, queue = self.attempt(can_start=can_start)

        self.assertFalse(started)
        probe.assert_called_once()
        self.assertEqual(can_start.call_count, 2)
        queue.assert_not_called()
        thread.assert_not_called()


class AutomaticVodRecoveryLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        clear_tracked_jobs()
        self.tmp = TemporaryDirectory()
        root = Path(self.tmp.name)
        self.config = BotConfig(
            download_dir=root / "downloads",
            state_dir=root / "state",
            auto_redownload_failed_finalization=True,
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
        self.state.mark_exited(self.stream.video_id, 1)
        self.sleep_started = asyncio.Event()
        self.sleep_permits = asyncio.Queue()

        async def controlled_sleep(_delay):
            self.sleep_started.set()
            await self.sleep_permits.get()

        logger = logging.Logger("automatic-vod-recovery-test")
        logger.addHandler(logging.NullHandler())
        self.manager = DownloadManager(
            self.config,
            self.state,
            probe=Mock(),
            sleep_func=controlled_sleep,
            probe_video_func=AsyncMock(return_value=replace(self.stream, is_live=False)),
            logger=logger,
        )

    async def asyncTearDown(self) -> None:
        await self.manager.stop_all()
        self.state.close()
        clear_tracked_jobs()
        self.tmp.cleanup()

    async def test_new_finalization_failure_starts_automatic_recovery_immediately(self) -> None:
        self.manager.finalize_ended_segment = AsyncMock(return_value=False)
        self.manager.maybe_redownload_failed_finalization = AsyncMock(return_value=True)

        await self.manager.finish_ended_stream(
            self.stream, 1, expected_status="checking_after_exit", end_confirmed=True
        )

        self.assertEqual(
            self.state.get_stream(self.stream.video_id).status, "finalization_failed"
        )
        self.manager.maybe_redownload_failed_finalization.assert_awaited_once_with(self.stream)
        self.assertNotIn(self.stream.video_id, self.manager._finalization_retry_tasks)

    async def test_historical_failure_tries_vod_before_waiting_for_finalization_retry(self) -> None:
        self.state.mark_finalization_failed(self.stream.video_id)
        self.manager.maybe_redownload_failed_finalization = AsyncMock(return_value=True)

        self.manager.resume_finalization_retry(self.stream, 1)
        retry = self.manager._finalization_retry_tasks[self.stream.video_id]
        await asyncio.wait_for(retry, timeout=1)

        self.manager.maybe_redownload_failed_finalization.assert_awaited_once_with(self.stream)
        self.assertFalse(self.sleep_started.is_set())
        self.manager.probe_video.assert_not_awaited()

    async def test_disabled_option_does_not_start_background_vod_probe(self) -> None:
        self.config.auto_redownload_failed_finalization = False
        with patch("onlysavemevods.web.start_automatic_vod_redownload_job") as start:
            result = await self.manager.maybe_redownload_failed_finalization(self.stream)
        self.assertFalse(result)
        start.assert_not_called()

    async def test_stopping_manager_does_not_start_background_vod_probe(self) -> None:
        self.manager._stopping = True
        with patch("onlysavemevods.web.start_automatic_vod_redownload_job") as start:
            result = await self.manager.maybe_redownload_failed_finalization(self.stream)
        self.assertFalse(result)
        start.assert_not_called()
