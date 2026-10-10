from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, MagicMock, patch
import asyncio
import logging
import unittest

from onlysavemevods.config import BotConfig, DEFAULT_POST_EXIT_CHECK_SECONDS
from onlysavemevods.downloader import (
    DownloadManager,
    FinalizeMediaStream,
    FinalizeOutputValidation,
    segment_directory,
)
from onlysavemevods.models import LiveStream, video_url
from onlysavemevods.sources import KickChannelOfflineError
from onlysavemevods.state import StateStore
from onlysavemevods.youtube import (
    ConfirmedLiveEndError,
    ConfirmedLiveTerminationError,
    ConfirmedVideoRemovalError,
    TerminalVideoUnavailableError,
    YouTubeLiveEdge,
)


NULL_LOGGER = logging.getLogger("tests.null")
NULL_LOGGER.addHandler(logging.NullHandler())
NULL_LOGGER.propagate = False


class SequenceProbe:
    def __init__(self, streams: list[LiveStream | Exception]) -> None:
        self.streams = streams
        self.calls = 0
        self.urls: list[str] = []

    def probe_video(self, url: str) -> LiveStream:
        self.calls += 1
        self.urls.append(url)
        result = self.streams.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    async def probe_video_async(self, url: str) -> LiveStream:
        return self.probe_video(url)


class RecordingDownloadManager(DownloadManager):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.started: list[tuple[LiveStream, int | None]] = []

    async def start_stream(
        self,
        stream: LiveStream,
        *,
        segment_index: int | None = None,
    ) -> bool:
        self.started.append((stream, segment_index))
        return True


class FakeProcess:
    def __init__(self) -> None:
        self.returncode: int | None = None
        self.terminated = False
        self.killed = False

    def terminate(self) -> None:
        self.terminated = True
        self.returncode = -15

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9

    async def wait(self) -> int:
        return self.returncode or 0


class PostExitTests(unittest.IsolatedAsyncioTestCase):
    async def test_old_frozen_youtube_edge_prevents_metadata_driven_restart(
        self,
    ) -> None:
        sleeps: list[float] = []

        async def fake_sleep(delay: float) -> None:
            sleeps.append(delay)

        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                youtube_stale_live_timeout_seconds=900,
                post_exit_check_seconds=[0],
            )
            original = LiveStream(
                video_id="youtube:LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                platform="youtube",
                is_live=True,
            )
            still_reported_live = LiveStream(
                video_id=original.video_id,
                url=original.url,
                platform="youtube",
                is_live=True,
            )
            state = StateStore(config.db_path)
            state.upsert_detected(original)
            state.mark_exited(original.video_id, 0)
            probe = SequenceProbe([still_reported_live])
            old_edge = YouTubeLiveEdge(
                9655,
                datetime.now(timezone.utc) - timedelta(hours=1),
            )
            edge_probe = AsyncMock(side_effect=[old_edge, old_edge])
            manager = RecordingDownloadManager(
                config,
                state,
                probe,  # type: ignore[arg-type]
                sleep_func=fake_sleep,
                probe_video_func=probe.probe_video_async,
                probe_youtube_live_edge_func=edge_probe,
                logger=NULL_LOGGER,
            )

            with patch.object(
                manager,
                "monitor_stalled_youtube",
                new=AsyncMock(),
            ) as monitor:
                await manager.handle_post_exit(original, 1)
            record = state.get_stream(original.video_id)
            state.close()

        self.assertEqual(manager.started, [])
        self.assertEqual(edge_probe.await_count, 2)
        self.assertEqual(sleeps, [30])
        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(record.status, "stalled")
        self.assertTrue(record.youtube_stale_detected_at)
        monitor.assert_awaited_once_with(original, 1)

    async def test_old_frozen_edge_also_prevents_planned_reconnect(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                youtube_stale_live_timeout_seconds=900,
            )
            stream = LiveStream(
                video_id="youtube:LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                platform="youtube",
                is_live=True,
            )
            state = StateStore(config.db_path)
            state.upsert_detected(stream)
            state.mark_exited(stream.video_id, 0)
            probe = SequenceProbe([stream])
            old_edge = YouTubeLiveEdge(
                9655,
                datetime.now(timezone.utc) - timedelta(hours=1),
            )
            edge_probe = AsyncMock(side_effect=[old_edge, old_edge])
            manager = RecordingDownloadManager(
                config,
                state,
                probe,  # type: ignore[arg-type]
                sleep_func=AsyncMock(),
                probe_video_func=probe.probe_video_async,
                probe_youtube_live_edge_func=edge_probe,
                logger=NULL_LOGGER,
            )

            with patch.object(
                manager,
                "monitor_stalled_youtube",
                new=AsyncMock(),
            ) as monitor:
                await manager.handle_planned_reconnect(stream, 1)
            record = state.get_stream(stream.video_id)
            state.close()

        self.assertEqual(manager.started, [])
        self.assertEqual(edge_probe.await_count, 2)
        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(record.status, "stalled")
        self.assertTrue(record.youtube_stale_detected_at)
        monitor.assert_awaited_once_with(stream, 1)

    async def test_fresh_youtube_edge_must_advance_before_restart(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                youtube_stale_live_timeout_seconds=900,
                post_exit_check_seconds=[0],
            )
            original = LiveStream(
                video_id="youtube:LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                platform="youtube",
                is_live=True,
            )
            still_live = LiveStream(
                video_id=original.video_id,
                url=original.url,
                platform="youtube",
                is_live=True,
            )
            state = StateStore(config.db_path)
            state.upsert_detected(original)
            state.mark_exited(original.video_id, 0)
            probe = SequenceProbe([still_live])
            fresh_edge = YouTubeLiveEdge(
                9655,
                datetime.now(timezone.utc) - timedelta(seconds=5),
            )
            advanced_edge = YouTubeLiveEdge(
                9656,
                datetime.now(timezone.utc),
            )
            edge_probe = AsyncMock(
                side_effect=[fresh_edge, fresh_edge, advanced_edge]
            )
            sleep = AsyncMock()
            manager = RecordingDownloadManager(
                config,
                state,
                probe,  # type: ignore[arg-type]
                sleep_func=sleep,
                probe_video_func=probe.probe_video_async,
                probe_youtube_live_edge_func=edge_probe,
                logger=NULL_LOGGER,
            )

            await manager.handle_post_exit(original, 1)
            state.close()

        self.assertEqual(len(manager.started), 1)
        self.assertEqual(manager.started[0][0], still_live)
        self.assertEqual(edge_probe.await_count, 3)
        self.assertEqual(
            [call.args[0] for call in sleep.await_args_list],
            [30, 30],
        )

    async def test_confirmed_frozen_youtube_edge_stays_stalled_for_monitoring(
        self,
    ) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                post_exit_check_seconds=[30, 60],
            )
            stream = LiveStream(
                video_id="youtube:LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                platform="youtube",
                is_live=True,
            )
            state = StateStore(config.db_path)
            state.upsert_detected(stream)
            state.mark_youtube_stale_live(
                stream.video_id,
                media_sequence=9655,
                edge_at="2026-08-01T08:29:04.025+00:00",
            )
            probe = SequenceProbe([])
            sleep = AsyncMock()
            manager = DownloadManager(
                config,
                state,
                probe,  # type: ignore[arg-type]
                sleep_func=sleep,
                probe_video_func=probe.probe_video_async,
                logger=NULL_LOGGER,
            )

            with patch.object(
                manager,
                "monitor_stalled_youtube",
                new=AsyncMock(),
            ) as monitor:
                await manager.handle_post_exit(stream, 1)
            record = state.get_stream(stream.video_id)
            state.close()

        monitor.assert_awaited_once_with(stream, 1)
        sleep.assert_not_awaited()
        self.assertEqual(probe.calls, 0)
        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(record.status, "stalled")

    async def test_stalled_explicit_youtube_end_preserves_unmerged_tracks(self) -> None:
        for end_error in (ConfirmedVideoRemovalError, ConfirmedLiveEndError):
            with self.subTest(end_error=end_error.__name__), TemporaryDirectory() as tmp:
                config = BotConfig(
                    download_dir=Path(tmp) / "downloads",
                    state_dir=Path(tmp) / "state",
                )
                stream = LiveStream(
                    video_id="youtube:r2ORTHCeg_A",
                    url=video_url("r2ORTHCeg_A"),
                    platform="youtube",
                    is_live=True,
                )
                state = StateStore(config.db_path)
                state.upsert_detected(stream)
                state.mark_youtube_stale_live(
                    stream.video_id,
                    media_sequence=9655,
                    edge_at="2026-10-04T22:17:00+00:00",
                )
                directory = segment_directory(config, stream.video_id)
                directory.mkdir(parents=True)
                saved_audio = directory / "segment-001.f140.m4a.part"
                saved_video = directory / "segment-001.f299.mp4.part"
                saved_audio.write_bytes(b"saved audio")
                saved_video.write_bytes(b"saved video")
                probe = SequenceProbe([
                    end_error("YouTube explicitly confirmed this stream ended"),
                    end_error("YouTube explicitly confirmed this stream ended"),
                ])
                manager = DownloadManager(
                    config,
                    state,
                    probe,  # type: ignore[arg-type]
                    sleep_func=AsyncMock(),
                    probe_video_func=probe.probe_video_async,
                    probe_youtube_live_edge_func=AsyncMock(
                        side_effect=RuntimeError("no playlist is available for an ended video")
                    ),
                    logger=NULL_LOGGER,
                )
                manager._schedule_finalization_retry = MagicMock()
                manager.finalize_ended_segment = AsyncMock(return_value=False)  # type: ignore[method-assign]
                manager._stop_draining_audio = AsyncMock()  # type: ignore[method-assign]
                audio_task = MagicMock()
                audio_task.done.return_value = False
                manager._draining_audio[stream.video_id] = SimpleNamespace(  # type: ignore[assignment]
                    audio_end_confirmed=False, audio_task=audio_task
                )
                try:
                    await manager.monitor_stalled_youtube(stream, 1)
                    status = state.get_stream(stream.video_id).status
                finally:
                    state.close()

                self.assertEqual(probe.calls, 2)
                self.assertEqual(status, "finalization_failed")
                manager._stop_draining_audio.assert_awaited_once_with(stream.video_id)
                self.assertEqual(saved_audio.read_bytes(), b"saved audio")
                self.assertEqual(saved_video.read_bytes(), b"saved video")
                manager._schedule_finalization_retry.assert_called_once_with(
                    stream, 1, expected_status="finalization_failed"
                )

    async def test_stalled_explicit_youtube_end_resets_after_error_or_live_reply(self) -> None:
        for end_error in (ConfirmedVideoRemovalError, ConfirmedLiveEndError):
            with self.subTest(end_error=end_error.__name__):
                stream = LiveStream(
                    video_id="youtube:r2ORTHCeg_A",
                    url=video_url("r2ORTHCeg_A"),
                    platform="youtube",
                    is_live=True,
                )
                for interruption in (RuntimeError("network timeout"), stream):
                    with self.subTest(interruption=type(interruption).__name__), TemporaryDirectory() as tmp:
                        config = BotConfig(
                            download_dir=Path(tmp) / "downloads",
                            state_dir=Path(tmp) / "state",
                        )
                        state = StateStore(config.db_path)
                        state.upsert_detected(stream)
                        state.mark_youtube_stale_live(
                            stream.video_id,
                            media_sequence=9655,
                            edge_at="2026-10-04T22:17:00+00:00",
                        )
                        probe = SequenceProbe([
                            end_error("YouTube explicitly confirmed this stream ended"),
                            interruption,
                            end_error("YouTube explicitly confirmed this stream ended"),
                        ])
                        async def sleep(_delay: float) -> None:
                            if probe.calls >= 3:
                                manager._stopping = True

                        manager = DownloadManager(
                            config,
                            state,
                            probe,  # type: ignore[arg-type]
                            sleep_func=sleep,
                            probe_video_func=probe.probe_video_async,
                            probe_youtube_live_edge_func=AsyncMock(
                                return_value=YouTubeLiveEdge(None, None)
                            ),
                            logger=NULL_LOGGER,
                        )
                        manager.finalize_ended_segment = AsyncMock()  # type: ignore[method-assign]
                        try:
                            await manager.monitor_stalled_youtube(stream, 1)
                            status = state.get_stream(stream.video_id).status
                        finally:
                            state.close()

                        self.assertEqual(status, "stalled")
                        manager.finalize_ended_segment.assert_not_awaited()

    async def test_explicit_youtube_end_finishes_after_service_restart_checks(self) -> None:
        for end_error in (ConfirmedVideoRemovalError, ConfirmedLiveEndError):
            with self.subTest(end_error=end_error.__name__), TemporaryDirectory() as tmp:
                config = BotConfig(
                    download_dir=Path(tmp) / "downloads",
                    state_dir=Path(tmp) / "state",
                    post_exit_check_seconds=[30, 60, 90],
                )
                stream = LiveStream(
                    video_id="youtube:r2ORTHCeg_A",
                    url=video_url("r2ORTHCeg_A"),
                    platform="youtube",
                    is_live=True,
                )
                state = StateStore(config.db_path)
                state.upsert_detected(stream)
                state.mark_exited(stream.video_id, 1)
                probe = SequenceProbe([
                    end_error("YouTube confirms this live stream ended"),
                    end_error("YouTube confirms this live stream ended"),
                ])
                sleep = AsyncMock()
                manager = RecordingDownloadManager(
                    config,
                    state,
                    probe,  # type: ignore[arg-type]
                    sleep_func=sleep,
                    probe_video_func=probe.probe_video_async,
                    logger=NULL_LOGGER,
                )
                actions: list[str] = []

                async def stop_audio(video_id: str) -> None:
                    self.assertEqual(video_id, stream.video_id)
                    actions.append("stop_audio")

                async def merge(video_id: str, index: int, channel: str) -> bool:
                    self.assertEqual(probe.calls, 2)
                    self.assertEqual(actions, ["stop_audio"])
                    actions.append("merge")
                    return True

                manager._stop_draining_audio = AsyncMock(side_effect=stop_audio)  # type: ignore[method-assign]
                manager.finalize_ended_segment = AsyncMock(side_effect=merge)  # type: ignore[method-assign]
                manager.rename_finalized_segments = MagicMock(return_value=[])  # type: ignore[method-assign]
                manager.finalize_powerchat_sidecars = MagicMock()  # type: ignore[method-assign]
                manager.enqueue_finalized_post_processing = MagicMock()  # type: ignore[method-assign]
                manager.process_pending_post_processing = AsyncMock()  # type: ignore[method-assign]
                audio_task = MagicMock()
                audio_task.done.return_value = False
                draining = SimpleNamespace(audio_end_confirmed=False, audio_task=audio_task)
                manager._draining_audio[stream.video_id] = draining  # type: ignore[assignment]
                try:
                    await manager.handle_post_exit(
                        stream, 1,
                        expected_status="checking_after_exit",
                        elapsed_since_exit_seconds=3600,
                    )
                    status = state.get_stream(stream.video_id).status
                finally:
                    state.close()

                self.assertEqual(status, "ended")
                self.assertEqual(probe.calls, 2)
                self.assertEqual(actions, ["stop_audio", "merge"])
                self.assertTrue(draining.audio_end_confirmed)
                self.assertEqual(manager.started, [])
                sleep.assert_not_awaited()
                self.assertNotIn(stream.video_id, manager._finalization_retry_tasks)

    async def test_finalization_retry_accepts_explicit_youtube_end_with_tracks_preserved(self) -> None:
        for end_error in (ConfirmedVideoRemovalError, ConfirmedLiveEndError):
            with self.subTest(end_error=end_error.__name__), TemporaryDirectory() as tmp:
                config = BotConfig(
                    download_dir=Path(tmp) / "downloads",
                    state_dir=Path(tmp) / "state",
                )
                stream = LiveStream(
                    video_id="youtube:r2ORTHCeg_A",
                    url=video_url("r2ORTHCeg_A"),
                    platform="youtube",
                    is_live=True,
                )
                state = StateStore(config.db_path)
                state.upsert_detected(stream)
                state.mark_exited(stream.video_id, 1)
                directory = segment_directory(config, stream.video_id)
                directory.mkdir(parents=True)
                saved_video = directory / "segment-001.f299.mp4.part"
                saved_video.write_bytes(b"recoverable saved video")
                probe = SequenceProbe([end_error("YouTube explicitly confirmed this stream ended")])
                async def sleep(_delay: float) -> None:
                    if probe.calls:
                        raise asyncio.CancelledError

                manager = DownloadManager(
                    config,
                    state,
                    probe,  # type: ignore[arg-type]
                    sleep_func=sleep,
                    probe_video_func=probe.probe_video_async,
                    logger=NULL_LOGGER,
                )
                manager.finalize_ended_segment = AsyncMock(return_value=False)  # type: ignore[method-assign]
                try:
                    await manager.finish_ended_stream(
                        stream, 1, expected_status="checking_after_exit", end_confirmed=True
                    )
                    with self.assertRaises(asyncio.CancelledError):
                        await asyncio.wait_for(manager._finalization_retry_tasks[stream.video_id], 1)
                    status = state.get_stream(stream.video_id).status
                finally:
                    state.close()

                self.assertEqual(status, "finalization_failed")
                self.assertEqual(probe.calls, 1)
                self.assertEqual(saved_video.read_bytes(), b"recoverable saved video")
                self.assertEqual(manager.finalize_ended_segment.await_count, 2)

    async def test_explicit_youtube_end_during_planned_reconnect_is_reconfirmed(self) -> None:
        for end_error in (ConfirmedVideoRemovalError, ConfirmedLiveEndError):
            with self.subTest(end_error=end_error.__name__), TemporaryDirectory() as tmp:
                config = BotConfig(
                    download_dir=Path(tmp) / "downloads",
                    state_dir=Path(tmp) / "state",
                    post_exit_check_seconds=[0, 1],
                )
                stream = LiveStream(
                    video_id="youtube:r2ORTHCeg_A",
                    url=video_url("r2ORTHCeg_A"),
                    platform="youtube",
                    is_live=True,
                )
                state = StateStore(config.db_path)
                state.upsert_detected(stream)
                state.mark_exited(stream.video_id, 1)
                probe = SequenceProbe([
                    end_error("YouTube explicitly confirmed this stream ended") for _ in range(3)
                ])
                manager = DownloadManager(
                    config,
                    state,
                    probe,  # type: ignore[arg-type]
                    sleep_func=AsyncMock(),
                    probe_video_func=probe.probe_video_async,
                    probe_youtube_live_edge_func=AsyncMock(),
                    logger=NULL_LOGGER,
                )
                try:
                    await manager.handle_planned_reconnect(stream, 1)
                    status = state.get_stream(stream.video_id).status
                finally:
                    state.close()

                self.assertEqual(status, "ended")
                self.assertEqual(probe.calls, 3)
                manager.probe_youtube_live_edge.assert_not_awaited()

    async def test_stalled_youtube_explicit_termination_finishes_without_endlist(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            stream = LiveStream(
                video_id="youtube:8YbgANWF8pk",
                url=video_url("8YbgANWF8pk"),
                platform="youtube",
                is_live=True,
            )
            state = StateStore(config.db_path)
            state.upsert_detected(stream)
            state.mark_youtube_stale_live(
                stream.video_id,
                media_sequence=9655,
                edge_at="2026-09-28T07:20:00+00:00",
            )
            probe = SequenceProbe([
                ConfirmedLiveTerminationError("terminated due to third-party content"),
                ConfirmedLiveTerminationError("terminated due to third-party content"),
            ])
            edge_probe = AsyncMock(return_value=YouTubeLiveEdge(None, None))
            sleep = AsyncMock()
            manager = DownloadManager(
                config,
                state,
                probe,  # type: ignore[arg-type]
                sleep_func=sleep,
                probe_video_func=probe.probe_video_async,
                probe_youtube_live_edge_func=edge_probe,
                logger=NULL_LOGGER,
            )
            manager.finalize_ended_segment = AsyncMock(return_value=True)  # type: ignore[method-assign]
            manager.rename_finalized_segments = MagicMock(return_value=[])  # type: ignore[method-assign]
            manager.finalize_powerchat_sidecars = MagicMock()  # type: ignore[method-assign]
            manager.enqueue_finalized_post_processing = MagicMock()  # type: ignore[method-assign]
            manager.process_pending_post_processing = AsyncMock()  # type: ignore[method-assign]
            try:
                await manager.monitor_stalled_youtube(stream, 1)
                status = state.get_stream(stream.video_id).status
            finally:
                state.close()

            self.assertEqual(status, "ended")
            self.assertEqual(probe.calls, 2)
            edge_probe.assert_awaited_once()
            sleep.assert_awaited_once_with(config.poll_interval_seconds)

    async def test_stalled_youtube_termination_then_not_live_stops_audio(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            stream = LiveStream(
                video_id="youtube:8YbgANWF8pk",
                url=video_url("8YbgANWF8pk"),
                platform="youtube",
                is_live=True,
            )
            state = StateStore(config.db_path)
            state.upsert_detected(stream)
            state.mark_youtube_stale_live(
                stream.video_id,
                media_sequence=9655,
                edge_at="2026-09-28T07:20:00+00:00",
            )
            probe = SequenceProbe([
                ConfirmedLiveTerminationError("terminated due to third-party content"),
                LiveStream(
                    video_id=stream.video_id,
                    url=stream.url,
                    platform="youtube",
                    is_live=False,
                ),
            ])
            manager = DownloadManager(
                config,
                state,
                probe,  # type: ignore[arg-type]
                sleep_func=AsyncMock(),
                probe_video_func=probe.probe_video_async,
                probe_youtube_live_edge_func=AsyncMock(
                    return_value=YouTubeLiveEdge(None, None)
                ),
                logger=NULL_LOGGER,
            )
            manager.finish_ended_stream = AsyncMock()  # type: ignore[method-assign]
            try:
                await manager.monitor_stalled_youtube(stream, 1)
            finally:
                state.close()

            manager.finish_ended_stream.assert_awaited_once()
            self.assertTrue(
                manager.finish_ended_stream.await_args.kwargs["stop_draining_audio"]
            )
            self.assertFalse(
                manager.finish_ended_stream.await_args.kwargs["allow_chat_replay"]
            )

    async def test_frozen_edge_takes_priority_over_simultaneous_planned_reconnect(
        self,
    ) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            stream = LiveStream(
                video_id="youtube:LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                platform="youtube",
                is_live=True,
            )
            state = StateStore(config.db_path)
            state.upsert_detected(stream)
            state.mark_youtube_stale_live(
                stream.video_id,
                media_sequence=9655,
                edge_at="2026-08-01T08:29:04.025+00:00",
            )
            manager = DownloadManager(
                config,
                state,
                probe=SequenceProbe([]),  # type: ignore[arg-type]
                logger=NULL_LOGGER,
            )
            manager._planned_reconnects.add(stream.video_id)
            process = FakeProcess()
            process.returncode = 0

            with (
                patch.object(
                    manager,
                    "handle_post_exit",
                    new=AsyncMock(),
                ) as post_exit,
                patch.object(
                    manager,
                    "handle_planned_reconnect",
                    new=AsyncMock(),
                ) as planned,
            ):
                await manager._watch_process(stream, process, 1)  # type: ignore[arg-type]
                await asyncio.sleep(0)
            state.close()

        post_exit.assert_awaited_once_with(
            stream,
            1,
            expected_status="checking_after_exit",
        )
        planned.assert_not_awaited()
        self.assertNotIn(stream.video_id, manager._planned_reconnects)

    async def test_planned_reconnect_terminates_to_leave_part_files_for_resume(self) -> None:
        sleeps: list[float] = []

        async def fake_sleep(delay: float) -> None:
            sleeps.append(delay)

        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                reconnect_interval_seconds=30,
            )
            state = StateStore(config.db_path)
            probe = SequenceProbe([])
            manager = DownloadManager(
                config,
                state,
                probe,  # type: ignore[arg-type]
                sleep_func=fake_sleep,
                logger=NULL_LOGGER,
            )
            process = FakeProcess()
            reconnect_ready = asyncio.Event()
            reconnect_ready.set()

            await manager._planned_reconnect_timer(  # type: ignore[arg-type]
                "LIVEVIDEO01",
                process,
                reconnect_ready,
            )
            state.close()

        self.assertEqual(sleeps, [30])
        self.assertTrue(process.terminated)
        self.assertFalse(process.killed)
        self.assertIn("LIVEVIDEO01", manager._planned_reconnects)

    async def test_planned_reconnect_waits_until_stream_has_caught_up(self) -> None:
        sleeps: list[float] = []

        async def fake_sleep(delay: float) -> None:
            sleeps.append(delay)

        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                reconnect_interval_seconds=30,
            )
            state = StateStore(config.db_path)
            probe = SequenceProbe([])
            manager = DownloadManager(
                config,
                state,
                probe,  # type: ignore[arg-type]
                sleep_func=fake_sleep,
                logger=NULL_LOGGER,
            )
            process = FakeProcess()
            reconnect_ready = asyncio.Event()

            task = asyncio.create_task(
                manager._planned_reconnect_timer(  # type: ignore[arg-type]
                    "LIVEVIDEO01",
                    process,
                    reconnect_ready,
                )
            )
            await asyncio.sleep(0)
            self.assertEqual(sleeps, [])
            self.assertFalse(process.terminated)

            reconnect_ready.set()
            await task
            state.close()

        self.assertEqual(sleeps, [30])
        self.assertTrue(process.terminated)

    async def test_split_track_watchdog_confirms_before_reconnecting(self) -> None:
        sleeps: list[float] = []

        async def fake_sleep(delay: float) -> None:
            sleeps.append(delay)

        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                channel="Example Channel",
            )
            segment_dir = config.download_dir / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            (segment_dir / "segment-001.f140.mp4").write_text("audio", encoding="utf-8")
            (segment_dir / "segment-001.f137.mp4.part").write_text("video", encoding="utf-8")
            state = StateStore(config.db_path)
            probe = SequenceProbe([])
            manager = DownloadManager(
                config,
                state,
                probe,  # type: ignore[arg-type]
                sleep_func=fake_sleep,
                logger=NULL_LOGGER,
            )
            process = FakeProcess()

            await manager._mixed_segment_watchdog(  # type: ignore[arg-type]
                stream,
                process,
                1,
            )
            state.close()

        self.assertEqual(sleeps, [10, 120])
        self.assertTrue(process.terminated)
        self.assertIn("LIVEVIDEO01", manager._planned_reconnects)

    async def test_planned_reconnect_restarts_immediately_if_still_live(self) -> None:
        sleeps: list[float] = []

        async def fake_sleep(delay: float) -> None:
            sleeps.append(delay)

        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                post_exit_check_seconds=[30, 60],
            )
            original = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                is_live=True,
            )
            live_again = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                is_live=True,
            )
            state = StateStore(config.db_path)
            state.upsert_detected(original)
            state.mark_exited(original.video_id, 0)
            probe = SequenceProbe([live_again])
            manager = RecordingDownloadManager(
                config,
                state,
                probe,  # type: ignore[arg-type]
                sleep_func=fake_sleep,
                probe_video_func=probe.probe_video_async,
                logger=NULL_LOGGER,
            )

            await manager.handle_planned_reconnect(original, 1)
            record = state.get_stream(original.video_id)
            state.close()

        self.assertEqual(probe.calls, 1)
        self.assertEqual(sleeps, [])
        self.assertEqual(len(manager.started), 1)
        self.assertEqual(manager.started[0][0].video_id, "LIVEVIDEO01")
        self.assertEqual(manager.started[0][1], 1)
        self.assertIsNotNone(record)
        self.assertNotEqual(record.status, "ended")

    async def test_planned_reconnect_restores_mixed_segment_for_resume(self) -> None:
        async def fake_sleep(delay: float) -> None:
            return None

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = BotConfig(
                download_dir=root / "downloads",
                state_dir=root / "state",
                post_exit_check_seconds=[30, 60],
            )
            original = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                channel="Example Channel",
                is_live=True,
            )
            live_again = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                channel="Example Channel",
                is_live=True,
            )
            segment_dir = config.download_dir / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            (segment_dir / "segment-001.f140.mp4").write_text("audio", encoding="utf-8")
            (segment_dir / "segment-001.f140.mp4.part-Frag1").write_text(
                "a1",
                encoding="utf-8",
            )
            (segment_dir / "segment-001.f137.mp4.part").write_text("video", encoding="utf-8")
            (segment_dir / "segment-001.f137.mp4.ytdl").write_text("{}", encoding="utf-8")
            state = StateStore(config.db_path)
            state.upsert_detected(original)
            state.mark_exited(original.video_id, 0)
            probe = SequenceProbe([live_again])
            manager = RecordingDownloadManager(
                config,
                state,
                probe,  # type: ignore[arg-type]
                sleep_func=fake_sleep,
                probe_video_func=probe.probe_video_async,
                logger=NULL_LOGGER,
            )

            await manager.handle_planned_reconnect(original, 1)
            record = state.get_stream(original.video_id)
            state.close()

            self.assertFalse((segment_dir / "segment-001.f140.mp4").exists())
            self.assertTrue((segment_dir / "segment-001.f140.mp4.part").exists())
            self.assertTrue((segment_dir / "segment-001.f140.mp4.ytdl").exists())
            self.assertTrue((segment_dir / "segment-001.f137.mp4.part").exists())
            self.assertTrue((segment_dir / "segment-001.f137.mp4.ytdl").exists())

        self.assertEqual(len(manager.started), 1)
        self.assertEqual(manager.started[0][1], 1)
        self.assertIsNotNone(record)
        self.assertEqual(record.segment_index, 1)

    async def test_post_exit_live_check_restores_mixed_segment_for_resume(self) -> None:
        async def fake_sleep(delay: float) -> None:
            return None

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = BotConfig(
                download_dir=root / "downloads",
                state_dir=root / "state",
                post_exit_check_seconds=[0],
            )
            original = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                channel="Example Channel",
                is_live=True,
            )
            live_again = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                channel="Example Channel",
                is_live=True,
            )
            segment_dir = config.download_dir / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            (segment_dir / "segment-001.f140.mp4").write_text("audio", encoding="utf-8")
            (segment_dir / "segment-001.f140.mp4.part-Frag1").write_text(
                "a1",
                encoding="utf-8",
            )
            (segment_dir / "segment-001.f137.mp4.part").write_text("video", encoding="utf-8")
            state = StateStore(config.db_path)
            state.upsert_detected(original)
            state.mark_exited(original.video_id, 0)
            probe = SequenceProbe([live_again])
            manager = RecordingDownloadManager(
                config,
                state,
                probe,  # type: ignore[arg-type]
                sleep_func=fake_sleep,
                probe_video_func=probe.probe_video_async,
                logger=NULL_LOGGER,
            )

            await manager.handle_post_exit(original, 1)
            record = state.get_stream(original.video_id)
            state.close()

            self.assertFalse((segment_dir / "segment-001.f140.mp4").exists())
            self.assertTrue((segment_dir / "segment-001.f140.mp4.part").exists())
            self.assertTrue((segment_dir / "segment-001.f140.mp4.ytdl").exists())
            self.assertTrue((segment_dir / "segment-001.f137.mp4.part").exists())

        self.assertEqual(len(manager.started), 1)
        self.assertEqual(manager.started[0][1], 1)
        self.assertIsNotNone(record)
        self.assertEqual(record.segment_index, 1)

    async def test_marks_ended_only_after_full_post_exit_schedule(self) -> None:
        sleeps: list[float] = []

        async def fake_sleep(delay: float) -> None:
            sleeps.append(delay)

        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                post_exit_check_seconds=list(DEFAULT_POST_EXIT_CHECK_SECONDS),
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                is_live=True,
            )
            non_live = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                is_live=False,
                live_status="was_live",
            )
            state = StateStore(config.db_path)
            state.upsert_detected(stream)
            state.mark_exited(stream.video_id, 0)
            probe = SequenceProbe([non_live] * len(DEFAULT_POST_EXIT_CHECK_SECONDS))
            manager = RecordingDownloadManager(
                config,
                state,
                probe,  # type: ignore[arg-type]
                sleep_func=fake_sleep,
                probe_video_func=probe.probe_video_async,
                logger=NULL_LOGGER,
            )

            await manager.handle_post_exit(stream, 1)
            record = state.get_stream(stream.video_id)
            state.close()

        self.assertEqual(probe.calls, len(DEFAULT_POST_EXIT_CHECK_SECONDS))
        self.assertEqual(sleeps, [30] * len(DEFAULT_POST_EXIT_CHECK_SECONDS))
        self.assertIsNotNone(record)
        self.assertEqual(record.status, "ended")
        self.assertEqual(manager.started, [])

    async def test_recovered_post_exit_skips_elapsed_delays(self) -> None:
        sleeps: list[float] = []

        async def fake_sleep(delay: float) -> None:
            sleeps.append(delay)

        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                post_exit_check_seconds=[30, 60, 90],
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                is_live=True,
            )
            non_live = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                is_live=False,
            )
            state = StateStore(config.db_path)
            state.upsert_detected(stream)
            state.mark_exited(stream.video_id, 0)
            probe = SequenceProbe([non_live, non_live, non_live])
            manager = RecordingDownloadManager(
                config,
                state,
                probe,  # type: ignore[arg-type]
                sleep_func=fake_sleep,
                probe_video_func=probe.probe_video_async,
                logger=NULL_LOGGER,
            )

            await manager.handle_post_exit(
                stream,
                1,
                elapsed_since_exit_seconds=45,
                expected_status="checking_after_exit",
            )
            record = state.get_stream(stream.video_id)
            state.close()

        self.assertEqual(probe.calls, 3)
        self.assertEqual(sleeps, [15, 30])
        self.assertIsNotNone(record)
        self.assertEqual(record.status, "ended")

    async def test_restarts_if_any_post_exit_check_says_live(self) -> None:
        async def fake_sleep(delay: float) -> None:
            await asyncio.sleep(0)

        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                post_exit_check_seconds=[30, 60],
            )
            original = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                is_live=True,
            )
            non_live = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                is_live=False,
            )
            live_again = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                is_live=True,
            )
            state = StateStore(config.db_path)
            state.upsert_detected(original)
            state.mark_exited(original.video_id, 0)
            probe = SequenceProbe([non_live, live_again])
            manager = RecordingDownloadManager(
                config,
                state,
                probe,  # type: ignore[arg-type]
                sleep_func=fake_sleep,
                probe_video_func=probe.probe_video_async,
                logger=NULL_LOGGER,
            )

            await manager.handle_post_exit(original, 1)
            record = state.get_stream(original.video_id)
            state.close()

        self.assertEqual(probe.calls, 2)
        self.assertEqual(len(manager.started), 1)
        self.assertEqual(manager.started[0][0].video_id, "LIVEVIDEO01")
        self.assertIsNotNone(record)
        self.assertNotEqual(record.status, "ended")

    async def test_kick_post_exit_probes_configured_source_for_restarts(self) -> None:
        async def fake_sleep(delay: float) -> None:
            await asyncio.sleep(0)

        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                post_exit_check_seconds=[0],
            )
            original = LiveStream(
                video_id="kick:oumb",
                url="https://kick.com/oumb/videos/temporary-extracted-url",
                title="Kick Stream",
                channel="oumb",
                platform="kick",
                source="kick:oumb",
                is_live=True,
            )
            live_again = LiveStream(
                video_id="kick:oumb",
                url="https://kick.com/oumb",
                title="Kick Stream",
                channel="oumb",
                platform="kick",
                source="kick:oumb",
                is_live=True,
            )
            state = StateStore(config.db_path)
            state.upsert_detected(original)
            state.mark_exited(original.video_id, 0)
            probe = SequenceProbe([live_again])
            manager = RecordingDownloadManager(
                config,
                state,
                probe,  # type: ignore[arg-type]
                sleep_func=fake_sleep,
                probe_video_func=probe.probe_video_async,
                logger=NULL_LOGGER,
            )

            await manager.handle_post_exit(original, 1)
            record = state.get_stream(original.video_id)
            state.close()

        self.assertEqual(probe.urls, ["kick:oumb"])
        self.assertEqual(len(manager.started), 1)
        self.assertEqual(manager.started[0][0].video_id, "kick:oumb")
        self.assertIsNotNone(record)
        self.assertNotEqual(record.status, "ended")

    async def test_two_explicit_kick_offline_probes_end_saved_stream(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                post_exit_check_seconds=[0, 1, 2],
            )
            stream = LiveStream(
                video_id="kick:eab3048bd7da3c2b-motd-chilling-3tts-no-toxicity",
                url="https://kick.com/oumb",
                title="MOTD Chilling",
                channel="oumb",
                platform="kick",
                source="kick:oumb",
                is_live=True,
            )
            directory = segment_directory(config, stream.video_id, stream.channel)
            directory.mkdir(parents=True)
            saved_media = directory / "segment-001.mp4"
            saved_media.write_bytes(b"already finalized recording")
            state = StateStore(config.db_path)
            state.upsert_detected(stream)
            state.mark_exited(stream.video_id, 0)
            probe = SequenceProbe([
                KickChannelOfflineError("The channel is not currently live"),
                KickChannelOfflineError("The channel is not currently live"),
            ])
            manager = DownloadManager(
                config,
                state,
                probe,  # type: ignore[arg-type]
                sleep_func=AsyncMock(),
                probe_video_func=probe.probe_video_async,
                logger=NULL_LOGGER,
            )
            manager.enqueue_finalized_post_processing = MagicMock()  # type: ignore[method-assign]
            manager.process_pending_post_processing = AsyncMock()  # type: ignore[method-assign]
            try:
                await manager.handle_post_exit(
                    stream, 1, expected_status="checking_after_exit"
                )
                status = state.get_stream(stream.video_id).status
            finally:
                state.close()

            self.assertEqual(probe.urls, ["kick:oumb", "kick:oumb"])
            self.assertEqual(status, "ended")
            self.assertEqual(
                [path.read_bytes() for path in directory.glob("*.mp4")],
                [b"already finalized recording"],
            )

    async def test_kick_offline_confirmation_is_broken_by_probe_failure(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                post_exit_check_seconds=[0, 1],
            )
            stream = LiveStream(
                video_id="kick:old-session",
                url="https://kick.com/oumb",
                platform="kick",
                source="kick:oumb",
                is_live=True,
            )
            state = StateStore(config.db_path)
            state.upsert_detected(stream)
            state.mark_exited(stream.video_id, 0)
            probe = SequenceProbe([
                KickChannelOfflineError("The channel is not currently live"),
                RuntimeError("network timeout"),
            ])
            manager = DownloadManager(
                config,
                state,
                probe,  # type: ignore[arg-type]
                sleep_func=AsyncMock(),
                probe_video_func=probe.probe_video_async,
                logger=NULL_LOGGER,
            )
            manager.finalize_ended_segment = AsyncMock()  # type: ignore[method-assign]
            manager._defer_post_exit_retry = MagicMock()  # type: ignore[method-assign]
            try:
                await manager.handle_post_exit(
                    stream, 1, expected_status="checking_after_exit"
                )
                status = state.get_stream(stream.video_id).status
            finally:
                state.close()

            self.assertEqual(status, "checking_after_exit")
            manager.finalize_ended_segment.assert_not_awaited()
            manager._defer_post_exit_retry.assert_called_once_with(stream, 1)

    async def test_explicit_youtube_end_keeps_unmerged_tracks_available(self) -> None:
        for end_error in (ConfirmedLiveTerminationError, ConfirmedVideoRemovalError, ConfirmedLiveEndError):
            with self.subTest(end_error=end_error.__name__), TemporaryDirectory() as tmp:
                config = BotConfig(
                    download_dir=Path(tmp) / "downloads",
                    state_dir=Path(tmp) / "state",
                    post_exit_check_seconds=[0, 1, 2],
                )
                stream = LiveStream(
                    video_id="youtube:8YbgANWF8pk",
                    url=video_url("8YbgANWF8pk"),
                    channel="Creator",
                    platform="youtube",
                    is_live=True,
                )
                directory = segment_directory(config, stream.video_id, stream.channel)
                directory.mkdir(parents=True)
                audio = directory / "segment-001.f140.m4a.part"
                video = directory / "segment-001.f299.mp4.part"
                audio.write_bytes(b"saved audio")
                video.write_bytes(b"saved video")
                state = StateStore(config.db_path)
                state.upsert_detected(stream)
                state.mark_exited(stream.video_id, 1)
                probe = SequenceProbe([
                    end_error("YouTube explicitly confirmed this stream ended"),
                    end_error("YouTube explicitly confirmed this stream ended"),
                ])
                manager = DownloadManager(
                    config,
                    state,
                    probe,  # type: ignore[arg-type]
                    sleep_func=AsyncMock(),
                    probe_video_func=probe.probe_video_async,
                    logger=NULL_LOGGER,
                )
                manager._schedule_finalization_retry = MagicMock()
                manager.finalize_ended_segment = AsyncMock(return_value=False)  # type: ignore[method-assign]
                manager._stop_draining_audio = AsyncMock()  # type: ignore[method-assign]
                fake_audio_task = MagicMock()
                fake_audio_task.done.return_value = False
                draining = SimpleNamespace(audio_end_confirmed=False, audio_task=fake_audio_task)
                manager._draining_audio[stream.video_id] = draining  # type: ignore[assignment]
                try:
                    await manager.handle_post_exit(
                        stream, 1, expected_status="checking_after_exit"
                    )
                    status = state.get_stream(stream.video_id).status
                    events = state.list_stream_events([stream.video_id])[stream.video_id]
                finally:
                    state.close()

                self.assertEqual(probe.calls, 2)
                self.assertEqual(status, "finalization_failed")
                self.assertTrue(draining.audio_end_confirmed)
                manager._stop_draining_audio.assert_awaited_once_with(stream.video_id)
                self.assertEqual(audio.read_bytes(), b"saved audio")
                self.assertEqual(video.read_bytes(), b"saved video")
                self.assertTrue(any("preserving media tracks for recovery" in event.message for event in events))
                manager._schedule_finalization_retry.assert_called_once_with(
                    stream, 1, expected_status="finalization_failed"
                )

    async def test_single_explicit_youtube_end_is_not_confirmed(self) -> None:
        for end_error in (ConfirmedLiveTerminationError, ConfirmedVideoRemovalError, ConfirmedLiveEndError):
            with self.subTest(end_error=end_error.__name__), TemporaryDirectory() as tmp:
                config = BotConfig(
                    download_dir=Path(tmp) / "downloads",
                    state_dir=Path(tmp) / "state",
                    post_exit_check_seconds=[0, 1],
                )
                stream = LiveStream(
                    video_id="youtube:8YbgANWF8pk",
                    url=video_url("8YbgANWF8pk"),
                    platform="youtube",
                    is_live=True,
                )
                state = StateStore(config.db_path)
                state.upsert_detected(stream)
                state.mark_exited(stream.video_id, 1)
                probe = SequenceProbe([
                    end_error("YouTube explicitly confirmed this stream ended"),
                    RuntimeError("network timeout"),
                ])
                manager = DownloadManager(
                    config,
                    state,
                    probe,  # type: ignore[arg-type]
                    sleep_func=AsyncMock(),
                    probe_video_func=probe.probe_video_async,
                    logger=NULL_LOGGER,
                )
                manager.finalize_ended_segment = AsyncMock()  # type: ignore[method-assign]
                manager._defer_post_exit_retry = MagicMock()  # type: ignore[method-assign]
                try:
                    await manager.handle_post_exit(
                        stream, 1, expected_status="checking_after_exit"
                    )
                    status = state.get_stream(stream.video_id).status
                finally:
                    state.close()

                self.assertEqual(status, "checking_after_exit")
                manager.finalize_ended_segment.assert_not_awaited()
                manager._defer_post_exit_retry.assert_called_once_with(stream, 1)

    async def test_probe_failures_require_two_subsequent_end_reports(self) -> None:
        sleeps = 0

        async def fake_sleep(delay: float) -> None:
            nonlocal sleeps
            sleeps += 1

        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                post_exit_check_seconds=[30, 60, 90, 120],
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                is_live=True,
            )
            non_live = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                is_live=False,
            )
            state = StateStore(config.db_path)
            state.upsert_detected(stream)
            state.mark_exited(stream.video_id, 0)
            probe = SequenceProbe(
                [RuntimeError("network"), RuntimeError("extractor"), non_live, non_live]
            )
            manager = RecordingDownloadManager(
                config,
                state,
                probe,  # type: ignore[arg-type]
                sleep_func=fake_sleep,
                probe_video_func=probe.probe_video_async,
                logger=NULL_LOGGER,
            )

            await manager.handle_post_exit(stream, 1)
            record = state.get_stream(stream.video_id)
            state.close()

        self.assertEqual(probe.calls, 4)
        self.assertEqual(sleeps, 4)
        self.assertIsNotNone(record)
        self.assertEqual(record.status, "ended")

    async def test_failed_probes_keep_tracks_for_later_confirmation(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                post_exit_check_seconds=[0, 1],
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                channel="Creator",
                is_live=True,
            )
            segment_dir = config.download_dir / "Creator" / stream.video_id
            segment_dir.mkdir(parents=True)
            audio = segment_dir / "segment-001.f140.mp4.part"
            video = segment_dir / "segment-001.f299.mp4.part"
            audio.write_bytes(b"audio")
            video.write_bytes(b"video")
            state = StateStore(config.db_path)
            state.upsert_detected(stream)
            state.mark_exited(stream.video_id, 1)
            probe = SequenceProbe([RuntimeError("network"), RuntimeError("network")])
            manager = DownloadManager(
                config,
                state,
                probe,  # type: ignore[arg-type]
                sleep_func=AsyncMock(),
                probe_video_func=probe.probe_video_async,
                logger=NULL_LOGGER,
            )
            manager.finalize_ended_segment = AsyncMock()  # type: ignore[method-assign]
            manager._defer_post_exit_retry = MagicMock()  # type: ignore[method-assign]
            try:
                await manager.handle_post_exit(
                    stream, 1, expected_status="checking_after_exit"
                )
                status = state.get_stream(stream.video_id).status
                tracks_preserved = audio.exists() and video.exists()
            finally:
                state.close()

        self.assertEqual(probe.calls, 2)
        self.assertEqual(status, "checking_after_exit")
        self.assertTrue(tracks_preserved)
        manager.finalize_ended_segment.assert_not_awaited()
        manager._defer_post_exit_retry.assert_called_once_with(stream, 1)

    async def test_inconclusive_checks_retry_until_end_is_confirmed(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                post_exit_check_seconds=[0, 1],
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                is_live=True,
            )
            ended = LiveStream(
                video_id=stream.video_id,
                url=stream.url,
                is_live=False,
            )
            state = StateStore(config.db_path)
            state.upsert_detected(stream)
            state.mark_exited(stream.video_id, 1)
            probe = SequenceProbe(
                [RuntimeError("network"), RuntimeError("network"), ended, ended]
            )

            async def quick_sleep(_delay: float) -> None:
                await asyncio.sleep(0)

            manager = DownloadManager(
                config,
                state,
                probe,  # type: ignore[arg-type]
                sleep_func=quick_sleep,
                probe_video_func=probe.probe_video_async,
                logger=NULL_LOGGER,
            )
            try:
                await manager.handle_post_exit(
                    stream, 1, expected_status="checking_after_exit"
                )
                pending = list(manager._post_exit_tasks)
                self.assertEqual(state.get_stream(stream.video_id).status, "checking_after_exit")
                await asyncio.gather(*pending)
                status = state.get_stream(stream.video_id).status
            finally:
                state.close()

        self.assertEqual(probe.calls, 4)
        self.assertEqual(status, "ended")

    async def test_single_non_live_probe_does_not_confirm_end(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                post_exit_check_seconds=[0],
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                is_live=True,
            )
            ended = LiveStream(
                video_id=stream.video_id,
                url=stream.url,
                is_live=False,
            )
            state = StateStore(config.db_path)
            state.upsert_detected(stream)
            state.mark_exited(stream.video_id, 0)
            manager = DownloadManager(
                config,
                state,
                probe=None,  # type: ignore[arg-type]
                probe_video_func=AsyncMock(return_value=ended),
                logger=NULL_LOGGER,
            )
            manager.finalize_ended_segment = AsyncMock()  # type: ignore[method-assign]
            manager._defer_post_exit_retry = MagicMock()  # type: ignore[method-assign]
            try:
                await manager.handle_post_exit(
                    stream, 1, expected_status="checking_after_exit"
                )
                status = state.get_stream(stream.video_id).status
            finally:
                state.close()

        self.assertEqual(status, "checking_after_exit")
        manager.finalize_ended_segment.assert_not_awaited()
        manager._defer_post_exit_retry.assert_called_once_with(stream, 1)

    async def test_live_rollover_preserves_mixed_tracks_until_confirmed_end(
        self,
    ) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                channel="Creator",
                is_live=True,
            )
            segment_dir = config.download_dir / "Creator" / stream.video_id
            segment_dir.mkdir(parents=True)
            audio = segment_dir / "segment-001.f140.mp4"
            video = segment_dir / "segment-001.f299.mp4.part"
            audio.write_bytes(b"audio")
            video.write_bytes(b"video")
            state = StateStore(config.db_path)
            manager = DownloadManager(
                config, state, probe=None, logger=NULL_LOGGER  # type: ignore[arg-type]
            )
            manager.finalize_ended_segment = AsyncMock()  # type: ignore[method-assign]
            try:
                next_segment = await manager.choose_live_restart_segment(stream, 1)
                audio_data = audio.read_bytes()
                video_data = video.read_bytes()
            finally:
                state.close()

        self.assertEqual(next_segment, 2)
        self.assertEqual(audio_data, b"audio")
        self.assertEqual(video_data, b"video")
        manager.finalize_ended_segment.assert_not_awaited()

    async def test_old_session_explicit_youtube_end_requires_two_confirmations(self) -> None:
        stream = LiveStream(
            video_id="youtube:r2ORTHCeg_A",
            url=video_url("r2ORTHCeg_A"),
            platform="youtube",
            source="https://www.youtube.com/@Creator/live",
            is_live=True,
        )
        for end_error in (
            ConfirmedLiveEndError,
            ConfirmedVideoRemovalError,
            ConfirmedLiveTerminationError,
        ):
            for second_result, expected in (
                (end_error("Source explicitly confirmed ended"), True),
                (RuntimeError("network timeout"), False),
                (TerminalVideoUnavailableError("generic unavailable video"), False),
                (stream, False),
                (LiveStream(video_id="youtube:NEWVIDEO001", url=video_url("NEWVIDEO001"), is_live=False), False),
            ):
                with self.subTest(
                    end_error=end_error.__name__,
                    second_result=type(second_result).__name__,
                    expected=expected,
                ), TemporaryDirectory() as tmp:
                    config = BotConfig(
                        download_dir=Path(tmp) / "downloads",
                        state_dir=Path(tmp) / "state",
                    )
                    state = StateStore(config.db_path)
                    probe = SequenceProbe([
                        end_error("Source explicitly confirmed ended"), second_result,
                    ])
                    sleep = AsyncMock()
                    manager = DownloadManager(
                        config,
                        state,
                        probe,  # type: ignore[arg-type]
                        sleep_func=sleep,
                        probe_video_func=probe.probe_video_async,
                        probe_youtube_live_edge_func=AsyncMock(
                            return_value=YouTubeLiveEdge(None, None)
                        ),
                        logger=NULL_LOGGER,
                    )
                    try:
                        confirmed = await manager._old_session_end_confirmed(stream)
                    finally:
                        state.close()

                    self.assertEqual(confirmed, expected)
                    self.assertEqual(probe.urls, [stream.url, stream.url])
                    sleep.assert_awaited_once_with(
                        max(1, min(config.poll_interval_seconds, 30))
                    )

    async def test_confirmed_end_finalizes_every_retained_segment(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                is_live=True,
            )
            state = StateStore(config.db_path)
            state.upsert_detected(stream)
            state.mark_exited(stream.video_id, 0)
            manager = DownloadManager(
                config, state, probe=None, logger=NULL_LOGGER  # type: ignore[arg-type]
            )
            manager.finalize_ended_segment = AsyncMock(return_value=True)  # type: ignore[method-assign]
            manager.rename_finalized_segments = MagicMock(return_value=[])  # type: ignore[method-assign]
            manager.finalize_powerchat_sidecars = MagicMock()  # type: ignore[method-assign]
            manager.enqueue_finalized_post_processing = MagicMock()  # type: ignore[method-assign]
            manager.process_pending_post_processing = AsyncMock()  # type: ignore[method-assign]
            try:
                await manager.finish_ended_stream(stream, 2)
                before = state.get_stream(stream.video_id).status
                await manager.finish_ended_stream(stream, 2, end_confirmed=True)
                after = state.get_stream(stream.video_id).status
            finally:
                state.close()

        self.assertEqual(before, "checking_after_exit")
        self.assertEqual(after, "ended")
        self.assertEqual(
            [args.args[1] for args in manager.finalize_ended_segment.await_args_list],
            [1, 2],
        )

    async def test_terminal_unavailable_preserves_tracks_after_post_exit_probe(self) -> None:
        sleeps: list[float] = []

        async def fake_sleep(delay: float) -> None:
            sleeps.append(delay)

        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                post_exit_check_seconds=[30, 60, 90],
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                is_live=True,
            )
            state = StateStore(config.db_path)
            state.upsert_detected(stream)
            state.mark_exited(stream.video_id, 0)
            probe = SequenceProbe([TerminalVideoUnavailableError("private video")])
            manager = RecordingDownloadManager(
                config,
                state,
                probe,  # type: ignore[arg-type]
                sleep_func=fake_sleep,
                probe_video_func=probe.probe_video_async,
                logger=NULL_LOGGER,
            )

            manager._defer_post_exit_retry = MagicMock()  # type: ignore[method-assign]
            await manager.handle_post_exit(stream, 1)
            record = state.get_stream(stream.video_id)
            state.close()

        self.assertEqual(probe.calls, 1)
        self.assertEqual(sleeps, [30])
        self.assertIsNotNone(record)
        self.assertEqual(record.status, "checking_after_exit")
        self.assertEqual(manager.started, [])
        manager._defer_post_exit_retry.assert_called_once_with(stream, 1)

    async def test_terminal_unavailable_preserves_tracks_during_reconnect(self) -> None:
        sleeps: list[float] = []

        async def fake_sleep(delay: float) -> None:
            sleeps.append(delay)

        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                post_exit_check_seconds=[30, 60],
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                is_live=True,
            )
            state = StateStore(config.db_path)
            state.upsert_detected(stream)
            state.mark_exited(stream.video_id, 0)
            probe = SequenceProbe([TerminalVideoUnavailableError("deleted video")])
            manager = RecordingDownloadManager(
                config,
                state,
                probe,  # type: ignore[arg-type]
                sleep_func=fake_sleep,
                probe_video_func=probe.probe_video_async,
                logger=NULL_LOGGER,
            )

            manager._defer_post_exit_retry = MagicMock()  # type: ignore[method-assign]
            await manager.handle_planned_reconnect(stream, 1)
            record = state.get_stream(stream.video_id)
            state.close()

        self.assertEqual(probe.calls, 1)
        self.assertEqual(sleeps, [])
        self.assertIsNotNone(record)
        self.assertEqual(record.status, "checking_after_exit")
        self.assertEqual(manager.started, [])
        manager._defer_post_exit_retry.assert_called_once_with(stream, 1)

    async def test_hls_endlist_confirms_end_when_metadata_is_unavailable(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                post_exit_check_seconds=[0],
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                is_live=True,
            )
            state = StateStore(config.db_path)
            state.upsert_detected(stream)
            state.mark_exited(stream.video_id, 0)
            probe = SequenceProbe([TerminalVideoUnavailableError("private video")])
            edge_probe = AsyncMock(
                return_value=YouTubeLiveEdge(None, None, has_endlist=True)
            )
            manager = DownloadManager(
                config,
                state,
                probe,  # type: ignore[arg-type]
                probe_video_func=probe.probe_video_async,
                probe_youtube_live_edge_func=edge_probe,
                logger=NULL_LOGGER,
            )
            try:
                await manager.handle_post_exit(
                    stream, 1, expected_status="checking_after_exit"
                )
                status = state.get_stream(stream.video_id).status
            finally:
                state.close()

        self.assertEqual(status, "ended")
        edge_probe.assert_awaited_once_with(stream.url)

    async def test_finalizes_leftover_part_files_after_post_exit_window(self) -> None:
        async def fake_sleep(delay: float) -> None:
            return None

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake_ffmpeg = root / "fake-ffmpeg"
            fake_ffmpeg.write_text(
                "#!/bin/sh\n"
                "out=\"\"\n"
                "for arg do out=\"$arg\"; done\n"
                "printf merged > \"$out\"\n",
                encoding="utf-8",
            )
            fake_ffmpeg.chmod(0o755)

            config = BotConfig(
                download_dir=root / "downloads",
                state_dir=root / "state",
                post_exit_check_seconds=[0, 1],
                ffmpeg_path=str(fake_ffmpeg),
            )
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                channel="Example Channel",
                is_live=True,
            )
            non_live = LiveStream(
                video_id="LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                channel="Example Channel",
                is_live=False,
            )
            segment_dir = config.download_dir / "Example_Channel" / "LIVEVIDEO01"
            segment_dir.mkdir(parents=True)
            (segment_dir / "segment-001.f140.mp4.part").write_text("audio", encoding="utf-8")
            (segment_dir / "segment-001.f140.mp4.ytdl").write_text("{}", encoding="utf-8")
            (segment_dir / "segment-001.f299.mp4.part").write_text("video", encoding="utf-8")
            (segment_dir / "segment-001.f299.mp4.ytdl").write_text("{}", encoding="utf-8")
            (segment_dir / "segment-001.f299.mp4.part-Frag2727.part").write_text(
                "",
                encoding="utf-8",
            )

            state = StateStore(config.db_path)
            state.upsert_detected(stream)
            state.mark_exited(stream.video_id, 0)
            probe = SequenceProbe([non_live, non_live])
            manager = DownloadManager(
                config,
                state,
                probe,  # type: ignore[arg-type]
                sleep_func=fake_sleep,
                probe_video_func=probe.probe_video_async,
                logger=NULL_LOGGER,
            )

            media_inputs = [
                FinalizeMediaStream(
                    path=segment_dir / "segment-001.f140.mp4.part",
                    input_index=0,
                    stream_index=0,
                    codec_type="audio",
                    duration=100.0,
                    size=5,
                    partial=True,
                ),
                FinalizeMediaStream(
                    path=segment_dir / "segment-001.f299.mp4.part",
                    input_index=1,
                    stream_index=0,
                    codec_type="video",
                    duration=100.0,
                    size=5,
                    partial=True,
                ),
            ]
            with (
                patch(
                    "onlysavemevods.downloader.probe_finalize_media_streams",
                    return_value=media_inputs,
                ),
                patch(
                    "onlysavemevods.downloader.validate_finalize_output",
                    return_value=FinalizeOutputValidation(
                        duration=100.0,
                        audio_streams=1,
                        video_streams=1,
                    ),
                ),
            ):
                await manager.handle_post_exit(stream, 1)
            record = state.get_stream(stream.video_id)
            state.close()

            self.assertEqual(
                (segment_dir / "video [LIVEVIDEO01].mp4").read_text(),
                "merged",
            )
            self.assertFalse((segment_dir / "segment-001.mp4").exists())
            self.assertFalse((segment_dir / "segment-001.f140.mp4.part").exists())
            self.assertFalse((segment_dir / "segment-001.f299.mp4.part").exists())
            self.assertFalse((segment_dir / "segment-001.f140.mp4").exists())
            self.assertFalse((segment_dir / "segment-001.f299.mp4").exists())
            self.assertFalse((segment_dir / "segment-001.f140.mp4.ytdl").exists())
            self.assertFalse((segment_dir / "segment-001.f299.mp4.ytdl").exists())
            self.assertFalse(
                (segment_dir / "segment-001.f299.mp4.part-Frag2727.part").exists()
            )

        self.assertIsNotNone(record)
        self.assertEqual(record.status, "ended")
