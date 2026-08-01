from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, patch
import asyncio
import logging
import unittest

from onlysavemevods.config import BotConfig, DEFAULT_POST_EXIT_CHECK_SECONDS
from onlysavemevods.downloader import (
    DownloadManager,
    FinalizeMediaStream,
    FinalizeOutputValidation,
)
from onlysavemevods.models import LiveStream, video_url
from onlysavemevods.state import StateStore
from onlysavemevods.youtube import TerminalVideoUnavailableError, YouTubeLiveEdge


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

        post_exit.assert_awaited_once_with(stream, 1)
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

    async def test_probe_failures_do_not_end_early(self) -> None:
        sleeps = 0

        async def fake_sleep(delay: float) -> None:
            nonlocal sleeps
            sleeps += 1

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
            probe = SequenceProbe([RuntimeError("network"), RuntimeError("extractor"), non_live])
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

        self.assertEqual(probe.calls, 3)
        self.assertEqual(sleeps, 3)
        self.assertIsNotNone(record)
        self.assertEqual(record.status, "ended")

    async def test_terminal_unavailable_stops_post_exit_checks(self) -> None:
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

            await manager.handle_post_exit(stream, 1)
            record = state.get_stream(stream.video_id)
            state.close()

        self.assertEqual(probe.calls, 1)
        self.assertEqual(sleeps, [30])
        self.assertIsNotNone(record)
        self.assertEqual(record.status, "ended")
        self.assertEqual(manager.started, [])

    async def test_terminal_unavailable_stops_planned_reconnect_checks(self) -> None:
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

            await manager.handle_planned_reconnect(stream, 1)
            record = state.get_stream(stream.video_id)
            state.close()

        self.assertEqual(probe.calls, 1)
        self.assertEqual(sleeps, [])
        self.assertIsNotNone(record)
        self.assertEqual(record.status, "ended")
        self.assertEqual(manager.started, [])

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
                post_exit_check_seconds=[0],
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
            probe = SequenceProbe([non_live])
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
