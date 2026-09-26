"""Independent live-track recovery must preserve the other active recorder."""

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

from onlysavemevods.config import BotConfig
from onlysavemevods.downloader import (
    DownloadManager,
    LIVE_PROGRESS_MARKER,
    segment_directory,
)
from onlysavemevods.models import LiveStream, video_url
from onlysavemevods.state import StateStore
from onlysavemevods.youtube import YouTubeLiveEdge


class FakeProcess:
    def __init__(self) -> None:
        self.stdout = None
        self.returncode: int | None = None
        self.exited = asyncio.Event()

    async def wait(self) -> int:
        await self.exited.wait()
        assert self.returncode is not None
        return self.returncode

    def terminate(self) -> None:
        self.returncode = -15
        self.exited.set()

    def kill(self) -> None:
        self.terminate()


def stream_with_split_formats() -> LiveStream:
    return LiveStream(
        video_id="youtube:LIVEVIDEO01",
        url=video_url("LIVEVIDEO01"),
        channel="Example",
        platform="youtube",
        is_live=True,
        raw={
            "formats": [
                {
                    "format_id": "303",
                    "vcodec": "vp9",
                    "acodec": "none",
                    "height": 1080,
                },
                {
                    "format_id": "140",
                    "vcodec": "none",
                    "acodec": "mp4a.40.2",
                },
            ]
        },
    )


def progress(track: str, fragment: int) -> str:
    format_id, video_codec, audio_codec = (
        ("303", "vp9", "none")
        if track == "video"
        else ("140", "none", "mp4a.40.2")
    )
    return (
        f"{LIVE_PROGRESS_MARKER}\t{format_id}\t{video_codec}\t{audio_codec}\t"
        f"{fragment}\t{fragment}\t0\t1"
    )


async def wait_until(predicate) -> None:
    async def poll() -> None:
        while not predicate():
            await asyncio.sleep(0)

    await asyncio.wait_for(poll(), timeout=2)


class TrackRestartTests(IsolatedAsyncioTestCase):
    async def test_video_restart_keeps_audio_and_recording_lifecycle(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = BotConfig(
                download_dir=root / "downloads",
                state_dir=root / "state",
                youtube_live_edge_recovery_seconds=0,
                youtube_stale_live_timeout_seconds=0,
            )
            state = StateStore(config.db_path)
            stream = stream_with_split_formats()
            manager = DownloadManager(config, state, probe=None)  # type: ignore[arg-type]
            original_video = FakeProcess()
            original_audio = FakeProcess()
            replacement_video = FakeProcess()
            with patch(
                "onlysavemevods.downloader.asyncio.create_subprocess_exec",
                new=AsyncMock(
                    side_effect=[original_video, original_audio, replacement_video]
                ),
            ) as spawn:
                try:
                    self.assertTrue(await manager.start_stream(stream))
                    await wait_until(lambda: spawn.await_count == 2)
                    active = manager.active[stream.video_id]
                    tracker = active.audio_tracker
                    self.assertIsNotNone(tracker)
                    assert tracker is not None
                    original_video_task = active.task

                    # The exiting old video process must not run the usual
                    # post-exit path or stop the independent audio process.
                    with patch.object(
                        state, "mark_exited", wraps=state.mark_exited
                    ) as mark_exited:
                        await manager._restart_video_track(active, tracker)
                        await wait_until(lambda: original_video_task.done())
                        mark_exited.assert_not_called()

                    self.assertIs(manager.active[stream.video_id], active)
                    self.assertIs(active.process, replacement_video)
                    self.assertIs(active.audio_process, original_audio)
                    self.assertIsNone(original_audio.returncode)
                    self.assertEqual(original_video.returncode, -15)
                    self.assertEqual(state.get_stream(stream.video_id).status, "downloading")
                    self.assertNotIn(stream.video_id, manager._draining_audio)
                    self.assertEqual(spawn.await_count, 3)
                finally:
                    await manager.stop_all()
                    state.close()

    async def test_unsafe_audio_rollover_takes_over_while_video_restarts(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = BotConfig(
                download_dir=root / "downloads",
                state_dir=root / "state",
                youtube_live_edge_recovery_seconds=0,
                youtube_stale_live_timeout_seconds=0,
            )
            state = StateStore(config.db_path)
            stream = stream_with_split_formats()
            manager = DownloadManager(
                config,
                state,
                probe=None,  # type: ignore[arg-type]
                sleep_func=AsyncMock(),
            )
            video_process = FakeProcess()
            audio_process = FakeProcess()
            video_stopped = asyncio.Event()
            finish_video_stop = asyncio.Event()

            async def pause_video_stop(video_id: str, process: FakeProcess) -> None:
                self.assertEqual(video_id, stream.video_id)
                self.assertIs(process, video_process)
                process.terminate()
                video_stopped.set()
                await finish_video_stop.wait()

            with (
                patch(
                    "onlysavemevods.downloader.asyncio.create_subprocess_exec",
                    new=AsyncMock(side_effect=[video_process, audio_process]),
                ) as spawn,
                patch.object(manager, "_stop_stale_live_process", new=pause_video_stop),
                patch.object(manager, "handle_planned_reconnect", new=AsyncMock()) as rollover,
                patch(
                    "onlysavemevods.downloader.restore_mixed_segment_for_resume",
                    return_value=False,
                ),
            ):
                restart_task = None
                try:
                    self.assertTrue(await manager.start_stream(stream))
                    await wait_until(lambda: spawn.await_count == 2)
                    active = manager.active[stream.video_id]
                    tracker = active.audio_tracker
                    self.assertIsNotNone(tracker)
                    assert tracker is not None
                    audio_final = segment_directory(
                        config, stream.video_id, stream.channel
                    ) / "segment-001.f140.m4a"
                    audio_final.write_bytes(b"completed audio with no resume checkpoint")

                    restart_task = asyncio.create_task(
                        manager._restart_video_track(active, tracker)
                    )
                    await asyncio.wait_for(video_stopped.wait(), timeout=2)
                    self.assertEqual(video_process.returncode, -15)
                    self.assertIsNone(audio_process.returncode)

                    # Audio discovers its completed file cannot be resumed.
                    # It requests a segment rollover against a video process
                    # that has already exited for its independent retry.
                    audio_process.terminate()
                    await wait_until(
                        lambda: stream.video_id in manager._planned_reconnects
                    )
                    self.assertFalse(restart_task.done())
                    finish_video_stop.set()
                    await asyncio.wait_for(restart_task, timeout=2)
                    await wait_until(lambda: rollover.await_count == 1)

                    rollover.assert_awaited_once_with(stream, 1)
                    self.assertNotIn(stream.video_id, manager.active)
                    self.assertEqual(
                        state.get_stream(stream.video_id).status,
                        "checking_after_exit",
                    )
                    self.assertEqual(spawn.await_count, 2)
                    self.assertFalse(active.video_restarting)
                finally:
                    finish_video_stop.set()
                    if restart_task is not None:
                        await asyncio.gather(restart_task, return_exceptions=True)
                    await manager.stop_all()
                    state.close()

    async def test_audio_rollover_during_video_spawn_retry_hands_off_lifecycle(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = BotConfig(
                download_dir=root / "downloads",
                state_dir=root / "state",
                youtube_live_edge_recovery_seconds=0,
                youtube_stale_live_timeout_seconds=0,
            )
            state = StateStore(config.db_path)
            stream = stream_with_split_formats()
            video_process = FakeProcess()
            audio_process = FakeProcess()
            video_retry_sleeping = asyncio.Event()
            release_video_retry = asyncio.Event()
            restart_task = None

            async def controlled_sleep(_seconds: float) -> None:
                if asyncio.current_task() is restart_task:
                    video_retry_sleeping.set()
                    await release_video_retry.wait()
                else:
                    await asyncio.sleep(0)

            manager = DownloadManager(
                config,
                state,
                probe=None,  # type: ignore[arg-type]
                sleep_func=controlled_sleep,
            )
            with (
                patch(
                    "onlysavemevods.downloader.asyncio.create_subprocess_exec",
                    new=AsyncMock(
                        side_effect=[
                            video_process,
                            audio_process,
                            OSError("video respawn failed"),
                        ]
                    ),
                ) as spawn,
                patch.object(manager, "handle_planned_reconnect", new=AsyncMock()) as rollover,
                patch(
                    "onlysavemevods.downloader.restore_mixed_segment_for_resume",
                    return_value=False,
                ),
            ):
                try:
                    self.assertTrue(await manager.start_stream(stream))
                    await wait_until(lambda: spawn.await_count == 2)
                    active = manager.active[stream.video_id]
                    tracker = active.audio_tracker
                    self.assertIsNotNone(tracker)
                    assert tracker is not None
                    audio_final = segment_directory(
                        config, stream.video_id, stream.channel
                    ) / "segment-001.f140.m4a"
                    audio_final.write_bytes(b"completed audio with no resume checkpoint")

                    restart_task = asyncio.create_task(
                        manager._restart_video_track(active, tracker)
                    )
                    await asyncio.wait_for(video_retry_sleeping.wait(), timeout=2)
                    self.assertEqual(video_process.returncode, -15)
                    self.assertTrue(active.video_restarting)
                    self.assertEqual(spawn.await_count, 3)

                    audio_process.terminate()
                    await wait_until(
                        lambda: stream.video_id in manager._planned_reconnects
                    )
                    release_video_retry.set()
                    await asyncio.wait_for(restart_task, timeout=2)
                    await wait_until(lambda: rollover.await_count == 1)

                    rollover.assert_awaited_once_with(stream, 1)
                    self.assertNotIn(stream.video_id, manager.active)
                    self.assertEqual(
                        state.get_stream(stream.video_id).status,
                        "checking_after_exit",
                    )
                    self.assertEqual(spawn.await_count, 3)
                    self.assertFalse(active.video_restarting)
                finally:
                    release_video_retry.set()
                    if restart_task is not None:
                        await asyncio.gather(restart_task, return_exceptions=True)
                    await manager.stop_all()
                    state.close()

    async def test_moving_edge_restarts_each_stalled_track_independently(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = BotConfig(
                download_dir=root / "downloads",
                state_dir=root / "state",
                youtube_live_edge_recovery_seconds=0,
                youtube_stale_live_timeout_seconds=0,
            )
            state = StateStore(config.db_path)
            stream = stream_with_split_formats()
            now = [0.0]
            first_video, replacement_video = FakeProcess(), FakeProcess()
            first_audio, replacement_audio = FakeProcess(), FakeProcess()
            processes = {
                "303": [first_video, replacement_video],
                "140": [first_audio, replacement_audio],
            }

            async def spawn(*command, **_kwargs):
                fmt = command[command.index("--format") + 1]
                return processes[fmt].pop(0)

            async def advance(seconds: float) -> None:
                now[0] += seconds
                await asyncio.sleep(0)

            source_time = datetime.now(timezone.utc)
            edge_probe = AsyncMock(
                side_effect=[
                    YouTubeLiveEdge(100, source_time),
                    YouTubeLiveEdge(101, source_time),
                ]
            )
            manager = DownloadManager(
                config,
                state,
                probe=None,  # type: ignore[arg-type]
                sleep_func=advance,
                monotonic_func=lambda: now[0],
                probe_youtube_live_edge_func=edge_probe,
            )
            with patch(
                "onlysavemevods.downloader.asyncio.create_subprocess_exec",
                new=AsyncMock(side_effect=spawn),
            ) as spawn_mock:
                try:
                    self.assertTrue(await manager.start_stream(stream))
                    await wait_until(lambda: spawn_mock.await_count == 2)
                    active = manager.active[stream.video_id]
                    tracker = active.audio_tracker
                    self.assertIsNotNone(tracker)
                    assert tracker is not None
                    tracker.update(progress("video", 100))
                    tracker.update(progress("audio", 772))

                    # Enable recovery after startup so this test controls the
                    # watchdog run and its virtual clock directly.
                    config.youtube_live_edge_recovery_seconds = 30
                    with (
                        patch.object(state, "mark_exited", wraps=state.mark_exited) as mark_exited,
                        patch.object(
                            manager,
                            "_request_process_reconnect",
                            new=AsyncMock(
                                side_effect=AssertionError(
                                    "whole-stream reconnect must not run for track recovery"
                                )
                            ),
                        ),
                    ):
                        await asyncio.wait_for(
                            manager._stale_youtube_live_watchdog(
                                stream, first_video, tracker
                            ),
                            timeout=2,
                        )
                        await wait_until(
                            lambda: active.process is replacement_video
                            and active.audio_process is replacement_audio
                        )
                        mark_exited.assert_not_called()

                    self.assertEqual(edge_probe.await_count, 2)
                    self.assertEqual(first_video.returncode, -15)
                    self.assertEqual(first_audio.returncode, -15)
                    self.assertIsNone(replacement_video.returncode)
                    self.assertIsNone(replacement_audio.returncode)
                    self.assertIs(manager.active[stream.video_id], active)
                    self.assertEqual(state.get_stream(stream.video_id).status, "downloading")
                    self.assertNotIn(stream.video_id, manager._draining_audio)
                    self.assertEqual(spawn_mock.await_count, 4)
                finally:
                    await manager.stop_all()
                    state.close()
