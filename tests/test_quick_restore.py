"""Fast recovery after a YouTube download reaches the live edge."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
import asyncio
import unittest

from onlysavemevods.config import BotConfig
from onlysavemevods.downloader import (
    ActiveDownload,
    CatchupTracker,
    DownloadManager,
    LIVE_PROGRESS_MARKER,
)
from onlysavemevods.models import LiveStream, video_url
from onlysavemevods.youtube import YouTubeLiveEdge


def progress(track: str, fragment: int, count: int) -> str:
    format_id, video_codec, audio_codec = (
        ("303", "vp9", "none")
        if track == "video"
        else ("140", "none", "mp4a.40.2")
    )
    return (
        f"{LIVE_PROGRESS_MARKER}\t{format_id}\t{video_codec}\t{audio_codec}\t"
        f"{fragment}\t{count}\t0\t1"
    )


def live_stream() -> LiveStream:
    return LiveStream(
        video_id="youtube:LIVEVIDEO01",
        url=video_url("LIVEVIDEO01"),
        platform="youtube",
        is_live=True,
    )


class QuickRestoreTrackerTests(unittest.TestCase):
    def test_quick_recovery_defaults_to_thirty_seconds(self) -> None:
        self.assertEqual(BotConfig().youtube_live_edge_recovery_seconds, 30)

    def test_playlist_growth_does_not_reset_saved_fragment_inactivity(self) -> None:
        now = [10.0]
        tracker = CatchupTracker(asyncio.Event(), monotonic_func=lambda: now[0])
        tracker.update(progress("video", 99, 100))

        now[0] = 20.0
        tracker.update(progress("video", 99, 101))

        self.assertEqual(tracker.fragments["video"], (99, 101))
        self.assertEqual(tracker.inactive_seconds(), 10.0)
        self.assertEqual(tracker.fragment_inactive_seconds(), 10.0)

    def test_zero_fragment_updates_do_not_count_as_saved_progress(self) -> None:
        now = [10.0]
        tracker = CatchupTracker(asyncio.Event(), monotonic_func=lambda: now[0])
        tracker.start_track("audio")

        now[0] = 20.0
        tracker.update(progress("audio", 0, 100))
        now[0] = 25.0
        tracker.update(progress("audio", 0, 101))

        self.assertNotIn("audio", tracker.track_fragment_progress_at)
        self.assertEqual(tracker.inactive_seconds(), 15.0)
        self.assertEqual(tracker.fragment_inactive_seconds(), 15.0)

        tracker.update(progress("audio", 1, 101))
        self.assertEqual(tracker.inactive_seconds(), 0.0)
        self.assertEqual(tracker.fragment_inactive_seconds(), 0.0)

    def test_stalled_audio_at_live_edge_is_detected_after_thirty_seconds(self) -> None:
        now = [0.0]
        tracker = CatchupTracker(asyncio.Event(), monotonic_func=lambda: now[0])
        tracker.update(progress("audio", 772, 772))
        tracker.update(progress("video", 100, 100))

        now[0] = 29.0
        tracker.update(progress("video", 101, 101))
        self.assertIsNone(tracker.stalled_live_edge_track(30))

        now[0] = 30.0
        # Playlist growth alone is not evidence that another audio fragment saved.
        tracker.update(progress("audio", 772, 773))
        self.assertEqual(tracker.stalled_live_edge_track(30), "audio")

        tracker.update(progress("audio", 773, 773))
        self.assertIsNone(tracker.stalled_live_edge_track(30))

    def test_fast_recovery_requires_prior_live_edge_and_advancing_companion(self) -> None:
        now = [0.0]
        tracker = CatchupTracker(asyncio.Event(), monotonic_func=lambda: now[0])
        tracker.update(progress("audio", 500, 772))
        tracker.update(progress("video", 100, 100))
        now[0] = 31.0
        tracker.update(progress("video", 101, 101))
        self.assertIsNone(tracker.stalled_live_edge_track(30))

        tracker.update(progress("audio", 772, 772))
        now[0] = 62.0
        # Both tracks are quiet, so no track-specific restart is justified.
        self.assertIsNone(tracker.stalled_live_edge_track(30))


    def test_both_restarted_tracks_can_be_idle_without_new_fragments(self) -> None:
        now = [0.0]
        tracker = CatchupTracker(asyncio.Event(), monotonic_func=lambda: now[0])
        tracker.update(progress("video", 100, 100))
        tracker.update(progress("audio", 772, 772))
        now[0] = 10.0
        tracker.start_track("video")
        tracker.start_track("audio")
        self.assertFalse(tracker.track_fragment_progress_at)
        self.assertEqual(tracker.track_live_edge_seen, {"video", "audio"})

        now[0] = 39.0
        self.assertEqual(tracker.fragment_inactive_seconds(), 29.0)
        now[0] = 40.0
        self.assertEqual(tracker.fragment_inactive_seconds(), 30.0)
        self.assertIsNone(tracker.stalled_live_edge_track(30))

    def test_restarted_track_with_no_fragments_is_detected_at_thirty_seconds(self) -> None:
        for silent_track in ("video", "audio"):
            with self.subTest(track=silent_track):
                now = [0.0]
                tracker = CatchupTracker(
                    asyncio.Event(), monotonic_func=lambda: now[0]
                )
                tracker.update(progress("video", 100, 100))
                tracker.update(progress("audio", 772, 772))
                tracker.start_track(silent_track)
                self.assertNotIn(silent_track, tracker.fragments)

                advancing_track = "audio" if silent_track == "video" else "video"
                base_fragment = 772 if advancing_track == "audio" else 100
                now[0] = 29.0
                tracker.update(progress(advancing_track, base_fragment + 1, base_fragment + 1))
                self.assertIsNone(tracker.stalled_live_edge_track(30))

                now[0] = 30.0
                tracker.update(progress(advancing_track, base_fragment + 2, base_fragment + 2))
                self.assertEqual(tracker.stalled_live_edge_track(30), silent_track)


class QuickRestoreWatchdogTests(unittest.IsolatedAsyncioTestCase):
    async def test_long_watchdog_restarts_idle_tracks_when_source_edge_advances(
        self,
    ) -> None:
        for recovery_seconds, saved, count in ((0, 100, 100), (30, 10, 1000)):
            with self.subTest(recovery_seconds=recovery_seconds):
                now = [0.0]
                stream = live_stream()
                tracker = CatchupTracker(
                    asyncio.Event(), monotonic_func=lambda: now[0]
                )
                tracker.update(progress("video", saved, count))
                tracker.update(progress("audio", saved, count))
                source_time = datetime.now(timezone.utc)
                edge_probe = AsyncMock(
                    side_effect=[
                        YouTubeLiveEdge(100, source_time),
                        YouTubeLiveEdge(101, source_time),
                    ]
                )

                async def advance(seconds: float) -> None:
                    now[0] += seconds
                    # Remote fragment totals keep increasing while both saved
                    # counters stay fixed.
                    for track in ("video", "audio"):
                        tracker.update(progress(track, saved, count + int(now[0])))

                state = MagicMock()
                manager = DownloadManager(
                    BotConfig(
                        youtube_live_edge_recovery_seconds=recovery_seconds,
                        youtube_stale_live_timeout_seconds=10,
                    ),
                    state,
                    probe=None,  # type: ignore[arg-type]
                    sleep_func=advance,
                    probe_youtube_live_edge_func=edge_probe,
                    monotonic_func=lambda: now[0],
                )
                video_process = MagicMock(returncode=None)
                audio_process = MagicMock(returncode=None)
                active = ActiveDownload(
                    stream=stream,
                    process=video_process,
                    segment_index=1,
                    output_template=Path("segment-001.%(ext)s"),
                    task=MagicMock(),
                    audio_process=audio_process,
                    audio_task=MagicMock(),
                    video_format_id="303",
                )
                manager.active[stream.video_id] = active
                with (
                    patch.object(manager, "_restart_video_track", new=AsyncMock()) as restart_video,
                    patch.object(manager, "_stop_stale_live_process", new=AsyncMock()) as stop_audio,
                    patch.object(manager, "_request_process_reconnect", new=AsyncMock()) as reconnect,
                ):
                    await manager._stale_youtube_live_watchdog(
                        stream, video_process, tracker
                    )

                restart_video.assert_awaited_once_with(active, tracker)
                stop_audio.assert_awaited_once_with(stream.video_id, audio_process)
                reconnect.assert_not_awaited()
                state.mark_youtube_stale_live.assert_not_called()
                self.assertEqual(edge_probe.await_count, 2)
                self.assertEqual(tracker.last_fragment_progress_at, 0.0)

    async def test_long_probe_does_not_stop_audio_started_during_confirmation(
        self,
    ) -> None:
        now = [0.0]
        stream = live_stream()
        tracker = CatchupTracker(asyncio.Event(), monotonic_func=lambda: now[0])
        tracker.update(progress("video", 100, 100))
        tracker.update(progress("audio", 100, 100))
        video_process = MagicMock(returncode=None)
        fresh_audio = MagicMock(returncode=None)
        source_time = datetime.now(timezone.utc)
        edge_probe = AsyncMock(
            side_effect=[
                YouTubeLiveEdge(100, source_time),
                YouTubeLiveEdge(101, source_time),
            ]
        )

        async def advance(seconds: float) -> None:
            now[0] += seconds
            if seconds == 30:
                active.audio_process = fresh_audio
                tracker.start_track("audio")
            elif now[0] >= 50:
                video_process.returncode = 0

        manager = DownloadManager(
            BotConfig(
                youtube_live_edge_recovery_seconds=0,
                youtube_stale_live_timeout_seconds=10,
            ),
            MagicMock(),
            probe=None,  # type: ignore[arg-type]
            sleep_func=advance,
            probe_youtube_live_edge_func=edge_probe,
            monotonic_func=lambda: now[0],
        )
        active = ActiveDownload(
            stream=stream,
            process=video_process,
            segment_index=1,
            output_template=Path("segment-001.%(ext)s"),
            task=MagicMock(),
            audio_process=MagicMock(returncode=-15),
            audio_task=MagicMock(),
            video_format_id="303",
        )
        manager.active[stream.video_id] = active
        with (
            patch.object(manager, "_restart_video_track", new=AsyncMock()) as restart_video,
            patch.object(manager, "_stop_stale_live_process", new=AsyncMock()) as stop_track,
            patch.object(manager, "_request_process_reconnect", new=AsyncMock()) as reconnect,
        ):
            await manager._stale_youtube_live_watchdog(stream, video_process, tracker)

        self.assertIs(active.audio_process, fresh_audio)
        self.assertIsNone(fresh_audio.returncode)
        self.assertEqual(edge_probe.await_count, 1)
        restart_video.assert_not_awaited()
        stop_track.assert_not_awaited()
        reconnect.assert_not_awaited()

    async def test_long_watchdog_waits_for_audio_started_before_first_probe(
        self,
    ) -> None:
        now = [0.0]
        stream = live_stream()
        tracker = CatchupTracker(asyncio.Event(), monotonic_func=lambda: now[0])
        tracker.update(progress("video", 100, 100))
        tracker.update(progress("audio", 100, 100))
        video_process = MagicMock(returncode=None)
        fresh_audio = MagicMock(returncode=None)
        edge_probe = AsyncMock()

        async def advance(seconds: float) -> None:
            if now[0] == 0.0:
                # Audio starts midway through the first watchdog interval.
                now[0] = 5.0
                active.audio_process = fresh_audio
                tracker.start_track("audio")
                now[0] = seconds
            else:
                video_process.returncode = 0

        manager = DownloadManager(
            BotConfig(
                youtube_live_edge_recovery_seconds=0,
                youtube_stale_live_timeout_seconds=10,
            ),
            MagicMock(),
            probe=None,  # type: ignore[arg-type]
            sleep_func=advance,
            probe_youtube_live_edge_func=edge_probe,
            monotonic_func=lambda: now[0],
        )
        active = ActiveDownload(
            stream=stream,
            process=video_process,
            segment_index=1,
            output_template=Path("segment-001.%(ext)s"),
            task=MagicMock(),
            audio_process=MagicMock(returncode=-15),
            audio_task=MagicMock(),
            video_format_id="303",
        )
        manager.active[stream.video_id] = active
        with (
            patch.object(manager, "_restart_video_track", new=AsyncMock()) as restart_video,
            patch.object(manager, "_stop_stale_live_process", new=AsyncMock()) as stop_track,
            patch.object(manager, "_request_process_reconnect", new=AsyncMock()) as reconnect,
        ):
            await manager._stale_youtube_live_watchdog(stream, video_process, tracker)

        self.assertEqual(tracker.inactive_seconds(), 10.0)
        self.assertEqual(tracker.fragment_inactive_seconds(), 5.0)
        self.assertIs(active.audio_process, fresh_audio)
        self.assertIsNone(fresh_audio.returncode)
        edge_probe.assert_not_awaited()
        restart_video.assert_not_awaited()
        stop_track.assert_not_awaited()
        reconnect.assert_not_awaited()

    async def test_frozen_live_source_is_paused_without_finalizing_media(self) -> None:
        now = [0.0]
        stream = live_stream()
        tracker = CatchupTracker(asyncio.Event(), monotonic_func=lambda: now[0])
        tracker.update(progress("video", 100, 100))
        tracker.update(progress("audio", 100, 100))
        edge = YouTubeLiveEdge(
            100, datetime.now(timezone.utc) - timedelta(hours=1)
        )
        edge_probe = AsyncMock(return_value=edge)

        async def advance(seconds: float) -> None:
            now[0] += seconds

        state = MagicMock()
        manager = DownloadManager(
            BotConfig(
                youtube_live_edge_recovery_seconds=0,
                youtube_stale_live_timeout_seconds=10,
            ),
            state,
            probe=None,  # type: ignore[arg-type]
            sleep_func=advance,
            probe_youtube_live_edge_func=edge_probe,
            monotonic_func=lambda: now[0],
        )
        video_process = MagicMock(returncode=None)
        with (
            patch.object(manager, "_stop_stale_live_process", new=AsyncMock()) as stop_track,
            patch.object(manager, "_request_process_reconnect", new=AsyncMock()) as reconnect,
            patch.object(manager, "finish_ended_stream", new=AsyncMock()) as finish,
            patch.object(manager, "finalize_ended_segment", new=AsyncMock()) as finalize,
        ):
            await manager._stale_youtube_live_watchdog(stream, video_process, tracker)

        state.mark_youtube_stale_live.assert_called_once()
        stop_track.assert_awaited_once_with(stream.video_id, video_process)
        reconnect.assert_not_awaited()
        finish.assert_not_awaited()
        finalize.assert_not_awaited()
        self.assertEqual(edge_probe.await_count, 2)

    async def test_output_monitor_uses_process_track_when_codecs_are_missing(self) -> None:
        tracker = CatchupTracker(asyncio.Event())
        manager = DownloadManager(
            BotConfig(), MagicMock(), probe=None  # type: ignore[arg-type]
        )
        line = f"{LIVE_PROGRESS_MARKER}\tunknown\tnone\tnone\t1\t2\t0\t1\n"
        for track in ("video", "audio"):
            output = asyncio.StreamReader()
            output.feed_data(line.encode())
            output.feed_eof()
            await manager._monitor_process_output(
                "youtube:LIVEVIDEO01", output, tracker, track_hint=track
            )
        self.assertEqual(tracker.fragments, {"video": (1, 2), "audio": (1, 2)})

    async def test_stalled_live_edge_audio_restarts_only_audio_process(self) -> None:
        now = [0.0]
        stream = live_stream()
        tracker = CatchupTracker(asyncio.Event(), monotonic_func=lambda: now[0])
        tracker.update(progress("video", 100, 100))
        tracker.update(progress("audio", 772, 772))
        video_process = MagicMock(returncode=None)
        audio_process = MagicMock(returncode=None)

        async def advance(seconds: float) -> None:
            now[0] += seconds
            if now[0] <= 30:
                fragment = 100 + int(now[0] / 5)
                tracker.update(progress("video", fragment, fragment))
            else:
                video_process.returncode = 0

        edge_probe = AsyncMock()
        manager = DownloadManager(
            BotConfig(
                youtube_live_edge_recovery_seconds=30,
                youtube_stale_live_timeout_seconds=900,
            ),
            MagicMock(),
            probe=None,  # type: ignore[arg-type]
            sleep_func=advance,
            probe_youtube_live_edge_func=edge_probe,
            monotonic_func=lambda: now[0],
        )
        manager.active[stream.video_id] = ActiveDownload(
            stream=stream,
            process=video_process,
            segment_index=1,
            output_template=Path("segment-001.%(ext)s"),
            task=MagicMock(),
            audio_process=audio_process,
            audio_task=MagicMock(),
        )
        with (
            patch.object(manager, "_stop_stale_live_process", new=AsyncMock()) as stop,
            patch.object(manager, "_request_process_reconnect", new=AsyncMock()) as reconnect,
        ):
            await manager._stale_youtube_live_watchdog(stream, video_process, tracker)

        stop.assert_awaited_once_with(stream.video_id, audio_process)
        reconnect.assert_not_awaited()
        edge_probe.assert_not_awaited()
        self.assertNotIn("audio", tracker.fragments)

    async def test_stalled_live_edge_video_restarts_only_video_process(self) -> None:
        now = [0.0]
        stream = live_stream()
        tracker = CatchupTracker(asyncio.Event(), monotonic_func=lambda: now[0])
        tracker.update(progress("video", 100, 100))
        tracker.update(progress("audio", 772, 772))
        video_process = MagicMock(returncode=None)
        audio_process = MagicMock(returncode=None)

        async def advance(seconds: float) -> None:
            now[0] += seconds
            if now[0] <= 30:
                fragment = 772 + int(now[0] / 5)
                tracker.update(progress("audio", fragment, fragment))
            else:
                video_process.returncode = 0

        manager = DownloadManager(
            BotConfig(
                youtube_live_edge_recovery_seconds=30,
                youtube_stale_live_timeout_seconds=900,
            ),
            MagicMock(),
            probe=None,  # type: ignore[arg-type]
            sleep_func=advance,
            monotonic_func=lambda: now[0],
        )
        active = ActiveDownload(
            stream=stream,
            process=video_process,
            segment_index=1,
            output_template=Path("segment-001.%(ext)s"),
            task=MagicMock(),
            audio_process=audio_process,
            audio_task=MagicMock(),
        )
        manager.active[stream.video_id] = active
        with (
            patch.object(manager, "_restart_video_track", new=AsyncMock()) as restart_video,
            patch.object(manager, "_stop_stale_live_process", new=AsyncMock()) as stop_audio,
            patch.object(manager, "_request_process_reconnect", new=AsyncMock()) as reconnect,
        ):
            await manager._stale_youtube_live_watchdog(
                stream, video_process, tracker
            )

        restart_video.assert_awaited_once_with(active, tracker)
        stop_audio.assert_not_awaited()
        reconnect.assert_not_awaited()

    async def test_both_tracks_idle_restart_each_track_when_source_edge_advances(self) -> None:
        now = [0.0]
        stream = live_stream()
        tracker = CatchupTracker(asyncio.Event(), monotonic_func=lambda: now[0])
        tracker.update(progress("video", 100, 100))
        tracker.update(progress("audio", 772, 772))
        source_time = datetime.now(timezone.utc)
        edge_probe = AsyncMock(
            side_effect=[
                YouTubeLiveEdge(100, source_time),
                YouTubeLiveEdge(101, source_time),
            ]
        )

        async def advance(seconds: float) -> None:
            now[0] += seconds

        manager = DownloadManager(
            BotConfig(
                youtube_live_edge_recovery_seconds=30,
                youtube_stale_live_timeout_seconds=900,
            ),
            MagicMock(),
            probe=None,  # type: ignore[arg-type]
            sleep_func=advance,
            probe_youtube_live_edge_func=edge_probe,
            monotonic_func=lambda: now[0],
        )
        process = MagicMock(returncode=None)
        audio_process = MagicMock(returncode=None)
        active = ActiveDownload(
            stream=stream,
            process=process,
            segment_index=1,
            output_template=Path("segment-001.%(ext)s"),
            task=MagicMock(),
            audio_process=audio_process,
            audio_task=MagicMock(),
        )
        manager.active[stream.video_id] = active
        with (
            patch.object(manager, "_restart_video_track", new=AsyncMock()) as restart_video,
            patch.object(manager, "_stop_stale_live_process", new=AsyncMock()) as stop_audio,
            patch.object(manager, "_request_process_reconnect", new=AsyncMock()) as reconnect,
        ):
            await manager._stale_youtube_live_watchdog(stream, process, tracker)

        restart_video.assert_awaited_once_with(active, tracker)
        stop_audio.assert_awaited_once_with(stream.video_id, audio_process)
        reconnect.assert_not_awaited()
        self.assertEqual(edge_probe.await_count, 2)

    async def test_new_track_process_during_hls_probe_is_not_restarted(self) -> None:
        now = [0.0]
        stream = live_stream()
        tracker = CatchupTracker(asyncio.Event(), monotonic_func=lambda: now[0])
        tracker.update(progress("video", 100, 100))
        tracker.update(progress("audio", 772, 772))
        source_time = datetime.now(timezone.utc)
        edge_probe = AsyncMock(
            side_effect=[
                YouTubeLiveEdge(100, source_time),
                YouTubeLiveEdge(101, source_time),
            ]
        )
        video_process = MagicMock(returncode=None)
        old_audio = MagicMock(returncode=-15)
        fresh_audio = MagicMock(returncode=None)

        async def advance(seconds: float) -> None:
            now[0] += seconds
            if now[0] == 35.0:
                # Audio restarted during the HLS confirmation delay but has
                # not had time to report its first fragment yet.
                active.audio_process = fresh_audio
                tracker.start_track("audio")
            elif now[0] >= 40.0:
                video_process.returncode = 0

        manager = DownloadManager(
            BotConfig(
                youtube_live_edge_recovery_seconds=30,
                youtube_stale_live_timeout_seconds=900,
            ),
            MagicMock(),
            probe=None,  # type: ignore[arg-type]
            sleep_func=advance,
            probe_youtube_live_edge_func=edge_probe,
            monotonic_func=lambda: now[0],
        )
        active = ActiveDownload(
            stream=stream,
            process=video_process,
            segment_index=1,
            output_template=Path("segment-001.%(ext)s"),
            task=MagicMock(),
            audio_process=old_audio,
            audio_task=MagicMock(),
        )
        manager.active[stream.video_id] = active
        with (
            patch.object(manager, "_restart_video_track", new=AsyncMock()) as restart_video,
            patch.object(manager, "_stop_stale_live_process", new=AsyncMock()) as stop_track,
            patch.object(manager, "_request_process_reconnect", new=AsyncMock()) as reconnect,
        ):
            await manager._stale_youtube_live_watchdog(
                stream, video_process, tracker
            )

        self.assertIs(active.audio_process, fresh_audio)
        self.assertIsNone(fresh_audio.returncode)
        self.assertEqual(edge_probe.await_count, 1)
        restart_video.assert_not_awaited()
        stop_track.assert_not_awaited()
        reconnect.assert_not_awaited()

    async def test_both_tracks_idle_does_not_reconnect_when_source_edge_is_frozen(
        self,
    ) -> None:
        now = [0.0]
        stream = live_stream()
        tracker = CatchupTracker(asyncio.Event(), monotonic_func=lambda: now[0])
        tracker.update(progress("video", 100, 100))
        tracker.update(progress("audio", 772, 772))
        source_time = datetime.now(timezone.utc)
        edge = YouTubeLiveEdge(100, source_time)
        edge_probe = AsyncMock(return_value=edge)
        process = MagicMock(returncode=None)

        async def advance(seconds: float) -> None:
            now[0] += seconds
            if now[0] >= 40:
                process.returncode = 0

        state = MagicMock()
        manager = DownloadManager(
            BotConfig(
                youtube_live_edge_recovery_seconds=30,
                youtube_stale_live_timeout_seconds=900,
            ),
            state,
            probe=None,  # type: ignore[arg-type]
            sleep_func=advance,
            probe_youtube_live_edge_func=edge_probe,
            monotonic_func=lambda: now[0],
        )
        manager.active[stream.video_id] = ActiveDownload(
            stream=stream,
            process=process,
            segment_index=1,
            output_template=Path("segment-001.%(ext)s"),
            task=MagicMock(),
            audio_process=MagicMock(returncode=None),
            audio_task=MagicMock(),
        )
        with patch.object(
            manager, "_request_process_reconnect", new=AsyncMock()
        ) as reconnect:
            await manager._stale_youtube_live_watchdog(stream, process, tracker)

        reconnect.assert_not_awaited()
        state.mark_youtube_stale_live.assert_not_called()
        self.assertEqual(edge_probe.await_count, 2)

    async def test_combined_job_keeps_long_stale_watchdog(self) -> None:
        now = [0.0]
        stream = live_stream()
        tracker = CatchupTracker(asyncio.Event(), monotonic_func=lambda: now[0])
        tracker.update(progress("video", 100, 100))
        tracker.update(progress("audio", 772, 772))
        process = MagicMock(returncode=None)

        async def advance(seconds: float) -> None:
            now[0] += seconds
            if now[0] >= 35:
                process.returncode = 0

        edge_probe = AsyncMock()
        manager = DownloadManager(
            BotConfig(
                youtube_live_edge_recovery_seconds=30,
                youtube_stale_live_timeout_seconds=900,
            ),
            MagicMock(),
            probe=None,  # type: ignore[arg-type]
            sleep_func=advance,
            probe_youtube_live_edge_func=edge_probe,
            monotonic_func=lambda: now[0],
        )
        manager.active[stream.video_id] = ActiveDownload(
            stream=stream,
            process=process,
            segment_index=1,
            output_template=Path("segment-001.%(ext)s"),
            task=MagicMock(),
        )
        with patch.object(
            manager, "_request_process_reconnect", new=AsyncMock()
        ) as reconnect:
            await manager._stale_youtube_live_watchdog(stream, process, tracker)

        reconnect.assert_not_awaited()
        edge_probe.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
