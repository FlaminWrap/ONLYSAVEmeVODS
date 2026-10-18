from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import AsyncMock, Mock, patch

from onlysavemevods.config import BotConfig, StreamerConfig
from onlysavemevods.downloader import DownloadManager
from onlysavemevods.models import LiveStream
from onlysavemevods.state import StateStore


class StreamerArchiveDownloadsTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        self.config = BotConfig(
            streamers={
                "Example": StreamerConfig(sources=["twitch:Example"], archived=True)
            },
            download_dir=root / "downloads",
            state_dir=root / "state",
            reconnect_interval_seconds=0,
        )
        self.state = StateStore(self.config.db_path)
        self.addCleanup(self.state.close)
        self.manager = DownloadManager(self.config, self.state, probe=None)  # type: ignore[arg-type]
        self.manager._ensure_spawn_retry = Mock()  # type: ignore[method-assign]
        self.stream = LiveStream(
            video_id="twitch:LIVESESSION01",
            url="https://www.twitch.tv/Example",
            channel="Example",
            platform="twitch",
            source="twitch:Example",
            is_live=True,
        )

    async def test_archived_streamer_cannot_start_unseen_recording(self) -> None:
        with patch(
            "onlysavemevods.downloader.asyncio.create_subprocess_exec",
            new=AsyncMock(),
        ) as spawn:
            started = await self.manager.start_stream(self.stream)

        self.assertFalse(started)
        spawn.assert_not_awaited()
        self.assertIsNone(self.state.get_stream(self.stream.video_id))

    async def test_archived_previously_detected_stream_cannot_start(self) -> None:
        self.state.upsert_detected(self.stream)
        with patch(
            "onlysavemevods.downloader.asyncio.create_subprocess_exec",
            new=AsyncMock(),
        ) as spawn:
            started = await self.manager.start_stream(self.stream)

        self.assertFalse(started)
        spawn.assert_not_awaited()
        self.assertIsNone(self.state.get_stream(self.stream.video_id).last_started_at)

    async def test_archived_other_platform_does_not_block_active_same_username(self) -> None:
        self.config.streamers = {
            "Archived Kick": StreamerConfig(sources=["kick:Example"], archived=True),
            "Active Twitch": StreamerConfig(sources=["twitch:Example"]),
        }
        with patch(
            "onlysavemevods.downloader.asyncio.create_subprocess_exec",
            new=AsyncMock(side_effect=FileNotFoundError("test spawn")),
        ) as spawn:
            await self.manager.start_stream(self.stream)

        spawn.assert_awaited_once()

    async def test_archived_other_platform_does_not_block_unowned_explicit_source(self) -> None:
        self.config.streamers = {
            "Archived Kick": StreamerConfig(sources=["kick:Example"], archived=True)
        }
        with patch(
            "onlysavemevods.downloader.asyncio.create_subprocess_exec",
            new=AsyncMock(side_effect=FileNotFoundError("test spawn")),
        ) as spawn:
            await self.manager.start_stream(self.stream)

        spawn.assert_awaited_once()

    async def test_source_less_stream_uses_platform_aware_channel_url(self) -> None:
        self.config.streamers = {
            "Archived Kick": StreamerConfig(sources=["kick:Example"], archived=True),
            "Active Twitch": StreamerConfig(sources=["twitch:Example"]),
        }
        stream = replace(self.stream, source="")
        with patch(
            "onlysavemevods.downloader.asyncio.create_subprocess_exec",
            new=AsyncMock(side_effect=FileNotFoundError("test spawn")),
        ) as spawn:
            await self.manager.start_stream(stream)

        spawn.assert_awaited_once()

    async def test_archived_ended_recording_does_not_reopen(self) -> None:
        self.state.mark_downloading(self.stream, 1)
        self.state.mark_exited(self.stream.video_id, 0)
        self.state.mark_ended(self.stream.video_id)
        with patch(
            "onlysavemevods.downloader.asyncio.create_subprocess_exec",
            new=AsyncMock(),
        ) as spawn:
            started = await self.manager.start_stream(self.stream)

        self.assertFalse(started)
        spawn.assert_not_awaited()
        self.assertEqual(self.state.get_stream(self.stream.video_id).status, "ended")

    async def test_archived_recording_can_reconnect_after_exit(self) -> None:
        self.state.mark_downloading(self.stream, 1)
        self.state.mark_exited(self.stream.video_id, 1)
        with patch(
            "onlysavemevods.downloader.asyncio.create_subprocess_exec",
            new=AsyncMock(side_effect=FileNotFoundError("test spawn")),
        ) as spawn:
            await self.manager.start_stream(self.stream, segment_index=2)

        spawn.assert_awaited_once()
        record = self.state.get_stream(self.stream.video_id)
        self.assertEqual(record.segment_index, 2)
        self.assertEqual(record.status, "waiting_retry")

    async def test_archived_stalled_recording_can_resume_after_edge_recovers(self) -> None:
        self.state.mark_downloading(self.stream, 1)
        self.state.mark_youtube_stale_live(
            self.stream.video_id, media_sequence=10, edge_at=""
        )
        self.state.clear_youtube_stale_live(self.stream.video_id)
        with patch(
            "onlysavemevods.downloader.asyncio.create_subprocess_exec",
            new=AsyncMock(side_effect=FileNotFoundError("test spawn")),
        ) as spawn:
            await self.manager.start_stream(self.stream, segment_index=2)

        spawn.assert_awaited_once()
        self.assertEqual(self.state.get_stream(self.stream.video_id).segment_index, 2)

    async def test_archiving_during_startup_prevents_process_spawn(self) -> None:
        self.config.streamers["Example"] = replace(
            self.config.streamers["Example"], archived=False
        )

        async def archive_during_startup(stream: LiveStream) -> bool:
            self.config.streamers["Example"] = replace(
                self.config.streamers["Example"], archived=True
            )
            return True

        self.manager._youtube_stalled_state_allows_start = AsyncMock(  # type: ignore[method-assign]
            side_effect=archive_during_startup
        )
        with patch(
            "onlysavemevods.downloader.asyncio.create_subprocess_exec",
            new=AsyncMock(),
        ) as spawn:
            started = await self.manager.start_stream(self.stream)

        self.assertFalse(started)
        spawn.assert_not_awaited()
        self.assertIsNone(self.state.get_stream(self.stream.video_id).last_started_at)

    async def test_reconnect_probe_cannot_start_replacement_session_when_archived(self) -> None:
        self.state.mark_downloading(self.stream, 1)
        self.state.mark_exited(self.stream.video_id, 1)
        replacement = replace(self.stream, video_id="twitch:NEWSESSION1")
        self.manager.probe_video = AsyncMock(return_value=replacement)
        self.manager._confirm_replaced_session_end = AsyncMock()  # type: ignore[method-assign]

        with patch(
            "onlysavemevods.downloader.asyncio.create_subprocess_exec",
            new=AsyncMock(),
        ) as spawn:
            await self.manager.handle_planned_reconnect(self.stream, 1)

        spawn.assert_not_awaited()
        self.assertIsNone(self.state.get_stream(replacement.video_id))
        self.manager._confirm_replaced_session_end.assert_awaited_once_with(
            self.stream, 1, expected_status="checking_after_exit"
        )

    async def test_archiving_does_not_stop_active_recording(self) -> None:
        self.state.mark_downloading(self.stream, 1)
        active = object()
        self.manager.active[self.stream.video_id] = active  # type: ignore[assignment]

        with patch(
            "onlysavemevods.downloader.asyncio.create_subprocess_exec",
            new=AsyncMock(),
        ) as spawn:
            started = await self.manager.start_stream(self.stream)

        self.assertFalse(started)
        spawn.assert_not_awaited()
        self.assertIs(self.manager.active[self.stream.video_id], active)
        self.assertEqual(self.state.get_stream(self.stream.video_id).status, "downloading")


if __name__ == "__main__":
    unittest.main()
