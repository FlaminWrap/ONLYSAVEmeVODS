import asyncio
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import AsyncMock, Mock, patch

from onlysavemevods.config import BotConfig, StreamerConfig
from onlysavemevods.daemon import OnlySaveMeVodsDaemon
from onlysavemevods.models import LiveStream


class StreamerArchiveDaemonTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        self.config = BotConfig(
            streamers={"Example": StreamerConfig(sources=["@Example"])},
            download_dir=root / "downloads",
            state_dir=root / "state",
            web_enabled=False,
        )
        self.daemon = OnlySaveMeVodsDaemon(self.config)
        self.addCleanup(self.daemon.state.close)
        self.daemon.sources.discover_live_streams = Mock(return_value=[])  # type: ignore[method-assign]
        self.daemon.downloads.start_stream = AsyncMock(return_value=True)  # type: ignore[method-assign]
        self.daemon.downloads.resume_pending_post_processing_jobs = Mock()  # type: ignore[method-assign]

        async def inline_to_thread(func, /, *args, **kwargs):
            return func(*args, **kwargs)

        worker = patch("onlysavemevods.daemon.asyncio.to_thread", inline_to_thread)
        worker.start()
        self.addCleanup(worker.stop)

    def archive(self, name: str = "Example") -> None:
        self.config.streamers[name] = replace(
            self.config.streamers[name], archived=True
        )

    @staticmethod
    def stream(video_id: str = "youtube:LIVEVIDEO01") -> LiveStream:
        return LiveStream(
            video_id=video_id,
            url="https://www.youtube.com/watch?v=" + video_id.split(":")[-1],
            channel="Example",
            platform="youtube",
            source="@Example",
            is_live=True,
        )

    async def test_archived_sources_are_not_probed_and_active_work_is_preserved(self) -> None:
        self.archive()
        stream = self.stream()
        self.daemon.state.mark_downloading(stream, 1)
        active = object()
        self.daemon.downloads.active[stream.video_id] = active  # type: ignore[assignment]

        await self.daemon.poll_once()

        self.daemon.sources.discover_live_streams.assert_not_called()
        self.daemon.downloads.start_stream.assert_not_awaited()
        self.assertIs(self.daemon.downloads.active[stream.video_id], active)
        record = self.daemon.state.get_stream(stream.video_id)
        self.assertEqual(record.status, "downloading")
        self.daemon.downloads.resume_pending_post_processing_jobs.assert_called_once()

    async def test_archiving_during_probe_discards_its_results(self) -> None:
        stream = self.stream()
        self.daemon.sources.discover_live_streams.return_value = [stream]
        probe_started = asyncio.Event()
        release_probe = asyncio.Event()

        async def controlled_to_thread(func, /, *args, **kwargs):
            result = func(*args, **kwargs)
            probe_started.set()
            await release_probe.wait()
            return result

        with patch("onlysavemevods.daemon.asyncio.to_thread", controlled_to_thread):
            poll = asyncio.create_task(self.daemon.poll_once())
            try:
                await asyncio.wait_for(probe_started.wait(), timeout=2)
                self.archive()
            finally:
                release_probe.set()
                await poll

        self.daemon.sources.discover_live_streams.assert_called_once_with("@Example")
        self.daemon.downloads.start_stream.assert_not_awaited()
        self.assertIsNone(self.daemon.state.get_stream(stream.video_id))

    async def test_archiving_later_source_skips_stale_poll_list_entry(self) -> None:
        self.config.streamers["Later"] = StreamerConfig(sources=["@Later"])
        stream = self.stream()

        def discover(source: str) -> list[LiveStream]:
            self.archive("Later")
            return [stream]

        self.daemon.sources.discover_live_streams.side_effect = discover

        await self.daemon.poll_once()

        self.daemon.sources.discover_live_streams.assert_called_once_with("@Example")
        self.daemon.downloads.start_stream.assert_awaited_once_with(stream)

    async def test_archiving_between_stream_starts_skips_remaining_results(self) -> None:
        first = self.stream("youtube:FIRSTVIDEO1")
        second = self.stream("youtube:SECONDVID01")
        self.daemon.sources.discover_live_streams.return_value = [first, second]

        async def start_and_archive(stream: LiveStream) -> bool:
            self.archive()
            return True

        self.daemon.downloads.start_stream.side_effect = start_and_archive

        await self.daemon.poll_once()

        self.daemon.downloads.start_stream.assert_awaited_once_with(first)
        self.assertIsNone(self.daemon.state.get_stream(second.video_id))

    async def test_restoring_streamer_resumes_source_discovery(self) -> None:
        self.archive()
        stream = self.stream()
        self.daemon.sources.discover_live_streams.return_value = [stream]

        await self.daemon.poll_once()
        self.config.streamers["Example"] = replace(
            self.config.streamers["Example"], archived=False
        )
        await self.daemon.poll_once()

        self.daemon.sources.discover_live_streams.assert_called_once_with("@Example")
        self.daemon.downloads.start_stream.assert_awaited_once_with(stream)

    async def test_archived_failed_finalization_still_resumes(self) -> None:
        stream = self.stream()
        self.daemon.state.mark_downloading(stream, 1)
        self.daemon.state.mark_exited(stream.video_id, 1)
        self.daemon.state.mark_finalization_failed(stream.video_id)
        self.archive()
        self.daemon.downloads.resume_finalization_retry = Mock()  # type: ignore[method-assign]

        await self.daemon.poll_once()

        retry = self.daemon.downloads.resume_finalization_retry
        retry.assert_called_once()
        self.assertEqual(retry.call_args.args[0].video_id, stream.video_id)
        self.assertEqual(retry.call_args.args[1], 1)
        self.daemon.sources.discover_live_streams.assert_not_called()


if __name__ == "__main__":
    unittest.main()
