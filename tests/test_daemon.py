from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, Mock, patch
import unittest

from onlysavemevods.config import BotConfig
from onlysavemevods.daemon import OnlySaveMeVodsDaemon
from onlysavemevods.models import LiveStream


class FakeSourceMonitor:
    def __init__(self, stream: LiveStream) -> None:
        self.stream = stream
        self.checked: list[str] = []

    def discover_live_streams(self, source: str) -> list[LiveStream]:
        self.checked.append(source)
        return [self.stream]

    def probe_video(self, url: str) -> LiveStream:
        return self.stream


class DaemonTests(unittest.IsolatedAsyncioTestCase):
    async def test_resume_stale_post_exit_checks_queues_recovery(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                web_enabled=False,
            )
            stream = LiveStream(
                video_id="youtube:LIVEVIDEO01",
                url="https://www.youtube.com/watch?v=LIVEVIDEO01",
                title="Interrupted Post Exit",
                channel="Example Channel",
                platform="youtube",
                source="@Example",
            )
            daemon = OnlySaveMeVodsDaemon(config)
            daemon.state.upsert_detected(stream)
            daemon.state.mark_exited(stream.video_id, -15)
            records = daemon.state.list_streams_by_status(["checking_after_exit"])
            daemon.downloads.resume_post_exit_check = Mock()  # type: ignore[method-assign]

            try:
                daemon.resume_stale_post_exit_checks(records)
            finally:
                daemon.state.close()

        daemon.downloads.resume_post_exit_check.assert_called_once()
        queued_stream = daemon.downloads.resume_post_exit_check.call_args.args[0]
        queued_segment = daemon.downloads.resume_post_exit_check.call_args.args[1]
        elapsed = daemon.downloads.resume_post_exit_check.call_args.kwargs[
            "elapsed_since_exit_seconds"
        ]
        self.assertEqual(queued_stream.video_id, stream.video_id)
        self.assertEqual(queued_stream.url, stream.url)
        self.assertEqual(queued_stream.channel, stream.channel)
        self.assertEqual(queued_segment, 1)
        self.assertGreaterEqual(elapsed, 0)

    async def test_poll_once_uses_source_monitor_registry(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                channels=["twitch:OUMB3rd"],
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                web_enabled=False,
            )
            stream = LiveStream(
                video_id="twitch:OUMB3rd",
                url="https://www.twitch.tv/OUMB3rd",
                title="Live on Twitch",
                channel="OUMB3rd",
                platform="twitch",
                source="twitch:OUMB3rd",
            )
            daemon = OnlySaveMeVodsDaemon(config)
            fake_sources = FakeSourceMonitor(stream)
            daemon.sources = fake_sources  # type: ignore[assignment]
            daemon.downloads.start_stream = AsyncMock(return_value=True)  # type: ignore[method-assign]

            async def inline_to_thread(func, /, *args, **kwargs):
                return func(*args, **kwargs)

            try:
                with patch("onlysavemevods.daemon.asyncio.to_thread", inline_to_thread):
                    await daemon.poll_once()
            finally:
                daemon.state.close()

        self.assertEqual(fake_sources.checked, ["twitch:OUMB3rd"])
        daemon.downloads.start_stream.assert_awaited_once_with(stream)

    async def test_poll_once_runs_enabled_fragment_cleanup(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                fragment_retention_hours=24,
                web_enabled=False,
            )
            daemon = OnlySaveMeVodsDaemon(config)
            cleanup = Mock(return_value=(1, 2, 1024))

            async def inline_to_thread(func, /, *args, **kwargs):
                return func(*args, **kwargs)

            try:
                with (
                    patch("onlysavemevods.daemon.asyncio.to_thread", inline_to_thread),
                    patch("onlysavemevods.daemon.cleanup_expired_stream_fragments", cleanup),
                ):
                    await daemon.poll_once()
            finally:
                daemon.state.close()

        cleanup.assert_called_once_with(config)


if __name__ == "__main__":
    unittest.main()
