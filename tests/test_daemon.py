from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, patch
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


if __name__ == "__main__":
    unittest.main()
