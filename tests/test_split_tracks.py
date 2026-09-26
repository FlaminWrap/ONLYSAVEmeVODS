import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

from onlysavemevods.config import BotConfig
from onlysavemevods.downloader import DownloadManager
from onlysavemevods.models import LiveStream, video_url
from onlysavemevods.state import StateStore


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


class SplitTrackLifecycleTests(IsolatedAsyncioTestCase):
    async def test_audio_continues_when_video_exits_before_end_confirmation(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = BotConfig(
                download_dir=root / "downloads",
                state_dir=root / "state",
                youtube_stale_live_timeout_seconds=0,
            )
            state = StateStore(config.db_path)
            stream = LiveStream(
                video_id="youtube:LIVEVIDEO01",
                url=video_url("LIVEVIDEO01"),
                platform="youtube",
                channel="Example",
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
            manager = DownloadManager(config, state, probe=None)  # type: ignore[arg-type]
            video_process = FakeProcess()
            audio_process = FakeProcess()
            with patch(
                "onlysavemevods.downloader.asyncio.create_subprocess_exec",
                new=AsyncMock(side_effect=[video_process, audio_process]),
            ) as spawn:
                try:
                    self.assertTrue(await manager.start_stream(stream))
                    for _ in range(10):
                        if spawn.await_count == 2:
                            break
                        await asyncio.sleep(0)
                    self.assertEqual(spawn.await_count, 2)
                    video_process.terminate()
                    for _ in range(20):
                        if stream.video_id in manager._draining_audio:
                            break
                        await asyncio.sleep(0)
                    self.assertIn(stream.video_id, manager._draining_audio)
                    self.assertIsNone(audio_process.returncode)
                    self.assertNotIn(stream.video_id, manager.active)
                    await manager.finish_ended_stream(
                        stream,
                        1,
                        expected_status="checking_after_exit",
                        end_confirmed=True,
                    )
                    self.assertTrue(
                        manager._draining_audio[stream.video_id].audio_end_confirmed
                    )
                    self.assertIsNone(audio_process.returncode)
                    self.assertEqual(
                        state.get_stream(stream.video_id).status,
                        "checking_after_exit",
                    )
                finally:
                    await manager.stop_all()
                    state.close()
            self.assertEqual(audio_process.returncode, -15)
