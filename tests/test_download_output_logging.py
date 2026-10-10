import asyncio
from io import StringIO
import logging
from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock

from onlysavemevods.config import BotConfig
from onlysavemevods.downloader import CatchupTracker, DownloadManager


class DownloadOutputLoggingTests(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.output = StringIO()
        self.logger = logging.Logger("download-output-test", logging.INFO)
        handler = logging.StreamHandler(self.output)
        handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
        self.logger.addHandler(handler)
        self.manager = DownloadManager(
            BotConfig(), MagicMock(), probe=MagicMock(), logger=self.logger
        )
        self.tracker = CatchupTracker(asyncio.Event())

    async def test_unterminated_video_error_survives_info_logging(self) -> None:
        stream = asyncio.StreamReader()
        stream.feed_data(
            b"[download] Destination: segment-001.f299.mp4\n"
            b"[download] (frag 4/100)\nERR"
        )
        stream.feed_data(b"OR: unable to download video data: HTTP Error 403")
        stream.feed_eof()

        await self.manager._monitor_process_output(
            "youtube:LIVEVIDEO01", stream, self.tracker, track_hint="video"
        )

        self.assertEqual(self.tracker.fragments, {"video": (4, 100)})
        self.assertEqual(
            self.output.getvalue(),
            "ERROR yt-dlp youtube:LIVEVIDEO01 video: "
            "ERROR: unable to download video data: HTTP Error 403\n",
        )

    async def test_audio_warning_keeps_track_context(self) -> None:
        self.manager._handle_process_output_line(
            "youtube:LIVEVIDEO01",
            "\x1b[0;33mWARNING:\x1b[0m fragment unavailable; retrying",
            self.tracker,
            track_hint="audio",
        )

        self.assertEqual(
            self.output.getvalue(),
            "WARNING yt-dlp youtube:LIVEVIDEO01 audio: "
            "WARNING: fragment unavailable; retrying\n",
        )

    async def test_error_redacts_signed_urls_and_echoed_credentials(self) -> None:
        self.manager._handle_process_output_line(
            "youtube:LIVEVIDEO01",
            "ERROR: failed https://media.example/videoplayback?sig=secret "
            "using --proxy 'socks5://user:password@proxy.example' "
            "--password=private Authorization: Bearer access-token",
            self.tracker,
            track_hint="video",
        )

        logged = self.output.getvalue()
        self.assertIn("ERROR: failed <redacted URL>", logged)
        self.assertIn("--proxy=<redacted>", logged)
        self.assertIn("--password=<redacted>", logged)
        self.assertIn("Authorization: <redacted>", logged)
        for secret in ("secret", "user:password", "proxy.example", "private", "access-token"):
            self.assertNotIn(secret, logged)

    async def test_sidecar_warning_visible_and_routine_output_quiet(self) -> None:
        self.manager._handle_sidecar_output_line(
            "youtube:LIVEVIDEO01", "chat", "[youtube] Downloading webpage"
        )
        self.manager._handle_sidecar_output_line(
            "youtube:LIVEVIDEO01", "chat", "WARNING: Live chat download failed"
        )

        self.assertEqual(
            self.output.getvalue(),
            "WARNING yt-dlp youtube:LIVEVIDEO01 chat: "
            "WARNING: Live chat download failed\n",
        )
