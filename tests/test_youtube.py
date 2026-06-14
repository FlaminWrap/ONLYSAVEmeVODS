import unittest
from subprocess import CompletedProcess
from unittest.mock import patch

from onlysavemevods.youtube import (
    TerminalVideoUnavailableError,
    YoutubeProbe,
    YtDlpRunner,
    channel_live_url,
    channel_streams_url,
    is_terminal_video_unavailable_message,
    live_stream_from_info,
)


class FakeRunner:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def run_json(self, args: list[str], timeout: int = 120) -> dict:
        self.calls.append(args)
        if "--dump-single-json" in args:
            return {
                "entries": [
                    {"id": "LIVEVIDEO01"},
                    {"url": "https://www.youtube.com/watch?v=LIVEVIDEO02"},
                    {"id": "ENDEDVIDEO1"},
                ]
            }

        target = args[-1]
        if target.endswith("/live"):
            return {
                "id": "LIVEVIDEO01",
                "title": "Fast live",
                "channel": "Example",
                "webpage_url": "https://www.youtube.com/watch?v=LIVEVIDEO01",
                "live_status": "is_live",
            }

        video_id = target.rsplit("=", 1)[-1]
        if video_id.startswith("LIVE"):
            return {
                "id": video_id,
                "title": f"Stream {video_id}",
                "channel": "Example",
                "webpage_url": target,
                "live_status": "is_live",
            }
        return {
            "id": video_id,
            "title": "Old stream",
            "webpage_url": target,
            "live_status": "was_live",
        }


class CacheRunner:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def run_json(self, args: list[str], timeout: int = 120) -> dict:
        self.calls.append(args)
        if "--dump-single-json" in args:
            return {"entries": [{"id": "LIVEVIDEO01"}, {"id": "ENDEDVIDEO1"}]}

        target = args[-1]
        video_id = target.rsplit("=", 1)[-1]
        if video_id == "LIVEVIDEO01":
            return {
                "id": video_id,
                "webpage_url": target,
                "live_status": "is_live",
            }
        return {
            "id": video_id,
            "webpage_url": target,
            "live_status": "was_live",
        }


class YoutubeProbeTests(unittest.TestCase):
    def test_channel_url_normalization(self) -> None:
        self.assertEqual(
            channel_streams_url("@Example"),
            "https://www.youtube.com/@Example/streams",
        )
        self.assertEqual(
            channel_streams_url("https://www.youtube.com/@Example/videos"),
            "https://www.youtube.com/@Example/streams",
        )
        self.assertEqual(
            channel_live_url("https://www.youtube.com/@Example/streams"),
            "https://www.youtube.com/@Example/live",
        )

    def test_live_stream_from_info(self) -> None:
        stream = live_stream_from_info(
            {
                "id": "LIVEVIDEO01",
                "title": "Live now",
                "uploader": "Uploader",
                "live_status": "is_live",
            }
        )

        self.assertTrue(stream.is_live)
        self.assertEqual(stream.video_id, "youtube:LIVEVIDEO01")
        self.assertEqual(stream.platform, "youtube")
        self.assertEqual(stream.channel, "Uploader")

    def test_discovers_multiple_live_streams(self) -> None:
        probe = YoutubeProbe(FakeRunner(), channel_scan_limit=10)

        streams = probe.discover_channel_live_streams("@Example")

        self.assertEqual(
            [stream.video_id for stream in streams],
            ["youtube:LIVEVIDEO01", "youtube:LIVEVIDEO02"],
        )

    def test_probe_channel_live_stream_uses_live_url_fast_path(self) -> None:
        runner = FakeRunner()
        probe = YoutubeProbe(runner)

        stream = probe.probe_channel_live_stream("@Example")

        self.assertIsNotNone(stream)
        assert stream is not None
        self.assertEqual(stream.video_id, "youtube:LIVEVIDEO01")
        self.assertEqual(runner.calls[0][-1], "https://www.youtube.com/@Example/live")

    def test_ended_streams_are_not_rechecked_on_later_scans(self) -> None:
        runner = CacheRunner()
        probe = YoutubeProbe(
            runner,
            channel_scan_limit=10,
            discovery_probe_concurrency=1,
        )

        probe.discover_channel_live_streams("@Example", include_channel_live=False)
        probe.discover_channel_live_streams("@Example", include_channel_live=False)

        ended_probes = [
            call for call in runner.calls if call[-1].endswith("v=ENDEDVIDEO1")
        ]
        self.assertEqual(len(ended_probes), 1)

    def test_private_or_deleted_errors_are_terminal(self) -> None:
        self.assertTrue(
            is_terminal_video_unavailable_message(
                "ERROR: [youtube] LIVEVIDEO01: Private video"
            )
        )
        self.assertTrue(
            is_terminal_video_unavailable_message(
                "ERROR: [youtube] LIVEVIDEO01: Video unavailable. "
                "This video has been removed by the uploader"
            )
        )
        self.assertFalse(
            is_terminal_video_unavailable_message(
                "ERROR: [youtube] LIVEVIDEO01: HTTP Error 503: Service Unavailable"
            )
        )

    def test_runner_raises_terminal_error_for_private_video(self) -> None:
        completed = CompletedProcess(
            args=["yt-dlp"],
            returncode=1,
            stdout="",
            stderr="ERROR: [youtube] LIVEVIDEO01: Private video",
        )

        with patch("subprocess.run", return_value=completed):
            with self.assertRaises(TerminalVideoUnavailableError):
                YtDlpRunner().run_json(["--dump-json", "https://example.test"])

    def test_runner_raises_terminal_error_for_removed_video(self) -> None:
        completed = CompletedProcess(
            args=["yt-dlp"],
            returncode=1,
            stdout="",
            stderr=(
                "ERROR: [youtube] LIVEVIDEO01: Video unavailable. "
                "This video has been removed by the uploader"
            ),
        )

        with patch("subprocess.run", return_value=completed):
            with self.assertRaises(TerminalVideoUnavailableError):
                YtDlpRunner().run_json(["--dump-json", "https://example.test"])
