import unittest

from onlysavemevods.sources import SourceError, SourceMonitor, canonical_source, resolve_source


class FakeRunner:
    def __init__(self, responses: dict[str, dict]) -> None:
        self.responses = responses
        self.calls: list[list[str]] = []

    def run_json(self, args: list[str], timeout: int = 120) -> dict:
        self.calls.append(args)
        return self.responses[args[-1]]


class SourceResolutionTests(unittest.TestCase):
    def test_resolves_existing_youtube_sources(self) -> None:
        self.assertEqual(resolve_source("@Example").platform, "youtube")
        self.assertEqual(resolve_source("Example").platform, "youtube")
        self.assertEqual(
            resolve_source("https://www.youtube.com/@Example").platform,
            "youtube",
        )

    def test_resolves_supported_prefixes(self) -> None:
        self.assertEqual(resolve_source("twitch:OUMB3rd").url, "https://www.twitch.tv/OUMB3rd")
        self.assertEqual(resolve_source("kick:OUMB3rd").url, "https://kick.com/OUMB3rd")
        self.assertEqual(
            resolve_source("rumble:user/OUMB3rd").url,
            "https://rumble.com/user/OUMB3rd",
        )

    def test_resolves_supported_urls(self) -> None:
        self.assertEqual(resolve_source("https://www.twitch.tv/OUMB3rd").platform, "twitch")
        self.assertEqual(resolve_source("https://kick.com/OUMB3rd").platform, "kick")
        self.assertEqual(resolve_source("https://rumble.com/vabc-title.html").platform, "rumble")

    def test_canonicalizes_supported_url_sources(self) -> None:
        self.assertEqual(canonical_source("https://kick.com/oumb"), "kick:oumb")
        self.assertEqual(canonical_source("https://rumble.com/user/OUMB2"), "rumble:user/OUMB2")
        self.assertEqual(canonical_source("https://www.twitch.tv/OUMB3rd"), "twitch:OUMB3rd")
        self.assertEqual(canonical_source("https://www.youtube.com/@Example"), "@Example")

    def test_rejects_unsupported_prefix_or_url(self) -> None:
        with self.assertRaises(SourceError):
            resolve_source("trovo:someone")
        with self.assertRaises(SourceError):
            resolve_source("https://example.com/someone")


class SourceMonitorTests(unittest.TestCase):
    def test_twitch_live_source_produces_platform_stream(self) -> None:
        runner = FakeRunner(
            {
                "https://www.twitch.tv/OUMB3rd": {
                    "id": "1234567890",
                    "title": "Live on Twitch",
                    "uploader": "OUMB3rd",
                    "webpage_url": "https://www.twitch.tv/OUMB3rd",
                    "live_status": "is_live",
                }
            }
        )
        monitor = SourceMonitor(runner)

        streams = monitor.discover_live_streams("twitch:OUMB3rd")

        self.assertEqual(len(streams), 1)
        self.assertEqual(streams[0].platform, "twitch")
        self.assertEqual(streams[0].source, "twitch:OUMB3rd")
        self.assertEqual(streams[0].video_id, "twitch:OUMB3rd")

    def test_kick_offline_source_returns_no_streams(self) -> None:
        runner = FakeRunner(
            {
                "https://kick.com/OUMB3rd": {
                    "id": "OUMB3rd",
                    "title": "Offline",
                    "uploader": "OUMB3rd",
                    "webpage_url": "https://kick.com/OUMB3rd",
                    "live_status": "not_live",
                }
            }
        )
        monitor = SourceMonitor(runner)

        self.assertEqual(monitor.discover_live_streams("kick:OUMB3rd"), [])

    def test_rumble_live_url_produces_platform_stream(self) -> None:
        runner = FakeRunner(
            {
                "https://rumble.com/vabc-title.html": {
                    "id": "vabc",
                    "title": "Live on Rumble",
                    "uploader": "OUMB3rd",
                    "webpage_url": "https://rumble.com/vabc-title.html",
                    "is_live": True,
                }
            }
        )
        monitor = SourceMonitor(runner)

        streams = monitor.discover_live_streams("https://rumble.com/vabc-title.html")

        self.assertEqual(len(streams), 1)
        self.assertEqual(streams[0].platform, "rumble")
        self.assertEqual(streams[0].video_id, "rumble:vabc")


if __name__ == "__main__":
    unittest.main()
