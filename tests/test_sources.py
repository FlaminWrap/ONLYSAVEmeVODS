from datetime import datetime, timezone
import unittest

from onlysavemevods.sources import (
    SourceError,
    SourceMonitor,
    canonical_source,
    playlist_candidate_urls,
    resolve_source,
)
from onlysavemevods.youtube import YtDlpError


class FakeRunner:
    def __init__(self, responses: dict[str, dict]) -> None:
        self.responses = responses
        self.calls: list[list[str]] = []

    def run_json(self, args: list[str], timeout: int = 120) -> dict:
        self.calls.append(args)
        response = self.responses[args[-1]]
        if isinstance(response, list):
            response = response.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


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
        start_timestamp = datetime(2026, 7, 5, 8, 30, tzinfo=timezone.utc).timestamp()
        expected_start = (
            datetime.fromtimestamp(start_timestamp, tz=timezone.utc)
            .astimezone()
            .strftime("%Y-%m-%d %H:%M")
        )
        runner = FakeRunner(
            {
                "https://www.twitch.tv/OUMB3rd": {
                    "id": "1234567890",
                    "title": "Live on Twitch",
                    "uploader": "OUMB3rd",
                    "webpage_url": "https://www.twitch.tv/OUMB3rd",
                    "live_status": "is_live",
                    "timestamp": start_timestamp,
                }
            }
        )
        monitor = SourceMonitor(runner)

        streams = monitor.discover_live_streams("twitch:OUMB3rd")

        self.assertEqual(len(streams), 1)
        self.assertEqual(streams[0].platform, "twitch")
        self.assertEqual(streams[0].source, "twitch:OUMB3rd")
        self.assertEqual(streams[0].video_id, f"twitch:Live on Twitch {expected_start}")

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

    def test_kick_live_source_uses_stable_livestream_id(self) -> None:
        start_timestamp = datetime(2026, 7, 5, 5, 18, tzinfo=timezone.utc).timestamp()
        runner = FakeRunner(
            {
                "https://kick.com/OUMB3rd": {
                    "id": "92722911-hungover-4th-of-july",
                    "title": "Hungover 4th of July $3 tts no toxicity 2026-07-05 06:18",
                    "uploader": "OUMB3rd",
                    "webpage_url": "https://kick.com/OUMB3rd",
                    "live_status": "is_live",
                    "timestamp": start_timestamp,
                    "release_timestamp": start_timestamp - 60,
                }
            }
        )
        monitor = SourceMonitor(runner)

        streams = monitor.discover_live_streams("kick:OUMB3rd")

        self.assertEqual(len(streams), 1)
        self.assertEqual(streams[0].platform, "kick")
        self.assertEqual(streams[0].source, "kick:OUMB3rd")
        self.assertEqual(streams[0].video_id, "kick:92722911-hungover-4th-of-july")
        self.assertEqual(streams[0].title, "Hungover 4th of July $3 tts no toxicity")

    def test_kick_repeated_live_probes_keep_the_same_stream_id(self) -> None:
        start_timestamp = datetime(2026, 7, 15, 1, 42, tzinfo=timezone.utc).timestamp()
        base_info = {
            "id": "92722911-black-ops-ports-hotel-internet",
            "uploader": "oumb",
            "webpage_url": "https://kick.com/oumb",
            "live_status": "is_live",
            "release_timestamp": start_timestamp,
        }
        runner = FakeRunner(
            {
                "https://kick.com/oumb": [
                    {
                        **base_info,
                        "title": "Black ops ports hotel internet 2026-07-15 02:42",
                        "timestamp": start_timestamp + 60,
                    },
                    {
                        **base_info,
                        "title": "Black ops ports hotel internet 2026-07-15 02:43",
                        "timestamp": start_timestamp + 120,
                    },
                ]
            }
        )
        monitor = SourceMonitor(runner)

        first = monitor.discover_live_streams("kick:oumb")
        second = monitor.discover_live_streams("kick:oumb")

        self.assertEqual(first[0].video_id, second[0].video_id)
        self.assertEqual(
            first[0].video_id,
            "kick:92722911-black-ops-ports-hotel-internet",
        )
        self.assertEqual(first[0].title, "Black ops ports hotel internet")
        self.assertEqual(second[0].title, "Black ops ports hotel internet")

    def test_kick_channel_id_fallback_uses_release_time(self) -> None:
        release_timestamp = datetime(2026, 7, 15, 1, 42, tzinfo=timezone.utc).timestamp()
        expected_start = (
            datetime.fromtimestamp(release_timestamp, tz=timezone.utc)
            .astimezone()
            .strftime("%Y-%m-%d %H:%M")
        )
        runner = FakeRunner(
            {
                "https://kick.com/oumb": {
                    "id": "oumb",
                    "title": "Black ops ports hotel internet 2026-07-15 02:43",
                    "uploader": "oumb",
                    "webpage_url": "https://kick.com/oumb",
                    "live_status": "is_live",
                    "timestamp": release_timestamp + 60,
                    "release_timestamp": release_timestamp,
                }
            }
        )
        monitor = SourceMonitor(runner)

        streams = monitor.discover_live_streams("kick:oumb")

        self.assertEqual(
            streams[0].video_id,
            f"kick:Black ops ports hotel internet {expected_start}",
        )

    def test_rumble_live_url_produces_platform_stream(self) -> None:
        start_timestamp = datetime(2026, 7, 5, 10, 45, tzinfo=timezone.utc).timestamp()
        expected_start = (
            datetime.fromtimestamp(start_timestamp, tz=timezone.utc)
            .astimezone()
            .strftime("%Y-%m-%d %H:%M")
        )
        runner = FakeRunner(
            {
                "https://rumble.com/vabc-title.html": {
                    "id": "vabc",
                    "title": "Live on Rumble",
                    "uploader": "OUMB3rd",
                    "webpage_url": "https://rumble.com/vabc-title.html",
                    "is_live": True,
                    "timestamp": start_timestamp,
                }
            }
        )
        monitor = SourceMonitor(runner)

        streams = monitor.discover_live_streams("https://rumble.com/vabc-title.html")

        self.assertEqual(len(streams), 1)
        self.assertEqual(streams[0].platform, "rumble")
        self.assertEqual(streams[0].video_id, f"rumble:Live on Rumble {expected_start}")

    def test_empty_non_youtube_probe_output_returns_no_streams(self) -> None:
        runner = FakeRunner(
            {
                "https://rumble.com/user/OUMB2": YtDlpError("yt-dlp returned no JSON output"),
            }
        )
        monitor = SourceMonitor(runner)

        self.assertEqual(monitor.discover_live_streams("rumble:user/OUMB2"), [])
        self.assertTrue(any("--dump-single-json" in call for call in runner.calls))

    def test_rumble_user_playlist_fallback_finds_live_video(self) -> None:
        start_timestamp = datetime(2026, 7, 5, 10, 45, tzinfo=timezone.utc).timestamp()
        expected_start = (
            datetime.fromtimestamp(start_timestamp, tz=timezone.utc)
            .astimezone()
            .strftime("%Y-%m-%d %H:%M")
        )
        runner = FakeRunner(
            {
                "https://rumble.com/user/OUMB2": [
                    YtDlpError("yt-dlp returned no JSON output"),
                    {
                        "entries": [
                            {"webpage_url": "https://rumble.com/vabc-title.html"},
                        ]
                    },
                ],
                "https://rumble.com/vabc-title.html": {
                    "id": "vabc",
                    "title": "Live on Rumble",
                    "uploader": "OUMB2",
                    "webpage_url": "https://rumble.com/vabc-title.html",
                    "is_live": True,
                    "timestamp": start_timestamp,
                },
            }
        )
        monitor = SourceMonitor(runner)

        streams = monitor.discover_live_streams("rumble:user/OUMB2")

        self.assertEqual(len(streams), 1)
        self.assertEqual(streams[0].video_id, f"rumble:Live on Rumble {expected_start}")
        self.assertEqual(streams[0].source, "rumble:user/OUMB2")

    def test_rumble_playlist_candidates_resolve_at_site_root(self) -> None:
        self.assertEqual(
            playlist_candidate_urls(
                {"entries": [{"url": "vabc-title.html"}]},
                "https://rumble.com/user/OUMB2",
            ),
            ["https://rumble.com/vabc-title.html"],
        )


if __name__ == "__main__":
    unittest.main()
