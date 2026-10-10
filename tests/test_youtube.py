from datetime import datetime, timezone
from io import BytesIO
import json
import unittest
from subprocess import CompletedProcess
from unittest.mock import MagicMock, patch

from onlysavemevods.youtube import (
    ConfirmedLiveEndError,
    ConfirmedLiveTerminationError,
    ConfirmedVideoRemovalError,
    TerminalVideoUnavailableError,
    YoutubeProbe,
    YtDlpError,
    YtDlpRunner,
    WATCH_PAGE_READ_LIMIT,
    WATCH_PAGE_TIMEOUT_SECONDS,
    channel_live_url,
    channel_streams_url,
    is_confirmed_live_termination_message,
    is_confirmed_video_removal_message,
    is_terminal_video_unavailable_message,
    live_stream_from_info,
    parse_youtube_hls_live_edge,
    watch_page_confirmed_live_end,
    watch_page_video_removal_reason,
    youtube_hls_media_manifest,
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
    @staticmethod
    def removal_page(reason: str = "This video has been removed by the uploader") -> str:
        return "var ytInitialPlayerResponse = " + json.dumps(
            {
                "playabilityStatus": {
                    "status": "ERROR",
                    "reason": "Video unavailable",
                    "errorScreen": {
                        "playerInterstitialRenderer": {
                            "content": {
                                "interstitialViewModel": {
                                    "title": {"content": "Video unavailable"},
                                    "description": {"content": reason},
                                }
                            }
                        }
                    },
                }
            }
        ) + ";"

    @staticmethod
    def unavailable_runner(video_id: str = "r2ORTHCeg_A") -> tuple[MagicMock, YtDlpError]:
        error = YtDlpError(
            f"yt-dlp failed with code 1: ERROR: [youtube] {video_id}: Video unavailable"
        )
        runner = MagicMock()
        runner.run_json.side_effect = error
        return runner, error

    @staticmethod
    def watch_response(page: str, video_id: str = "r2ORTHCeg_A") -> MagicMock:
        response = MagicMock()
        response.geturl.return_value = f"https://www.youtube.com/watch?v={video_id}&hl=en"
        response.read.side_effect = BytesIO(page.encode()).read
        response.__enter__.return_value = response
        return response

    @staticmethod
    def ended_broadcast_player(
        video_id: str = "wsY_4jPsH6Y",
        status: str = "UNPLAYABLE",
    ) -> dict:
        start = "2026-10-07T09:48:00+00:00"
        end = "2026-10-07T19:32:34+00:00"
        if video_id == "bHgxdHvO4fQ":
            start = "2026-10-04T23:15:59+00:00"
            end = "2026-10-05T01:23:36+00:00"
        return {
            "playabilityStatus": {"status": status, "reason": "Video unavailable"},
            "videoDetails": {
                "videoId": video_id,
                "isLiveContent": True,
                "isLowLatencyLiveStream": True,
            },
            "microformat": {
                "playerMicroformatRenderer": {
                    "externalVideoId": video_id,
                    "liveBroadcastDetails": {
                        "isLiveNow": False,
                        "startTimestamp": start,
                        "endTimestamp": end,
                    },
                }
            },
        }

    @staticmethod
    def player_page(player: object) -> str:
        return "var ytInitialPlayerResponse = " + json.dumps(player) + ";"

    def test_watch_page_confirms_explicit_end_of_restricted_broadcasts(self) -> None:
        now = datetime(2026, 10, 8, tzinfo=timezone.utc)
        for video_id, status in (
            ("wsY_4jPsH6Y", "UNPLAYABLE"),
            ("bHgxdHvO4fQ", "LOGIN_REQUIRED"),
        ):
            with self.subTest(video_id=video_id, status=status):
                page = self.player_page(self.ended_broadcast_player(video_id, status))
                self.assertEqual(
                    watch_page_confirmed_live_end(page, video_id, now=now),
                    datetime(2026, 10, 7, 19, 32, 34, tzinfo=timezone.utc)
                    if video_id == "wsY_4jPsH6Y"
                    else datetime(2026, 10, 5, 1, 23, 36, tzinfo=timezone.utc),
                )

    def test_watch_page_end_requires_exact_video_identity(self) -> None:
        for mismatch in ("missing_details", "missing_id", "wrong_id", "wrong_external_id"):
            with self.subTest(mismatch=mismatch):
                player = self.ended_broadcast_player()
                if mismatch == "missing_details":
                    del player["videoDetails"]
                elif mismatch == "missing_id":
                    del player["videoDetails"]["videoId"]
                elif mismatch == "wrong_id":
                    player["videoDetails"]["videoId"] = "bHgxdHvO4fQ"
                else:
                    player["microformat"]["playerMicroformatRenderer"]["externalVideoId"] = "bHgxdHvO4fQ"
                self.assertIsNone(
                    watch_page_confirmed_live_end(
                        self.player_page(player), "wsY_4jPsH6Y",
                        now=datetime(2026, 10, 8, tzinfo=timezone.utc),
                    )
                )

    def test_watch_page_end_requires_boolean_false_live_now(self) -> None:
        for value in (None, True, "false", 0, "missing"):
            with self.subTest(value=value):
                player = self.ended_broadcast_player()
                broadcast = player["microformat"]["playerMicroformatRenderer"]["liveBroadcastDetails"]
                if value == "missing":
                    del broadcast["isLiveNow"]
                else:
                    broadcast["isLiveNow"] = value
                self.assertIsNone(
                    watch_page_confirmed_live_end(
                        self.player_page(player), "wsY_4jPsH6Y",
                        now=datetime(2026, 10, 8, tzinfo=timezone.utc),
                    )
                )

    def test_watch_page_end_requires_valid_past_timestamp(self) -> None:
        for value in (
            None, True, 1791390000, "bad date", "2026-10-07", "2026-10-07T19:32:34",
            "2026-10-09T00:00:00Z", "2026-10-08T00:00:00Z",
            "9999-12-31T23:59:59-23:59", "0001-01-01T00:00:00+23:59", "missing",
        ):
            with self.subTest(value=value):
                player = self.ended_broadcast_player()
                broadcast = player["microformat"]["playerMicroformatRenderer"]["liveBroadcastDetails"]
                if value == "missing":
                    del broadcast["endTimestamp"]
                else:
                    broadcast["endTimestamp"] = value
                self.assertIsNone(
                    watch_page_confirmed_live_end(
                        self.player_page(player), "wsY_4jPsH6Y",
                        now=datetime(2026, 10, 8, tzinfo=timezone.utc),
                    )
                )

    def test_watch_page_end_rejects_contradictory_live_flags(self) -> None:
        for location in ("player", "details", "renderer", "broadcast"):
            for flag in ("isLive", "isLiveNow", "isUpcoming"):
                with self.subTest(location=location, flag=flag):
                    player = self.ended_broadcast_player()
                    renderer = player["microformat"]["playerMicroformatRenderer"]
                    target = {
                        "player": player,
                        "details": player["videoDetails"],
                        "renderer": renderer,
                        "broadcast": renderer["liveBroadcastDetails"],
                    }[location]
                    target[flag] = True
                    self.assertIsNone(
                        watch_page_confirmed_live_end(
                            self.player_page(player), "wsY_4jPsH6Y",
                            now=datetime(2026, 10, 8, tzinfo=timezone.utc),
                        )
                    )

    def test_watch_page_end_rejects_contradictory_start_timestamp(self) -> None:
        for value in (None, "bad date", "2026-10-08T00:00:00Z"):
            with self.subTest(value=value):
                player = self.ended_broadcast_player()
                player["microformat"]["playerMicroformatRenderer"]["liveBroadcastDetails"]["startTimestamp"] = value
                self.assertIsNone(
                    watch_page_confirmed_live_end(
                        self.player_page(player), "wsY_4jPsH6Y",
                        now=datetime(2026, 10, 8, tzinfo=timezone.utc),
                    )
                )

    def test_watch_page_does_not_infer_end_from_duration_or_live_content(self) -> None:
        player = self.ended_broadcast_player()
        del player["microformat"]
        player["videoDetails"]["lengthSeconds"] = "5000"
        self.assertIsNone(watch_page_confirmed_live_end(self.player_page(player), "wsY_4jPsH6Y"))
        for page in ("", 'var ytInitialPlayerResponse = {"videoDetails":', self.player_page(None)):
            with self.subTest(page=page):
                self.assertIsNone(watch_page_confirmed_live_end(page, "wsY_4jPsH6Y"))

    def test_unavailable_and_age_restricted_metadata_confirm_watch_page_end(self) -> None:
        for video_id, message, status in (
            ("wsY_4jPsH6Y", "Video unavailable", "UNPLAYABLE"),
            ("bHgxdHvO4fQ", "Sign in to confirm your age. This video may be inappropriate for some users.", "LOGIN_REQUIRED"),
        ):
            for method in ("probe_video", "probe_live_edge"):
                with self.subTest(video_id=video_id, method=method):
                    original = YtDlpError(f"yt-dlp failed with code 1: ERROR: [youtube] {video_id}: {message}")
                    runner = MagicMock()
                    runner.run_json.side_effect = original
                    player = self.ended_broadcast_player(video_id, status)
                    # A past timestamp keeps this integration test independent of wall-clock time.
                    player["microformat"]["playerMicroformatRenderer"]["liveBroadcastDetails"] = {
                        "isLiveNow": False, "endTimestamp": "2000-01-01T00:00:00Z",
                    }
                    response = self.watch_response(self.player_page(player), video_id)
                    with patch("onlysavemevods.youtube.urlopen", return_value=response) as opened:
                        with self.assertRaises(ConfirmedLiveEndError) as caught:
                            getattr(YoutubeProbe(runner), method)(video_id)
                    self.assertIs(caught.exception.__cause__, original)
                    opened.assert_called_once()
                    self.assertEqual(opened.call_args.kwargs["timeout"], WATCH_PAGE_TIMEOUT_SECONDS)
                    response.read.assert_called_once_with(WATCH_PAGE_READ_LIMIT + 1)

    def test_age_restriction_alone_preserves_original_probe_error(self) -> None:
        video_id = "bHgxdHvO4fQ"
        original = YtDlpError(f"ERROR: [youtube] {video_id}: Sign in to confirm your age.")
        runner = MagicMock()
        runner.run_json.side_effect = original
        player = self.ended_broadcast_player(video_id, "LOGIN_REQUIRED")
        del player["microformat"]["playerMicroformatRenderer"]["liveBroadcastDetails"]["endTimestamp"]
        response = self.watch_response(self.player_page(player), video_id)
        with patch("onlysavemevods.youtube.urlopen", return_value=response):
            with self.assertRaises(YtDlpError) as caught:
                YoutubeProbe(runner).probe_video(video_id)
        self.assertIs(caught.exception, original)

    def test_watch_page_removal_takes_precedence_over_end_timestamp(self) -> None:
        runner, original = self.unavailable_runner()
        player = json.loads(self.removal_page().partition(" = ")[2].removesuffix(";"))
        ended = self.ended_broadcast_player("r2ORTHCeg_A")
        player.update({key: value for key, value in ended.items() if key != "playabilityStatus"})
        player["microformat"]["playerMicroformatRenderer"]["liveBroadcastDetails"] = {
            "isLiveNow": False, "endTimestamp": "2000-01-01T00:00:00Z",
        }
        response = self.watch_response(self.player_page(player))
        with patch("onlysavemevods.youtube.urlopen", return_value=response):
            with self.assertRaises(ConfirmedVideoRemovalError) as caught:
                YoutubeProbe(runner).probe_video("r2ORTHCeg_A")
        self.assertIs(caught.exception.__cause__, original)

    def test_confirmed_probe_errors_do_not_fetch_watch_page(self) -> None:
        for error_type in (ConfirmedLiveTerminationError, ConfirmedVideoRemovalError, ConfirmedLiveEndError):
            with self.subTest(error_type=error_type):
                original = error_type("ERROR: [youtube] r2ORTHCeg_A: Video unavailable")
                runner = MagicMock()
                runner.run_json.side_effect = original
                with patch("onlysavemevods.youtube.urlopen") as opened:
                    with self.assertRaises(error_type) as caught:
                        YoutubeProbe(runner).probe_video("r2ORTHCeg_A")
                opened.assert_not_called()
                self.assertIs(caught.exception, original)

    def test_watch_page_new_interstitial_confirms_explicit_removal(self) -> None:
        reason = "This video has been removed by the uploader"
        self.assertEqual(
            watch_page_video_removal_reason(self.removal_page(reason), "r2ORTHCeg_A"),
            reason,
        )

    def test_watch_page_legacy_error_confirms_explicit_platform_removal(self) -> None:
        reason = "This video has been removed for violating YouTube's policy"
        page = "var ytInitialPlayerResponse = " + json.dumps(
            {
                "playabilityStatus": {
                    "status": "UNPLAYABLE",
                    "errorScreen": {
                        "playerErrorMessageRenderer": {
                            "subreason": {"runs": [{"text": reason}]}
                        }
                    },
                }
            }
        )
        self.assertEqual(watch_page_video_removal_reason(page, "r2ORTHCeg_A"), reason)

    def test_watch_page_does_not_confirm_generic_or_restricted_errors(self) -> None:
        for reason in (
            "Video unavailable",
            "Private video",
            "Sign in to confirm you're not a bot",
            "YouTube is requiring a captcha challenge before playback",
            "This content isn't available, try again later",
            "The current session has been rate-limited by YouTube",
            "The video could not be loaded due to a network error",
        ):
            with self.subTest(reason=reason):
                self.assertIsNone(
                    watch_page_video_removal_reason(self.removal_page(reason), "r2ORTHCeg_A")
                )

    def test_watch_page_requires_valid_player_error_for_same_video(self) -> None:
        reason = "This video has been removed by the uploader"
        pages = (
            reason,
            'var ytInitialPlayerResponse = {"playabilityStatus":',
            "var ytInitialPlayerResponse = null;",
            "var ytInitialPlayerResponse = " + json.dumps(
                {"playabilityStatus": {"status": "OK", "reason": reason}}
            ),
            "var ytInitialPlayerResponse = " + json.dumps(
                {"videoDetails": {"videoId": "EeMqyZVAsMk"},
                 "playabilityStatus": {"status": "ERROR", "reason": reason}}
            ),
        )
        for page in pages:
            with self.subTest(page=page):
                self.assertIsNone(watch_page_video_removal_reason(page, "r2ORTHCeg_A"))

    def test_generic_unavailable_checks_watch_page_for_metadata_and_hls(self) -> None:
        for method in ("probe_video", "probe_live_edge"):
            with self.subTest(method=method):
                runner, original = self.unavailable_runner()
                response = self.watch_response(self.removal_page())
                with patch("onlysavemevods.youtube.urlopen", return_value=response) as opened:
                    with self.assertRaises(ConfirmedVideoRemovalError) as caught:
                        getattr(YoutubeProbe(runner), method)("r2ORTHCeg_A")
                self.assertIs(caught.exception.__cause__, original)
                request = opened.call_args.args[0]
                self.assertEqual(request.full_url, "https://www.youtube.com/watch?v=r2ORTHCeg_A&hl=en")
                self.assertEqual(opened.call_args.kwargs["timeout"], WATCH_PAGE_TIMEOUT_SECONDS)
                response.read.assert_called_once_with(WATCH_PAGE_READ_LIMIT + 1)

    def test_watch_fallback_preserves_original_error_without_explicit_removal(self) -> None:
        runner, original = self.unavailable_runner()
        response = self.watch_response(self.removal_page("Video unavailable"))
        with patch("onlysavemevods.youtube.urlopen", return_value=response):
            with self.assertRaises(YtDlpError) as caught:
                YoutubeProbe(runner).probe_video("r2ORTHCeg_A")
        self.assertIs(caught.exception, original)

    def test_watch_fallback_preserves_original_error_on_network_failure(self) -> None:
        runner, original = self.unavailable_runner()
        with patch("onlysavemevods.youtube.urlopen", side_effect=TimeoutError("timed out")):
            with self.assertRaises(YtDlpError) as caught:
                YoutubeProbe(runner).probe_video("r2ORTHCeg_A")
        self.assertIs(caught.exception, original)

    def test_watch_fallback_rejects_redirect_to_different_video(self) -> None:
        runner, original = self.unavailable_runner()
        response = self.watch_response(self.removal_page(), "EeMqyZVAsMk")
        with patch("onlysavemevods.youtube.urlopen", return_value=response):
            with self.assertRaises(YtDlpError) as caught:
                YoutubeProbe(runner).probe_video("r2ORTHCeg_A")
        self.assertIs(caught.exception, original)
        response.read.assert_not_called()

    def test_watch_fallback_rejects_oversized_page(self) -> None:
        runner, original = self.unavailable_runner()
        response = self.watch_response(self.removal_page() + " " * WATCH_PAGE_READ_LIMIT)
        with patch("onlysavemevods.youtube.urlopen", return_value=response):
            with self.assertRaises(YtDlpError) as caught:
                YoutubeProbe(runner).probe_video("r2ORTHCeg_A")
        self.assertIs(caught.exception, original)

    def test_nonmatching_probe_errors_do_not_fetch_watch_page(self) -> None:
        for message in (
            "ERROR: [youtube] r2ORTHCeg_A: Private video",
            "ERROR: [youtube] r2ORTHCeg_A: HTTP Error 503: Service Unavailable",
            "ERROR: [youtube] r2ORTHCeg_A: Video unavailable. Sign in to continue",
            "ERROR: [youtube] EeMqyZVAsMk: Video unavailable",
            "ERROR: [kick:live] r2ORTHCeg_A: Video unavailable",
            "ERROR: [youtube] EeMqyZVAsMk: Sign in to confirm your age.",
            "ERROR: [youtube] r2ORTHCeg_A: Sign in to confirm you are not a bot.",
        ):
            with self.subTest(message=message):
                original = YtDlpError(message)
                runner = MagicMock()
                runner.run_json.side_effect = original
                with patch("onlysavemevods.youtube.urlopen") as opened:
                    with self.assertRaises(YtDlpError) as caught:
                        YoutubeProbe(runner).probe_video("r2ORTHCeg_A")
                self.assertIs(caught.exception, original)
                opened.assert_not_called()

    def test_parses_hls_live_edge_timestamp_and_sequence(self) -> None:
        edge = parse_youtube_hls_live_edge(
            """#EXTM3U
#EXT-X-MEDIA-SEQUENCE:9655
#EXT-X-PROGRAM-DATE-TIME:2026-08-01T08:28:54.025+00:00
#EXTINF:5.0,
segment-9655.ts
#EXTINF:5.0,
segment-9656.ts
"""
        )

        self.assertEqual(edge.media_sequence, 9655)
        self.assertEqual(
            edge.newest_segment_at,
            datetime(2026, 8, 1, 8, 29, 4, 25000, tzinfo=timezone.utc),
        )
        self.assertFalse(edge.has_endlist)

    def test_parses_hls_endlist(self) -> None:
        edge = parse_youtube_hls_live_edge(
            """#EXTM3U
#EXT-X-MEDIA-SEQUENCE:42
#EXT-X-PROGRAM-DATE-TIME:2026-08-01T08:00:00Z
#EXTINF:6.0,
segment.ts
#EXT-X-ENDLIST
"""
        )

        self.assertTrue(edge.has_endlist)

    def test_selects_highest_non_drm_hls_media_manifest(self) -> None:
        manifest_url, headers = youtube_hls_media_manifest(
            {
                "formats": [
                    {
                        "protocol": "https",
                        "url": "https://example.test/video.mp4",
                        "height": 2160,
                    },
                    {
                        "protocol": "m3u8_native",
                        "url": "https://example.test/720.m3u8",
                        "height": 720,
                    },
                    {
                        "protocol": "m3u8_native",
                        "url": "https://example.test/drm.m3u8",
                        "height": 2160,
                        "has_drm": True,
                    },
                    {
                        "protocol": "m3u8_native",
                        "url": "https://example.test/1080.m3u8",
                        "height": 1080,
                        "http_headers": {"User-Agent": "test-agent"},
                    },
                ]
            }
        )

        self.assertEqual(manifest_url, "https://example.test/1080.m3u8")
        self.assertEqual(headers, {"User-Agent": "test-agent"})

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
        runner = FakeRunner()
        probe = YoutubeProbe(runner, channel_scan_limit=10)

        streams = probe.discover_channel_live_streams("@Example")

        self.assertEqual(
            [stream.video_id for stream in streams],
            ["youtube:LIVEVIDEO01", "youtube:LIVEVIDEO02"],
        )
        self.assertEqual(runner.calls[0][-1], "https://www.youtube.com/@Example/live")
        self.assertEqual(runner.calls[1][-1], "https://www.youtube.com/@Example/streams")
        self.assertFalse(any(call[-1].endswith("v=LIVEVIDEO01") for call in runner.calls))

    def test_streams_listing_error_preserves_confirmed_channel_live_stream(self) -> None:
        runner = MagicMock()
        runner.run_json.side_effect = [
            {
                "id": "LIVEVIDEO01",
                "webpage_url": "https://www.youtube.com/watch?v=LIVEVIDEO01",
                "live_status": "is_live",
            },
            YtDlpError("streams page unavailable"),
        ]
        probe = YoutubeProbe(runner)

        with self.assertLogs("onlysavemevods.youtube", level="WARNING") as captured:
            streams = probe.discover_channel_live_streams("@Example")

        self.assertEqual([stream.video_id for stream in streams], ["youtube:LIVEVIDEO01"])
        self.assertIn("keeping 1 confirmed live stream(s)", captured.output[0])
        self.assertIn("streams page unavailable", captured.output[0])

    def test_streams_listing_error_without_confirmed_live_stream_is_raised(self) -> None:
        runner = MagicMock()
        listing_error = YtDlpError("streams page unavailable")
        runner.run_json.side_effect = [
            {"id": "ENDEDVIDEO1", "live_status": "was_live"},
            listing_error,
        ]
        probe = YoutubeProbe(runner)

        with self.assertRaises(YtDlpError) as captured:
            probe.discover_channel_live_streams("@Example")

        self.assertIs(captured.exception, listing_error)

    def test_probe_channel_live_stream_uses_live_url_fast_path(self) -> None:
        runner = FakeRunner()
        probe = YoutubeProbe(runner)

        stream = probe.probe_channel_live_stream("@Example")

        self.assertIsNotNone(stream)
        assert stream is not None
        self.assertEqual(stream.video_id, "youtube:LIVEVIDEO01")
        self.assertEqual(runner.calls[0][-1], "https://www.youtube.com/@Example/live")

    def test_video_probe_matches_live_from_start_download_mode(self) -> None:
        runner = FakeRunner()
        probe = YoutubeProbe(runner, live_from_start=True)

        probe.probe_video("LIVEVIDEO01")

        self.assertIn("--live-from-start", runner.calls[0])

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
        self.assertTrue(
            is_terminal_video_unavailable_message(
                "ERROR: [youtube] LIVEVIDEO01: Video unavailable. "
                "It was blocked due to the claimed content by SME."
            )
        )
        self.assertFalse(
            is_terminal_video_unavailable_message(
                "ERROR: [youtube] LIVEVIDEO01: HTTP Error 503: Service Unavailable"
            )
        )

    def test_explicit_live_termination_is_confirmed(self) -> None:
        message = (
            "ERROR: [youtube] 8YbgANWF8pk: Video unavailable. "
            "This live stream has been terminated due to use of "
            "3rd party audio or video content."
        )
        self.assertTrue(is_confirmed_live_termination_message(message))
        self.assertTrue(is_terminal_video_unavailable_message(message))
        self.assertTrue(
            is_confirmed_live_termination_message(
                "ERROR: [youtube] 8YbgANWF8pk: The live stream was "
                "terminated due to a policy violation."
            )
        )
        self.assertFalse(
            is_confirmed_live_termination_message(
                "Video unavailable. This video has been removed by the uploader"
            )
        )
        self.assertFalse(
            is_confirmed_live_termination_message("Private video")
        )
        self.assertFalse(
            is_confirmed_live_termination_message(
                "HTTP Error 503: Service Unavailable"
            )
        )
        self.assertFalse(
            is_confirmed_live_termination_message(
                "The live stream was terminated due to a policy violation."
            )
        )
        self.assertFalse(
            is_confirmed_live_termination_message(
                "ERROR: [kick:live] oumb: The live stream was terminated due to policy."
            )
        )
        self.assertFalse(
            is_confirmed_live_termination_message(
                "ERROR: [youtube] 8YbgANWF8pk: The live stream was terminated unexpectedly"
            )
        )

    def test_explicit_youtube_removal_is_confirmed(self) -> None:
        for reason in (
            "Video unavailable. This video has been removed by the uploader",
            "This video has been removed for violating YouTube's policy",
            "This video has been deleted",
            "This video was removed by the uploader",
            "This video is deleted",
        ):
            with self.subTest(reason=reason):
                message = f"ERROR: [youtube] r2ORTHCeg_A: {reason}"
                self.assertTrue(is_confirmed_video_removal_message(message))
                self.assertTrue(is_terminal_video_unavailable_message(message))
        self.assertFalse(is_confirmed_video_removal_message(
            "ERROR: [youtube] r2ORTHCeg_A: This video has been removed",
            video_id="EeMqyZVAsMk",
        ))
        for message in (
            "This video has been removed by the uploader",
            "ERROR: [kick:live] r2ORTHCeg_A: This video has been removed",
            "ERROR: [youtube] invalid: This video has been removed",
            "ERROR: [youtube] r2ORTHCeg_A: Video unavailable",
            "ERROR: [youtube] r2ORTHCeg_A: Private video",
            "ERROR: [youtube] r2ORTHCeg_A: HTTP Error 503: Service Unavailable",
        ):
            with self.subTest(message=message):
                self.assertFalse(is_confirmed_video_removal_message(message))

    def test_runner_raises_confirmed_live_termination_for_youtube_message(self) -> None:
        completed = CompletedProcess(
            args=["yt-dlp"],
            returncode=1,
            stdout="",
            stderr=(
                "ERROR: [youtube] 8YbgANWF8pk: Video unavailable. "
                "This live stream has been terminated due to use of "
                "3rd party audio or video content."
            ),
        )

        with patch("subprocess.run", return_value=completed):
            with self.assertRaises(ConfirmedLiveTerminationError):
                YtDlpRunner().run_json(["--dump-json", "https://example.test"])

    def test_runner_raises_terminal_error_for_private_video(self) -> None:
        completed = CompletedProcess(
            args=["yt-dlp"],
            returncode=1,
            stdout="",
            stderr="ERROR: [youtube] LIVEVIDEO01: Private video",
        )

        with patch("subprocess.run", return_value=completed):
            with self.assertRaises(TerminalVideoUnavailableError) as caught:
                YtDlpRunner().run_json(["--dump-json", "https://example.test"])
        self.assertIs(type(caught.exception), TerminalVideoUnavailableError)

    def test_runner_raises_confirmed_removal_error_for_removed_video(self) -> None:
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
            with self.assertRaises(TerminalVideoUnavailableError) as caught:
                YtDlpRunner().run_json(["--dump-json", "https://example.test"])
        self.assertIs(type(caught.exception), ConfirmedVideoRemovalError)

    def test_runner_reports_empty_json_output(self) -> None:
        completed = CompletedProcess(
            args=["yt-dlp"],
            returncode=0,
            stdout="",
            stderr="",
        )

        with patch("subprocess.run", return_value=completed):
            with self.assertRaisesRegex(YtDlpError, "no JSON output"):
                YtDlpRunner().run_json(["--dump-json", "https://example.test"])
