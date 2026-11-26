import unittest
from unittest.mock import patch

from onlysavemevods.kick_archive_resolver import KickArchiveResolver
from onlysavemevods.youtube import YtDlpError


REPLAY = "01a21901-2090-7c9b-898f-52d7d2662387"
OTHER = "01a215ab-4a88-7fa4-8648-745137de129f"
FMT = {
    "format_id": "1080p60",
    "protocol": "m3u8_native",
    "vcodec": "avc1",
    "acodec": "aac",
    "url": "https://cdn.example/selected.m3u8",
}


def entry(replay=REPLAY, start=1000, live=True):
    return {
        "id": replay,
        "timestamp": start,
        "is_live": live,
        "duration": 0 if live else 200,
    }


def video(**changes):
    return {
        "id": REPLAY,
        "timestamp": 1000,
        "format_id": "1080p60",
        "formats": [FMT],
        **changes,
    }


class KickArchiveResolutionTests(unittest.TestCase):
    def test_matches_start_time_not_newest_entry_or_created_at(self):
        r = KickArchiveResolver("kick:example", "kick:42")
        results = [
            {"id": "42", "timestamp": 1006, "release_timestamp": 1000},
            {"entries": [entry(OTHER, 1006), entry()]},
            video(),
        ]
        with patch.object(r, "_metadata", side_effect=results) as probe:
            self.assertEqual(r.resolve(), (FMT["url"], False))
            self.assertEqual(r.resolve(), (FMT["url"], False))
        self.assertEqual(probe.call_count, 3)
        self.assertEqual(
            (r.replay_id, r.started_at, r.format_id), (REPLAY, 1000, "1080p60")
        )

    def test_uncertain_broadcast_is_never_replaced_by_current_live_id(self):
        r = KickArchiveResolver("https://kick.com/example", "kick:old")
        with patch.object(
            r, "_metadata", return_value={"id": "new", "release_timestamp": 2000}
        ) as probe:
            self.assertIsNone(r.resolve())
        self.assertIsNone(r.replay_id)
        self.assertEqual(probe.call_count, 1)

    def test_ambiguous_start_waits(self):
        r = KickArchiveResolver("example", "kick:42", started_at=1000)
        with patch.object(
            r, "_metadata", return_value={"entries": [entry(), entry(OTHER)]}
        ):
            self.assertIsNone(r.resolve())
        self.assertIsNone(r.replay_id)

    def test_recovery_uses_pinned_uuid_and_format_and_explicit_ended_signal(self):
        r = KickArchiveResolver(
            "kick:example",
            "kick:42",
            started_at=1000,
            replay_id=REPLAY,
            format_id="1080p60",
        )
        with patch.object(
            r, "_metadata", side_effect=[{"entries": [entry(live=False)]}, video()]
        ):
            self.assertEqual(r.resolve(), (FMT["url"], True))
        self.assertEqual(r.expected_duration, 200)
        r.invalidate()
        with patch.object(
            r, "_metadata", side_effect=[{"entries": []}, video(duration=200)]
        ):
            self.assertEqual(r.resolve(), (FMT["url"], False))
        r.invalidate()
        with patch.object(
            r,
            "_metadata",
            side_effect=[{"entries": [entry(live=False)]}, video(is_live=True)],
        ):
            self.assertEqual(r.resolve(), (FMT["url"], False))
        r.invalidate()
        with patch.object(
            r,
            "_metadata",
            side_effect=[
                {"entries": [entry()]},
                video(format_id="480p", formats=[dict(FMT, format_id="480p")]),
            ],
        ):
            with self.assertRaisesRegex(ValueError, "rendition"):
                r.resolve()

    def test_different_start_fails_and_metadata_error_is_sanitized(self):
        r = KickArchiveResolver(
            "kick:example", "kick:42", started_at=1000, replay_id=REPLAY
        )
        with patch.object(
            r, "_metadata", side_effect=[{"entries": []}, video(timestamp=1030)]
        ):
            with self.assertRaisesRegex(ValueError, "start time"):
                r.resolve()
        r.invalidate()
        with patch.object(
            r, "_metadata", side_effect=YtDlpError("private query string")
        ):
            with self.assertRaisesRegex(
                YtDlpError, "^Kick replay metadata is unavailable; retrying$"
            ):
                r.resolve()
