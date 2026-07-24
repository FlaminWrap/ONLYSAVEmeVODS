from pathlib import Path
from tempfile import TemporaryDirectory
import sqlite3
import unittest

from onlysavemevods.models import LiveStream
from onlysavemevods.state import StateStore


class StateWatermarkTests(unittest.TestCase):
    def test_youtube_video_format_lock_persists_across_reopen(self) -> None:
        with TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "state.sqlite3"
            state = StateStore(db_path)
            stream = LiveStream(
                video_id="youtube:LIVEVIDEO01",
                url="https://www.youtube.com/watch?v=LIVEVIDEO01",
            )
            state.upsert_detected(stream)
            state.lock_youtube_video_format(
                stream.video_id,
                format_id="303",
                codec="vp9",
                selector="303+bestaudio",
            )
            state.close()

            reopened = StateStore(db_path)
            record = reopened.get_stream(stream.video_id)
            events = reopened.list_stream_events(
                [stream.video_id],
                limit_per_stream=10,
            )[stream.video_id]
            reopened.close()

        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(record.youtube_video_format_id, "303")
        self.assertEqual(record.youtube_video_codec, "vp9")
        self.assertEqual(record.youtube_video_format_selector, "303+bestaudio")
        self.assertTrue(
            any("Locked YouTube video format" in event.message for event in events)
        )

    def test_existing_stream_database_migrates_video_format_columns(self) -> None:
        with TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "state.sqlite3"
            connection = sqlite3.connect(db_path)
            connection.execute(
                """
                CREATE TABLE streams (
                    video_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL DEFAULT '',
                    channel TEXT NOT NULL DEFAULT '',
                    url TEXT NOT NULL,
                    platform TEXT NOT NULL DEFAULT 'youtube',
                    source TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL,
                    segment_index INTEGER NOT NULL DEFAULT 1,
                    first_seen_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_started_at TEXT,
                    last_exit_at TEXT,
                    exit_code INTEGER
                )
                """
            )
            connection.execute(
                """
                INSERT INTO streams (
                    video_id, url, status, first_seen_at, updated_at
                ) VALUES (
                    'youtube:LIVEVIDEO01',
                    'https://www.youtube.com/watch?v=LIVEVIDEO01',
                    'interrupted',
                    '2026-07-24T00:00:00+00:00',
                    '2026-07-24T00:00:00+00:00'
                )
                """
            )
            connection.commit()
            connection.close()

            state = StateStore(db_path)
            record = state.get_stream("youtube:LIVEVIDEO01")
            columns = {
                str(row[1])
                for row in state.conn.execute("PRAGMA table_info(streams)").fetchall()
            }
            state.close()

        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(record.youtube_video_format_selector, "")
        self.assertIn("youtube_video_format_id", columns)
        self.assertIn("youtube_video_codec", columns)
        self.assertIn("youtube_video_format_selector", columns)

    def test_stream_records_include_platform_and_source(self) -> None:
        with TemporaryDirectory() as tmp:
            state = StateStore(Path(tmp) / "state.sqlite3")
            state.upsert_detected(
                LiveStream(
                    video_id="twitch:OUMB3rd",
                    url="https://www.twitch.tv/OUMB3rd",
                    title="Live on Twitch",
                    channel="OUMB3rd",
                    platform="twitch",
                    source="twitch:OUMB3rd",
                )
            )
            record = state.get_stream("twitch:OUMB3rd")
            state.close()

        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(record.platform, "twitch")
        self.assertEqual(record.source, "twitch:OUMB3rd")

    def test_legacy_kick_detected_duplicates_become_deletable(self) -> None:
        with TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "state.sqlite3"
            state = StateStore(db_path)
            legacy_id = "kick:Black ops ports hotel internet 2026-07-15 02:43"
            stable_id = "kick:92722911-black-ops-ports-hotel-internet"
            fallback_id = "kick:Black ops ports hotel internet 2026-07-15 01:42"
            state.upsert_detected(
                LiveStream(
                    video_id=legacy_id,
                    url="https://kick.com/oumb",
                    title="Black ops ports hotel internet 2026-07-15 02:43",
                    channel="oumb",
                    platform="kick",
                    source="kick:oumb",
                )
            )
            state.upsert_detected(
                LiveStream(
                    video_id=stable_id,
                    url="https://kick.com/oumb",
                    title="Black ops ports hotel internet",
                    channel="oumb",
                    platform="kick",
                    source="kick:oumb",
                )
            )
            state.upsert_detected(
                LiveStream(
                    video_id=fallback_id,
                    url="https://kick.com/oumb",
                    title="Black ops ports hotel internet",
                    channel="oumb",
                    platform="kick",
                    source="kick:oumb",
                )
            )
            state.close()

            reopened = StateStore(db_path)
            legacy = reopened.get_stream(legacy_id)
            stable = reopened.get_stream(stable_id)
            fallback = reopened.get_stream(fallback_id)
            reopened.close()

        self.assertIsNotNone(legacy)
        self.assertIsNotNone(stable)
        self.assertIsNotNone(fallback)
        assert legacy is not None
        assert stable is not None
        assert fallback is not None
        self.assertEqual(legacy.status, "ended")
        self.assertEqual(stable.status, "detected")
        self.assertEqual(fallback.status, "detected")

    def test_watermark_copy_lifecycle(self) -> None:
        with TemporaryDirectory() as tmp:
            state = StateStore(Path(tmp) / "state.sqlite3")

            created = state.create_watermark_copy(
                copy_id="wm_copy001",
                video_id="LIVEVIDEO01",
                source_name="Live [LIVEVIDEO01].mp4",
                output_name=".watermarks/Live [LIVEVIDEO01] - wm-copy001.mp4",
                recipient_label="Recipient A",
            )
            state.update_watermark_copy(
                "wm_copy001",
                status="running",
                message="Rendering",
                started=True,
                phase="Rendering frames",
                progress=0.5,
            )
            state.update_watermark_copy(
                "wm_copy001",
                status="done",
                message="Completed",
                finished=True,
                phase="Complete",
                progress=1.0,
            )
            fetched = state.get_watermark_copy("wm_copy001")
            listed = state.list_watermark_copies(
                video_id="LIVEVIDEO01",
                statuses=["done"],
            )
            deleted = state.delete_watermark_copy("wm_copy001")
            after_delete = state.get_watermark_copy("wm_copy001")
            state.close()

        self.assertEqual(created.status, "queued")
        self.assertIsNotNone(fetched)
        assert fetched is not None
        self.assertEqual(fetched.status, "done")
        self.assertEqual(fetched.message, "Completed")
        self.assertEqual(fetched.phase, "Complete")
        self.assertEqual(fetched.progress, 1.0)
        self.assertIsNotNone(fetched.started_at)
        self.assertIsNotNone(fetched.finished_at)
        self.assertEqual([record.copy_id for record in listed], ["wm_copy001"])
        self.assertTrue(deleted)
        self.assertIsNone(after_delete)

    def test_vod_stream_lifecycle_records_events(self) -> None:
        with TemporaryDirectory() as tmp:
            state = StateStore(Path(tmp) / "state.sqlite3")
            stream = LiveStream(
                video_id="youtube:VODVIDEO001",
                url="https://www.youtube.com/watch?v=VODVIDEO001",
                title="VOD Stream",
                channel="Example Streamer",
                platform="youtube",
                source="https://www.youtube.com/watch?v=VODVIDEO001",
                is_live=False,
            )

            state.upsert_vod_stream(stream, event_message="Added manual VOD")
            state.mark_vod_downloading(stream, message="Started VOD download")
            state.mark_vod_download_finished(stream.video_id)
            record = state.get_stream(stream.video_id)
            events = state.list_stream_events([stream.video_id], limit_per_stream=10)
            state.close()

        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(record.status, "ended")
        self.assertEqual(record.channel, "Example Streamer")
        self.assertEqual(record.platform, "youtube")
        self.assertEqual(record.source, "https://www.youtube.com/watch?v=VODVIDEO001")
        self.assertEqual(record.exit_code, 0)
        self.assertIn("Added manual VOD", [event.message for event in events[stream.video_id]])
        self.assertIn("Started VOD download", [event.message for event in events[stream.video_id]])
        self.assertIn("VOD download completed", [event.message for event in events[stream.video_id]])

    def test_delete_stream_removes_record_events_and_watermark_copies(self) -> None:
        with TemporaryDirectory() as tmp:
            state = StateStore(Path(tmp) / "state.sqlite3")
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url="https://www.youtube.com/watch?v=LIVEVIDEO01",
                title="Live",
            )
            state.upsert_detected(stream)
            state.mark_ended(stream.video_id)
            state.add_stream_event(stream.video_id, "custom event")
            state.create_watermark_copy(
                copy_id="wm_copy001",
                video_id=stream.video_id,
                source_name="Live [LIVEVIDEO01].mp4",
                output_name=".watermarks/Live [LIVEVIDEO01] - wm-copy001.mp4",
                recipient_label="Recipient A",
            )

            deleted = state.delete_stream(stream.video_id)
            record = state.get_stream(stream.video_id)
            events = state.list_stream_events([stream.video_id], limit_per_stream=8)
            watermarks = state.list_watermark_copies(video_id=stream.video_id)
            missing = state.delete_stream("MISSING")
            state.close()

        self.assertTrue(deleted)
        self.assertIsNone(record)
        self.assertEqual(events[stream.video_id], [])
        self.assertEqual(watermarks, [])
        self.assertFalse(missing)

    def test_stream_events_are_listed_oldest_first_and_capped(self) -> None:
        with TemporaryDirectory() as tmp:
            state = StateStore(Path(tmp) / "state.sqlite3")
            for index in range(205):
                state.add_stream_event(
                    "LIVEVIDEO01",
                    f"event {index:03d}",
                    level="warning" if index == 204 else "info",
                    segment_index=index if index == 204 else None,
                )

            events = state.list_stream_events(
                ["LIVEVIDEO01", "MISSING"],
                limit_per_stream=5,
            )
            retained_count = state.conn.execute(
                "SELECT COUNT(*) FROM stream_events WHERE video_id = ?",
                ("LIVEVIDEO01",),
            ).fetchone()[0]
            state.close()

        self.assertEqual(retained_count, 200)
        self.assertEqual(events["MISSING"], [])
        self.assertEqual(
            [event.message for event in events["LIVEVIDEO01"]],
            [
                "event 200",
                "event 201",
                "event 202",
                "event 203",
                "event 204",
            ],
        )
        self.assertEqual(events["LIVEVIDEO01"][-1].level, "warning")
        self.assertEqual(events["LIVEVIDEO01"][-1].segment_index, 204)


    def test_streams_can_be_listed_by_status(self) -> None:
        with TemporaryDirectory() as tmp:
            state = StateStore(Path(tmp) / "state.sqlite3")
            checking = LiveStream(
                video_id="CHECKING01",
                url="https://www.youtube.com/watch?v=CHECKING01",
                title="Checking",
            )
            downloading = LiveStream(
                video_id="DOWNLOADING01",
                url="https://www.youtube.com/watch?v=DOWNLOADING01",
                title="Downloading",
            )
            ended = LiveStream(
                video_id="ENDED01",
                url="https://www.youtube.com/watch?v=ENDED01",
                title="Ended",
            )
            state.upsert_detected(checking)
            state.mark_exited(checking.video_id, 0)
            state.mark_downloading(downloading, 1)
            state.upsert_detected(ended)
            state.mark_ended(ended.video_id)

            records = state.list_streams_by_status(["checking_after_exit", "downloading"])
            empty = state.list_streams_by_status([])
            state.close()

        self.assertEqual(empty, [])
        self.assertEqual(
            {record.video_id for record in records},
            {"CHECKING01", "DOWNLOADING01"},
        )

    def test_stale_watermark_jobs_are_marked_interrupted(self) -> None:
        with TemporaryDirectory() as tmp:
            state = StateStore(Path(tmp) / "state.sqlite3")
            state.create_watermark_copy(
                copy_id="wm_copy001",
                video_id="LIVEVIDEO01",
                source_name="Live [LIVEVIDEO01].mp4",
                output_name=".watermarks/Live [LIVEVIDEO01] - wm-copy001.mp4",
                recipient_label="Recipient A",
            )
            state.update_watermark_copy(
                "wm_copy001",
                status="running",
                message="Rendering",
                started=True,
            )

            state.mark_stale_watermarks_interrupted()
            fetched = state.get_watermark_copy("wm_copy001")
            state.close()

        self.assertIsNotNone(fetched)
        assert fetched is not None
        self.assertEqual(fetched.status, "interrupted")
        self.assertIsNotNone(fetched.finished_at)


if __name__ == "__main__":
    unittest.main()
