from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from onlysavemevods.models import LiveStream
from onlysavemevods.state import StateStore


class StateWatermarkTests(unittest.TestCase):
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
