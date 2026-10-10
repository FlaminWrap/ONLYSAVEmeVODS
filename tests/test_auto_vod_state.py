from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from onlysavemevods.models import LiveStream
from onlysavemevods.state import StateStore


class AutomaticVodRecoveryStateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = Path(self.tmp.name) / "state.sqlite3"
        self.state = StateStore(self.db_path)
        self.addCleanup(self.state.close)
        self.stream = LiveStream(
            video_id="youtube:6WKBYU9Rg-c",
            url="https://www.youtube.com/watch?v=6WKBYU9Rg-c",
            title="Original live title",
            channel="Example",
            platform="youtube",
            source="@Example",
        )
        self.state.mark_downloading(self.stream, 3)
        self.state.mark_exited(self.stream.video_id, 1)
        self.state.mark_finalization_failed(self.stream.video_id)
        self.original = self.state.get_stream(self.stream.video_id)
        self.output_template = str(Path(self.tmp.name) / "replacement.%(ext)s")

    def begin(self, now: float = 1000.0):
        return self.state.begin_automatic_vod_recovery(
            self.stream.video_id, self.output_template, now=now
        )

    def assert_original_session(self) -> None:
        current = self.state.get_stream(self.stream.video_id)
        for name in (
            "video_id", "title", "channel", "url", "source", "recording_kind",
            "segment_index", "first_seen_at", "last_started_at", "last_exit_at", "exit_code",
        ):
            self.assertEqual(getattr(current, name), getattr(self.original, name), name)

    def test_cooldown_persists_and_retries_reuse_the_original_output_template(self) -> None:
        first = self.begin()
        self.assertEqual(first.attempts, 1)
        self.assertEqual(first.next_attempt_at, 1300)
        self.assertFalse(first.in_progress)
        self.assertIsNone(self.begin(1299))
        self.state.close()
        self.state = StateStore(self.db_path)
        self.addCleanup(self.state.close)
        self.assertIsNone(self.begin(1299))
        second = self.state.begin_automatic_vod_recovery(
            self.stream.video_id, "different-output.%(ext)s", now=1300
        )
        self.assertEqual(second.attempts, 2)
        self.assertEqual(second.next_attempt_at, 1900)
        self.assertEqual(second.output_template, self.output_template)
        self.assert_original_session()

    def test_backoff_caps_at_one_hour(self) -> None:
        now = 1000.0
        for attempt, delay in enumerate((300, 600, 1200, 2400, 3600, 3600), 1):
            record = self.begin(now)
            self.assertEqual(record.attempts, attempt)
            self.assertEqual(record.next_attempt_at, now + delay)
            now = record.next_attempt_at

    def test_attempt_reservation_excludes_a_second_database_connection(self) -> None:
        other = StateStore(self.db_path)
        self.addCleanup(other.close)
        self.assertIsNotNone(self.begin())
        self.assertIsNone(other.begin_automatic_vod_recovery(
            self.stream.video_id, "other.%(ext)s", now=1000
        ))
        self.assertTrue(other.mark_vod_downloading(self.stream, automatic=True))
        self.assertFalse(self.state.mark_vod_downloading(self.stream, automatic=True))
        self.assertTrue(self.state.get_automatic_vod_recovery(self.stream.video_id).in_progress)
        self.assert_original_session()

    def test_automatic_claim_requires_a_failed_recording_and_reservation(self) -> None:
        self.assertFalse(self.state.mark_vod_downloading(self.stream, automatic=True))
        unknown = replace(self.stream, video_id="youtube:UNKNOWN")
        self.assertFalse(self.state.mark_vod_downloading(unknown, automatic=True))
        self.assertIsNone(self.state.get_stream(unknown.video_id))
        for status in ("downloading", "checking_after_exit", "finalizing", "ended"):
            self.state.conn.execute(
                "UPDATE streams SET status = ? WHERE video_id = ?",
                (status, self.stream.video_id),
            )
            self.state.conn.commit()
            self.assertIsNone(self.begin())
            self.assertFalse(self.state.mark_vod_downloading(self.stream, automatic=True))
        self.assertIsNone(self.state.get_automatic_vod_recovery(self.stream.video_id))

    def test_failed_attempt_restores_original_session_and_obeys_cooldown(self) -> None:
        self.begin()
        replacement = replace(self.stream, title="VOD title", source="replacement", channel="Other")
        self.assertTrue(self.state.mark_vod_downloading(replacement, automatic=True))
        self.state.mark_vod_download_failed(
            self.stream.video_id, "temporary archive failure",
            restore_status="finalization_failed", exit_code=2,
        )
        recovery = self.state.get_automatic_vod_recovery(self.stream.video_id)
        self.assertFalse(recovery.in_progress)
        self.assertFalse(recovery.completed)
        self.assertEqual(self.state.get_stream(self.stream.video_id).status, "finalization_failed")
        self.assert_original_session()
        self.assertIsNone(self.begin(1299))
        self.assertIsNotNone(self.begin(1300))

    def test_completed_attempt_cannot_be_automatically_requeued(self) -> None:
        self.begin()
        self.assertTrue(self.state.mark_vod_downloading(self.stream, automatic=True))
        self.state.mark_vod_download_finished(self.stream.video_id)
        recovery = self.state.get_automatic_vod_recovery(self.stream.video_id)
        self.assertTrue(recovery.completed)
        self.assertFalse(recovery.in_progress)
        self.assertEqual(self.state.get_stream(self.stream.video_id).status, "ended")
        self.state.conn.execute(
            "UPDATE streams SET status = 'finalization_failed' WHERE video_id = ?",
            (self.stream.video_id,),
        )
        self.state.conn.commit()
        self.assertIsNone(self.begin(10000))
        self.assertFalse(self.state.mark_vod_downloading(self.stream, automatic=True))

    def test_blocked_attempt_allows_manual_redownload_without_creating_an_automatic_claim(self) -> None:
        self.begin()
        self.state.block_automatic_vod_recovery(self.stream.video_id)
        self.assertIsNone(self.begin(10000))
        self.assertFalse(self.state.mark_vod_downloading(self.stream, automatic=True))
        self.assertTrue(self.state.mark_vod_downloading(self.stream))
        self.assertFalse(self.state.get_automatic_vod_recovery(self.stream.video_id).in_progress)
        self.state.mark_vod_download_finished(self.stream.video_id)
        self.assertFalse(self.state.get_automatic_vod_recovery(self.stream.video_id).completed)

    def test_restart_restores_an_interrupted_copy_before_live_reconciliation(self) -> None:
        self.begin()
        self.assertTrue(self.state.mark_vod_downloading(self.stream, automatic=True))
        self.state.close()
        self.state = StateStore(self.db_path)
        self.addCleanup(self.state.close)
        self.state.reconcile_stale_downloads()
        self.assertEqual(self.state.get_stream(self.stream.video_id).status, "finalization_failed")
        self.assert_original_session()
        recovery = self.state.get_automatic_vod_recovery(self.stream.video_id)
        self.assertFalse(recovery.in_progress)
        self.assertEqual(recovery.next_attempt_at, 1300)
        self.assertEqual(recovery.output_template, self.output_template)
        events = self.state.list_stream_events([self.stream.video_id])[self.stream.video_id]
        self.assertTrue(any("Automatic VOD recovery interrupted" in event.message for event in events))
        self.assertFalse(any("Recovering active download" in event.message for event in events))

    def test_restart_clears_orphaned_claim_without_overriding_finished_status(self) -> None:
        self.begin()
        self.assertTrue(self.state.mark_vod_downloading(self.stream, automatic=True))
        self.state.mark_ended(self.stream.video_id)
        self.state.reconcile_stale_downloads()
        self.assertEqual(self.state.get_stream(self.stream.video_id).status, "ended")
        self.assertFalse(self.state.get_automatic_vod_recovery(self.stream.video_id).in_progress)

    def test_delete_removes_retry_record_and_missing_stream_cannot_reserve(self) -> None:
        self.begin()
        self.assertTrue(self.state.delete_stream(self.stream.video_id))
        self.assertIsNone(self.state.get_automatic_vod_recovery(self.stream.video_id))
        self.assertIsNone(self.begin(10000))


if __name__ == "__main__":
    unittest.main()
