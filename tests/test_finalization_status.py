from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from onlysavemevods.config import BotConfig, ConfigError
from onlysavemevods.downloader import segment_directory
from onlysavemevods.job_tracker import clear_tracked_jobs, start_tracked_job
from onlysavemevods.models import LiveStream
from onlysavemevods.python_update import idle_result_from_state, idle_result_from_status_snapshot
from onlysavemevods.state import StateStore
from onlysavemevods.web import (
    build_status_snapshot,
    claim_stream_finalization,
    cleanup_expired_stream_fragments,
    cleanup_stream_fragments,
    delete_stream,
    finish_failed_vod_download,
    render_cleanup_fragments_action,
    render_delete_stream_action,
    render_segment_recovery_actions,
    render_stream_signals,
    render_stream_vod_redownload_form,
    stream_needs_attention,
    STREAM_DELETE_CONFIRM_VALUE,
)


class FinalizationStatusTests(unittest.TestCase):
    def setUp(self) -> None:
        clear_tracked_jobs()
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(clear_tracked_jobs)
        root = Path(self.tmp.name)
        self.config = BotConfig(download_dir=root / "downloads", state_dir=root / "state")
        self.stream = LiveStream(
            video_id="youtube:6WKBYU9Rg-c",
            url="https://www.youtube.com/watch?v=6WKBYU9Rg-c",
            platform="youtube",
            channel="Example",
            source="@Example",
        )
        self.state = StateStore(self.config.db_path)
        self.addCleanup(self.state.close)
        self.state.mark_downloading(self.stream, 2)
        self.state.mark_exited(self.stream.video_id, 1)
        self.directory = segment_directory(self.config, self.stream.video_id, self.stream.channel)
        self.directory.mkdir(parents=True)
        self.raw = self.directory / "segment-001.f140.m4a.part"
        self.raw.write_bytes(b"saved audio")
        self.fragment = self.directory / "segment-001.f299.mp4.part-Frag1"
        self.fragment.write_bytes(b"saved fragment")

    def set_status(self, status: str) -> None:
        self.state.conn.execute("UPDATE streams SET status = ? WHERE video_id = ?", (status, self.stream.video_id))
        self.state.conn.commit()

    def test_confirmed_failure_does_not_override_an_active_recording(self) -> None:
        self.set_status("downloading")
        self.assertFalse(self.state.mark_finalization_failed(self.stream.video_id))
        self.assertEqual(self.state.get_stream(self.stream.video_id).status, "downloading")
        self.state.mark_youtube_stale_live(self.stream.video_id, media_sequence=10, edge_at="2026-10-10T08:00:00Z")
        self.assertTrue(self.state.mark_finalization_failed(self.stream.video_id))
        record = self.state.get_stream(self.stream.video_id)
        self.assertEqual(record.status, "finalization_failed")
        self.assertEqual(record.youtube_stale_detected_at, "")
        self.assertIsNone(record.youtube_stale_media_sequence)

    def test_restart_preserves_interrupted_finalization_for_retry(self) -> None:
        self.state.mark_youtube_stale_live(self.stream.video_id, media_sequence=10, edge_at="2026-10-10T08:00:00Z")
        self.assertTrue(claim_stream_finalization(self.config, self.stream.video_id, "stalled"))
        self.state.reconcile_stale_downloads()
        record = self.state.get_stream(self.stream.video_id)
        self.assertEqual(record.status, "finalization_failed")
        self.assertEqual(record.segment_index, 2)
        self.assertEqual(record.youtube_stale_detected_at, "")
        self.assertEqual(record.youtube_stale_edge_at, "")
        self.assertIsNone(record.youtube_stale_media_sequence)
        self.assertEqual(self.raw.read_bytes(), b"saved audio")
        self.assertEqual(self.fragment.read_bytes(), b"saved fragment")
        events = self.state.list_stream_events([self.stream.video_id])[self.stream.video_id]
        self.assertTrue(any("Finalization interrupted" in event.message for event in events))

    def test_finalization_claim_excludes_dashboard_jobs_and_changed_status(self) -> None:
        start_tracked_job("recovery", kind="Video recovery", video_id=self.stream.video_id, item="segment-001")
        self.assertFalse(claim_stream_finalization(self.config, self.stream.video_id, "checking_after_exit"))
        clear_tracked_jobs()
        self.assertFalse(claim_stream_finalization(self.config, self.stream.video_id, "finalization_failed"))
        self.assertTrue(claim_stream_finalization(self.config, self.stream.video_id, "checking_after_exit"))
        self.assertFalse(claim_stream_finalization(self.config, self.stream.video_id, "finalizing"))
        self.assertFalse(self.state.mark_downloading(self.stream, 2))
        self.assertFalse(self.state.mark_vod_downloading(self.stream))
        self.assertEqual(self.state.get_stream(self.stream.video_id).status, "finalizing")

    def test_failed_recording_exposes_recovery_and_requires_confirmed_deletion(self) -> None:
        self.state.mark_finalization_failed(self.stream.video_id)
        stream = build_status_snapshot(self.config, include_speaker_scan=False).streams[0]
        self.assertTrue(stream_needs_attention(stream))
        self.assertIn("source ended; saved media needs recovery", render_stream_signals(stream))
        self.assertIn("Redownload from VOD", render_stream_vod_redownload_form(stream))
        self.assertIn("Recover segment 001", render_segment_recovery_actions(stream))
        self.assertEqual(render_cleanup_fragments_action(stream), "")
        self.assertIn("Delete stream", render_delete_stream_action(stream))
        with self.assertRaises(ConfigError):
            cleanup_stream_fragments(self.config, self.stream.video_id)
        deleted, _message = delete_stream(self.config, self.stream.video_id)
        self.assertFalse(deleted)
        self.assertTrue(self.raw.exists())
        self.assertTrue(self.fragment.exists())

    def test_failed_recording_can_be_deleted_after_confirmation(self) -> None:
        self.state.mark_finalization_failed(self.stream.video_id)
        deleted, _message = delete_stream(self.config, self.stream.video_id, STREAM_DELETE_CONFIRM_VALUE)
        self.assertTrue(deleted)
        self.assertIsNone(self.state.get_stream(self.stream.video_id))
        self.assertFalse(self.raw.exists())
        self.assertFalse(self.fragment.exists())

    def test_finalizing_blocks_manual_recovery_actions(self) -> None:
        self.set_status("finalizing")
        stream = build_status_snapshot(self.config, include_speaker_scan=False).streams[0]
        self.assertEqual(render_stream_vod_redownload_form(stream), "")
        self.assertEqual(render_segment_recovery_actions(stream), "")
        self.assertEqual(render_cleanup_fragments_action(stream), "")
        self.assertEqual(render_delete_stream_action(stream), "")

    def test_failed_sources_are_excluded_from_fragment_retention(self) -> None:
        self.state.mark_finalization_failed(self.stream.video_id)
        self.state.conn.execute("UPDATE streams SET updated_at = '2026-10-01T00:00:00+00:00'")
        self.state.conn.commit()
        config = BotConfig(download_dir=self.config.download_dir, state_dir=self.config.state_dir, fragment_retention_hours=1)
        self.assertEqual(cleanup_expired_stream_fragments(config, now=datetime(2026, 10, 10, tzinfo=timezone.utc)), (0, 0, 0))
        self.assertTrue(self.fragment.exists())

    def test_failed_vod_copy_restores_recoverable_status(self) -> None:
        self.state.mark_finalization_failed(self.stream.video_id)
        self.state.conn.execute(
            "UPDATE streams SET last_exit_at = '2026-10-01T00:00:00+00:00', exit_code = 1"
        )
        self.state.conn.commit()
        original = self.state.get_stream(self.stream.video_id)
        replacement = LiveStream(
            video_id=self.stream.video_id,
            url="https://www.youtube.com/watch?v=REPLACEMENT",
            platform="youtube",
            channel="Replacement",
            source="@Replacement",
            title="Replacement copy",
        )
        self.assertTrue(self.state.mark_vod_downloading(replacement))
        finish_failed_vod_download(self.config, "copy", self.stream.video_id, "Download failed", "finalization_failed")
        restored = self.state.get_stream(self.stream.video_id)
        self.assertEqual(restored.status, "finalization_failed")
        self.assertEqual(restored.segment_index, 2)
        self.assertEqual(restored.recording_kind, "live")
        for attribute in ("url", "channel", "source", "title", "first_seen_at", "last_started_at", "last_exit_at", "exit_code"):
            self.assertEqual(getattr(restored, attribute), getattr(original, attribute))
        self.assertTrue(self.raw.exists())

    def test_updater_waits_during_merge_and_can_restart_pending_recovery(self) -> None:
        for status, idle in (("finalizing", False), ("finalization_failed", True)):
            with self.subTest(status=status):
                self.set_status(status)
                self.assertEqual(idle_result_from_state(self.config.db_path).idle, idle)
                self.assertEqual(idle_result_from_status_snapshot({"counts": {status: 1}, "jobs": []}).idle, idle)


if __name__ == "__main__":
    unittest.main()
