from __future__ import annotations

from datetime import datetime
from importlib.util import find_spec
import json
import os
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from onlysavemevods.config import BotConfig
from onlysavemevods.downloader import segment_directory
from onlysavemevods.kick_archive import checkpoint_path
from onlysavemevods.models import LiveStream
from onlysavemevods.state import StateStore
from onlysavemevods.web import (
    build_lite_status_payload,
    build_status_snapshot,
    is_downloadable_file,
    render_admin_fragment_with_state,
    render_stream_download_progress,
    snapshot_to_dict,
)


class KickCaptureDashboardTests(unittest.TestCase):
    def setUp(self):
        workspace = TemporaryDirectory()
        self.addCleanup(workspace.cleanup)
        self.config = BotConfig(download_dir=Path(workspace.name) / "downloads", state_dir=Path(workspace.name) / "state")
        self.stream = LiveStream(
            video_id="kick:1234", title="Test", channel="example", platform="kick",
            url="https://kick.com/example", source="kick:example",
        )
        self.state = StateStore(self.config.db_path)
        self.addCleanup(self.state.close)
        self.state.mark_downloading(self.stream, 2)
        self.now = datetime.fromisoformat(self.state.get_stream(self.stream.video_id).last_started_at).timestamp() + 100
        self.directory = segment_directory(self.config, self.stream.video_id, self.stream.channel)
        self.directory.mkdir(parents=True)
        self.checkpoint = checkpoint_path(self.directory / "segment-002.ts.part")
        self.checkpoint.parent.mkdir(parents=True, exist_ok=True)

    def capture(self):
        with patch("onlysavemevods.web.time.time", return_value=self.now):
            snapshot = build_status_snapshot(self.config, include_speaker_scan=False, include_file_diagnostics=False)
            lite = build_lite_status_payload(self.config)
        self.assertEqual(lite["stream_revision"], snapshot.stream_revision)
        stream = snapshot.streams[0]
        return render_stream_download_progress(stream), snapshot_to_dict(snapshot)["streams"][0], lite

    def write_checkpoint(self, **updates):
        document = dict(video_id=self.stream.video_id, segment_index=2, phase="recording", saved_segments=3,
                        available_segments=10, bytes_written=3000, saved_seconds=6, available_seconds=20,
                        delay_seconds=14, target_delay_seconds=10, updated_at=self.now, complete=False)
        document.update(updates)
        self.checkpoint.write_text(json.dumps(document))

    def test_media_file_progress_ignores_chat_and_previous_segments(self):
        (self.directory / "segment-001.mp4.part").write_bytes(b"older recording")
        (self.directory / "segment-002.live_chat.json.part").write_bytes(b"chat")
        waiting, stream, first = self.capture()
        self.assertIn("Waiting for media writes", waiting)
        self.assertNotIn("is-stale", waiting)
        self.assertFalse(stream["download_progress_waiting_stale"])
        target = self.directory / "segment-002.mp4.part"
        target.write_bytes(b"media")
        os.utime(target, (self.now, self.now))
        writing, stream, second = self.capture()
        self.assertEqual(stream["kick_capture_progress"]["bytes_written"], 5)
        self.assertIn("Media file updated recently", writing)
        self.assertIn("Live-edge delay unknown", writing)
        self.assertNotIn("is-stale", writing)
        self.assertNotEqual(first["stream_revision"], second["stream_revision"])
        self.now += 60
        quiet, _, third = self.capture()
        self.assertIn("Waiting for media writes", quiet)
        self.assertNotEqual(second["stream_revision"], third["stream_revision"])

    def test_archive_metrics_and_phase_reach_overview_and_api(self):
        for phase, reason, label in (
            ("waiting_archive", "", "Waiting for Kick"),
            ("recording", "", "Catching up to the target delay"),
            ("waiting_delay", "", "Waiting for the target delay"),
            ("waiting_snapshot", "", "Waiting for the next archive update"),
            ("retrying", "missing_segment", "Retrying an archive gap"),
            ("needs_attention", "invalid_segment", "Archive needs attention"),
        ):
            with self.subTest(phase=phase):
                self.write_checkpoint(phase=phase, reason=reason)
                html, payload, lite = self.capture()
                self.assertIn(label, html)
                self.assertIn("3 of 10 published segments verified", html)
                self.assertIn("Estimated live-edge delay 14s", html)
                self.assertIn("Target delay 10s", html)
                self.assertEqual(payload["kick_capture_progress"]["phase"], phase)
                self.assertEqual(lite["attention_count"], int(phase == "needs_attention"))
        overview, _, revision = render_admin_fragment_with_state(self.config, "overview", {})
        self.assertIn("Archive needs attention", overview)
        self.write_checkpoint(phase="waiting_snapshot")
        _, _, recovered_revision = render_admin_fragment_with_state(self.config, "overview", {})
        self.assertNotEqual(revision, recovered_revision)
        self.assertFalse(is_downloadable_file(self.checkpoint.name))

    @unittest.skipUnless(find_spec("fcntl"), "requires POSIX advisory locking")
    def test_old_checkpoint_and_worker_restart(self):
        import fcntl

        self.write_checkpoint()
        _, _, fresh = self.capture()
        self.now += 3600
        html, data, stopped = self.capture()
        self.assertIn("No active archive worker", html)
        self.assertEqual(data["kick_capture_progress"]["report_state"], "no_worker")
        self.assertEqual(stopped["attention_count"], 1)
        self.assertNotEqual(stopped["stream_revision"], fresh["stream_revision"])
        with self.checkpoint.with_suffix(".lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            html, _, delayed = self.capture()
            self.assertIn("Waiting for a fresh worker report", html)
            self.assertEqual(delayed["attention_count"], 0)
            self.write_checkpoint()
            html, _, recovered = self.capture()
            self.assertIn("Catching up to the target delay", html)
            self.assertEqual(recovered["attention_count"], 0)
        self.now = datetime.fromisoformat(self.state.get_stream(self.stream.video_id).last_started_at).timestamp() + 1
        self.write_checkpoint(updated_at=self.now - 3600)
        self.assertIn("first report", self.capture()[0])

    def test_worker_availability_is_unknown_without_lock_support(self):
        self.write_checkpoint()
        self.now += 3600
        with patch.dict(sys.modules, {"fcntl": None}):
            html, data, lite = self.capture()
        self.assertIn("Waiting for a fresh worker report", html)
        self.assertEqual(data["kick_capture_progress"]["report_state"], "awaiting_update")
        self.assertEqual(lite["attention_count"], 0)

    @unittest.skipUnless(hasattr(os, "mkfifo"), "requires named pipes")
    def test_nonregular_checkpoint_does_not_block_status(self):
        os.mkfifo(self.checkpoint)
        self.assertIn("checkpoint unreadable", self.capture()[0])

    def test_archive_integrity_and_pending_finalization(self):
        self.write_checkpoint(saved_segments=10)
        self.assertNotIn("finished", self.capture()[0])
        self.write_checkpoint(phase="complete", complete=False)
        self.assertIn("verification pending", self.capture()[0])
        self.write_checkpoint(phase="complete", saved_segments=10, complete=True)
        self.assertIn("awaiting recording finalization", self.capture()[0])
        self.state.mark_ended(self.stream.video_id)
        self.assertIn("Archive capture finished", self.capture()[0])
        self.write_checkpoint(phase="complete", saved_segments=3, available_segments=10, complete=True)
        self.assertIn("recording ended without a verified archive", self.capture()[0])
        self.write_checkpoint(video_id="kick:other")
        self.assertIn("checkpoint unreadable", self.capture()[0])
        self.checkpoint.write_bytes(b"x" * 40_000)
        self.assertEqual(self.capture()[2]["attention_count"], 1)
