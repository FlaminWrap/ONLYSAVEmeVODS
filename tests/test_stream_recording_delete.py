from pathlib import Path
from tempfile import TemporaryDirectory
import os
import sqlite3
import sys
import unittest
from unittest.mock import patch
from urllib.parse import urlencode

from onlysavemevods.config import BotConfig, ConfigError
from onlysavemevods.downloader import remember_recording_directory, segment_directory
from onlysavemevods.models import LiveStream
from onlysavemevods.state import StateStore
from onlysavemevods.web import (
    STREAM_DELETE_CONFIRM_VALUE,
    delete_stream,
    preflight_stream_recording_delete,
    remove_stream_recordings,
)
from tests.test_web_ui import run_dashboard_request


class OptionalRecordingDeletionTests(unittest.TestCase):
    def setUp(self):
        storage = TemporaryDirectory()
        self.addCleanup(storage.cleanup)
        self.root = Path(storage.name)
        self.config = BotConfig(download_dir=self.root / "downloads", state_dir=self.root / "state")
        self.state = StateStore(self.config.db_path)
        self.addCleanup(self.state.close)

    def ended(self, video_id, channel="Example"):
        stream = LiveStream(video_id=video_id, url="", title="Recording", channel=channel)
        self.state.upsert_detected(stream)
        self.state.mark_ended(video_id)
        directory = segment_directory(self.config, video_id, channel)
        directory.mkdir(parents=True, exist_ok=True)
        media = directory / "segment-001.mp4"
        media.write_bytes(b"original")
        return media

    def submit(self, video_id, choices):
        fields = [("video_id", video_id), ("confirm_delete", STREAM_DELETE_CONFIRM_VALUE)]
        fields.extend(("delete_recordings", choice) for choice in choices)
        body = urlencode(fields).encode()
        request = (
            b"POST /delete-stream HTTP/1.1\r\nHost: localhost:8080\r\n"
            b"Content-Type: application/x-www-form-urlencoded\r\n"
            + f"Content-Length: {len(body)}\r\n\r\n".encode()
            + body
        )
        return run_dashboard_request(self.config, request).splitlines()[0]

    def test_http_removes_media_only_with_one_exact_opt_in(self):
        unrelated = self.ended("unrelated")
        for index, choices in enumerate(([], ["0"], ["true"], ["1", ""], ["1", "0"], ["1"])):
            with self.subTest(choices=choices):
                video_id = f"record-{index}"
                media = self.ended(video_id)
                self.assertIn("303", self.submit(video_id, choices))
                self.assertIsNone(self.state.get_stream(video_id))
                self.assertEqual(media.exists(), choices != ["1"])
                self.assertEqual(unrelated.read_bytes(), b"original")

    def test_removing_history_forgets_its_saved_location(self):
        for delete_files in (False, True):
            with self.subTest(delete_files=delete_files):
                video_id = f"retained-{delete_files}"
                original = self.ended(video_id)
                stored = original.parent.parent.with_name("previous-group") / original.parent.name
                stored.parent.mkdir(exist_ok=True)
                original.parent.rename(stored)
                remember_recording_directory(self.config, video_id, stored)
                self.assertEqual(segment_directory(self.config, video_id, "Example"), stored)
                self.assertIn("303", self.submit(video_id, ["1"] if delete_files else []))
                self.assertEqual(stored.exists(), not delete_files)
                self.assertEqual(segment_directory(self.config, video_id, "Example"), original.parent)

    def test_configured_root_can_point_to_a_media_volume(self):
        volume = self.root / "media-volume"
        volume.mkdir()
        self.config.download_dir.symlink_to(volume, target_is_directory=True)
        media = self.ended("on-volume")
        other = self.ended("on-volume-other")
        self.assertIn("303", self.submit("on-volume", ["1"]))
        self.assertFalse(media.parent.exists())
        self.assertEqual(other.read_bytes(), b"original")
        self.assertTrue(self.config.download_dir.is_symlink())

    def test_parent_symlink_and_recording_symlink_do_not_delete_files(self):
        for index, parent_link in enumerate((True, False)):
            video_id, channel = f"linked-{index}", f"group-{index}"
            media = self.ended(video_id, channel)
            directory = media.parent.parent if parent_link else media.parent
            elsewhere = self.root / f"elsewhere-{index}"
            directory.rename(elsewhere)
            directory.symlink_to(elsewhere, target_is_directory=True)
            self.assertIn("400", self.submit(video_id, ["1"]))
            self.assertEqual(media.read_bytes(), b"original")
            self.assertEqual(self.state.get_stream(video_id).status, "ended")
            self.assertIn("303", self.submit(video_id, []))
            self.assertEqual(media.read_bytes(), b"original")

    def test_shared_sanitized_path_refuses_file_removal(self):
        media = self.ended("kick:88")
        same = self.ended("kick_88")
        self.assertEqual(same, media)
        self.assertIn("400", self.submit("kick:88", ["1"]))
        self.assertEqual(self.state.get_stream("kick:88").status, "ended")
        self.assertEqual(self.state.get_stream("kick_88").status, "ended")
        self.assertEqual(media.read_bytes(), b"original")

    def test_replacement_since_preflight_is_preserved(self):
        media = self.ended("replace")
        record = self.state.get_stream("replace")
        target = preflight_stream_recording_delete(self.config, record, [record])
        media.parent.rename(self.root / "former")
        media.parent.mkdir()
        media.write_bytes(b"replacement")
        with self.assertRaisesRegex(ConfigError, "changed"):
            remove_stream_recordings(self.config, target)
        self.assertEqual(media.read_bytes(), b"replacement")
        self.assertEqual((self.root / "former" / media.name).read_bytes(), b"original")

    def test_missing_preflight_target_cannot_remove_later_recording(self):
        media = self.ended("arrives-later")
        media.unlink()
        media.parent.rmdir()
        record = self.state.get_stream("arrives-later")
        target = preflight_stream_recording_delete(self.config, record, [record])
        media.parent.mkdir()
        media.write_bytes(b"new")
        self.assertEqual(remove_stream_recordings(self.config, target), (0, 0, False))
        self.assertEqual(media.read_bytes(), b"new")

    def test_database_failure_after_file_removal_rolls_back_claim(self):
        media = self.ended("commit-fails")
        with patch.object(
            StateStore, "delete_stream", side_effect=sqlite3.OperationalError("disk full")
        ):
            ok, message = delete_stream(
                self.config,
                "commit-fails",
                STREAM_DELETE_CONFIRM_VALUE,
                delete_recordings=True,
            )
        self.assertFalse(ok)
        self.assertIn("Recording directory removed (1 file", message)
        self.assertIn("disk full", message)
        self.assertFalse(media.parent.exists())
        self.assertEqual(self.state.get_stream("commit-fails").status, "ended")
        self.assertIn("303", self.submit("commit-fails", ["1"]))

    def test_database_failure_before_files_preserves_row_and_media(self):
        media = self.ended("db-busy")
        with patch.object(
            StateStore,
            "compare_and_set_stream_status",
            side_effect=sqlite3.OperationalError("database is locked"),
        ):
            ok, message = delete_stream(
                self.config,
                "db-busy",
                STREAM_DELETE_CONFIRM_VALUE,
                delete_recordings=True,
            )
        self.assertFalse(ok)
        self.assertIn("database is locked", message)
        self.assertEqual(media.read_bytes(), b"original")
        self.assertEqual(self.state.get_stream("db-busy").status, "ended")

    @unittest.skipUnless(sys.platform == "linux", "requires Linux mountinfo")
    def test_preflight_and_removal_refuse_file_and_directory_mounts(self):
        media = self.ended("mounted")
        record = self.state.get_stream("mounted")
        mountinfo = self.root / "mountinfo"
        nested = media.parent / "nested mount"
        nested.mkdir()

        def mount(path):
            escaped = os.fsencode(path).replace(b" ", rb"\040")
            mountinfo.write_bytes(b"7 1 0:1 / " + escaped + b" rw - ext4 /dev/fake rw\n")

        with patch("onlysavemevods.web._LINUX_MOUNTINFO", mountinfo):
            mount(self.config.download_dir)
            target = preflight_stream_recording_delete(self.config, record, [record])
            for path in (media.parent, nested, media):
                with self.subTest(path=path):
                    mount(path)
                    with self.assertRaisesRegex(ConfigError, "mount point"):
                        preflight_stream_recording_delete(self.config, record, [record])
                    with self.assertRaisesRegex(ConfigError, "mount point"):
                        remove_stream_recordings(self.config, target)
                    self.assertEqual(media.read_bytes(), b"original")
            mount(media.parent.parent)
            self.assertEqual(remove_stream_recordings(self.config, target), (1, 8, True))

    @unittest.skipUnless(sys.platform == "linux", "requires Linux mount IDs")
    def test_mount_arriving_during_walk_preserves_its_contents(self):
        media = self.ended("mount-race")
        nested = media.parent / "incoming"
        nested.mkdir()
        foreign = nested / "external.mp4"
        foreign.write_bytes(b"external")
        record = self.state.get_stream("mount-race")
        target = preflight_stream_recording_delete(self.config, record, [record])
        with patch("onlysavemevods.web._linux_mount_id", side_effect=[b"root", b"new"]):
            with self.assertRaisesRegex(ConfigError, "mount point"):
                remove_stream_recordings(self.config, target)
        self.assertEqual(foreign.read_bytes(), b"external")
        self.assertEqual(media.read_bytes(), b"original")

    def test_nested_symlink_is_removed_without_traversing_target(self):
        media = self.ended("prune")
        unrelated = self.ended("keep")
        (media.parent / "linked").symlink_to(unrelated.parent, target_is_directory=True)
        ok, message = delete_stream(
            self.config,
            "prune",
            STREAM_DELETE_CONFIRM_VALUE,
            delete_recordings=True,
        )
        self.assertTrue(ok, message)
        self.assertFalse(media.parent.exists())
        self.assertEqual(unrelated.read_bytes(), b"original")
