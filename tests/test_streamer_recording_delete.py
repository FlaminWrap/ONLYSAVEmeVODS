from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from onlysavemevods import web
from onlysavemevods.config import ConfigError, load_config
from onlysavemevods.downloader import segment_directory
from onlysavemevods.models import LiveStream
from onlysavemevods.state import StateStore


class StreamerRecordingDeleteTests(unittest.TestCase):
    def setUp(self):
        temp = TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.path = Path(temp.name) / "config.toml"
        self.path.write_text(
            'channels = ["@standalone"]\ndownload_dir = "downloads"\nstate_dir = "state"\n'
            '[streamers.Alpha] # handwritten header\nsources = ["@first", "kick:shared"]\n'
            'download_dir_name = "Alpha Files"\n'
            '[streamers.Alpha.post_stream]\ntranscribe_subtitles = false\n'
            '[streamers."Beta"]\nsources = ["@second", "twitch:shared"]\n'
        )
        self.config = load_config(self.path)
        self.state = StateStore(self.config.db_path)
        self.addCleanup(self.state.close)

    def recording(self, video_id, channel, source, *, url="", vod=False, status="ended"):
        stream = LiveStream(
            video_id=video_id, platform=video_id.split(":")[0], url=url, source=source, channel=channel,
        )
        if vod:
            self.state.upsert_vod_stream(stream, status=status)
        else:
            self.state.upsert_detected(stream)
            if status == "ended":
                self.state.mark_ended(video_id)
        media = segment_directory(self.config, video_id, channel) / "segment-001.mp4"
        media.parent.mkdir(parents=True, exist_ok=True)
        media.write_bytes(video_id.encode())
        return media

    def remove(self, **fields):
        web.update_streamer_from_form(self.config, {
            "action": ["delete"], "streamer_name": ["Alpha"],
            "confirm_delete": ["delete_streamer_recordings"], **fields,
        })

    def test_only_matching_platform_sources_and_assigned_vods_are_removed(self):
        owned = {
            "youtube:1": self.recording("youtube:1", "Changed uploader", "@first"),
            "kick:2": self.recording("kick:2", "SHARED", "https://kick.com/shared"),
            "youtube:3": self.recording("youtube:3", "Alpha", "https://youtube.com/watch?v=3", vod=True),
            "youtube:4": self.recording("youtube:4", "First", "", url="https://youtube.com/watch?v=4"),
        }
        others = {
            "twitch:1": self.recording("twitch:1", "SHARED", "twitch:shared"),
            "twitch:2": self.recording("twitch:2", "SHARED", "", url="https://twitch.tv/videos/123"),
            "kick:3": self.recording("kick:3", "Beta", "https://kick.com/shared/videos/abc", vod=True),
            "youtube:5": self.recording("youtube:5", "Alpha", "@second"),
            "youtube:6": self.recording("youtube:6", "Alpha", "@standalone"),
        }
        folder = owned["kick:2"].parent.parent
        untracked = folder / "loose.mp4"
        untracked.write_bytes(b"untracked")
        self.remove(delete_recordings=["1"])
        for video_id, media in owned.items():
            self.assertIsNone(self.state.get_stream(video_id))
            self.assertFalse(media.parent.exists())
        for video_id, media in others.items():
            self.assertEqual(self.state.get_stream(video_id).status, "ended")
            record = self.state.get_stream(video_id)
            after_restart = segment_directory(load_config(self.path), video_id, record.channel)
            self.assertEqual(after_restart / media.name, media)
            self.assertEqual(media.read_bytes(), video_id.encode())
        self.assertEqual(untracked.read_bytes(), b"untracked")
        self.assertEqual(set(self.config.streamers), {"Beta"})
        self.assertEqual(set(load_config(self.path).streamers), {"Beta"})

    def test_confirm_and_checkbox_are_checked_separately(self):
        media = self.recording("kick:1", "shared", "kick:shared")
        for fields in (
            {"delete_recordings": ["0"]}, {"delete_recordings": ["1", "0"]},
            {"delete_recordings": ["1"], "confirm_delete": []},
            {"delete_recordings": ["1"], "confirm_delete": ["delete_stream"]},
        ):
            with self.subTest(fields=fields), self.assertRaisesRegex(ConfigError, "confirmation"):
                self.remove(**fields)
        self.assertIn("Alpha", load_config(self.path).streamers)
        self.remove()
        self.assertNotIn("Alpha", load_config(self.path).streamers)
        self.assertEqual(segment_directory(load_config(self.path), "kick:1", "shared"), media.parent)
        self.assertEqual(media.read_bytes(), b"kick:1")
        self.assertEqual(self.state.get_stream("kick:1").status, "ended")

    def test_config_only_removal_does_not_adopt_an_untracked_matching_basename(self):
        media = self.recording("youtube:same", "First", "@first")
        unrelated = self.config.download_dir / "another" / media.parent.name
        unrelated.mkdir(parents=True)
        (unrelated / media.name).write_bytes(b"untracked")
        self.remove()
        reopened = load_config(self.path)
        self.assertEqual(segment_directory(reopened, "youtube:same", "First"), media.parent)
        media.unlink()
        media.parent.rmdir()
        self.assertNotEqual(segment_directory(reopened, "youtube:same", "First"), unrelated)
        self.assertEqual((unrelated / media.name).read_bytes(), b"untracked")

    def test_batch_preflight_blocks_if_any_record_can_run_or_has_queued_work(self):
        oldest = self.recording("kick:old", "shared", "kick:shared", status="detected")
        newest = self.recording("kick:new", "shared", "kick:shared")
        with self.assertRaisesRegex(ConfigError, "may still run"):
            self.remove(delete_recordings=["1"])
        self.state.mark_ended("kick:old")
        self.state.enqueue_post_processing_job(
            video_id="kick:old", kind="transcription", segment_index=1, media_path=oldest,
        )
        with self.assertRaisesRegex(ConfigError, "queued or running"):
            self.remove(delete_recordings=["1"])
        self.assertFalse(self.config.streamers["Alpha"].archived)
        self.assertTrue(oldest.is_file())
        self.assertTrue(newest.is_file())
        self.assertEqual(self.state.get_stream("kick:new").status, "ended")

    def test_unregistered_conflicting_channel_url_blocks_all(self):
        media = self.recording("kick:old", "shared", "", url="https://kick.com/different")
        other = self.recording("kick:new", "shared", "kick:shared")
        with self.assertRaisesRegex(ConfigError, "overlapping"):
            self.remove(delete_recordings=["1"])
        self.assertTrue(media.is_file())
        self.assertTrue(other.is_file())

    def test_shared_canonical_source_blocks_batch(self):
        self.path.write_text(self.path.read_text().replace('"twitch:shared"', '"https://kick.com/shared"'))
        self.config = load_config(self.path)
        ambiguous = self.recording("kick:shared", "shared", "kick:shared")
        unrelated = self.recording("youtube:1", "First", "@first")
        with self.assertRaisesRegex(ConfigError, "multiple"):
            self.remove(delete_recordings=["1"])
        self.assertTrue(ambiguous.is_file())
        self.assertTrue(unrelated.is_file())

    def test_filesystem_failure_commits_finished_rows_and_can_retry(self):
        oldest = self.recording("kick:old", "shared", "kick:shared")
        newest = self.recording("kick:new", "shared", "kick:shared")
        actual = web.remove_stream_recordings

        def remove(config, target):
            if target.directory == oldest.parent:
                raise OSError("disk busy")
            return actual(config, target)

        with patch.object(web, "remove_stream_recordings", side_effect=remove):
            with self.assertRaisesRegex(ConfigError, "kick:old.*Removed 1 recording"):
                self.remove(delete_recordings=["1"])
        self.assertTrue(oldest.is_file())
        self.assertFalse(newest.is_file())
        self.assertIsNone(self.state.get_stream("kick:new"))
        self.assertEqual(self.state.get_stream("kick:old").status, "ended")
        self.assertFalse(self.config.streamers["Alpha"].archived)
        self.assertIn("Alpha", load_config(self.path).streamers)
        self.remove(delete_recordings=["1"])
        self.assertIsNone(self.state.get_stream("kick:old"))
        self.assertNotIn("Alpha", load_config(self.path).streamers)

    def test_config_write_failure_keeps_monitoring_and_reports_removed_count(self):
        media = self.recording("kick:old", "shared", "kick:shared")
        actual = web.remove_streamer_config

        def write_config(path, name, **options):
            if options.get("validate_only"):
                return actual(path, name, **options)
            raise OSError("config read-only")

        with patch.object(web, "remove_streamer_config", side_effect=write_config):
            with self.assertRaisesRegex(ConfigError, "configuration.*Removed 1 recording"):
                self.remove(delete_recordings=["1"])
        self.assertFalse(media.is_file())
        self.assertIsNone(self.state.get_stream("kick:old"))
        self.assertFalse(self.config.streamers["Alpha"].archived)
        self.assertIn("Alpha", load_config(self.path).streamers)
        self.remove(delete_recordings=["1"])
        self.assertNotIn("Alpha", load_config(self.path).streamers)


if __name__ == "__main__":
    unittest.main()
