from pathlib import Path
from tempfile import TemporaryDirectory
import tomllib
import unittest

from onlysavemevods.config import (
    BotConfig,
    ConfigError,
    StreamerConfig,
    download_group_name_for_channel,
    load_config,
    load_config_text,
    monitored_sources,
    post_stream_config_for_channel,
    set_streamer_archived,
    streamer_for_channel,
    streamer_for_source,
    streamer_sources,
    update_streamer_config,
)


class StreamerArchiveConfigTests(unittest.TestCase):
    def test_archive_flag_defaults_to_false_and_loads_booleans(self) -> None:
        self.assertFalse(StreamerConfig().archived)
        for assignment, expected in (("", False), ("archived = true\n", True),
                                     ("archived = false\n", False)):
            with self.subTest(assignment=assignment):
                config = load_config_text(
                    '[streamers."Example"]\nsources = ["@Example"]\n' + assignment,
                    Path("/tmp/config.toml"),
                )
                self.assertIs(config.streamers["Example"].archived, expected)

    def test_archive_flag_rejects_non_booleans(self) -> None:
        for value in ('"true"', "1", "[]", "{}"):
            with self.subTest(value=value), self.assertRaisesRegex(
                ConfigError, r"streamers.Example.archived must be true or false"
            ):
                load_config_text(
                    '[streamers.Example]\nsources = ["@Example"]\n'
                    f"archived = {value}\n",
                    Path("/tmp/config.toml"),
                )

    def test_archive_and_restore_keep_sources_and_existing_settings(self) -> None:
        original = (
            '# Keep this comment.\nchannels = ["@Other"]\n'
            '[streamers."Example"] # Streamer settings\n'
            'sources = ["@Example", "kick:Example"]\n'
            'download_dir_name = "Original directory"\n'
            'powerchat_enabled = true\npowerchat_username = "Example"\n'
            'timezone = "Europe/London"\n'
            '# Preserve nested settings too.\n'
            '[streamers."Example".post_stream]\ntranscribe_subtitles = true\n'
            '[streamers."Example".voice_detection]\nmode = "fixed"\nspeakers = 2\n'
            '[streamers."Example".voices."Host"]\nsamples = ["host.wav"]\n'
            'notes = "Known voice"\n'
            '[[streamers."Example".stream_event_rules]]\nname = "Hype"\n'
            'keywords = ["wow"]\n'
        )
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.toml"
            path.write_text(original, encoding="utf-8")
            self.assertTrue(set_streamer_archived(path, "Example", True))
            archived_text = path.read_text(encoding="utf-8")
            archived = load_config(path)
            self.assertTrue(archived.streamers["Example"].archived)
            self.assertEqual(monitored_sources(archived), ["@Other"])
            self.assertFalse(set_streamer_archived(path, "Example", True))
            self.assertEqual(path.read_text(encoding="utf-8"), archived_text)
            self.assertTrue(set_streamer_archived(path, "Example", False))
            restored_text = path.read_text(encoding="utf-8")
            restored = load_config(path)

        self.assertEqual(archived_text.replace("archived = true\n", ""), original)
        self.assertEqual(restored_text.replace("archived = false\n", ""), original)
        self.assertEqual(
            monitored_sources(restored), ["@Other", "@Example", "kick:Example"]
        )
        before = tomllib.loads(original)
        after = tomllib.loads(restored_text)
        after["streamers"]["Example"].pop("archived")
        self.assertEqual(before, after)

    def test_archive_updates_existing_assignment_and_preserves_its_comment(self) -> None:
        for header in ("[streamers.Example]", '[streamers."Example"]',
                       "[streamers.'Example']"):
            with self.subTest(header=header), TemporaryDirectory() as tmp:
                path = Path(tmp) / "config.toml"
                original = (
                    f'{header}\nsources = ["@Example"]\n'
                    '"archived" = false  # Keep this comment\n'
                    '[streamers.Other]\nsources = ["@Other"]\n'
                )
                path.write_text(original, encoding="utf-8")
                self.assertTrue(set_streamer_archived(path, "Example", True))
                self.assertEqual(
                    path.read_text(encoding="utf-8"),
                    original.replace('"archived" = false', '"archived" = true'),
                )
                self.assertTrue(load_config(path).streamers["Example"].archived)
                self.assertFalse(load_config(path).streamers["Other"].archived)

    def test_ordinary_settings_save_preserves_archived_state_and_nested_tables(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.toml"
            path.write_text(
                '[streamers.Example]\nsources = ["@OldExample"]\narchived = true\n'
                '[streamers.Example.post_stream]\ntranscribe_subtitles = true\n',
                encoding="utf-8",
            )
            self.assertTrue(update_streamer_config(
                path, "Example", ["@NewExample"], timezone="Europe/London"
            ))
            config = load_config(path)
        streamer = config.streamers["Example"]
        self.assertTrue(streamer.archived)
        self.assertEqual(streamer.sources, ["@NewExample"])
        self.assertEqual(streamer.timezone, "Europe/London")
        self.assertIsNotNone(streamer.post_stream)
        self.assertTrue(streamer.post_stream.transcribe_subtitles)
        self.assertEqual(monitored_sources(config), [])

    def test_streamer_settings_can_explicitly_archive_or_restore(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.toml"
            path.write_text("channels = []\n", encoding="utf-8")
            update_streamer_config(path, "Example", ["@Example"], archived=True)
            self.assertTrue(load_config(path).streamers["Example"].archived)
            update_streamer_config(path, "Example", ["@Example"], archived=False)
            config = load_config(path)
            self.assertFalse(config.streamers["Example"].archived)
            self.assertEqual(monitored_sources(config), ["@Example"])

    def test_archive_leaves_table_and_assignment_text_inside_strings_untouched(self) -> None:
        original = (
            '[streamers.Example]\nsources = ["@Example"]\n'
            'description = """\narchived = false\n[streamers.Another]\n"""\n'
            'archived = false # Real setting\n'
            '[streamers.Example.voices.Host]\nsamples = ["host.wav"]\n'
        )
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.toml"
            path.write_text(original, encoding="utf-8")
            self.assertTrue(set_streamer_archived(path, "Example", True))
            self.assertEqual(
                path.read_text(encoding="utf-8"),
                original.replace("archived = false # Real setting", "archived = true # Real setting"),
            )
            self.assertTrue(load_config(path).streamers["Example"].archived)

    def test_archived_sources_are_excluded_without_changing_other_streamers(self) -> None:
        config = BotConfig(
            channels=["@Legacy"],
            streamers={
                "Archived": StreamerConfig(sources=["@Archived"], archived=True),
                "Active": StreamerConfig(sources=["@Active", "kick:Active"]),
            },
        )
        self.assertEqual(streamer_sources(config), ["@Active", "kick:Active"])
        self.assertEqual(monitored_sources(config), ["@Legacy", "@Active", "kick:Active"])
        self.assertEqual(config.streamers["Archived"].sources, ["@Archived"])

    def test_archived_legacy_aliases_are_filtered_with_platform_identity(self) -> None:
        config = BotConfig(
            channels=[
                "@Example", "https://www.youtube.com/@EXAMPLE/streams/",
                "youtube:Example", "Example", "https://kick.com/Other/",
                "https://www.twitch.tv/Third/", "twitch:Example", "@Remain",
            ],
            streamers={
                "Archived": StreamerConfig(
                    sources=["@Example", "kick:Other", "twitch:Third"], archived=True
                ),
            },
        )
        self.assertEqual(monitored_sources(config), ["twitch:Example", "@Remain"])

    def test_archiving_keeps_recording_group_and_post_stream_settings(self) -> None:
        config = load_config_text(
            '[streamers."Friendly name"]\nsources = ["@Example"]\n'
            'download_dir_name = "Saved directory"\narchived = true\n'
            '[streamers."Friendly name".post_stream]\ntranscribe_subtitles = true\n',
            Path("/tmp/config.toml"),
        )
        self.assertEqual(monitored_sources(config), [])
        owner = streamer_for_channel(config, "@Example")
        self.assertIsNotNone(owner)
        self.assertEqual(owner[0], "Friendly name")
        self.assertEqual(download_group_name_for_channel(config, "@Example"), "Saved directory")
        self.assertTrue(post_stream_config_for_channel(config, "@Example").transcribe_subtitles)

    def test_source_ownership_distinguishes_platforms_and_resolves_url_aliases(self) -> None:
        config = BotConfig(streamers={
            "Archived Kick": StreamerConfig(sources=["kick:Alice"], archived=True),
            "Active Twitch": StreamerConfig(sources=["twitch:Alice"]),
            "Active YouTube": StreamerConfig(sources=["@Alice"]),
        })
        for source, owner in (
            ("kick:alice", "Archived Kick"),
            ("https://kick.com/ALICE/", "Archived Kick"),
            ("twitch:alice", "Active Twitch"),
            ("https://www.twitch.tv/ALICE/", "Active Twitch"),
            ("youtube:Alice", "Active YouTube"),
            ("https://www.youtube.com/@ALICE/streams", "Active YouTube"),
        ):
            with self.subTest(source=source):
                match = streamer_for_source(config, source)
                self.assertIsNotNone(match)
                self.assertEqual(match[0], owner)
                self.assertIs(match[1], config.streamers[owner])

    def test_source_ownership_uses_platform_for_plain_channel_names(self) -> None:
        config = BotConfig(streamers={
            "Kick": StreamerConfig(sources=["kick:Alice"], archived=True),
            "Twitch": StreamerConfig(sources=["twitch:Alice"]),
            "YouTube": StreamerConfig(sources=["@Alice"]),
        })
        for platform, owner in (("kick", "Kick"), ("twitch", "Twitch"),
                                ("youtube", "YouTube"), (None, "YouTube")):
            with self.subTest(platform=platform):
                match = streamer_for_source(config, "Alice", default_platform=platform)
                self.assertIsNotNone(match)
                self.assertEqual(match[0], owner)

    def test_source_ownership_returns_none_for_missing_or_unsupported_sources(self) -> None:
        config = BotConfig(streamers={"Example": StreamerConfig(sources=["@Example"])})
        for source in ("", "   ", "@Unknown", "https://unsupported.invalid/Example"):
            with self.subTest(source=source):
                self.assertIsNone(streamer_for_source(config, source))

    def test_invalid_archive_requests_leave_config_untouched(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.toml"
            original = '[streamers.Example]\nsources = ["@Example"]\n'
            path.write_text(original, encoding="utf-8")
            for name, archived in (("Missing", True), ("", True), ("Example", "true")):
                with self.subTest(name=name, archived=archived), self.assertRaises(ConfigError):
                    set_streamer_archived(path, name, archived)
                self.assertEqual(path.read_text(encoding="utf-8"), original)


if __name__ == "__main__":
    unittest.main()
