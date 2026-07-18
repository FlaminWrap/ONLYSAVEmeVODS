from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from onlysavemevods.config import load_config, migrate_legacy_channels_to_streamer
from onlysavemevods.web import (
    ConfigRevisionConflict,
    app_config_updates_from_json_values,
    build_admin_static_snapshot,
    config_file_revision,
    render_admin_page,
    render_admin_settings,
    safe_return_to,
    update_app_config_from_json,
    update_streamer_from_json,
)


BASE_CONFIG = """channels = []
download_dir = "downloads"
state_dir = "state"
poll_interval_seconds = 60
max_concurrent_downloads = 4
web_enabled = true
web_host = "127.0.0.1"
web_port = 8080
"""


class DashboardUiTests(unittest.TestCase):
    def test_static_pages_use_sidebar_shell_and_packaged_assets(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.toml"
            config_path.write_text(BASE_CONFIG, encoding="utf-8")
            config = load_config(config_path)

            settings = render_admin_page(config, "settings", {})
            tools = render_admin_page(config, "tools", {})
            about = render_admin_page(config, "about", {})

        for html, page in ((settings, "settings"), (tools, "tools"), (about, "about")):
            self.assertIn(f'data-page="{page}"', html)
            self.assertIn('class="app-sidebar"', html)
            self.assertIn('/assets/dashboard.css', html)
            self.assertIn('/assets/dashboard.js', html)
            self.assertIn('aria-current="page"', html)

    def test_settings_are_guided_searchable_and_autosaved(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.toml"
            config_path.write_text(BASE_CONFIG, encoding="utf-8")
            config = load_config(config_path)
            snapshot = build_admin_static_snapshot(config)

            general = render_admin_settings(snapshot, "general")
            advanced = render_admin_settings(snapshot, "advanced")

        self.assertIn('data-autosave="config"', general)
        self.assertIn("Recording folder", general)
        self.assertIn("download_dir", general)
        self.assertIn("Changes save automatically", general)
        self.assertIn("Restart", general)
        self.assertIn("data-settings-search", advanced)
        self.assertIn("Additional yt-dlp arguments", advanced)

    def test_partial_config_update_validates_and_reports_restart(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.toml"
            config_path.write_text(BASE_CONFIG, encoding="utf-8")
            config = load_config(config_path)
            revision = config_file_revision(config)

            result = update_app_config_from_json(
                config,
                {
                    "revision": revision,
                    "values": {"poll_interval_seconds": "75", "web_port": "8081"},
                },
            )

            reloaded = load_config(config_path)
            self.assertEqual(reloaded.poll_interval_seconds, 75)
            self.assertEqual(reloaded.web_port, 8081)
            self.assertEqual(result["saved"], ["poll_interval_seconds", "web_port"])
            self.assertEqual(result["restart_required"], ["web_port"])
            self.assertNotEqual(result["revision"], revision)

    def test_stale_config_revision_is_rejected_without_writing(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.toml"
            config_path.write_text(BASE_CONFIG, encoding="utf-8")
            config = load_config(config_path)
            before = config_path.read_text(encoding="utf-8")

            with self.assertRaises(ConfigRevisionConflict):
                update_app_config_from_json(
                    config,
                    {"revision": "stale", "values": {"poll_interval_seconds": "75"}},
                )

            self.assertEqual(config_path.read_text(encoding="utf-8"), before)
            self.assertEqual(config.poll_interval_seconds, 60)

    def test_invalid_partial_config_value_is_rejected_without_writing(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.toml"
            config_path.write_text(BASE_CONFIG, encoding="utf-8")
            config = load_config(config_path)
            before = config_path.read_text(encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "must be at least 1"):
                update_app_config_from_json(
                    config,
                    {
                        "revision": config_file_revision(config),
                        "values": {"poll_interval_seconds": "0"},
                    },
                )

            self.assertEqual(config_path.read_text(encoding="utf-8"), before)

    def test_partial_update_replaces_multiline_root_array_without_losing_following_settings(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.toml"
            config_path.write_text(
                BASE_CONFIG
                + """\n# Verification schedule.
post_exit_check_seconds = [
  30, 60, 90,
  120,
]
retry_backoff_seconds = [30, 60, 120]
""",
                encoding="utf-8",
            )
            config = load_config(config_path)

            update_app_config_from_json(
                config,
                {
                    "revision": config_file_revision(config),
                    "values": {"post_exit_check_seconds": "15, 30, 45"},
                },
            )

            loaded = load_config(config_path)
            text = config_path.read_text(encoding="utf-8")
            self.assertEqual(loaded.post_exit_check_seconds, [15, 30, 45])
            self.assertEqual(loaded.retry_backoff_seconds, [30, 60, 120])
            self.assertIn("# Verification schedule.", text)

    def test_existing_streamer_basic_fields_can_be_autosaved(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.toml"
            config_path.write_text(
                BASE_CONFIG
                + '\n[streamers."Example"]\n'
                + 'sources = ["@Example"]\n',
                encoding="utf-8",
            )
            config = load_config(config_path)

            result = update_streamer_from_json(
                config,
                {
                    "revision": config_file_revision(config),
                    "streamer_name": "Example",
                    "values": {
                        "sources": "@Example\nkick:example",
                        "download_dir_name": "Example VODs",
                        "powerchat_enabled": True,
                        "powerchat_username": "example",
                    },
                },
            )

            streamer = load_config(config_path).streamers["Example"]
            self.assertEqual(streamer.sources, ["@Example", "kick:example"])
            self.assertEqual(streamer.download_dir_name, "Example VODs")
            self.assertTrue(streamer.powerchat_enabled)
            self.assertEqual(streamer.powerchat_username, "example")
            self.assertTrue(result["ok"])

    def test_legacy_migration_is_single_valid_config_write(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.toml"
            config_path.write_text(
                """# Keep this comment.
channels = [
  "@First",
  "kick:second",
]
download_dir = "downloads"
state_dir = "state"
""",
                encoding="utf-8",
            )

            changed = migrate_legacy_channels_to_streamer(
                config_path,
                "First",
                ["@First"],
                "First VODs",
            )

            loaded = load_config(config_path)
            text = config_path.read_text(encoding="utf-8")
            self.assertTrue(changed)
            self.assertEqual(loaded.channels, ["kick:second"])
            self.assertEqual(loaded.streamers["First"].sources, ["@First"])
            self.assertEqual(loaded.streamers["First"].download_dir_name, "First VODs")
            self.assertIn("# Keep this comment.", text)

    def test_json_config_parser_rejects_unknown_fields(self) -> None:
        with self.assertRaisesRegex(ValueError, "Unknown configuration setting"):
            app_config_updates_from_json_values({"made_up": "value"})

    def test_return_locations_are_limited_to_local_paths(self) -> None:
        self.assertEqual(safe_return_to("/streamers?selected=Example", "/"), "/streamers?selected=Example")
        self.assertEqual(safe_return_to("https://example.com", "/settings"), "/settings")
        self.assertEqual(safe_return_to("//example.com/path", "/settings"), "/settings")


if __name__ == "__main__":
    unittest.main()
