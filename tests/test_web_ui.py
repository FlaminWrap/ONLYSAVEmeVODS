from __future__ import annotations

from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from onlysavemevods.config import BotConfig, ConfigError, load_config, migrate_legacy_channels_to_streamer
from onlysavemevods.models import LiveStream
from onlysavemevods.state import StateStore
from onlysavemevods.web_ui import (
    NAVIGATION_ICONS,
    NAVIGATION_ITEMS,
    dashboard_asset_revision,
)
from onlysavemevods.web import (
    ConfigRevisionConflict,
    app_config_updates_from_json_values,
    build_admin_static_snapshot,
    build_handler,
    build_status_snapshot,
    config_file_revision,
    render_admin_fragment,
    render_admin_page,
    render_admin_settings,
    render_admin_streamer_powerchat,
    safe_return_to,
    update_app_config_from_json,
    update_streamer_from_form,
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
    def test_old_status_route_redirects_to_current_dashboard(self) -> None:
        class NonClosingBuffer(BytesIO):
            def close(self) -> None:
                pass

        class Request:
            def __init__(self) -> None:
                self.input = NonClosingBuffer(
                    b"GET /status HTTP/1.1\r\nHost: localhost\r\n\r\n"
                )
                self.output = NonClosingBuffer()

            def makefile(self, mode: str, buffering: int | None = None) -> NonClosingBuffer:
                return self.input

            def sendall(self, data: bytes) -> None:
                self.output.write(data)

        class Server:
            server_name = "localhost"
            server_port = 8080

        request = Request()
        build_handler(BotConfig())(request, ("127.0.0.1", 1), Server())
        response = request.output.getvalue().decode("latin-1")

        self.assertIn(" 308 Permanent Redirect\r\n", response)
        self.assertIn("Location: /\r\n", response)

    def test_tools_navigation_uses_pipe_wrench_icon(self) -> None:
        tools_item = next(item for item in NAVIGATION_ITEMS if item.key == "tools")

        self.assertEqual(tools_item.icon, "pipe_wrench")
        self.assertIn('viewBox="0 0 194.799 194.799"', NAVIGATION_ICONS[tools_item.icon])
        self.assertIn('data-tool-style="pipe-wrench"', NAVIGATION_ICONS[tools_item.icon])

    def test_autosave_uses_form_action_attribute_not_named_control(self) -> None:
        script = (
            Path(__file__).parents[1]
            / "src"
            / "onlysavemevods"
            / "assets"
            / "dashboard.js"
        ).read_text(encoding="utf-8")

        self.assertIn('form.getAttribute("action")', script)
        self.assertNotIn("fetch(form.action", script)

    def test_fragment_refresh_preserves_expanded_details(self) -> None:
        script = (
            Path(__file__).parents[1]
            / "src"
            / "onlysavemevods"
            / "assets"
            / "dashboard.js"
        ).read_text(encoding="utf-8")

        capture = script.index("const detailsState = captureDetailsState(region);")
        replace = script.index("region.innerHTML = html;")
        restore = script.index("restoreDetailsState(region, detailsState);")
        self.assertLess(capture, replace)
        self.assertLess(replace, restore)
        self.assertIn('details[data-details-key]', script)

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
            self.assertIn('class="nav-icon"', html)
            self.assertIn('<svg viewBox="0 0 24 24"', html)
            self.assertIn(
                f'/assets/dashboard.css?v={dashboard_asset_revision()}',
                html,
            )
            self.assertIn('aria-current="page"', html)

    def test_settings_are_guided_searchable_and_autosaved(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.toml"
            config_path.write_text(BASE_CONFIG, encoding="utf-8")
            config = load_config(config_path)
            snapshot = build_admin_static_snapshot(config)

            general = render_admin_settings(snapshot, "general")
            recording = render_admin_settings(snapshot, "recording")
            advanced = render_admin_settings(snapshot, "advanced")

        self.assertIn('data-autosave="config"', general)
        self.assertIn("Recording folder", general)
        self.assertIn("download_dir", general)
        self.assertIn("Changes save automatically", general)
        self.assertIn("Restart", general)
        self.assertIn("Clear ended-stream fragments after", recording)
        self.assertIn("fragment_retention_hours", recording)
        self.assertIn("(hours)", recording)
        self.assertIn("data-settings-search", advanced)
        self.assertIn("Additional yt-dlp arguments", advanced)
        self.assertIn("Create subtitles automatically", advanced)

    def test_after_stream_controls_are_per_streamer_and_inherit_defaults(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.toml"
            config_path.write_text(
                BASE_CONFIG
                + "\ntranscribe_subtitles = false\n"
                + '[streamers."Example"]\n'
                + 'sources = ["@Example"]\n',
                encoding="utf-8",
            )
            config = load_config(config_path)

            html = render_admin_page(
                config,
                "streamers",
                {"selected": ["Example"], "tab": ["settings"]},
            )

            result = update_streamer_from_json(
                config,
                {
                    "revision": config_file_revision(config),
                    "form_kind": "streamer-post-stream",
                    "streamer_name": "Example",
                    "values": {
                        "twitch_ad_repair_enabled": "disabled",
                        "transcribe_subtitles": "enabled",
                        "voice_match_enabled": "inherit",
                        "stream_event_detection_enabled": "enabled",
                        "render_live_chat_video": "disabled",
                    },
                },
            )
            post_stream = load_config(config_path).streamers["Example"].post_stream
            update_streamer_from_form(
                config,
                {
                    "form_kind": ["streamer_post_stream"],
                    "streamer_name": ["Example"],
                    "twitch_ad_repair_enabled": ["inherit"],
                    "transcribe_subtitles": ["inherit"],
                    "voice_match_enabled": ["inherit"],
                    "stream_event_detection_enabled": ["inherit"],
                    "render_live_chat_video": ["inherit"],
                },
            )
            inherited = load_config(config_path).streamers["Example"].post_stream

        self.assertIn("After a stream", html)
        self.assertIn('data-autosave="streamer-post-stream"', html)
        self.assertIn("App default (Off)", html)
        self.assertIn("Always run", html)
        self.assertIn("Never run", html)
        self.assertIn('class="active" href="/streamers?selected=Example&amp;tab=settings"', html)
        self.assertNotIn('fragment=streams', html)
        self.assertTrue(result["ok"])
        self.assertIsNotNone(post_stream)
        assert post_stream is not None
        self.assertFalse(post_stream.twitch_ad_repair_enabled)
        self.assertTrue(post_stream.transcribe_subtitles)
        self.assertIsNone(post_stream.voice_match_enabled)
        self.assertTrue(post_stream.stream_event_detection_enabled)
        self.assertFalse(post_stream.render_live_chat_video)
        self.assertIsNone(inherited)

    def test_optional_streamer_managers_are_migrated_into_current_dashboard(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.toml"
            config_path.write_text(
                BASE_CONFIG
                + '\n[streamers."Example"]\n'
                + 'sources = ["kick:example"]\n',
                encoding="utf-8",
            )
            config = load_config(config_path)

            html = render_admin_page(
                config,
                "streamers",
                {"selected": ["Example"], "tab": ["settings"]},
            )

        self.assertIn('id="voices"', html)
        self.assertIn('action="/streamer-voices"', html)
        self.assertIn('id="speaker-names"', html)
        self.assertIn('action="/speaker-labels"', html)
        self.assertIn('id="content-events"', html)
        self.assertIn('action="/stream-event-rules"', html)
        self.assertIn('id="manual-vod"', html)
        self.assertIn('action="/vod-download"', html)
        self.assertNotIn('/status#streamers', html)

    def test_powerchat_uses_packaged_dashboard_script(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.toml"
            config_path.write_text(BASE_CONFIG, encoding="utf-8")
            config = load_config(config_path)

            html = render_admin_page(config, "powerchat", {})

        self.assertIn('/assets/dashboard.js', html)
        self.assertIn('id="powerchat-stats-json"', html)
        self.assertNotIn('const tabKey = "onlysavemevods.dashboardTab"', html)

    def test_streamer_history_is_paginated_in_the_new_dashboard(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.toml"
            config_path.write_text(
                BASE_CONFIG
                + '\n[streamers."Example"]\n'
                + 'sources = ["kick:example"]\n',
                encoding="utf-8",
            )
            config = load_config(config_path)
            state = StateStore(config.db_path)
            for index in range(12):
                state.upsert_vod_stream(
                    LiveStream(
                        video_id=f"kick:example:{index:02d}",
                        url=f"https://kick.com/example?stream={index}",
                        title=f"Example stream {index:02d}",
                        channel="example",
                        platform="kick",
                        source="kick:example",
                    )
                )
            state.close()

            first_page = render_admin_page(
                config,
                "streamers",
                {"selected": ["Example"]},
            )
            second_page = render_admin_page(
                config,
                "streamers",
                {"selected": ["Example"], "page": ["2"]},
            )
            fragment, _revision = render_admin_fragment(
                config,
                "streamers",
                {"selected": ["Example"], "page": ["2"]},
            )

        self.assertIn("Showing 1–10 of 12", first_page)
        self.assertIn("Page 1 of 2", first_page)
        self.assertIn('rel="next"', first_page)
        self.assertNotIn("Open complete history", first_page)
        self.assertNotIn("compatibility workspace for the complete", first_page)
        self.assertIn('data-details-key="stream:kick:example:00"', first_page)
        self.assertIn('data-details-key="stream:kick:example:00:jobs"', first_page)
        self.assertIn('class="stream-subsection processing-jobs-section"', first_page)
        self.assertIn("Showing 11–12 of 12", second_page)
        self.assertIn("Page 2 of 2", second_page)
        self.assertIn('rel="prev"', second_page)
        self.assertIn("Showing 11–12 of 12", fragment)

    def test_streamer_history_uses_historical_timezone_and_dst(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.toml"
            config_path.write_text(
                BASE_CONFIG
                + '\n[streamers."Example"]\n'
                + 'sources = ["kick:example"]\n'
                + 'timezone = "America/Los_Angeles"\n',
                encoding="utf-8",
            )
            config = load_config(config_path)
            state = StateStore(config.db_path)
            streams = (
                ("winter", "Winter stream", "2026-01-15T12:00:00+00:00"),
                ("summer", "Summer stream", "2026-07-15T12:00:00+00:00"),
                ("previous-day", "Previous local day", "2026-01-15T07:30:00+00:00"),
            )
            for video_id, title, timestamp in streams:
                state.upsert_vod_stream(
                    LiveStream(
                        video_id=video_id,
                        url=f"https://kick.com/example?stream={video_id}",
                        title=title,
                        channel="example",
                        platform="kick",
                        source="kick:example",
                    )
                )
                state.conn.execute(
                    """
                    UPDATE streams
                    SET first_seen_at = ?, updated_at = ?, last_started_at = ?,
                        last_exit_at = ?
                    WHERE video_id = ?
                    """,
                    (timestamp, timestamp, timestamp, timestamp, video_id),
                )
                state.conn.execute(
                    "UPDATE stream_events SET created_at = ? WHERE video_id = ?",
                    (timestamp, video_id),
                )
            state.conn.commit()
            state.close()

            html = render_admin_page(
                config,
                "streamers",
                {"selected": ["Example"]},
            )
            filtered = render_admin_page(
                config,
                "streamers",
                {"selected": ["Example"], "from": ["2026-01-15"]},
            )

        self.assertIn("Times in America/Los_Angeles", html)
        self.assertIn("2026-01-15 04:00:00 PST", html)
        self.assertIn("2026-07-15 05:00:00 PDT", html)
        self.assertIn("Winter stream", filtered)
        self.assertNotIn("Previous local day", filtered)

    def test_streamer_overview_and_settings_are_separate_tabs(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.toml"
            config_path.write_text(
                BASE_CONFIG
                + '\n[streamers."Example"]\n'
                + 'sources = ["kick:example"]\n'
                + 'timezone = "Europe/London"\n',
                encoding="utf-8",
            )
            config = load_config(config_path)

            overview = render_admin_page(
                config,
                "streamers",
                {"selected": ["Example"]},
            )
            settings = render_admin_page(
                config,
                "streamers",
                {"selected": ["Example"], "tab": ["settings"]},
            )

        self.assertIn('aria-label="Streamer sections"', overview)
        self.assertIn('class="active" href="/streamers?selected=Example"', overview)
        self.assertIn('data-autosave="streamer"', overview)
        self.assertNotIn('name="powerchat_username"', overview)
        self.assertNotIn('name="timezone"', overview)
        self.assertIn("Streams", overview)
        self.assertNotIn("After a stream", overview)
        self.assertNotIn("Optional features", overview)
        self.assertIn("After a stream", settings)
        self.assertIn("Optional features", settings)
        self.assertIn("Streamer settings", settings)
        self.assertIn('data-autosave="streamer"', settings)
        self.assertIn('<select id="streamer-timezone-Example" name="timezone">', settings)
        self.assertIn('<option value="Europe/London" selected>Europe/London</option>', settings)
        self.assertIn('label="Europe"', settings)
        self.assertIn('data-autosave="streamer-post-stream"', settings)

    def test_streamer_powerchat_tab_has_charts_and_empty_state(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.toml"
            config_path.write_text(
                BASE_CONFIG
                + '\n[streamers."Example"]\n'
                + 'sources = ["kick:example"]\n'
                + 'powerchat_enabled = true\n'
                + 'powerchat_username = "example"\n'
                + 'timezone = "Europe/London"\n',
                encoding="utf-8",
            )
            config = load_config(config_path)
            snapshot = build_status_snapshot(config, include_speaker_scan=False)
            streamer = snapshot.streamer_stats[0]
            empty_page = render_admin_page(
                config,
                "streamers",
                {"selected": ["Example"], "tab": ["powerchat"]},
            )
            stats = {
                "streamer_dashboards": [
                    {
                        "streamer": "Example",
                        "event_count": 3,
                        "stream_count": 1,
                        "money_totals": [{"currency": "USD", "amount": 25.0}],
                        "unit_totals": [],
                        "money_rates": [{"currency": "USD", "amount_per_hour": 12.5}],
                        "events_without_offset": 0,
                        "top_donors": [
                            {"donor": "Alice", "event_count": 2, "money_totals": [{"currency": "USD", "amount": 20.0}], "unit_totals": [], "latest_received_at": "2026-07-18T12:00:00+00:00"},
                        ],
                        "hourly_totals": [
                            {"hour_label": "0:00-0:59", "event_count": 3, "money_totals": [{"currency": "USD", "amount": 25.0}], "unit_totals": []},
                        ],
                        "stream_totals": [],
                    }
                ],
                "events": [
                    {"streamer": "Example", "stream_title": "Launch stream", "donor": "Alice", "kind": "money", "money_amount": 20.0, "money_currency": "USD", "platform": "Powerchat", "message": "Great stream", "received_at": "2026-07-18T12:00:00+00:00", "offset_seconds": 90.0},
                ],
            }
            dashboard = render_admin_streamer_powerchat(streamer, stats)

        self.assertIn('class="active" href="/streamers?selected=Example&amp;tab=powerchat"', empty_page)
        self.assertIn("No Powerchat activity yet", empty_page)
        self.assertNotIn("fragment=streams", empty_page)
        self.assertIn('data-autosave="streamer"', empty_page)
        self.assertIn('name="powerchat_username" value="example"', empty_page)
        self.assertNotIn('name="timezone"', empty_page)
        self.assertNotIn("Use mine", empty_page)
        self.assertNotIn("Configure listener", empty_page)
        self.assertIn("Activity by stream hour", dashboard)
        self.assertIn("Most active supporters", dashboard)
        self.assertIn("Alice", dashboard)
        self.assertIn("Launch stream", dashboard)
        self.assertIn("Download CSV", dashboard)
        self.assertIn("Received (Europe/London)", dashboard)
        self.assertIn("2026-07-18 13:00:00 BST", dashboard)
        self.assertIn("<td>1:30</td>", dashboard)

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

    def test_config_preview_does_not_create_a_sibling_temp_file(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.toml"
            config_path.write_text(
                BASE_CONFIG.replace("channels = []", 'channels = ["@OUMB3rd"]'),
                encoding="utf-8",
            )
            config = load_config(config_path)

            with patch(
                "onlysavemevods.web.tempfile.NamedTemporaryFile",
                side_effect=OSError(30, "Read-only file system"),
            ):
                result = update_app_config_from_json(
                    config,
                    {
                        "revision": config_file_revision(config),
                        "values": {"channels": ""},
                    },
                )
            reloaded = load_config(config_path)

        self.assertTrue(result["ok"])
        self.assertEqual(reloaded.channels, [])

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
                        "timezone": "Europe/London",
                    },
                },
            )

            streamer = load_config(config_path).streamers["Example"]
            self.assertEqual(streamer.sources, ["@Example", "kick:example"])
            self.assertEqual(streamer.download_dir_name, "Example VODs")
            self.assertTrue(streamer.powerchat_enabled)
            self.assertEqual(streamer.powerchat_username, "example")
            self.assertEqual(streamer.timezone, "Europe/London")
            self.assertTrue(result["ok"])

    def test_invalid_streamer_timezone_does_not_modify_configuration(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.toml"
            config_path.write_text(
                BASE_CONFIG
                + '\n[streamers."Example"]\n'
                + 'sources = ["kick:example"]\n',
                encoding="utf-8",
            )
            config = load_config(config_path)
            before = config_path.read_text(encoding="utf-8")

            with self.assertRaisesRegex(ConfigError, "valid IANA time zone"):
                update_streamer_from_json(
                    config,
                    {
                        "revision": config_file_revision(config),
                        "streamer_name": "Example",
                        "values": {"timezone": "Not/A_Real_Zone"},
                    },
                )

            self.assertEqual(config_path.read_text(encoding="utf-8"), before)
            self.assertEqual(config.streamers["Example"].timezone, "UTC")

    def test_listener_form_fallback_saves_without_overwriting_streamer_sources(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.toml"
            config_path.write_text(
                BASE_CONFIG
                + '\n[streamers."Example"]\n'
                + 'sources = ["kick:example"]\n'
                + 'download_dir_name = "Example VODs"\n'
                + 'powerchat_enabled = true\n',
                encoding="utf-8",
            )
            config = load_config(config_path)

            update_streamer_from_form(
                config,
                {
                    "form_kind": ["streamer_form"],
                    "action": ["save"],
                    "streamer_name": ["Example"],
                    "powerchat_enabled": ["false"],
                    "powerchat_username": ["example"],
                    "timezone": ["America/New_York"],
                },
            )

            streamer = load_config(config_path).streamers["Example"]
            self.assertEqual(streamer.sources, ["kick:example"])
            self.assertEqual(streamer.download_dir_name, "Example VODs")
            self.assertFalse(streamer.powerchat_enabled)
            self.assertEqual(streamer.powerchat_username, "example")
            self.assertEqual(streamer.timezone, "America/New_York")

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
