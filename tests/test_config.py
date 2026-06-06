from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from onlysavemevods.config import (
    DEFAULT_POST_EXIT_CHECK_SECONDS,
    ConfigError,
    append_missing_config_values,
    load_config,
)


class ConfigTests(unittest.TestCase):
    def test_append_missing_config_values_preserves_existing_options(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.toml"
            defaults_path = root / "config.example.toml"
            config_path.write_text(
                "# existing config\n"
                'channels = ["@Existing"]\n'
                "web_port = 9090\n",
                encoding="utf-8",
            )
            defaults_path.write_text(
                "channels = []\n"
                "web_port = 8080\n"
                "record_live_chat = false\n"
                "post_exit_check_seconds = [30, 60]\n"
                'ffmpeg_path = "ffmpeg"\n',
                encoding="utf-8",
            )

            added = append_missing_config_values(config_path, defaults_path)
            text = config_path.read_text(encoding="utf-8")
            config = load_config(config_path)

        self.assertEqual(
            added,
            ["record_live_chat", "post_exit_check_seconds", "ffmpeg_path"],
        )
        self.assertIn("# existing config", text)
        self.assertIn('channels = ["@Existing"]', text)
        self.assertIn("web_port = 9090", text)
        self.assertNotIn("web_port = 8080", text)
        self.assertIn("record_live_chat = false", text)
        self.assertIn("post_exit_check_seconds = [30, 60]", text)
        self.assertIn('ffmpeg_path = "ffmpeg"', text)
        self.assertEqual(config.channels, ["@Existing"])
        self.assertEqual(config.web_port, 9090)
        self.assertFalse(config.record_live_chat)
        self.assertEqual(config.post_exit_check_seconds, [30, 60])

    def test_append_missing_config_values_is_noop_when_complete(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.toml"
            defaults_path = root / "config.example.toml"
            original = 'channels = ["@Existing"]\nweb_port = 9090\n'
            config_path.write_text(original, encoding="utf-8")
            defaults_path.write_text(
                "channels = []\nweb_port = 8080\n",
                encoding="utf-8",
            )

            added = append_missing_config_values(config_path, defaults_path)
            text = config_path.read_text(encoding="utf-8")

        self.assertEqual(added, [])
        self.assertEqual(text, original)

    def test_defaults_include_requested_post_exit_schedule(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config = load_config(config_path)

        self.assertEqual(config.post_exit_check_seconds, DEFAULT_POST_EXIT_CHECK_SECONDS)
        self.assertEqual(config.post_exit_check_seconds[0], 30)
        self.assertEqual(config.post_exit_check_seconds[-1], 600)
        self.assertEqual(len(config.post_exit_check_seconds), 20)
        self.assertEqual(config.reconnect_interval_seconds, 0)
        self.assertEqual(config.channel_scan_limit, 10)
        self.assertEqual(config.discovery_probe_concurrency, 4)
        self.assertTrue(config.keep_fragments_for_resume)
        self.assertFalse(config.record_live_chat)
        self.assertFalse(config.render_live_chat_video)
        self.assertEqual(config.chat_render_panel_workers, 0)
        self.assertFalse(config.chat_render_use_nvenc)
        self.assertEqual(config.chat_render_nvenc_devices, [])
        self.assertFalse(config.transcribe_subtitles)
        self.assertEqual(config.transcription_max_concurrent, 1)
        self.assertEqual(config.whisperx_path, "whisperx")
        self.assertEqual(config.whisperx_model, "large-v3")
        self.assertEqual(config.whisperx_device, "cuda")
        self.assertEqual(config.whisperx_compute_type, "float16")
        self.assertEqual(config.whisperx_batch_size, 16)
        self.assertEqual(config.whisperx_language, "")
        self.assertTrue(config.whisperx_diarize)
        self.assertEqual(config.whisperx_hf_token_env, "HF_TOKEN")
        self.assertEqual(config.whisperx_min_speakers, 0)
        self.assertEqual(config.whisperx_max_speakers, 0)
        self.assertTrue(config.web_enabled)
        self.assertEqual(config.web_host, "127.0.0.1")
        self.assertEqual(config.web_port, 8080)
        self.assertEqual(config.log_level, "INFO")
        self.assertFalse(config.watermark_enabled)
        self.assertEqual(config.watermark_secret_env, "ONLYSAVEMEVODS_WATERMARK_SECRET")
        self.assertEqual(config.watermark_strength, "invisible")
        self.assertEqual(config.watermark_detect_upload_max_bytes, 2_147_483_648)

    def test_relative_paths_resolve_next_to_config(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "nested" / "config.toml"
            config_path.parent.mkdir()
            config_path.write_text(
                'channels = ["@Example"]\ndownload_dir = "dl"\nstate_dir = "st"\n',
                encoding="utf-8",
            )

            config = load_config(config_path)

        self.assertEqual(config.channels, ["@Example"])
        self.assertEqual(config.download_dir, (root / "nested" / "dl").resolve())
        self.assertEqual(config.state_dir, (root / "nested" / "st").resolve())

    def test_db_path_uses_legacy_database_when_new_database_is_absent(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.toml"
            state_dir = root / "state"
            state_dir.mkdir()
            legacy_db = state_dir / "ytdlbot.sqlite3"
            legacy_db.write_text("", encoding="utf-8")
            config_path.write_text('state_dir = "state"\n', encoding="utf-8")

            config = load_config(config_path)
            db_path = config.db_path

        self.assertEqual(db_path, legacy_db)

    def test_post_exit_schedule_must_be_increasing(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                "post_exit_check_seconds = [30, 30]\n",
                encoding="utf-8",
            )

            with self.assertRaises(ConfigError):
                load_config(config_path)

    def test_reconnect_interval_can_be_disabled(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text("reconnect_interval_seconds = 0\n", encoding="utf-8")

            config = load_config(config_path)

        self.assertEqual(config.reconnect_interval_seconds, 0)

    def test_reconnect_interval_can_be_enabled(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text("reconnect_interval_seconds = 60\n", encoding="utf-8")

            config = load_config(config_path)

        self.assertEqual(config.reconnect_interval_seconds, 60)

    def test_keep_fragments_for_resume_can_be_disabled(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                "keep_fragments_for_resume = false\n",
                encoding="utf-8",
            )

            config = load_config(config_path)

        self.assertFalse(config.keep_fragments_for_resume)

    def test_record_live_chat_can_be_enabled(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text("record_live_chat = true\n", encoding="utf-8")

            config = load_config(config_path)

        self.assertTrue(config.record_live_chat)

    def test_render_live_chat_video_can_be_enabled(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text("render_live_chat_video = true\n", encoding="utf-8")

            config = load_config(config_path)

        self.assertTrue(config.render_live_chat_video)

    def test_chat_render_panel_workers_can_be_configured(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text("chat_render_panel_workers = 6\n", encoding="utf-8")

            config = load_config(config_path)

        self.assertEqual(config.chat_render_panel_workers, 6)

    def test_chat_render_panel_workers_must_not_be_negative(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text("chat_render_panel_workers = -1\n", encoding="utf-8")

            with self.assertRaises(ConfigError):
                load_config(config_path)

    def test_chat_render_nvenc_can_be_configured(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                'chat_render_use_nvenc = true\nchat_render_nvenc_devices = ["0"]\n',
                encoding="utf-8",
            )

            config = load_config(config_path)

        self.assertTrue(config.chat_render_use_nvenc)
        self.assertEqual(config.chat_render_nvenc_devices, ["0"])

    def test_chat_render_nvenc_devices_can_be_numeric(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text("chat_render_nvenc_devices = 1\n", encoding="utf-8")

            config = load_config(config_path)

        self.assertEqual(config.chat_render_nvenc_devices, ["1"])

    def test_chat_render_nvenc_devices_can_be_configured(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                'chat_render_nvenc_devices = ["0", "1"]\n',
                encoding="utf-8",
            )

            config = load_config(config_path)

        self.assertEqual(config.chat_render_nvenc_devices, ["0", "1"])

    def test_chat_render_nvenc_devices_can_be_comma_separated(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                'chat_render_nvenc_devices = "0, 1"\n',
                encoding="utf-8",
            )

            config = load_config(config_path)

        self.assertEqual(config.chat_render_nvenc_devices, ["0", "1"])

    def test_chat_render_nvenc_devices_must_be_string_list_or_non_negative_int(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text("chat_render_nvenc_devices = -1\n", encoding="utf-8")

            with self.assertRaises(ConfigError):
                load_config(config_path)

    def test_transcription_settings_can_be_configured(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                "transcribe_subtitles = true\n"
                "transcription_max_concurrent = 2\n"
                'whisperx_path = "/opt/whisperx/bin/whisperx"\n'
                'whisperx_model = "medium"\n'
                'whisperx_device = "cuda:0"\n'
                'whisperx_compute_type = "int8_float16"\n'
                "whisperx_batch_size = 8\n"
                'whisperx_language = "en"\n'
                "whisperx_diarize = false\n"
                'whisperx_hf_token_env = "HUGGINGFACE_TOKEN"\n'
                "whisperx_min_speakers = 2\n"
                "whisperx_max_speakers = 4\n",
                encoding="utf-8",
            )

            config = load_config(config_path)

        self.assertTrue(config.transcribe_subtitles)
        self.assertEqual(config.transcription_max_concurrent, 2)
        self.assertEqual(config.whisperx_path, "/opt/whisperx/bin/whisperx")
        self.assertEqual(config.whisperx_model, "medium")
        self.assertEqual(config.whisperx_device, "cuda:0")
        self.assertEqual(config.whisperx_compute_type, "int8_float16")
        self.assertEqual(config.whisperx_batch_size, 8)
        self.assertEqual(config.whisperx_language, "en")
        self.assertFalse(config.whisperx_diarize)
        self.assertEqual(config.whisperx_hf_token_env, "HUGGINGFACE_TOKEN")
        self.assertEqual(config.whisperx_min_speakers, 2)
        self.assertEqual(config.whisperx_max_speakers, 4)

    def test_transcription_speaker_bounds_must_be_ordered(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                "whisperx_min_speakers = 4\nwhisperx_max_speakers = 2\n",
                encoding="utf-8",
            )

            with self.assertRaises(ConfigError):
                load_config(config_path)

    def test_whisperx_hf_token_env_must_not_be_token_value(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                'whisperx_hf_token_env = "hf_this_is_a_token_value"\n',
                encoding="utf-8",
            )

            with self.assertRaises(ConfigError):
                load_config(config_path)

    def test_whisperx_hf_token_env_must_be_env_var_name(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                'whisperx_hf_token_env = "HF-TOKEN"\n',
                encoding="utf-8",
            )

            with self.assertRaises(ConfigError):
                load_config(config_path)

    def test_web_settings_can_be_configured(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                'web_enabled = false\nweb_host = "0.0.0.0"\nweb_port = 9090\n',
                encoding="utf-8",
            )

            config = load_config(config_path)

        self.assertFalse(config.web_enabled)
        self.assertEqual(config.web_host, "0.0.0.0")
        self.assertEqual(config.web_port, 9090)

    def test_web_port_must_be_valid(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text("web_port = 70000\n", encoding="utf-8")

            with self.assertRaises(ConfigError):
                load_config(config_path)

    def test_log_level_can_be_configured(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text('log_level = "debug"\n', encoding="utf-8")

            config = load_config(config_path)

        self.assertEqual(config.log_level, "DEBUG")

    def test_log_level_must_be_valid(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text('log_level = "chatty"\n', encoding="utf-8")

            with self.assertRaises(ConfigError):
                load_config(config_path)

    def test_extra_args_cannot_disable_media_downloads(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                'extra_yt_dlp_args = ["--skip-download"]\n',
                encoding="utf-8",
            )

            with self.assertRaises(ConfigError):
                load_config(config_path)

    def test_watermark_settings_can_be_configured(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                "watermark_enabled = true\n"
                'watermark_secret_env = "CUSTOM_WATERMARK_SECRET"\n'
                'watermark_strength = "balanced"\n'
                "watermark_detect_upload_max_bytes = 12345\n",
                encoding="utf-8",
            )

            config = load_config(config_path)

        self.assertTrue(config.watermark_enabled)
        self.assertEqual(config.watermark_secret_env, "CUSTOM_WATERMARK_SECRET")
        self.assertEqual(config.watermark_strength, "balanced")
        self.assertEqual(config.watermark_detect_upload_max_bytes, 12345)

    def test_watermark_strength_must_be_valid(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text('watermark_strength = "loud"\n', encoding="utf-8")

            with self.assertRaises(ConfigError):
                load_config(config_path)
