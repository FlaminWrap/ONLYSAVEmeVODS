from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from onlysavemevods.config import (
    DEFAULT_POST_EXIT_CHECK_SECONDS,
    ConfigError,
    StreamEventDetectionConfig,
    StreamEventRuleConfig,
    VoiceDetectionConfig,
    VoiceProfileConfig,
    append_missing_config_values,
    download_group_name_for_channel,
    load_config,
    monitored_sources,
    streamer_display_name_for_channel,
    update_channel_speaker_labels_config,
    update_channel_voice_detection_config,
    remove_streamer_config,
    update_global_stream_event_rules_config,
    update_config_values,
    update_streamer_config,
    update_streamer_speaker_labels_config,
    update_streamer_stream_event_config,
    update_streamer_voice_detection_config,
    update_streamer_voice_profile_config,
    sanitize_voice_sample_filename,
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
            [
                "record_live_chat",
                "post_exit_check_seconds",
                "ffmpeg_path",
            ],
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

    def test_app_update_settings_parse_and_validate(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.toml"
            config_path.write_text(
                'app_update_mode = "auto_install"\n'
                'app_update_repository = "Example/Repo.git"\n'
                'app_update_include_prereleases = true\n'
                'app_update_github_token_env = "ONLYSAVE_GITHUB_TOKEN"\n',
                encoding="utf-8",
            )

            config = load_config(config_path)

        self.assertEqual(config.app_update_mode, "auto_install")
        self.assertEqual(config.app_update_repository, "Example/Repo")
        self.assertTrue(config.app_update_include_prereleases)
        self.assertEqual(config.app_update_github_token_env, "ONLYSAVE_GITHUB_TOKEN")

    def test_app_update_rejects_invalid_mode_and_repository(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.toml"
            config_path.write_text('app_update_mode = "sometimes"\n', encoding="utf-8")

            with self.assertRaises(ConfigError):
                load_config(config_path)

            config_path.write_text(
                'app_update_repository = "https://github.com/Example/Repo"\n',
                encoding="utf-8",
            )

            with self.assertRaises(ConfigError):
                load_config(config_path)

    def test_append_missing_config_values_inserts_root_values_before_tables(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.toml"
            defaults_path = root / "config.example.toml"
            config_path.write_text(
                'channels = ["@Existing"]\n'
                '\n'
                '[streamers."OUMB3rd"]\n'
                'sources = ["@OUMB3rd"]\n'
                '\n'
                '[streamers."OUMB3rd".voices."Host"]\n'
                'enabled = true\n'
                'samples = []\n',
                encoding="utf-8",
            )
            defaults_path.write_text(
                "channels = []\n"
                "voice_match_enabled = true\n"
                'voice_match_model = "pyannote/embedding"\n'
                '\n'
                '[streamers."Example"]\n'
                'sources = ["@Example"]\n',
                encoding="utf-8",
            )

            added = append_missing_config_values(config_path, defaults_path)
            text = config_path.read_text(encoding="utf-8")
            config = load_config(config_path)

        self.assertEqual(added, ["voice_match_enabled", "voice_match_model"])
        self.assertLess(
            text.index("voice_match_enabled = true"),
            text.index('[streamers."OUMB3rd"]'),
        )
        self.assertLess(
            text.index('voice_match_model = "pyannote/embedding"'),
            text.index('[streamers."OUMB3rd"]'),
        )
        self.assertTrue(config.voice_match_enabled)
        self.assertEqual(config.voice_match_model, "pyannote/embedding")
        self.assertIn("OUMB3rd", config.streamers)
        self.assertIn("Host", config.streamers["OUMB3rd"].voices)

    def test_append_missing_config_values_repairs_misplaced_root_values(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.toml"
            defaults_path = root / "config.example.toml"
            config_path.write_text(
                'channels = ["@Existing"]\n'
                '\n'
                '[streamers."OUMB3rd"]\n'
                'sources = ["@OUMB3rd"]\n'
                '\n'
                '# Added by ONLYSAVEmeVODS config update. Existing settings above were left unchanged.\n'
                'voice_match_enabled = true\n'
                'voice_match_model = "pyannote/embedding"\n'
                'voice_match_model = "pyannote/embedding"\n'
                'voice_match_threshold = 0.35\n'
                'voice_match_min_margin = 0.05\n'
                'voice_sample_max_bytes = 104857600\n',
                encoding="utf-8",
            )
            defaults_path.write_text(
                "channels = []\n"
                "voice_match_enabled = true\n"
                'voice_match_model = "pyannote/embedding"\n'
                "voice_match_threshold = 0.35\n"
                "voice_match_min_margin = 0.05\n"
                "voice_sample_max_bytes = 104857600\n",
                encoding="utf-8",
            )

            changed = append_missing_config_values(config_path, defaults_path)
            text = config_path.read_text(encoding="utf-8")
            config = load_config(config_path)

        self.assertEqual(
            changed,
            [
                "voice_match_enabled",
                "voice_match_model",
                "voice_match_threshold",
                "voice_match_min_margin",
                "voice_sample_max_bytes",
            ],
        )
        self.assertLess(
            text.index("voice_match_enabled = true"),
            text.index('[streamers."OUMB3rd"]'),
        )
        self.assertEqual(text.count('voice_match_model = "pyannote/embedding"'), 1)
        self.assertTrue(config.voice_match_enabled)
        self.assertEqual(config.voice_sample_max_bytes, 104_857_600)
        self.assertIn("OUMB3rd", config.streamers)

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
        self.assertEqual(config.chat_render_timeout_seconds, 3600)
        self.assertFalse(config.chat_render_use_nvenc)
        self.assertEqual(config.chat_render_nvenc_devices, [])
        self.assertEqual(config.chat_emoji_cache_dir, config.state_dir / "chat_emoji_cache")
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
        self.assertTrue(config.voice_match_enabled)
        self.assertEqual(config.voice_match_model, "pyannote/embedding")
        self.assertEqual(config.voice_match_threshold, 0.35)
        self.assertEqual(config.voice_match_min_margin, 0.05)
        self.assertEqual(config.voice_sample_max_bytes, 104_857_600)
        self.assertFalse(config.stream_event_detection_enabled)
        self.assertEqual(config.stream_event_model, "MIT/ast-finetuned-audioset-10-10-0.4593")
        self.assertEqual(config.stream_event_device, "auto")
        self.assertEqual(config.stream_event_window_seconds, 10.0)
        self.assertEqual(config.stream_event_hop_seconds, 5.0)
        self.assertEqual(config.stream_event_min_confidence, 0.35)
        self.assertEqual(config.stream_event_max_events_per_media, 100)
        self.assertEqual(config.stream_event_rules, [])

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

    def test_db_path_uses_onlysavemevods_database(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.toml"
            state_dir = root / "state"
            state_dir.mkdir()
            config_path.write_text('state_dir = "state"\n', encoding="utf-8")

            config = load_config(config_path)
            db_path = config.db_path

        self.assertEqual(db_path, state_dir / "onlysavemevods.sqlite3")

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

    def test_chat_render_timeout_can_be_configured(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text("chat_render_timeout_seconds = 7200\n", encoding="utf-8")

            config = load_config(config_path)

        self.assertEqual(config.chat_render_timeout_seconds, 7200)

    def test_chat_render_timeout_can_be_disabled(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text("chat_render_timeout_seconds = 0\n", encoding="utf-8")

            config = load_config(config_path)

        self.assertEqual(config.chat_render_timeout_seconds, 0)

    def test_chat_render_timeout_must_not_be_negative(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text("chat_render_timeout_seconds = -1\n", encoding="utf-8")

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

    def test_streamers_group_sources_and_shared_settings(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                'channels = ["@Legacy"]\n'
                '[streamers."OUMB3rd"]\n'
                'sources = ["@OUMB3rd", "https://www.youtube.com/@OUMB3rdVODS"]\n'
                'download_dir_name = "OUMB3rd Shared"\n'
                '[streamers."OUMB3rd".voice_detection]\n'
                'mode = "fixed"\n'
                'speakers = 2\n'
                'hf_token_env = "PYANNOTE_TOKEN"\n'
                '[streamers."OUMB3rd".speaker_labels]\n'
                'SPEAKER_00 = "OUMB3rd"\n',
                encoding="utf-8",
            )

            config = load_config(config_path)

        self.assertEqual(
            monitored_sources(config),
            ["@Legacy", "@OUMB3rd", "https://www.youtube.com/@OUMB3rdVODS"],
        )
        self.assertEqual(streamer_display_name_for_channel(config, "OUMB3rd VODS"), "OUMB3rd")
        self.assertEqual(
            download_group_name_for_channel(config, "OUMB3rd VODS"),
            "OUMB3rd Shared",
        )
        streamer = config.streamers["OUMB3rd"]
        self.assertEqual(streamer.sources, ["@OUMB3rd", "https://www.youtube.com/@OUMB3rdVODS"])
        self.assertIsNotNone(streamer.voice_detection)
        assert streamer.voice_detection is not None
        self.assertEqual(streamer.voice_detection.mode, "fixed")
        self.assertEqual(streamer.voice_detection.min_speakers, 2)
        self.assertEqual(streamer.voice_detection.hf_token_env, "PYANNOTE_TOKEN")
        self.assertEqual(streamer.speaker_labels, {"SPEAKER_00": "OUMB3rd"})

    def test_supported_platform_sources_are_validated(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                'channels = ["twitch:OUMB3rd", "kick:OUMB3rd", "https://rumble.com/vabc-title.html"]\n',
                encoding="utf-8",
            )

            config = load_config(config_path)

        self.assertEqual(
            monitored_sources(config),
            ["twitch:OUMB3rd", "kick:OUMB3rd", "https://rumble.com/vabc-title.html"],
        )

    def test_unsupported_platform_source_fails_config_load(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text('channels = ["trovo:OUMB3rd"]\n', encoding="utf-8")

            with self.assertRaises(ConfigError):
                load_config(config_path)

    def test_prefixed_streamer_source_matches_detected_channel_name(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                '[streamers."OUMB3rd"]\n'
                'sources = ["twitch:OUMB3rd"]\n'
                'download_dir_name = "OUMB3rd Shared"\n',
                encoding="utf-8",
            )
            config = load_config(config_path)

        self.assertEqual(streamer_display_name_for_channel(config, "OUMB3rd"), "OUMB3rd")
        self.assertEqual(
            download_group_name_for_channel(config, "OUMB3rd"),
            "OUMB3rd Shared",
        )

    def test_streamer_config_update_writes_updates_and_removes_group(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text("channels = []\n", encoding="utf-8")

            created = update_streamer_config(
                config_path,
                "OUMB3rd",
                ["@OUMB3rd", "@OUMB3rdVODS"],
                "OUMB3rd Shared",
            )
            update_streamer_voice_detection_config(
                config_path,
                "OUMB3rd",
                VoiceDetectionConfig(mode="fixed", min_speakers=2, max_speakers=2),
            )
            updated = update_streamer_config(
                config_path,
                "OUMB3rd",
                ["@OUMB3rd"],
                "",
            )
            config = load_config(config_path)
            removed = remove_streamer_config(config_path, "OUMB3rd")
            config_after_remove = load_config(config_path)
            text_after_remove = config_path.read_text(encoding="utf-8")

        self.assertTrue(created)
        self.assertTrue(updated)
        self.assertEqual(config.streamers["OUMB3rd"].sources, ["@OUMB3rd"])
        self.assertEqual(config.streamers["OUMB3rd"].download_dir_name, "")
        self.assertIsNotNone(config.streamers["OUMB3rd"].voice_detection)
        self.assertTrue(removed)
        self.assertEqual(config_after_remove.streamers, {})
        self.assertNotIn("OUMB3rd", text_after_remove)

    def test_streamer_config_reads_and_writes_powerchat_settings(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                '[streamers."OUMB3rd"]\n'
                'sources = ["kick:oumb"]\n'
                "powerchat_enabled = true\n"
                'powerchat_username = "oumb"\n',
                encoding="utf-8",
            )
            config = load_config(config_path)

            updated = update_streamer_config(
                config_path,
                "OUMB3rd",
                ["kick:oumb", "@OUMB3rd"],
                "OUMB3rd Shared",
                powerchat_enabled=False,
                powerchat_username="",
            )
            updated_config = load_config(config_path)
            updated_text = config_path.read_text(encoding="utf-8")

        self.assertTrue(config.streamers["OUMB3rd"].powerchat_enabled)
        self.assertEqual(config.streamers["OUMB3rd"].powerchat_username, "oumb")
        self.assertTrue(updated)
        self.assertFalse(updated_config.streamers["OUMB3rd"].powerchat_enabled)
        self.assertEqual(updated_config.streamers["OUMB3rd"].powerchat_username, "")
        self.assertNotIn("powerchat_enabled", updated_text)
        self.assertNotIn("powerchat_username", updated_text)

    def test_streamer_shared_settings_update_writes_and_removes_tables(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                '[streamers."OUMB3rd"]\n'
                'sources = ["@OUMB3rd"]\n',
                encoding="utf-8",
            )

            voice_changed = update_streamer_voice_detection_config(
                config_path,
                "OUMB3rd",
                VoiceDetectionConfig(mode="range", min_speakers=2, max_speakers=4),
            )
            labels_changed = update_streamer_speaker_labels_config(
                config_path,
                "OUMB3rd",
                {"SPEAKER_00": "OUMB3rd", "SPEAKER_01": ""},
            )
            config = load_config(config_path)
            voice_removed = update_streamer_voice_detection_config(
                config_path,
                "OUMB3rd",
                None,
            )
            labels_removed = update_streamer_speaker_labels_config(
                config_path,
                "OUMB3rd",
                {},
            )
            config_after_remove = load_config(config_path)

        self.assertTrue(voice_changed)
        self.assertTrue(labels_changed)
        self.assertIsNotNone(config.streamers["OUMB3rd"].voice_detection)
        assert config.streamers["OUMB3rd"].voice_detection is not None
        self.assertEqual(config.streamers["OUMB3rd"].voice_detection.mode, "range")
        self.assertEqual(config.streamers["OUMB3rd"].voice_detection.max_speakers, 4)
        self.assertEqual(config.streamers["OUMB3rd"].speaker_labels, {"SPEAKER_00": "OUMB3rd"})
        self.assertTrue(voice_removed)
        self.assertTrue(labels_removed)
        self.assertIsNone(config_after_remove.streamers["OUMB3rd"].voice_detection)
        self.assertEqual(config_after_remove.streamers["OUMB3rd"].speaker_labels, {})


    def test_streamer_voice_profiles_can_be_configured(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                '[streamers."OUMB3rd"]\n'
                'sources = ["@OUMB3rd"]\n'
                '[streamers."OUMB3rd".voices."Host"]\n'
                'enabled = true\n'
                'samples = ["host.wav", "known.voice-sample.json"]\n'
                'threshold = 0.22\n'
                'notes = "main streamer"\n',
                encoding="utf-8",
            )

            config = load_config(config_path)
            profile = config.streamers["OUMB3rd"].voices["Host"]

        self.assertTrue(profile.enabled)
        self.assertEqual(profile.samples, ["host.wav", "known.voice-sample.json"])
        self.assertEqual(profile.threshold, 0.22)
        self.assertEqual(profile.notes, "main streamer")

    def test_streamer_voice_profile_update_writes_and_removes_table(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                '[streamers."OUMB3rd"]\n'
                'sources = ["@OUMB3rd"]\n',
                encoding="utf-8",
            )

            changed = update_streamer_voice_profile_config(
                config_path,
                "OUMB3rd",
                "Host",
                VoiceProfileConfig(
                    enabled=True,
                    samples=["host.wav"],
                    threshold=0.25,
                    notes="main voice",
                ),
            )
            config = load_config(config_path)
            removed = update_streamer_voice_profile_config(config_path, "OUMB3rd", "Host", None)
            config_after_remove = load_config(config_path)

        self.assertTrue(changed)
        self.assertEqual(config.streamers["OUMB3rd"].voices["Host"].samples, ["host.wav"])
        self.assertEqual(config.streamers["OUMB3rd"].voices["Host"].threshold, 0.25)
        self.assertTrue(removed)
        self.assertEqual(config_after_remove.streamers["OUMB3rd"].voices, {})

    def test_streamer_voice_profile_rejects_path_traversal_samples(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                '[streamers."OUMB3rd"]\n'
                'sources = ["@OUMB3rd"]\n'
                '[streamers."OUMB3rd".voices."Host"]\n'
                'samples = ["../host.wav"]\n',
                encoding="utf-8",
            )

            with self.assertRaises(ConfigError):
                load_config(config_path)

    def test_voice_sample_filename_is_sanitized(self) -> None:
        self.assertEqual(
            sanitize_voice_sample_filename("../Host Voice!!.mp3"),
            "Host_Voice.mp3",
        )

    def test_stream_event_settings_and_streamer_overrides_can_be_configured(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                'stream_event_detection_enabled = true\n'
                'stream_event_model = "custom/ast"\n'
                'stream_event_device = "cpu"\n'
                'stream_event_window_seconds = 8.0\n'
                'stream_event_hop_seconds = 2.0\n'
                'stream_event_min_confidence = 0.55\n'
                'stream_event_max_events_per_media = 25\n'
                '[[stream_event_rules]]\n'
                'name = "Laughter"\n'
                'labels = ["Laughter", "Giggle"]\n'
                'keywords = ["haha"]\n'
                'voice = "Host"\n'
                'min_loudness_dbfs = -24.5\n'
                'min_duration_seconds = 1.5\n'
                'max_duration_seconds = 30.0\n'
                'severity = "high"\n'
                '[streamers."OUMB3rd"]\n'
                'sources = ["@OUMB3rd"]\n'
                '[streamers."OUMB3rd".stream_event_detection]\n'
                'enabled = false\n'
                'model = "streamer/ast"\n'
                'device = "cuda:0"\n'
                'window_seconds = 12.0\n'
                'hop_seconds = 6.0\n'
                'min_confidence = 0.7\n'
                'max_events_per_media = 5\n'
                '[[streamers."OUMB3rd".stream_event_rules]]\n'
                'name = "Hype"\n'
                'keywords = ["lets go"]\n'
                'voice = "Guest"\n'
                'severity = "warning"\n',
                encoding="utf-8",
            )

            config = load_config(config_path)

        self.assertTrue(config.stream_event_detection_enabled)
        self.assertEqual(config.stream_event_model, "custom/ast")
        self.assertEqual(config.stream_event_device, "cpu")
        self.assertEqual(config.stream_event_window_seconds, 8.0)
        self.assertEqual(config.stream_event_hop_seconds, 2.0)
        self.assertEqual(config.stream_event_min_confidence, 0.55)
        self.assertEqual(config.stream_event_max_events_per_media, 25)
        self.assertEqual(len(config.stream_event_rules), 1)
        rule = config.stream_event_rules[0]
        self.assertEqual(rule.name, "Laughter")
        self.assertEqual(rule.labels, ["Laughter", "Giggle"])
        self.assertEqual(rule.keywords, ["haha"])
        self.assertEqual(rule.voice, "Host")
        self.assertEqual(rule.min_loudness_dbfs, -24.5)
        self.assertEqual(rule.min_duration_seconds, 1.5)
        self.assertEqual(rule.max_duration_seconds, 30.0)
        self.assertEqual(rule.severity, "high")
        streamer = config.streamers["OUMB3rd"]
        self.assertIsNotNone(streamer.stream_event_detection)
        assert streamer.stream_event_detection is not None
        self.assertFalse(streamer.stream_event_detection.enabled)
        self.assertEqual(streamer.stream_event_detection.model, "streamer/ast")
        self.assertEqual(streamer.stream_event_detection.device, "cuda:0")
        self.assertEqual(streamer.stream_event_detection.window_seconds, 12.0)
        self.assertEqual(streamer.stream_event_detection.hop_seconds, 6.0)
        self.assertEqual(streamer.stream_event_detection.min_confidence, 0.7)
        self.assertEqual(streamer.stream_event_detection.max_events_per_media, 5)
        self.assertEqual(len(streamer.stream_event_rules), 1)
        self.assertEqual(streamer.stream_event_rules[0].name, "Hype")
        self.assertEqual(streamer.stream_event_rules[0].voice, "Guest")

    def test_stream_event_config_update_writes_and_removes_tables(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                '[streamers."OUMB3rd"]\n'
                'sources = ["@OUMB3rd"]\n',
                encoding="utf-8",
            )

            global_changed = update_global_stream_event_rules_config(
                config_path,
                [
                    StreamEventRuleConfig(
                        name="Laughter",
                        labels=["Laughter"],
                        voice="Host",
                        severity="high",
                    )
                ],
            )
            streamer_changed = update_streamer_stream_event_config(
                config_path,
                "OUMB3rd",
                StreamEventDetectionConfig(enabled=True, min_confidence=0.65),
                [
                    StreamEventRuleConfig(
                        name="Catchphrase",
                        keywords=["lets go"],
                        voice="Guest",
                    )
                ],
            )
            config = load_config(config_path)
            removed = update_streamer_stream_event_config(config_path, "OUMB3rd", None, [])
            config_after_remove = load_config(config_path)

        self.assertTrue(global_changed)
        self.assertTrue(streamer_changed)
        self.assertEqual(config.stream_event_rules[0].name, "Laughter")
        self.assertEqual(config.stream_event_rules[0].voice, "Host")
        streamer = config.streamers["OUMB3rd"]
        self.assertIsNotNone(streamer.stream_event_detection)
        assert streamer.stream_event_detection is not None
        self.assertTrue(streamer.stream_event_detection.enabled)
        self.assertEqual(streamer.stream_event_detection.min_confidence, 0.65)
        self.assertEqual(streamer.stream_event_rules[0].name, "Catchphrase")
        self.assertEqual(streamer.stream_event_rules[0].voice, "Guest")
        self.assertTrue(removed)
        self.assertIsNone(config_after_remove.streamers["OUMB3rd"].stream_event_detection)
        self.assertEqual(config_after_remove.streamers["OUMB3rd"].stream_event_rules, [])

    def test_stream_event_rules_are_validated(self) -> None:
        invalid_configs = [
            '[[stream_event_rules]]\nname = ""\nkeywords = ["hype"]\n',
            '[[stream_event_rules]]\nname = "Empty"\n',
            (
                '[[stream_event_rules]]\n'
                'name = "Bad duration"\n'
                'keywords = ["hype"]\n'
                'min_duration_seconds = 5.0\n'
                'max_duration_seconds = 2.0\n'
            ),
            'stream_event_min_confidence = 1.5\n',
        ]
        for body in invalid_configs:
            with self.subTest(body=body):
                with TemporaryDirectory() as tmp:
                    config_path = Path(tmp) / "config.toml"
                    config_path.write_text(body, encoding="utf-8")

                    with self.assertRaises(ConfigError):
                        load_config(config_path)


    def test_channel_voice_detection_overrides_can_be_configured(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                '[channel_voice_detection."Example Channel"]\n'
                'mode = "fixed"\n'
                'speakers = 3\n'
                'hf_token_env = "PYANNOTE_TOKEN"\n'
                '[channel_voice_detection."@Other"]\n'
                'mode = "range"\n'
                'min_speakers = 2\n',
                encoding="utf-8",
            )

            config = load_config(config_path)

        fixed = config.channel_voice_detection["Example Channel"]
        ranged = config.channel_voice_detection["@Other"]
        self.assertEqual(fixed.mode, "fixed")
        self.assertEqual(fixed.min_speakers, 3)
        self.assertEqual(fixed.max_speakers, 3)
        self.assertEqual(fixed.hf_token_env, "PYANNOTE_TOKEN")
        self.assertEqual(ranged.mode, "range")
        self.assertEqual(ranged.min_speakers, 2)
        self.assertEqual(ranged.max_speakers, 0)

    def test_root_config_update_inserts_missing_keys_before_channel_tables(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                '[channel_voice_detection."Example Channel"]\n'
                'mode = "fixed"\n'
                'speakers = 2\n',
                encoding="utf-8",
            )

            update_config_values(config_path, {"whisperx_diarize": False})
            text = config_path.read_text(encoding="utf-8")
            config = load_config(config_path)

        self.assertLess(
            text.index("whisperx_diarize"),
            text.index('[channel_voice_detection."Example Channel"]'),
        )
        self.assertFalse(config.whisperx_diarize)
        self.assertIn("Example Channel", config.channel_voice_detection)

    def test_channel_voice_detection_update_writes_and_removes_table(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text('channels = ["@Example"]\n', encoding="utf-8")

            changed = update_channel_voice_detection_config(
                config_path,
                "Example Channel",
                VoiceDetectionConfig(mode="fixed", min_speakers=2, max_speakers=2),
            )
            config = load_config(config_path)
            removed = update_channel_voice_detection_config(
                config_path,
                "Example Channel",
                None,
            )
            config_after_remove = load_config(config_path)

        self.assertTrue(changed)
        self.assertEqual(config.channel_voice_detection["Example Channel"].mode, "fixed")
        self.assertTrue(removed)
        self.assertEqual(config_after_remove.channel_voice_detection, {})

    def test_channel_speaker_labels_can_be_configured(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                '[channel_speaker_labels."Example Channel"]\n'
                'SPEAKER_00 = "OUMB3rd"\n'
                'SPEAKER_01 = "Guest"\n',
                encoding="utf-8",
            )

            config = load_config(config_path)

        self.assertEqual(
            config.channel_speaker_labels["Example Channel"],
            {"SPEAKER_00": "OUMB3rd", "SPEAKER_01": "Guest"},
        )

    def test_channel_speaker_labels_update_writes_and_removes_table(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text('channels = ["@Example"]\n', encoding="utf-8")

            changed = update_channel_speaker_labels_config(
                config_path,
                "Example Channel",
                {"SPEAKER_00": "OUMB3rd", "SPEAKER_01": ""},
            )
            config = load_config(config_path)
            removed = update_channel_speaker_labels_config(
                config_path,
                "Example Channel",
                {},
            )
            config_after_remove = load_config(config_path)

        self.assertTrue(changed)
        self.assertEqual(
            config.channel_speaker_labels["Example Channel"],
            {"SPEAKER_00": "OUMB3rd"},
        )
        self.assertTrue(removed)
        self.assertEqual(config_after_remove.channel_speaker_labels, {})

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
