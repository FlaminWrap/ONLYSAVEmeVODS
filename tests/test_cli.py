from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import json
import unittest

from onlysavemevods import __version__
from onlysavemevods.cli import main
from onlysavemevods.config import load_config
from onlysavemevods.state import StateStore
from onlysavemevods.watermark import DetectionCandidate, DetectionResult


class CliVersionTests(unittest.TestCase):
    def test_version_flag_prints_app_version(self) -> None:
        output = StringIO()
        with redirect_stdout(output), self.assertRaises(SystemExit) as raised:
            main(["--version"])

        self.assertEqual(raised.exception.code, 0)
        self.assertIn(__version__, output.getvalue())


class CliRenderChatTests(unittest.TestCase):
    def test_render_chat_file_writes_progress_file(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.toml"
            config_path.write_text("", encoding="utf-8")
            media_file = root / "media.mp4"
            chat_file = root / "media.live_chat.json"
            output_file = root / "media - chat.mp4"
            progress_file = root / "progress.json"
            media_file.write_text("media", encoding="utf-8")
            chat_file.write_text("chat", encoding="utf-8")
            captured: dict[str, object] = {}

            def fake_render_chat_video_file(*_args: object, **kwargs: object) -> Path:
                captured["platform"] = kwargs.get("platform")
                captured["emoji_cache_dir"] = kwargs.get("emoji_cache_dir")
                panel_file = output_file.with_name(f"{output_file.stem}.panel{output_file.suffix}")
                panel_file.write_bytes(b"x" * 2048)
                progress_callback = kwargs.get("progress_callback")
                assert callable(progress_callback)
                progress_callback("Rendering panel frames 2/4", 0.5)
                captured["during"] = json.loads(progress_file.read_text(encoding="utf-8"))
                output_file.write_text("done", encoding="utf-8")
                return output_file

            with patch("onlysavemevods.cli.render_chat_video_file", side_effect=fake_render_chat_video_file):
                result = main(
                    [
                        "render-chat-file",
                        "--config",
                        str(config_path),
                        "--media",
                        str(media_file),
                        "--chat",
                        str(chat_file),
                        "--output",
                        str(output_file),
                        "--platform",
                        "kick",
                        "--progress-file",
                        str(progress_file),
                    ]
                )
            captured["final"] = json.loads(progress_file.read_text(encoding="utf-8"))

        during = captured["during"]
        final = captured["final"]
        self.assertEqual(result, 0)
        self.assertEqual(during["phase"], "Rendering panel frames 2/4")
        self.assertEqual(during["progress"], 0.5)
        self.assertEqual(during["media_name"], "media.mp4")
        self.assertEqual(during["chat_name"], "media.live_chat.json")
        self.assertEqual(during["output_name"], "media - chat.mp4")
        self.assertEqual(during["outputs"]["panel"]["size_bytes"], 2048)
        self.assertEqual(captured["platform"], "kick")
        self.assertEqual(captured["emoji_cache_dir"], root / "state" / "chat_emoji_cache")
        self.assertEqual(final["phase"], "Complete")
        self.assertEqual(final["progress"], 1.0)

    def test_render_chat_file_ignores_unwritable_progress_file(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.toml"
            config_path.write_text("", encoding="utf-8")
            media_file = root / "media.mp4"
            chat_file = root / "media.live_chat.json"
            output_file = root / "media - chat.mp4"
            blocked_parent = root / "not-a-directory"
            blocked_parent.write_text("blocked", encoding="utf-8")
            media_file.write_text("media", encoding="utf-8")
            chat_file.write_text("chat", encoding="utf-8")

            def fake_render_chat_video_file(*_args: object, **kwargs: object) -> Path:
                progress_callback = kwargs.get("progress_callback")
                assert callable(progress_callback)
                progress_callback("Rendering panel", 0.5)
                output_file.write_text("done", encoding="utf-8")
                return output_file

            with patch("onlysavemevods.cli.render_chat_video_file", side_effect=fake_render_chat_video_file):
                result = main(
                    [
                        "render-chat-file",
                        "--config",
                        str(config_path),
                        "--media",
                        str(media_file),
                        "--chat",
                        str(chat_file),
                        "--output",
                        str(output_file),
                        "--progress-file",
                        str(blocked_parent / "progress.json"),
                    ]
                )
            output = output_file.read_text(encoding="utf-8")

        self.assertEqual(result, 0)
        self.assertEqual(output, "done")


class CliVoiceDetectionTests(unittest.TestCase):
    def test_voice_detection_set_fixed_updates_config(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                "whisperx_diarize = true\n"
                "whisperx_min_speakers = 0\n"
                "whisperx_max_speakers = 0\n",
                encoding="utf-8",
            )

            output = StringIO()
            with redirect_stdout(output):
                result = main(
                    [
                        "voice-detection",
                        "set",
                        "--config",
                        str(config_path),
                        "--mode",
                        "fixed",
                        "--speakers",
                        "3",
                        "--hf-token-env",
                        "PYANNOTE_TOKEN",
                    ]
                )

            config = load_config(config_path)

        self.assertEqual(result, 0)
        self.assertTrue(config.whisperx_diarize)
        self.assertEqual(config.whisperx_min_speakers, 3)
        self.assertEqual(config.whisperx_max_speakers, 3)
        self.assertEqual(config.whisperx_hf_token_env, "PYANNOTE_TOKEN")
        self.assertIn("Voice detection: fixed", output.getvalue())
        self.assertIn("Speaker count: exactly 3", output.getvalue())

    def test_voice_detection_set_range_requires_a_bound(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text("", encoding="utf-8")

            result = main(
                [
                    "voice-detection",
                    "set",
                    "--config",
                    str(config_path),
                    "--mode",
                    "range",
                ]
            )

        self.assertEqual(result, 2)

    def test_voice_detection_show_reports_token_status(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                'whisperx_hf_token_env = "PYANNOTE_TOKEN"\n',
                encoding="utf-8",
            )

            output = StringIO()
            with patch.dict("os.environ", {"PYANNOTE_TOKEN": "secret"}, clear=True):
                with redirect_stdout(output):
                    result = main(
                        [
                            "voice-detection",
                            "show",
                            "--config",
                            str(config_path),
                        ]
                    )

        self.assertEqual(result, 0)
        self.assertIn("Voice detection: auto", output.getvalue())
        self.assertIn("Hugging Face token env: PYANNOTE_TOKEN (set)", output.getvalue())


class CliWatermarkTests(unittest.TestCase):
    def test_detect_watermark_missing_file_returns_usage_error(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text("", encoding="utf-8")

            result = main(
                [
                    "detect-watermark",
                    "--config",
                    str(config_path),
                    "--media",
                    str(Path(tmp) / "missing.mp4"),
                ]
            )

        self.assertEqual(result, 2)

    def test_detect_watermark_json_output(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.toml"
            config_path.write_text(
                'state_dir = "state"\nwatermark_secret_env = "TEST_WATERMARK_SECRET"\n',
                encoding="utf-8",
            )
            media = root / "suspect.mp4"
            media.write_text("video", encoding="utf-8")
            state = StateStore(root / "state" / "onlysavemevods.sqlite3")
            state.create_watermark_copy(
                copy_id="wm_copy001",
                video_id="LIVEVIDEO01",
                source_name="Live [LIVEVIDEO01].mp4",
                output_name=".watermarks/Live [LIVEVIDEO01] - wm-copy001.mp4",
                recipient_label="Recipient A",
            )
            state.update_watermark_copy(
                "wm_copy001",
                status="done",
                message="Completed",
                finished=True,
            )
            state.close()
            candidate = DetectionCandidate(
                copy_id="wm_copy001",
                video_id="LIVEVIDEO01",
                source_name="Live [LIVEVIDEO01].mp4",
                output_name=".watermarks/Live [LIVEVIDEO01] - wm-copy001.mp4",
                recipient_label="Recipient A",
                score=0.05,
            )
            detection = DetectionResult(
                matched=True,
                confidence="high",
                score=0.05,
                margin=0.03,
                frames_analyzed=20,
                best=candidate,
                runner_up=None,
                candidates=[candidate],
                message="Matched wm_copy001 for Recipient A",
            )

            output = StringIO()
            with (
                patch.dict("os.environ", {"TEST_WATERMARK_SECRET": "secret"}),
                patch("onlysavemevods.cli.detect_watermark", return_value=detection),
                redirect_stdout(output),
            ):
                result = main(
                    [
                        "detect-watermark",
                        "--config",
                        str(config_path),
                        "--media",
                        str(media),
                        "--json",
                    ]
                )

        payload = json.loads(output.getvalue())
        self.assertEqual(result, 0)
        self.assertTrue(payload["matched"])
        self.assertEqual(payload["best"]["recipient_label"], "Recipient A")


if __name__ == "__main__":
    unittest.main()
