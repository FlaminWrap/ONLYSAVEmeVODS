from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from ytdlbot.config import BotConfig
from ytdlbot.transcription import (
    build_whisperx_command,
    command_for_log,
    existing_transcription_outputs,
    handle_process_output_line,
    redact_sensitive_text,
    transcription_outputs_exist,
    whisperx_process_env,
)


class TranscriptionTests(unittest.TestCase):
    def test_whisperx_command_uses_configured_gpu_and_diarization_options(self) -> None:
        with TemporaryDirectory() as tmp:
            media_file = Path(tmp) / "Live Status [LIVEVIDEO01].mp4"
            config = BotConfig(
                whisperx_path="/opt/bin/whisperx",
                whisperx_model="large-v3",
                whisperx_device="cuda:0",
                whisperx_compute_type="float16",
                whisperx_batch_size=8,
                whisperx_language="en",
                whisperx_diarize=True,
                whisperx_min_speakers=2,
                whisperx_max_speakers=4,
            )

            command = build_whisperx_command(config, media_file)

        self.assertEqual(command[0], "/opt/bin/whisperx")
        self.assertIn(str(media_file), command)
        self.assertIn("--model", command)
        self.assertEqual(command[command.index("--model") + 1], "large-v3")
        self.assertIn("--device", command)
        self.assertEqual(command[command.index("--device") + 1], "cuda:0")
        self.assertIn("--compute_type", command)
        self.assertEqual(command[command.index("--compute_type") + 1], "float16")
        self.assertIn("--batch_size", command)
        self.assertEqual(command[command.index("--batch_size") + 1], "8")
        self.assertIn("--output_dir", command)
        self.assertEqual(
            command[command.index("--output_dir") + 1],
            str(media_file.parent),
        )
        self.assertIn("--output_format", command)
        self.assertEqual(command[command.index("--output_format") + 1], "all")
        self.assertIn("--language", command)
        self.assertIn("--diarize", command)
        self.assertNotIn("--hf_token", command)
        self.assertIn("--min_speakers", command)
        self.assertIn("--max_speakers", command)

    def test_whisperx_command_can_disable_diarization(self) -> None:
        config = BotConfig(whisperx_diarize=False)
        command = build_whisperx_command(config, Path("/tmp/video.mp4"))

        self.assertNotIn("--diarize", command)
        self.assertNotIn("--hf_token", command)
        self.assertNotIn("--min_speakers", command)
        self.assertNotIn("--max_speakers", command)

    def test_whisperx_process_env_passes_tokens_without_command_line(self) -> None:
        with patch.dict(
            "os.environ",
            {"XDG_CACHE_HOME": "/tmp/ytdlbot-cache"},
            clear=True,
        ):
            env = whisperx_process_env("hf_secret_token_value")

        self.assertEqual(env["HF_TOKEN"], "hf_secret_token_value")
        self.assertEqual(env["HUGGINGFACE_HUB_TOKEN"], "hf_secret_token_value")
        self.assertEqual(env["MPLCONFIGDIR"], "/tmp/ytdlbot-cache/matplotlib")
        self.assertEqual(env["NLTK_DATA"], "/tmp/ytdlbot-cache/nltk_data")

    def test_whisperx_process_env_keeps_existing_cache_env_values(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "XDG_CACHE_HOME": "/tmp/ytdlbot-cache",
                "MPLCONFIGDIR": "/tmp/custom-matplotlib",
                "NLTK_DATA": "/tmp/custom-nltk",
            },
            clear=True,
        ):
            env = whisperx_process_env("")

        self.assertEqual(env["MPLCONFIGDIR"], "/tmp/custom-matplotlib")
        self.assertEqual(env["NLTK_DATA"], "/tmp/custom-nltk")

    def test_command_and_output_redaction_hide_huggingface_tokens(self) -> None:
        self.assertNotIn(
            "hf_ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890",
            command_for_log(
                ["whisperx", "--hf_token", "hf_ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"]
            ),
        )
        self.assertEqual(
            redact_sensitive_text(
                "using hf_ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890 for auth"
            ),
            "using hf_<redacted> for auth",
        )
        self.assertEqual(
            redact_sensitive_text("huggingface_hub calls hf_raise_for_status"),
            "huggingface_hub calls hf_raise_for_status",
        )

    def test_transcription_outputs_require_srt_and_vtt(self) -> None:
        with TemporaryDirectory() as tmp:
            media_file = Path(tmp) / "Live Status [LIVEVIDEO01].mp4"
            srt_file = media_file.with_suffix(".srt")
            vtt_file = media_file.with_suffix(".vtt")
            json_file = media_file.with_suffix(".json")

            srt_file.write_text("subtitle", encoding="utf-8")
            json_file.write_text("{}", encoding="utf-8")

            self.assertFalse(transcription_outputs_exist(media_file))

            vtt_file.write_text("subtitle", encoding="utf-8")

            self.assertTrue(transcription_outputs_exist(media_file))
            self.assertEqual(
                existing_transcription_outputs(media_file),
                [srt_file, vtt_file, json_file],
            )

    def test_whisperx_output_lines_are_logged_and_bounded(self) -> None:
        class ListLogger:
            def __init__(self) -> None:
                self.messages: list[str] = []

            def info(self, message: str, *args: object) -> None:
                self.messages.append(message % args)

        logger = ListLogger()
        output_buffer: list[str] = []

        for index in range(205):
            handle_process_output_line(
                f"line {index}",
                output_buffer,
                logger,  # type: ignore[arg-type]
                label="WhisperX",
            )

        self.assertEqual(len(output_buffer), 200)
        self.assertEqual(output_buffer[0], "line 5")
        self.assertIn("WhisperX: line 204", logger.messages[-1])
