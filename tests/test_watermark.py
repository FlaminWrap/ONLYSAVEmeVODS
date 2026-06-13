from pathlib import Path
from unittest.mock import patch
import unittest

from onlysavemevods.config import BotConfig
from onlysavemevods.state import WatermarkCopyRecord
from onlysavemevods.watermark import (
    build_audio_mux_command,
    derive_pattern,
    score_watermark_records,
    validate_recipient_label,
    watermark_secret,
    watermarked_output_name,
)


def copy_record(copy_id: str, label: str) -> WatermarkCopyRecord:
    return WatermarkCopyRecord(
        copy_id=copy_id,
        video_id="LIVEVIDEO01",
        source_name="Live [LIVEVIDEO01].mp4",
        output_name=f".watermarks/Live [LIVEVIDEO01] - {copy_id}.mp4",
        recipient_label=label,
        status="done",
        message="Completed",
        error="",
        phase="Complete",
        progress=1.0,
        created_at="2026-01-01T00:00:00+00:00",
        updated_at="2026-01-01T00:00:00+00:00",
        started_at="2026-01-01T00:00:00+00:00",
        finished_at="2026-01-01T00:00:00+00:00",
    )


class WatermarkTests(unittest.TestCase):
    def test_pattern_is_secret_keyed_and_deterministic(self) -> None:
        first = derive_pattern(
            "secret-a",
            "wm_copy001",
            "LIVEVIDEO01",
            "Live [LIVEVIDEO01].mp4",
        )
        same = derive_pattern(
            "secret-a",
            "wm_copy001",
            "LIVEVIDEO01",
            "Live [LIVEVIDEO01].mp4",
        )
        different = derive_pattern(
            "secret-b",
            "wm_copy001",
            "LIVEVIDEO01",
            "Live [LIVEVIDEO01].mp4",
        )

        self.assertEqual(first.shape, (36, 64))
        self.assertTrue((first == same).all())
        self.assertFalse((first == different).all())
        self.assertAlmostEqual(float(first.mean()), 0.0, places=6)

    def test_output_name_uses_hidden_subfolder_and_copy_id(self) -> None:
        name = watermarked_output_name("Live [LIVEVIDEO01].mp4", "wm_abcdef123456")

        self.assertEqual(
            name,
            ".watermarks/Live [LIVEVIDEO01] - wm-abcdef1234.mp4",
        )

    def test_watermark_secret_reads_configured_env_var(self) -> None:
        with patch.dict(
            "os.environ",
            {"ONLYSAVEMEVODS_WATERMARK_SECRET": "current-secret"},
            clear=True,
        ):
            secret = watermark_secret(BotConfig())

        self.assertEqual(secret, "current-secret")

    def test_recipient_label_is_required_and_normalized(self) -> None:
        self.assertEqual(validate_recipient_label("  Alice   Example  "), "Alice Example")
        with self.assertRaises(Exception):
            validate_recipient_label("   ")

    def test_mux_command_preserves_video_and_maps_optional_audio(self) -> None:
        command = build_audio_mux_command(
            "ffmpeg",
            Path("/tmp/watermarked.video.mp4"),
            Path("/tmp/source.mp4"),
            Path("/tmp/output.mp4"),
        )

        self.assertIn("-map", command)
        self.assertIn("0:v:0", command)
        self.assertIn("1:a?", command)
        self.assertIn("libx264", command)
        self.assertIn("aac", command)

    def test_scoring_prefers_matching_copy_pattern(self) -> None:
        matching = copy_record("wm_copy001", "Recipient A")
        other = copy_record("wm_copy002", "Recipient B")
        pattern = derive_pattern(
            "secret-a",
            matching.copy_id,
            matching.video_id,
            matching.source_name,
        )

        candidates = score_watermark_records([pattern], [other, matching], "secret-a")

        self.assertEqual(candidates[0].copy_id, matching.copy_id)
        self.assertGreater(candidates[0].score, candidates[1].score)


if __name__ == "__main__":
    unittest.main()
