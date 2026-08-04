from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import unittest

from onlysavemevods.config import BotConfig
from onlysavemevods.state import WatermarkCopyRecord
from onlysavemevods.watermark import (
    build_audio_mux_command,
    create_watermarked_copy,
    derive_pattern,
    score_watermark_frame_groups,
    score_watermark_records,
    validate_watermark_output,
    validate_recipient_label,
    WatermarkError,
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
    def test_failed_validation_preserves_existing_output_atomically(self) -> None:
        import numpy as np

        class FakeCapture:
            def __init__(self) -> None:
                self.frames = [np.zeros((2, 2, 3), dtype=np.uint8) for _ in range(3)]

            def isOpened(self) -> bool:
                return True

            def get(self, property_id: int) -> float:
                return {
                    1: 30.0,
                    2: 3.0,
                    3: 2.0,
                    4: 2.0,
                }.get(property_id, 0.0)

            def read(self):
                return (True, self.frames.pop(0)) if self.frames else (False, None)

            def release(self) -> None:
                return None

        class FakeWriter:
            def isOpened(self) -> bool:
                return True

            def write(self, _frame) -> None:
                return None

            def release(self) -> None:
                return None

        class FakeCv2:
            CAP_PROP_FPS = 1
            CAP_PROP_FRAME_COUNT = 2
            CAP_PROP_FRAME_WIDTH = 3
            CAP_PROP_FRAME_HEIGHT = 4
            INTER_CUBIC = 5

            @staticmethod
            def VideoCapture(_path: str) -> FakeCapture:
                return FakeCapture()

            @staticmethod
            def VideoWriter_fourcc(*_args: str) -> int:
                return 1

            @staticmethod
            def VideoWriter(*_args) -> FakeWriter:
                return FakeWriter()

            @staticmethod
            def resize(pattern, _size, interpolation=None):
                return pattern

        class Result:
            returncode = 0
            stdout = b""
            stderr = b""

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source.mp4"
            output = root / "output.mp4"
            source.write_bytes(b"source")
            output.write_bytes(b"existing")

            def fake_run(command, **_kwargs):
                Path(command[-1]).write_bytes(b"truncated")
                return Result()

            with (
                patch(
                    "onlysavemevods.watermark.optional_cv_dependencies",
                    return_value=(np, FakeCv2),
                ),
                patch(
                    "onlysavemevods.watermark.apply_watermark_to_frame",
                    side_effect=lambda frame, *_args: frame,
                ),
                patch("onlysavemevods.watermark.subprocess.run", fake_run),
                patch(
                    "onlysavemevods.watermark.validate_watermark_output",
                    side_effect=WatermarkError("truncated"),
                ),
            ):
                with self.assertRaises(WatermarkError):
                    create_watermarked_copy(
                        source_file=source,
                        output_file=output,
                        secret="secret",
                        copy_id="wm_copy",
                        video_id="video",
                        source_name=source.name,
                        strength="balanced",
                        ffmpeg_path="ffmpeg",
                        overwrite=True,
                    )
            existing = output.read_bytes()
            temp_exists = (root / "output.muxing.mp4").exists()

        self.assertEqual(existing, b"existing")
        self.assertFalse(temp_exists)

    def test_truncated_watermark_output_is_rejected(self) -> None:
        class FakeCapture:
            def __init__(self) -> None:
                self.remaining = 20

            def isOpened(self) -> bool:
                return True

            def get(self, _property: object) -> float:
                return 30.0

            def read(self):
                if self.remaining <= 0:
                    return False, None
                self.remaining -= 1
                return True, object()

            def release(self) -> None:
                return None

        class FakeCv2:
            CAP_PROP_FPS = 5

            @staticmethod
            def VideoCapture(_path: str) -> FakeCapture:
                return FakeCapture()

        with self.assertRaisesRegex(WatermarkError, "truncated"):
            validate_watermark_output(
                Path("rendering.mp4"),
                expected_frames=300,
                expected_duration=10.0,
                cv2=FakeCv2,
            )

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

    def test_grouped_scoring_does_not_dilute_full_frame_match_with_crop_variants(self) -> None:
        matching = copy_record("wm_copy001", "Recipient A")
        other = copy_record("wm_copy002", "Recipient B")
        pattern = derive_pattern(
            "secret-a",
            matching.copy_id,
            matching.video_id,
            matching.source_name,
        )
        groups = [[("full", pattern), ("crop-3pct", -pattern)] for _index in range(12)]

        candidates = score_watermark_frame_groups(groups, [other, matching], "secret-a")

        self.assertEqual(candidates[0].copy_id, matching.copy_id)
        self.assertEqual(candidates[0].variant, "full")
        self.assertGreater(candidates[0].score, 0.5)

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
