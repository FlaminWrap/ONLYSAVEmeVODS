from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import json
import unittest

from ytdlbot.cli import main
from ytdlbot.state import StateStore
from ytdlbot.watermark import DetectionCandidate, DetectionResult


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
            state = StateStore(root / "state" / "ytdlbot.sqlite3")
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
                patch("ytdlbot.cli.detect_watermark", return_value=detection),
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
