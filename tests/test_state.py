from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from onlysavemevods.state import StateStore


class StateWatermarkTests(unittest.TestCase):
    def test_watermark_copy_lifecycle(self) -> None:
        with TemporaryDirectory() as tmp:
            state = StateStore(Path(tmp) / "state.sqlite3")

            created = state.create_watermark_copy(
                copy_id="wm_copy001",
                video_id="LIVEVIDEO01",
                source_name="Live [LIVEVIDEO01].mp4",
                output_name=".watermarks/Live [LIVEVIDEO01] - wm-copy001.mp4",
                recipient_label="Recipient A",
            )
            state.update_watermark_copy(
                "wm_copy001",
                status="running",
                message="Rendering",
                started=True,
            )
            state.update_watermark_copy(
                "wm_copy001",
                status="done",
                message="Completed",
                finished=True,
            )
            fetched = state.get_watermark_copy("wm_copy001")
            listed = state.list_watermark_copies(
                video_id="LIVEVIDEO01",
                statuses=["done"],
            )
            state.close()

        self.assertEqual(created.status, "queued")
        self.assertIsNotNone(fetched)
        assert fetched is not None
        self.assertEqual(fetched.status, "done")
        self.assertEqual(fetched.message, "Completed")
        self.assertIsNotNone(fetched.started_at)
        self.assertIsNotNone(fetched.finished_at)
        self.assertEqual([record.copy_id for record in listed], ["wm_copy001"])

    def test_stale_watermark_jobs_are_marked_interrupted(self) -> None:
        with TemporaryDirectory() as tmp:
            state = StateStore(Path(tmp) / "state.sqlite3")
            state.create_watermark_copy(
                copy_id="wm_copy001",
                video_id="LIVEVIDEO01",
                source_name="Live [LIVEVIDEO01].mp4",
                output_name=".watermarks/Live [LIVEVIDEO01] - wm-copy001.mp4",
                recipient_label="Recipient A",
            )
            state.update_watermark_copy(
                "wm_copy001",
                status="running",
                message="Rendering",
                started=True,
            )

            state.mark_stale_watermarks_interrupted()
            fetched = state.get_watermark_copy("wm_copy001")
            state.close()

        self.assertIsNotNone(fetched)
        assert fetched is not None
        self.assertEqual(fetched.status, "interrupted")
        self.assertIsNotNone(fetched.finished_at)


if __name__ == "__main__":
    unittest.main()
