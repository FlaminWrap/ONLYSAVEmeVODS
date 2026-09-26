from __future__ import annotations

import os
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from onlysavemevods.download_progress import (
    clear_all_download_progress,
    download_progress_for,
    download_progress_path,
    fragment_high_water_for,
    fragment_high_water_path,
    load_download_progress,
    record_download_progress,
)
from onlysavemevods.web import render_live_download_progress


class FragmentHighWaterTests(unittest.TestCase):
    def tearDown(self) -> None:
        clear_all_download_progress()

    def test_per_track_highs_survive_counter_reset_and_service_restart(self) -> None:
        video_id = "youtube:LIVEVIDEO01"
        with TemporaryDirectory() as temp_dir:
            state_dir = Path(temp_dir)
            progress_file = download_progress_path(state_dir)
            record_download_progress(
                video_id,
                {"video": (100, 120), "audio": (25, 30)},
                segment_index=3,
                updated_at=100.0,
                progress_file=progress_file,
            )
            progress = record_download_progress(
                video_id,
                {"video": (90, 100), "audio": (10, 15)},
                segment_index=3,
                updated_at=200.0,
                progress_file=progress_file,
            )
            self.assertEqual(
                [(item.fragment_index, item.fragment_count) for item in progress],
                [(90, 100), (10, 15)],
            )
            self.assertEqual(
                [
                    (item.highest_fragment_index, item.highest_fragment_count)
                    for item in progress
                ],
                [(100, 120), (25, 30)],
            )
            self.assertEqual([item.lag_fragments for item in progress], [10, 5])
            self.assertTrue(
                fragment_high_water_path(state_dir, video_id, 3).exists()
            )

            # A new process clears its transient progress and reloads the
            # durable high-water file when the same segment reconnects.
            clear_all_download_progress(progress_file=progress_file)
            self.assertFalse(progress_file.exists())
            self.assertEqual(
                fragment_high_water_for(video_id, 3, state_dir=state_dir),
                {"video": (100, 120), "audio": (25, 30)},
            )
            restarted = record_download_progress(
                video_id,
                {"video": (12, 20), "audio": (2, 8)},
                segment_index=3,
                updated_at=300.0,
                progress_file=progress_file,
            )
            self.assertEqual(
                [
                    (item.highest_fragment_index, item.highest_fragment_count)
                    for item in restarted
                ],
                [(100, 120), (25, 30)],
            )
            clear_all_download_progress()
            loaded = load_download_progress(progress_file, current_time=300.0)
            self.assertEqual(
                [
                    (item.highest_fragment_index, item.highest_fragment_count)
                    for item in loaded[video_id]
                ],
                [(100, 120), (25, 30)],
            )

            # A new segment has its own counters; the prior file remains
            # available for post-stream diagnostics.
            next_segment = record_download_progress(
                video_id,
                {"video": (3, 4), "audio": (1, 2)},
                segment_index=4,
                progress_file=progress_file,
            )
            self.assertEqual(
                [
                    (item.highest_fragment_index, item.highest_fragment_count)
                    for item in next_segment
                ],
                [(3, 4), (1, 2)],
            )
            self.assertEqual(
                fragment_high_water_for(video_id, 3, state_dir=state_dir),
                {"video": (100, 120), "audio": (25, 30)},
            )

    def test_failed_high_water_write_retries_unchanged_progress(self) -> None:
        video_id = "youtube:LIVEVIDEO01"
        with TemporaryDirectory() as temp_dir:
            state_dir = Path(temp_dir)
            progress_file = download_progress_path(state_dir)
            high_water_file = fragment_high_water_path(state_dir, video_id, 1)
            real_replace = os.replace
            failed = False

            def fail_first_high_water_write(source: str, target: str) -> None:
                nonlocal failed
                if Path(target) == high_water_file and not failed:
                    failed = True
                    raise OSError("temporary disk failure")
                real_replace(source, target)

            with (
                patch(
                    "onlysavemevods.download_progress.os.replace",
                    side_effect=fail_first_high_water_write,
                ),
                patch("onlysavemevods.download_progress.LOGGER.warning") as warning,
            ):
                record_download_progress(
                    video_id,
                    {"video": (10, 12), "audio": (5, 7)},
                    segment_index=1,
                    progress_file=progress_file,
                )
                self.assertFalse(high_water_file.exists())
                record_download_progress(
                    video_id,
                    {"video": (10, 12), "audio": (5, 7)},
                    segment_index=1,
                    progress_file=progress_file,
                )

            warning.assert_called_once()
            self.assertTrue(failed)
            self.assertTrue(high_water_file.exists())
            clear_all_download_progress()
            self.assertEqual(
                fragment_high_water_for(video_id, 1, state_dir=state_dir),
                {"video": (10, 12), "audio": (5, 7)},
            )

    def test_dashboard_labels_regressed_track_without_changing_current_status(self) -> None:
        video_id = "youtube:LIVEVIDEO01"
        record_download_progress(
            video_id,
            {"video": (20, 25), "audio": (10, 20)},
        )
        record_download_progress(
            video_id,
            {"video": (20, 25), "audio": (5, 8)},
        )
        stream = SimpleNamespace(
            video_id=video_id,
            title="Live progress",
            status="downloading",
            recording_kind="live",
            platform="youtube",
            download_progress=download_progress_for(video_id),
            download_progress_waiting_stale=False,
        )

        html = render_live_download_progress(stream)

        self.assertIn("5 / 8 fragments · 3 fragments behind", html)
        self.assertIn("highest reported 10 / 20", html)
        self.assertIn('max="8" value="5"', html)
        self.assertEqual(html.count("highest reported"), 2)  # row and aria-label
        self.assertNotIn("highest reported 20 / 25", html)


if __name__ == "__main__":
    unittest.main()
