from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from onlysavemevods.config import BotConfig
from onlysavemevods.youtube import LiveStream
from onlysavemevods.web import (
    VOD_DOWNLOAD_PLAN_PREFIX,
    VOD_DOWNLOAD_POSTPROCESS_PREFIX,
    VOD_DOWNLOAD_PROGRESS_PREFIX,
    VodDownloadProgress,
    build_vod_download_command,
    run_kick_vod_chat_download_job,
    run_vod_download_job,
)


def plan_line(*formats: dict[str, object], format_id: str = "299+140") -> str:
    return VOD_DOWNLOAD_PLAN_PREFIX + json.dumps({"format_id": format_id, "formats": formats})


def progress_line(format_id: str, downloaded: int, *, total: int | None = None, finished: bool = False) -> str:
    data = {"status": "finished" if finished else "downloading", "downloaded_bytes": downloaded}
    if total is not None:
        data["total_bytes"] = total
    return VOD_DOWNLOAD_PROGRESS_PREFIX + json.dumps({"format_id": format_id, "progress": data})


class VodOverallProgressTests(unittest.TestCase):
    def test_video_then_audio_share_one_byte_weighted_percentage(self) -> None:
        tracker = VodDownloadProgress()
        tracker.consume_line(plan_line(
            {"format_id": "299", "filesize": 900},
            {"format_id": "140", "filesize": 100},
        ))
        percentages = []
        for line in (
            progress_line("299", 450),
            progress_line("299", 900, finished=True),
            progress_line("140", 10),
            progress_line("140", 50),
            progress_line("140", 100, finished=True),
        ):
            tracker.consume_line(line)
            percentages.append(tracker.progress)
        self.assertEqual(percentages, [0.45, 0.9, 0.91, 0.95, 0.98])
        tracker.consume_line(VOD_DOWNLOAD_POSTPROCESS_PREFIX + '{"postprocessor":"Merger","status":"started"}')
        self.assertEqual(tracker.phase, "Merging VOD")
        self.assertEqual(tracker.progress, 0.98)

    def test_unknown_future_track_size_stays_indeterminate_until_known(self) -> None:
        tracker = VodDownloadProgress()
        tracker.consume_line(plan_line(
            {"format_id": "299", "filesize": 900},
            {"format_id": "140"},
        ))
        tracker.consume_line(progress_line("299", 900, finished=True))
        self.assertIsNone(tracker.progress)
        tracker.consume_line(progress_line("140", 50, total=100))
        self.assertEqual(tracker.progress, 0.95)

    def test_single_combined_file_and_resumed_bytes(self) -> None:
        tracker = VodDownloadProgress()
        tracker.consume_line(VOD_DOWNLOAD_PLAN_PREFIX + json.dumps({
            "format_id": "18", "filesize": 1000, "formats": [],
        }))
        tracker.consume_line(progress_line("18", 600, total=1000))
        self.assertEqual(tracker.progress, 0.6)
        tracker.consume_line(progress_line("18", 1000, total=1000, finished=True))
        self.assertEqual(tracker.progress, 0.98)

    def test_external_downloader_can_transfer_selected_formats_together(self) -> None:
        tracker = VodDownloadProgress()
        tracker.consume_line(plan_line(
            {"format_id": "299", "filesize": 900},
            {"format_id": "140", "filesize": 100},
        ))
        tracker.consume_line(progress_line("299+140", 600, total=1000))
        self.assertEqual(tracker.progress, 0.6)

    def test_estimate_updates_and_resume_do_not_move_bar_backwards(self) -> None:
        tracker = VodDownloadProgress()
        tracker.consume_line(plan_line({"format_id": "18", "filesize_approx": 1000}, format_id="18"))
        tracker.consume_line(progress_line("18", 500))
        self.assertEqual(tracker.progress, 0.5)
        tracker.consume_line(progress_line("18", 550, total=1200))
        self.assertEqual(tracker.progress, 0.5)
        tracker.consume_line(progress_line("18", 800, total=1200))
        self.assertAlmostEqual(tracker.progress, 2 / 3)

    def test_unplanned_progress_and_plain_percentages_cannot_claim_completion(self) -> None:
        tracker = VodDownloadProgress()
        self.assertFalse(tracker.consume_line("[download] 100.0% of 1.00GiB"))
        tracker.consume_line(progress_line("299", 1000, total=1000, finished=True))
        self.assertIsNone(tracker.progress)
        self.assertTrue(tracker.consume_line(VOD_DOWNLOAD_PROGRESS_PREFIX + "invalid json"))
        self.assertIsNone(tracker.progress)

    def test_command_requests_structured_plan_and_progress_without_simulating(self) -> None:
        config = BotConfig(extra_yt_dlp_args=["--cookies", "cookies.txt"])
        command = build_vod_download_command(config, "https://example.test/vod", Path("copy.%(ext)s"))
        self.assertIn("--no-simulate", command)
        self.assertIn("--no-quiet", command)
        self.assertIn("before_dl:" + VOD_DOWNLOAD_PLAN_PREFIX, command[command.index("--print") + 1])
        templates = [command[index + 1] for index, argument in enumerate(command) if argument == "--progress-template"]
        self.assertEqual(len(templates), 2)
        self.assertTrue(templates[0].startswith("download:" + VOD_DOWNLOAD_PROGRESS_PREFIX))
        self.assertTrue(templates[1].startswith("postprocess:" + VOD_DOWNLOAD_POSTPROCESS_PREFIX))
        self.assertTrue(all("url" not in template and "http_headers" not in template for template in templates))
        self.assertEqual(command[-1], "https://example.test/vod")

    def test_runner_uses_overall_progress_and_keeps_structured_lines_out_of_errors(self) -> None:
        class FakeProcess:
            stdout = iter([
                plan_line({"format_id": "299", "filesize": 900}, {"format_id": "140", "filesize": 100}),
                progress_line("299", 900, finished=True),
                progress_line("140", 50),
                "ERROR: failed to download audio",
            ])

            def wait(self) -> int:
                return 1

        with TemporaryDirectory() as tmp:
            config = BotConfig(download_dir=Path(tmp) / "downloads", state_dir=Path(tmp) / "state")
            stream = LiveStream(video_id="test", url="https://example.test/vod", title="VOD", channel="Example")
            with (
                patch("onlysavemevods.web.subprocess.Popen", return_value=FakeProcess()),
                patch("onlysavemevods.web.update_tracked_job") as update,
                patch("onlysavemevods.web.finish_failed_vod_download") as failure,
            ):
                run_vod_download_job(config, "job", stream, stream.url, Path(tmp) / "copy.%(ext)s")
        self.assertEqual([call.kwargs["progress"] for call in update.call_args_list if "progress" in call.kwargs], [None, 0.0, 0.9, 0.95])
        self.assertEqual(failure.call_args.args[3], "ERROR: failed to download audio")
        self.assertTrue(all(VOD_DOWNLOAD_PROGRESS_PREFIX not in call.kwargs.get("message", "") for call in update.call_args_list))

    def test_kick_chat_replay_does_not_reset_completed_media_progress(self) -> None:
        def replay(_stream: LiveStream, _output: Path, *, progress: object) -> object:
            progress("Downloading Kick chat replay", 0.1)
            progress("Downloading Kick chat replay", 0.7)
            return type("Result", (), {"ok": False, "message": "Unavailable"})()

        stream = LiveStream(video_id="kick:test", url="https://kick.com/vod", title="VOD", channel="Example", platform="kick")
        with (
            patch("onlysavemevods.web.update_tracked_job") as update,
            patch("onlysavemevods.web.download_kick_vod_chat_replay", side_effect=replay),
        ):
            run_kick_vod_chat_download_job(BotConfig(), "job", stream, Path("copy.%(ext)s"))
        self.assertEqual([call.kwargs["progress"] for call in update.call_args_list], [0.99, 0.99, 0.99])


if __name__ == "__main__":
    unittest.main()
