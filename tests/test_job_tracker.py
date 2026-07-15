from unittest.mock import patch
import unittest

from onlysavemevods.job_tracker import (
    clear_tracked_jobs,
    finish_tracked_job,
    list_tracked_jobs,
    start_tracked_job,
    update_tracked_job,
)


class JobTrackerTests(unittest.TestCase):
    def tearDown(self) -> None:
        clear_tracked_jobs()

    def test_list_tracked_jobs_orders_by_start_time_not_update_time(self) -> None:
        with patch("onlysavemevods.job_tracker.time.time", side_effect=[100.0, 200.0, 300.0]):
            start_tracked_job(
                "old-job",
                kind="VOD download",
                video_id="VIDEO1",
                item="old.mp4",
            )
            start_tracked_job(
                "new-job",
                kind="VOD download",
                video_id="VIDEO2",
                item="new.mp4",
            )
            update_tracked_job("old-job", progress=0.5, message="Still running")

        jobs = list_tracked_jobs()

        self.assertEqual([job.job_id for job in jobs[:2]], ["new-job", "old-job"])
        self.assertEqual(jobs[1].updated_at, 300.0)

    def test_completed_jobs_expire_after_retention_window(self) -> None:
        with patch("onlysavemevods.job_tracker.time.time", return_value=100.0):
            start_tracked_job(
                "auto-transcription:VIDEO1:video.mp4",
                kind="Transcription",
                video_id="VIDEO1",
                item="video.mp4",
            )
        with patch("onlysavemevods.job_tracker.time.time", return_value=200.0):
            finish_tracked_job("auto-transcription:VIDEO1:video.mp4")

        with patch("onlysavemevods.job_tracker.time.time", return_value=499.0):
            self.assertEqual(len(list_tracked_jobs()), 1)
        with patch("onlysavemevods.job_tracker.time.time", return_value=500.0):
            self.assertEqual(list_tracked_jobs(), [])

    def test_failed_jobs_do_not_expire_with_successful_jobs(self) -> None:
        with patch("onlysavemevods.job_tracker.time.time", return_value=100.0):
            start_tracked_job(
                "auto-transcription:VIDEO1:video.mp4",
                kind="Transcription",
                video_id="VIDEO1",
                item="video.mp4",
            )
        with patch("onlysavemevods.job_tracker.time.time", return_value=200.0):
            finish_tracked_job(
                "auto-transcription:VIDEO1:video.mp4",
                status="failed",
                phase="Failed",
                message="WhisperX failed",
                progress=None,
            )

        with patch("onlysavemevods.job_tracker.time.time", return_value=10_000.0):
            jobs = list_tracked_jobs()

        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].status, "failed")


if __name__ == "__main__":
    unittest.main()
