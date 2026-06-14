from pathlib import Path
from tempfile import TemporaryDirectory
import tomllib
import unittest

from onlysavemevods.models import LiveStream
from onlysavemevods.python_update import (
    idle_result_from_state,
    idle_result_from_status_snapshot,
    render_python_update_service_unit,
    render_python_update_timer_unit,
)
from onlysavemevods.state import StateStore


class PythonUpdateIdleTests(unittest.TestCase):
    def test_status_snapshot_idle_when_no_busy_streams_or_jobs(self) -> None:
        result = idle_result_from_status_snapshot(
            {
                "counts": {"ended": 2, "interrupted": 1},
                "jobs": [{"status": "done"}, {"status": "failed"}],
            }
        )

        self.assertTrue(result.known)
        self.assertTrue(result.idle)
        self.assertEqual(result.exit_code, 0)

    def test_status_snapshot_busy_for_active_streams_and_jobs(self) -> None:
        result = idle_result_from_status_snapshot(
            {
                "counts": {"downloading": 1, "checking_after_exit": "2"},
                "jobs": [{"status": "queued"}, {"status": "done"}],
            }
        )

        self.assertTrue(result.known)
        self.assertFalse(result.idle)
        self.assertEqual(result.exit_code, 1)
        self.assertIn("downloading streams=1", result.reasons)
        self.assertIn("checking_after_exit streams=2", result.reasons)
        self.assertIn("active jobs=1", result.reasons)

    def test_status_snapshot_unknown_without_jobs(self) -> None:
        result = idle_result_from_status_snapshot({"counts": {}})

        self.assertFalse(result.known)
        self.assertFalse(result.idle)
        self.assertEqual(result.exit_code, 2)

    def test_state_idle_detection_uses_sqlite_streams_and_watermarks(self) -> None:
        with TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "state.sqlite3"
            state = StateStore(db_path)
            stream = LiveStream(
                video_id="LIVEVIDEO01",
                url="https://www.youtube.com/watch?v=LIVEVIDEO01",
                title="Live",
                channel="Example",
            )
            state.upsert_detected(stream)
            idle = idle_result_from_state(db_path)

            state.mark_downloading(stream, 1)
            downloading = idle_result_from_state(db_path)

            state.mark_ended(stream.video_id)
            state.create_watermark_copy(
                copy_id="wm_copy001",
                video_id=stream.video_id,
                source_name="Live [LIVEVIDEO01].mp4",
                output_name=".watermarks/Live [LIVEVIDEO01] - wm-copy001.mp4",
                recipient_label="Recipient A",
            )
            watermark = idle_result_from_state(db_path)
            state.close()

        self.assertTrue(idle.known)
        self.assertTrue(idle.idle)
        self.assertTrue(downloading.known)
        self.assertFalse(downloading.idle)
        self.assertIn("downloading streams=1", downloading.reasons)
        self.assertTrue(watermark.known)
        self.assertFalse(watermark.idle)
        self.assertIn("active watermark jobs=1", watermark.reasons)


class PythonUpdateUnitTests(unittest.TestCase):
    def test_generated_units_include_schedule_and_install_paths(self) -> None:
        service = render_python_update_service_unit(
            install_dir="/srv/onlysavemevods",
            app_dir="/srv/onlysavemevods/app",
            venv_dir="/srv/onlysavemevods/.venv",
            config_file="/srv/onlysavemevods/config.toml",
            main_service_name="onlysavemevods.service",
        )
        timer = render_python_update_timer_unit(
            update_service_name="onlysavemevods-python-update.service",
            calendar="*-*-* 04:15:00",
            random_delay="45m",
        )

        self.assertIn('Environment="ONLYSAVEMEVODS_INSTALL_DIR=/srv/onlysavemevods"', service)
        self.assertIn("ExecStart=/srv/onlysavemevods/app/scripts/update-python-deps.sh", service)
        self.assertIn("OnCalendar=*-*-* 04:15:00", timer)
        self.assertIn("RandomizedDelaySec=45m", timer)
        self.assertIn("Persistent=true", timer)
        self.assertIn("Unit=onlysavemevods-python-update.service", timer)


class PythonUpdateScriptTests(unittest.TestCase):
    def test_voice_match_extra_uses_whisperx_compatible_pins(self) -> None:
        pyproject = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
        extra = pyproject["project"]["optional-dependencies"]["voice-match"]

        self.assertIn("pyannote.audio>=3.1.1,<4.0", extra)
        self.assertIn("huggingface-hub>=0.34.0,<1.0", extra)
        self.assertIn("torch~=2.8.0", extra)
        self.assertIn("torchaudio~=2.8.0", extra)
        self.assertTrue(any(item.startswith("torchcodec>=0.6.0,<0.8.0") for item in extra))

    def test_installer_can_install_voice_match_extra(self) -> None:
        script = Path("scripts/install-almalinux.sh").read_text(encoding="utf-8")

        self.assertIn("ONLYSAVEMEVODS_INSTALL_VOICE_MATCH", script)
        self.assertIn("config.voice_match_enabled", script)
        self.assertIn('"${APP_DIR}[voice-match]"', script)
        self.assertIn("install_voice_match_if_needed", script)
        self.assertNotIn('--upgrade-strategy eager --editable "${APP_DIR}[voice-match]"', script)

    def test_python_updater_refreshes_voice_match_extra(self) -> None:
        script = Path("scripts/update-python-deps.sh").read_text(encoding="utf-8")

        self.assertIn("config_enables_voice_match", script)
        self.assertIn("voice_match_dependency_installed", script)
        self.assertIn('"${APP_DIR}[voice-match]"', script)
        self.assertNotIn('--upgrade-strategy eager --editable "${APP_DIR}[voice-match]"', script)


if __name__ == "__main__":
    unittest.main()
