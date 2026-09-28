"""Behavior checks for the systemd app updater wrapper."""

import os
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "app-update.sh"


class AppUpdateScriptTests(unittest.TestCase):
    def run_updater(
        self,
        *,
        intent: str = "normal",
        active: bool = True,
        idle_code: int = 0,
        intent_code: int = 0,
        apply_code: int = 0,
        recreate_trigger: bool = False,
        missing_python: bool = False,
        lock_busy: bool = False,
        pending_request: bool = True,
        auto_creates_request: bool = False,
    ) -> tuple[subprocess.CompletedProcess[str], list[str], bool]:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            install_dir = root / "install"
            app_dir = install_dir / "app"
            venv_bin = install_dir / ".venv" / "bin"
            fake_bin = root / "fake-bin"
            for directory in (app_dir, venv_bin, fake_bin):
                directory.mkdir(parents=True)
            config_file = install_dir / "config.toml"
            config_file.write_text("", encoding="utf-8")
            log_file = root / "calls.log"
            state_dir = root / "state"
            state_dir.mkdir()
            trigger = state_dir / "app-update-trigger"
            trigger.touch()
            if pending_request:
                (state_dir / "app-update-request.json").write_text("{}", encoding="utf-8")

            fake_python = venv_bin / "python"
            fake_python.write_text(
                "#!/usr/bin/env bash\n"
                "printf 'python %s\\n' \"${3:-}\" >>\"${FAKE_LOG}\"\n"
                "case \"${3:-}\" in\n"
                "  check-trusted-auto)\n"
                "    if [[ \"${FAKE_AUTO_CREATES_REQUEST}\" == 1 ]]; then printf '{}\\n' >\"${ONLYSAVEMEVODS_APP_UPDATE_STATE_DIR}/app-update-request.json\"; fi\n"
                "    exit 0 ;;\n"
                "  has-request) [[ -f \"${ONLYSAVEMEVODS_APP_UPDATE_STATE_DIR}/app-update-request.json\" ]] ;;\n"
                "  request-intent)\n"
                "    if [[ \"${FAKE_INTENT_CODE}\" != 0 ]]; then exit \"${FAKE_INTENT_CODE}\"; fi\n"
                "    if [[ \"${FAKE_RECREATE_TRIGGER}\" == 1 ]]; then touch \"${ONLYSAVEMEVODS_APP_UPDATE_STATE_DIR}/app-update-trigger\"; fi\n"
                "    printf '%s\\n' \"${FAKE_REQUEST_INTENT}\" ;;\n"
                "  check-idle) exit \"${FAKE_IDLE_CODE}\" ;;\n"
                "  apply) exit \"${FAKE_APPLY_CODE}\" ;;\n"
                "  *) exit 99 ;;\n"
                "esac\n",
                encoding="utf-8",
            )
            fake_python.chmod(0o644 if missing_python else 0o755)
            if lock_busy:
                flock = fake_bin / "flock"
                flock.write_text("#!/usr/bin/env bash\nexit 1\n", encoding="utf-8")
                flock.chmod(0o755)
            systemctl = fake_bin / "systemctl"
            systemctl.write_text(
                "#!/usr/bin/env bash\n"
                "printf 'systemctl %s\\n' \"${1:-}\" >>\"${FAKE_LOG}\"\n"
                "if [[ \"${1:-}\" == is-active ]]; then\n"
                "  [[ \"${FAKE_SERVICE_ACTIVE}\" == 1 ]]\n"
                "fi\n",
                encoding="utf-8",
            )
            systemctl.chmod(0o755)

            # The wrapper's root guard concerns the actual service install. This
            # copy exercises its logic with isolated fake binaries as any test user.
            script_text = SCRIPT.read_text(encoding="utf-8")
            marker = "\nrequire_root\n"
            self.assertIn(marker, script_text)
            test_script = root / "app-update.sh"
            test_script.write_text(script_text.replace(marker, "\n", 1), encoding="utf-8")
            env = os.environ.copy()
            env.update(
                {
                    "PATH": f"{fake_bin}:{env['PATH']}",
                    "ONLYSAVEMEVODS_INSTALL_DIR": str(install_dir),
                    "ONLYSAVEMEVODS_APP_DIR": str(app_dir),
                    "ONLYSAVEMEVODS_VENV_DIR": str(venv_bin.parent),
                    "ONLYSAVEMEVODS_CONFIG_FILE": str(config_file),
                    "ONLYSAVEMEVODS_APP_UPDATE_STATE_DIR": str(state_dir),
                    "FAKE_RECREATE_TRIGGER": "1" if recreate_trigger else "0",
                    "FAKE_AUTO_CREATES_REQUEST": "1" if auto_creates_request else "0",
                    "FAKE_LOG": str(log_file),
                    "FAKE_REQUEST_INTENT": intent,
                    "FAKE_SERVICE_ACTIVE": "1" if active else "0",
                    "FAKE_IDLE_CODE": str(idle_code),
                    "FAKE_INTENT_CODE": str(intent_code),
                    "FAKE_APPLY_CODE": str(apply_code),
                }
            )
            result = subprocess.run(
                ["bash", str(test_script)],
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )
            calls = log_file.read_text(encoding="utf-8").splitlines() if log_file.exists() else []
            return result, calls, trigger.exists()

    def test_force_request_stops_busy_service_then_restarts_after_apply(self) -> None:
        result, calls, trigger_exists = self.run_updater(intent="force", idle_code=1)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Active recordings will be interrupted", result.stderr)
        self.assertEqual(
            calls,
            [
                "python has-request",
                "python request-intent",
                "systemctl is-active",
                "systemctl stop",
                "python apply",
                "systemctl start",
            ],
        )

    def test_no_pending_request_checks_trusted_auto(self) -> None:
        result, calls, trigger_exists = self.run_updater(pending_request=False)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            calls,
            ["python has-request", "python check-trusted-auto", "python has-request"],
        )
        self.assertFalse(trigger_exists)

    def test_trusted_auto_request_proceeds_to_apply(self) -> None:
        result, calls, trigger_exists = self.run_updater(
            pending_request=False,
            auto_creates_request=True,
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            calls[:4],
            ["python has-request", "python check-trusted-auto", "python has-request", "python request-intent"],
        )
        self.assertIn("python apply", calls)

    def test_normal_request_keeps_busy_service_running(self) -> None:
        result, calls, trigger_exists = self.run_updater(intent="normal", idle_code=1)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("app update remains pending", result.stdout)
        self.assertFalse(trigger_exists, "busy skip must not retrigger the path unit")
        self.assertEqual(calls[-2:], ["systemctl is-active", "python check-idle"])
        self.assertNotIn("systemctl stop", calls)
        self.assertNotIn("python apply", calls)

    def test_invalid_intent_fails_before_service_is_stopped(self) -> None:
        for intent, intent_code in (("unexpected", 0), ("force", 2)):
            with self.subTest(intent=intent, intent_code=intent_code):
                result, calls, trigger_exists = self.run_updater(intent=intent, intent_code=intent_code)
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn("systemctl stop", calls)
                self.assertNotIn("python apply", calls)

    def test_force_request_restarts_service_if_apply_fails(self) -> None:
        result, calls, trigger_exists = self.run_updater(intent="force", apply_code=7)

        self.assertEqual(result.returncode, 7)
        self.assertEqual(calls[-3:], ["systemctl stop", "python apply", "systemctl start"])

    def test_trigger_created_after_consumption_remains_for_next_start(self) -> None:
        result, calls, trigger_exists = self.run_updater(recreate_trigger=True)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(trigger_exists)
        self.assertIn("python apply", calls)

    def test_early_exit_consumes_trigger_to_avoid_path_restart_loop(self) -> None:
        for options in ({"missing_python": True}, {"lock_busy": True}):
            with self.subTest(options=options):
                result, calls, trigger_exists = self.run_updater(**options)
                self.assertFalse(trigger_exists)
                self.assertEqual(calls, [])
                self.assertNotIn("systemctl stop", calls)
                self.assertEqual(result.returncode, 1 if options.get("missing_python") else 0)


if __name__ == "__main__":
    unittest.main()
