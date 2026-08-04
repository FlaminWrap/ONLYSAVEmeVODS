from pathlib import Path
from tempfile import TemporaryDirectory
import subprocess
import unittest


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
INSTALL_SCRIPT = SCRIPTS_DIR / "install-systemd.sh"


class SystemdInstallerTests(unittest.TestCase):
    def test_service_can_write_web_managed_config_file(self) -> None:
        script = INSTALL_SCRIPT.read_text(encoding="utf-8")

        self.assertIn("ensure_config_file_service_writable", script)
        self.assertIn("sudo chmod 0664 \"${CONFIG_FILE}\"", script)
        self.assertIn("sudo chown root:\"${service_group}\" \"${CONFIG_FILE}\"", script)
        self.assertIn("ReadWritePaths=${CACHE_DIR} ${DOWNLOAD_DIR} ${STATE_DIR} ${CONFIG_FILE}", script)

    def test_distro_installers_delegate_to_shared_systemd_installer(self) -> None:
        for name in ("install-almalinux.sh", "install-debian.sh", "install-ubuntu.sh"):
            with self.subTest(name=name):
                script = (SCRIPTS_DIR / name).read_text(encoding="utf-8")

                self.assertIn('exec "${SCRIPT_DIR}/install-systemd.sh" "$@"', script)
                self.assertNotIn('exec "${SCRIPT_DIR}/install-almalinux.sh" "$@"', script)

    def test_installer_writes_app_update_systemd_units(self) -> None:
        script = INSTALL_SCRIPT.read_text(encoding="utf-8")

        self.assertIn("onlysavemevods-app-update.service", script)
        self.assertIn("onlysavemevods-app-update.path", script)
        self.assertIn("onlysavemevods-app-update.timer", script)
        self.assertIn('APP_UPDATE_STATE_DIR="${INSTALL_DIR}/state"', script)
        self.assertIn(
            "PathExists=${APP_UPDATE_STATE_DIR}/app-update-request.json",
            script,
        )
        self.assertIn("ExecStart=/usr/bin/env bash ${APP_DIR}/scripts/app-update.sh", script)
        self.assertIn("EnvironmentFile=${SECRETS_FILE}", script)
        self.assertEqual(
            script.count(
                'Environment="ONLYSAVEMEVODS_APP_UPDATE_STATE_DIR=${APP_UPDATE_STATE_DIR}"'
            ),
            2,
        )
        self.assertIn(
            "ONLYSAVEMEVODS_TRUSTED_APP_UPDATE_REPOSITORY=${TRUSTED_APP_UPDATE_REPOSITORY}",
            script,
        )
        self.assertIn(
            "ONLYSAVEMEVODS_TRUSTED_APP_UPDATE_MODE=${TRUSTED_APP_UPDATE_MODE}",
            script,
        )
        self.assertIn('sudo systemctl enable "${APP_UPDATE_PATH_NAME}" --now', script)
        self.assertIn('sudo systemctl enable "${APP_UPDATE_TIMER_NAME}" --now', script)

        app_updater = (SCRIPTS_DIR / "app-update.sh").read_text(encoding="utf-8")
        self.assertIn("ONLYSAVEMEVODS_APP_UPDATE_STATE_DIR", app_updater)
        self.assertEqual(app_updater.count('--state-dir "${APP_UPDATE_STATE_DIR}"'), 3)

    def test_installer_and_updaters_share_stale_safe_flock(self) -> None:
        installer = INSTALL_SCRIPT.read_text(encoding="utf-8")
        app_updater = (SCRIPTS_DIR / "app-update.sh").read_text(encoding="utf-8")
        python_updater = (SCRIPTS_DIR / "update-python-deps.sh").read_text(encoding="utf-8")

        for script in (installer, app_updater, python_updater):
            self.assertIn("ONLYSAVEMEVODS_UPDATE_LOCK_FILE", script)
            self.assertIn("flock", script)
        self.assertNotIn(".app-update.lock", app_updater)
        self.assertNotIn(".python-update.lock", python_updater)

    def test_installer_stops_service_and_stages_before_replacing_live_app(self) -> None:
        script = INSTALL_SCRIPT.read_text(encoding="utf-8")

        self.assertLess(script.rindex("take_update_lock\n"), script.rindex("stop_services_for_upgrade\n"))
        self.assertLess(script.rindex("stop_services_for_upgrade\n"), script.rindex("install_application_files\n"))
        self.assertLess(script.rindex("if source_is_inside_install_tree; then"), script.rindex("stage_source_if_inside_install_tree\n"))
        self.assertIn(".app-install-stage.XXXXXXXX", script)
        self.assertIn('"${ROOT_DIR}/LICENSE"', script)
        self.assertIn('sudo mv "${staged_app}" "${APP_DIR}"', script)
        self.assertIn('sudo chmod -R a+rX "${staged_app}"', script)
        self.assertIn('sudo restorecon -RF "${APP_DIR}"', script)
        self.assertIn('if ! sudo mv "${displaced_app}" "${APP_DIR}"; then', script)
        self.assertIn(
            'if [[ "${exit_code}" -ne 0 && -n "${DISPLACED_APP_DIR}" ]]',
            script,
        )
        self.assertLess(
            script.rindex('sudo systemctl enable "${APP_UPDATE_TIMER_NAME}" --now'),
            script.rindex("commit_application_files\n"),
        )

    def test_later_failure_restores_displaced_application(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source"
            install_dir = root / "install"
            for directory in (source / "src", source / "scripts", source / "tests"):
                directory.mkdir(parents=True)
            for filename in (
                "pyproject.toml",
                "README.md",
                "LICENSE",
                "config.example.toml",
            ):
                (source / filename).write_text(f"new {filename}\n", encoding="utf-8")
            old_app = install_dir / "app"
            old_app.mkdir(parents=True)
            (old_app / "old-version.txt").write_text("old\n", encoding="utf-8")

            harness = r'''
source "$1"
ROOT_DIR="$2"
INSTALL_DIR="$3"
APP_DIR="${INSTALL_DIR}/app"
CACHE_DIR="${INSTALL_DIR}/.cache"
DOWNLOAD_DIR="${INSTALL_DIR}/downloads"
STATE_DIR="${INSTALL_DIR}/state"
SERVICE_USER="$(id -un)"
SERVICE_WAS_ACTIVE=0
SERVICE_RESTARTED=0
DISPLACED_APP_DIR=""
APP_ROLLBACK_FAILED=0

sudo() {
  case "$1" in
    chown)
      return 0
      ;;
    systemctl)
      return 1
      ;;
    install)
      shift
      local -a filtered=()
      while [[ "$#" -gt 0 ]]; do
        case "$1" in
          -o|-g) shift 2 ;;
          *) filtered+=("$1"); shift ;;
        esac
      done
      command install "${filtered[@]}"
      ;;
    *)
      command "$@"
      ;;
  esac
}

install_application_files
[[ -f "${APP_DIR}/pyproject.toml" ]]
[[ ! -f "${APP_DIR}/old-version.txt" ]]
[[ -n "${DISPLACED_APP_DIR}" ]]
[[ "$(stat -c '%a' "${APP_DIR}")" == "755" ]]
trap installer_cleanup EXIT
exit 37
'''
            result = subprocess.run(
                [
                    "bash",
                    "-c",
                    harness,
                    "installer-rollback-test",
                    str(INSTALL_SCRIPT),
                    str(source),
                    str(install_dir),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 37, result.stderr)
            self.assertEqual(
                (install_dir / "app" / "old-version.txt").read_text(encoding="utf-8"),
                "old\n",
            )
            self.assertFalse((install_dir / "app" / "pyproject.toml").exists())
            self.assertEqual(list(install_dir.glob(".app-install-old.*")), [])
            self.assertEqual(list(install_dir.glob(".app-install-failed.*")), [])


if __name__ == "__main__":
    unittest.main()
