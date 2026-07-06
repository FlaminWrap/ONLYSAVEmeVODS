from pathlib import Path
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
        self.assertIn("PathExists=${STATE_DIR}/app-update-request.json", script)
        self.assertIn("ExecStart=${APP_DIR}/scripts/app-update.sh", script)
        self.assertIn("EnvironmentFile=${SECRETS_FILE}", script)
        self.assertIn('sudo systemctl enable "${APP_UPDATE_PATH_NAME}" --now', script)
        self.assertIn('sudo systemctl enable "${APP_UPDATE_TIMER_NAME}" --now', script)


if __name__ == "__main__":
    unittest.main()
