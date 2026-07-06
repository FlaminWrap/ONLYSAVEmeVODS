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


if __name__ == "__main__":
    unittest.main()
