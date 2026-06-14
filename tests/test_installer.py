from pathlib import Path
import unittest


INSTALL_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "install-almalinux.sh"


class SystemdInstallerTests(unittest.TestCase):
    def test_service_can_write_web_managed_config_file(self) -> None:
        script = INSTALL_SCRIPT.read_text(encoding="utf-8")

        self.assertIn("ensure_config_file_service_writable", script)
        self.assertIn("sudo chmod 0664 \"${CONFIG_FILE}\"", script)
        self.assertIn("sudo chown root:\"${service_group}\" \"${CONFIG_FILE}\"", script)
        self.assertIn("ReadWritePaths=${CACHE_DIR} ${DOWNLOAD_DIR} ${STATE_DIR} ${CONFIG_FILE}", script)


if __name__ == "__main__":
    unittest.main()
