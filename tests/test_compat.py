from pathlib import Path
import importlib
import os
import subprocess
import sys
import unittest


class CompatibilityTests(unittest.TestCase):
    def test_old_module_import_aliases_new_module_object(self) -> None:
        new_config = importlib.import_module("onlysavemevods.config")
        old_config = importlib.import_module("ytdlbot.config")
        new_web = importlib.import_module("onlysavemevods.web")
        old_web = importlib.import_module("ytdlbot.web")
        from ytdlbot import config as old_from_config

        self.assertIs(old_config, new_config)
        self.assertIs(old_web, new_web)
        self.assertIs(old_from_config, new_config)

    def test_old_daemon_class_name_aliases_new_class(self) -> None:
        daemon = importlib.import_module("onlysavemevods.daemon")

        self.assertIs(daemon.YTDLBotDaemon, daemon.OnlySaveMeVodsDaemon)

    def test_python_m_ytdlbot_dispatches_to_new_cli(self) -> None:
        root = Path(__file__).resolve().parents[1]
        env = os.environ.copy()
        env["PYTHONPATH"] = (
            str(root / "src")
            + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
        )

        completed = subprocess.run(
            [sys.executable, "-m", "ytdlbot", "--help"],
            capture_output=True,
            check=False,
            env=env,
            text=True,
        )

        self.assertEqual(completed.returncode, 0)
        self.assertIn("usage: onlysavemevods", completed.stdout)


if __name__ == "__main__":
    unittest.main()
