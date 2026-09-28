"""HTTP boundary tests for the dashboard's force install action."""

import http.client
from http.server import ThreadingHTTPServer
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
import unittest
from urllib.parse import urlencode

from onlysavemevods.app_update import request_path, status_path
from onlysavemevods.config import BotConfig
from onlysavemevods.web import build_handler


class ForceInstallRouteTests(unittest.TestCase):
    def test_force_route_requires_confirmation_and_queues_force_request(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = BotConfig(
                state_dir=root / "state",
                config_path=root / "config.toml",
                web_host="127.0.0.1",
                app_update_mode="manual",
            )
            config.state_dir.mkdir()
            status_path(config).write_text(
                json.dumps({
                    "status": "update_available",
                    "latest_tag": "v999.0.0",
                    "latest_version": "999.0.0",
                }),
                encoding="utf-8",
            )
            with ThreadingHTTPServer(
                ("127.0.0.1", 0),
                build_handler(config, trusted_host="127.0.0.1"),
            ) as server:
                server.daemon_threads = True
                thread = Thread(target=server.serve_forever, daemon=True)
                thread.start()
                try:
                    def post(path: str, fields: dict[str, str]) -> int:
                        connection = http.client.HTTPConnection(
                            "127.0.0.1", server.server_port, timeout=5
                        )
                        try:
                            connection.request(
                                "POST",
                                path,
                                urlencode(fields),
                                {"Content-Type": "application/x-www-form-urlencoded"},
                            )
                            response = connection.getresponse()
                            response.read()
                            return response.status
                        finally:
                            connection.close()

                    self.assertEqual(
                        post("/app-update/request-force", {"tag": "v999.0.0"}),
                        400,
                    )
                    self.assertFalse(request_path(config).exists())
                    self.assertEqual(
                        post(
                            "/app-update/request-force",
                            {
                                "tag": "v999.0.0",
                                "confirm_force": "interrupt-active-work",
                            },
                        ),
                        303,
                    )
                    request = json.loads(request_path(config).read_text(encoding="utf-8"))
                    self.assertEqual(request["tag"], "v999.0.0")
                    self.assertEqual(request["source"], "manual")
                    self.assertIs(request["force"], True)
                    self.assertEqual(
                        post("/app-update/request", {"tag": "v999.0.0"}),
                        400,
                    )
                    request = json.loads(request_path(config).read_text(encoding="utf-8"))
                    self.assertIs(request["force"], True)
                finally:
                    server.shutdown()
                    thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
