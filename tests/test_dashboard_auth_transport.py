from __future__ import annotations

import base64
import json
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock


SRC_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from others.dashboard_http import DashboardHTTPServer  # noqa: E402


CONTROL_TOKEN = "dashboard-regression-control-token"


@contextmanager
def running_dashboard(*, token: str = CONTROL_TOKEN, protocol_url: str = "http://example.test"):
    with tempfile.TemporaryDirectory() as directory:
        server = DashboardHTTPServer(
            listen="127.0.0.1:0",
            shared_root=Path(directory),
            easy_protocol_base_url=protocol_url,
            easy_protocol_token=token,
            easy_protocol_actor="dashboard-test",
            recent_window_seconds=900,
        )
        server.start()
        try:
            yield server, f"http://127.0.0.1:{server._httpd.server_address[1]}"
        finally:
            server.stop()


@contextmanager
def protocol_endpoint(*, redirect: bool = False):
    requests: list[tuple[str, str]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            requests.append((self.path, self.headers.get("Authorization", "")))
            if redirect and self.path == "/api/internal/stats":
                self.send_response(302)
                self.send_header("Location", "/unexpected-redirect-target")
                self.end_headers()
                return
            body = b'{"services":[{"service":"PythonProtocol-test","success_count":7}]}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_address[1]}", requests
        finally:
            server.shutdown()
            thread.join(timeout=2)


class DashboardAuthTransportTests(unittest.TestCase):
    def test_browser_authentication_covers_page_and_status_fetch(self) -> None:
        with running_dashboard() as (server, base):
            with mock.patch.object(server, "_fetch_easy_protocol_stats", return_value={}):
                with self.assertRaises(urllib.error.HTTPError) as denied:
                    urllib.request.urlopen(base + "/", timeout=3)
                self.assertEqual(401, denied.exception.code)
                self.assertIn("Basic", denied.exception.headers.get("WWW-Authenticate", ""))
                denied.exception.close()

                passwords = urllib.request.HTTPPasswordMgrWithDefaultRealm()
                passwords.add_password(None, base, "dashboard", CONTROL_TOKEN)
                browser = urllib.request.build_opener(urllib.request.HTTPBasicAuthHandler(passwords))
                with browser.open(base + "/", timeout=3) as response:
                    page = response.read().decode("utf-8")
                with browser.open(base + "/api/status", timeout=3) as response:
                    status = json.load(response)

        self.assertIn("Register Dashboard", page)
        self.assertNotIn(CONTROL_TOKEN, page)
        self.assertIn("pipelines", status)

    def test_status_rejects_missing_malformed_and_wrong_credentials(self) -> None:
        with running_dashboard() as (server, base):
            with mock.patch.object(server, "_build_status_payload") as build_status:
                for authorization in (
                    "",
                    "Bearer ",
                    "Bearer wrong-token",
                    "Basic !!!not-base64!!!",
                    "Basic " + base64.b64encode(b"dashboard:wrong-token").decode("ascii"),
                ):
                    with self.subTest(authorization=authorization):
                        request = urllib.request.Request(
                            base + "/api/status", headers={"Authorization": authorization}
                        )
                        with self.assertRaises(urllib.error.HTTPError) as denied:
                            urllib.request.urlopen(request, timeout=3)
                        self.assertEqual(401, denied.exception.code)
                        denied.exception.close()
                build_status.assert_not_called()

    def test_empty_configured_token_cannot_authorize_an_empty_bearer(self) -> None:
        with running_dashboard(token="") as (server, base):
            with mock.patch.object(server, "_build_status_payload", return_value={}):
                request = urllib.request.Request(
                    base + "/api/status", headers={"Authorization": "Bearer "}
                )
                with self.assertRaises(urllib.error.HTTPError) as denied:
                    urllib.request.urlopen(request, timeout=3)
                self.assertEqual(401, denied.exception.code)
                denied.exception.close()

    def test_bearer_authentication_returns_status_without_exposing_token(self) -> None:
        with running_dashboard() as (server, base):
            with mock.patch.object(server, "_fetch_easy_protocol_stats", return_value={}):
                request = urllib.request.Request(
                    base + "/api/status", headers={"Authorization": f"Bearer {CONTROL_TOKEN}"}
                )
                with urllib.request.urlopen(request, timeout=3) as response:
                    body = response.read().decode("utf-8")
        self.assertIn("pipelines", json.loads(body))
        self.assertNotIn(CONTROL_TOKEN, body)

    def test_configured_loopback_protocol_stats_remain_reachable(self) -> None:
        with protocol_endpoint() as (protocol_url, requests):
            with running_dashboard(protocol_url=protocol_url + "/api/public/request") as (server, _):
                stats = server._fetch_easy_protocol_stats()
        self.assertEqual(7, stats["services"][0]["success_count"])
        self.assertEqual([("/api/internal/stats", f"Bearer {CONTROL_TOKEN}")], requests)

    def test_protocol_stats_do_not_follow_redirects_with_control_credentials(self) -> None:
        with protocol_endpoint(redirect=True) as (protocol_url, requests):
            with running_dashboard(protocol_url=protocol_url) as (server, _):
                self.assertEqual({}, server._fetch_easy_protocol_stats())
        self.assertEqual([("/api/internal/stats", f"Bearer {CONTROL_TOKEN}")], requests)


if __name__ == "__main__":
    unittest.main()
