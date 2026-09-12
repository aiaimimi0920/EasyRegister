from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest import mock


SRC_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from others import runtime  # noqa: E402
import dashboard_server  # noqa: E402


class SecurityDefaultsTests(unittest.TestCase):
    def test_ensure_easy_email_env_defaults_does_not_scan_retired_local_service(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            with mock.patch.object(Path, "exists", return_value=True), \
                mock.patch.object(Path, "read_text", return_value='apiKey: "retired-key"') as read_text:
                runtime.ensure_easy_email_env_defaults()
            read_text.assert_not_called()
            self.assertEqual("http://192.168.15.200:18081", os.environ.get("MAILBOX_SERVICE_BASE_URL"))
            self.assertNotIn("MAILBOX_SERVICE_API_KEY", os.environ)

    def test_ensure_easy_email_env_defaults_prefers_nas_sdk_names(self) -> None:
        env = {
            "EASY_EMAIL_BASE_URL": "http://easy-email-service:8080",
            "EASY_EMAIL_API_KEY": "nas-token",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            runtime.ensure_easy_email_env_defaults()

            self.assertEqual("http://easy-email-service:8080", os.environ.get("MAILBOX_SERVICE_BASE_URL"))
            self.assertEqual("nas-token", os.environ.get("MAILBOX_SERVICE_API_KEY"))
            self.assertEqual("nas-token", os.environ.get("EASY_EMAIL_API_KEY"))

    def test_ensure_easy_email_env_defaults_does_not_inject_hardcoded_key(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            runtime.ensure_easy_email_env_defaults()

            self.assertEqual("http://192.168.15.200:18081", os.environ.get("MAILBOX_SERVICE_BASE_URL"))
            self.assertEqual("http://192.168.15.200:18081", os.environ.get("EASY_EMAIL_BASE_URL"))
            self.assertNotIn("MAILBOX_SERVICE_API_KEY", os.environ)
            self.assertNotIn("EASY_EMAIL_API_KEY", os.environ)

    def test_ensure_easy_email_env_defaults_normalizes_blank_and_conflicting_aliases(self) -> None:
        with mock.patch.dict(os.environ, {
            "MAILBOX_SERVICE_BASE_URL": "  ",
            "MAILBOX_SERVICE_API_KEY": " ",
            "EASY_EMAIL_BASE_URL": "http://192.168.15.200:18081",
            "EASY_EMAIL_API_KEY": "nas-token",
        }, clear=True):
            runtime.ensure_easy_email_env_defaults()
            self.assertEqual(os.environ["EASY_EMAIL_BASE_URL"], os.environ["MAILBOX_SERVICE_BASE_URL"])
            self.assertEqual("nas-token", os.environ["MAILBOX_SERVICE_API_KEY"])
            os.environ["MAILBOX_SERVICE_API_KEY"] = "explicit-legacy-name-token"
            runtime.ensure_easy_email_env_defaults()
            self.assertEqual("explicit-legacy-name-token", os.environ["EASY_EMAIL_API_KEY"])

    def test_dashboard_disabled_without_secure_token(self) -> None:
        with mock.patch.dict(os.environ, {"REGISTER_DASHBOARD_ENABLED": "true"}, clear=True):
            with mock.patch.object(dashboard_server, "DashboardHTTPServer") as server_cls:
                server = dashboard_server.start_dashboard_server_if_enabled(
                    output_root=Path("tmp"),
                    easy_protocol_base_url="http://example.test",
                    easy_protocol_token="",
                    easy_protocol_actor="actor",
                )

            self.assertIsNone(server)
            server_cls.assert_not_called()

    def test_dashboard_defaults_to_localhost_listen(self) -> None:
        with mock.patch.dict(os.environ, {"REGISTER_DASHBOARD_ENABLED": "true"}, clear=True):
            with mock.patch.object(dashboard_server, "DashboardHTTPServer") as server_cls:
                server_instance = server_cls.return_value
                server = dashboard_server.start_dashboard_server_if_enabled(
                    output_root=Path("tmp"),
                    easy_protocol_base_url="http://example.test",
                    easy_protocol_token="secure-token-16ch",
                    easy_protocol_actor="actor",
                )

            self.assertIs(server, server_instance)
            self.assertEqual("127.0.0.1:9790", server_cls.call_args.kwargs["listen"])
            server_instance.start.assert_called_once_with()

    def test_dashboard_rejects_remote_listen_without_opt_in(self) -> None:
        env = {
            "REGISTER_DASHBOARD_ENABLED": "true",
            "REGISTER_DASHBOARD_LISTEN": "0.0.0.0:9790",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch.object(dashboard_server, "DashboardHTTPServer") as server_cls:
                server = dashboard_server.start_dashboard_server_if_enabled(
                    output_root=Path("tmp"),
                    easy_protocol_base_url="http://example.test",
                    easy_protocol_token="secure-token-16ch",
                    easy_protocol_actor="actor",
                )

            self.assertIsNone(server)
            server_cls.assert_not_called()

    def test_dashboard_allows_remote_listen_with_opt_in(self) -> None:
        env = {
            "REGISTER_DASHBOARD_ENABLED": "true",
            "REGISTER_DASHBOARD_LISTEN": "0.0.0.0:9790",
            "REGISTER_DASHBOARD_ALLOW_REMOTE": "true",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch.object(dashboard_server, "DashboardHTTPServer") as server_cls:
                server_instance = server_cls.return_value
                server = dashboard_server.start_dashboard_server_if_enabled(
                    output_root=Path("tmp"),
                    easy_protocol_base_url="http://example.test",
                    easy_protocol_token="secure-token-16ch",
                    easy_protocol_actor="actor",
                )

            self.assertIs(server, server_instance)
            self.assertEqual("0.0.0.0:9790", server_cls.call_args.kwargs["listen"])


if __name__ == "__main__":
    unittest.main()
