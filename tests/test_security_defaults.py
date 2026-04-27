from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SRC_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from others import local_config, runtime  # noqa: E402
import dashboard_server  # noqa: E402


class SecurityDefaultsTests(unittest.TestCase):
    def test_read_easyemail_server_api_key_from_local_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            probe_path = root / "server" / "services" / "orchestration_service" / "src" / "others" / "probe.py"
            probe_path.parent.mkdir(parents=True, exist_ok=True)
            probe_path.write_text("", encoding="utf-8")
            config_path = root / "server" / "EmailService" / "deploy" / "EasyEmail" / "config.yaml"
            config_path.parent.mkdir(parents=True, exist_ok=True)
            config_path.write_text('apiKey: "discovered-secret"\n', encoding="utf-8")

            self.assertEqual(
                "discovered-secret",
                local_config.read_easyemail_server_api_key(start_path=probe_path),
            )

    def test_ensure_easy_email_env_defaults_discovers_local_key(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            with mock.patch.object(runtime, "read_easyemail_server_api_key", return_value="mailbox-key"):
                runtime.ensure_easy_email_env_defaults()

            self.assertEqual("http://localhost:18080", os.environ.get("MAILBOX_SERVICE_BASE_URL"))
            self.assertEqual("mailbox-key", os.environ.get("MAILBOX_SERVICE_API_KEY"))

    def test_ensure_easy_email_env_defaults_does_not_inject_hardcoded_key(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            with mock.patch.object(runtime, "read_easyemail_server_api_key", return_value=""):
                runtime.ensure_easy_email_env_defaults()

            self.assertEqual("http://localhost:18080", os.environ.get("MAILBOX_SERVICE_BASE_URL"))
            self.assertNotIn("MAILBOX_SERVICE_API_KEY", os.environ)

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
                    easy_protocol_token="secure-token",
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
                    easy_protocol_token="secure-token",
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
                    easy_protocol_token="secure-token",
                    easy_protocol_actor="actor",
                )

            self.assertIs(server, server_instance)
            self.assertEqual("0.0.0.0:9790", server_cls.call_args.kwargs["listen"])


if __name__ == "__main__":
    unittest.main()
