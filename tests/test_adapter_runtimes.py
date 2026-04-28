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

from others import easyemail_runtime, easyprotocol_runtime, runtime_proxy_support  # noqa: E402


class EasyProtocolRuntimeTests(unittest.TestCase):
    def test_dispatch_revoke_codex_member_can_skip_for_manual_oauth_preserve(self) -> None:
        result = easyprotocol_runtime.dispatch_easyprotocol_step(
            step_type="revoke_codex_member",
            step_input={
                "error_code": "token_invalidated",
                "preserve_enabled": True,
                "preserve_on_error_codes": "token_invalidated,other_code",
                "invite_email": "user@example.com",
            },
        )

        self.assertTrue(result["ok"])
        self.assertEqual("skipped_preserved_for_manual_oauth", result["status"])
        self.assertEqual("user@example.com", result["invite_email"])


class EasyEmailRuntimeTests(unittest.TestCase):
    def test_release_mailbox_preserve_updates_team_flow_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            source_path = Path(tmp_dir) / "artifact.json"
            source_path.write_text('{"teamFlow":{}}', encoding="utf-8")

            result = easyemail_runtime.dispatch_easyemail_step(
                step_type="release_mailbox",
                step_input={
                    "provider": "moemail",
                    "source_path": str(source_path),
                    "error_code": "token_invalidated",
                    "preserve_enabled": True,
                    "preserve_on_error_codes": "token_invalidated",
                },
            )

            updated = source_path.read_text(encoding="utf-8")

        self.assertEqual("skipped_preserved_for_manual_oauth", result["detail"])
        self.assertIn('"mailboxRelease"', updated)
        self.assertIn("skipped_preserved_for_manual_oauth", updated)


class RuntimeProxySupportTests(unittest.TestCase):
    def test_runtime_reachable_proxy_url_rewrites_localhost_when_runtime_host_is_set(self) -> None:
        with mock.patch.object(runtime_proxy_support, "resolve_easy_proxy_runtime_host", return_value="easy-proxy-service"):
            rewritten = runtime_proxy_support.runtime_reachable_proxy_url("http://127.0.0.1:8080")

        self.assertEqual("http://easy-proxy-service:8080", rewritten)

    def test_flow_network_env_clears_proxy_vars_when_easy_proxy_disabled(self) -> None:
        env = {
            "REGISTER_ENABLE_EASY_PROXY": "false",
            "HTTP_PROXY": "http://proxy.example:8080",
            "HTTPS_PROXY": "http://proxy.example:8443",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            with runtime_proxy_support.flow_network_env():
                self.assertEqual("*", os.environ.get("NO_PROXY"))
                self.assertEqual("*", os.environ.get("no_proxy"))
                self.assertNotIn("HTTP_PROXY", os.environ)
                self.assertNotIn("HTTPS_PROXY", os.environ)
            self.assertEqual("http://proxy.example:8080", os.environ.get("HTTP_PROXY"))
            self.assertEqual("http://proxy.example:8443", os.environ.get("HTTPS_PROXY"))
