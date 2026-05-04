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

from others import easyemail_runtime, easyprotocol_runtime, runtime_mailbox, runtime_proxy_support  # noqa: E402


class EasyProtocolRuntimeTests(unittest.TestCase):
    def test_dispatch_revoke_codex_member_skips_when_no_target_identifiers(self) -> None:
        result = easyprotocol_runtime.dispatch_easyprotocol_step(
            step_type="revoke_codex_member",
            step_input={},
        )

        self.assertTrue(result["ok"])
        self.assertEqual("skipped_missing_revoke_target", result["status"])
        self.assertEqual("missing_revoke_target", result["detail"])

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

    def test_create_openai_account_can_bridge_storage_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            source_path = Path(tmp_dir) / "openai-oauth.json"
            source_path.write_text('{"ok": true}', encoding="utf-8")
            bridge_dir = Path(tmp_dir) / "bridge"

            with mock.patch.object(
                easyprotocol_runtime,
                "invoke_easyprotocol",
                return_value={"storage_path": str(source_path), "ok": True},
            ):
                with mock.patch.dict(
                    os.environ,
                    {"REGISTER_PROTOCOL_BRIDGE_DIR": str(bridge_dir)},
                    clear=False,
                ):
                    result = easyprotocol_runtime.dispatch_easyprotocol_step(
                        step_type="create_openai_account",
                        step_input={},
                    )

            expected_path = bridge_dir / source_path.name
            self.assertEqual(str(expected_path.resolve()), result["storage_path"])
            self.assertEqual(str(source_path), result["original_storage_path"])
            self.assertEqual(str(expected_path.resolve()), result["bridged_storage_path"])
            self.assertTrue(expected_path.is_file())


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

    def test_release_mailbox_skips_team_flow_update_when_source_missing(self) -> None:
        missing_path = Path(tempfile.gettempdir()) / f"missing-{os.getpid()}-{id(self)}.json"
        if missing_path.exists():
            missing_path.unlink()

        with mock.patch.object(
            easyemail_runtime,
            "release_mailbox",
            return_value={"released": True, "detail": "deleted", "provider": "moemail"},
        ) as release_mailbox:
            result = easyemail_runtime.dispatch_easyemail_step(
                step_type="release_mailbox",
                step_input={
                    "provider": "moemail",
                    "source_path": str(missing_path),
                    "mailbox_ref": "moemail:test",
                    "mailbox_session_id": "session-test",
                },
            )

        release_mailbox.assert_called_once()
        self.assertTrue(result["released"])
        self.assertEqual("deleted", result["detail"])

    def test_release_mailbox_sessions_by_email_reports_cleanup_summary(self) -> None:
        with mock.patch.object(easyemail_runtime, "ensure_easyemail_runtime_defaults"):
            with mock.patch.object(
                easyemail_runtime,
                "release_mailbox_sessions_by_email",
                return_value=[
                    {
                        "sessionId": "sess-1",
                        "email": "user@example.com",
                        "release": {"released": True, "detail": "deleted"},
                    },
                    {
                        "sessionId": "sess-2",
                        "email": "user@example.com",
                        "release": {"released": False, "detail": "not_found"},
                    },
                ],
            ) as release_sessions:
                result = easyemail_runtime.dispatch_easyemail_step(
                    step_type="release_mailbox_sessions_by_email",
                    step_input={
                        "email_address": "user@example.com",
                        "reason": "openai_login_recover",
                    },
                )

        release_sessions.assert_called_once()
        self.assertTrue(result["ok"])
        self.assertEqual("released_sessions", result["status"])
        self.assertEqual(2, result["matched_session_count"])
        self.assertEqual(2, result["released_count"])
        self.assertEqual(0, result["failed_count"])

    def test_release_mailbox_sessions_by_email_rejects_missing_email(self) -> None:
        with mock.patch.object(easyemail_runtime, "ensure_easyemail_runtime_defaults"):
            result = easyemail_runtime.dispatch_easyemail_step(
                step_type="release_mailbox_sessions_by_email",
                step_input={},
            )

        self.assertFalse(result["ok"])
        self.assertEqual("invalid_email_address", result["detail"])


class RuntimeProxySupportTests(unittest.TestCase):
    def test_runtime_reachable_proxy_url_rewrites_localhost_when_runtime_host_is_set(self) -> None:
        with mock.patch.object(runtime_proxy_support, "resolve_easy_proxy_runtime_host", return_value="easy-proxy"):
            rewritten = runtime_proxy_support.runtime_reachable_proxy_url("http://127.0.0.1:8080")

        self.assertEqual("http://easy-proxy:8080", rewritten)

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


class RuntimeMailboxTests(unittest.TestCase):
    def test_domain_is_not_blacklisted_by_failure_rate_only(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "REGISTER_MAILBOX_BUSINESS_KEY": "openai",
                "REGISTER_MAILBOX_DOMAIN_BLACKLIST": "",
            },
            clear=True,
        ):
            self.assertFalse(
                runtime_mailbox._mailbox_domain_is_business_blacklisted(
                    "cksa.eu.cc",
                    {
                        "businesses": {
                            "openai": {
                                "domains": {
                                    "cksa.eu.cc": {
                                        "attempts": 50,
                                        "failures": 49,
                                        "blacklisted": False,
                                    }
                                }
                            }
                        }
                    },
                )
            )

    def test_resolve_mailbox_retries_blacklisted_business_domain(self) -> None:
        first_mailbox = runtime_mailbox.Mailbox(
            provider="moemail",
            email="bad@coolkid.icu",
            ref="moemail:first",
            session_id="first",
        )
        second_mailbox = runtime_mailbox.Mailbox(
            provider="moemail",
            email="good@zhooo.org",
            ref="moemail:second",
            session_id="second",
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "REGISTER_MAILBOX_BUSINESS_KEY": "generic",
                    "REGISTER_MAILBOX_DOMAIN_BLACKLIST": "fallback.test",
                    "REGISTER_MAILBOX_DOMAIN_POOL": "fallback.test",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": (
                        '{"openai":{"domainPool":["zhooo.org","cnmlgb.de"],'
                        '"explicitBlacklistDomains":["coolkid.icu"]}}'
                    ),
                },
                clear=True,
            ):
                with mock.patch.object(
                    runtime_mailbox,
                    "_resolve_planned_mailbox_provider",
                    return_value="moemail",
                ):
                    with mock.patch.object(
                        runtime_mailbox,
                        "create_mailbox",
                        side_effect=[first_mailbox, second_mailbox],
                    ) as create_mailbox:
                        with mock.patch.object(runtime_mailbox, "release_mailbox") as release_mailbox:
                            mailbox = runtime_mailbox.resolve_mailbox(
                                preallocated_email=None,
                                preallocated_session_id=None,
                                preallocated_mailbox_ref=None,
                                business_key="openai",
                            )
        self.assertEqual("good@zhooo.org", mailbox.email)
        self.assertEqual(2, create_mailbox.call_count)
        release_mailbox.assert_called_once()

    def test_mailbox_domain_policy_violation_applies_business_pool_to_m2u(self) -> None:
        mailbox = runtime_mailbox.Mailbox(
            provider="m2u",
            email="blocked@cpu.edu.kg",
            ref="m2u:test",
            session_id="m2u-session",
        )
        with mock.patch.dict(
            os.environ,
            {
                "REGISTER_MAILBOX_BUSINESS_KEY": "generic",
                "REGISTER_MAILBOX_DOMAIN_POOL": "fallback.test",
                "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": (
                    '{"openai":{"domainPool":["zhooo.org","cnmlgb.de"],'
                    '"explicitBlacklistDomains":["coolkid.icu","cpu.edu.kg"]}}'
                ),
            },
            clear=True,
        ):
            violation = runtime_mailbox._mailbox_domain_policy_violation(
                mailbox,
                business_key="openai",
            )

        self.assertIsNotNone(violation)
        assert violation is not None
        self.assertEqual("explicit_business_blacklist", violation["reason"])
        self.assertEqual("m2u", violation["provider"])
        self.assertEqual("cpu.edu.kg", violation["domain"])

    def test_resolve_mailbox_retries_m2u_domain_outside_business_pool(self) -> None:
        first_mailbox = runtime_mailbox.Mailbox(
            provider="m2u",
            email="blocked@cpu.edu.kg",
            ref="m2u:first",
            session_id="first",
        )
        second_mailbox = runtime_mailbox.Mailbox(
            provider="m2u",
            email="good@cnmlgb.de",
            ref="m2u:second",
            session_id="second",
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "REGISTER_MAILBOX_BUSINESS_KEY": "generic",
                    "REGISTER_MAILBOX_DOMAIN_BLACKLIST": "",
                    "REGISTER_MAILBOX_DOMAIN_POOL": "fallback.test",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": (
                        '{"openai":{"domainPool":["zhooo.org","cnmlgb.de"],'
                        '"explicitBlacklistDomains":["coolkid.icu","shaole.me","cpu.edu.kg","tmail.bio","do4.tech"]}}'
                    ),
                },
                clear=True,
            ):
                with mock.patch.object(
                    runtime_mailbox,
                    "_resolve_planned_mailbox_provider",
                    return_value="m2u",
                ):
                    with mock.patch.object(
                        runtime_mailbox,
                        "create_mailbox",
                        side_effect=[first_mailbox, second_mailbox],
                    ) as create_mailbox:
                        with mock.patch.object(runtime_mailbox, "release_mailbox") as release_mailbox:
                            mailbox = runtime_mailbox.resolve_mailbox(
                                preallocated_email=None,
                                preallocated_session_id=None,
                                preallocated_mailbox_ref=None,
                                business_key="openai",
                            )

        self.assertEqual("good@cnmlgb.de", mailbox.email)
        self.assertEqual(2, create_mailbox.call_count)
        release_mailbox.assert_called_once()
