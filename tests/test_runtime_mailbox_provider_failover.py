from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SRC_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

PYTHON_SHARED_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "python_shared" / "src"
if str(PYTHON_SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(PYTHON_SHARED_ROOT))

from others import runtime_mailbox  # noqa: E402


class RuntimeMailboxProviderFailoverTests(unittest.TestCase):
    def setUp(self) -> None:
        with runtime_mailbox._PROVIDER_OPEN_FAILURE_CIRCUITS_LOCK:
            runtime_mailbox._PROVIDER_OPEN_FAILURE_CIRCUITS.clear()

    def tearDown(self) -> None:
        with runtime_mailbox._PROVIDER_OPEN_FAILURE_CIRCUITS_LOCK:
            runtime_mailbox._PROVIDER_OPEN_FAILURE_CIRCUITS.clear()

    def test_provider_auth_failure_opens_circuit_and_replans(self) -> None:
        mailbox = runtime_mailbox.Mailbox(
            provider="cloudflare_temp_email",
            email="candidate@healthy.test",
            ref="cloudflare_temp_email:session",
            session_id="session",
        )
        create_calls: list[dict[str, object]] = []

        def _create_mailbox(**kwargs):
            create_calls.append(dict(kwargs))
            if len(create_calls) == 1:
                raise RuntimeError(
                    "mail service POST /mail/mailboxes/open failed: HTTP 500 "
                    "[code=TEMPORAM_PROVIDER_FAILURE]: temporam getDomains failed: "
                    "status=401 (Unauthorized)"
                )
            return mailbox

        with tempfile.TemporaryDirectory() as tmp_dir:
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(Path(tmp_dir) / "register-output"),
                    "REGISTER_MAILBOX_PROVIDERS": "temporam,cloudflare_temp_email",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": '{"openai":{"domainPool":[]}}',
                    "REGISTER_MAILBOX_BUSINESS_RETRY_ATTEMPTS": "1",
                    "REGISTER_MAILBOX_PROVIDER_OPEN_FAILURE_CIRCUIT_TTL_SECONDS": "900",
                },
                clear=True,
            ), mock.patch.object(
                runtime_mailbox,
                "_resolve_planned_mailbox_provider",
                side_effect=("temporam", "cloudflare_temp_email"),
            ), mock.patch.object(
                runtime_mailbox,
                "create_mailbox",
                side_effect=_create_mailbox,
            ), mock.patch.object(runtime_mailbox, "json_log"):
                resolved = runtime_mailbox.resolve_mailbox(
                    preallocated_email=None,
                    preallocated_session_id=None,
                    preallocated_mailbox_ref=None,
                    business_key="openai",
                )

        self.assertEqual("cloudflare_temp_email", resolved.provider)
        self.assertEqual(2, len(create_calls))
        self.assertIn(
            "temporam",
            tuple(create_calls[1].get("excluded_provider_type_keys") or ()),
        )

    def test_domain_rejection_retries_with_another_domain(self) -> None:
        mailbox = runtime_mailbox.Mailbox(
            provider="im215",
            email="candidate@good.test",
            ref="im215:session",
            session_id="session",
        )
        create_calls: list[dict[str, object]] = []

        def _create_mailbox(**kwargs):
            create_calls.append(dict(kwargs))
            if len(create_calls) == 1:
                raise RuntimeError(
                    "mail service POST /mail/mailboxes/open failed: HTTP 500 "
                    "[code=MAILBOX_DOMAIN_EXCLUDED]: shared domain restricted"
                )
            return mailbox

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            state_path = output_root / "others" / "register-mailbox-domain-state.json"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_text(
                json.dumps(
                    {
                        "schemaVersion": 3,
                        "businesses": {
                            "openai": {
                                "domains": {
                                    "bad.test": {"provider": "im215"},
                                    "good.test": {"provider": "im215"},
                                }
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "REGISTER_MAILBOX_PROVIDERS": "im215",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": (
                        '{"openai":{"domainPool":["bad.test","good.test"]}}'
                    ),
                    "REGISTER_MAILBOX_BUSINESS_RETRY_ATTEMPTS": "1",
                },
                clear=True,
            ), mock.patch.object(
                runtime_mailbox,
                "_resolve_planned_mailbox_provider",
                return_value="im215",
            ), mock.patch.object(
                runtime_mailbox.random,
                "choice",
                side_effect=lambda values: values[0],
            ), mock.patch.object(
                runtime_mailbox,
                "create_mailbox",
                side_effect=_create_mailbox,
            ), mock.patch.object(runtime_mailbox, "json_log"):
                resolved = runtime_mailbox.resolve_mailbox(
                    preallocated_email=None,
                    preallocated_session_id=None,
                    preallocated_mailbox_ref=None,
                    business_key="openai",
                )

        self.assertEqual("im215", resolved.provider)
        self.assertEqual(2, len(create_calls))
        self.assertEqual("bad.test", create_calls[0].get("mailcreate_domain"))
        self.assertEqual("good.test", create_calls[1].get("mailcreate_domain"))

    def test_provider_selected_domain_rejection_replans_other_provider(self) -> None:
        mailbox = runtime_mailbox.Mailbox(
            provider="cloudflare_temp_email",
            email="candidate@healthy.test",
            ref="cloudflare_temp_email:session",
            session_id="session",
        )
        create_calls: list[dict[str, object]] = []

        def _create_mailbox(**kwargs):
            create_calls.append(dict(kwargs))
            if len(create_calls) == 1:
                raise RuntimeError(
                    "mail service POST /mail/mailboxes/open failed: HTTP 500 "
                    "[code=MAILBOX_DOMAIN_EXCLUDED]"
                )
            return mailbox

        with tempfile.TemporaryDirectory() as tmp_dir:
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(Path(tmp_dir) / "register-output"),
                    "REGISTER_MAILBOX_PROVIDERS": "m2u,cloudflare_temp_email",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": '{"openai":{"domainPool":[]}}',
                    "REGISTER_MAILBOX_BUSINESS_RETRY_ATTEMPTS": "1",
                },
                clear=True,
            ), mock.patch.object(
                runtime_mailbox,
                "_resolve_planned_mailbox_provider",
                side_effect=("m2u", "cloudflare_temp_email"),
            ), mock.patch.object(
                runtime_mailbox,
                "create_mailbox",
                side_effect=_create_mailbox,
            ), mock.patch.object(runtime_mailbox, "json_log"):
                resolved = runtime_mailbox.resolve_mailbox(
                    preallocated_email=None,
                    preallocated_session_id=None,
                    preallocated_mailbox_ref=None,
                    business_key="openai",
                )

        self.assertEqual("cloudflare_temp_email", resolved.provider)
        self.assertEqual("m2u", create_calls[0].get("provider"))
        self.assertEqual("cloudflare_temp_email", create_calls[1].get("provider"))

    def test_recreate_domain_failure_falls_back_to_new_mailbox(self) -> None:
        mailbox = runtime_mailbox.Mailbox(
            provider="cloudflare_temp_email",
            email="candidate@healthy.test",
            ref="cloudflare_temp_email:session-new",
            session_id="session-new",
        )
        create_calls: list[dict[str, object]] = []

        def _create_mailbox(**kwargs):
            create_calls.append(dict(kwargs))
            if len(create_calls) == 1:
                raise RuntimeError(
                    "mail service POST /mail/mailboxes/open failed: HTTP 500 "
                    "[code=MAILBOX_DOMAIN_EXCLUDED]: shared domain restricted"
                )
            return mailbox

        with tempfile.TemporaryDirectory() as tmp_dir:
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(Path(tmp_dir) / "register-output"),
                    "REGISTER_MAILBOX_PROVIDERS": "im215,cloudflare_temp_email",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": '{"openai":{"domainPool":[]}}',
                    "REGISTER_MAILBOX_BUSINESS_RETRY_ATTEMPTS": "1",
                },
                clear=True,
            ), mock.patch.object(
                runtime_mailbox,
                "_resolve_planned_mailbox_provider",
                return_value="cloudflare_temp_email",
            ), mock.patch.object(
                runtime_mailbox,
                "create_mailbox",
                side_effect=_create_mailbox,
            ), mock.patch.object(runtime_mailbox, "json_log"):
                resolved = runtime_mailbox.resolve_mailbox(
                    preallocated_email="stale@restricted.test",
                    preallocated_session_id="session-old",
                    preallocated_mailbox_ref="im215:session-old",
                    recreate_preallocated_email=True,
                    business_key="openai",
                )

        self.assertEqual("cloudflare_temp_email", resolved.provider)
        self.assertEqual("stale@restricted.test", create_calls[0].get("requested_email_address"))
        self.assertNotIn("requested_email_address", create_calls[1])
        self.assertIn("restricted.test", create_calls[1].get("excluded_domains") or ())

    def test_recreate_excluded_email_falls_back_to_new_mailbox(self) -> None:
        mailbox = runtime_mailbox.Mailbox(
            provider="cloudflare_temp_email",
            email="candidate@healthy.test",
            ref="cloudflare_temp_email:session-new",
            session_id="session-new",
        )
        create_calls: list[dict[str, object]] = []

        def _create_mailbox(**kwargs):
            create_calls.append(dict(kwargs))
            if len(create_calls) == 1:
                raise RuntimeError(
                    "mail service POST /mail/mailboxes/open failed: HTTP 500 "
                    "[code=MAILBOX_EMAIL_EXCLUDED]"
                )
            return mailbox

        with tempfile.TemporaryDirectory() as tmp_dir:
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(Path(tmp_dir) / "register-output"),
                    "REGISTER_MAILBOX_PROVIDERS": "etempmail,cloudflare_temp_email",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": '{"openai":{"domainPool":[]}}',
                    "REGISTER_MAILBOX_BUSINESS_RETRY_ATTEMPTS": "1",
                },
                clear=True,
            ), mock.patch.object(
                runtime_mailbox,
                "_resolve_planned_mailbox_provider",
                return_value="cloudflare_temp_email",
            ), mock.patch.object(
                runtime_mailbox,
                "create_mailbox",
                side_effect=_create_mailbox,
            ), mock.patch.object(runtime_mailbox, "json_log"):
                resolved = runtime_mailbox.resolve_mailbox(
                    preallocated_email="stale@restricted.test",
                    preallocated_session_id="session-old",
                    preallocated_mailbox_ref="etempmail:session-old",
                    recreate_preallocated_email=True,
                    business_key="openai",
                )

        self.assertEqual("cloudflare_temp_email", resolved.provider)
        self.assertEqual(2, len(create_calls))
        self.assertEqual("stale@restricted.test", create_calls[0].get("requested_email_address"))
        self.assertNotIn("requested_email_address", create_calls[1])
        self.assertIn(
            "stale@restricted.test",
            create_calls[1].get("excluded_email_addresses") or (),
        )

    def test_dynamic_excluded_email_retries_with_a_new_address(self) -> None:
        mailbox = runtime_mailbox.Mailbox(
            provider="etempmail",
            email="candidate@healthy.test",
            ref="etempmail:session-new",
            session_id="session-new",
        )
        create_calls: list[dict[str, object]] = []

        def _create_mailbox(**kwargs):
            create_calls.append(dict(kwargs))
            if len(create_calls) == 1:
                raise RuntimeError(
                    'mail service POST /mail/mailboxes/open failed: HTTP 500 '
                    '[code=MAILBOX_EMAIL_EXCLUDED] Mailbox address "rejected@bad.test" was excluded'
                )
            return mailbox

        with tempfile.TemporaryDirectory() as tmp_dir:
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(Path(tmp_dir) / "register-output"),
                    "REGISTER_MAILBOX_PROVIDERS": "etempmail,cloudflare_temp_email",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": '{"openai":{"domainPool":[]}}',
                    "REGISTER_MAILBOX_BUSINESS_RETRY_ATTEMPTS": "1",
                },
                clear=True,
            ), mock.patch.object(
                runtime_mailbox,
                "_resolve_planned_mailbox_provider",
                return_value="etempmail",
            ), mock.patch.object(
                runtime_mailbox,
                "create_mailbox",
                side_effect=_create_mailbox,
            ), mock.patch.object(runtime_mailbox, "json_log"):
                resolved = runtime_mailbox.resolve_mailbox(
                    preallocated_email=None,
                    preallocated_session_id=None,
                    preallocated_mailbox_ref=None,
                    business_key="openai",
                )

        self.assertEqual("etempmail", resolved.provider)
        self.assertEqual(2, len(create_calls))
        self.assertIn(
            "rejected@bad.test",
            create_calls[1].get("excluded_email_addresses") or (),
        )

    def test_recreate_provider_route_failure_falls_back_to_new_mailbox(self) -> None:
        mailbox = runtime_mailbox.Mailbox(
            provider="cloudflare_temp_email",
            email="candidate@healthy.test",
            ref="cloudflare_temp_email:session-new",
            session_id="session-new",
        )
        create_calls: list[dict[str, object]] = []

        def _create_mailbox(**kwargs):
            create_calls.append(dict(kwargs))
            if len(create_calls) == 1:
                raise RuntimeError(
                    "mail service POST /mail/mailboxes/open failed: HTTP 500 "
                    "[code=PROVIDER_SELECTION_FAILED]: no mail provider route"
                )
            return mailbox

        with tempfile.TemporaryDirectory() as tmp_dir:
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(Path(tmp_dir) / "register-output"),
                    "REGISTER_MAILBOX_PROVIDERS": "guerrillamail,cloudflare_temp_email",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": '{"openai":{"domainPool":[]}}',
                    "REGISTER_MAILBOX_BUSINESS_RETRY_ATTEMPTS": "1",
                },
                clear=True,
            ), mock.patch.object(
                runtime_mailbox,
                "_resolve_planned_mailbox_provider",
                return_value="cloudflare_temp_email",
            ), mock.patch.object(
                runtime_mailbox,
                "create_mailbox",
                side_effect=_create_mailbox,
            ), mock.patch.object(runtime_mailbox, "json_log"):
                resolved = runtime_mailbox.resolve_mailbox(
                    preallocated_email="stale@restricted.test",
                    preallocated_session_id="session-old",
                    preallocated_mailbox_ref="guerrillamail:session-old",
                    recreate_preallocated_email=True,
                    business_key="openai",
                )

        self.assertEqual("cloudflare_temp_email", resolved.provider)
        self.assertEqual(2, len(create_calls))
        self.assertEqual("stale@restricted.test", create_calls[0].get("requested_email_address"))
        self.assertNotIn("requested_email_address", create_calls[1])
        self.assertIn(
            "stale@restricted.test",
            create_calls[1].get("excluded_email_addresses") or (),
        )

    def test_unknown_open_failure_is_not_hidden_by_failover(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(Path(tmp_dir) / "register-output"),
                    "REGISTER_MAILBOX_PROVIDERS": "temporam,cloudflare_temp_email",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": '{"openai":{"domainPool":[]}}',
                    "REGISTER_MAILBOX_BUSINESS_RETRY_ATTEMPTS": "1",
                },
                clear=True,
            ), mock.patch.object(
                runtime_mailbox,
                "_resolve_planned_mailbox_provider",
                return_value="temporam",
            ), mock.patch.object(
                runtime_mailbox,
                "create_mailbox",
                side_effect=RuntimeError("unexpected mailbox serialization failure"),
            ) as create_mailbox:
                with self.assertRaisesRegex(RuntimeError, "unexpected mailbox serialization failure"):
                    runtime_mailbox.resolve_mailbox(
                        preallocated_email=None,
                        preallocated_session_id=None,
                        preallocated_mailbox_ref=None,
                        business_key="openai",
                    )

        self.assertEqual(1, create_mailbox.call_count)
        self.assertEqual((), runtime_mailbox._active_provider_open_failure_circuits())

    def test_easyemail_server_auth_failure_does_not_quarantine_planned_provider(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(Path(tmp_dir) / "register-output"),
                    "REGISTER_MAILBOX_PROVIDERS": "temporam,cloudflare_temp_email",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": '{"openai":{"domainPool":[]}}',
                    "REGISTER_MAILBOX_BUSINESS_RETRY_ATTEMPTS": "1",
                },
                clear=True,
            ), mock.patch.object(
                runtime_mailbox,
                "_resolve_planned_mailbox_provider",
                return_value="temporam",
            ), mock.patch.object(
                runtime_mailbox,
                "create_mailbox",
                side_effect=RuntimeError(
                    "mail service POST /mail/mailboxes/open failed: HTTP 401: unauthorized"
                ),
            ) as create_mailbox:
                with self.assertRaisesRegex(RuntimeError, "HTTP 401"):
                    runtime_mailbox.resolve_mailbox(
                        preallocated_email=None,
                        preallocated_session_id=None,
                        preallocated_mailbox_ref=None,
                        business_key="openai",
                    )

        self.assertEqual(1, create_mailbox.call_count)
        self.assertEqual((), runtime_mailbox._active_provider_open_failure_circuits())

    def test_pinned_provider_transient_failure_opens_circuit_and_replans(self) -> None:
        mailbox = runtime_mailbox.Mailbox(
            provider="cloudflare_temp_email",
            email="candidate@healthy.test",
            ref="cloudflare_temp_email:session",
            session_id="session",
        )
        create_calls: list[dict[str, object]] = []

        def _create_mailbox(**kwargs):
            create_calls.append(dict(kwargs))
            if len(create_calls) == 1:
                raise RuntimeError(
                    "mail service POST /mail/mailboxes/open failed: HTTP 500 "
                    "[code=215]: upstream request failed"
                )
            return mailbox

        with tempfile.TemporaryDirectory() as tmp_dir:
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(Path(tmp_dir) / "register-output"),
                    "REGISTER_MAILBOX_PROVIDERS": "im215,cloudflare_temp_email",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": '{"openai":{"domainPool":[]}}',
                    "REGISTER_MAILBOX_BUSINESS_RETRY_ATTEMPTS": "1",
                },
                clear=True,
            ), mock.patch.object(
                runtime_mailbox,
                "_resolve_planned_mailbox_provider",
                side_effect=("im215", "cloudflare_temp_email"),
            ), mock.patch.object(
                runtime_mailbox,
                "create_mailbox",
                side_effect=_create_mailbox,
            ), mock.patch.object(runtime_mailbox, "json_log"):
                resolved = runtime_mailbox.resolve_mailbox(
                    preallocated_email=None,
                    preallocated_session_id=None,
                    preallocated_mailbox_ref=None,
                    business_key="openai",
                )

        self.assertEqual("cloudflare_temp_email", resolved.provider)
        self.assertEqual("im215", create_calls[0].get("provider"))
        self.assertEqual("cloudflare_temp_email", create_calls[1].get("provider"))

    def test_auto_provider_transient_failure_does_not_guess_planned_provider(self) -> None:
        self.assertEqual(
            ("", "", ""),
            runtime_mailbox._mailbox_open_failure_avoidance(
                RuntimeError(
                    "mail service POST /mail/mailboxes/open failed: HTTP 500 "
                    "[code=MAILBOX_UPSTREAM_TRANSIENT]"
                ),
                attempted_provider="moemail",
                attempted_provider_pinned=False,
                attempted_domain="",
            ),
        )

    def test_auto_provider_domain_failure_uses_structured_domain(self) -> None:
        self.assertEqual(
            ("im215", "blocked.test", "domain_unavailable"),
            runtime_mailbox._mailbox_open_failure_avoidance(
                RuntimeError(
                    'mail service POST /mail/mailboxes/open failed: HTTP 500 '
                    '[code=MAILBOX_DOMAIN_EXCLUDED]: '
                    '{"code":"MAILBOX_DOMAIN_EXCLUDED","domain":"blocked.test",'
                    '"providerTypeKey":"im215"}'
                ),
                attempted_provider="moemail",
                attempted_provider_pinned=False,
                attempted_domain="",
            ),
        )

    def test_repeated_structured_domain_failure_escalates_to_provider_failover(self) -> None:
        mailbox = runtime_mailbox.Mailbox(
            provider="cloudflare_temp_email",
            email="candidate@healthy.test",
            ref="cloudflare_temp_email:session",
            session_id="session",
        )
        create_calls: list[dict[str, object]] = []

        def _create_mailbox(**kwargs):
            create_calls.append(dict(kwargs))
            if len(create_calls) <= 2:
                raise RuntimeError(
                    'mail service POST /mail/mailboxes/open failed: HTTP 500 '
                    '[code=MAILBOX_DOMAIN_EXCLUDED]: '
                    '{"code":"MAILBOX_DOMAIN_EXCLUDED","domain":"blocked.test",'
                    '"providerTypeKey":"etempmail","providerInstanceId":"etempmail_shared_default"}'
                )
            return mailbox

        with tempfile.TemporaryDirectory() as tmp_dir:
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(Path(tmp_dir) / "register-output"),
                    "REGISTER_MAILBOX_PROVIDERS": "etempmail,cloudflare_temp_email",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": '{"openai":{"domainPool":[]}}',
                    "REGISTER_MAILBOX_BUSINESS_RETRY_ATTEMPTS": "1",
                },
                clear=True,
            ), mock.patch.object(
                runtime_mailbox,
                "_resolve_planned_mailbox_provider",
                side_effect=("etempmail", "etempmail", "cloudflare_temp_email"),
            ), mock.patch.object(
                runtime_mailbox,
                "create_mailbox",
                side_effect=_create_mailbox,
            ), mock.patch.object(runtime_mailbox, "json_log"):
                resolved = runtime_mailbox.resolve_mailbox(
                    preallocated_email=None,
                    preallocated_session_id=None,
                    preallocated_mailbox_ref=None,
                    business_key="openai",
                )

        self.assertEqual("cloudflare_temp_email", resolved.provider)
        self.assertEqual(3, len(create_calls))
        self.assertIn("blocked.test", create_calls[1].get("excluded_domains") or ())
        self.assertIn("etempmail", runtime_mailbox._active_provider_open_failure_circuits())

    def test_repeated_same_mailbox_address_escalates_to_provider_failover(self) -> None:
        reused = runtime_mailbox.Mailbox(
            provider="etempmail",
            email="reused@example.test",
            ref="etempmail:reused",
            session_id="reused",
        )
        fresh = runtime_mailbox.Mailbox(
            provider="cloudflare_temp_email",
            email="fresh@example.test",
            ref="cloudflare_temp_email:fresh",
            session_id="fresh",
        )
        create_calls: list[dict[str, object]] = []

        def _create_mailbox(**kwargs):
            create_calls.append(dict(kwargs))
            return reused if len(create_calls) == 1 else fresh

        with tempfile.TemporaryDirectory() as tmp_dir:
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(Path(tmp_dir) / "register-output"),
                    "REGISTER_MAILBOX_PROVIDERS": "etempmail,cloudflare_temp_email",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": '{"openai":{"domainPool":[]}}',
                    "REGISTER_MAILBOX_BUSINESS_RETRY_ATTEMPTS": "1",
                },
                clear=True,
            ), mock.patch.object(
                runtime_mailbox,
                "_resolve_planned_mailbox_provider",
                side_effect=("etempmail", "cloudflare_temp_email"),
            ), mock.patch.object(
                runtime_mailbox,
                "create_mailbox",
                side_effect=_create_mailbox,
            ), mock.patch.object(runtime_mailbox, "json_log"):
                resolved = runtime_mailbox.resolve_mailbox(
                    preallocated_email=None,
                    preallocated_session_id=None,
                    preallocated_mailbox_ref=None,
                    business_key="openai",
                    avoid_emails=("reused@example.test",),
                    avoid_reason="mailbox_address_excluded",
                )

        self.assertEqual("cloudflare_temp_email", resolved.provider)
        self.assertEqual(2, len(create_calls))
        self.assertIn(
            "etempmail",
            tuple(create_calls[1].get("excluded_provider_type_keys") or ()),
        )

    def test_explicit_provider_fallback_bypasses_empty_auto_plan(self) -> None:
        mailbox = runtime_mailbox.Mailbox(
            provider="guerrillamail",
            email="candidate@healthy.test",
            ref="guerrillamail:session",
            session_id="session",
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(Path(tmp_dir) / "register-output"),
                    "REGISTER_MAILBOX_PROVIDERS": "guerrillamail,cloudflare_temp_email",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": '{"openai":{"domainPool":[]}}',
                    "REGISTER_MAILBOX_BUSINESS_RETRY_ATTEMPTS": "1",
                },
                clear=True,
            ), mock.patch.object(
                runtime_mailbox,
                "_resolve_planned_mailbox_provider",
                return_value="",
            ), mock.patch.object(
                runtime_mailbox,
                "create_mailbox",
                return_value=mailbox,
            ) as create_mailbox, mock.patch.object(runtime_mailbox, "json_log"):
                resolved = runtime_mailbox.resolve_mailbox(
                    preallocated_email=None,
                    preallocated_session_id=None,
                    preallocated_mailbox_ref=None,
                    business_key="openai",
                )

        self.assertEqual("guerrillamail", resolved.provider)
        self.assertEqual(1, create_mailbox.call_count)
        self.assertEqual("guerrillamail", create_mailbox.call_args.kwargs.get("provider"))

    def test_single_configured_provider_is_pinned_for_plan_and_create(self) -> None:
        mailbox = runtime_mailbox.Mailbox(
            provider="mail2925",
            email="candidate@healthy.test",
            ref="mail2925:session",
            session_id="session",
        )
        plan_calls: list[dict[str, object]] = []

        def _plan_mailbox(**kwargs):
            plan_calls.append(dict(kwargs))
            # Simulate EasyEmail's available-first planner returning an
            # unrelated provider despite providerGroupSelections.
            return {"instance": {"providerTypeKey": "etempmail"}}

        with tempfile.TemporaryDirectory() as tmp_dir:
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(Path(tmp_dir) / "register-output"),
                    "REGISTER_MAILBOX_PROVIDERS": "mail2925",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": '{"openai":{"domainPool":[]}}',
                    "REGISTER_MAILBOX_BUSINESS_RETRY_ATTEMPTS": "1",
                },
                clear=True,
            ), mock.patch.object(
                runtime_mailbox,
                "plan_mailbox",
                side_effect=_plan_mailbox,
            ), mock.patch.object(
                runtime_mailbox,
                "create_mailbox",
                return_value=mailbox,
            ) as create_mailbox, mock.patch.object(runtime_mailbox, "json_log"):
                resolved = runtime_mailbox.resolve_mailbox(
                    preallocated_email=None,
                    preallocated_session_id=None,
                    preallocated_mailbox_ref=None,
                    business_key="openai",
                )

        self.assertEqual("mail2925", resolved.provider)
        self.assertEqual(1, len(plan_calls))
        self.assertEqual("mail2925", plan_calls[0].get("provider"))
        self.assertEqual(1, create_mailbox.call_count)
        self.assertEqual("mail2925", create_mailbox.call_args.kwargs.get("provider"))

    def test_single_configured_provider_drops_stale_retry_exclusion(self) -> None:
        mailbox = runtime_mailbox.Mailbox(
            provider="mail2925",
            email="candidate@healthy.test",
            ref="mail2925:session",
            session_id="session",
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(Path(tmp_dir) / "register-output"),
                    "REGISTER_MAILBOX_PROVIDERS": "mail2925",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": '{"openai":{"domainPool":[]}}',
                    "REGISTER_MAILBOX_BUSINESS_RETRY_ATTEMPTS": "1",
                },
                clear=True,
            ), mock.patch.object(
                runtime_mailbox,
                "_resolve_planned_mailbox_provider",
                return_value="mail2925",
            ), mock.patch.object(
                runtime_mailbox,
                "create_mailbox",
                return_value=mailbox,
            ) as create_mailbox, mock.patch.object(runtime_mailbox, "json_log"):
                resolved = runtime_mailbox.resolve_mailbox(
                    preallocated_email=None,
                    preallocated_session_id=None,
                    preallocated_mailbox_ref=None,
                    business_key="openai",
                    avoid_providers=("mail2925",),
                    avoid_reason="previous_task_retry",
                )

        self.assertEqual("mail2925", resolved.provider)
        self.assertEqual(1, create_mailbox.call_count)
        self.assertNotIn(
            "mail2925",
            tuple(create_mailbox.call_args.kwargs.get("excluded_provider_type_keys") or ()),
        )

    def test_single_configured_provider_drops_dynamic_exclusion_from_create_request(self) -> None:
        mailbox = runtime_mailbox.Mailbox(
            provider="mail2925",
            email="candidate@healthy.test",
            ref="mail2925:session",
            session_id="session",
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(Path(tmp_dir) / "register-output"),
                    "REGISTER_MAILBOX_PROVIDERS": "mail2925",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": '{"openai":{"domainPool":[]}}',
                    "REGISTER_MAILBOX_BUSINESS_RETRY_ATTEMPTS": "1",
                },
                clear=True,
            ), mock.patch.object(
                runtime_mailbox,
                "_resolve_planned_mailbox_provider",
                return_value="",
            ), mock.patch.object(
                runtime_mailbox,
                "_resolve_mailbox_auto_excluded_provider_type_keys",
                return_value=("mail2925",),
            ), mock.patch.object(
                runtime_mailbox,
                "create_mailbox",
                return_value=mailbox,
            ) as create_mailbox, mock.patch.object(runtime_mailbox, "json_log"):
                resolved = runtime_mailbox.resolve_mailbox(
                    preallocated_email=None,
                    preallocated_session_id=None,
                    preallocated_mailbox_ref=None,
                    business_key="openai",
                    avoid_reason="previous_task_retry",
                )

        self.assertEqual("mail2925", resolved.provider)
        self.assertEqual(1, create_mailbox.call_count)
        self.assertNotIn(
            "mail2925",
            tuple(create_mailbox.call_args.kwargs.get("excluded_provider_type_keys") or ()),
        )

    def test_single_configured_provider_can_retry_after_zero_success_dynamic_blacklist(self) -> None:
        mailbox = runtime_mailbox.Mailbox(
            provider="mail2925",
            email="candidate@2925.com",
            ref="mail2925:session",
            session_id="session",
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            state_path = output_root / "others" / "register-mailbox-domain-state.json"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_text(
                json.dumps(
                    {
                        "schemaVersion": 3,
                        "businesses": {
                            "openai": {
                                "providers": {
                                    "mail2925": {
                                        "attempts": 20,
                                        "failures": 20,
                                        "successes": 0,
                                        "consecutiveFailures": 20,
                                        "blacklisted": False,
                                    }
                                },
                                "domains": {
                                    "2925.com": {
                                        "provider": "mail2925",
                                        "attempts": 20,
                                        "failures": 20,
                                        "successes": 0,
                                        "consecutiveFailures": 20,
                                        "blacklisted": False,
                                    }
                                },
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "REGISTER_MAILBOX_PROVIDERS": "mail2925",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": '{"openai":{"domainPool":[]}}',
                    "REGISTER_MAILBOX_BUSINESS_RETRY_ATTEMPTS": "1",
                    "REGISTER_MAILBOX_PROVIDER_ZERO_SUCCESS_BLACKLIST_MIN_ATTEMPTS": "20",
                    "REGISTER_MAILBOX_DOMAIN_BLACKLIST_MIN_ATTEMPTS": "20",
                },
                clear=True,
            ), mock.patch.object(
                runtime_mailbox,
                "_resolve_planned_mailbox_provider",
                return_value="",
            ), mock.patch.object(
                runtime_mailbox,
                "create_mailbox",
                return_value=mailbox,
            ) as create_mailbox, mock.patch.object(runtime_mailbox, "json_log"):
                resolved = runtime_mailbox.resolve_mailbox(
                    preallocated_email=None,
                    preallocated_session_id=None,
                    preallocated_mailbox_ref=None,
                    business_key="openai",
                )

        self.assertEqual("mail2925", resolved.provider)
        self.assertEqual(1, create_mailbox.call_count)
        self.assertNotIn(
            "mail2925",
            tuple(create_mailbox.call_args.kwargs.get("excluded_provider_type_keys") or ()),
        )

    def test_single_configured_provider_probes_and_retries_transient_instance_unavailable(self) -> None:
        mailbox = runtime_mailbox.Mailbox(
            provider="mail2925",
            email="candidate@2925.com",
            ref="mail2925:session",
            session_id="session",
        )
        unavailable = RuntimeError(
            "mail service POST /mail/mailboxes/open failed: HTTP 500 "
            "[code=PROVIDER_INSTANCE_UNAVAILABLE]: no active provider instance"
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(Path(tmp_dir) / "register-output"),
                    "REGISTER_MAILBOX_PROVIDERS": "mail2925",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": '{"openai":{"domainPool":[]}}',
                    "REGISTER_MAILBOX_BUSINESS_RETRY_ATTEMPTS": "1",
                    "REGISTER_MAILBOX_PROVIDER_INSTANCE_RECOVERY_ATTEMPTS": "1",
                    "REGISTER_MAILBOX_PROVIDER_INSTANCE_RECOVERY_DELAY_SECONDS": "6",
                },
                clear=True,
            ), mock.patch.object(
                runtime_mailbox,
                "_resolve_planned_mailbox_provider",
                return_value="mail2925",
            ), mock.patch.object(
                runtime_mailbox,
                "create_mailbox",
                side_effect=(unavailable, mailbox),
            ) as create_mailbox, mock.patch.object(
                runtime_mailbox,
                "probe_mailbox_provider",
                return_value={"ok": True, "status": "active"},
            ) as probe_mailbox_provider, mock.patch.object(
                runtime_mailbox.time,
                "sleep",
            ) as recovery_sleep, mock.patch.object(runtime_mailbox, "json_log"):
                resolved = runtime_mailbox.resolve_mailbox(
                    preallocated_email=None,
                    preallocated_session_id=None,
                    preallocated_mailbox_ref=None,
                    business_key="openai",
                    avoid_domains=("2925.com",),
                )

        self.assertEqual("mail2925", resolved.provider)
        self.assertEqual(2, create_mailbox.call_count)
        self.assertIn("2925.com", create_mailbox.call_args_list[0].kwargs["excluded_domains"])
        self.assertNotIn("2925.com", create_mailbox.call_args_list[1].kwargs["excluded_domains"])
        recovery_sleep.assert_called_once_with(6.0)
        probe_mailbox_provider.assert_called_once_with(
            provider_type_key="mail2925",
            preferred_instance_id="",
        )

    def test_provider_instance_recovery_retries_transient_probe_failure(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "REGISTER_MAILBOX_PROVIDER_INSTANCE_RECOVERY_DELAY_SECONDS": "6",
                "REGISTER_MAILBOX_PROVIDER_INSTANCE_RECOVERY_PROBE_ATTEMPTS": "3",
                "REGISTER_MAILBOX_PROVIDER_INSTANCE_RECOVERY_PROBE_RETRY_DELAY_SECONDS": "5",
            },
            clear=True,
        ), mock.patch.object(
            runtime_mailbox,
            "probe_mailbox_provider",
            side_effect=(RuntimeError("fetch failed"), {"ok": True, "status": "active"}),
        ) as probe_mailbox_provider, mock.patch.object(
            runtime_mailbox.time,
            "sleep",
        ) as recovery_sleep, mock.patch.object(runtime_mailbox, "json_log") as json_log:
            recovered = runtime_mailbox._recover_mailbox_provider_instance(
                provider="mail2925",
                business_key="openai",
            )

        self.assertTrue(recovered)
        self.assertEqual(2, probe_mailbox_provider.call_count)
        self.assertEqual([mock.call(6.0), mock.call(5.0)], recovery_sleep.call_args_list)
        self.assertTrue(json_log.call_args.args[0]["recovered"])
        self.assertEqual(2, json_log.call_args.args[0]["probeAttempts"])
        self.assertEqual("active", json_log.call_args.args[0]["probeStatus"])

    def test_single_configured_provider_instance_recovery_is_bounded(self) -> None:
        unavailable = RuntimeError(
            "mail service POST /mail/mailboxes/open failed: HTTP 500 "
            "[code=PROVIDER_INSTANCE_UNAVAILABLE]: no active provider instance"
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(Path(tmp_dir) / "register-output"),
                    "REGISTER_MAILBOX_PROVIDERS": "mail2925",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": '{"openai":{"domainPool":[]}}',
                    "REGISTER_MAILBOX_BUSINESS_RETRY_ATTEMPTS": "1",
                    "REGISTER_MAILBOX_PROVIDER_INSTANCE_RECOVERY_ATTEMPTS": "1",
                    "REGISTER_MAILBOX_PROVIDER_INSTANCE_RECOVERY_DELAY_SECONDS": "0",
                },
                clear=True,
            ), mock.patch.object(
                runtime_mailbox,
                "_resolve_planned_mailbox_provider",
                return_value="mail2925",
            ), mock.patch.object(
                runtime_mailbox,
                "create_mailbox",
                side_effect=unavailable,
            ) as create_mailbox, mock.patch.object(
                runtime_mailbox,
                "probe_mailbox_provider",
                return_value={"ok": True, "status": "active"},
            ) as probe_mailbox_provider, mock.patch.object(runtime_mailbox, "json_log"):
                with self.assertRaisesRegex(RuntimeError, "PROVIDER_INSTANCE_UNAVAILABLE"):
                    runtime_mailbox.resolve_mailbox(
                        preallocated_email=None,
                        preallocated_session_id=None,
                        preallocated_mailbox_ref=None,
                        business_key="openai",
                    )

        self.assertEqual(2, create_mailbox.call_count)
        self.assertEqual(1, probe_mailbox_provider.call_count)

    def test_provider_plan_scopes_dynamic_domains_to_selected_provider(self) -> None:
        mailbox = runtime_mailbox.Mailbox(
            provider="guerrillamail",
            email="candidate@healthy.test",
            ref="guerrillamail:session",
            session_id="session",
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            state_path = output_root / "others" / "register-mailbox-domain-state.json"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_text(
                json.dumps(
                    {
                        "schemaVersion": 3,
                        "businesses": {
                            "openai": {
                                "domains": {
                                    "cloud.test": {
                                        "provider": "cloudflare_temp_email",
                                        "blacklisted": True,
                                    },
                                    "guerrilla.test": {
                                        "provider": "guerrillamail",
                                        "blacklisted": True,
                                    },
                                }
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "REGISTER_MAILBOX_PROVIDERS": "guerrillamail,cloudflare_temp_email",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": '{"openai":{"domainPool":[]}}',
                    "REGISTER_MAILBOX_BUSINESS_RETRY_ATTEMPTS": "1",
                },
                clear=True,
            ), mock.patch.object(
                runtime_mailbox,
                "_resolve_planned_mailbox_provider",
                return_value="guerrillamail",
            ), mock.patch.object(
                runtime_mailbox,
                "create_mailbox",
                return_value=mailbox,
            ) as create_mailbox, mock.patch.object(runtime_mailbox, "json_log"):
                resolved = runtime_mailbox.resolve_mailbox(
                    preallocated_email=None,
                    preallocated_session_id=None,
                    preallocated_mailbox_ref=None,
                    business_key="openai",
                )

        self.assertEqual("guerrillamail", resolved.provider)
        excluded_domains = tuple(create_mailbox.call_args.kwargs.get("excluded_domains") or ())
        self.assertEqual(("guerrilla.test",), excluded_domains)
        self.assertNotIn("cloud.test", excluded_domains)

    def test_auto_provider_transient_failure_uses_structured_provider(self) -> None:
        self.assertEqual(
            ("im215", "", "provider_upstream_unavailable"),
            runtime_mailbox._mailbox_open_failure_avoidance(
                RuntimeError(
                    'mail service POST /mail/mailboxes/open failed: HTTP 500 '
                    '[code=MAILBOX_UPSTREAM_TRANSIENT]: '
                    '{"code":"MAILBOX_UPSTREAM_TRANSIENT","providerTypeKey":"im215"}'
                ),
                attempted_provider="moemail",
                attempted_provider_pinned=False,
                attempted_domain="",
            ),
        )

    def test_repeated_failure_from_same_provider_is_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(Path(tmp_dir) / "register-output"),
                    "REGISTER_MAILBOX_PROVIDERS": "temporam,cloudflare_temp_email",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": '{"openai":{"domainPool":[]}}',
                    "REGISTER_MAILBOX_BUSINESS_RETRY_ATTEMPTS": "1",
                },
                clear=True,
            ), mock.patch.object(
                runtime_mailbox,
                "_resolve_planned_mailbox_provider",
                return_value="temporam",
            ), mock.patch.object(
                runtime_mailbox,
                "create_mailbox",
                side_effect=RuntimeError(
                    "mail service POST /mail/mailboxes/open failed: HTTP 500 "
                    "[code=TEMPORAM_PROVIDER_FAILURE]: temporam getDomains failed: "
                    "status=401 (Unauthorized)"
                ),
            ) as create_mailbox, mock.patch.object(runtime_mailbox, "json_log"):
                with self.assertRaisesRegex(RuntimeError, "TEMPORAM_PROVIDER_FAILURE"):
                    runtime_mailbox.resolve_mailbox(
                        preallocated_email=None,
                        preallocated_session_id=None,
                        preallocated_mailbox_ref=None,
                        business_key="openai",
                    )

        self.assertEqual(2, create_mailbox.call_count)

    def test_provider_open_circuit_expires_after_ttl(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"REGISTER_MAILBOX_PROVIDER_OPEN_FAILURE_CIRCUIT_TTL_SECONDS": "1"},
            clear=False,
        ), mock.patch.object(
            runtime_mailbox.time,
            "monotonic",
            side_effect=(100.0, 102.0),
        ), mock.patch.object(runtime_mailbox, "json_log"):
            self.assertTrue(
                runtime_mailbox._open_provider_failure_circuit(
                    provider="temporam",
                    reason="provider_auth_unavailable",
                )
            )
            self.assertEqual((), runtime_mailbox._active_provider_open_failure_circuits())


if __name__ == "__main__":
    unittest.main()
