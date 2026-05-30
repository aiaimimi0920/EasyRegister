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

from others import easyemail_runtime, easyprotocol_runtime, local_config, runtime_mailbox, runtime_proxy_support, runtime_sms  # noqa: E402
from shared_mailbox import easy_email_client  # noqa: E402
from shared_sms import easy_sms_client  # noqa: E402


class EasyProtocolRuntimeTests(unittest.TestCase):
    def test_easy_sms_client_open_session_builds_free_first_request(self) -> None:
        with mock.patch.dict(
            os.environ,
        {
            "SMS_SERVICE_BASE_URL": "http://easy-sms:8080",
            "SMS_SERVICE_API_KEY": "sms-key",
        },
        clear=False,
    ), mock.patch.object(
        easy_sms_client,
        "_wait_sms_service_ready",
        return_value=None,
    ), mock.patch.object(
        easy_sms_client,
        "_get_json",
        return_value={
            "candidates": [
                {"providerKey": "hero_sms"},
                {"providerKey": "sms24"},
            ]
        },
    ), mock.patch.object(
        easy_sms_client,
        "_post_json",
        return_value={
            "session": {
                "id": "sms_123",
                "phoneNumber": "+15551234567",
                "providerKey": "sms24",
            }
        },
    ) as post_json:
            session = easy_sms_client.open_sms_session(
                business_key="openai",
                provider_blacklist=("hero_sms",),
                allow_paid=False,
                allow_reuse=False,
                max_bindings_per_phone=1,
                country_codes=("us",),
                selection_mode="available-first",
            )

        payload = post_json.call_args.args[1]
        self.assertEqual("openai", payload["businessKey"])
        self.assertEqual("free", payload["costTier"])
        self.assertEqual(False, payload["allowReuse"])
        self.assertEqual(1, payload["maxBindingsPerPhone"])
        self.assertEqual("sms24", payload["providerKey"])
        self.assertEqual("us", payload["countryCode"])
        self.assertNotIn("providerBlacklist", payload)
        self.assertNotIn("countryCodes", payload)
        self.assertNotIn("selectionMode", payload)
        self.assertEqual("sms_123", session.session_id)
        self.assertEqual("+15551234567", session.phone_number)
        self.assertEqual("sms24", session.provider_key)

    def test_easy_sms_client_report_outcome_uses_native_payload(self) -> None:
        with mock.patch.object(
            easy_sms_client,
            "_post_json",
            return_value={"result": {"recorded": True}},
        ) as post_json:
            result = easy_sms_client.report_sms_outcome(
                session_id="sms_123",
                outcome="failure",
                detail="wait_code_timeout",
            )

        payload = post_json.call_args.args[1]
        self.assertEqual("sms_123", payload["sessionId"])
        self.assertFalse(payload["success"])
        self.assertEqual("failure", payload["failureReason"])
        self.assertEqual("wait_code_timeout", payload["detail"])
        self.assertEqual("easyregister", payload["source"])
        self.assertIn("observedAt", payload)
        self.assertEqual({"recorded": True}, result)

    def test_easy_sms_client_preserves_supported_paid_selection_mode(self) -> None:
        with mock.patch.object(
            easy_sms_client,
            "_wait_sms_service_ready",
            return_value=None,
        ), mock.patch.object(
            easy_sms_client,
            "_get_json",
            return_value={"candidates": [{"providerKey": "hero_sms"}]},
        ), mock.patch.object(
            easy_sms_client,
            "_post_json",
            return_value={
                "session": {
                    "id": "sms_123",
                    "phoneNumber": "+15551234567",
                    "providerKey": "hero_sms",
                }
            },
        ) as post_json:
            session = easy_sms_client.open_sms_session(
                business_key="openai",
                provider_blacklist=(),
                allow_paid=True,
                allow_reuse=True,
                max_bindings_per_phone=1,
                country_codes=(),
                selection_mode="balanced",
            )

        payload = post_json.call_args.args[1]
        self.assertEqual("hero_sms", payload["providerKey"])
        self.assertEqual("paid", payload["costTier"])
        self.assertTrue(payload["allowReuse"])
        self.assertEqual("balanced", payload["selectionMode"])
        self.assertEqual("hero_sms", session.provider_key)

    def test_easy_sms_client_retries_next_provider_when_selected_candidate_is_unavailable(self) -> None:
        post_payloads: list[dict[str, object]] = []

        def _post(path: str, payload: dict[str, object]) -> dict[str, object]:
            post_payloads.append(dict(payload))
            provider_key = str(payload.get("providerKey") or "")
            if provider_key == "receive_smss":
                raise RuntimeError(
                    'sms service POST /sms/sessions/open failed: HTTP 503 '
                    '[code=Provider "receive_smss" is currently unavailable: '
                    'No eligible public numbers were available for a synthetic activation session.]'
                )
            return {
                "session": {
                    "id": "sms_124",
                    "phoneNumber": "+15557654321",
                    "providerKey": provider_key,
                }
            }

        with mock.patch.object(
            easy_sms_client,
            "_wait_sms_service_ready",
            return_value=None,
        ), mock.patch.object(
            easy_sms_client,
            "_get_json",
            return_value={
                "candidates": [
                    {"providerKey": "receive_smss"},
                    {"providerKey": "sms24"},
                ]
            },
        ), mock.patch.object(
            easy_sms_client,
            "_post_json",
            side_effect=_post,
        ):
            session = easy_sms_client.open_sms_session(
                business_key="openai",
                provider_blacklist=(),
                allow_paid=False,
                allow_reuse=False,
                max_bindings_per_phone=1,
                country_codes=(),
                selection_mode="balanced",
            )

        self.assertEqual(["receive_smss", "sms24"], [payload["providerKey"] for payload in post_payloads])
        self.assertEqual("sms24", session.provider_key)
        self.assertEqual("sms_124", session.session_id)

    def test_easy_sms_client_filters_empty_selection_plan_providers(self) -> None:
        with mock.patch.object(
            easy_sms_client,
            "_get_json",
            return_value={
                "candidates": [
                    {"providerKey": "onlinesim", "available": True, "healthState": "healthy"},
                    {"providerKey": "sms24", "available": True, "healthState": "empty"},
                    {"providerKey": "receive_smss", "available": False, "healthState": "degraded"},
                ]
            },
        ):
            candidates = easy_sms_client._query_provider_selection_candidates(
                provider_blacklist=(),
                allow_paid=False,
                country_codes=("+44",),
            )

        self.assertEqual(["onlinesim"], candidates)

    def test_easy_sms_client_catalog_fallback_when_selection_plan_is_exhausted(self) -> None:
        post_payloads: list[dict[str, object]] = []

        def _get(path: str) -> dict[str, object]:
            if path.startswith("/sms/query/providers/selection-plan?"):
                return {"candidates": [{"providerKey": "receive_smss"}]}
            if path.startswith("/sms/query/providers?"):
                return {"providers": [{"key": "receive_smss"}, {"key": "sms24"}]}
            return {}

        def _post(path: str, payload: dict[str, object]) -> dict[str, object]:
            post_payloads.append(dict(payload))
            provider_key = str(payload.get("providerKey") or "")
            if provider_key == "receive_smss":
                raise RuntimeError(
                    'sms service POST /sms/sessions/open failed: HTTP 503 '
                    '[code=Provider "receive_smss" is currently unavailable: '
                    'No eligible public numbers were available for a synthetic activation session.]'
                )
            return {
                "session": {
                    "id": "sms_124",
                    "phoneNumber": "+15551234567",
                    "providerKey": "sms24",
                }
            }

        with mock.patch.object(
            easy_sms_client,
            "_wait_sms_service_ready",
            return_value=None,
        ), mock.patch.object(
            easy_sms_client,
            "_get_json",
            side_effect=_get,
        ), mock.patch.object(
            easy_sms_client,
            "_post_json",
            side_effect=_post,
        ):
            session = easy_sms_client.open_sms_session(
                business_key="openai",
                provider_blacklist=(),
                allow_paid=False,
                allow_reuse=False,
                max_bindings_per_phone=1,
                country_codes=("+31",),
                selection_mode="balanced",
            )

        self.assertEqual(["receive_smss", "sms24"], [payload["providerKey"] for payload in post_payloads])
        self.assertEqual("sms_124", session.session_id)

    def test_easy_sms_client_catalog_fallback_when_selection_plan_is_empty(self) -> None:
        post_payloads: list[dict[str, object]] = []

        def _get(path: str) -> dict[str, object]:
            if path.startswith("/sms/query/providers/selection-plan?"):
                return {"candidates": []}
            if path.startswith("/sms/query/providers?"):
                return {"providers": [{"key": "onlinesim"}]}
            return {}

        def _post(path: str, payload: dict[str, object]) -> dict[str, object]:
            post_payloads.append(dict(payload))
            return {
                "session": {
                    "id": "sms_125",
                    "phoneNumber": "+15557654321",
                    "providerKey": "onlinesim",
                }
            }

        with mock.patch.object(
            easy_sms_client,
            "_wait_sms_service_ready",
            return_value=None,
        ), mock.patch.object(
            easy_sms_client,
            "_get_json",
            side_effect=_get,
        ), mock.patch.object(
            easy_sms_client,
            "_post_json",
            side_effect=_post,
        ):
            session = easy_sms_client.open_sms_session(
                business_key="openai",
                provider_blacklist=(),
                allow_paid=False,
                allow_reuse=False,
                max_bindings_per_phone=1,
                country_codes=("+46",),
                selection_mode="balanced",
            )

        self.assertEqual(["onlinesim"], [payload["providerKey"] for payload in post_payloads])
        self.assertEqual("sms_125", session.session_id)

    def test_easy_sms_client_rotates_country_codes_when_phone_is_blacklisted(self) -> None:
        post_payloads: list[dict[str, object]] = []
        reported_outcomes: list[dict[str, object]] = []

        def _post(path: str, payload: dict[str, object]) -> dict[str, object]:
            if path == "/sms/sessions/report-outcome":
                reported_outcomes.append(dict(payload))
                return {"result": {"accepted": True}}
            post_payloads.append(dict(payload))
            country_code = str(payload.get("countryCode") or "")
            if country_code == "+31":
                return {
                    "session": {
                        "id": "sms_bad",
                        "phoneNumber": "+31616835325",
                        "providerKey": "onlinesim",
                    }
                }
            return {
                "session": {
                    "id": "sms_good",
                    "phoneNumber": "+33774749623",
                    "providerKey": "onlinesim",
                }
            }

        with mock.patch.object(
            easy_sms_client,
            "_wait_sms_service_ready",
            return_value=None,
        ), mock.patch.object(
            easy_sms_client,
            "_get_json",
            return_value={"candidates": [{"providerKey": "onlinesim"}]},
        ), mock.patch.object(
            easy_sms_client,
            "_post_json",
            side_effect=_post,
        ):
            session = easy_sms_client.open_sms_session(
                business_key="openai",
                provider_blacklist=(),
                allow_paid=False,
                allow_reuse=False,
                max_bindings_per_phone=1,
                country_codes=("+31", "+33"),
                selection_mode="balanced",
                phone_blacklist=("+31616835325",),
            )

        self.assertEqual(["+31", "+33"], [payload["countryCode"] for payload in post_payloads])
        self.assertEqual(["sms_bad"], [payload["sessionId"] for payload in reported_outcomes])
        self.assertFalse(bool(reported_outcomes[0]["success"]))
        self.assertEqual("blacklisted_phone_number", reported_outcomes[0]["detail"])
        self.assertEqual("sms_good", session.session_id)
        self.assertEqual("+33774749623", session.phone_number)

    def test_easy_sms_client_skips_provider_country_blacklist_before_opening_session(self) -> None:
        post_payloads: list[dict[str, object]] = []

        def _post(path: str, payload: dict[str, object]) -> dict[str, object]:
            post_payloads.append(dict(payload))
            country_code = str(payload.get("countryCode") or "")
            if country_code == "+31":
                raise AssertionError("blacklisted provider-country should not be opened")
            return {
                "session": {
                    "id": "sms_good",
                    "phoneNumber": "+33774749623",
                    "providerKey": "onlinesim",
                }
            }

        with mock.patch.object(
            easy_sms_client,
            "_wait_sms_service_ready",
            return_value=None,
        ), mock.patch.object(
            easy_sms_client,
            "_get_json",
            return_value={"candidates": [{"providerKey": "onlinesim"}]},
        ), mock.patch.object(
            easy_sms_client,
            "_post_json",
            side_effect=_post,
        ):
            session = easy_sms_client.open_sms_session(
                business_key="openai",
                provider_blacklist=(),
                allow_paid=False,
                allow_reuse=False,
                max_bindings_per_phone=1,
                country_codes=("+31", "+33"),
                selection_mode="balanced",
                provider_country_blacklist=("onlinesim|+31",),
            )

        self.assertEqual(["+33"], [payload["countryCode"] for payload in post_payloads])
        self.assertEqual("sms_good", session.session_id)
        self.assertEqual("+33774749623", session.phone_number)

    def test_easy_sms_client_does_not_catalog_fallback_when_selection_plan_is_fully_country_blacklisted(self) -> None:
        def _get(path: str) -> dict[str, object]:
            if path.startswith("/sms/query/providers/selection-plan?"):
                return {"candidates": [{"providerKey": "onlinesim"}]}
            if path.startswith("/sms/query/providers?"):
                raise AssertionError("catalog fallback should not run after provider-country exhaustion")
            return {}

        with mock.patch.object(
            easy_sms_client,
            "_wait_sms_service_ready",
            return_value=None,
        ), mock.patch.object(
            easy_sms_client,
            "_get_json",
            side_effect=_get,
        ), mock.patch.object(
            easy_sms_client,
            "_post_json",
            side_effect=AssertionError("blacklisted country should not be opened"),
        ):
            with self.assertRaisesRegex(RuntimeError, "sms_no_unblocked_provider_country_candidates"):
                easy_sms_client.open_sms_session(
                    business_key="openai",
                    provider_blacklist=(),
                    allow_paid=False,
                    allow_reuse=False,
                    max_bindings_per_phone=1,
                    country_codes=("+31",),
                    selection_mode="balanced",
                    provider_country_blacklist=("onlinesim|+31",),
                )

    def test_easy_sms_client_wait_code_polls_until_value(self) -> None:
        with mock.patch.object(
            easy_sms_client,
            "_get_json",
            side_effect=[
                {"code": {}},
                {"code": {"value": "123456"}},
            ],
        ), mock.patch("shared_sms.easy_sms_client.time.sleep", return_value=None):
            code = easy_sms_client.wait_sms_code(session_id="sms_123", timeout_seconds=10)

        self.assertEqual("123456", code)

    def test_read_easysms_server_api_key_reads_direct_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            easysms_dir = root / "EasySms"
            easysms_dir.mkdir(parents=True, exist_ok=True)
            (easysms_dir / "config.yaml").write_text('apiKey: "sms-secret"\n', encoding="utf-8")
            start_path = root / "project" / "dummy.py"
            start_path.parent.mkdir(parents=True, exist_ok=True)
            start_path.write_text("", encoding="utf-8")

            api_key = local_config.read_easysms_server_api_key(start_path=start_path)

        self.assertEqual("sms-secret", api_key)

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

    def test_create_openai_account_can_bridge_protocol_storage_path_from_mirror(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            protocol_mirror_dir = Path(tmp_dir) / "protocol-output"
            protocol_storage_path = "/shared/register-output/others/run/small.json"
            mirrored_source = protocol_mirror_dir / "others" / "run" / "small.json"
            mirrored_source.parent.mkdir(parents=True)
            mirrored_source.write_text('{"ok": true}', encoding="utf-8")
            bridge_dir = Path(tmp_dir) / "bridge"

            with mock.patch.object(
                easyprotocol_runtime,
                "invoke_easyprotocol",
                return_value={"storage_path": protocol_storage_path, "ok": True},
            ):
                with mock.patch.dict(
                    os.environ,
                    {
                        "REGISTER_PROTOCOL_BRIDGE_DIR": str(bridge_dir),
                        "REGISTER_PROTOCOL_OUTPUT_TARGET_DIR": "/shared/register-output",
                        "REGISTER_PROTOCOL_OUTPUT_MIRROR_DIR": str(protocol_mirror_dir),
                    },
                    clear=False,
                ):
                    result = easyprotocol_runtime.dispatch_easyprotocol_step(
                        step_type="create_openai_account",
                        step_input={},
                    )

            expected_path = bridge_dir / mirrored_source.name
            self.assertEqual(str(expected_path.resolve()), result["storage_path"])
            self.assertEqual(protocol_storage_path, result["original_storage_path"])
            self.assertEqual(str(expected_path.resolve()), result["bridged_storage_path"])
            self.assertTrue(expected_path.is_file())

    def test_easyprotocol_source_path_is_bridged_and_synced_back(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            local_source = Path(tmp_dir) / "local-claim.json"
            local_source.write_text('{"email": "user@example.com"}', encoding="utf-8")
            bridge_dir = Path(tmp_dir) / "protocol-visible"
            captured_inputs: list[dict[str, object]] = []

            def _invoke(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                captured_inputs.append(dict(step_input))
                protocol_source = Path(str(step_input["source_path"]))
                self.assertEqual((bridge_dir / local_source.name).resolve(), protocol_source.resolve())
                protocol_source.write_text(
                    '{"email": "user@example.com", "organizationId": "org_123"}',
                    encoding="utf-8",
                )
                return {
                    "ok": True,
                    "sourcePath": str(protocol_source),
                    "successPath": str(protocol_source),
                }

            with mock.patch.object(easyprotocol_runtime, "invoke_easyprotocol", side_effect=_invoke):
                with mock.patch.dict(
                    os.environ,
                    {"REGISTER_PROTOCOL_BRIDGE_DIR": str(bridge_dir)},
                    clear=False,
                ):
                    result = easyprotocol_runtime.dispatch_easyprotocol_step(
                        step_type="initialize_platform_organization",
                        step_input={"source_path": str(local_source), "proxy_url": "http://proxy.local:8080"},
                    )

            self.assertEqual(1, len(captured_inputs))
            self.assertEqual(str(local_source.resolve()), result["sourcePath"])
            self.assertEqual(str(local_source.resolve()), result["successPath"])
            self.assertIn("org_123", local_source.read_text(encoding="utf-8"))

    def test_easyprotocol_source_path_bridge_can_use_protocol_target_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            local_source = Path(tmp_dir) / "local-claim.json"
            local_source.write_text('{"email": "user@example.com"}', encoding="utf-8")
            bridge_dir = Path(tmp_dir) / "protocol-visible"
            protocol_target_dir = "/shared/register-output/easyregister-bridge"
            captured_inputs: list[dict[str, object]] = []

            def _invoke(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                captured_inputs.append(dict(step_input))
                protocol_source = str(step_input["source_path"])
                expected_protocol_source = f"{protocol_target_dir}/{local_source.name}"
                self.assertEqual(expected_protocol_source, protocol_source)
                local_bridge_source = bridge_dir / local_source.name
                self.assertTrue(local_bridge_source.is_file())
                local_bridge_source.write_text(
                    '{"email": "user@example.com", "organizationId": "org_target"}',
                    encoding="utf-8",
                )
                return {
                    "ok": True,
                    "sourcePath": protocol_source,
                    "successPath": protocol_source,
                }

            with mock.patch.object(easyprotocol_runtime, "invoke_easyprotocol", side_effect=_invoke):
                with mock.patch.dict(
                    os.environ,
                    {
                        "REGISTER_PROTOCOL_BRIDGE_DIR": str(bridge_dir),
                        "REGISTER_PROTOCOL_BRIDGE_TARGET_DIR": protocol_target_dir,
                    },
                    clear=False,
                ):
                    result = easyprotocol_runtime.dispatch_easyprotocol_step(
                        step_type="initialize_platform_organization",
                        step_input={"source_path": str(local_source), "proxy_url": "http://proxy.local:8080"},
                    )

            self.assertEqual(1, len(captured_inputs))
            self.assertEqual(str(local_source.resolve()), result["sourcePath"])
            self.assertEqual(str(local_source.resolve()), result["successPath"])
            self.assertIn("org_target", local_source.read_text(encoding="utf-8"))

    def test_dispatch_obtain_codex_oauth_completes_phone_verification_when_phone_wall_returned(self) -> None:
        captured_inputs: list[dict[str, object]] = []

        def _invoke(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
            captured_inputs.append(dict(step_input))
            if len(captured_inputs) == 1:
                return {
                    "ok": True,
                    "status": "phone_verification_required",
                    "phoneVerificationRequired": True,
                    "pageType": "add_phone",
                    "resumeContext": {"flow": "oauth", "token": "resume_123"},
                }
            if len(captured_inputs) == 2:
                return {
                    "ok": True,
                    "status": "phone_number_submitted",
                    "pageType": "sms_verification",
                    "resumeContext": {"flow": "oauth", "token": "resume_123_step2"},
                }
            return {
                "ok": True,
                "status": "completed",
                "successPath": "C:/tmp/codex-free.json",
                "userId": "user_123",
            }

        with mock.patch.dict(
            os.environ,
            {
                "REGISTER_SMS_BUSINESS_POLICIES_JSON": (
                    '{"openai":{"enabled":true,"providerBlacklist":["hero_sms"],'
                    '"allowPaid":false,"allowReuse":false,"maxBindingsPerPhone":1,'
                    '"countryCodes":["US"],"selectionMode":"balanced"}}'
                )
            },
            clear=False,
        ), mock.patch.object(
            easyprotocol_runtime,
            "invoke_easyprotocol",
            side_effect=_invoke,
        ), mock.patch.object(
            easyprotocol_runtime.runtime_sms,
            "open_phone_session_for_business",
            return_value={"sessionId": "sms_123", "phoneNumber": "+15551234567", "providerKey": "sms24"},
        ), mock.patch.object(
            easyprotocol_runtime.runtime_sms,
            "wait_phone_code_for_session",
            return_value="123456",
        ), mock.patch.object(
            easyprotocol_runtime.runtime_sms,
            "report_phone_outcome_for_session",
            return_value={"ok": True},
        ):
            result = easyprotocol_runtime.dispatch_easyprotocol_step(
                step_type="obtain_codex_oauth",
                step_input={"source_path": "C:/tmp/small.json", "output_dir": "C:/tmp/out"},
            )

        self.assertTrue(result["ok"])
        self.assertEqual("completed", result["status"])
        self.assertTrue(result["phoneVerificationAttempted"])
        self.assertEqual("sms24", result["phoneProvider"])
        self.assertEqual("sms_123", result["phoneSessionId"])
        self.assertIn("sms_verification", captured_inputs[0])
        self.assertEqual(["hero_sms"], captured_inputs[0]["sms_verification"]["provider_blacklist"])
        self.assertFalse(bool(captured_inputs[0]["sms_verification"]["allow_paid"]))
        self.assertEqual(["us"], captured_inputs[0]["sms_verification"]["country_codes"])
        self.assertEqual("balanced", captured_inputs[0]["sms_verification"]["selection_mode"])
        self.assertEqual("resume_123", captured_inputs[1]["resume_context"]["token"])
        self.assertEqual("resume_123_step2", captured_inputs[2]["resume_context"]["token"])

    def test_dispatch_obtain_codex_oauth_recovers_phone_wall_artifact_after_protocol_timeout(self) -> None:
        captured_inputs: list[dict[str, object]] = []

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir) / "run"
            first_phone_dir = output_dir / "first_phone"
            first_phone_dir.mkdir(parents=True)
            (first_phone_dir / "phone-wall.json").write_text(
                json.dumps(
                    {
                        "outcome": "phone_wall",
                        "pageType": "add_phone",
                        "finalUrl": "https://auth.openai.com/add-phone",
                        "resumeContext": {"flow": "oauth", "token": "resume_from_artifact"},
                    }
                ),
                encoding="utf-8",
            )

            def _invoke(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                captured_inputs.append({"step_type": step_type, **dict(step_input)})
                if step_type == "obtain_codex_oauth":
                    raise RuntimeError("flow_timeout_exceeded")
                if step_type == "submit_phone_verification_number":
                    return {
                        "ok": True,
                        "status": "phone_number_submitted",
                        "pageType": "sms_verification",
                        "resumeContext": {"flow": "oauth", "token": "resume_after_number"},
                    }
                return {
                    "ok": True,
                    "status": "completed",
                    "successPath": "C:/tmp/codex-free.json",
                    "userId": "user_123",
                }

            with mock.patch.object(
                easyprotocol_runtime,
                "invoke_easyprotocol",
                side_effect=_invoke,
            ), mock.patch.object(
                easyprotocol_runtime.runtime_sms,
                "open_phone_session_for_business",
                return_value={"sessionId": "sms_123", "phoneNumber": "+15551234567", "providerKey": "sms24"},
            ), mock.patch.object(
                easyprotocol_runtime.runtime_sms,
                "wait_phone_code_for_session",
                return_value="123456",
            ), mock.patch.object(
                easyprotocol_runtime.runtime_sms,
                "report_phone_outcome_for_session",
                return_value={"ok": True},
            ):
                result = easyprotocol_runtime.dispatch_easyprotocol_step(
                    step_type="obtain_codex_oauth",
                    step_input={"source_path": "C:/tmp/small.json", "output_dir": str(output_dir)},
                )

        self.assertTrue(result["ok"])
        self.assertEqual("completed", result["status"])
        self.assertTrue(result["phoneVerificationAttempted"])
        self.assertEqual(
            "resume_from_artifact",
            next(item for item in captured_inputs if item["step_type"] == "submit_phone_verification_number")[
                "resume_context"
            ]["token"],
        )

    def test_dispatch_obtain_codex_oauth_uses_short_initial_protocol_timeout_for_phone_wall_recovery(self) -> None:
        captured_calls: list[dict[str, object]] = []

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir) / "run"
            first_phone_dir = output_dir / "first_phone"
            first_phone_dir.mkdir(parents=True)
            (first_phone_dir / "phone-wall.json").write_text(
                json.dumps(
                    {
                        "outcome": "phone_wall",
                        "pageType": "add_phone",
                        "finalUrl": "https://auth.openai.com/add-phone",
                        "resumeContext": {"flow": "oauth", "token": "resume_from_artifact"},
                    }
                ),
                encoding="utf-8",
            )

            class _FakeResponse:
                def __init__(self, payload: dict[str, object]) -> None:
                    self._payload = payload

                def __enter__(self) -> "_FakeResponse":
                    return self

                def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
                    return False

                def read(self) -> bytes:
                    return json.dumps(self._payload).encode("utf-8")

            def _urlopen(req: object, timeout: object = None) -> object:
                request_payload = json.loads(req.data.decode("utf-8"))  # type: ignore[attr-defined]
                step_type = request_payload["payload"]["step_type"]
                captured_calls.append({"step_type": step_type, "timeout_seconds": timeout})
                if step_type == "obtain_codex_oauth":
                    raise TimeoutError("timed out")
                if step_type == "submit_phone_verification_number":
                    return _FakeResponse(
                        {
                            "status": "completed",
                            "result": {
                                "step_result": {
                                    "ok": True,
                                    "status": "phone_number_submitted",
                                    "pageType": "sms_verification",
                                    "resumeContext": {"flow": "oauth", "token": "resume_after_number"},
                                }
                            },
                        }
                    )
                raise AssertionError(f"unexpected invoke: {step_type} {request_payload!r}")

            with mock.patch.dict(
                os.environ,
                {"EASY_PROTOCOL_OAUTH_TIMEOUT_SECONDS": "45"},
                clear=False,
            ), mock.patch.object(
                easyprotocol_runtime.urllib.request,
                "urlopen",
                side_effect=_urlopen,
            ), mock.patch.object(
                easyprotocol_runtime.runtime_sms,
                "open_phone_session_for_business",
                return_value={"sessionId": "sms_123", "phoneNumber": "+15551234567", "providerKey": "sms24"},
            ), mock.patch.object(
                easyprotocol_runtime.runtime_sms,
                "wait_phone_code_for_session",
                side_effect=RuntimeError("wait_code_timeout"),
            ), mock.patch.object(
                easyprotocol_runtime.runtime_sms,
                "report_phone_outcome_for_session",
                return_value={"ok": True},
            ):
                result = easyprotocol_runtime.dispatch_easyprotocol_step(
                    step_type="obtain_codex_oauth",
                    step_input={"source_path": "C:/tmp/small.json", "output_dir": str(output_dir)},
                )

        self.assertEqual("phone_verification_submitted_small_success", result["status"])
        self.assertEqual(45, captured_calls[0]["timeout_seconds"])
        self.assertEqual("submit_phone_verification_number", captured_calls[1]["step_type"])

    def test_dispatch_obtain_codex_oauth_uses_short_phone_submit_protocol_timeout(self) -> None:
        captured_calls: list[dict[str, object]] = []

        class _FakeResponse:
            def __init__(self, payload: dict[str, object]) -> None:
                self._payload = payload

            def __enter__(self) -> "_FakeResponse":
                return self

            def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
                return False

            def read(self) -> bytes:
                return json.dumps(self._payload).encode("utf-8")

        def _urlopen(req: object, timeout: object = None) -> object:
            request_payload = json.loads(req.data.decode("utf-8"))  # type: ignore[attr-defined]
            step_type = request_payload["payload"]["step_type"]
            captured_calls.append({"step_type": step_type, "timeout_seconds": timeout})
            if step_type == "obtain_codex_oauth":
                return _FakeResponse(
                    {
                        "status": "completed",
                        "result": {
                            "step_result": {
                                "ok": True,
                                "status": "phone_verification_required",
                                "phoneVerificationRequired": True,
                                "pageType": "add_phone",
                                "resumeContext": {"flow": "oauth", "token": "resume_123"},
                            }
                        },
                    }
                )
            if step_type == "submit_phone_verification_number":
                raise TimeoutError("timed out")
            raise AssertionError(f"unexpected invoke: {step_type} {request_payload!r}")

        with mock.patch.dict(
            os.environ,
            {
                "EASY_PROTOCOL_OAUTH_TIMEOUT_SECONDS": "45",
                "EASY_PROTOCOL_PHONE_TIMEOUT_SECONDS": "12",
            },
            clear=False,
        ), mock.patch.object(
            easyprotocol_runtime.urllib.request,
            "urlopen",
            side_effect=_urlopen,
        ), mock.patch.object(
            easyprotocol_runtime.runtime_sms,
            "open_phone_session_for_business",
            return_value={"sessionId": "sms_123", "phoneNumber": "+15551234567", "providerKey": "sms24"},
        ), mock.patch.object(
            easyprotocol_runtime.runtime_sms,
            "report_phone_outcome_for_session",
            return_value={"ok": True},
        ):
            result = easyprotocol_runtime.dispatch_easyprotocol_step(
                step_type="obtain_codex_oauth",
                step_input={"source_path": "C:/tmp/small.json", "output_dir": "C:/tmp/out"},
            )

        self.assertEqual("obtain_codex_oauth", captured_calls[0]["step_type"])
        self.assertEqual(45, captured_calls[0]["timeout_seconds"])
        self.assertEqual("submit_phone_verification_number", captured_calls[1]["step_type"])
        self.assertEqual(12, captured_calls[1]["timeout_seconds"])
        self.assertEqual("phone_verification_attempted_small_success", result["status"])

    def test_dispatch_obtain_codex_oauth_treats_phone_submit_timeout_as_intermediate_result(self) -> None:
        def _invoke(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
            if step_type == "obtain_codex_oauth":
                return {
                    "ok": True,
                    "status": "phone_verification_required",
                    "phoneVerificationRequired": True,
                    "pageType": "add_phone",
                    "resumeContext": {"flow": "oauth", "token": "resume_123"},
                    "successPath": "C:/tmp/openai-oauth.json",
                }
            if step_type == "submit_phone_verification_number":
                raise RuntimeError("easyprotocol_transport_failed:timed out")
            raise AssertionError(f"unexpected invoke: {step_type}")

        with mock.patch.object(
            easyprotocol_runtime,
            "invoke_easyprotocol",
            side_effect=_invoke,
        ), mock.patch.object(
            easyprotocol_runtime.runtime_sms,
            "open_phone_session_for_business",
            return_value={"sessionId": "sms_123", "phoneNumber": "+15551234567", "providerKey": "sms24"},
        ), mock.patch.object(
            easyprotocol_runtime.runtime_sms,
            "report_phone_outcome_for_session",
            return_value={"ok": True},
        ):
            result = easyprotocol_runtime.dispatch_easyprotocol_step(
                step_type="obtain_codex_oauth",
                step_input={"source_path": "C:/tmp/small.json", "output_dir": "C:/tmp/out"},
            )

        self.assertEqual("phone_verification_attempted_small_success", result["status"])
        self.assertTrue(result["phoneVerificationAttempted"])
        self.assertFalse(result["phoneVerificationSubmitted"])
        self.assertEqual("submit_phone_verification_number", result["phoneVerificationFailureStage"])
        self.assertEqual("sms24", result["phoneProvider"])
        self.assertEqual("sms_123", result["phoneSessionId"])

    def test_dispatch_obtain_codex_oauth_treats_terminal_phone_verification_as_intermediate_result(self) -> None:
        captured_inputs: list[dict[str, object]] = []

        def _invoke(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
            captured_inputs.append(dict(step_input))
            if len(captured_inputs) == 1:
                return {
                    "ok": True,
                    "status": "phone_verification_required",
                    "phoneVerificationRequired": True,
                    "pageType": "add_phone",
                    "resumeContext": {"flow": "oauth", "token": "resume_123"},
                    "successPath": "C:/tmp/openai-oauth.json",
                }
            return {
                "ok": True,
                "status": "phone_verification_terminal",
                "pageType": "add_phone",
                "resumeContext": {"flow": "oauth", "token": "resume_123_step2"},
                "phoneVerificationAttempted": True,
                "phoneVerificationTerminal": True,
                "phoneVerificationTerminalCode": "unsupported_phone_region",
                "phoneVerificationTerminalMessage": "Phone region is not supported.",
                "phoneVerificationTerminalStatusCode": 403,
            }

        with mock.patch.object(
            easyprotocol_runtime,
            "invoke_easyprotocol",
            side_effect=_invoke,
        ), mock.patch.object(
            easyprotocol_runtime.runtime_sms,
            "open_phone_session_for_business",
            return_value={"sessionId": "sms_123", "phoneNumber": "+15551234567", "providerKey": "sms24"},
        ), mock.patch.object(
            easyprotocol_runtime.runtime_sms,
            "wait_phone_code_for_session",
        ) as wait_phone_code, mock.patch.object(
            easyprotocol_runtime.runtime_sms,
            "record_terminal_phone_outcome",
            return_value={"ok": True},
        ) as record_terminal_phone_outcome, mock.patch.object(
            easyprotocol_runtime.runtime_sms,
            "report_phone_outcome_for_session",
            return_value={"ok": True},
        ) as report_phone_outcome:
            result = easyprotocol_runtime.dispatch_easyprotocol_step(
                step_type="obtain_codex_oauth",
                step_input={"source_path": "C:/tmp/small.json", "output_dir": "C:/tmp/out"},
            )

        self.assertTrue(result["ok"])
        self.assertEqual("phone_verification_terminal", result["status"])
        self.assertTrue(result["phoneVerificationAttempted"])
        self.assertTrue(result["phoneVerificationTerminal"])
        self.assertEqual("unsupported_phone_region", result["phoneVerificationTerminalCode"])
        self.assertEqual("sms24", result["phoneProvider"])
        wait_phone_code.assert_not_called()
        record_terminal_phone_outcome.assert_called_once_with(
            phone_number="+15551234567",
            provider_key="sms24",
            terminal_code="unsupported_phone_region",
            terminal_message="Phone region is not supported.",
        )
        report_phone_outcome.assert_called_once()

    def test_dispatch_obtain_codex_oauth_retries_phone_scoped_terminal_with_next_number(self) -> None:
        captured_inputs: list[dict[str, object]] = []
        phone_sessions = [
            {"sessionId": "sms_1", "phoneNumber": "+46720085698", "providerKey": "onlinesim"},
            {"sessionId": "sms_2", "phoneNumber": "+33774749623", "providerKey": "onlinesim"},
        ]

        def _invoke(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
            captured_inputs.append({"step_type": step_type, **dict(step_input)})
            if step_type == "obtain_codex_oauth":
                return {
                    "ok": True,
                    "status": "phone_verification_required",
                    "phoneVerificationRequired": True,
                    "pageType": "add_phone",
                    "resumeContext": {"flow": "oauth", "token": "resume_123"},
                    "successPath": "C:/tmp/openai-oauth.json",
                }
            if step_type == "submit_phone_verification_number" and step_input.get("phone_session_id") == "sms_1":
                return {
                    "ok": True,
                    "status": "phone_verification_terminal",
                    "pageType": "add_phone",
                    "resumeContext": {"flow": "oauth", "token": "resume_123_retry"},
                    "phoneVerificationAttempted": True,
                    "phoneVerificationTerminal": True,
                    "phoneVerificationTerminalCode": "phone_number_in_use",
                    "phoneVerificationTerminalMessage": "Phone number already in use.",
                    "phoneVerificationTerminalStatusCode": 400,
                }
            if step_type == "submit_phone_verification_number" and step_input.get("phone_session_id") == "sms_2":
                return {
                    "ok": True,
                    "status": "phone_number_submitted",
                    "pageType": "sms_verification",
                    "resumeContext": {"flow": "oauth", "token": "resume_123_sms", "pageType": "sms_verification"},
                }
            raise AssertionError(f"unexpected invoke: {step_type} {step_input!r}")

        with mock.patch.object(
            easyprotocol_runtime,
            "invoke_easyprotocol",
            side_effect=_invoke,
        ), mock.patch.object(
            easyprotocol_runtime.runtime_sms,
            "open_phone_session_for_business",
            side_effect=phone_sessions,
        ) as open_phone_session_for_business, mock.patch.object(
            easyprotocol_runtime.runtime_sms,
            "wait_phone_code_for_session",
            side_effect=RuntimeError("wait_code_timeout"),
        ) as wait_phone_code, mock.patch.object(
            easyprotocol_runtime.runtime_sms,
            "record_terminal_phone_outcome",
            return_value={"ok": True},
        ) as record_terminal_phone_outcome, mock.patch.object(
            easyprotocol_runtime.runtime_sms,
            "report_phone_outcome_for_session",
            return_value={"ok": True},
        ) as report_phone_outcome:
            result = easyprotocol_runtime.dispatch_easyprotocol_step(
                step_type="obtain_codex_oauth",
                step_input={"source_path": "C:/tmp/small.json", "output_dir": "C:/tmp/out"},
            )

        self.assertTrue(result["ok"])
        self.assertEqual("phone_verification_submitted_small_success", result["status"])
        self.assertEqual("sms_2", result["phoneSessionId"])
        self.assertEqual("+33774749623", result["phoneNumber"])
        self.assertEqual("wait_sms_code", result["phoneVerificationFailureStage"])
        self.assertEqual(2, open_phone_session_for_business.call_count)
        self.assertEqual(
            ["sms_1", "sms_2"],
            [
                item["phone_session_id"]
                for item in captured_inputs
                if item["step_type"] == "submit_phone_verification_number"
            ],
        )
        wait_phone_code.assert_called_once_with(session_id="sms_2", timeout_seconds=180)
        record_terminal_phone_outcome.assert_called_once_with(
            phone_number="+46720085698",
            provider_key="onlinesim",
            terminal_code="phone_number_in_use",
            terminal_message="Phone number already in use.",
        )
        self.assertEqual(2, report_phone_outcome.call_count)

    def test_dispatch_obtain_codex_oauth_retries_rate_limit_terminal_with_next_number(self) -> None:
        captured_inputs: list[dict[str, object]] = []
        phone_sessions = [
            {"sessionId": "sms_1", "phoneNumber": "+41779793490", "providerKey": "onlinesim"},
            {"sessionId": "sms_2", "phoneNumber": "+353894602760", "providerKey": "onlinesim"},
        ]

        def _invoke(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
            captured_inputs.append({"step_type": step_type, **dict(step_input)})
            if step_type == "obtain_codex_oauth":
                return {
                    "ok": True,
                    "status": "phone_verification_required",
                    "phoneVerificationRequired": True,
                    "pageType": "add_phone",
                    "resumeContext": {"flow": "oauth", "token": "resume_123"},
                    "successPath": "C:/tmp/openai-oauth.json",
                }
            if step_type == "submit_phone_verification_number" and step_input.get("phone_session_id") == "sms_1":
                return {
                    "ok": True,
                    "status": "phone_verification_terminal",
                    "pageType": "add_phone",
                    "resumeContext": {"flow": "oauth", "token": "resume_123_retry"},
                    "phoneVerificationAttempted": True,
                    "phoneVerificationTerminal": True,
                    "phoneVerificationTerminalCode": "rate_limit_exceeded",
                    "phoneVerificationTerminalMessage": "Too many phone verification requests.",
                    "phoneVerificationTerminalStatusCode": 400,
                }
            if step_type == "submit_phone_verification_number" and step_input.get("phone_session_id") == "sms_2":
                return {
                    "ok": True,
                    "status": "phone_number_submitted",
                    "pageType": "sms_verification",
                    "resumeContext": {"flow": "oauth", "token": "resume_123_sms", "pageType": "sms_verification"},
                }
            raise AssertionError(f"unexpected invoke: {step_type} {step_input!r}")

        with mock.patch.object(
            easyprotocol_runtime,
            "invoke_easyprotocol",
            side_effect=_invoke,
        ), mock.patch.object(
            easyprotocol_runtime.runtime_sms,
            "open_phone_session_for_business",
            side_effect=phone_sessions,
        ) as open_phone_session_for_business, mock.patch.object(
            easyprotocol_runtime.runtime_sms,
            "wait_phone_code_for_session",
            side_effect=RuntimeError("wait_code_timeout"),
        ) as wait_phone_code, mock.patch.object(
            easyprotocol_runtime.runtime_sms,
            "record_terminal_phone_outcome",
            return_value={"ok": True},
        ) as record_terminal_phone_outcome, mock.patch.object(
            easyprotocol_runtime.runtime_sms,
            "report_phone_outcome_for_session",
            return_value={"ok": True},
        ) as report_phone_outcome:
            result = easyprotocol_runtime.dispatch_easyprotocol_step(
                step_type="obtain_codex_oauth",
                step_input={"source_path": "C:/tmp/small.json", "output_dir": "C:/tmp/out"},
            )

        self.assertTrue(result["ok"])
        self.assertEqual("phone_verification_submitted_small_success", result["status"])
        self.assertEqual("sms_2", result["phoneSessionId"])
        self.assertEqual("+353894602760", result["phoneNumber"])
        self.assertEqual("wait_sms_code", result["phoneVerificationFailureStage"])
        self.assertEqual(2, open_phone_session_for_business.call_count)
        self.assertEqual(
            ["sms_1", "sms_2"],
            [
                item["phone_session_id"]
                for item in captured_inputs
                if item["step_type"] == "submit_phone_verification_number"
            ],
        )
        wait_phone_code.assert_called_once_with(session_id="sms_2", timeout_seconds=180)
        record_terminal_phone_outcome.assert_called_once_with(
            phone_number="+41779793490",
            provider_key="onlinesim",
            terminal_code="rate_limit_exceeded",
            terminal_message="Too many phone verification requests.",
        )
        self.assertEqual(2, report_phone_outcome.call_count)

    def test_dispatch_obtain_codex_oauth_retries_wrong_sms_code_with_next_number(self) -> None:
        captured_inputs: list[dict[str, object]] = []
        phone_sessions = [
            {"sessionId": "sms_1", "phoneNumber": "+15550000001", "providerKey": "freepool"},
            {"sessionId": "sms_2", "phoneNumber": "+15550000002", "providerKey": "freepool"},
        ]

        def _invoke(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
            captured_inputs.append({"step_type": step_type, **dict(step_input)})
            if step_type == "obtain_codex_oauth":
                return {
                    "ok": True,
                    "status": "phone_verification_required",
                    "phoneVerificationRequired": True,
                    "pageType": "add_phone",
                    "resumeContext": {"flow": "oauth", "token": "resume_123"},
                    "successPath": "C:/tmp/openai-oauth.json",
                }
            if step_type == "submit_phone_verification_number":
                return {
                    "ok": True,
                    "status": "phone_number_submitted",
                    "pageType": "sms_verification",
                    "resumeContext": {"flow": "oauth", "token": f"resume_{step_input['phone_session_id']}"},
                }
            if step_type == "submit_phone_verification_code" and step_input.get("phone_session_id") == "sms_1":
                raise RuntimeError("otp_incorrect body={\"error\":{\"code\":\"wrong_email_otp_code\"}}")
            if step_type == "submit_phone_verification_code" and step_input.get("phone_session_id") == "sms_2":
                return {
                    "ok": True,
                    "status": "completed",
                    "successPath": "C:/tmp/codex-free.json",
                    "userId": "user_123",
                }
            raise AssertionError(f"unexpected invoke: {step_type} {step_input!r}")

        with mock.patch.object(
            easyprotocol_runtime,
            "invoke_easyprotocol",
            side_effect=_invoke,
        ), mock.patch.object(
            easyprotocol_runtime.runtime_sms,
            "open_phone_session_for_business",
            side_effect=phone_sessions,
        ) as open_phone_session_for_business, mock.patch.object(
            easyprotocol_runtime.runtime_sms,
            "wait_phone_code_for_session",
            side_effect=["111111", "222222"],
        ), mock.patch.object(
            easyprotocol_runtime.runtime_sms,
            "record_terminal_phone_outcome",
            return_value={"ok": True},
        ) as record_terminal_phone_outcome, mock.patch.object(
            easyprotocol_runtime.runtime_sms,
            "report_phone_outcome_for_session",
            return_value={"ok": True},
        ) as report_phone_outcome:
            result = easyprotocol_runtime.dispatch_easyprotocol_step(
                step_type="obtain_codex_oauth",
                step_input={"source_path": "C:/tmp/small.json", "output_dir": "C:/tmp/out"},
            )

        self.assertTrue(result["ok"])
        self.assertEqual("completed", result["status"])
        self.assertEqual("sms_2", result["phoneSessionId"])
        self.assertEqual(2, open_phone_session_for_business.call_count)
        self.assertEqual(
            ["sms_1", "sms_2"],
            [
                item["phone_session_id"]
                for item in captured_inputs
                if item["step_type"] == "submit_phone_verification_code"
            ],
        )
        record_terminal_phone_outcome.assert_called_once_with(
            phone_number="+15550000001",
            provider_key="freepool",
            terminal_code="wrong_otp_code",
            terminal_message=mock.ANY,
        )
        self.assertEqual(
            [mock.call(session_id="sms_1", outcome="failure", detail=mock.ANY), mock.call(session_id="sms_2", outcome="success", detail="codex_oauth_completed")],
            report_phone_outcome.call_args_list,
        )

    def test_dispatch_obtain_codex_oauth_retries_generic_phone_code_submit_failed_with_next_number(self) -> None:
        captured_inputs: list[dict[str, object]] = []
        phone_sessions = [
            {"sessionId": "sms_1", "phoneNumber": "+15550000011", "providerKey": "freepool"},
            {"sessionId": "sms_2", "phoneNumber": "+15550000012", "providerKey": "freepool"},
        ]

        def _invoke(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
            captured_inputs.append({"step_type": step_type, **dict(step_input)})
            if step_type == "obtain_codex_oauth":
                return {
                    "ok": True,
                    "status": "phone_verification_required",
                    "phoneVerificationRequired": True,
                    "pageType": "add_phone",
                    "resumeContext": {"flow": "oauth", "token": "resume_123"},
                    "successPath": "C:/tmp/openai-oauth.json",
                }
            if step_type == "submit_phone_verification_number":
                return {
                    "ok": True,
                    "status": "phone_number_submitted",
                    "pageType": "sms_verification",
                    "resumeContext": {"flow": "oauth", "token": f"resume_{step_input['phone_session_id']}"},
                }
            if step_type == "submit_phone_verification_code" and step_input.get("phone_session_id") == "sms_1":
                raise RuntimeError("phone_code_submit_failed")
            if step_type == "submit_phone_verification_code" and step_input.get("phone_session_id") == "sms_2":
                return {
                    "ok": True,
                    "status": "completed",
                    "successPath": "C:/tmp/codex-free.json",
                    "userId": "user_123",
                }
            raise AssertionError(f"unexpected invoke: {step_type} {step_input!r}")

        with mock.patch.object(
            easyprotocol_runtime,
            "invoke_easyprotocol",
            side_effect=_invoke,
        ), mock.patch.object(
            easyprotocol_runtime.runtime_sms,
            "open_phone_session_for_business",
            side_effect=phone_sessions,
        ) as open_phone_session_for_business, mock.patch.object(
            easyprotocol_runtime.runtime_sms,
            "wait_phone_code_for_session",
            side_effect=["111111", "222222"],
        ), mock.patch.object(
            easyprotocol_runtime.runtime_sms,
            "record_terminal_phone_outcome",
            return_value={"ok": True},
        ) as record_terminal_phone_outcome, mock.patch.object(
            easyprotocol_runtime.runtime_sms,
            "report_phone_outcome_for_session",
            return_value={"ok": True},
        ) as report_phone_outcome:
            result = easyprotocol_runtime.dispatch_easyprotocol_step(
                step_type="obtain_codex_oauth",
                step_input={"source_path": "C:/tmp/small.json", "output_dir": "C:/tmp/out"},
            )

        self.assertTrue(result["ok"])
        self.assertEqual("completed", result["status"])
        self.assertEqual("sms_2", result["phoneSessionId"])
        self.assertEqual(2, open_phone_session_for_business.call_count)
        self.assertEqual(
            ["sms_1", "sms_2"],
            [
                item["phone_session_id"]
                for item in captured_inputs
                if item["step_type"] == "submit_phone_verification_code"
            ],
        )
        record_terminal_phone_outcome.assert_called_once_with(
            phone_number="+15550000011",
            provider_key="freepool",
            terminal_code="wrong_otp_code",
            terminal_message=mock.ANY,
        )
        self.assertEqual(
            [mock.call(session_id="sms_1", outcome="failure", detail=mock.ANY), mock.call(session_id="sms_2", outcome="success", detail="codex_oauth_completed")],
            report_phone_outcome.call_args_list,
        )

    def test_open_phone_session_for_business_skips_blacklisted_phone_by_rotating_provider(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "register-sms-state.json"
            state_path.write_text(
                '{"phones":{"+15551234567":{"blockedUntilTs":9999999999,"reason":"phone_number_in_use"}}}',
                encoding="utf-8",
            )
            captured_blacklists: list[tuple[str, ...]] = []

            def _open_sms_session(**kwargs):
                captured_blacklists.append(tuple(kwargs["provider_blacklist"]))
                if len(captured_blacklists) == 1:
                    return easy_sms_client.SmsSession(
                        session_id="sms_1",
                        phone_number="+15551234567",
                        provider_key="onlinesim",
                    )
                return easy_sms_client.SmsSession(
                    session_id="sms_2",
                    phone_number="+15557654321",
                    provider_key="smstome",
                )

            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_SMS_STATE_PATH": str(state_path),
                    "REGISTER_SMS_BUSINESS_POLICIES_JSON": (
                        '{"openai":{"enabled":true,"providerBlacklist":["hero_sms"],'
                        '"allowPaid":false,"allowReuse":false,"maxBindingsPerPhone":1,'
                        '"countryCodes":[],"selectionMode":"balanced"}}'
                    ),
                },
                clear=False,
            ), mock.patch.object(
                runtime_sms,
                "open_sms_session",
                side_effect=_open_sms_session,
            ), mock.patch.object(
                runtime_sms,
                "report_sms_outcome",
                return_value={"ok": True},
            ) as report_sms_outcome:
                session = runtime_sms.open_phone_session_for_business(business_key="openai")

        self.assertEqual("sms_2", session["sessionId"])
        self.assertEqual("+15557654321", session["phoneNumber"])
        self.assertEqual("smstome", session["providerKey"])
        self.assertEqual(("hero_sms",), captured_blacklists[0])
        self.assertEqual(("hero_sms", "onlinesim"), captured_blacklists[1])
        report_sms_outcome.assert_called_once_with(
            session_id="sms_1",
            outcome="failure",
            detail="blacklisted_phone_number",
        )

    def test_open_phone_session_for_business_passes_provider_country_blacklist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "register-sms-state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "phones": {
                            "+31616835325": {
                                "blockedUntilTs": 9999999999,
                                "providerKey": "onlinesim",
                                "reason": "rate_limit_exceeded",
                            },
                            "+447568371100": {
                                "blockedUntilTs": 9999999999,
                                "providerKey": "smstome",
                                "reason": "phone_number_in_use",
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )
            captured_provider_country_blacklists: list[tuple[str, ...]] = []

            def _open_sms_session(**kwargs):
                captured_provider_country_blacklists.append(tuple(kwargs["provider_country_blacklist"]))
                return easy_sms_client.SmsSession(
                    session_id="sms_2",
                    phone_number="+33774749623",
                    provider_key="onlinesim",
                )

            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_SMS_STATE_PATH": str(state_path),
                    "REGISTER_SMS_BUSINESS_POLICIES_JSON": (
                        '{"openai":{"enabled":true,"providerBlacklist":["hero_sms"],'
                        '"allowPaid":false,"allowReuse":false,"maxBindingsPerPhone":1,'
                        '"countryCodes":["+31","+33","+44"],"selectionMode":"balanced"}}'
                    ),
                },
                clear=False,
            ), mock.patch.object(
                runtime_sms,
                "open_sms_session",
                side_effect=_open_sms_session,
            ):
                session = runtime_sms.open_phone_session_for_business(business_key="openai")

        self.assertEqual("sms_2", session["sessionId"])
        self.assertEqual(
            [("onlinesim|+31", "smstome|+44")],
            captured_provider_country_blacklists,
        )

    def test_open_phone_session_for_business_blocks_provider_from_repeated_phone_scoped_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "register-sms-state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "phones": {
                            "+15550000001": {
                                "blockedUntilTs": 9999999999,
                                "blockedAt": "2026-01-01T00:00:00Z",
                                "providerKey": "generic_provider",
                                "reason": "phone_number_in_use",
                            },
                            "+15550000002": {
                                "blockedUntilTs": 9999999999,
                                "blockedAt": "2026-01-01T00:00:10Z",
                                "providerKey": "generic_provider",
                                "reason": "rate_limit_exceeded",
                            },
                            "+15550000003": {
                                "blockedUntilTs": 9999999999,
                                "blockedAt": "2026-01-01T00:00:20Z",
                                "providerKey": "generic_provider",
                                "reason": "phone_max_usage_exceeded",
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )
            captured_blacklists: list[tuple[str, ...]] = []

            def _open_sms_session(**kwargs):
                captured_blacklists.append(tuple(kwargs["provider_blacklist"]))
                return easy_sms_client.SmsSession(
                    session_id="sms_2",
                    phone_number="+15557654321",
                    provider_key="other_provider",
                )

            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_SMS_STATE_PATH": str(state_path),
                    "REGISTER_SMS_PHONE_SCOPED_PROVIDER_FAILURE_THRESHOLD": "3",
                    "REGISTER_SMS_PHONE_SCOPED_PROVIDER_FAILURE_WINDOW_SECONDS": "9999999999",
                    "REGISTER_SMS_BUSINESS_POLICIES_JSON": (
                        '{"openai":{"enabled":true,"providerBlacklist":["static_blocked"],'
                        '"allowPaid":false,"allowReuse":false,"maxBindingsPerPhone":1,'
                        '"countryCodes":[],"selectionMode":"balanced"}}'
                    ),
                },
                clear=False,
            ), mock.patch.object(
                runtime_sms,
                "open_sms_session",
                side_effect=_open_sms_session,
            ):
                session = runtime_sms.open_phone_session_for_business(business_key="openai")

        self.assertEqual("sms_2", session["sessionId"])
        self.assertEqual(("generic_provider", "static_blocked"), captured_blacklists[0])

    def test_open_phone_session_for_business_retries_transient_open_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "register-sms-state.json"
            captured_blacklists: list[tuple[str, ...]] = []

            def _open_sms_session(**kwargs):
                captured_blacklists.append(tuple(kwargs["provider_blacklist"]))
                if len(captured_blacklists) == 1:
                    raise TimeoutError("timed out")
                return easy_sms_client.SmsSession(
                    session_id="sms_2",
                    phone_number="+15557654321",
                    provider_key="smstome",
                )

            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_SMS_STATE_PATH": str(state_path),
                    "REGISTER_SMS_SESSION_LOCAL_RETRY_ATTEMPTS": "3",
                    "REGISTER_SMS_BUSINESS_POLICIES_JSON": (
                        '{"openai":{"enabled":true,"providerBlacklist":["hero_sms"],'
                        '"allowPaid":false,"allowReuse":false,"maxBindingsPerPhone":1,'
                        '"countryCodes":[],"selectionMode":"balanced"}}'
                    ),
                },
                clear=False,
            ), mock.patch.object(
                runtime_sms,
                "open_sms_session",
                side_effect=_open_sms_session,
            ):
                session = runtime_sms.open_phone_session_for_business(business_key="openai")

        self.assertEqual("sms_2", session["sessionId"])
        self.assertEqual("+15557654321", session["phoneNumber"])
        self.assertEqual("smstome", session["providerKey"])
        self.assertEqual([("hero_sms",), ("hero_sms",)], captured_blacklists)

    def test_record_terminal_phone_outcome_keeps_used_number_block_phone_scoped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "register-sms-state.json"

            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_SMS_STATE_PATH": str(state_path),
                    "REGISTER_SMS_TERMINAL_PHONE_BLACKLIST_SECONDS": "3600",
                    "REGISTER_SMS_TERMINAL_PROVIDER_BLACKLIST_SECONDS": "3600",
                },
                clear=False,
            ):
                runtime_sms.record_terminal_phone_outcome(
                    phone_number="+46720085698",
                    provider_key="onlinesim",
                    terminal_code="phone_number_in_use",
                    terminal_message="Phone number already in use.",
                )

            payload = json.loads(state_path.read_text(encoding="utf-8"))

        self.assertIn("+46720085698", payload["phones"])
        self.assertEqual("phone_number_in_use", payload["phones"]["+46720085698"]["reason"])
        self.assertNotIn("onlinesim", payload["providers"])

    def test_record_terminal_phone_outcome_keeps_rate_limit_block_phone_scoped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "register-sms-state.json"

            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_SMS_STATE_PATH": str(state_path),
                    "REGISTER_SMS_TERMINAL_PHONE_BLACKLIST_SECONDS": "3600",
                    "REGISTER_SMS_TERMINAL_PROVIDER_BLACKLIST_SECONDS": "3600",
                },
                clear=False,
            ):
                runtime_sms.record_terminal_phone_outcome(
                    phone_number="+36707448042",
                    provider_key="onlinesim",
                    terminal_code="rate_limit_exceeded",
                    terminal_message="Too many phone verification requests.",
                )

            payload = json.loads(state_path.read_text(encoding="utf-8"))

        self.assertIn("+36707448042", payload["phones"])
        self.assertEqual("rate_limit_exceeded", payload["phones"]["+36707448042"]["reason"])
        self.assertNotIn("onlinesim", payload["providers"])

    def test_record_terminal_phone_outcome_keeps_invalid_and_wrong_code_phone_scoped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "register-sms-state.json"

            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_SMS_STATE_PATH": str(state_path),
                    "REGISTER_SMS_TERMINAL_PHONE_BLACKLIST_SECONDS": "3600",
                    "REGISTER_SMS_TERMINAL_PROVIDER_BLACKLIST_SECONDS": "3600",
                },
                clear=False,
            ):
                runtime_sms.record_terminal_phone_outcome(
                    phone_number="+15550000003",
                    provider_key="freepool",
                    terminal_code="invalid_phone_number",
                    terminal_message="Invalid phone number.",
                )
                runtime_sms.record_terminal_phone_outcome(
                    phone_number="+15550000004",
                    provider_key="freepool",
                    terminal_code="wrong_otp_code",
                    terminal_message="Wrong code.",
                )

            payload = json.loads(state_path.read_text(encoding="utf-8"))

        self.assertEqual("invalid_phone_number", payload["phones"]["+15550000003"]["reason"])
        self.assertEqual("wrong_otp_code", payload["phones"]["+15550000004"]["reason"])
        self.assertNotIn("freepool", payload["providers"])

    def test_record_terminal_phone_outcome_emits_sanitized_summary_event(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "register-sms-state.json"

            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_SMS_STATE_PATH": str(state_path),
                    "REGISTER_SMS_TERMINAL_PHONE_BLACKLIST_SECONDS": "3600",
                    "REGISTER_SMS_TERMINAL_PROVIDER_BLACKLIST_SECONDS": "3600",
                },
                clear=False,
            ), mock.patch.object(runtime_sms, "json_log") as json_log:
                runtime_sms.record_terminal_phone_outcome(
                    phone_number="+15550000005",
                    provider_key="freepool",
                    terminal_code="wrong_otp_code",
                    terminal_message="Wrong code.",
                )

        json_log.assert_called_once()
        event = json_log.call_args.args[0]
        self.assertEqual("register_sms_terminal_phone_outcome_recorded", event["event"])
        self.assertEqual("freepool", event["providerKey"])
        self.assertEqual("wrong_otp_code", event["terminalCode"])
        self.assertTrue(event["phoneRecorded"])
        self.assertNotIn("phoneNumber", event)

    def test_record_terminal_phone_outcome_escalates_repeated_phone_scoped_failures_to_provider_block(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "register-sms-state.json"

            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_SMS_STATE_PATH": str(state_path),
                    "REGISTER_SMS_TERMINAL_PHONE_BLACKLIST_SECONDS": "3600",
                    "REGISTER_SMS_TERMINAL_PROVIDER_BLACKLIST_SECONDS": "1800",
                    "REGISTER_SMS_PHONE_SCOPED_PROVIDER_FAILURE_THRESHOLD": "3",
                    "REGISTER_SMS_PHONE_SCOPED_PROVIDER_FAILURE_WINDOW_SECONDS": "3600",
                },
                clear=False,
            ):
                for index, terminal_code in enumerate(
                    ("phone_number_in_use", "rate_limit_exceeded", "phone_max_usage_exceeded"),
                    start=1,
                ):
                    runtime_sms.record_terminal_phone_outcome(
                        phone_number=f"+1555000000{index}",
                        provider_key="generic_provider",
                        terminal_code=terminal_code,
                        terminal_message="Phone verification terminal.",
                    )

            payload = json.loads(state_path.read_text(encoding="utf-8"))

        self.assertIn("generic_provider", payload["providers"])
        self.assertEqual("repeated_phone_scoped_terminal", payload["providers"]["generic_provider"]["reason"])
        self.assertEqual(3, payload["providers"]["generic_provider"]["terminalFailureCount"])

    def test_open_phone_session_for_business_ignores_legacy_phone_scoped_provider_block(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "register-sms-state.json"
            state_path.write_text(
                '{"phones":{},"providers":{"onlinesim":{'
                '"blockedUntilTs":9999999999,'
                '"reason":"phone_number_in_use",'
                '"phoneNumber":"+46720085698"'
                "}}}",
                encoding="utf-8",
            )
            captured_blacklists: list[tuple[str, ...]] = []

            def _open_sms_session(**kwargs):
                captured_blacklists.append(tuple(kwargs["provider_blacklist"]))
                return easy_sms_client.SmsSession(
                    session_id="sms_1",
                    phone_number="+33774749623",
                    provider_key="onlinesim",
                )

            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_SMS_STATE_PATH": str(state_path),
                    "REGISTER_SMS_BUSINESS_POLICIES_JSON": (
                        '{"openai":{"enabled":true,"providerBlacklist":["hero_sms"],'
                        '"allowPaid":false,"allowReuse":false,"maxBindingsPerPhone":1,'
                        '"countryCodes":[],"selectionMode":"balanced"}}'
                    ),
                },
                clear=False,
            ), mock.patch.object(
                runtime_sms,
                "open_sms_session",
                side_effect=_open_sms_session,
            ):
                session = runtime_sms.open_phone_session_for_business(business_key="openai")

        self.assertEqual("sms_1", session["sessionId"])
        self.assertEqual(("hero_sms",), captured_blacklists[0])

    def test_open_phone_session_for_business_ignores_legacy_rate_limit_provider_block(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "register-sms-state.json"
            state_path.write_text(
                '{"phones":{},"providers":{"onlinesim":{'
                '"blockedUntilTs":9999999999,'
                '"reason":"rate_limit_exceeded",'
                '"phoneNumber":"+36707448042"'
                "}}}",
                encoding="utf-8",
            )
            captured_blacklists: list[tuple[str, ...]] = []

            def _open_sms_session(**kwargs):
                captured_blacklists.append(tuple(kwargs["provider_blacklist"]))
                return easy_sms_client.SmsSession(
                    session_id="sms_1",
                    phone_number="+33774749623",
                    provider_key="onlinesim",
                )

            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_SMS_STATE_PATH": str(state_path),
                    "REGISTER_SMS_BUSINESS_POLICIES_JSON": (
                        '{"openai":{"enabled":true,"providerBlacklist":["hero_sms"],'
                        '"allowPaid":false,"allowReuse":false,"maxBindingsPerPhone":1,'
                        '"countryCodes":[],"selectionMode":"balanced"}}'
                    ),
                },
                clear=False,
            ), mock.patch.object(
                runtime_sms,
                "open_sms_session",
                side_effect=_open_sms_session,
            ):
                session = runtime_sms.open_phone_session_for_business(business_key="openai")

        self.assertEqual("sms_1", session["sessionId"])
        self.assertEqual(("hero_sms",), captured_blacklists[0])

    def test_dispatch_obtain_codex_oauth_treats_submitted_phone_then_code_wait_failure_as_intermediate_result(self) -> None:
        captured_inputs: list[dict[str, object]] = []

        def _invoke(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
            captured_inputs.append(dict(step_input))
            if len(captured_inputs) == 1:
                return {
                    "ok": True,
                    "status": "phone_verification_required",
                    "phoneVerificationRequired": True,
                    "pageType": "add_phone",
                    "resumeContext": {"flow": "oauth", "token": "resume_123"},
                    "successPath": "C:/tmp/openai-oauth.json",
                }
            return {
                "ok": True,
                "status": "phone_number_submitted",
                "pageType": "add_phone",
                "resumeContext": {"flow": "oauth", "token": "resume_123_step2"},
            }

        with mock.patch.object(
            easyprotocol_runtime,
            "invoke_easyprotocol",
            side_effect=_invoke,
        ), mock.patch.object(
            easyprotocol_runtime.runtime_sms,
            "open_phone_session_for_business",
            return_value={"sessionId": "sms_123", "phoneNumber": "+15551234567", "providerKey": "sms24"},
        ), mock.patch.object(
            easyprotocol_runtime.runtime_sms,
            "wait_phone_code_for_session",
            side_effect=RuntimeError("wait_code_timeout"),
        ) as wait_phone_code, mock.patch.object(
            easyprotocol_runtime.runtime_sms,
            "report_phone_outcome_for_session",
            return_value={"ok": True},
        ) as report_phone_outcome:
            result = easyprotocol_runtime.dispatch_easyprotocol_step(
                step_type="obtain_codex_oauth",
                step_input={"source_path": "C:/tmp/small.json", "output_dir": "C:/tmp/out"},
            )

        self.assertTrue(result["ok"])
        self.assertEqual("phone_verification_submitted_small_success", result["status"])
        self.assertTrue(result["phoneVerificationAttempted"])
        self.assertTrue(result["phoneVerificationSubmitted"])
        self.assertEqual("wait_sms_code", result["phoneVerificationFailureStage"])
        self.assertEqual("sms24", result["phoneProvider"])
        wait_phone_code.assert_called_once()
        report_phone_outcome.assert_called_once()

    def test_dispatch_obtain_codex_oauth_rejects_missing_login_session_handoff_before_protocol_call(self) -> None:
        with mock.patch.object(easyprotocol_runtime, "invoke_easyprotocol") as invoke_easyprotocol:
            with self.assertRaisesRegex(RuntimeError, "authorize_missing_login_session"):
                easyprotocol_runtime.dispatch_easyprotocol_step(
                    step_type="obtain_codex_oauth",
                    step_input={
                        "source_path": "C:/tmp/small.json",
                        "output_dir": "C:/tmp/out",
                        "login_session": "",
                    },
                )

        invoke_easyprotocol.assert_not_called()


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

    def test_release_mailbox_treats_missing_session_as_not_found(self) -> None:
        with mock.patch.object(
            easyemail_runtime,
            "release_mailbox",
            side_effect=RuntimeError(
                'mail service POST /mail/mailboxes/release failed: HTTP 500 '
                '[code=MAILBOX_SESSION_NOT_FOUND]: {"code":"MAILBOX_SESSION_NOT_FOUND",'
                '"error":"Unknown mailbox session: mailbox_123.","message":"Unknown mailbox session: mailbox_123."}'
            ),
        ):
            result = easyemail_runtime.dispatch_easyemail_step(
                step_type="release_mailbox",
                step_input={
                    "provider": "im215",
                    "mailbox_ref": "im215:test",
                    "mailbox_session_id": "mailbox_123",
                },
            )

        self.assertFalse(result["released"])
        self.assertEqual("not_found", result["detail"])
        self.assertEqual("im215", result["provider"])

    def test_release_mailbox_treats_moemail_unauthorized_delete_as_not_found(self) -> None:
        with mock.patch.object(
            easyemail_runtime,
            "release_mailbox",
            side_effect=RuntimeError(
                'mail service POST /mail/mailboxes/release failed: HTTP 500 '
                '[code=MoEmail deleteMailboxWeb failed with status 401. 未授权]: '
                '{"error":"MoEmail deleteMailboxWeb failed with status 401. 未授权"}'
            ),
        ):
            result = easyemail_runtime.dispatch_easyemail_step(
                step_type="release_mailbox",
                step_input={
                    "provider": "moemail",
                    "mailbox_ref": "moemail:test",
                    "mailbox_session_id": "mailbox_123",
                },
            )

        self.assertFalse(result["released"])
        self.assertEqual("not_found", result["detail"])
        self.assertEqual("moemail", result["provider"])

    def test_release_mailbox_recovers_by_email_when_session_id_is_missing(self) -> None:
        with mock.patch.object(easyemail_runtime, "ensure_easyemail_runtime_defaults"), mock.patch.object(
            easyemail_runtime,
            "release_mailbox",
            return_value={"released": False, "detail": "missing_session_id"},
        ) as release_mailbox, mock.patch.object(
            easyemail_runtime,
            "release_mailbox_sessions_by_email",
            return_value=[
                {
                    "sessionId": "recovered-session",
                    "email": "user@example.com",
                    "release": {"released": True, "detail": "deleted"},
                }
            ],
        ) as release_sessions:
            result = easyemail_runtime.dispatch_easyemail_step(
                step_type="release_mailbox",
                step_input={
                    "provider": "cloudflare_temp_email",
                    "email_address": "user@example.com",
                    "mailbox_session_id": "",
                },
            )

        release_mailbox.assert_called_once_with(
            mailbox_ref=None,
            session_id=None,
            reason="dst_flow_cleanup",
        )
        release_sessions.assert_called_once_with(
            email_address="user@example.com",
            provider_type_key="cloudflare_temp_email",
            reason="dst_flow_cleanup_missing_session",
            limit=20,
        )
        self.assertTrue(result["released"])
        self.assertEqual("recovered_by_email", result["detail"])
        self.assertEqual(1, result["released_count"])
        self.assertEqual("cloudflare_temp_email", result["provider"])

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
    def test_default_mailbox_business_retry_attempts_covers_noisy_provider_pool(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertGreaterEqual(runtime_mailbox._resolve_mailbox_business_retry_attempts(), 12)

    def test_dynamic_blacklist_exhausted_fallback_is_enabled_by_default(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertTrue(runtime_mailbox._dynamic_blacklist_exhausted_fallback_enabled())

    def test_mailbox_request_payload_does_not_default_to_high_availability_profile(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "REGISTER_MAILBOX_ROUTING_PROFILE_ID": "",
                "MAILBOX_PROVIDER_ROUTING_PROFILE_ID": "",
                "REGISTER_MAILBOX_STRATEGY_MODE_ID": "",
                "MAILBOX_PROVIDER_STRATEGY_MODE_ID": "",
                "MAILBOX_STRATEGY_MODE_JSON": "",
                "REGISTER_INBOX_STRATEGY_MODE_JSON": "",
            },
            clear=True,
        ):
            with mock.patch.object(easy_email_client, "_wait_mail_service_ready"):
                payload, provider_key, requested_email = easy_email_client._build_mailbox_request_payload(
                    provider="auto",
                    default_host_id="python-register-orchestration",
                    ttl_seconds=90,
                )

        self.assertEqual("", provider_key)
        self.assertEqual("", requested_email)
        self.assertNotIn("providerRoutingProfileId", payload)
        self.assertNotIn("providerStrategyModeId", payload)
        self.assertNotIn("providerGroupSelections", payload)

    def test_release_mailbox_client_treats_missing_session_as_not_found(self) -> None:
        with mock.patch.object(
            easy_email_client,
            "_post_json",
            side_effect=RuntimeError(
                'mail service POST /mail/mailboxes/release failed: HTTP 500 '
                '[code=MAILBOX_SESSION_NOT_FOUND]: {"code":"MAILBOX_SESSION_NOT_FOUND",'
                '"error":"Unknown mailbox session: mailbox_456.","message":"Unknown mailbox session: mailbox_456."}'
            ),
        ):
            result = easy_email_client.release_mailbox(session_id="mailbox_456", reason="dst_flow_cleanup")

        self.assertEqual({"released": False, "detail": "not_found"}, result)

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

    def test_mailbox_domain_policy_violation_applies_business_blacklist_to_m2u(self) -> None:
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

    def test_mailbox_domain_policy_violation_rejects_moemail_domain_outside_business_pool(self) -> None:
        mailbox = runtime_mailbox.Mailbox(
            provider="moemail",
            email="user@outside.test",
            ref="moemail:test",
            session_id="moemail-session",
        )
        with mock.patch.dict(
            os.environ,
            {
                "REGISTER_MAILBOX_BUSINESS_KEY": "generic",
                "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": (
                    '{"openai":{"domainPool":["zhooo.org","cnmlgb.de"],'
                    '"explicitBlacklistDomains":["coolkid.icu"]}}'
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
        self.assertEqual("outside_business_domain_pool", violation["reason"])
        self.assertEqual("moemail", violation["provider"])
        self.assertEqual("outside.test", violation["domain"])

    def test_mailbox_domain_policy_violation_allows_non_moemail_outside_business_pool(self) -> None:
        mailbox = runtime_mailbox.Mailbox(
            provider="mail2925",
            email="user@outside.test",
            ref="mail2925:test",
            session_id="mail2925-session",
        )
        with mock.patch.dict(
            os.environ,
            {
                "REGISTER_MAILBOX_BUSINESS_KEY": "generic",
                "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": (
                    '{"openai":{"domainPool":["zhooo.org","cnmlgb.de"],'
                    '"explicitBlacklistDomains":["coolkid.icu"]}}'
                ),
            },
            clear=True,
        ):
            violation = runtime_mailbox._mailbox_domain_policy_violation(
                mailbox,
                business_key="openai",
            )

        self.assertIsNone(violation)

    def test_mailbox_domain_policy_violation_applies_business_provider_blacklist(self) -> None:
        mailbox = runtime_mailbox.Mailbox(
            provider="m2u",
            email="allowed@cnmlgb.de",
            ref="m2u:test",
            session_id="m2u-session",
        )
        with mock.patch.dict(
            os.environ,
            {
                "REGISTER_MAILBOX_BUSINESS_KEY": "generic",
                "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": (
                    '{"openai":{"explicitBlacklistDomains":["coolkid.icu"],'
                    '"providerBlacklist":["m2u"]}}'
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
        self.assertEqual("explicit_business_provider_blacklist", violation["reason"])
        self.assertEqual("m2u", violation["provider"])
        self.assertEqual("cnmlgb.de", violation["domain"])

    def test_mailbox_domain_policy_violation_applies_dynamic_business_provider_blacklist(self) -> None:
        mailbox = runtime_mailbox.Mailbox(
            provider="m2u",
            email="allowed@cnmlgb.de",
            ref="m2u:test",
            session_id="m2u-session",
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
                                    "m2u": {
                                        "attempts": 3,
                                        "successes": 0,
                                        "failures": 3,
                                        "failureRate": 100.0,
                                        "blacklisted": True,
                                        "blacklistReason": "provider_failure_rate_threshold",
                                    }
                                }
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "REGISTER_MAILBOX_BUSINESS_KEY": "generic",
                },
                clear=True,
            ):
                violation = runtime_mailbox._mailbox_domain_policy_violation(
                    mailbox,
                    business_key="openai",
                )

        self.assertIsNotNone(violation)
        assert violation is not None
        self.assertEqual("dynamic_business_provider_blacklist", violation["reason"])
        self.assertEqual("m2u", violation["provider"])
        self.assertEqual("cnmlgb.de", violation["domain"])

    def test_mailbox_domain_policy_violation_applies_dynamic_email_otp_provider_threshold(self) -> None:
        mailbox = runtime_mailbox.Mailbox(
            provider="mail2925",
            email="allowed@ok.test",
            ref="mail2925:test",
            session_id="mail2925-session",
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
                                        "attempts": 4,
                                        "successes": 0,
                                        "failures": 4,
                                        "blacklisted": False,
                                        "failureReasons": {
                                            "email_otp_timeout": 2,
                                            "email_otp_wrong_code": 1,
                                        },
                                    }
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
                    "REGISTER_MAILBOX_BUSINESS_KEY": "generic",
                    "REGISTER_MAILBOX_EMAIL_OTP_PROVIDER_FAILURE_BLACKLIST_THRESHOLD": "3",
                },
                clear=True,
            ):
                violation = runtime_mailbox._mailbox_domain_policy_violation(
                    mailbox,
                    business_key="openai",
                )

        self.assertIsNotNone(violation)
        assert violation is not None
        self.assertEqual("dynamic_business_provider_blacklist", violation["reason"])
        self.assertEqual("mail2925", violation["provider"])
        self.assertEqual("ok.test", violation["domain"])

    def test_create_mailbox_with_business_policy_uses_dynamic_blacklist_exhausted_fallback_by_default(self) -> None:
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
                                        "successes": 0,
                                        "failures": 20,
                                        "blacklisted": True,
                                        "blacklistReason": "provider_failure_rate_threshold",
                                    }
                                }
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            mailboxes = [
                runtime_mailbox.Mailbox(
                    provider="mail2925",
                    email="first@2925.com",
                    ref="mail2925:first",
                    session_id="first",
                ),
                runtime_mailbox.Mailbox(
                    provider="mail2925",
                    email="fallback@2925.com",
                    ref="mail2925:fallback",
                    session_id="fallback",
                ),
            ]

            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "REGISTER_MAILBOX_BUSINESS_KEY": "generic",
                    "REGISTER_MAILBOX_BUSINESS_RETRY_ATTEMPTS": "2",
                },
                clear=True,
            ), mock.patch.object(runtime_mailbox, "_release_mailbox_quiet") as release_mock:
                selected = runtime_mailbox._create_mailbox_with_business_policy(
                    create_fn=lambda: mailboxes.pop(0),
                    business_key="openai",
                )

        self.assertEqual("fallback@2925.com", selected.email)
        release_mock.assert_called_once()

    def test_create_mailbox_with_business_policy_can_opt_out_of_dynamic_blacklist_exhausted_fallback(self) -> None:
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
                                        "successes": 0,
                                        "failures": 20,
                                        "blacklisted": True,
                                        "blacklistReason": "provider_failure_rate_threshold",
                                    }
                                }
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            mailboxes = [
                runtime_mailbox.Mailbox(
                    provider="mail2925",
                    email="first@2925.com",
                    ref="mail2925:first",
                    session_id="first",
                ),
                runtime_mailbox.Mailbox(
                    provider="mail2925",
                    email="fallback@2925.com",
                    ref="mail2925:fallback",
                    session_id="fallback",
                ),
            ]

            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "REGISTER_MAILBOX_BUSINESS_KEY": "generic",
                    "REGISTER_MAILBOX_BUSINESS_RETRY_ATTEMPTS": "2",
                    "REGISTER_MAILBOX_DYNAMIC_BLACKLIST_EXHAUSTED_FALLBACK": "false",
                },
                clear=True,
            ), mock.patch.object(runtime_mailbox, "_release_mailbox_quiet") as release_mock:
                with self.assertRaisesRegex(RuntimeError, "mailbox_business_policy_retries_exhausted"):
                    runtime_mailbox._create_mailbox_with_business_policy(
                        create_fn=lambda: mailboxes.pop(0),
                        business_key="openai",
                    )

        self.assertEqual(2, release_mock.call_count)

    def test_mailbox_domain_policy_violation_applies_dynamic_provider_blacklist_with_domain_pool(self) -> None:
        mailbox = runtime_mailbox.Mailbox(
            provider="mail2925",
            email="allowed@outside.test",
            ref="mail2925:test",
            session_id="mail2925-session",
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
                                        "attempts": 4,
                                        "successes": 0,
                                        "failures": 4,
                                        "blacklisted": False,
                                        "failureReasons": {
                                            "email_otp_timeout": 3,
                                        },
                                    }
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
                    "REGISTER_MAILBOX_BUSINESS_KEY": "generic",
                    "REGISTER_MAILBOX_EMAIL_OTP_PROVIDER_FAILURE_BLACKLIST_THRESHOLD": "3",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": (
                        '{"openai":{"domainPool":["zhooo.org"],'
                        '"explicitBlacklistDomains":["coolkid.icu"]}}'
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
        self.assertEqual("dynamic_business_provider_blacklist", violation["reason"])
        self.assertEqual("mail2925", violation["provider"])

    def test_mailbox_domain_policy_violation_keeps_business_pool_domain_despite_dynamic_provider_threshold(self) -> None:
        mailbox = runtime_mailbox.Mailbox(
            provider="moemail",
            email="allowed@zhooo.org",
            ref="moemail:test",
            session_id="moemail-session",
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
                                    "moemail": {
                                        "attempts": 4,
                                        "successes": 0,
                                        "failures": 4,
                                        "blacklisted": False,
                                        "failureReasons": {
                                            "email_otp_timeout": 2,
                                            "email_otp_wrong_code": 1,
                                        },
                                    }
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
                    "REGISTER_MAILBOX_BUSINESS_KEY": "generic",
                    "REGISTER_MAILBOX_EMAIL_OTP_PROVIDER_FAILURE_BLACKLIST_THRESHOLD": "3",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": (
                        '{"openai":{"domainPool":["zhooo.org"],'
                        '"explicitBlacklistDomains":["coolkid.icu"]}}'
                    ),
                },
                clear=True,
            ):
                violation = runtime_mailbox._mailbox_domain_policy_violation(
                    mailbox,
                    business_key="openai",
                )

        self.assertIsNone(violation)

    def test_mailbox_domain_policy_violation_ignores_legacy_dynamic_state(self) -> None:
        mailbox = runtime_mailbox.Mailbox(
            provider="m2u",
            email="allowed@cnmlgb.de",
            ref="m2u:test",
            session_id="m2u-session",
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            state_path = output_root / "others" / "register-mailbox-domain-state.json"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_text(
                json.dumps(
                    {
                        "schemaVersion": 2,
                        "businesses": {
                            "openai": {
                                "providers": {
                                    "m2u": {
                                        "attempts": 99,
                                        "successes": 0,
                                        "failures": 99,
                                        "failureRate": 100.0,
                                        "blacklisted": True,
                                        "blacklistReason": "provider_failure_rate_threshold",
                                    }
                                }
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "REGISTER_MAILBOX_BUSINESS_KEY": "generic",
                },
                clear=True,
            ):
                violation = runtime_mailbox._mailbox_domain_policy_violation(
                    mailbox,
                    business_key="openai",
                )

        self.assertIsNone(violation)

    def test_resolve_mailbox_retries_m2u_provider_blacklist(self) -> None:
        first_mailbox = runtime_mailbox.Mailbox(
            provider="m2u",
            email="allowed@cnmlgb.de",
            ref="m2u:first",
            session_id="first",
        )
        second_mailbox = runtime_mailbox.Mailbox(
            provider="moemail",
            email="good@cnmlgb.de",
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
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": (
                        '{"openai":{"explicitBlacklistDomains":["coolkid.icu","shaole.me","cpu.edu.kg","tmail.bio","do4.tech"],'
                        '"providerBlacklist":["m2u"]}}'
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

    def test_resolve_mailbox_uses_business_domain_pool_as_moemail_requested_domain(self) -> None:
        mailbox = runtime_mailbox.Mailbox(
            provider="moemail",
            email="good@zhooo.org",
            ref="moemail:session",
            session_id="session",
        )

        def _create_mailbox(**kwargs):
            self.assertEqual("moemail", kwargs.get("provider"))
            self.assertEqual("zhooo.org", kwargs.get("mailcreate_domain"))
            return mailbox

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "REGISTER_MAILBOX_BUSINESS_KEY": "generic",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": (
                        '{"openai":{"domainPool":["zhooo.org"],'
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
                        side_effect=_create_mailbox,
                    ) as create_mailbox:
                        resolved = runtime_mailbox.resolve_mailbox(
                            preallocated_email=None,
                            preallocated_session_id=None,
                            preallocated_mailbox_ref=None,
                            business_key="openai",
                        )

        self.assertEqual("good@zhooo.org", resolved.email)
        self.assertEqual(1, create_mailbox.call_count)

    def test_resolve_mailbox_does_not_force_moemail_when_plan_prefers_other_provider(self) -> None:
        mailbox = runtime_mailbox.Mailbox(
            provider="mail2925",
            email="candidate@outside.test",
            ref="mail2925:session",
            session_id="session",
        )
        create_kwargs: list[dict[str, object]] = []

        def _create_mailbox(**kwargs):
            create_kwargs.append(dict(kwargs))
            return mailbox

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "REGISTER_MAILBOX_BUSINESS_KEY": "generic",
                    "REGISTER_MAILBOX_BUSINESS_RETRY_ATTEMPTS": "1",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": (
                        '{"openai":{"domainPool":["zhooo.org"],'
                        '"explicitBlacklistDomains":["coolkid.icu"]}}'
                    ),
                },
                clear=True,
            ):
                with mock.patch.object(
                    runtime_mailbox,
                    "_resolve_planned_mailbox_provider",
                    return_value="mail2925",
                ):
                    with mock.patch.object(
                        runtime_mailbox,
                        "create_mailbox",
                        side_effect=_create_mailbox,
                    ), mock.patch.object(runtime_mailbox, "release_mailbox"):
                        resolved = runtime_mailbox.resolve_mailbox(
                            preallocated_email=None,
                            preallocated_session_id=None,
                            preallocated_mailbox_ref=None,
                            business_key="openai",
                        )

        self.assertEqual("candidate@outside.test", resolved.email)
        self.assertEqual(1, len(create_kwargs))
        self.assertEqual("auto", create_kwargs[0].get("provider"))
        self.assertNotIn("mailcreate_domain", create_kwargs[0])

    def test_resolve_mailbox_provider_selections_defaults_to_easyemail_unfiltered(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "REGISTER_MAILBOX_PROVIDERS": "",
            },
            clear=True,
        ):
            self.assertEqual((), runtime_mailbox.resolve_mailbox_provider_selections())
