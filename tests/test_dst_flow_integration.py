from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SRC_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "src"
PYTHON_SHARED_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "python_shared" / "src"
for candidate in (SRC_ROOT, PYTHON_SHARED_ROOT):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

import dst_flow  # noqa: E402
from errors import ErrorCodes, ProtocolRuntimeError  # noqa: E402
from others import easyemail_runtime  # noqa: E402
from others.dst_flow_loader import load_dst_flow  # noqa: E402


class DstFlowIntegrationTests(unittest.TestCase):
    def test_main_and_continue_flows_use_capitalized_personal_organization_name(self) -> None:
        flow_root = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "flows"

        for flow_name in (
            "codex-openai-account-v1.semantic-flow.json",
            "codex-openai-oauth-continue-v1.semantic-flow.json",
        ):
            with self.subTest(flow=flow_name):
                plan = load_dst_flow(flow_root / flow_name)
                platform_org_steps = [
                    statement
                    for statement in plan.steps
                    if statement.step_id == "initialize-platform-organization"
                ]

                self.assertEqual(1, len(platform_org_steps))
                self.assertEqual("Personal", platform_org_steps[0].input["organization_name"])
                self.assertEqual("Personal", platform_org_steps[0].input["organization_title"])

    def test_cleanup_release_mailbox_missing_session_does_not_fail_successful_flow(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            flow_path = Path(tmp_dir) / "temp-flow.json"
            flow_path.write_text(
                json.dumps(
                    {
                        "definition": {
                            "platform": "chatgpt",
                            "steps": [
                                {
                                    "id": "main-work",
                                    "type": "noop_success",
                                    "metadata": {"owner": "orchestration"},
                                    "saveAs": "main_work",
                                },
                                {
                                    "id": "release-mailbox",
                                    "type": "release_mailbox",
                                    "metadata": {
                                        "owner": "easyemail",
                                        "stage": "cleanup",
                                        "alwaysRun": True,
                                    },
                                    "input": {"mailbox_session_id": ""},
                                    "saveAs": "release_mailbox",
                                },
                            ],
                        }
                    }
                ),
                encoding="utf-8",
            )

            def _dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                if step_type == "noop_success":
                    return {"ok": True, "status": "ok"}
                if step_type == "release_mailbox":
                    return {"released": False, "detail": "missing_session_id"}
                raise AssertionError(step_type)

            with mock.patch.dict(
                dst_flow.OWNER_DISPATCHERS,
                {"orchestration": _dispatcher, "easyemail": _dispatcher},
                clear=True,
            ):
                result = dst_flow.run_dst_flow_once(
                    output_dir=str(Path(tmp_dir) / "out"),
                    flow_path=flow_path,
                )

        self.assertTrue(result.ok)
        self.assertEqual("ok", result.steps["main-work"])
        self.assertEqual("cleanup_warning", result.steps["release-mailbox"])
        self.assertEqual("missing_session_id", result.step_errors["release-mailbox"]["message"])
        self.assertEqual("", result.error_step)

    def test_cleanup_release_mailbox_accepts_moemail_upstream_delete_unauthorized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            flow_path = Path(tmp_dir) / "temp-flow.json"
            flow_path.write_text(
                json.dumps(
                    {
                        "definition": {
                            "platform": "chatgpt",
                            "steps": [
                                {
                                    "id": "main-work",
                                    "type": "noop_success",
                                    "metadata": {"owner": "orchestration"},
                                    "saveAs": "main_work",
                                },
                                {
                                    "id": "release-mailbox",
                                    "type": "release_mailbox",
                                    "metadata": {
                                        "owner": "easyemail",
                                        "stage": "cleanup",
                                        "alwaysRun": True,
                                    },
                                    "input": {"mailbox_session_id": "mailbox_123"},
                                    "saveAs": "release_mailbox",
                                },
                            ],
                        }
                    }
                ),
                encoding="utf-8",
            )

            def _dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                if step_type == "noop_success":
                    return {"ok": True, "status": "ok"}
                if step_type == "release_mailbox":
                    return {"released": False, "detail": "upstream_delete_unauthorized"}
                raise AssertionError(step_type)

            with mock.patch.dict(
                dst_flow.OWNER_DISPATCHERS,
                {"orchestration": _dispatcher, "easyemail": _dispatcher},
                clear=True,
            ):
                result = dst_flow.run_dst_flow_once(
                    output_dir=str(Path(tmp_dir) / "out"),
                    flow_path=flow_path,
                )

        self.assertTrue(result.ok)
        self.assertEqual("ok", result.steps["main-work"])
        self.assertEqual("ok", result.steps["release-mailbox"])
        self.assertNotIn("release-mailbox", result.step_errors)
        self.assertEqual("upstream_delete_unauthorized", result.outputs["release-mailbox"]["detail"])

    def test_obtain_codex_oauth_false_result_fails_flow(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            flow_path = Path(tmp_dir) / "temp-flow.json"
            flow_path.write_text(
                json.dumps(
                    {
                        "definition": {
                            "platform": "chatgpt",
                            "steps": [
                                {
                                    "id": "obtain-codex-oauth",
                                    "type": "obtain_codex_oauth",
                                    "metadata": {"owner": "easyprotocol"},
                                    "saveAs": "obtain_codex_oauth",
                                }
                            ],
                        }
                    }
                ),
                encoding="utf-8",
            )

            def _dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                return {"ok": False, "status": "authorize_continue_failed", "detail": "authorize_missing_login_session"}

            with mock.patch.dict(dst_flow.OWNER_DISPATCHERS, {"easyprotocol": _dispatcher}, clear=True):
                result = dst_flow.run_dst_flow_once(
                    output_dir=str(Path(tmp_dir) / "out"),
                    flow_path=flow_path,
                )

        self.assertFalse(result.ok)
        self.assertEqual("obtain-codex-oauth", result.error_step)
        self.assertEqual("authorize_missing_login_session", result.step_errors["obtain-codex-oauth"]["code"])

    def test_canonical_openai_flows_handoff_login_session_to_codex_oauth(self) -> None:
        flows_dir = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "flows"
        for flow_name in ("codex-openai-account-v1.semantic-flow.json", "codex-openai-oauth-continue-v1.semantic-flow.json"):
            with self.subTest(flow_name=flow_name):
                plan = load_dst_flow(flows_dir / flow_name)
                obtain_steps = [statement for statement in plan.steps if statement.step_id == "obtain-codex-oauth"]
                self.assertEqual(1, len(obtain_steps))
                self.assertEqual("{{initialize_chatgpt_login_session}}", obtain_steps[0].input.get("login_session"))

    def test_canonical_continue_flow_recovers_mailbox_for_claimed_artifact_email(self) -> None:
        flows_dir = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "flows"
        plan = load_dst_flow(flows_dir / "codex-openai-oauth-continue-v1.semantic-flow.json")

        ordered_ids = [statement.step_id for statement in plan.steps]
        self.assertLess(
            ordered_ids.index("acquire-openai-oauth-artifact"),
            ordered_ids.index("acquire-mailbox"),
        )
        acquire_steps = [statement for statement in plan.steps if statement.step_id == "acquire-mailbox"]
        self.assertEqual(1, len(acquire_steps))
        acquire_input = acquire_steps[0].input
        self.assertEqual("{{openai_oauth_artifact.email}}", acquire_input.get("preallocated_email"))
        self.assertEqual("{{openai_oauth_artifact.mailboxRef}}", acquire_input.get("preallocated_mailbox_ref"))
        self.assertEqual("{{openai_oauth_artifact.mailboxSessionId}}", acquire_input.get("preallocated_session_id"))
        self.assertTrue(acquire_input.get("recover_preallocated_email"))

    def test_continue_flow_uses_claimed_artifact_email_mailbox_for_chatgpt_login(self) -> None:
        flows_dir = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "flows"
        flow_path = flows_dir / "codex-openai-oauth-continue-v1.semantic-flow.json"
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "continue-runs" / "worker-01" / "run-20260602-task000001"
            source_pool_dir = output_root / "openai" / "failed-once"
            source_pool_dir.mkdir(parents=True, exist_ok=True)
            seed_path = source_pool_dir / "small-seed@example.com.json"
            seed_path.write_text(
                json.dumps(
                    {
                        "email": "seed@example.com",
                        "mailboxRef": "cloudflare_temp_email:old-ref",
                        "mailboxSessionId": "old-session",
                        "createdAt": "2026-05-01T00:00:00Z",
                        "platformOrganization": {"status": "completed"},
                        "chatgptLogin": {"status": "completed", "workspaceId": "ws_123"},
                        "chatgptLoginDetails": {
                            "clientBootstrap": {
                                "authStatus": "logged_in",
                                "structure": "personal",
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            calls: list[tuple[str, dict[str, object]]] = []

            def _easyemail_dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                calls.append((step_type, dict(step_input)))
                if step_type == "acquire_mailbox":
                    self.assertEqual("seed@example.com", step_input.get("preallocated_email"))
                    self.assertEqual("cloudflare_temp_email:old-ref", step_input.get("preallocated_mailbox_ref"))
                    self.assertEqual("old-session", step_input.get("preallocated_session_id"))
                    self.assertTrue(step_input.get("recover_preallocated_email"))
                    return {
                        "ok": True,
                        "provider": "cloudflare_temp_email",
                        "email": "seed@example.com",
                        "mailbox_ref": "cloudflare_temp_email:recovered-ref",
                        "session_id": "mailbox_recovered",
                    }
                if step_type == "release_mailbox":
                    return {"released": True, "detail": "deleted"}
                raise AssertionError(step_type)

            def _easyproxy_dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                calls.append((step_type, dict(step_input)))
                if step_type == "acquire_proxy_chain":
                    return {"ok": True, "proxy_url": "http://proxy.local:25000", "lease_id": "lease-1"}
                if step_type == "release_proxy_chain":
                    return {"released": True}
                raise AssertionError(step_type)

            def _easyprotocol_dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                calls.append((step_type, dict(step_input)))
                if step_type == "initialize_platform_organization":
                    return {"ok": True, "status": "already_initialized"}
                if step_type == "initialize_chatgpt_login_session":
                    self.assertEqual("cloudflare_temp_email:recovered-ref", step_input.get("mailbox_ref"))
                    self.assertEqual("mailbox_recovered", step_input.get("mailbox_session_id"))
                    return {"ok": True, "status": "completed", "mailboxEmail": "seed@example.com"}
                if step_type == "obtain_codex_oauth":
                    self.assertEqual("cloudflare_temp_email:recovered-ref", step_input.get("mailbox_ref"))
                    self.assertEqual("mailbox_recovered", step_input.get("mailbox_session_id"))
                    return {"ok": True, "status": "completed", "successPath": str(Path(tmp_dir) / "success.json")}
                if step_type == "revoke_codex_member":
                    return {"ok": True, "status": "skipped"}
                if step_type == "upload_file_to_r2":
                    return {"ok": True}
                raise AssertionError(step_type)

            def _orchestration_dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                calls.append((step_type, dict(step_input)))
                if step_type == "acquire_openai_oauth_artifact":
                    from artifact_pool_flow import dispatch_orchestration_step

                    return dispatch_orchestration_step(step_type=step_type, step_input=step_input)
                if step_type == "validate_free_personal_oauth":
                    return {"ok": True, "status": "personal_oauth_confirmed"}
                if step_type == "finalize_openai_oauth_artifact":
                    return {"ok": True, "status": "promoted_success"}
                raise AssertionError(step_type)

            with mock.patch.dict(
                dst_flow.OWNER_DISPATCHERS,
                {
                    "orchestration": _orchestration_dispatcher,
                    "easyemail": _easyemail_dispatcher,
                    "easyproxy": _easyproxy_dispatcher,
                    "easyprotocol": _easyprotocol_dispatcher,
                },
                clear=True,
            ), mock.patch.dict(os.environ, {"REGISTER_OPENAI_OAUTH_SEED_MAX_AGE_SECONDS": "0"}, clear=False):
                result = dst_flow.run_dst_flow_once(
                    output_dir=str(run_output_dir),
                    flow_path=flow_path,
                    openai_oauth_pool_dir=str(source_pool_dir),
                )

        self.assertTrue(result.ok)
        call_order = [step_type for step_type, _ in calls]
        self.assertLess(call_order.index("acquire_openai_oauth_artifact"), call_order.index("acquire_mailbox"))
        self.assertLess(call_order.index("acquire_mailbox"), call_order.index("initialize_chatgpt_login_session"))
        self.assertEqual("seed@example.com", result.outputs["acquire-mailbox"]["email"])

    def test_canonical_openai_flows_probe_full_openai_registration_surfaces(self) -> None:
        flows_dir = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "flows"
        flow_names = (
            "codex-openai-account-v1.semantic-flow.json",
            "codex-openai-oauth-continue-v1.semantic-flow.json",
            "codex-team-expand-v1.semantic-flow.json",
        )
        expected_probe_urls = [
            "https://chatgpt.com/auth/login",
            "https://platform.openai.com/login",
            "https://auth.openai.com/log-in-or-create-account",
        ]
        for flow_name in flow_names:
            with self.subTest(flow_name=flow_name):
                plan = load_dst_flow(flows_dir / flow_name)
                proxy_steps = [statement for statement in plan.steps if statement.step_id == "acquire-proxy-chain"]
                self.assertEqual(1, len(proxy_steps))
                self.assertEqual("https://chatgpt.com/auth/login", proxy_steps[0].input.get("probe_url"))
                self.assertEqual(expected_probe_urls, proxy_steps[0].input.get("probe_urls"))
                self.assertEqual([200], proxy_steps[0].input.get("probe_expected_statuses"))

    def test_run_dst_flow_once_claims_configured_input_file_and_releases_mailbox_sessions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_dir = Path(tmp_dir) / "input"
            input_dir.mkdir(parents=True, exist_ok=True)
            source_path = input_dir / "seed.json"
            source_path.write_text(
                json.dumps(
                    {
                        "email": "user@example.com",
                        "mailbox_ref": "mailcreate:test",
                        "session_id": "session-1",
                    }
                ),
                encoding="utf-8",
            )
            flow_path = Path(tmp_dir) / "temp-flow.json"
            flow_path.write_text(
                json.dumps(
                    {
                        "definition": {
                            "id": "openai-login-v1",
                            "platform": "openai-login",
                            "steps": [
                                {
                                    "id": "claim-input-file",
                                    "type": "acquire_configured_input_file",
                                    "metadata": {"owner": "orchestration"},
                                    "input": {"input_source_dir": "{{task.input_source_dir}}"},
                                    "saveAs": "input_artifact",
                                },
                                {
                                    "id": "release-mailbox-sessions-by-email",
                                    "type": "release_mailbox_sessions_by_email",
                                    "metadata": {"owner": "easyemail"},
                                    "input": {"email_address": "{{input_artifact.email}}"},
                                    "saveAs": "mailbox_recovery",
                                },
                            ],
                        }
                    }
                ),
                encoding="utf-8",
            )

            with mock.patch.object(easyemail_runtime, "ensure_easyemail_runtime_defaults"):
                with mock.patch.object(
                    easyemail_runtime,
                    "release_mailbox_sessions_by_email",
                    return_value=[
                        {
                            "sessionId": "session-1",
                            "email": "user@example.com",
                            "release": {"released": True, "detail": "deleted"},
                        }
                    ],
                ) as release_sessions:
                    result = dst_flow.run_dst_flow_once(
                        output_dir=str(Path(tmp_dir) / "out"),
                        input_source_dir=str(input_dir),
                        flow_path=flow_path,
                    )
                    claimed_path = Path(result.outputs["claim-input-file"]["claimed_path"])
                    self.assertTrue(claimed_path.is_file())
                    self.assertFalse(source_path.exists())

        self.assertTrue(result.ok)
        self.assertEqual("claimed", result.outputs["claim-input-file"]["status"])
        self.assertEqual("user@example.com", result.outputs["claim-input-file"]["email"])
        release_sessions.assert_called_once_with(
            email_address="user@example.com",
            provider_type_key="",
            reason="",
            limit=200,
        )
        self.assertEqual("released_sessions", result.outputs["release-mailbox-sessions-by-email"]["status"])
        self.assertEqual(1, result.outputs["release-mailbox-sessions-by-email"]["released_count"])

    def test_run_dst_flow_once_executes_temp_flow_end_to_end(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            flow_path = Path(tmp_dir) / "temp-flow.json"
            flow_path.write_text(
                json.dumps(
                    {
                        "definition": {
                            "platform": "chatgpt",
                            "steps": [
                                {
                                    "id": "acquire-mailbox",
                                    "type": "acquire_mailbox",
                                    "metadata": {"owner": "easyprotocol"},
                                    "saveAs": "mailbox",
                                },
                                {
                                    "id": "create-session",
                                    "type": "initialize_chatgpt_login_session",
                                    "metadata": {"owner": "easyprotocol"},
                                    "input": {"email": "{{mailbox.email}}"},
                                    "saveAs": "session",
                                },
                                {
                                    "id": "upload-oauth-artifact",
                                    "type": "upload_file_to_r2",
                                    "metadata": {"owner": "easyprotocol"},
                                    "input": {"artifactEmail": "{{session.email}}"},
                                },
                            ]
                        }
                    }
                ),
                encoding="utf-8",
            )

            calls: list[tuple[str, dict[str, object]]] = []

            def _dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                calls.append((step_type, dict(step_input)))
                if step_type == "acquire_mailbox":
                    return {"ok": True, "email": "user@example.com"}
                if step_type == "initialize_chatgpt_login_session":
                    return {"ok": True, "email": str(step_input.get("email") or "")}
                if step_type == "upload_file_to_r2":
                    return {
                        "ok": True,
                        "object_key": "artifacts/oauth.json",
                        "bucket": "test-bucket",
                        "artifactEmail": str(step_input.get("artifactEmail") or ""),
                    }
                raise AssertionError(step_type)

            with mock.patch.dict(dst_flow.OWNER_DISPATCHERS, {"easyprotocol": _dispatcher}, clear=True):
                result = dst_flow.run_dst_flow_once(
                    output_dir=str(Path(tmp_dir) / "out"),
                    flow_path=flow_path,
                    r2_upload_enabled=True,
                    r2_bucket="test-bucket",
                )

        self.assertTrue(result.ok)
        self.assertEqual(["acquire-mailbox", "create-session", "upload-oauth-artifact"], list(result.steps.keys()))
        self.assertEqual("user@example.com", result.outputs["create-session"]["email"])
        self.assertEqual("user@example.com", result.outputs["upload-oauth-artifact"]["artifactEmail"])
        self.assertEqual(
            [
                ("acquire_mailbox", {}),
                ("initialize_chatgpt_login_session", {"email": "user@example.com"}),
                ("upload_file_to_r2", {"artifactEmail": "user@example.com"}),
            ],
            calls,
        )

    def test_run_dst_flow_once_propagates_mailbox_business_key_from_flow_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            flow_path = Path(tmp_dir) / "temp-flow.json"
            flow_path.write_text(
                json.dumps(
                    {
                        "definition": {
                            "id": "test-openai-flow",
                            "platform": "chatgpt",
                            "metadata": {
                                "mailbox": {
                                    "businessKey": "openai"
                                }
                            },
                            "steps": [
                                {
                                    "id": "acquire-mailbox",
                                    "type": "acquire_mailbox",
                                    "metadata": {"owner": "easyprotocol"},
                                    "input": {"business_key": "{{task.mailbox_business_key}}"},
                                }
                            ]
                        }
                    }
                ),
                encoding="utf-8",
            )

            calls: list[tuple[str, dict[str, object]]] = []

            def _dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                calls.append((step_type, dict(step_input)))
                return {"ok": True, "email": "user@example.com"}

            with mock.patch.dict(dst_flow.OWNER_DISPATCHERS, {"easyprotocol": _dispatcher}, clear=True):
                result = dst_flow.run_dst_flow_once(
                    output_dir=str(Path(tmp_dir) / "out"),
                    flow_path=flow_path,
                )

        self.assertTrue(result.ok)
        self.assertEqual("openai", result.to_dict()["taskContext"]["mailboxBusinessKey"])
        self.assertEqual([("acquire_mailbox", {"business_key": "openai"})], calls)

    def test_run_dst_flow_once_propagates_independent_login_entry_url(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            flow_path = Path(tmp_dir) / "temp-flow.json"
            flow_path.write_text(
                json.dumps(
                    {
                        "definition": {
                            "id": "openai-login-v1",
                            "platform": "openai-login",
                            "steps": [
                                {
                                    "id": "initialize-login",
                                    "type": "initialize_chatgpt_login_session",
                                    "metadata": {"owner": "easyprotocol"},
                                    "input": {"login_entry_url": "{{task.login_entry_url}}"},
                                    "saveAs": "login_session",
                                }
                            ],
                        }
                    }
                ),
                encoding="utf-8",
            )

            calls: list[tuple[str, dict[str, object]]] = []

            def _dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                calls.append((step_type, dict(step_input)))
                return {"ok": True, "status": "completed"}

            with mock.patch.dict(dst_flow.OWNER_DISPATCHERS, {"easyprotocol": _dispatcher}, clear=True):
                result = dst_flow.run_dst_flow_once(
                    output_dir=str(Path(tmp_dir) / "out"),
                    flow_path=flow_path,
                )

        self.assertTrue(result.ok)
        self.assertEqual(
            "https://auth.openai.com/log-in-or-create-account",
            result.to_dict()["taskContext"]["loginEntryUrl"],
        )
        self.assertEqual(
            [
                (
                    "initialize_chatgpt_login_session",
                    {"login_entry_url": "https://auth.openai.com/log-in-or-create-account"},
                )
            ],
            calls,
        )

    def test_run_dst_flow_once_retries_invite_after_proxy_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            flow_path = Path(tmp_dir) / "temp-flow.json"
            flow_path.write_text(
                json.dumps(
                    {
                        "definition": {
                            "platform": "chatgpt",
                            "steps": [
                                {
                                    "id": "acquire-proxy-chain",
                                    "type": "acquire_proxy_chain",
                                    "metadata": {"owner": "easyprotocol"},
                                    "saveAs": "proxy_chain",
                                },
                                {
                                    "id": "refresh-team-auth-on-demand",
                                    "type": "obtain_team_mother_oauth",
                                    "metadata": {"owner": "easyprotocol"},
                                    "saveAs": "team_mother_oauth_refresh",
                                },
                                {
                                    "id": "invite-codex-member",
                                    "type": "invite_codex_member",
                                    "metadata": {
                                        "owner": "easyprotocol",
                                        "retry": {
                                            "maxAttempts": 2,
                                            "retryProfile": "step-invite-recover",
                                            "refreshSavedStates": [
                                                "proxy_chain",
                                                "team_mother_oauth_refresh",
                                            ],
                                        },
                                    },
                                    "input": {
                                        "proxy_url": "{{proxy_chain.proxy_url}}",
                                        "team_auth_path": "{{team_mother_oauth_refresh.successPath}}",
                                    },
                                    "saveAs": "invite_codex_member",
                                },
                            ]
                        }
                    }
                ),
                encoding="utf-8",
            )

            proxy_call_count = 0
            refresh_call_count = 0
            invite_proxy_urls: list[str] = []
            invite_team_auth_paths: list[str] = []

            def _dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                nonlocal proxy_call_count, refresh_call_count
                if step_type == "acquire_proxy_chain":
                    proxy_call_count += 1
                    return {"ok": True, "proxy_url": f"http://proxy-{proxy_call_count}"}
                if step_type == "obtain_team_mother_oauth":
                    refresh_call_count += 1
                    return {
                        "ok": True,
                        "successPath": f"/tmp/team-auth-refresh-{refresh_call_count}.json",
                    }
                if step_type == "invite_codex_member":
                    invite_proxy_urls.append(str(step_input.get("proxy_url") or ""))
                    invite_team_auth_paths.append(str(step_input.get("team_auth_path") or ""))
                    if len(invite_proxy_urls) == 1:
                        raise RuntimeError(
                            "Failed to perform, curl: (28) Operation timed out after 30001 milliseconds with 0 bytes received."
                        )
                    return {
                        "ok": True,
                        "status": "already_invited",
                        "team_account_id": "acct_123",
                        "team_email": "mother@example.com",
                    }
                raise AssertionError(step_type)

            with mock.patch.dict(dst_flow.OWNER_DISPATCHERS, {"easyprotocol": _dispatcher}, clear=True):
                result = dst_flow.run_dst_flow_once(
                    output_dir=str(Path(tmp_dir) / "out"),
                    flow_path=flow_path,
                )

        self.assertTrue(result.ok)
        self.assertEqual(2, result.step_attempts["invite-codex-member"])
        self.assertEqual(["http://proxy-1", "http://proxy-2"], invite_proxy_urls)
        self.assertEqual(
            [
                "/tmp/team-auth-refresh-1.json",
                "/tmp/team-auth-refresh-2.json",
            ],
            invite_team_auth_paths,
        )
        self.assertEqual(2, proxy_call_count)
        self.assertEqual(2, refresh_call_count)

    def test_run_dst_flow_once_skips_invite_chain_when_team_invite_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            flow_path = Path(tmp_dir) / "temp-flow.json"
            flow_path.write_text(
                json.dumps(
                    {
                        "definition": {
                            "platform": "chatgpt",
                            "steps": [
                                {
                                    "id": "refresh-team-auth-on-demand",
                                    "type": "obtain_team_mother_oauth",
                                    "metadata": {
                                        "owner": "easyprotocol",
                                        "enabledWhen": "{{task.team_invite_enabled}}",
                                    },
                                    "saveAs": "team_mother_oauth_refresh",
                                },
                                {
                                    "id": "invite-codex-member",
                                    "type": "invite_codex_member",
                                    "metadata": {
                                        "owner": "easyprotocol",
                                        "enabledWhen": "{{task.team_invite_enabled}}",
                                    },
                                    "saveAs": "invite_codex_member",
                                },
                                {
                                    "id": "obtain-codex-oauth",
                                    "type": "obtain_codex_oauth",
                                    "metadata": {"owner": "easyprotocol"},
                                    "saveAs": "obtain_codex_oauth",
                                },
                                {
                                    "id": "validate-free-personal-oauth",
                                    "type": "validate_free_personal_oauth",
                                    "metadata": {"owner": "orchestration"},
                                    "input": {
                                        "oauth_result": "{{obtain_codex_oauth}}",
                                        "invite_result": "{{invite_codex_member}}",
                                    },
                                    "saveAs": "validate_free_personal_oauth",
                                },
                            ]
                        }
                    }
                ),
                encoding="utf-8",
            )

            calls: list[str] = []

            def _dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                calls.append(step_type)
                if step_type == "obtain_codex_oauth":
                    return {
                        "ok": True,
                        "status": "completed",
                        "organizations": [{"id": "org_123"}],
                    }
                if step_type == "validate_free_personal_oauth":
                    return {"ok": True, "status": "personal_oauth_confirmed"}
                raise AssertionError(step_type)

            with mock.patch.dict(
                dst_flow.OWNER_DISPATCHERS,
                {"easyprotocol": _dispatcher, "orchestration": _dispatcher},
                clear=True,
            ):
                result = dst_flow.run_dst_flow_once(
                    output_dir=str(Path(tmp_dir) / "out"),
                    flow_path=flow_path,
                    team_invite_enabled=False,
                )

        self.assertTrue(result.ok)
        self.assertEqual("skipped", result.steps["refresh-team-auth-on-demand"])
        self.assertEqual("skipped", result.steps["invite-codex-member"])
        self.assertEqual(
            ["obtain_codex_oauth", "validate_free_personal_oauth"],
            calls,
        )

    def test_run_dst_flow_once_keeps_phone_failure_in_existing_failed_semantics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            flow_path = Path(tmp_dir) / "temp-flow.json"
            flow_path.write_text(
                json.dumps(
                    {
                        "definition": {
                            "platform": "chatgpt",
                            "steps": [
                                {
                                    "id": "obtain-codex-oauth",
                                    "type": "obtain_codex_oauth",
                                    "metadata": {"owner": "easyprotocol"},
                                    "saveAs": "obtain_codex_oauth",
                                }
                            ]
                        }
                    }
                ),
                encoding="utf-8",
            )

            def _dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                raise RuntimeError("wait_code_timeout")

            with mock.patch.dict(dst_flow.OWNER_DISPATCHERS, {"easyprotocol": _dispatcher}, clear=True):
                result = dst_flow.run_dst_flow_once(
                    output_dir=str(Path(tmp_dir) / "out"),
                    flow_path=flow_path,
                )

        self.assertFalse(result.ok)
        self.assertEqual("obtain-codex-oauth", result.error_step)
        self.assertEqual("wait_code_timeout", result.error)

    def test_run_dst_flow_once_obtain_codex_oauth_can_complete_sms_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            flow_path = Path(tmp_dir) / "temp-flow.json"
            flow_path.write_text(
                json.dumps(
                    {
                        "definition": {
                            "platform": "chatgpt",
                            "steps": [
                                {
                                    "id": "obtain-codex-oauth",
                                    "type": "obtain_codex_oauth",
                                    "metadata": {"owner": "easyprotocol"},
                                    "saveAs": "obtain_codex_oauth",
                                },
                                {
                                    "id": "validate-free-personal-oauth",
                                    "type": "validate_free_personal_oauth",
                                    "metadata": {"owner": "orchestration"},
                                    "input": {"oauth_result": "{{obtain_codex_oauth}}"},
                                    "saveAs": "validate_free_personal_oauth",
                                },
                            ]
                        }
                    }
                ),
                encoding="utf-8",
            )

            def _dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                if step_type == "obtain_codex_oauth":
                    return {
                        "ok": True,
                        "status": "completed",
                        "successPath": "C:/tmp/codex-free.json",
                        "phoneVerificationAttempted": True,
                        "phoneProvider": "sms24",
                    }
                if step_type == "validate_free_personal_oauth":
                    return {"ok": True, "status": "personal_oauth_confirmed"}
                raise AssertionError(step_type)

            with mock.patch.dict(
                dst_flow.OWNER_DISPATCHERS,
                {"easyprotocol": _dispatcher, "orchestration": _dispatcher},
                clear=True,
            ):
                result = dst_flow.run_dst_flow_once(
                    output_dir=str(Path(tmp_dir) / "out"),
                    flow_path=flow_path,
                )

        self.assertTrue(result.ok)
        self.assertEqual("sms24", result.outputs["obtain-codex-oauth"]["phoneProvider"])
        self.assertTrue(result.outputs["obtain-codex-oauth"]["phoneVerificationAttempted"])

    def test_run_dst_flow_once_does_not_retry_obtain_after_phone_submission_lacks_sms_code(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            flow_path = Path(tmp_dir) / "temp-flow.json"
            oauth_path = Path(tmp_dir) / "oauth.json"
            oauth_path.write_text('{"refresh_token":"rt"}', encoding="utf-8")
            flow_path.write_text(
                json.dumps(
                    {
                        "definition": {
                            "platform": "chatgpt",
                            "steps": [
                                {
                                    "id": "acquire-proxy-chain",
                                    "type": "acquire_proxy_chain",
                                    "metadata": {"owner": "easyproxy"},
                                    "saveAs": "proxy_chain",
                                },
                                {
                                    "id": "initialize-chatgpt-login-session",
                                    "type": "initialize_chatgpt_login_session",
                                    "metadata": {"owner": "easyprotocol"},
                                    "input": {"proxy_url": "{{proxy_chain.proxy_url}}"},
                                    "saveAs": "initialize_chatgpt_login_session",
                                },
                                {
                                    "id": "obtain-codex-oauth",
                                    "type": "obtain_codex_oauth",
                                    "metadata": {
                                        "owner": "easyprotocol",
                                        "retry": {
                                            "maxAttempts": 2,
                                            "retryProfile": "step-oauth-recover",
                                            "refreshSavedStates": [
                                                "proxy_chain",
                                                "initialize_chatgpt_login_session",
                                            ],
                                        },
                                    },
                                    "input": {
                                        "proxy_url": "{{proxy_chain.proxy_url}}",
                                        "login_session": "{{initialize_chatgpt_login_session}}",
                                    },
                                    "saveAs": "obtain_codex_oauth",
                                },
                                {
                                    "id": "validate-free-personal-oauth",
                                    "type": "validate_free_personal_oauth",
                                    "metadata": {"owner": "orchestration"},
                                    "input": {"oauth_result": "{{obtain_codex_oauth}}"},
                                    "saveAs": "validate_free_personal_oauth",
                                },
                            ]
                        }
                    }
                ),
                encoding="utf-8",
            )

            proxy_calls = 0
            login_calls = 0
            obtain_calls = 0
            validate_calls = 0

            def _dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                nonlocal proxy_calls, login_calls, obtain_calls, validate_calls
                if step_type == "acquire_proxy_chain":
                    proxy_calls += 1
                    return {"proxy_url": f"http://proxy-{proxy_calls}"}
                if step_type == "release_proxy_chain":
                    return {"released": True}
                if step_type == "initialize_chatgpt_login_session":
                    login_calls += 1
                    return {"ok": True, "session": f"login-{login_calls}"}
                if step_type == "obtain_codex_oauth":
                    obtain_calls += 1
                    if obtain_calls == 1:
                        return {
                            "ok": True,
                            "status": "phone_verification_submitted_small_success",
                            "successPath": str(oauth_path),
                            "phoneVerificationAttempted": True,
                            "phoneVerificationSubmitted": True,
                            "phoneVerificationAccepted": False,
                            "phoneVerificationFailureStage": "wait_sms_code",
                            "phoneVerificationFailureDetail": "sms service failed: HTTP 502",
                        }
                    raise AssertionError("phone-submitted small-success should not retry in the same task")
                if step_type == "validate_free_personal_oauth":
                    validate_calls += 1
                    if bool(step_input.get("oauth_result", {}).get("phoneVerificationSubmitted")):
                        raise AssertionError("validate should not run before retrying SMS failure")
                    return {"ok": True, "status": "personal_oauth_confirmed"}
                raise AssertionError(step_type)

            with mock.patch.dict(
                dst_flow.OWNER_DISPATCHERS,
                {
                    "easyproxy": _dispatcher,
                    "easyprotocol": _dispatcher,
                    "orchestration": _dispatcher,
                },
                clear=True,
            ):
                result = dst_flow.run_dst_flow_once(
                    output_dir=str(Path(tmp_dir) / "out"),
                    flow_path=flow_path,
                )

        self.assertEqual(1, obtain_calls)
        self.assertEqual(1, proxy_calls)
        self.assertEqual(1, login_calls)
        self.assertEqual(0, validate_calls)
        self.assertFalse(result.ok)
        self.assertEqual("obtain-codex-oauth", result.error_step)
        self.assertEqual("failed", result.steps["obtain-codex-oauth"])
        self.assertEqual(ErrorCodes.PHONE_VERIFICATION_SUBMITTED_SMALL_SUCCESS, result.step_errors["obtain-codex-oauth"]["code"])
        self.assertEqual("skipped", result.steps["validate-free-personal-oauth"])

    def test_run_dst_flow_once_retries_obtain_after_phone_attempt_fails_before_submission(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            flow_path = Path(tmp_dir) / "temp-flow.json"
            oauth_path = Path(tmp_dir) / "oauth.json"
            oauth_path.write_text('{"refresh_token":"rt"}', encoding="utf-8")
            flow_path.write_text(
                json.dumps(
                    {
                        "definition": {
                            "platform": "chatgpt",
                            "steps": [
                                {
                                    "id": "acquire-proxy-chain",
                                    "type": "acquire_proxy_chain",
                                    "metadata": {"owner": "easyproxy"},
                                    "saveAs": "proxy_chain",
                                },
                                {
                                    "id": "initialize-chatgpt-login-session",
                                    "type": "initialize_chatgpt_login_session",
                                    "metadata": {"owner": "easyprotocol"},
                                    "input": {"proxy_url": "{{proxy_chain.proxy_url}}"},
                                    "saveAs": "initialize_chatgpt_login_session",
                                },
                                {
                                    "id": "obtain-codex-oauth",
                                    "type": "obtain_codex_oauth",
                                    "metadata": {
                                        "owner": "easyprotocol",
                                        "retry": {
                                            "maxAttempts": 2,
                                            "retryProfile": "step-oauth-recover",
                                            "refreshSavedStates": [
                                                "proxy_chain",
                                                "initialize_chatgpt_login_session",
                                            ],
                                        },
                                    },
                                    "input": {
                                        "proxy_url": "{{proxy_chain.proxy_url}}",
                                        "login_session": "{{initialize_chatgpt_login_session}}",
                                    },
                                    "saveAs": "obtain_codex_oauth",
                                },
                                {
                                    "id": "validate-free-personal-oauth",
                                    "type": "validate_free_personal_oauth",
                                    "metadata": {"owner": "orchestration"},
                                    "input": {"oauth_result": "{{obtain_codex_oauth}}"},
                                    "saveAs": "validate_free_personal_oauth",
                                },
                            ]
                        }
                    }
                ),
                encoding="utf-8",
            )

            proxy_calls = 0
            login_calls = 0
            obtain_calls = 0
            validate_calls = 0

            def _dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                nonlocal proxy_calls, login_calls, obtain_calls, validate_calls
                if step_type == "acquire_proxy_chain":
                    proxy_calls += 1
                    return {"proxy_url": f"http://proxy-{proxy_calls}"}
                if step_type == "release_proxy_chain":
                    return {"released": True}
                if step_type == "initialize_chatgpt_login_session":
                    login_calls += 1
                    return {"ok": True, "session": f"login-{login_calls}"}
                if step_type == "obtain_codex_oauth":
                    obtain_calls += 1
                    if obtain_calls == 1:
                        return {
                            "ok": True,
                            "status": "phone_verification_attempted_small_success",
                            "successPath": str(oauth_path),
                            "phoneVerificationAttempted": True,
                            "phoneVerificationSubmitted": False,
                            "phoneVerificationAccepted": False,
                            "phoneVerificationFailureStage": "submit_phone_verification_number",
                            "phoneVerificationFailureDetail": (
                                "<urlopen error [SSL: UNEXPECTED_EOF_WHILE_READING] "
                                "EOF occurred in violation of protocol>"
                            ),
                        }
                    return {
                        "ok": True,
                        "status": "completed",
                        "successPath": str(oauth_path),
                    }
                if step_type == "validate_free_personal_oauth":
                    validate_calls += 1
                    if bool(step_input.get("oauth_result", {}).get("phoneVerificationAttempted")):
                        raise AssertionError("validate should not run for pre-submission phone attempt failures")
                    return {"ok": True, "status": "personal_oauth_confirmed"}
                raise AssertionError(step_type)

            with mock.patch.dict(
                dst_flow.OWNER_DISPATCHERS,
                {
                    "easyproxy": _dispatcher,
                    "easyprotocol": _dispatcher,
                    "orchestration": _dispatcher,
                },
                clear=True,
            ):
                result = dst_flow.run_dst_flow_once(
                    output_dir=str(Path(tmp_dir) / "out"),
                    flow_path=flow_path,
                )

        self.assertTrue(result.ok)
        self.assertEqual(2, obtain_calls)
        self.assertEqual(2, proxy_calls)
        self.assertEqual(2, login_calls)
        self.assertEqual(1, validate_calls)
        self.assertEqual(2, result.step_attempts["obtain-codex-oauth"])

    def test_run_dst_flow_once_retries_chatgpt_login_after_proxy_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            flow_path = Path(tmp_dir) / "temp-flow.json"
            flow_path.write_text(
                json.dumps(
                    {
                        "definition": {
                            "platform": "chatgpt",
                            "steps": [
                                {
                                    "id": "acquire-proxy-chain",
                                    "type": "acquire_proxy_chain",
                                    "metadata": {"owner": "easyprotocol"},
                                    "saveAs": "proxy_chain",
                                },
                                {
                                    "id": "initialize-chatgpt-login-session",
                                    "type": "initialize_chatgpt_login_session",
                                    "metadata": {
                                        "owner": "easyprotocol",
                                        "retry": {
                                            "maxAttempts": 2,
                                            "retryProfile": "step-proxy-refresh",
                                            "refreshSavedStates": [
                                                "proxy_chain",
                                            ],
                                        },
                                    },
                                    "input": {
                                        "proxy_url": "{{proxy_chain.proxy_url}}",
                                    },
                                    "saveAs": "initialize_chatgpt_login_session",
                                },
                            ]
                        }
                    }
                ),
                encoding="utf-8",
            )

            proxy_call_count = 0
            login_proxy_urls: list[str] = []

            def _dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                nonlocal proxy_call_count
                if step_type == "acquire_proxy_chain":
                    proxy_call_count += 1
                    return {"ok": True, "proxy_url": f"http://proxy-{proxy_call_count}"}
                if step_type == "initialize_chatgpt_login_session":
                    login_proxy_urls.append(str(step_input.get("proxy_url") or ""))
                    if len(login_proxy_urls) == 1:
                        raise RuntimeError("Failed to perform, curl: (7) Connection closed abruptly.")
                    return {
                        "ok": True,
                        "status": "completed",
                        "workspaceId": "ws_123",
                    }
                raise AssertionError(step_type)

            with mock.patch.dict(dst_flow.OWNER_DISPATCHERS, {"easyprotocol": _dispatcher}, clear=True):
                result = dst_flow.run_dst_flow_once(
                    output_dir=str(Path(tmp_dir) / "out"),
                    flow_path=flow_path,
                )

        self.assertTrue(result.ok)
        self.assertEqual(2, result.step_attempts["initialize-chatgpt-login-session"])
        self.assertEqual(["http://proxy-1", "http://proxy-2"], login_proxy_urls)
        self.assertEqual(2, proxy_call_count)

    def test_run_dst_flow_once_retries_protocol_source_artifact_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            flow_path = Path(tmp_dir) / "temp-flow.json"
            flow_path.write_text(
                json.dumps(
                    {
                        "definition": {
                            "platform": "chatgpt",
                            "steps": [
                                {
                                    "id": "acquire-proxy-chain",
                                    "type": "acquire_proxy_chain",
                                    "metadata": {"owner": "easyproxy"},
                                    "saveAs": "proxy_chain",
                                },
                                {
                                    "id": "initialize-platform-organization",
                                    "type": "initialize_platform_organization",
                                    "metadata": {
                                        "owner": "easyprotocol",
                                        "retry": {
                                            "maxAttempts": 2,
                                            "retryProfile": "step-proxy-refresh",
                                            "refreshSavedStates": [
                                                "proxy_chain",
                                            ],
                                        },
                                    },
                                    "input": {
                                        "source_path": "/shared/register-output/others/openai-oauth-claims/claimed.json",
                                        "proxy_url": "{{proxy_chain.proxy_url}}",
                                    },
                                    "saveAs": "initialize_platform_organization",
                                },
                            ],
                        }
                    }
                ),
                encoding="utf-8",
            )

            proxy_call_count = 0
            init_proxy_urls: list[str] = []

            def _dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                nonlocal proxy_call_count
                if step_type == "acquire_proxy_chain":
                    proxy_call_count += 1
                    return {"ok": True, "proxy_url": f"http://proxy-{proxy_call_count}"}
                if step_type == "release_proxy_chain":
                    return {"released": True, "detail": "released"}
                if step_type == "initialize_platform_organization":
                    init_proxy_urls.append(str(step_input.get("proxy_url") or ""))
                    if len(init_proxy_urls) == 1:
                        raise RuntimeError(
                            "[Errno 2] No such file or directory: "
                            "'/shared/register-output/others/openai-oauth-claims/claimed.json'"
                        )
                    return {
                        "ok": True,
                        "status": "completed",
                        "sourcePath": str(step_input.get("source_path") or ""),
                    }
                raise AssertionError(step_type)

            with mock.patch.dict(
                dst_flow.OWNER_DISPATCHERS,
                {
                    "easyproxy": _dispatcher,
                    "easyprotocol": _dispatcher,
                },
                clear=True,
            ):
                result = dst_flow.run_dst_flow_once(
                    output_dir=str(Path(tmp_dir) / "out"),
                    flow_path=flow_path,
                )

        self.assertTrue(result.ok)
        self.assertEqual(2, result.step_attempts["initialize-platform-organization"])
        self.assertEqual(["http://proxy-1", "http://proxy-2"], init_proxy_urls)

    def test_run_dst_flow_once_retries_create_account_after_proxy_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            flow_path = Path(tmp_dir) / "temp-flow.json"
            flow_path.write_text(
                json.dumps(
                    {
                        "definition": {
                            "platform": "chatgpt",
                            "steps": [
                                {
                                    "id": "acquire-proxy-chain",
                                    "type": "acquire_proxy_chain",
                                    "metadata": {"owner": "easyprotocol"},
                                    "saveAs": "proxy_chain",
                                },
                                {
                                    "id": "create-openai-account",
                                    "type": "create_openai_account",
                                    "metadata": {
                                        "owner": "easyprotocol",
                                        "retry": {
                                            "maxAttempts": 2,
                                            "retryProfile": "step-proxy-refresh",
                                            "refreshSavedStates": [
                                                "proxy_chain"
                                            ],
                                        },
                                    },
                                    "input": {
                                        "proxy_url": "{{proxy_chain.proxy_url}}",
                                    },
                                    "saveAs": "create_openai_account",
                                },
                            ]
                        }
                    }
                ),
                encoding="utf-8",
            )

            proxy_call_count = 0
            create_proxy_urls: list[str] = []

            def _dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                nonlocal proxy_call_count
                if step_type == "acquire_proxy_chain":
                    proxy_call_count += 1
                    return {"ok": True, "proxy_url": f"http://proxy-{proxy_call_count}"}
                if step_type == "create_openai_account":
                    create_proxy_urls.append(str(step_input.get("proxy_url") or ""))
                    if len(create_proxy_urls) == 1:
                        raise RuntimeError("Failed to perform, curl: (7) Connection closed abruptly.")
                    return {
                        "ok": True,
                        "status": "completed",
                        "storage_path": "/tmp/create-success.json",
                    }
                raise AssertionError(step_type)

            with mock.patch.dict(dst_flow.OWNER_DISPATCHERS, {"easyprotocol": _dispatcher}, clear=True):
                result = dst_flow.run_dst_flow_once(
                    output_dir=str(Path(tmp_dir) / "out"),
                    flow_path=flow_path,
                )

        self.assertTrue(result.ok)
        self.assertEqual(2, result.step_attempts["create-openai-account"])
        self.assertEqual(["http://proxy-1", "http://proxy-2"], create_proxy_urls)
        self.assertEqual(2, proxy_call_count)

    def test_run_dst_flow_once_retries_create_account_after_user_register_400(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            flow_path = Path(tmp_dir) / "temp-flow.json"
            flow_path.write_text(
                json.dumps(
                    {
                        "definition": {
                            "platform": "chatgpt",
                            "steps": [
                                {
                                    "id": "acquire-proxy-chain",
                                    "type": "acquire_proxy_chain",
                                    "metadata": {"owner": "easyprotocol"},
                                    "saveAs": "proxy_chain",
                                },
                                {
                                    "id": "create-openai-account",
                                    "type": "create_openai_account",
                                    "metadata": {
                                        "owner": "easyprotocol",
                                        "retry": {
                                            "maxAttempts": 2,
                                            "retryProfile": "step-create-account-recover",
                                            "refreshSavedStates": [
                                                "proxy_chain"
                                            ],
                                        },
                                    },
                                    "input": {
                                        "proxy_url": "{{proxy_chain.proxy_url}}",
                                    },
                                    "saveAs": "create_openai_account",
                                },
                            ]
                        }
                    }
                ),
                encoding="utf-8",
            )

            proxy_call_count = 0
            create_proxy_urls: list[str] = []

            def _dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                nonlocal proxy_call_count
                if step_type == "acquire_proxy_chain":
                    proxy_call_count += 1
                    return {"ok": True, "proxy_url": f"http://proxy-{proxy_call_count}"}
                if step_type == "create_openai_account":
                    create_proxy_urls.append(str(step_input.get("proxy_url") or ""))
                    if len(create_proxy_urls) == 1:
                        raise RuntimeError(
                            "user_register status=400 body={"
                            "\"error\":{\"message\":\"Failed to create account. Please try again.\"}}"
                        )
                    return {
                        "ok": True,
                        "status": "completed",
                        "storage_path": "/tmp/create-success.json",
                    }
                raise AssertionError(step_type)

            with mock.patch.dict(dst_flow.OWNER_DISPATCHERS, {"easyprotocol": _dispatcher}, clear=True):
                result = dst_flow.run_dst_flow_once(
                    output_dir=str(Path(tmp_dir) / "out"),
                    flow_path=flow_path,
                )

        self.assertTrue(result.ok)
        self.assertEqual(2, result.step_attempts["create-openai-account"])
        self.assertEqual(["http://proxy-1", "http://proxy-2"], create_proxy_urls)
        self.assertEqual(2, proxy_call_count)

    def test_run_dst_flow_once_retries_create_account_with_mailbox_and_proxy_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            flow_path = Path(tmp_dir) / "temp-flow.json"
            flow_path.write_text(
                json.dumps(
                    {
                        "definition": {
                            "platform": "chatgpt",
                            "steps": [
                                {
                                    "id": "acquire-mailbox",
                                    "type": "acquire_mailbox",
                                    "metadata": {"owner": "easyemail"},
                                    "input": {
                                        "business_key": "{{task.mailbox_business_key}}",
                                        "avoid_emails": "{{task.avoidMailboxEmails}}",
                                        "avoid_domains": "{{task.avoidMailboxDomains}}",
                                        "avoid_providers": "{{task.avoidMailboxProviders}}",
                                        "avoid_reason": "{{task.avoidMailboxReason}}",
                                    },
                                    "saveAs": "mailbox",
                                },
                                {
                                    "id": "acquire-proxy-chain",
                                    "type": "acquire_proxy_chain",
                                    "metadata": {"owner": "easyproxy"},
                                    "saveAs": "proxy_chain",
                                },
                                {
                                    "id": "create-openai-account",
                                    "type": "create_openai_account",
                                    "metadata": {
                                        "owner": "easyprotocol",
                                        "retry": {
                                            "maxAttempts": 2,
                                            "retryProfile": "step-create-account-recover",
                                            "refreshSavedStates": [
                                                "mailbox",
                                                "proxy_chain"
                                            ],
                                        },
                                    },
                                    "input": {
                                        "preallocated_email": "{{mailbox.email}}",
                                        "preallocated_session_id": "{{mailbox.session_id}}",
                                        "preallocated_mailbox_ref": "{{mailbox.mailbox_ref}}",
                                        "proxy_url": "{{proxy_chain.proxy_url}}",
                                    },
                                    "saveAs": "create_openai_account",
                                },
                            ]
                        }
                    }
                ),
                encoding="utf-8",
            )

            mailbox_call_count = 0
            proxy_call_count = 0
            create_inputs: list[tuple[str, str]] = []
            mailbox_inputs: list[dict[str, object]] = []
            released_mailboxes: list[tuple[str, str]] = []
            released_proxies: list[tuple[str, str]] = []

            def _easyemail_dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                nonlocal mailbox_call_count
                if step_type == "acquire_mailbox":
                    mailbox_inputs.append(dict(step_input))
                    mailbox_call_count += 1
                    return {
                        "ok": True,
                        "provider": "m2u" if mailbox_call_count == 1 else "cloudflare_temp_email",
                        "email": "user1@kkb.qzz.io" if mailbox_call_count == 1 else "user2@example.com",
                        "mailbox_ref": f"mailbox-ref-{mailbox_call_count}",
                        "session_id": f"mailbox-session-{mailbox_call_count}",
                        "business_key": "openai",
                    }
                if step_type == "release_mailbox":
                    released_mailboxes.append(
                        (
                            str(step_input.get("mailbox_ref") or ""),
                            str(step_input.get("mailbox_session_id") or ""),
                        )
                    )
                    return {"released": True, "detail": "deleted"}
                raise AssertionError(step_type)

            def _easyproxy_dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                nonlocal proxy_call_count
                if step_type == "acquire_proxy_chain":
                    proxy_call_count += 1
                    return {
                        "ok": True,
                        "proxy_url": f"http://proxy-{proxy_call_count}",
                        "lease_id": f"lease-{proxy_call_count}",
                    }
                if step_type == "release_proxy_chain":
                    released_proxies.append(
                        (
                            str(step_input.get("proxy_url") or ""),
                            str(step_input.get("lease_id") or ""),
                        )
                    )
                    return {"released": True, "detail": "released"}
                raise AssertionError(step_type)

            def _easyprotocol_dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                if step_type != "create_openai_account":
                    raise AssertionError(step_type)
                create_inputs.append(
                    (
                        str(step_input.get("preallocated_email") or ""),
                        str(step_input.get("proxy_url") or ""),
                    )
                )
                if len(create_inputs) == 1:
                    raise RuntimeError(
                        "user_register status=400 body={"
                        "\"error\":{\"message\":\"Failed to create account. Please try again.\"}}"
                    )
                return {
                    "ok": True,
                    "status": "completed",
                    "storage_path": "/tmp/create-success.json",
                }

            with mock.patch.dict(
                dst_flow.OWNER_DISPATCHERS,
                {
                    "easyemail": _easyemail_dispatcher,
                    "easyproxy": _easyproxy_dispatcher,
                    "easyprotocol": _easyprotocol_dispatcher,
                },
                clear=True,
            ):
                result = dst_flow.run_dst_flow_once(
                    output_dir=str(Path(tmp_dir) / "out"),
                    flow_path=flow_path,
                )

        self.assertTrue(result.ok)
        self.assertEqual(2, result.step_attempts["create-openai-account"])
        self.assertEqual(2, result.step_attempts["acquire-mailbox"])
        self.assertEqual(2, result.step_attempts["acquire-proxy-chain"])
        self.assertEqual(
            [
                ("user1@kkb.qzz.io", "http://proxy-1"),
                ("user2@example.com", "http://proxy-2"),
            ],
            create_inputs,
        )
        self.assertEqual("", mailbox_inputs[0]["avoid_emails"])
        self.assertEqual(["user1@kkb.qzz.io"], mailbox_inputs[1]["avoid_emails"])
        self.assertEqual(["kkb.qzz.io"], mailbox_inputs[1]["avoid_domains"])
        self.assertEqual(["m2u"], mailbox_inputs[1]["avoid_providers"])
        self.assertEqual("create_account_user_register_400", mailbox_inputs[1]["avoid_reason"])
        self.assertEqual(
            [
                {
                    "outcome": "failure",
                    "failureReason": "create_account_user_register_400",
                    "failureClass": "weak_attributed_generic_register_400",
                    "errorCode": "user_register_400",
                    "provider": "m2u",
                    "domain": "kkb.qzz.io",
                    "email": "user1@kkb.qzz.io",
                    "mailbox_ref": "mailbox-ref-1",
                    "mailbox_session_id": "mailbox-session-1",
                    "business_key": "openai",
                    "stepId": "create-openai-account",
                    "attempt": 1,
                }
            ],
            result.outputs["mailbox-attempt-outcomes"],
        )
        self.assertEqual([("mailbox-ref-1", "mailbox-session-1")], released_mailboxes)
        self.assertEqual([("http://proxy-1", "lease-1")], released_proxies)

    def test_run_dst_flow_once_retries_create_account_after_email_otp_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            flow_path = Path(tmp_dir) / "temp-flow.json"
            flow_path.write_text(
                json.dumps(
                    {
                        "definition": {
                            "platform": "chatgpt",
                            "steps": [
                                {
                                    "id": "acquire-mailbox",
                                    "type": "acquire_mailbox",
                                    "metadata": {"owner": "easyemail"},
                                    "input": {
                                        "business_key": "{{task.mailbox_business_key}}",
                                        "avoid_emails": "{{task.avoidMailboxEmails}}",
                                        "avoid_domains": "{{task.avoidMailboxDomains}}",
                                        "avoid_providers": "{{task.avoidMailboxProviders}}",
                                        "avoid_reason": "{{task.avoidMailboxReason}}",
                                    },
                                    "saveAs": "mailbox",
                                },
                                {
                                    "id": "acquire-proxy-chain",
                                    "type": "acquire_proxy_chain",
                                    "metadata": {"owner": "easyproxy"},
                                    "saveAs": "proxy_chain",
                                },
                                {
                                    "id": "create-openai-account",
                                    "type": "create_openai_account",
                                    "metadata": {
                                        "owner": "easyprotocol",
                                        "retry": {
                                            "maxAttempts": 2,
                                            "retryProfile": "step-create-account-recover",
                                            "refreshSavedStates": ["mailbox", "proxy_chain"],
                                        },
                                    },
                                    "input": {
                                        "preallocated_email": "{{mailbox.email}}",
                                        "preallocated_session_id": "{{mailbox.session_id}}",
                                        "preallocated_mailbox_ref": "{{mailbox.mailbox_ref}}",
                                        "proxy_url": "{{proxy_chain.proxy_url}}",
                                    },
                                    "saveAs": "create_openai_account",
                                },
                            ]
                        }
                    }
                ),
                encoding="utf-8",
            )

            mailbox_call_count = 0
            proxy_call_count = 0
            create_inputs: list[tuple[str, str]] = []
            mailbox_inputs: list[dict[str, object]] = []
            released_mailboxes: list[tuple[str, str]] = []
            released_proxies: list[tuple[str, str]] = []

            def _easyemail_dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                nonlocal mailbox_call_count
                if step_type == "acquire_mailbox":
                    mailbox_inputs.append(dict(step_input))
                    mailbox_call_count += 1
                    return {
                        "ok": True,
                        "provider": "slowmail" if mailbox_call_count == 1 else "cloudflare_temp_email",
                        "email": "first@example.invalid" if mailbox_call_count == 1 else "second@example.net",
                        "mailbox_ref": f"mailbox-ref-{mailbox_call_count}",
                        "session_id": f"mailbox-session-{mailbox_call_count}",
                        "business_key": "openai",
                    }
                if step_type == "release_mailbox":
                    released_mailboxes.append(
                        (
                            str(step_input.get("mailbox_ref") or ""),
                            str(step_input.get("mailbox_session_id") or ""),
                        )
                    )
                    return {"released": True, "detail": "deleted"}
                raise AssertionError(step_type)

            def _easyproxy_dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                nonlocal proxy_call_count
                if step_type == "acquire_proxy_chain":
                    proxy_call_count += 1
                    return {
                        "ok": True,
                        "proxy_url": f"http://proxy-{proxy_call_count}",
                        "lease_id": f"lease-{proxy_call_count}",
                    }
                if step_type == "release_proxy_chain":
                    released_proxies.append(
                        (
                            str(step_input.get("proxy_url") or ""),
                            str(step_input.get("lease_id") or ""),
                        )
                    )
                    return {"released": True, "detail": "released"}
                raise AssertionError(step_type)

            def _easyprotocol_dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                if step_type != "create_openai_account":
                    raise AssertionError(step_type)
                create_inputs.append(
                    (
                        str(step_input.get("preallocated_email") or ""),
                        str(step_input.get("proxy_url") or ""),
                    )
                )
                if len(create_inputs) == 1:
                    raise ProtocolRuntimeError(
                        "chatgpt_login_email_otp_wait_failed: timeout waiting for 6-digit code",
                        code=ErrorCodes.OTP_TIMEOUT,
                    )
                return {
                    "ok": True,
                    "status": "completed",
                    "storage_path": "/tmp/create-success.json",
                }

            with mock.patch.dict(
                dst_flow.OWNER_DISPATCHERS,
                {
                    "easyemail": _easyemail_dispatcher,
                    "easyproxy": _easyproxy_dispatcher,
                    "easyprotocol": _easyprotocol_dispatcher,
                },
                clear=True,
            ):
                result = dst_flow.run_dst_flow_once(
                    output_dir=str(Path(tmp_dir) / "out"),
                    flow_path=flow_path,
                )

        self.assertTrue(result.ok)
        self.assertEqual(2, result.step_attempts["create-openai-account"])
        self.assertEqual(2, result.step_attempts["acquire-mailbox"])
        self.assertEqual(2, result.step_attempts["acquire-proxy-chain"])
        self.assertEqual(
            [
                ("first@example.invalid", "http://proxy-1"),
                ("second@example.net", "http://proxy-2"),
            ],
            create_inputs,
        )
        self.assertEqual("", mailbox_inputs[0]["avoid_emails"])
        self.assertEqual(["first@example.invalid"], mailbox_inputs[1]["avoid_emails"])
        self.assertEqual(["example.invalid"], mailbox_inputs[1]["avoid_domains"])
        self.assertEqual(["slowmail"], mailbox_inputs[1]["avoid_providers"])
        self.assertEqual("email_otp_timeout", mailbox_inputs[1]["avoid_reason"])
        self.assertEqual(
            [
                {
                    "outcome": "failure",
                    "failureReason": "email_otp_timeout",
                    "failureClass": "weak_attributed_email_otp_timeout",
                    "errorCode": "otp_timeout",
                    "provider": "slowmail",
                    "domain": "example.invalid",
                    "email": "first@example.invalid",
                    "mailbox_ref": "mailbox-ref-1",
                    "mailbox_session_id": "mailbox-session-1",
                    "business_key": "openai",
                    "stepId": "create-openai-account",
                    "attempt": 1,
                }
            ],
            result.outputs["mailbox-attempt-outcomes"],
        )
        self.assertEqual([("mailbox-ref-1", "mailbox-session-1")], released_mailboxes)
        self.assertEqual([("http://proxy-1", "lease-1")], released_proxies)

    def test_run_dst_flow_once_retries_create_account_after_attributed_unsupported_email(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            flow_path = Path(tmp_dir) / "temp-flow.json"
            flow_path.write_text(
                json.dumps(
                    {
                        "definition": {
                            "platform": "chatgpt",
                            "steps": [
                                {
                                    "id": "acquire-mailbox",
                                    "type": "acquire_mailbox",
                                    "metadata": {"owner": "easyemail"},
                                    "input": {
                                        "business_key": "{{task.mailbox_business_key}}",
                                        "avoid_emails": "{{task.avoidMailboxEmails}}",
                                        "avoid_domains": "{{task.avoidMailboxDomains}}",
                                        "avoid_providers": "{{task.avoidMailboxProviders}}",
                                        "avoid_reason": "{{task.avoidMailboxReason}}",
                                    },
                                    "saveAs": "mailbox",
                                },
                                {
                                    "id": "acquire-proxy-chain",
                                    "type": "acquire_proxy_chain",
                                    "metadata": {"owner": "easyproxy"},
                                    "saveAs": "proxy_chain",
                                },
                                {
                                    "id": "create-openai-account",
                                    "type": "create_openai_account",
                                    "metadata": {
                                        "owner": "easyprotocol",
                                        "retry": {
                                            "maxAttempts": 2,
                                            "retryProfile": "step-create-account-recover",
                                            "refreshSavedStates": ["mailbox", "proxy_chain"],
                                        },
                                    },
                                    "input": {
                                        "preallocated_email": "{{mailbox.email}}",
                                        "preallocated_session_id": "{{mailbox.session_id}}",
                                        "preallocated_mailbox_ref": "{{mailbox.mailbox_ref}}",
                                        "proxy_url": "{{proxy_chain.proxy_url}}",
                                    },
                                    "saveAs": "create_openai_account",
                                },
                            ]
                        }
                    }
                ),
                encoding="utf-8",
            )

            mailbox_call_count = 0
            proxy_call_count = 0
            create_inputs: list[tuple[str, str]] = []
            mailbox_inputs: list[dict[str, object]] = []
            released_mailboxes: list[tuple[str, str]] = []

            def _easyemail_dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                nonlocal mailbox_call_count
                if step_type == "acquire_mailbox":
                    mailbox_inputs.append(dict(step_input))
                    mailbox_call_count += 1
                    return {
                        "ok": True,
                        "provider": "etempmail" if mailbox_call_count == 1 else "cloudflare_temp_email",
                        "email": f"user{mailbox_call_count}@example.com",
                        "mailbox_ref": f"mailbox-ref-{mailbox_call_count}",
                        "session_id": f"mailbox-session-{mailbox_call_count}",
                    }
                if step_type == "release_mailbox":
                    released_mailboxes.append(
                        (
                            str(step_input.get("mailbox_ref") or ""),
                            str(step_input.get("mailbox_session_id") or ""),
                        )
                    )
                    return {"released": True, "detail": "deleted"}
                raise AssertionError(step_type)

            def _easyproxy_dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                nonlocal proxy_call_count
                if step_type == "acquire_proxy_chain":
                    proxy_call_count += 1
                    return {
                        "ok": True,
                        "proxy_url": f"http://proxy-{proxy_call_count}",
                        "lease_id": f"lease-{proxy_call_count}",
                    }
                if step_type == "release_proxy_chain":
                    return {"released": True, "detail": "released"}
                raise AssertionError(step_type)

            def _easyprotocol_dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                if step_type != "create_openai_account":
                    raise AssertionError(step_type)
                create_inputs.append(
                    (
                        str(step_input.get("preallocated_email") or ""),
                        str(step_input.get("proxy_url") or ""),
                    )
                )
                if len(create_inputs) == 1:
                    raise ProtocolRuntimeError(
                        (
                            "create_account status=400 body={\"error\":{\"code\":\"unsupported_email\","
                            "\"message\":\"The email you provided is not supported.\"}} "
                            "[mailbox_provider=etempmail email=user1@example.com]"
                        ),
                        code=ErrorCodes.INVALID_REQUEST_ERROR,
                    )
                return {
                    "ok": True,
                    "status": "completed",
                    "storage_path": "/tmp/create-success.json",
                }

            with mock.patch.dict(
                dst_flow.OWNER_DISPATCHERS,
                {
                    "easyemail": _easyemail_dispatcher,
                    "easyproxy": _easyproxy_dispatcher,
                    "easyprotocol": _easyprotocol_dispatcher,
                },
                clear=True,
            ):
                result = dst_flow.run_dst_flow_once(
                    output_dir=str(Path(tmp_dir) / "out"),
                    flow_path=flow_path,
                )

        self.assertTrue(result.ok)
        self.assertEqual(2, result.step_attempts["create-openai-account"])
        self.assertEqual(
            [("user1@example.com", "http://proxy-1"), ("user2@example.com", "http://proxy-2")],
            create_inputs,
        )
        self.assertEqual(["user1@example.com"], mailbox_inputs[1]["avoid_emails"])
        self.assertEqual(["example.com"], mailbox_inputs[1]["avoid_domains"])
        self.assertEqual("", mailbox_inputs[1]["avoid_providers"])
        self.assertEqual("unsupported_email", mailbox_inputs[1]["avoid_reason"])
        self.assertEqual([("mailbox-ref-1", "mailbox-session-1")], released_mailboxes)

    def test_run_dst_flow_once_refresh_proxy_release_keeps_full_proxy_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            flow_path = Path(tmp_dir) / "temp-flow.json"
            flow_path.write_text(
                json.dumps(
                    {
                        "definition": {
                            "platform": "chatgpt",
                            "steps": [
                                {
                                    "id": "acquire-proxy-chain",
                                    "type": "acquire_proxy_chain",
                                    "metadata": {"owner": "easyproxy"},
                                    "saveAs": "proxy_chain",
                                },
                                {
                                    "id": "create-openai-account",
                                    "type": "create_openai_account",
                                    "metadata": {
                                        "owner": "easyprotocol",
                                        "retry": {
                                            "maxAttempts": 2,
                                            "backoffSeconds": 2,
                                            "retryProfile": "step-proxy-refresh",
                                            "refreshSavedStates": ["proxy_chain"],
                                        },
                                    },
                                    "input": {
                                        "proxy_url": "{{proxy_chain.proxy_url}}",
                                    },
                                    "saveAs": "create_openai_account",
                                },
                            ]
                        }
                    }
                ),
                encoding="utf-8",
            )

            released_payloads: list[dict[str, object]] = []
            proxy_call_count = 0

            def _easyproxy_dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                nonlocal proxy_call_count
                if step_type == "acquire_proxy_chain":
                    proxy_call_count += 1
                    return {
                        "ok": True,
                        "proxy_url": f"http://proxy-{proxy_call_count}",
                        "raw_proxy_url": f"http://127.0.0.1:{25000 + proxy_call_count}",
                        "lease_id": f"lease-{proxy_call_count}",
                        "unique_key": f"http://proxy-{proxy_call_count}",
                        "checked_out": True,
                    }
                if step_type == "release_proxy_chain":
                    released_payloads.append(dict(step_input))
                    return {"released": True, "detail": "released"}
                raise AssertionError(step_type)

            def _easyprotocol_dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                if step_type != "create_openai_account":
                    raise AssertionError(step_type)
                if proxy_call_count == 1:
                    raise RuntimeError("Failed to perform, curl: (7) Connection closed abruptly.")
                return {
                    "ok": True,
                    "status": "completed",
                    "storage_path": "/tmp/create-success.json",
                }

            with mock.patch.dict(
                dst_flow.OWNER_DISPATCHERS,
                {
                    "easyproxy": _easyproxy_dispatcher,
                    "easyprotocol": _easyprotocol_dispatcher,
                },
                clear=True,
            ):
                result = dst_flow.run_dst_flow_once(
                    output_dir=str(Path(tmp_dir) / "out"),
                    flow_path=flow_path,
                )

        self.assertTrue(result.ok)
        self.assertEqual(1, len(released_payloads))
        self.assertIn("proxy_chain", released_payloads[0])
        self.assertEqual(
            {
                "ok": True,
                "proxy_url": "http://proxy-1",
                "raw_proxy_url": "http://127.0.0.1:25001",
                "lease_id": "lease-1",
                "unique_key": "http://proxy-1",
                "checked_out": True,
            },
            released_payloads[0]["proxy_chain"],
        )

    def test_run_dst_flow_once_retries_chatgpt_login_after_chat_requirements_401(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            flow_path = Path(tmp_dir) / "temp-flow.json"
            flow_path.write_text(
                json.dumps(
                    {
                        "definition": {
                            "platform": "chatgpt",
                            "steps": [
                                {
                                    "id": "acquire-proxy-chain",
                                    "type": "acquire_proxy_chain",
                                    "metadata": {"owner": "easyprotocol"},
                                    "saveAs": "proxy_chain",
                                },
                                {
                                    "id": "initialize-chatgpt-login-session",
                                    "type": "initialize_chatgpt_login_session",
                                    "metadata": {
                                        "owner": "easyprotocol",
                                        "retry": {
                                            "maxAttempts": 2,
                                            "retryProfile": "step-login-init-recover",
                                            "refreshSavedStates": [
                                                "proxy_chain"
                                            ],
                                        },
                                    },
                                    "input": {
                                        "proxy_url": "{{proxy_chain.proxy_url}}",
                                    },
                                    "saveAs": "initialize_chatgpt_login_session",
                                },
                            ]
                        }
                    }
                ),
                encoding="utf-8",
            )

            proxy_call_count = 0
            login_proxy_urls: list[str] = []

            def _dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                nonlocal proxy_call_count
                if step_type == "acquire_proxy_chain":
                    proxy_call_count += 1
                    return {"ok": True, "proxy_url": f"http://proxy-{proxy_call_count}"}
                if step_type == "initialize_chatgpt_login_session":
                    login_proxy_urls.append(str(step_input.get("proxy_url") or ""))
                    if len(login_proxy_urls) == 1:
                        raise RuntimeError('chat_requirements_failed status=401 body={"detail":"Unauthorized"}')
                    return {
                        "ok": True,
                        "status": "completed",
                        "workspaceId": "ws_personal",
                    }
                raise AssertionError(step_type)

            with mock.patch.dict(dst_flow.OWNER_DISPATCHERS, {"easyprotocol": _dispatcher}, clear=True):
                result = dst_flow.run_dst_flow_once(
                    output_dir=str(Path(tmp_dir) / "out"),
                    flow_path=flow_path,
                )

        self.assertTrue(result.ok)
        self.assertEqual(2, result.step_attempts["initialize-chatgpt-login-session"])
        self.assertEqual(["http://proxy-1", "http://proxy-2"], login_proxy_urls)
        self.assertEqual(2, proxy_call_count)
