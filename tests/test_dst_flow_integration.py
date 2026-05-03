from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SRC_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

import dst_flow  # noqa: E402
from others import easyemail_runtime  # noqa: E402


class DstFlowIntegrationTests(unittest.TestCase):
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
            released_mailboxes: list[tuple[str, str]] = []
            released_proxies: list[tuple[str, str]] = []

            def _easyemail_dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                nonlocal mailbox_call_count
                if step_type == "acquire_mailbox":
                    mailbox_call_count += 1
                    return {
                        "ok": True,
                        "provider": "moemail",
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
                ("user1@example.com", "http://proxy-1"),
                ("user2@example.com", "http://proxy-2"),
            ],
            create_inputs,
        )
        self.assertEqual([("mailbox-ref-1", "mailbox-session-1")], released_mailboxes)
        self.assertEqual([("http://proxy-1", "lease-1")], released_proxies)

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
