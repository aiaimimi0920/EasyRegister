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


class DstFlowIntegrationTests(unittest.TestCase):
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
