from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, timezone
import os
import sys
import tempfile
import unittest
import json
from types import SimpleNamespace
from pathlib import Path
from unittest import mock


SRC_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "src"
PYTHON_SHARED_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "python_shared" / "src"
for candidate in (SRC_ROOT, PYTHON_SHARED_ROOT):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from errors import ErrorCodes, classify_error_code  # noqa: E402
from others.config import RunnerFlowSpec, RunnerMainConfig  # noqa: E402
from others import runner_artifacts, runner_credential_sync, runner_failures, runner_flow_scheduler, runner_mailbox, runner_process_supervisor, runner_team_artifacts, runner_team_auth, runner_team_auth_pool, runner_team_cleanup, runner_worker_loop, runner_worker_maintenance, runner_worker_results, storage  # noqa: E402


class RunnerArtifactsTests(unittest.TestCase):
    def test_legacy_storage_writers_preserve_recovery_data_credential(self) -> None:
        recovery_data = {
            "emailAddress": "seed@example.com",
            "providerTypeKey": "cloudflare_temp_email",
            "providerInstanceId": "cloudflare_temp_email_shared_default",
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            first_phone_path = Path(
                storage.persist_first_phone_record(
                    output_dir=tmp_dir,
                    email="seed@example.com",
                    password="secret",
                    mailbox_provider="cloudflare_temp_email",
                    mailbox_access_key="",
                    mailbox_ref="cloudflare_temp_email:old-ref",
                    mailbox_session_id="old-session",
                    first_name="Seed",
                    last_name="User",
                    birthdate="1990-01-01",
                    page_type="phone_wall",
                    final_url="https://platform.openai.com/",
                    recovery_data_credential=recovery_data,
                )
            )
            oauth_path = Path(
                storage.persist_openai_oauth_record(
                    output_dir=tmp_dir,
                    email="seed@example.com",
                    password="secret",
                    mailbox_provider="cloudflare_temp_email",
                    mailbox_access_key="",
                    mailbox_ref="cloudflare_temp_email:old-ref",
                    mailbox_session_id="old-session",
                    first_name="Seed",
                    last_name="User",
                    birthdate="1990-01-01",
                    page_type="oauth",
                    final_url="https://platform.openai.com/",
                    recovery_data_credential=recovery_data,
                )
            )

            self.assertEqual(
                recovery_data,
                json.loads(first_phone_path.read_text(encoding="utf-8"))["recoveryDataCredential"],
            )
            self.assertEqual(
                recovery_data,
                json.loads(oauth_path.read_text(encoding="utf-8"))["recoveryDataCredential"],
            )

    def test_select_local_split_obeys_percentage(self) -> None:
        with mock.patch("others.runner_artifacts.random.random", return_value=0.20):
            self.assertTrue(runner_artifacts.select_local_split(percent=50.0))
        with mock.patch("others.runner_artifacts.random.random", return_value=0.80):
            self.assertFalse(runner_artifacts.select_local_split(percent=50.0))

    def test_openai_oauth_failure_target_pool_dir_routes_failed_once_for_main(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            with mock.patch.dict(os.environ, {"REGISTER_OUTPUT_ROOT": str(output_root)}, clear=True):
                target = runner_artifacts.openai_oauth_failure_target_pool_dir(
                    output_root=output_root,
                    result_payload_value={"errorCode": "free_personal_workspace_missing", "instanceRole": "main"},
                )
        self.assertEqual((output_root / "openai" / "failed-once").resolve(), target)

    def test_openai_oauth_failure_target_pool_dir_routes_manual_oauth_pool(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "REGISTER_FREE_MANUAL_OAUTH_PRESERVE_ENABLED": "true",
                    "REGISTER_FREE_MANUAL_OAUTH_PRESERVE_ERROR_CODES": "token_invalidated",
                },
                clear=True,
            ):
                target = runner_artifacts.openai_oauth_failure_target_pool_dir(
                    output_root=output_root,
                    result_payload_value={"errorCode": "token_invalidated"},
                )
        self.assertEqual((output_root / "others" / "free-manual-oauth-pool").resolve(), target)

    def test_openai_oauth_failure_target_pool_dir_routes_failed_twice_for_continue(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            target = runner_artifacts.openai_oauth_failure_target_pool_dir(
                output_root=output_root,
                result_payload_value={
                    "instanceRole": "continue",
                    "errorStep": "obtain-codex-oauth",
                    "error": "phone_wall context=repair_otp_validate page_type=add_phone",
                    "stepErrors": {
                        "obtain-codex-oauth": {
                            "message": "phone_wall context=repair_otp_validate page_type=add_phone",
                            "detail": "page_type=add_phone",
                        }
                    },
                },
            )
        self.assertEqual((output_root / "openai" / "failed-twice").resolve(), target)

    def test_postprocess_free_success_artifact_can_materialize_from_oauth_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            openai_dir = output_root / "openai_oauth"
            openai_dir.mkdir(parents=True, exist_ok=True)
            seed_path = openai_dir / "seed.json"
            seed_path.write_text(
                json.dumps(
                    {
                        "email": "materialized@example.com",
                        "mailboxRef": "mailbox-ref",
                        "mailboxSessionId": "session-id",
                        "createdAt": "2026-05-01T00:00:00Z",
                        "platformOrganization": {"status": "completed"},
                        "chatgptLogin": {"status": "completed", "workspaceId": "ws_123"},
                        "chatgptLoginDetails": {"clientBootstrap": {"authStatus": "logged_in", "structure": "personal"}},
                    }
                ),
                encoding="utf-8",
            )
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "REGISTER_FREE_LOCAL_DIR": str(output_root / "codex" / "free"),
                    "REGISTER_OPENAI_OAUTH_SEED_MAX_AGE_SECONDS": "0",
                },
                clear=True,
            ):
                result = SimpleNamespace(
                    ok=True,
                    to_dict=lambda: {
                        "steps": {
                            "validate-free-personal-oauth": "ok",
                        },
                        "outputs": {
                            "create-openai-account": {
                                "storage_path": str(seed_path),
                            },
                            "obtain-codex-oauth": {
                                "email": "materialized@example.com",
                                "access_token": "token",
                                "refresh_token": "refresh",
                                "auth": {
                                    "account_id": "org-abcdef12-rest",
                                },
                            }
                        },
                    },
                )
                postprocess = runner_artifacts.postprocess_free_success_artifact(
                    result=result,
                    output_root=output_root,
                    worker_label="worker-01",
                    task_index=1,
                    free_local_selected=True,
                )
                self.assertTrue(postprocess["ok"])
                self.assertEqual("stored_local", postprocess["status"])
                stored_path = Path(str(postprocess["stored_path"]))
                self.assertTrue(stored_path.is_file())
                self.assertEqual("codex-free-org-materialized@example.com.json", stored_path.name)
                self.assertEqual(
                    [],
                    list((output_root / "codex" / "free" / "_materialized").glob("*.json")),
                )

    def test_postprocess_free_success_artifact_cleans_materialized_temp_on_missing_openai_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "REGISTER_FREE_LOCAL_DIR": str(output_root / "codex" / "free"),
                    "REGISTER_OPENAI_OAUTH_SEED_MAX_AGE_SECONDS": "0",
                },
                clear=True,
            ):
                result = SimpleNamespace(
                    ok=True,
                    to_dict=lambda: {
                        "steps": {
                            "validate-free-personal-oauth": "ok",
                        },
                        "outputs": {
                            "obtain-codex-oauth": {
                                "email": "missing-source@example.com",
                                "access_token": "token",
                                "refresh_token": "refresh",
                                "auth": {
                                    "account_id": "org-abcdef12-rest",
                                },
                            }
                        },
                    },
                )
                postprocess = runner_artifacts.postprocess_free_success_artifact(
                    result=result,
                    output_root=output_root,
                    worker_label="worker-01",
                    task_index=2,
                    free_local_selected=True,
                )

            self.assertFalse(postprocess["ok"])
            self.assertEqual("missing_free_artifact", postprocess["status"])
            self.assertEqual(
                [],
                list((output_root / "codex" / "free" / "_materialized").glob("*.json")),
            )

    def test_postprocess_free_success_artifact_uses_finalized_restored_openai_source_for_continue_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            converted_dir = output_root / "openai" / "converted"
            converted_dir.mkdir(parents=True, exist_ok=True)
            restored_openai_path = converted_dir / "small-success.json"
            restored_openai_path.write_text(
                json.dumps(
                    {
                        "email": "continue@example.com",
                        "mailboxRef": "mailbox-ref",
                        "mailboxSessionId": "session-id",
                        "createdAt": "2026-05-01T00:00:00Z",
                        "platformOrganization": {"status": "completed"},
                        "chatgptLogin": {"status": "completed", "workspaceId": "ws_123"},
                        "chatgptLoginDetails": {"clientBootstrap": {"authStatus": "logged_in", "structure": "personal"}},
                    }
                ),
                encoding="utf-8",
            )
            success_dir = output_root / "others" / "mixed-runs" / "worker-01" / "run-1" / "success"
            success_dir.mkdir(parents=True, exist_ok=True)
            codex_success_path = success_dir / "codex-success.json"
            codex_success_path.write_text(
                json.dumps(
                    {
                        "email": "continue@example.com",
                        "type": "codex",
                        "account_id": "d2cca0be-c722-4bb5-9b00-a4af91e20687",
                        "refresh_token": "refresh",
                        "access_token": "access",
                    }
                ),
                encoding="utf-8",
            )
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "REGISTER_FREE_LOCAL_DIR": str(output_root / "codex" / "free"),
                },
                clear=True,
            ):
                result = SimpleNamespace(
                    ok=True,
                    to_dict=lambda: {
                        "steps": {
                            "obtain-codex-oauth": "ok",
                            "validate-free-personal-oauth": "ok",
                        },
                        "outputs": {
                            "acquire-openai-oauth-artifact": {
                                "source_path": str(output_root / "others" / "openai-oauth-claims" / "missing.json"),
                                "claimed_path": str(output_root / "others" / "openai-oauth-claims" / "missing.json"),
                            },
                            "finalize-openai-oauth-artifact": {
                                "restored_path": str(restored_openai_path),
                            },
                            "obtain-codex-oauth": {
                                "successPath": str(codex_success_path),
                                "email": "continue@example.com",
                                "auth": {
                                    "account_id": "org-abcdef12-rest",
                                },
                            },
                        },
                    },
                )
                postprocess = runner_artifacts.postprocess_free_success_artifact(
                    result=result,
                    output_root=output_root,
                    worker_label="worker-continue",
                    task_index=22,
                    free_local_selected=True,
                )
                self.assertTrue(postprocess["ok"])
                self.assertEqual("stored_local", postprocess["status"])
                stored_path = Path(str(postprocess["stored_path"]))
                self.assertTrue(stored_path.is_file())
                self.assertEqual("codex-free-d2cca0be-continue@example.com.json", stored_path.name)

    def test_copy_openai_oauth_artifacts_to_pool_collects_legacy_small_success_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            run_output_dir = Path(tmp_dir) / "run-1"
            legacy_dir = run_output_dir / "small_success"
            pool_dir = Path(tmp_dir) / "openai" / "failed-once"
            legacy_dir.mkdir(parents=True, exist_ok=True)
            payload_path = legacy_dir / "small-legacy.json"
            payload_path.write_text(
                json.dumps(
                    {
                        "email": "legacy@example.com",
                        "mailboxRef": "mailbox-ref",
                        "mailboxSessionId": "session-id",
                        "createdAt": "2026-05-01T00:00:00Z",
                        "platformOrganization": {"status": "completed"},
                        "chatgptLogin": {"status": "completed", "workspaceId": "ws_123"},
                        "chatgptLoginDetails": {"clientBootstrap": {"authStatus": "logged_in", "structure": "personal"}},
                    }
                ),
                encoding="utf-8",
            )
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OPENAI_OAUTH_SEED_MAX_AGE_SECONDS": "0",
                },
                clear=False,
            ):
                copied_paths = runner_artifacts.copy_openai_oauth_artifacts_to_pool(
                    run_output_dir=run_output_dir,
                    pool_dir=pool_dir,
                    worker_label="worker-01",
                    task_index=1,
                )

            self.assertEqual(1, len(copied_paths))
            copied_path = Path(copied_paths[0])
            self.assertTrue(copied_path.is_file())
            self.assertEqual("small-legacy.json", copied_path.name)

    def test_copy_openai_oauth_artifacts_to_pool_removes_valid_protocol_bridge_source_after_copy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            run_output_dir = Path(tmp_dir) / "run-1"
            bridge_dir = Path(tmp_dir) / "easyregister-bridge"
            pool_dir = Path(tmp_dir) / "openai" / "failed-once"
            bridge_dir.mkdir(parents=True, exist_ok=True)
            payload_path = bridge_dir / "small-bridge-valid.json"
            payload_path.write_text(
                json.dumps(
                    {
                        "email": "bridge-valid@example.com",
                        "mailboxRef": "mailbox-ref",
                        "mailboxSessionId": "session-id",
                        "createdAt": "2026-05-01T00:00:00Z",
                        "platformOrganization": {"status": "completed"},
                        "chatgptLogin": {"status": "completed", "workspaceId": "ws_123"},
                        "chatgptLoginDetails": {
                            "clientBootstrap": {"authStatus": "logged_in", "structure": "personal"},
                        },
                    }
                ),
                encoding="utf-8",
            )
            result_payload = {
                "outputs": {
                    "create-openai-account": {
                        "storage_path": str(payload_path),
                    }
                }
            }

            with mock.patch.dict(
                os.environ,
                {"REGISTER_PROTOCOL_BRIDGE_DIR": str(bridge_dir)},
                clear=False,
            ):
                copied_paths = runner_artifacts.copy_openai_oauth_artifacts_to_pool(
                    run_output_dir=run_output_dir,
                    pool_dir=pool_dir,
                    worker_label="worker-01",
                    task_index=1,
                    result_or_payload=result_payload,
                )

            self.assertEqual(1, len(copied_paths))
            self.assertTrue(Path(copied_paths[0]).is_file())
            self.assertFalse(payload_path.exists())

    def test_copy_openai_oauth_artifacts_to_pool_removes_invalid_protocol_bridge_source_after_discard(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            run_output_dir = Path(tmp_dir) / "run-1"
            bridge_dir = Path(tmp_dir) / "easyregister-bridge"
            pool_dir = Path(tmp_dir) / "openai" / "failed-once"
            bridge_dir.mkdir(parents=True, exist_ok=True)
            payload_path = bridge_dir / "small-bridge-raw.json"
            payload_path.write_text(
                json.dumps(
                    {
                        "email": "bridge-raw@example.com",
                        "mailboxRef": "mailbox-ref",
                        "mailboxSessionId": "session-id",
                        "createdAt": "2026-05-01T00:00:00Z",
                    }
                ),
                encoding="utf-8",
            )
            result_payload = {
                "outputs": {
                    "create-openai-account": {
                        "storage_path": str(payload_path),
                    }
                }
            }

            with mock.patch.dict(
                os.environ,
                {"REGISTER_PROTOCOL_BRIDGE_DIR": str(bridge_dir)},
                clear=False,
            ):
                copied_paths = runner_artifacts.copy_openai_oauth_artifacts_to_pool(
                    run_output_dir=run_output_dir,
                    pool_dir=pool_dir,
                    worker_label="worker-01",
                    task_index=1,
                    result_or_payload=result_payload,
                )

            self.assertEqual([], copied_paths)
            self.assertFalse(payload_path.exists())

    def test_copy_openai_oauth_artifacts_to_pool_removes_invalid_target_bridge_source_after_discard(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            run_output_dir = Path(tmp_dir) / "run-1"
            bridge_dir = Path(tmp_dir) / "protocol-register-output" / "easyregister-bridge"
            pool_dir = Path(tmp_dir) / "openai" / "failed-once"
            bridge_dir.mkdir(parents=True, exist_ok=True)
            payload_path = bridge_dir / "small-target-bridge-raw.json"
            payload_path.write_text(
                json.dumps(
                    {
                        "email": "bridge-raw@example.com",
                        "mailboxRef": "mailbox-ref",
                        "mailboxSessionId": "session-id",
                        "createdAt": "2026-05-01T00:00:00Z",
                    }
                ),
                encoding="utf-8",
            )
            target_bridge_dir = "/shared/register-output/easyregister-bridge"
            result_payload = {
                "outputs": {
                    "obtain-codex-oauth": {
                        "successPath": f"{target_bridge_dir}/{payload_path.name}",
                    }
                }
            }

            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_PROTOCOL_BRIDGE_DIR": str(bridge_dir),
                    "REGISTER_PROTOCOL_BRIDGE_TARGET_DIR": target_bridge_dir,
                    "REGISTER_PROTOCOL_OUTPUT_TARGET_DIR": "/shared/register-output",
                    "REGISTER_PROTOCOL_OUTPUT_MIRROR_DIR": str(Path(tmp_dir) / "protocol-register-output"),
                },
                clear=False,
            ):
                copied_paths = runner_artifacts.copy_openai_oauth_artifacts_to_pool(
                    run_output_dir=run_output_dir,
                    pool_dir=pool_dir,
                    worker_label="worker-01",
                    task_index=1,
                    result_or_payload=result_payload,
                )

            self.assertEqual([], copied_paths)
            self.assertFalse(payload_path.exists())

    def test_copy_openai_oauth_artifacts_to_pool_removes_valid_target_bridge_source_after_copy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            run_output_dir = Path(tmp_dir) / "run-1"
            bridge_dir = Path(tmp_dir) / "protocol-register-output" / "easyregister-bridge"
            pool_dir = Path(tmp_dir) / "openai" / "failed-once"
            bridge_dir.mkdir(parents=True, exist_ok=True)
            payload_path = bridge_dir / "small-target-bridge-valid.json"
            payload_path.write_text(
                json.dumps(
                    {
                        "email": "bridge-valid@example.com",
                        "mailboxRef": "mailbox-ref",
                        "mailboxSessionId": "session-id",
                        "createdAt": "2026-05-01T00:00:00Z",
                        "platformOrganization": {"status": "completed"},
                        "chatgptLogin": {"status": "completed", "workspaceId": "ws_123"},
                        "chatgptLoginDetails": {
                            "clientBootstrap": {"authStatus": "logged_in", "structure": "personal"},
                        },
                    }
                ),
                encoding="utf-8",
            )
            target_bridge_dir = "/shared/register-output/easyregister-bridge"
            result_payload = {
                "outputs": {
                    "obtain-codex-oauth": {
                        "successPath": f"{target_bridge_dir}/{payload_path.name}",
                    }
                }
            }

            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_PROTOCOL_BRIDGE_DIR": str(bridge_dir),
                    "REGISTER_PROTOCOL_BRIDGE_TARGET_DIR": target_bridge_dir,
                    "REGISTER_PROTOCOL_OUTPUT_TARGET_DIR": "/shared/register-output",
                    "REGISTER_PROTOCOL_OUTPUT_MIRROR_DIR": str(Path(tmp_dir) / "protocol-register-output"),
                },
                clear=False,
            ):
                copied_paths = runner_artifacts.copy_openai_oauth_artifacts_to_pool(
                    run_output_dir=run_output_dir,
                    pool_dir=pool_dir,
                    worker_label="worker-01",
                    task_index=1,
                    result_or_payload=result_payload,
                )

            self.assertEqual(1, len(copied_paths))
            self.assertTrue(Path(copied_paths[0]).is_file())
            self.assertEqual("small-target-bridge-valid.json", Path(copied_paths[0]).name)
            self.assertFalse(payload_path.exists())

    def test_copy_openai_oauth_artifacts_to_pool_prefers_valid_bridge_sibling_over_partial_run_copy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            run_output_dir = Path(tmp_dir) / "run-1"
            run_small_dir = run_output_dir / "small_success"
            bridge_dir = Path(tmp_dir) / "protocol-register-output" / "easyregister-bridge"
            pool_dir = Path(tmp_dir) / "openai" / "failed-once"
            run_small_dir.mkdir(parents=True, exist_ok=True)
            bridge_dir.mkdir(parents=True, exist_ok=True)
            artifact_name = "small-bridge-sibling-valid.json"
            partial_run_path = run_small_dir / artifact_name
            bridge_path = bridge_dir / artifact_name
            partial_run_path.write_text(
                json.dumps(
                    {
                        "email": "bridge-sibling@example.com",
                        "mailboxRef": "mailbox-ref",
                        "mailboxSessionId": "session-id",
                        "createdAt": "2026-05-01T00:00:00Z",
                    }
                ),
                encoding="utf-8",
            )
            bridge_path.write_text(
                json.dumps(
                    {
                        "email": "bridge-sibling@example.com",
                        "mailboxRef": "mailbox-ref",
                        "mailboxSessionId": "session-id",
                        "createdAt": "2026-05-01T00:00:00Z",
                        "platformOrganization": {"status": "completed"},
                        "chatgptLogin": {"status": "completed", "workspaceId": "ws_123"},
                        "chatgptLoginDetails": {
                            "clientBootstrap": {"authStatus": "logged_in", "structure": "personal"},
                        },
                    }
                ),
                encoding="utf-8",
            )

            with mock.patch.dict(
                os.environ,
                {"REGISTER_PROTOCOL_BRIDGE_DIR": str(bridge_dir)},
                clear=False,
            ):
                copied_paths = runner_artifacts.copy_openai_oauth_artifacts_to_pool(
                    run_output_dir=run_output_dir,
                    pool_dir=pool_dir,
                    worker_label="worker-01",
                    task_index=1,
                )

            self.assertEqual(1, len(copied_paths))
            copied_path = Path(copied_paths[0])
            self.assertTrue(copied_path.is_file())
            self.assertEqual(artifact_name, copied_path.name)
            self.assertFalse(bridge_path.exists())
            copied_payload = json.loads(copied_path.read_text(encoding="utf-8"))
            self.assertEqual("completed", copied_payload["platformOrganization"]["status"])

    def test_copy_openai_oauth_artifacts_to_pool_ignores_seed_age_during_promotion(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            run_output_dir = Path(tmp_dir) / "run-1"
            legacy_dir = run_output_dir / "small_success"
            pool_dir = Path(tmp_dir) / "openai" / "failed-once"
            legacy_dir.mkdir(parents=True, exist_ok=True)
            payload_path = legacy_dir / "small-old-but-valid.json"
            payload_path.write_text(
                json.dumps(
                    {
                        "email": "legacy@example.com",
                        "mailboxRef": "mailbox-ref",
                        "mailboxSessionId": "session-id",
                        "createdAt": "2000-01-01T00:00:00Z",
                        "platformOrganization": {"status": "completed"},
                        "chatgptLogin": {"status": "completed", "workspaceId": "ws_123"},
                        "chatgptLoginDetails": {
                            "clientBootstrap": {"authStatus": "logged_in", "structure": "personal"},
                        },
                    }
                ),
                encoding="utf-8",
            )
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OPENAI_OAUTH_SEED_MAX_AGE_SECONDS": "5",
                },
                clear=False,
            ):
                copied_paths = runner_artifacts.copy_openai_oauth_artifacts_to_pool(
                    run_output_dir=run_output_dir,
                    pool_dir=pool_dir,
                    worker_label="worker-01",
                    task_index=1,
                )

            self.assertEqual(1, len(copied_paths))
            self.assertEqual("small-old-but-valid.json", Path(copied_paths[0]).name)

    def test_copy_openai_oauth_artifacts_to_pool_preserves_chatgpt_web_refresh_material(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            run_output_dir = Path(tmp_dir) / "run-1"
            source_dir = run_output_dir / "openai_oauth"
            pool_dir = Path(tmp_dir) / "openai" / "failed-twice"
            source_dir.mkdir(parents=True, exist_ok=True)
            payload_path = source_dir / "small-refresh.json"
            payload_path.write_text(
                json.dumps(
                    {
                        "email": "refresh@example.com",
                        "mailboxRef": "mailbox-ref",
                        "mailboxSessionId": "session-id",
                        "createdAt": "2026-05-30T00:00:00Z",
                        "accessToken": "access.demo",
                        "refreshToken": "refresh.demo",
                        "idToken": "id.demo",
                        "expiresAt": "2026-06-01T00:00:00Z",
                        "oauthClientId": "app_2SKx67EdpoN0G6j64rFvigXD",
                        "oauthTokenEndpoint": "https://auth.openai.com/api/accounts/oauth/token",
                        "refreshStrategy": "oauth_token",
                        "platformOrganization": {"status": "completed"},
                        "chatgptLogin": {"status": "completed", "workspaceId": "ws_123"},
                        "chatgptLoginDetails": {
                            "clientBootstrap": {"authStatus": "logged_in", "structure": "personal"},
                            "oauthTokens": {
                                "access_token": "access.demo",
                                "refresh_token": "refresh.demo",
                                "id_token": "id.demo",
                                "expires_in": 3600,
                                "token_type": "Bearer",
                                "exchanged_at": "2026-05-30T00:00:00Z",
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OPENAI_OAUTH_SEED_MAX_AGE_SECONDS": "0",
                },
                clear=False,
            ):
                copied_paths = runner_artifacts.copy_openai_oauth_artifacts_to_pool(
                    run_output_dir=run_output_dir,
                    pool_dir=pool_dir,
                    worker_label="worker-01",
                    task_index=1,
                )

            self.assertEqual(1, len(copied_paths))
            copied_payload = json.loads(Path(copied_paths[0]).read_text(encoding="utf-8"))
            self.assertEqual("refresh.demo", copied_payload["refreshToken"])
            self.assertEqual("id.demo", copied_payload["idToken"])
            self.assertEqual("refresh.demo", copied_payload["chatgptLoginDetails"]["oauthTokens"]["refresh_token"])

    def test_copy_openai_oauth_artifacts_to_pool_accepts_refresh_material_without_legacy_login(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            run_output_dir = Path(tmp_dir) / "run-1"
            source_dir = run_output_dir / "small_success"
            pool_dir = Path(tmp_dir) / "openai" / "failed-once"
            source_dir.mkdir(parents=True, exist_ok=True)
            payload_path = source_dir / "small-refresh-new-format.json"
            payload_path.write_text(
                json.dumps(
                    {
                        "email": "refresh@example.com",
                        "mailboxRef": "mailbox-ref",
                        "mailboxSessionId": "session-id",
                        "createdAt": "2026-05-30T00:00:00Z",
                        "accessToken": "access.demo",
                        "refreshToken": "refresh.demo",
                        "idToken": "id.demo",
                        "expiresAt": "2026-06-01T00:00:00Z",
                        "oauthClientId": "app_2SKx67EdpoN0G6j64rFvigXD",
                        "oauthTokenEndpoint": "https://auth.openai.com/api/accounts/oauth/token",
                        "refreshStrategy": "oauth_token",
                        "platformOrganization": {"status": "completed"},
                        "chatgptLoginDetails": {
                            "oauthTokens": {
                                "access_token": "access.demo",
                                "refresh_token": "refresh.demo",
                                "id_token": "id.demo",
                                "expires_in": 3600,
                                "token_type": "Bearer",
                                "exchanged_at": "2026-05-30T00:00:00Z",
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )
            copied_paths = runner_artifacts.copy_openai_oauth_artifacts_to_pool(
                run_output_dir=run_output_dir,
                pool_dir=pool_dir,
                worker_label="worker-01",
                task_index=1,
            )

            self.assertEqual(1, len(copied_paths))
            copied_payload = json.loads(Path(copied_paths[0]).read_text(encoding="utf-8"))
            self.assertEqual("refresh.demo", copied_payload["refreshToken"])

    def test_copy_openai_oauth_artifacts_to_pool_does_not_duplicate_artifact_already_in_target_pool(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            run_output_dir = Path(tmp_dir) / "run-1"
            pool_dir = Path(tmp_dir) / "openai" / "failed-twice"
            pool_dir.mkdir(parents=True, exist_ok=True)
            existing_path = pool_dir / "small-existing.json"
            existing_path.write_text(
                json.dumps(
                    {
                        "email": "existing@example.com",
                        "mailboxRef": "mailbox-ref",
                        "mailboxSessionId": "session-id",
                        "createdAt": "2026-05-30T00:00:00Z",
                        "platformOrganization": {"status": "completed"},
                        "chatgptLogin": {"status": "completed", "workspaceId": "ws_123"},
                        "chatgptLoginDetails": {
                            "clientBootstrap": {"authStatus": "logged_in", "structure": "personal"},
                        },
                    }
                ),
                encoding="utf-8",
            )
            result_payload = {
                "outputs": {
                    "finalize-openai-oauth-artifact": {
                        "restored_path": str(existing_path),
                    }
                }
            }

            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OPENAI_OAUTH_SEED_MAX_AGE_SECONDS": "0",
                },
                clear=False,
            ):
                copied_paths = runner_artifacts.copy_openai_oauth_artifacts_to_pool(
                    run_output_dir=run_output_dir,
                    pool_dir=pool_dir,
                    worker_label="worker-01",
                    task_index=1,
                    result_or_payload=result_payload,
                )

            self.assertEqual([str(existing_path.resolve())], copied_paths)
            self.assertEqual(["small-existing.json"], sorted(path.name for path in pool_dir.glob("small-existing*.json")))

    def test_copy_openai_oauth_artifacts_to_pool_enriches_existing_source_with_recovery_data(self) -> None:
        recovery_data = {
            "emailAddress": "seed@example.com",
            "providerTypeKey": "cloudflare_temp_email",
            "providerInstanceId": "cloudflare_temp_email_shared_default",
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            run_output_dir = Path(tmp_dir) / "run-1"
            pool_dir = Path(tmp_dir) / "openai" / "failed-twice"
            source_dir = run_output_dir / "openai_oauth"
            source_dir.mkdir(parents=True, exist_ok=True)
            source_path = source_dir / "small-existing-source.json"
            source_path.write_text(
                json.dumps(
                    {
                        "email": "seed@example.com",
                        "mailboxRef": "cloudflare_temp_email:old-ref",
                        "mailboxSessionId": "old-session",
                        "createdAt": "2026-05-30T00:00:00Z",
                        "platformOrganization": {"status": "completed"},
                        "chatgptLogin": {"status": "completed", "workspaceId": "ws_123"},
                        "chatgptLoginDetails": {
                            "clientBootstrap": {"authStatus": "logged_in", "structure": "personal"},
                        },
                    }
                ),
                encoding="utf-8",
            )
            result_payload = {
                "outputs": {
                    "create-openai-account": {
                        "recovery_data_credential": recovery_data,
                    }
                }
            }

            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OPENAI_OAUTH_SEED_MAX_AGE_SECONDS": "0",
                },
                clear=False,
            ):
                copied_paths = runner_artifacts.copy_openai_oauth_artifacts_to_pool(
                    run_output_dir=run_output_dir,
                    pool_dir=pool_dir,
                    worker_label="worker-01",
                    task_index=1,
                    result_or_payload=result_payload,
                )

            self.assertEqual(1, len(copied_paths))
            copied_payload = json.loads(Path(copied_paths[0]).read_text(encoding="utf-8"))
            self.assertEqual(recovery_data, copied_payload["recoveryDataCredential"])

    def test_copy_openai_oauth_artifacts_to_pool_materializes_from_step_outputs_when_source_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            run_output_dir = Path(tmp_dir) / "run-1"
            pool_dir = Path(tmp_dir) / "openai" / "failed-twice"
            result_payload = {
                "outputs": {
                    "create-openai-account": {
                        "email": "agnese18417@ke.for4u.net",
                        "password": "pw",
                        "mailbox_provider": "moemail",
                        "mailbox_access_key": "mailbox-key",
                        "mailbox_ref": "mailbox-ref",
                        "mailbox_session_id": "session-id",
                        "recovery_data_credential": {
                            "emailAddress": "agnese18417@ke.for4u.net",
                            "providerTypeKey": "moemail",
                            "providerInstanceId": "moemail_shared_default",
                        },
                        "first_name": "John",
                        "last_name": "Doe",
                        "birthdate": "1990-01-01",
                        "page_type": "platform_callback",
                        "final_url": "https://platform.openai.com/auth/callback",
                        "storage_path": str(
                            run_output_dir
                            / "small_success"
                            / "small-20260530-000254-agnese18417@ke.for4u.net-dcd88f.json"
                        ),
                    },
                    "initialize-platform-organization": {
                        "status": "completed",
                        "organizationId": "org_123",
                    },
                    "initialize-chatgpt-login-session": {
                        "status": "completed",
                        "workspaceId": "ws_123",
                        "personalWorkspaceId": "ws_123",
                        "mailboxRef": "mailbox-ref",
                        "mailboxSessionId": "session-id",
                    },
                }
            }

            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OPENAI_OAUTH_SEED_MAX_AGE_SECONDS": "0",
                },
                clear=False,
            ):
                copied_paths = runner_artifacts.copy_openai_oauth_artifacts_to_pool(
                    run_output_dir=run_output_dir,
                    pool_dir=pool_dir,
                    worker_label="worker-01",
                    task_index=1,
                    result_or_payload=result_payload,
                )

            self.assertEqual(1, len(copied_paths))
            copied_path = Path(copied_paths[0])
            self.assertTrue(copied_path.is_file())
            self.assertEqual("small-20260530-000254-agnese18417@ke.for4u.net-dcd88f.json", copied_path.name)
            payload = json.loads(copied_path.read_text(encoding="utf-8"))
            self.assertEqual("agnese18417@ke.for4u.net", payload["email"])
            self.assertEqual(
                {
                    "emailAddress": "agnese18417@ke.for4u.net",
                    "providerTypeKey": "moemail",
                    "providerInstanceId": "moemail_shared_default",
                },
                payload["recoveryDataCredential"],
            )
            self.assertEqual("completed", payload["platformOrganization"]["status"])
            self.assertEqual("completed", payload["chatgptLogin"]["status"])


class RunnerTeamArtifactsTests(unittest.TestCase):
    def test_team_has_collectable_artifacts_accepts_result_object(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            team_path = Path(tmp_dir) / "member.json"
            team_path.write_text("{}", encoding="utf-8")
            result = SimpleNamespace(
                to_dict=lambda: {
                    "outputs": {
                        "collect-team-pool-artifacts": {
                            "artifacts": [
                                {
                                    "kind": "member",
                                    "email": "member@example.com",
                                    "preferred_name": "member.json",
                                    "team_pool_path": str(team_path),
                                }
                            ]
                        }
                    }
                }
            )
            self.assertTrue(runner_team_artifacts.team_has_collectable_artifacts(result=result))

    def test_drain_oauth_pool_backlog_skips_when_pool_matches_local_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            pool_dir = Path(tmp_dir) / "free"
            pool_dir.mkdir(parents=True, exist_ok=True)
            marker_path = pool_dir / "a.json"
            marker_path.write_text("", encoding="utf-8")

            result = runner_team_artifacts.drain_oauth_pool_backlog(
                pool_dir=pool_dir,
                target_folder="codex",
                local_percent=100.0,
                local_dir=pool_dir,
            )

            self.assertTrue(result["ok"])
            self.assertEqual("same-dir-skipped", result["status"])
            self.assertTrue(marker_path.is_file())
            self.assertEqual("", marker_path.read_text(encoding="utf-8"))

    def test_sync_team_member_artifacts_skips_when_success_path_already_in_local_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared-root"
            output_root = shared_root / "others" / "mixed-runs"
            claims_dir = shared_root / "others" / "team-mother-claims"
            local_dir = shared_root / "codex" / "team"
            claims_dir.mkdir(parents=True, exist_ok=True)
            local_dir.mkdir(parents=True, exist_ok=True)

            existing_team_path = local_dir / "member-existing.json"
            existing_team_path.write_text("{}", encoding="utf-8")
            before_hash = existing_team_path.read_text(encoding="utf-8")
            before_mtime = existing_team_path.stat().st_mtime

            claim_path = claims_dir / "claim.json"
            claim_path.write_text(
                json.dumps(
                    {
                        "teamFlow": {
                            "teamExpandProgress": {
                                "successfulArtifacts": [
                                    {
                                        "email": "member@example.com",
                                        "successPath": str(existing_team_path),
                                    }
                                ]
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
                    "REGISTER_TEAM_LOCAL_DIR": str(local_dir),
                    "REGISTER_TEAM_LOCAL_SPLIT_PERCENT": "100",
                },
                clear=False,
            ):
                result = runner_team_artifacts.sync_team_member_artifacts_from_active_claims(
                    output_root=output_root,
                )

            self.assertTrue(result["ok"])
            self.assertIn(result["status"], {"processed", "idle"})
            self.assertTrue(existing_team_path.is_file())
            self.assertEqual(before_hash, existing_team_path.read_text(encoding="utf-8"))
            self.assertEqual(before_mtime, existing_team_path.stat().st_mtime)
            self.assertEqual(1, len(result["localized"]))
            self.assertEqual(str(existing_team_path), result["localized"][0]["stored_path"])

    def test_sync_team_member_artifacts_treats_unavailable_claim_dir_as_idle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared-root"
            output_root = shared_root / "others" / "mixed-runs"
            claims_dir = shared_root / "others" / "team-mother-claims"

            original_is_dir = Path.is_dir

            def _is_dir(path: Path) -> bool:
                if path == claims_dir:
                    raise OSError("[Errno 5] Input/output error")
                return original_is_dir(path)

            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "REGISTER_TEAM_LOCAL_SPLIT_PERCENT": "100",
                },
                clear=False,
            ), mock.patch.object(Path, "is_dir", _is_dir):
                result = runner_team_artifacts.sync_team_member_artifacts_from_active_claims(
                    output_root=output_root,
                )

        self.assertTrue(result["ok"])
        self.assertEqual("claims_dir_unavailable", result["status"])
        self.assertEqual([], result["localized"])
        self.assertEqual([], result["failures"])


class RunnerFlowSchedulerTests(unittest.TestCase):
    def test_runner_main_config_accepts_account_audit_input_source_dir_flow_spec(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            shared_root = output_root / "shared"
            account_dir = Path(tmp_dir) / "account-audit-input"
            claims_dir = account_dir / "_claims"
            raw = json.dumps(
                [
                    {
                        "name": "openai-account-availability-audit",
                        "flowPath": (
                            "server/services/orchestration_service/flows/"
                            "openai-account-availability-audit-v1.semantic-flow.json"
                        ),
                        "instanceRole": "account-audit",
                        "weight": 1,
                        "taskMaxAttempts": 1,
                        "inputSourceDir": str(account_dir),
                        "inputClaimsDir": str(claims_dir),
                        "concurrencyLimit": 1,
                    }
                ]
            )
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "REGISTER_SHARED_ROOT": str(shared_root),
                    "REGISTER_INSTANCE_ID": "mixed",
                    "REGISTER_INSTANCE_ROLE": "mixed",
                    "REGISTER_FLOW_SPECS_JSON": raw,
                },
                clear=True,
            ):
                specs = RunnerMainConfig.from_env().flow_specs

        self.assertEqual(1, len(specs))
        self.assertEqual("openai-account-availability-audit", specs[0].name)
        self.assertEqual("account-audit", specs[0].instance_role)
        self.assertEqual(str(account_dir), specs[0].input_source_dir)
        self.assertEqual(str(claims_dir), specs[0].input_claims_dir)
        self.assertEqual(1, specs[0].concurrency_limit)

    def test_choose_runnable_flow_spec_skips_empty_continue_pool(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            shared_root = output_root / "shared"
            continue_pool_dir = shared_root / "openai" / "failed-once"
            spec = RunnerFlowSpec(
                name="continue-openai",
                flow_path="continue-flow.json",
                instance_role="continue",
                weight=1.0,
                team_auth_path="",
                task_max_attempts=0,
                openai_oauth_pool_dir=continue_pool_dir,
                mailbox_business_key="openai",
                input_source_dir="",
                input_claims_dir="",
            )
            selected, selection = runner_flow_scheduler.choose_runnable_flow_spec(
                flow_specs=(spec,),
                output_root=output_root,
                shared_root=shared_root,
            )
        self.assertIsNone(selected)
        self.assertEqual("openai_oauth_pool_empty", selection["skipped"][0]["reason"])

    def test_choose_runnable_flow_spec_selects_ready_continue_pool(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            shared_root = output_root / "shared"
            continue_pool_dir = shared_root / "openai" / "failed-once"
            continue_pool_dir.mkdir(parents=True, exist_ok=True)
            (continue_pool_dir / "seed.json").write_text("{}", encoding="utf-8")
            spec = RunnerFlowSpec(
                name="continue-openai",
                flow_path="continue-flow.json",
                instance_role="continue",
                weight=1.0,
                team_auth_path="",
                task_max_attempts=0,
                openai_oauth_pool_dir=continue_pool_dir,
                mailbox_business_key="openai",
                input_source_dir="",
                input_claims_dir="",
            )
            selected, selection = runner_flow_scheduler.choose_runnable_flow_spec(
                flow_specs=(spec,),
                output_root=output_root,
                shared_root=shared_root,
            )
        self.assertIsNotNone(selected)
        self.assertEqual("continue-openai", selected.name)
        self.assertEqual("pool_ready", selection["selected"]["reason"])

    def test_choose_runnable_flow_spec_prefers_continue_over_always_runnable_main(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            shared_root = output_root / "shared"
            pending_dir = shared_root / "openai" / "pending"
            continue_pool_dir = shared_root / "openai" / "failed-once"
            continue_pool_dir.mkdir(parents=True, exist_ok=True)
            pending_dir.mkdir(parents=True, exist_ok=True)
            (continue_pool_dir / "seed.json").write_text("{}", encoding="utf-8")

            main_spec = RunnerFlowSpec(
                name="main-openai",
                flow_path="main-flow.json",
                instance_role="main",
                weight=99.0,
                team_auth_path="",
                task_max_attempts=0,
                openai_oauth_pool_dir=pending_dir,
                mailbox_business_key="openai",
                input_source_dir="",
                input_claims_dir="",
            )
            continue_spec = RunnerFlowSpec(
                name="continue-openai",
                flow_path="continue-flow.json",
                instance_role="continue",
                weight=1.0,
                team_auth_path="",
                task_max_attempts=0,
                openai_oauth_pool_dir=continue_pool_dir,
                mailbox_business_key="openai",
                input_source_dir="",
                input_claims_dir="",
            )

            selected, selection = runner_flow_scheduler.choose_runnable_flow_spec(
                flow_specs=(main_spec, continue_spec),
                output_root=output_root,
                shared_root=shared_root,
            )

        self.assertIsNotNone(selected)
        self.assertEqual("continue-openai", selected.name)
        self.assertEqual("continue", selection["selected"]["instanceRole"])

    def test_choose_runnable_flow_spec_skips_flow_when_concurrency_limit_reached(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            shared_root = output_root / "shared"
            pending_dir = shared_root / "openai" / "pending"
            pending_dir.mkdir(parents=True, exist_ok=True)
            spec = RunnerFlowSpec(
                name="main-openai",
                flow_path="main-flow.json",
                instance_role="main",
                weight=1.0,
                team_auth_path="",
                task_max_attempts=0,
                openai_oauth_pool_dir=pending_dir,
                mailbox_business_key="openai",
                input_source_dir="",
                input_claims_dir="",
                concurrency_limit=1,
            )
            selected, selection = runner_flow_scheduler.choose_runnable_flow_spec(
                flow_specs=(spec,),
                output_root=output_root,
                shared_root=shared_root,
                active_flow_counts={"main-openai": 1},
            )
        self.assertIsNone(selected)
        self.assertEqual("concurrency_limit_reached", selection["skipped"][0]["reason"])

    def test_choose_runnable_flow_spec_selects_account_audit_input_source_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            shared_root = output_root / "shared"
            account_dir = Path(tmp_dir) / "account-audit-input"
            account_dir.mkdir(parents=True, exist_ok=True)
            (account_dir / "seed.json").write_text(
                json.dumps({"email": "seed@example.com"}),
                encoding="utf-8",
            )
            spec = RunnerFlowSpec(
                name="openai-account-availability-audit",
                flow_path="server/services/orchestration_service/flows/openai-account-availability-audit-v1.semantic-flow.json",
                instance_role="account-audit",
                weight=1.0,
                team_auth_path="",
                task_max_attempts=1,
                openai_oauth_pool_dir=output_root / "openai" / "unused",
                mailbox_business_key="openai-account-audit",
                input_source_dir=str(account_dir),
                input_claims_dir="",
            )
            selected, selection = runner_flow_scheduler.choose_runnable_flow_spec(
                flow_specs=(spec,),
                output_root=output_root,
                shared_root=shared_root,
            )

        self.assertIsNotNone(selected)
        self.assertEqual("openai-account-availability-audit", selected.name)
        self.assertEqual("input_source_dir_ready", selection["selected"]["reason"])
        self.assertEqual(str(account_dir.resolve()), selection["selected"]["inputSourceDir"])

    def test_choose_runnable_flow_spec_does_not_deep_scan_account_audit_production_pool(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            output_root.mkdir(parents=True, exist_ok=True)
            spec = RunnerFlowSpec(
                name="openai-account-availability-audit",
                flow_path="server/services/orchestration_service/flows/openai-account-availability-audit-v1.semantic-flow.json",
                instance_role="account-audit",
                weight=1.0,
                team_auth_path="",
                task_max_attempts=1,
                openai_oauth_pool_dir=output_root / "openai" / "unused",
                mailbox_business_key="openai-account-audit",
                input_source_dir=str(output_root),
                input_claims_dir="",
            )
            with mock.patch.object(
                runner_flow_scheduler,
                "production_audit_has_due_targets",
                side_effect=AssertionError("scheduler must not deep scan production audit pools"),
            ):
                selected, selection = runner_flow_scheduler.choose_runnable_flow_spec(
                    flow_specs=(spec,),
                    output_root=output_root,
                    shared_root=output_root,
                )

        self.assertIsNotNone(selected)
        self.assertEqual("openai-account-availability-audit", selected.name)
        self.assertEqual("production_pool_maybe_ready", selection["selected"]["reason"])

    def test_flow_slot_reserve_and_release_roundtrip(self) -> None:
        spec = RunnerFlowSpec(
            name="continue-openai",
            flow_path="continue-flow.json",
            instance_role="continue",
            weight=1.0,
            team_auth_path="",
            task_max_attempts=0,
            openai_oauth_pool_dir=Path("C:/tmp/openai"),
            mailbox_business_key="openai",
            input_source_dir="",
            input_claims_dir="",
            concurrency_limit=2,
        )
        counts: dict[str, int] = {}
        self.assertTrue(
            runner_flow_scheduler.reserve_flow_slot(
                spec=spec,
                active_flow_counts=counts,
                active_flow_lock=None,
            )
        )
        self.assertEqual({"continue-openai": 1}, counts)
        self.assertTrue(
            runner_flow_scheduler.reserve_flow_slot(
                spec=spec,
                active_flow_counts=counts,
                active_flow_lock=None,
            )
        )
        self.assertEqual({"continue-openai": 2}, counts)
        self.assertFalse(
            runner_flow_scheduler.reserve_flow_slot(
                spec=spec,
                active_flow_counts=counts,
                active_flow_lock=None,
            )
        )
        runner_flow_scheduler.release_flow_slot(
            spec=spec,
            active_flow_counts=counts,
            active_flow_lock=None,
        )
        self.assertEqual({"continue-openai": 1}, counts)

    def test_release_flow_slot_for_owner_decrements_stale_worker_slot(self) -> None:
        counts: dict[str, int] = {"continue-openai": 2}
        owners: dict[str, str] = {"worker-01": "continue-openai"}

        released = runner_flow_scheduler.release_flow_slot_for_owner(
            owner_id="worker-01",
            active_flow_counts=counts,
            active_flow_owners=owners,
            active_flow_lock=None,
        )

        self.assertEqual("continue-openai", released)
        self.assertEqual({"continue-openai": 1}, counts)
        self.assertEqual({}, owners)


class RunnerProcessSupervisorTests(unittest.TestCase):
    def test_recover_stale_uninterruptible_worker_releases_flow_slot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            workers_dir = shared_root / "others" / "dashboard-state" / "easy-register" / "workers"
            workers_dir.mkdir(parents=True, exist_ok=True)
            (workers_dir / "worker-05.json").write_text(
                json.dumps(
                    {
                        "workerId": "worker-05",
                        "status": "running",
                        "updatedAt": "2026-06-19T01:03:05+00:00",
                        "currentTaskRole": "continue",
                        "currentFlowName": "openai-continue",
                        "currentOutputDir": "/shared/register-output/others/mixed-runs/worker-05/run-active",
                    }
                ),
                encoding="utf-8",
            )
            proc_root = Path(tmp_dir) / "proc"
            pid_status = proc_root / "123" / "status"
            pid_status.parent.mkdir(parents=True, exist_ok=True)
            pid_status.write_text("Name:\tpython\nState:\tD (disk sleep)\n", encoding="utf-8")
            active_counts = {"openai-continue": 1}
            active_owners = {"worker-05": "openai-continue"}
            process = mock.Mock()
            process.pid = 123
            process.is_alive.return_value = True

            recovered = runner_process_supervisor.recover_stale_uninterruptible_worker_slots(
                processes={5: process},
                shared_root=shared_root,
                instance_id="easy-register",
                active_flow_counts=active_counts,
                active_flow_owners=active_owners,
                active_flow_lock=None,
                stale_seconds=600.0,
                now=datetime(2026, 6, 19, 2, 13, 5, tzinfo=timezone.utc),
                proc_root=proc_root,
            )

        self.assertEqual(["openai-continue"], [item["slotKey"] for item in recovered])
        self.assertEqual([True], [item["terminateSignalSent"] for item in recovered])
        self.assertEqual({}, active_counts)
        self.assertEqual({}, active_owners)
        process.terminate.assert_called_once_with()

    def test_recover_stale_uninterruptible_worker_uses_longer_account_audit_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            workers_dir = shared_root / "others" / "dashboard-state" / "easy-register" / "workers"
            workers_dir.mkdir(parents=True, exist_ok=True)
            (workers_dir / "worker-05.json").write_text(
                json.dumps(
                    {
                        "workerId": "worker-05",
                        "status": "running",
                        "updatedAt": "2026-06-19T01:53:05+00:00",
                        "currentTaskRole": "account-audit",
                        "currentFlowName": "openai-account-availability-audit",
                        "currentOutputDir": "/shared/register-output/others/mixed-runs/worker-05/run-active",
                    }
                ),
                encoding="utf-8",
            )
            proc_root = Path(tmp_dir) / "proc"
            pid_status = proc_root / "123" / "status"
            pid_status.parent.mkdir(parents=True, exist_ok=True)
            pid_status.write_text("Name:\tpython\nState:\tD (disk sleep)\n", encoding="utf-8")
            active_counts = {"openai-account-availability-audit": 1}
            active_owners = {"worker-05": "openai-account-availability-audit"}
            process = mock.Mock()
            process.pid = 123
            process.is_alive.return_value = True

            recovered = runner_process_supervisor.recover_stale_uninterruptible_worker_slots(
                processes={5: process},
                shared_root=shared_root,
                instance_id="easy-register",
                active_flow_counts=active_counts,
                active_flow_owners=active_owners,
                active_flow_lock=None,
                stale_seconds=600.0,
                now=datetime(2026, 6, 19, 2, 13, 5, tzinfo=timezone.utc),
                proc_root=proc_root,
            )

        self.assertEqual([], recovered)
        self.assertEqual({"openai-account-availability-audit": 1}, active_counts)
        self.assertEqual({"worker-05": "openai-account-availability-audit"}, active_owners)
        process.terminate.assert_not_called()

    def test_recover_stale_uninterruptible_worker_allows_account_audit_threshold_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            workers_dir = shared_root / "others" / "dashboard-state" / "easy-register" / "workers"
            workers_dir.mkdir(parents=True, exist_ok=True)
            (workers_dir / "worker-05.json").write_text(
                json.dumps(
                    {
                        "workerId": "worker-05",
                        "status": "running",
                        "updatedAt": "2026-06-19T01:53:05+00:00",
                        "currentTaskRole": "account-audit",
                        "currentFlowName": "openai-account-availability-audit",
                        "currentOutputDir": "/shared/register-output/others/mixed-runs/worker-05/run-active",
                    }
                ),
                encoding="utf-8",
            )
            proc_root = Path(tmp_dir) / "proc"
            pid_status = proc_root / "123" / "status"
            pid_status.parent.mkdir(parents=True, exist_ok=True)
            pid_status.write_text("Name:\tpython\nState:\tD (disk sleep)\n", encoding="utf-8")
            active_counts = {"openai-account-availability-audit": 1}
            active_owners = {"worker-05": "openai-account-availability-audit"}
            process = mock.Mock()
            process.pid = 123
            process.is_alive.return_value = True

            with mock.patch.dict(
                os.environ,
                {"REGISTER_ACCOUNT_AUDIT_FLOW_SLOT_UNINTERRUPTIBLE_STALE_SECONDS": "900"},
                clear=False,
            ):
                recovered = runner_process_supervisor.recover_stale_uninterruptible_worker_slots(
                    processes={5: process},
                    shared_root=shared_root,
                    instance_id="easy-register",
                    active_flow_counts=active_counts,
                    active_flow_owners=active_owners,
                    active_flow_lock=None,
                    stale_seconds=600.0,
                    now=datetime(2026, 6, 19, 2, 13, 5, tzinfo=timezone.utc),
                    proc_root=proc_root,
                )

        self.assertEqual(["openai-account-availability-audit"], [item["slotKey"] for item in recovered])
        self.assertEqual([900.0], [item["thresholdSeconds"] for item in recovered])
        self.assertEqual([600.0], [item["defaultThresholdSeconds"] for item in recovered])
        process.terminate.assert_called_once_with()

    def test_recover_stale_account_audit_worker_releases_running_slot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            workers_dir = shared_root / "others" / "dashboard-state" / "easy-register" / "workers"
            workers_dir.mkdir(parents=True, exist_ok=True)
            (workers_dir / "worker-05.json").write_text(
                json.dumps(
                    {
                        "workerId": "worker-05",
                        "status": "running",
                        "updatedAt": "2026-06-19T02:00:00+00:00",
                        "startedAt": "2026-06-19T02:00:00+00:00",
                        "currentTaskRole": "account-audit",
                        "currentFlowName": "openai-account-availability-audit",
                        "currentOutputDir": "/shared/register-output/others/mixed-runs/worker-05/run-active",
                    }
                ),
                encoding="utf-8",
            )
            active_counts = {"openai-account-availability-audit": 1}
            active_owners = {"worker-05": "openai-account-availability-audit"}
            process = mock.Mock()
            process.pid = 123
            process.is_alive.return_value = True

            recovered = runner_process_supervisor.recover_stale_account_audit_workers(
                processes={5: process},
                shared_root=shared_root,
                instance_id="easy-register",
                active_flow_counts=active_counts,
                active_flow_owners=active_owners,
                active_flow_lock=None,
                now=datetime(2026, 6, 19, 2, 8, 0, tzinfo=timezone.utc),
            )

        self.assertEqual(["openai-account-availability-audit"], [item["slotKey"] for item in recovered])
        self.assertEqual([420.0], [item["thresholdSeconds"] for item in recovered])
        self.assertEqual({}, active_counts)
        self.assertEqual({}, active_owners)
        process.terminate.assert_called_once_with()

    def test_recover_stale_account_audit_worker_does_not_touch_main_worker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            workers_dir = shared_root / "others" / "dashboard-state" / "easy-register" / "workers"
            workers_dir.mkdir(parents=True, exist_ok=True)
            (workers_dir / "worker-05.json").write_text(
                json.dumps(
                    {
                        "workerId": "worker-05",
                        "status": "running",
                        "updatedAt": "2026-06-19T02:00:00+00:00",
                        "startedAt": "2026-06-19T02:00:00+00:00",
                        "currentTaskRole": "main",
                        "currentFlowName": "openai-main",
                    }
                ),
                encoding="utf-8",
            )
            active_counts = {"openai-main": 1}
            active_owners = {"worker-05": "openai-main"}
            process = mock.Mock()
            process.pid = 123
            process.is_alive.return_value = True

            recovered = runner_process_supervisor.recover_stale_account_audit_workers(
                processes={5: process},
                shared_root=shared_root,
                instance_id="easy-register",
                active_flow_counts=active_counts,
                active_flow_owners=active_owners,
                active_flow_lock=None,
                now=datetime(2026, 6, 19, 2, 30, 0, tzinfo=timezone.utc),
            )

        self.assertEqual([], recovered)
        self.assertEqual({"openai-main": 1}, active_counts)
        self.assertEqual({"worker-05": "openai-main"}, active_owners)
        process.terminate.assert_not_called()

    def test_task_slots_exhausted_reads_counter_without_lock(self) -> None:
        class _Counter:
            @property
            def value(self) -> int:
                raise AssertionError("synchronized value getter should not be used")

            def get_obj(self) -> Any:
                return SimpleNamespace(value=1)

        counter = _Counter()
        self.assertTrue(runner_process_supervisor.task_slots_exhausted(task_counter=counter, max_runs=1))

    def test_should_stop_supervisor_after_worker_stop_only_when_last_worker_and_exhausted(self) -> None:
        counter = SimpleNamespace(get_obj=lambda: SimpleNamespace(value=1))
        self.assertTrue(
            runner_process_supervisor.should_stop_supervisor_after_worker_stop(
                processes={},
                task_counter=counter,
                max_runs=1,
            )
        )
        self.assertFalse(
            runner_process_supervisor.should_stop_supervisor_after_worker_stop(
                processes={1: object()},
                task_counter=counter,
                max_runs=1,
            )
        )

    def test_cleanup_process_handle_joins_closes_and_optionally_terminates(self) -> None:
        process = mock.Mock()
        process.is_alive.return_value = True
        runner_process_supervisor.cleanup_process_handle(
            process=process,
            join_timeout=0.25,
            terminate_if_alive=True,
        )
        process.join.assert_any_call(timeout=0.25)
        process.terminate.assert_called_once_with()
        process.join.assert_any_call(timeout=1.0)
        process.close.assert_called_once_with()

    def test_main_exits_cleanly_after_last_worker_when_max_runs_reached(self) -> None:
        fake_process = mock.Mock()
        fake_process.pid = 321
        fake_process.exitcode = 0
        fake_process.is_alive.return_value = False

        stop_event = mock.Mock()
        stop_event.is_set.return_value = False
        task_counter = SimpleNamespace(get_obj=lambda: SimpleNamespace(value=1))
        active_flow_counts: dict[str, int] = {}
        active_flow_owners: dict[str, str] = {}
        ctx = SimpleNamespace(
            Event=mock.Mock(return_value=stop_event),
            Value=mock.Mock(return_value=task_counter),
            Manager=mock.Mock(
                return_value=SimpleNamespace(
                    dict=mock.Mock(side_effect=[active_flow_counts, active_flow_owners])
                )
            ),
            Lock=mock.Mock(return_value=nullcontext()),
        )
        config = SimpleNamespace(
            output_root=Path("C:/tmp/register-output"),
            shared_root=Path("C:/tmp/register-output"),
            openai_oauth_pool_dir=Path("C:/tmp/register-output/openai/pending"),
            free_oauth_pool_dir=Path("C:/tmp/register-output/codex/free"),
            flow_path="team-flow.json",
            instance_id="mixed-test",
            instance_role="mixed",
            worker_count=1,
            delay_seconds=0.0,
            worker_stagger_seconds=0.0,
            max_runs=1,
            task_max_attempts=1,
            flow_specs=(),
            easy_protocol_base_url="http://easy-protocol:9788",
            easy_protocol_control_token="secure-token",
            easy_protocol_control_actor="register-dashboard",
        )
        service_state = mock.Mock()
        with mock.patch.object(runner_process_supervisor, "_validate_runtime_preflight", return_value={}):
            with mock.patch.object(runner_process_supervisor, "RunnerMainConfig") as config_cls:
                config_cls.from_env.return_value = config
                with mock.patch.object(runner_process_supervisor, "_ensure_directory"):
                    with mock.patch.object(runner_process_supervisor, "cleanup_dashboard_worker_state_files"):
                        with mock.patch.object(runner_process_supervisor, "ServiceRuntimeState", return_value=service_state):
                            with mock.patch.object(runner_process_supervisor, "install_signal_handlers"):
                                with mock.patch.object(runner_process_supervisor, "start_dashboard_server_if_enabled", return_value=None):
                                    with mock.patch.object(runner_process_supervisor.mp, "get_context", return_value=ctx):
                                        with mock.patch.object(runner_process_supervisor, "start_worker", return_value=fake_process):
                                            with mock.patch.object(runner_process_supervisor, "_json_log") as json_log:
                                                exit_code = runner_process_supervisor.main()
        self.assertEqual(0, exit_code)
        service_state.started.assert_called_once_with(pid=mock.ANY, max_runs=1)
        service_state.stopped.assert_called_once_with(pid=mock.ANY, task_count=1)
        stop_event.set.assert_not_called()
        fake_process.join.assert_any_call(timeout=0.0)
        fake_process.close.assert_called_once_with()
        events = [call.args[0]["event"] for call in json_log.call_args_list if call.args and isinstance(call.args[0], dict) and "event" in call.args[0]]
        self.assertIn("register_supervisor_finally_entered", events)
        self.assertIn("register_supervisor_stopped", events)

    def test_main_releases_worker_owned_flow_slot_when_worker_crashes(self) -> None:
        fake_process = mock.Mock()
        fake_process.pid = 321
        fake_process.exitcode = 1
        fake_process.is_alive.return_value = False

        stop_event = mock.Mock()
        stop_event.is_set.side_effect = [False, False, False, False, True, True, True]
        task_counter = SimpleNamespace(get_obj=lambda: SimpleNamespace(value=0))
        active_flow_counts: dict[str, int] = {"openai-continue": 2}
        active_flow_owners: dict[str, str] = {"worker-01": "openai-continue"}
        manager = SimpleNamespace(
            dict=mock.Mock(side_effect=[active_flow_counts, active_flow_owners]),
            shutdown=mock.Mock(),
        )
        ctx = SimpleNamespace(
            Event=mock.Mock(return_value=stop_event),
            Value=mock.Mock(return_value=task_counter),
            Manager=mock.Mock(return_value=manager),
            Lock=mock.Mock(return_value=nullcontext()),
        )
        config = SimpleNamespace(
            output_root=Path("C:/tmp/register-output"),
            shared_root=Path("C:/tmp/register-output"),
            openai_oauth_pool_dir=Path("C:/tmp/register-output/openai/pending"),
            free_oauth_pool_dir=Path("C:/tmp/register-output/codex/free"),
            flow_path="team-flow.json",
            instance_id="mixed-test",
            instance_role="mixed",
            worker_count=1,
            delay_seconds=0.0,
            worker_stagger_seconds=0.0,
            max_runs=0,
            task_max_attempts=1,
            flow_specs=(),
            easy_protocol_base_url="http://easy-protocol:9788",
            easy_protocol_control_token="secure-token",
            easy_protocol_control_actor="register-dashboard",
        )
        service_state = mock.Mock()
        with mock.patch.object(runner_process_supervisor, "_validate_runtime_preflight", return_value={}):
            with mock.patch.object(runner_process_supervisor, "RunnerMainConfig") as config_cls:
                config_cls.from_env.return_value = config
                with mock.patch.object(runner_process_supervisor, "_ensure_directory"):
                    with mock.patch.object(runner_process_supervisor, "cleanup_dashboard_worker_state_files"):
                        with mock.patch.object(runner_process_supervisor, "ServiceRuntimeState", return_value=service_state):
                            with mock.patch.object(runner_process_supervisor, "install_signal_handlers"):
                                with mock.patch.object(runner_process_supervisor, "start_dashboard_server_if_enabled", return_value=None):
                                    with mock.patch.object(runner_process_supervisor.mp, "get_context", return_value=ctx):
                                        with mock.patch.object(runner_process_supervisor, "start_worker", return_value=fake_process):
                                            with mock.patch.object(runner_process_supervisor, "_json_log") as json_log:
                                                runner_process_supervisor.main()

        self.assertEqual({"openai-continue": 1}, active_flow_counts)
        self.assertEqual({}, active_flow_owners)
        events = [
            call.args[0]
            for call in json_log.call_args_list
            if call.args and isinstance(call.args[0], dict) and "event" in call.args[0]
        ]
        self.assertTrue(
            any(
                event.get("event") == "register_worker_flow_slot_recovered"
                and event.get("slotKey") == "openai-continue"
                for event in events
            )
        )


class RunnerFailuresTests(unittest.TestCase):
    def test_team_auth_blacklist_reason_requires_retry_evidence(self) -> None:
        payload = {
            "errorStep": "invite-codex-member",
            "stepAttempts": {
                "invite-codex-member": 2,
                "refresh-team-auth-on-demand": 1,
            },
            "stepErrors": {
                "invite-codex-member": {
                    "code": ErrorCodes.TEAM_AUTH_TOKEN_INVALIDATED,
                    "message": "token expired",
                }
            },
        }
        reason = runner_failures.team_auth_blacklist_reason(result_payload_value=payload)
        self.assertIn("token expired", reason)
        self.assertIn(ErrorCodes.TEAM_AUTH_TOKEN_INVALIDATED, reason)

    def test_team_auth_blacklist_reason_marks_deactivated_workspace_immediately(self) -> None:
        payload = {
            "errorStep": "invite-codex-member",
            "stepAttempts": {
                "invite-codex-member": 1,
            },
            "stepErrors": {
                "invite-codex-member": {
                    "code": ErrorCodes.TEAM_WORKSPACE_DEACTIVATED,
                    "message": "{'detail': {'code': 'deactivated_workspace'}, 'status_code': 402}",
                }
            },
        }
        reason = runner_failures.team_auth_blacklist_reason(result_payload_value=payload)
        self.assertIn("deactivated_workspace", reason)

    def test_extra_failure_cooldown_seconds_uses_typed_cleanup_config(self) -> None:
        payload = {
            "errorStep": "create-openai-account",
            "stepErrors": {
                "create-openai-account": {
                    "code": ErrorCodes.TRANSPORT_ERROR,
                    "message": "transport failure",
                }
            },
        }
        with mock.patch.dict(
            os.environ,
            {"REGISTER_CREATE_ACCOUNT_COOLDOWN_SECONDS": "45"},
            clear=True,
        ):
            cooldown = runner_failures.extra_failure_cooldown_seconds(result=payload)
        self.assertEqual(45.0, cooldown)

    def test_extra_failure_cooldown_seconds_covers_oauth_cloudflare_challenge(self) -> None:
        payload = {
            "errorStep": "obtain-codex-oauth",
            "stepErrors": {
                "obtain-codex-oauth": {
                    "code": ErrorCodes.AUTHORIZE_CONTINUE_BLOCKED,
                    "message": "oauth_authorize_repair_challenge status=403 cf_mitigated=challenge",
                }
            },
        }
        with mock.patch.dict(
            os.environ,
            {
                "REGISTER_CREATE_ACCOUNT_COOLDOWN_SECONDS": "11",
                "REGISTER_OAUTH_BLOCKED_COOLDOWN_SECONDS": "37",
            },
            clear=True,
        ):
            cooldown = runner_failures.extra_failure_cooldown_seconds(result=payload)
        self.assertEqual(37.0, cooldown)

    def test_extra_failure_cooldown_seconds_uses_rate_limit_specific_cooldown(self) -> None:
        payload = {
            "errorStep": "obtain-codex-oauth",
            "stepErrors": {
                "obtain-codex-oauth": {
                    "code": ErrorCodes.AUTHORIZE_CONTINUE_RATE_LIMITED,
                    "message": "authorize_continue status=429",
                }
            },
        }
        with mock.patch.dict(
            os.environ,
            {
                "REGISTER_CREATE_ACCOUNT_COOLDOWN_SECONDS": "11",
                "REGISTER_OAUTH_RATE_LIMIT_COOLDOWN_SECONDS": "321",
            },
            clear=True,
        ):
            cooldown = runner_failures.extra_failure_cooldown_seconds(result=payload)
        self.assertEqual(321.0, cooldown)

    def test_extra_failure_cooldown_seconds_uses_missing_session_specific_cooldown(self) -> None:
        payload = {
            "errorStep": "obtain-codex-oauth",
            "stepErrors": {
                "obtain-codex-oauth": {
                    "code": ErrorCodes.AUTHORIZE_MISSING_LOGIN_SESSION,
                    "message": "authorize_init_missing_login_session",
                }
            },
        }
        with mock.patch.dict(
            os.environ,
            {
                "REGISTER_CREATE_ACCOUNT_COOLDOWN_SECONDS": "11",
                "REGISTER_OAUTH_MISSING_SESSION_COOLDOWN_SECONDS": "67",
            },
            clear=True,
        ):
            cooldown = runner_failures.extra_failure_cooldown_seconds(result=payload)
        self.assertEqual(67.0, cooldown)

    def test_extra_failure_cooldown_seconds_covers_sms_no_selection_after_phone_wall(self) -> None:
        for error_code in (
            "sms_no_selection_plan_candidates",
            "sms_no_productive_selection_plan_candidates",
        ):
            with self.subTest(error_code=error_code):
                payload = {
                    "errorStep": "obtain-codex-oauth",
                    "error": error_code,
                    "stepErrors": {
                        "obtain-codex-oauth": {
                            "message": error_code,
                        }
                    },
                }
                with mock.patch.dict(
                    os.environ,
                    {"REGISTER_SMS_NO_SELECTION_COOLDOWN_SECONDS": "91"},
                    clear=True,
                ):
                    cooldown = runner_failures.extra_failure_cooldown_seconds(result=payload)
                self.assertEqual(91.0, cooldown)

    def test_extra_failure_cooldown_seconds_covers_oauth_flow_timeout(self) -> None:
        payload = {
            "errorStep": "obtain-codex-oauth",
            "stepErrors": {
                "obtain-codex-oauth": {
                    "code": ErrorCodes.FLOW_TIMEOUT_EXCEEDED,
                    "message": "timed out",
                }
            },
        }
        with mock.patch.dict(
            os.environ,
            {"REGISTER_CREATE_ACCOUNT_COOLDOWN_SECONDS": "33"},
            clear=True,
        ):
            cooldown = runner_failures.extra_failure_cooldown_seconds(result=payload)
        self.assertEqual(33.0, cooldown)

    def test_classify_error_code_maps_oauth_repair_challenge_to_blocked(self) -> None:
        code = classify_error_code(
            step_type="obtain_codex_oauth",
            message="oauth_authorize_repair_challenge status=403 cf_mitigated=challenge",
        )
        self.assertEqual(ErrorCodes.AUTHORIZE_CONTINUE_BLOCKED, code)

    def test_team_mother_failure_cooldown_seconds_uses_structured_codes(self) -> None:
        payload = {
            "errorStep": "invite-team-members",
            "stepErrors": {
                "invite-team-members": {
                    "code": ErrorCodes.TEAM_SEATS_FULL,
                    "message": "workspace full",
                }
            },
        }
        with mock.patch.dict(
            os.environ,
            {"REGISTER_TEAM_INVITE_FAILURE_COOLDOWN_SECONDS": "123"},
            clear=True,
        ):
            cooldown = runner_failures.team_mother_failure_cooldown_seconds(result=payload)
        self.assertEqual(123.0, cooldown)


class RunnerMailboxTests(unittest.TestCase):
    def test_mailbox_capacity_failure_detail_uses_structured_code(self) -> None:
        payload = {
            "errorStep": "acquire-mailbox",
            "stepErrors": {
                "acquire-mailbox": {
                    "code": ErrorCodes.MAILBOX_UNAVAILABLE,
                    "message": "mailbox capacity unavailable",
                }
            },
        }
        detail = runner_mailbox.mailbox_capacity_failure_detail(result_payload_value=payload)
        self.assertIn("mailbox capacity unavailable", detail)

    def test_record_business_mailbox_domain_outcome_writes_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            payload = {
                "ok": False,
                "steps": {"acquire-mailbox": "ok"},
                "outputs": {
                    "acquire-mailbox": {
                        "email": "user@sall.cc",
                        "provider": "moemail",
                        "business_key": "openai",
                    }
                },
            }
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_MAILBOX_BUSINESS_KEY": "generic",
                    "REGISTER_MAILBOX_DOMAIN_BLACKLIST": "fallback.test",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": (
                        '{"openai":{"explicitBlacklistDomains":["coolkid.icu"]}}'
                    ),
                },
                clear=True,
            ):
                outcome = runner_mailbox.record_business_mailbox_domain_outcome(
                    shared_root=shared_root,
                    result_payload_value=payload,
                    instance_role="main",
                )
            self.assertIsNotNone(outcome)
            self.assertEqual("openai", outcome["businessKey"])
            self.assertEqual("sall.cc", outcome["domain"])
            state_path = Path(outcome["statePath"])
            self.assertTrue(state_path.is_file())
            state_payload = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertIn("businesses", state_payload)
            self.assertIn("openai", state_payload["businesses"])
            self.assertEqual(
                ["coolkid.icu"],
                state_payload["businesses"]["openai"]["explicitBlacklistDomains"],
            )

    def test_record_business_mailbox_domain_outcome_ignores_sms_resource_failure(self) -> None:
        for error_code in (
            "sms_no_selection_plan_candidates",
            "sms_no_productive_selection_plan_candidates",
        ):
            with self.subTest(error_code=error_code), tempfile.TemporaryDirectory() as tmp_dir:
                shared_root = Path(tmp_dir) / "shared"
                payload = {
                    "ok": False,
                    "errorStep": "obtain-codex-oauth",
                    "error": error_code,
                    "steps": {"acquire-mailbox": "ok"},
                    "outputs": {
                        "acquire-mailbox": {
                            "email": "user@sms-good-mailbox.test",
                            "provider": "stablemail",
                            "business_key": "openai",
                        }
                    },
                    "stepErrors": {
                        "obtain-codex-oauth": {
                            "message": error_code,
                        }
                    },
                }
                outcome = runner_mailbox.record_business_mailbox_domain_outcome(
                    shared_root=shared_root,
                    result_payload_value=payload,
                    instance_role="main",
                )
                self.assertIsNotNone(outcome)
                assert outcome is not None
                self.assertTrue(outcome["ignored"])
                self.assertEqual("external_sms_no_selection", outcome["ignoreReason"])
                self.assertFalse(Path(outcome["statePath"]).is_file())

    def test_record_business_mailbox_domain_outcome_ignores_easy_sms_provider_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            message = (
                'sms service POST /sms/sessions/open failed: HTTP 503 [code=Provider "yunduanxin" '
                "is currently unavailable: No eligible public numbers were available for a synthetic "
                'activation session.]: {"error":"Provider \\"yunduanxin\\" is currently unavailable"}'
            )
            payload = {
                "ok": False,
                "errorStep": "obtain-codex-oauth",
                "error": message,
                "steps": {"acquire-mailbox": "ok"},
                "outputs": {
                    "acquire-mailbox": {
                        "email": "user@sms-provider-empty.test",
                        "provider": "m2u",
                        "business_key": "openai",
                    }
                },
                "stepErrors": {
                    "obtain-codex-oauth": {
                        "code": "obtain_codex_oauth_failed",
                        "message": message,
                    }
                },
            }

            outcome = runner_mailbox.record_business_mailbox_domain_outcome(
                shared_root=shared_root,
                result_payload_value=payload,
                instance_role="main",
            )

            self.assertIsNotNone(outcome)
            assert outcome is not None
            self.assertTrue(outcome["ignored"])
            self.assertEqual("external_sms_no_selection", outcome["ignoreReason"])
            self.assertFalse(Path(outcome["statePath"]).is_file())

    def test_record_business_mailbox_domain_outcome_ignores_oauth_missing_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            message = (
                "{'category': 'operation_error', 'counts_toward_cooling': False, "
                "'message': 'missing_workspace', "
                "'details': {'step_type': 'obtain_codex_oauth'}}"
            )
            payload = {
                "ok": False,
                "errorStep": "obtain-codex-oauth",
                "error": message,
                "steps": {"acquire-mailbox": "ok"},
                "outputs": {
                    "acquire-mailbox": {
                        "email": "user@not-mailbox-workspace.test",
                        "provider": "tempmail-lol",
                        "business_key": "openai",
                    }
                },
                "stepErrors": {
                    "obtain-codex-oauth": {
                        "code": "obtain_codex_oauth_failed",
                        "message": message,
                    }
                },
            }

            outcome = runner_mailbox.record_business_mailbox_domain_outcome(
                shared_root=shared_root,
                result_payload_value=payload,
                instance_role="continue",
            )

            self.assertIsNotNone(outcome)
            assert outcome is not None
            self.assertTrue(outcome["ignored"])
            self.assertEqual("external_oauth_workspace", outcome["ignoreReason"])
            self.assertFalse(Path(outcome["statePath"]).is_file())

    def test_record_business_mailbox_domain_outcome_records_email_otp_failure_reason(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            payload = {
                "ok": False,
                "errorStep": "initialize-chatgpt-login-session",
                "steps": {"acquire-mailbox": "ok"},
                "outputs": {
                    "acquire-mailbox": {
                        "email": "user@slow-mailbox.test",
                        "provider": "slowmail",
                        "business_key": "openai",
                    }
                },
                "stepErrors": {
                    "initialize-chatgpt-login-session": {
                        "message": "chatgpt_login_email_otp_wait_failed: timeout waiting for 6-digit code",
                    }
                },
            }
            outcome = runner_mailbox.record_business_mailbox_domain_outcome(
                shared_root=shared_root,
                result_payload_value=payload,
                instance_role="main",
            )
            self.assertIsNotNone(outcome)
            assert outcome is not None
            self.assertFalse(outcome.get("ignored", False))
            self.assertEqual("email_otp_timeout", outcome["failureReason"])
            state_payload = json.loads(Path(outcome["statePath"]).read_text(encoding="utf-8"))
            domain_stats = state_payload["businesses"]["openai"]["domains"]["slow-mailbox.test"]
            provider_stats = state_payload["businesses"]["openai"]["providers"]["slowmail"]
            self.assertEqual({"email_otp_timeout": 1}, domain_stats["failureReasons"])
            self.assertEqual({"email_otp_timeout": 1}, provider_stats["failureReasons"])

    def test_record_business_mailbox_domain_outcome_classifies_create_account_email_otp_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            payload = {
                "ok": False,
                "errorStep": "create-openai-account",
                "steps": {"acquire-mailbox": "ok"},
                "outputs": {
                    "acquire-mailbox": {
                        "email": "user@slow-create.test",
                        "provider": "slowmail",
                        "business_key": "openai",
                    }
                },
                "stepErrors": {
                    "create-openai-account": {
                        "message": "timeout waiting for 6-digit code [mailbox_provider=slowmail]",
                    }
                },
            }
            outcome = runner_mailbox.record_business_mailbox_domain_outcome(
                shared_root=shared_root,
                result_payload_value=payload,
                instance_role="main",
            )
            self.assertIsNotNone(outcome)
            assert outcome is not None
            self.assertEqual("email_otp_timeout", outcome["failureReason"])

    def test_record_business_mailbox_domain_outcome_ignores_registration_blocked_create_account(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            payload = {
                "ok": False,
                "errorStep": "create-openai-account",
                "steps": {"acquire-mailbox": "ok"},
                "outputs": {
                    "acquire-mailbox": {
                        "email": "user@probably-ok.test",
                        "provider": "stablemail",
                        "business_key": "openai",
                    }
                },
                "stepErrors": {
                    "create-openai-account": {
                        "message": "create_account status=400 body={\"error\":{\"message\":\"Sorry, we cannot create your account with the given information.\"}}",
                    }
                },
            }
            outcome = runner_mailbox.record_business_mailbox_domain_outcome(
                shared_root=shared_root,
                result_payload_value=payload,
                instance_role="main",
            )
            self.assertIsNotNone(outcome)
            assert outcome is not None
            self.assertTrue(outcome["ignored"])
            self.assertEqual("external_registration_blocked", outcome["ignoreReason"])
            self.assertFalse(Path(outcome["statePath"]).is_file())

    def test_record_business_mailbox_domain_outcome_records_attributed_registration_disallowed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            payload = {
                "ok": False,
                "errorStep": "create-openai-account",
                "steps": {"acquire-mailbox": "ok"},
                "outputs": {
                    "acquire-mailbox": {
                        "email": "user@provider-domain.test",
                        "provider": "mailtm",
                        "business_key": "openai",
                    }
                },
                "stepErrors": {
                    "create-openai-account": {
                        "message": (
                            "create_account status=400 body={\"error\":{\"code\":\"registration_disallowed\"}} "
                            "[mailbox_provider=mailtm email=user@provider-domain.test]"
                        ),
                    }
                },
            }
            outcome = runner_mailbox.record_business_mailbox_domain_outcome(
                shared_root=shared_root,
                result_payload_value=payload,
                instance_role="main",
            )
            self.assertIsNotNone(outcome)
            assert outcome is not None
            self.assertNotIn("ignored", outcome)
            self.assertEqual("registration_disallowed", outcome["failureReason"])
            self.assertEqual("mailtm", outcome["provider"])
            self.assertEqual("provider-domain.test", outcome["domain"])
            self.assertTrue(outcome["blacklisted"])
            self.assertEqual("registration_disallowed", outcome["blacklistReason"])
            self.assertTrue(Path(outcome["statePath"]).is_file())

    def test_record_business_mailbox_domain_outcome_ignores_oauth_network_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            payload = {
                "ok": False,
                "errorStep": "obtain-codex-oauth",
                "steps": {"acquire-mailbox": "ok"},
                "outputs": {
                    "acquire-mailbox": {
                        "email": "user@not-mailbox-fault.test",
                        "provider": "tempmail-lol",
                        "business_key": "openai",
                    }
                },
                "stepErrors": {
                    "obtain-codex-oauth": {
                        "code": ErrorCodes.FLOW_TIMEOUT_EXCEEDED,
                        "message": "<urlopen error _ssl.c:1000: The handshake operation timed out>",
                    }
                },
            }

            outcome = runner_mailbox.record_business_mailbox_domain_outcome(
                shared_root=shared_root,
                result_payload_value=payload,
                instance_role="main",
            )

            self.assertIsNotNone(outcome)
            assert outcome is not None
            self.assertTrue(outcome["ignored"])
            self.assertEqual("external_proxy_or_auth", outcome["ignoreReason"])
            self.assertFalse(Path(outcome["statePath"]).is_file())

    def test_record_business_mailbox_domain_outcome_ignores_protocol_worker_capacity_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            message = (
                "{'category': 'service_unavailable', 'counts_toward_cooling': False, "
                "'message': 'no execution worker became available before acquire timeout', "
                "'details': {'operation': 'codex.semantic.step', "
                "'step_type': 'initialize_platform_organization'}}"
            )
            payload = {
                "ok": False,
                "errorStep": "initialize-platform-organization",
                "error": message,
                "steps": {"acquire-mailbox": "ok"},
                "outputs": {
                    "acquire-mailbox": {
                        "email": "user@not-mailbox-capacity.test",
                        "provider": "duckmail",
                        "business_key": "openai",
                    }
                },
                "stepErrors": {
                    "initialize-platform-organization": {
                        "code": "initialize_platform_organization_failed",
                        "message": message,
                    }
                },
            }

            outcome = runner_mailbox.record_business_mailbox_domain_outcome(
                shared_root=shared_root,
                result_payload_value=payload,
                instance_role="continue",
            )

            self.assertIsNotNone(outcome)
            assert outcome is not None
            self.assertTrue(outcome["ignored"])
            self.assertEqual("external_protocol_capacity", outcome["ignoreReason"])
            self.assertFalse(Path(outcome["statePath"]).is_file())

    def test_record_business_mailbox_domain_outcome_ignores_proxy_acquire_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            payload = {
                "ok": False,
                "errorStep": "acquire-proxy-chain",
                "error": "easy_proxy_checkout_failed: all nodes unavailable",
                "steps": {
                    "acquire-mailbox": "ok",
                    "acquire-proxy-chain": "error",
                },
                "outputs": {
                    "acquire-mailbox": {
                        "email": "user@not-mailbox-fault.test",
                        "provider": "tempmail-lol",
                        "business_key": "openai",
                    }
                },
                "stepErrors": {
                    "acquire-proxy-chain": {
                        "message": "easy_proxy_checkout_failed: all nodes unavailable",
                    }
                },
            }

            outcome = runner_mailbox.record_business_mailbox_domain_outcome(
                shared_root=shared_root,
                result_payload_value=payload,
                instance_role="main",
            )

            self.assertIsNotNone(outcome)
            assert outcome is not None
            self.assertTrue(outcome["ignored"])
            self.assertEqual("external_proxy_or_auth", outcome["ignoreReason"])
            self.assertFalse(Path(outcome["statePath"]).is_file())

    def test_record_business_mailbox_domain_outcome_success_clears_dynamic_blacklists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            state_path = shared_root / "others" / "register-mailbox-domain-state.json"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_text(
                json.dumps(
                    {
                        "schemaVersion": 3,
                        "businesses": {
                            "openai": {
                                "domains": {
                                    "recover.test": {
                                        "provider": "recovermail",
                                        "attempts": 20,
                                        "successes": 0,
                                        "failures": 20,
                                        "consecutiveFailures": 20,
                                        "failureReasons": {"email_otp_timeout": 20},
                                        "blacklisted": True,
                                        "blacklistReason": "email_otp_failure_threshold",
                                    }
                                },
                                "providers": {
                                    "recovermail": {
                                        "attempts": 20,
                                        "successes": 0,
                                        "failures": 20,
                                        "consecutiveFailures": 20,
                                        "failureReasons": {"email_otp_timeout": 20},
                                        "blacklisted": True,
                                        "blacklistReason": "provider_email_otp_failure_threshold",
                                    }
                                },
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            payload = {
                "ok": True,
                "steps": {"acquire-mailbox": "ok"},
                "outputs": {
                    "acquire-mailbox": {
                        "email": "user@recover.test",
                        "provider": "recovermail",
                        "business_key": "openai",
                    }
                },
            }

            outcome = runner_mailbox.record_business_mailbox_domain_outcome(
                shared_root=shared_root,
                result_payload_value=payload,
                instance_role="main",
            )

            self.assertIsNotNone(outcome)
            assert outcome is not None
            self.assertFalse(outcome["blacklisted"])
            self.assertEqual("", outcome["blacklistReason"])
            self.assertFalse(outcome["providerBlacklisted"])
            self.assertEqual("", outcome["providerBlacklistReason"])
            state_payload = json.loads(state_path.read_text(encoding="utf-8"))
            domain_stats = state_payload["businesses"]["openai"]["domains"]["recover.test"]
            provider_stats = state_payload["businesses"]["openai"]["providers"]["recovermail"]
            self.assertFalse(domain_stats["blacklisted"])
            self.assertEqual("", domain_stats["blacklistReason"])
            self.assertEqual({}, domain_stats["failureReasons"])
            self.assertEqual(0, domain_stats["consecutiveFailures"])
            self.assertFalse(provider_stats["blacklisted"])
            self.assertEqual("", provider_stats["blacklistReason"])
            self.assertEqual({}, provider_stats["failureReasons"])
            self.assertEqual(0, provider_stats["consecutiveFailures"])

    def test_record_business_mailbox_domain_outcome_counts_small_success_artifact_as_mailbox_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            state_path = shared_root / "others" / "register-mailbox-domain-state.json"
            artifact_path = Path(tmp_dir) / "small-success.json"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_text(
                json.dumps(
                    {
                        "schemaVersion": 3,
                        "businesses": {
                            "openai": {
                                "domains": {
                                    "zhooo.ggff.net": {
                                        "provider": "moemail",
                                        "attempts": 58,
                                        "successes": 0,
                                        "failures": 58,
                                        "consecutiveFailures": 58,
                                        "failureReasons": {"email_otp_timeout": 58},
                                        "blacklisted": True,
                                        "blacklistReason": "failure_rate_threshold",
                                    }
                                },
                                "providers": {
                                    "moemail": {
                                        "attempts": 58,
                                        "successes": 0,
                                        "failures": 58,
                                        "consecutiveFailures": 58,
                                        "failureReasons": {"email_otp_timeout": 58},
                                        "blacklisted": True,
                                        "blacklistReason": "provider_failure_rate_threshold",
                                    }
                                },
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            artifact_path.write_text(
                json.dumps(
                    {
                        "outcome": "small_success",
                        "email": "user@zhooo.ggff.net",
                        "mailboxRef": "moemail:session-small",
                        "mailboxSessionId": "session-small",
                        "createdAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                        "platformOrganization": {"status": "completed"},
                        "chatgptLogin": {"status": "completed", "workspaceId": "ws_123"},
                        "chatgptLoginDetails": {
                            "clientBootstrap": {"authStatus": "logged_in", "structure": "personal"}
                        },
                    }
                ),
                encoding="utf-8",
            )
            payload = {
                "ok": False,
                "errorStep": "obtain-codex-oauth",
                "error": "phone_verification_submitted_small_success phoneVerificationFailureStage=wait_sms_code",
                "steps": {"acquire-mailbox": "ok", "create-openai-account": "ok"},
                "outputs": {
                    "acquire-mailbox": {
                        "email": "user@zhooo.ggff.net",
                        "provider": "moemail",
                        "mailbox_ref": "moemail:session-small",
                        "business_key": "openai",
                    },
                    "create-openai-account": {
                        "email": "user@zhooo.ggff.net",
                        "storage_path": str(artifact_path),
                    },
                    "obtain-codex-oauth": {
                        "status": "phone_verification_submitted_small_success",
                        "phoneVerificationFailureStage": "wait_sms_code",
                    },
                },
                "stepErrors": {
                    "obtain-codex-oauth": {
                        "message": "phone_verification_submitted_small_success wait_sms_code",
                    }
                },
            }

            outcome = runner_mailbox.record_business_mailbox_domain_outcome(
                shared_root=shared_root,
                result_payload_value=payload,
                instance_role="main",
            )

            self.assertIsNotNone(outcome)
            assert outcome is not None
            self.assertNotIn("ignored", outcome)
            self.assertEqual("success", outcome["lastOutcome"])
            self.assertEqual("openai_oauth_artifact", outcome["qualitySuccessReason"])
            self.assertFalse(outcome["blacklisted"])
            self.assertEqual("", outcome["blacklistReason"])
            self.assertFalse(outcome["providerBlacklisted"])
            self.assertEqual("", outcome["providerBlacklistReason"])
            state_payload = json.loads(state_path.read_text(encoding="utf-8"))
            domain_stats = state_payload["businesses"]["openai"]["domains"]["zhooo.ggff.net"]
            provider_stats = state_payload["businesses"]["openai"]["providers"]["moemail"]
            self.assertEqual(59, domain_stats["attempts"])
            self.assertEqual(1, domain_stats["successes"])
            self.assertEqual(58, domain_stats["failures"])
            self.assertEqual("success", domain_stats["lastOutcome"])
            self.assertEqual({}, domain_stats["failureReasons"])
            self.assertFalse(domain_stats["blacklisted"])
            self.assertEqual(0, provider_stats["consecutiveFailures"])
            self.assertFalse(provider_stats["blacklisted"])

    def test_record_business_mailbox_domain_outcome_records_retry_attempt_failure_before_final_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            payload = {
                "ok": True,
                "steps": {"acquire-mailbox": "ok", "create-openai-account": "ok"},
                "outputs": {
                    "acquire-mailbox": {
                        "email": "user2@example.com",
                        "provider": "cloudflare_temp_email",
                        "business_key": "openai",
                    },
                    "mailbox-attempt-outcomes": [
                        {
                            "outcome": "failure",
                            "failureReason": "create_account_user_register_400",
                            "failureClass": "weak_attributed_generic_register_400",
                            "errorCode": "user_register_400",
                            "provider": "m2u",
                            "domain": "kkb.qzz.io",
                            "email": "user1@kkb.qzz.io",
                            "mailbox_ref": "m2u:first",
                            "mailbox_session_id": "first",
                            "business_key": "openai",
                            "stepId": "create-openai-account",
                            "attempt": 1,
                        }
                    ],
                },
            }

            with mock.patch.object(runner_mailbox, "report_mailbox_outcome") as report_outcome:
                outcome = runner_mailbox.record_business_mailbox_domain_outcome(
                    shared_root=shared_root,
                    result_payload_value=payload,
                    instance_role="main",
                )

            self.assertIsNotNone(outcome)
            assert outcome is not None
            report_outcome.assert_called_once_with(
                session_id="first",
                success=False,
                failure_reason="create_account_user_register_400",
                business_flow="openai",
                retry_layer="step",
                attribution_strength="weak",
                attribution_kind="mailbox_domain_risk",
                provider_type_key="m2u",
                domain="kkb.qzz.io",
                email_address="user1@kkb.qzz.io",
                avoid_in_current_attempt=True,
                global_blacklist=False,
                cooldown_seconds=0,
                source="easyregister",
            )
            self.assertEqual("example.com", outcome["domain"])
            self.assertIn("attemptOutcomes", outcome)
            self.assertEqual("kkb.qzz.io", outcome["attemptOutcomes"][0]["domain"])
            self.assertFalse(outcome["attemptOutcomes"][0]["blacklisted"])
            self.assertEqual("", outcome["attemptOutcomes"][0]["blacklistReason"])
            state_payload = json.loads(Path(outcome["statePath"]).read_text(encoding="utf-8"))
            domains = state_payload["businesses"]["openai"]["domains"]
            providers = state_payload["businesses"]["openai"]["providers"]
            self.assertEqual("create_account_user_register_400", domains["kkb.qzz.io"]["lastFailureReason"])
            self.assertEqual({"create_account_user_register_400": 1}, domains["kkb.qzz.io"]["failureReasons"])
            self.assertEqual("success", domains["example.com"]["lastOutcome"])
            self.assertEqual({"create_account_user_register_400": 1}, providers["m2u"]["failureReasons"])

    def test_record_business_mailbox_domain_outcome_reports_unsupported_email_without_provider_global_blacklist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            payload = {
                "ok": True,
                "steps": {"acquire-mailbox": "ok", "create-openai-account": "ok"},
                "outputs": {
                    "acquire-mailbox": {
                        "email": "user2@example.com",
                        "provider": "cloudflare_temp_email",
                        "business_key": "openai",
                    },
                    "mailbox-attempt-outcomes": [
                        {
                            "outcome": "failure",
                            "failureReason": "unsupported_email",
                            "failureClass": "strong_mailbox_unsupported",
                            "errorCode": "unsupported_email",
                            "provider": "mailtm",
                            "domain": "blocked.test",
                            "email": "user1@blocked.test",
                            "mailbox_ref": "mailtm:first",
                            "mailbox_session_id": "first",
                            "business_key": "openai",
                            "stepId": "create-openai-account",
                            "attempt": 1,
                        }
                    ],
                },
            }

            with mock.patch.object(runner_mailbox, "report_mailbox_outcome") as report_outcome:
                outcome = runner_mailbox.record_business_mailbox_domain_outcome(
                    shared_root=shared_root,
                    result_payload_value=payload,
                    instance_role="main",
                )

            self.assertIsNotNone(outcome)
            assert outcome is not None
            report_outcome.assert_called_once_with(
                session_id="first",
                success=False,
                failure_reason="unsupported_email",
                business_flow="openai",
                retry_layer="step",
                attribution_strength="strong",
                attribution_kind="mailbox_domain_risk",
                provider_type_key="mailtm",
                domain="blocked.test",
                email_address="user1@blocked.test",
                avoid_in_current_attempt=True,
                global_blacklist=False,
                cooldown_seconds=0,
                source="easyregister",
            )
            self.assertTrue(outcome["attemptOutcomes"][0]["blacklisted"])
            self.assertEqual("unsupported_email", outcome["attemptOutcomes"][0]["blacklistReason"])
            state_payload = json.loads(Path(outcome["statePath"]).read_text(encoding="utf-8"))
            blocked_stats = state_payload["businesses"]["openai"]["domains"]["blocked.test"]
            self.assertTrue(blocked_stats["blacklisted"])
            self.assertEqual("unsupported_email", blocked_stats["blacklistReason"])

    def test_record_business_mailbox_domain_outcome_blacklists_email_otp_failures_quickly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"

            def _payload(domain: str) -> dict[str, object]:
                return {
                    "ok": False,
                    "errorStep": "create-openai-account",
                    "steps": {"acquire-mailbox": "ok"},
                    "outputs": {
                        "acquire-mailbox": {
                            "email": f"user@{domain}",
                            "provider": "slowmail",
                            "business_key": "openai",
                        }
                    },
                    "stepErrors": {
                        "create-openai-account": {
                            "message": "timeout waiting for 6-digit code [mailbox_provider=slowmail]",
                        }
                    },
                }

            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_MAILBOX_EMAIL_OTP_FAILURE_BLACKLIST_THRESHOLD": "2",
                    "REGISTER_MAILBOX_EMAIL_OTP_PROVIDER_FAILURE_BLACKLIST_THRESHOLD": "3",
                    "REGISTER_MAILBOX_DOMAIN_BLACKLIST_MIN_ATTEMPTS": "100",
                    "REGISTER_MAILBOX_DOMAIN_CONSECUTIVE_FAILURE_BLACKLIST_THRESHOLD": "100",
                },
                clear=True,
            ):
                first = runner_mailbox.record_business_mailbox_domain_outcome(
                    shared_root=shared_root,
                    result_payload_value=_payload("one.test"),
                    instance_role="main",
                )
                second = runner_mailbox.record_business_mailbox_domain_outcome(
                    shared_root=shared_root,
                    result_payload_value=_payload("one.test"),
                    instance_role="main",
                )
                third = runner_mailbox.record_business_mailbox_domain_outcome(
                    shared_root=shared_root,
                    result_payload_value=_payload("two.test"),
                    instance_role="main",
                )
            assert first is not None and second is not None and third is not None
            self.assertFalse(first["blacklisted"])
            self.assertEqual("email_otp_failure_threshold", second["blacklistReason"])
            self.assertTrue(second["blacklisted"])
            self.assertEqual("provider_email_otp_failure_threshold", third["providerBlacklistReason"])
            self.assertTrue(third["providerBlacklisted"])

    def test_record_business_mailbox_domain_outcome_does_not_provider_blacklist_high_success_otp_noise(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            state_path = shared_root / "others" / "register-mailbox-domain-state.json"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_text(
                json.dumps(
                    {
                        "schemaVersion": 3,
                        "businesses": {
                            "openai": {
                                "providers": {
                                    "tempmail-lol": {
                                        "attempts": 407,
                                        "successes": 315,
                                        "failures": 92,
                                        "consecutiveFailures": 1,
                                        "failureReasons": {"email_otp_timeout": 12},
                                        "failureRate": 22.6,
                                        "blacklisted": False,
                                        "blacklistReason": "",
                                    }
                                }
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            payload = {
                "ok": False,
                "errorStep": "create-openai-account",
                "steps": {"acquire-mailbox": "ok"},
                "outputs": {
                    "acquire-mailbox": {
                        "email": "user@ok.test",
                        "provider": "tempmail-lol",
                        "business_key": "openai",
                    }
                },
                "stepErrors": {
                    "create-openai-account": {
                        "message": "timeout waiting for 6-digit code [mailbox_provider=tempmail-lol]",
                    }
                },
            }

            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_MAILBOX_EMAIL_OTP_FAILURE_BLACKLIST_THRESHOLD": "6",
                    "REGISTER_MAILBOX_EMAIL_OTP_PROVIDER_FAILURE_BLACKLIST_THRESHOLD": "6",
                    "REGISTER_MAILBOX_DOMAIN_BLACKLIST_MIN_ATTEMPTS": "50",
                    "REGISTER_MAILBOX_DOMAIN_BLACKLIST_FAILURE_RATE": "95",
                    "REGISTER_MAILBOX_PROVIDER_BLACKLIST_RECOVERY_MIN_SUCCESSES": "10",
                    "REGISTER_MAILBOX_PROVIDER_BLACKLIST_RECOVERY_MIN_SUCCESS_RATE": "20",
                },
                clear=True,
            ):
                outcome = runner_mailbox.record_business_mailbox_domain_outcome(
                    shared_root=shared_root,
                    result_payload_value=payload,
                    instance_role="main",
                )

            self.assertIsNotNone(outcome)
            assert outcome is not None
            self.assertFalse(outcome["providerBlacklisted"])
            self.assertEqual("", outcome["providerBlacklistReason"])
            state_payload = json.loads(state_path.read_text(encoding="utf-8"))
            provider_stats = state_payload["businesses"]["openai"]["providers"]["tempmail-lol"]
            self.assertFalse(provider_stats["blacklisted"])
            self.assertEqual("", provider_stats["blacklistReason"])
            self.assertEqual(13, provider_stats["failureReasons"]["email_otp_timeout"])

    def test_record_business_mailbox_domain_outcome_reblacklists_high_success_provider_after_recent_otp_streak(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            state_path = shared_root / "others" / "register-mailbox-domain-state.json"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_text(
                json.dumps(
                    {
                        "schemaVersion": 3,
                        "businesses": {
                            "openai": {
                                "providers": {
                                    "tempmail-lol": {
                                        "attempts": 440,
                                        "successes": 315,
                                        "failures": 125,
                                        "consecutiveFailures": 11,
                                        "failureReasons": {"email_otp_timeout": 11},
                                        "failureRate": 28.4,
                                        "blacklisted": False,
                                        "blacklistReason": "",
                                    }
                                }
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            payload = {
                "ok": False,
                "errorStep": "create-openai-account",
                "steps": {"acquire-mailbox": "ok"},
                "outputs": {
                    "acquire-mailbox": {
                        "email": "user@ok.test",
                        "provider": "tempmail-lol",
                        "business_key": "openai",
                    }
                },
                "stepErrors": {
                    "create-openai-account": {
                        "message": "timeout waiting for 6-digit code [mailbox_provider=tempmail-lol]",
                    }
                },
            }

            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_MAILBOX_EMAIL_OTP_FAILURE_BLACKLIST_THRESHOLD": "6",
                    "REGISTER_MAILBOX_EMAIL_OTP_PROVIDER_FAILURE_BLACKLIST_THRESHOLD": "12",
                    "REGISTER_MAILBOX_DOMAIN_BLACKLIST_MIN_ATTEMPTS": "50",
                    "REGISTER_MAILBOX_DOMAIN_BLACKLIST_FAILURE_RATE": "95",
                    "REGISTER_MAILBOX_PROVIDER_BLACKLIST_RECOVERY_MIN_SUCCESSES": "10",
                    "REGISTER_MAILBOX_PROVIDER_BLACKLIST_RECOVERY_MIN_SUCCESS_RATE": "20",
                },
                clear=True,
            ):
                outcome = runner_mailbox.record_business_mailbox_domain_outcome(
                    shared_root=shared_root,
                    result_payload_value=payload,
                    instance_role="main",
                )

            self.assertIsNotNone(outcome)
            assert outcome is not None
            self.assertEqual("provider_email_otp_failure_threshold", outcome["providerBlacklistReason"])
            self.assertTrue(outcome["providerBlacklisted"])
            state_payload = json.loads(state_path.read_text(encoding="utf-8"))
            provider_stats = state_payload["businesses"]["openai"]["providers"]["tempmail-lol"]
            self.assertTrue(provider_stats["blacklisted"])
            self.assertEqual("provider_email_otp_failure_threshold", provider_stats["blacklistReason"])
            self.assertEqual(12, provider_stats["consecutiveFailures"])

    def test_record_business_mailbox_domain_outcome_blacklists_high_success_provider_after_recent_generic_streak(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            state_path = shared_root / "others" / "register-mailbox-domain-state.json"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_text(
                json.dumps(
                    {
                        "schemaVersion": 3,
                        "businesses": {
                            "openai": {
                                "providers": {
                                    "cloudflare_temp_email": {
                                        "attempts": 229,
                                        "successes": 27,
                                        "failures": 202,
                                        "consecutiveFailures": 11,
                                        "failureReasons": {"create_account_user_register_400": 11},
                                        "failureRate": 88.2,
                                        "blacklisted": False,
                                        "blacklistReason": "",
                                    }
                                }
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            payload = {
                "ok": False,
                "errorStep": "create-openai-account",
                "steps": {"acquire-mailbox": "ok"},
                "outputs": {
                    "acquire-mailbox": {
                        "email": "user@ok.test",
                        "provider": "cloudflare_temp_email",
                        "business_key": "openai",
                    }
                },
                "stepErrors": {
                    "create-openai-account": {
                        "code": "user_register_400",
                        "message": "create_account user_register_400 [mailbox_provider=cloudflare_temp_email]",
                    }
                },
            }

            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_MAILBOX_EMAIL_OTP_FAILURE_BLACKLIST_THRESHOLD": "6",
                    "REGISTER_MAILBOX_EMAIL_OTP_PROVIDER_FAILURE_BLACKLIST_THRESHOLD": "12",
                    "REGISTER_MAILBOX_DOMAIN_BLACKLIST_MIN_ATTEMPTS": "50",
                    "REGISTER_MAILBOX_DOMAIN_BLACKLIST_FAILURE_RATE": "95",
                    "REGISTER_MAILBOX_PROVIDER_BLACKLIST_RECOVERY_MIN_SUCCESSES": "10",
                    "REGISTER_MAILBOX_PROVIDER_BLACKLIST_RECOVERY_MIN_SUCCESS_RATE": "20",
                },
                clear=True,
            ):
                outcome = runner_mailbox.record_business_mailbox_domain_outcome(
                    shared_root=shared_root,
                    result_payload_value=payload,
                    instance_role="main",
                )

            self.assertIsNotNone(outcome)
            assert outcome is not None
            self.assertEqual("provider_consecutive_failures_threshold", outcome["providerBlacklistReason"])
            self.assertTrue(outcome["providerBlacklisted"])
            state_payload = json.loads(state_path.read_text(encoding="utf-8"))
            provider_stats = state_payload["businesses"]["openai"]["providers"]["cloudflare_temp_email"]
            self.assertTrue(provider_stats["blacklisted"])
            self.assertEqual("provider_consecutive_failures_threshold", provider_stats["blacklistReason"])
            self.assertEqual(12, provider_stats["consecutiveFailures"])

    def test_record_business_mailbox_domain_outcome_does_not_blacklist_provider_after_generic_create_account_failures(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            state_path = shared_root / "others" / "register-mailbox-domain-state.json"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_text(
                json.dumps(
                    {
                        "schemaVersion": 3,
                        "businesses": {
                            "openai": {
                                "providers": {
                                    "moemail": {
                                        "attempts": 176,
                                        "successes": 54,
                                        "failures": 122,
                                        "consecutiveFailures": 11,
                                        "failureReasons": {"create_account_failure": 11},
                                        "failureRate": 69.3,
                                        "blacklisted": False,
                                        "blacklistReason": "",
                                    }
                                }
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            payload = {
                "ok": False,
                "errorStep": "create-openai-account",
                "steps": {"acquire-mailbox": "ok"},
                "outputs": {
                    "acquire-mailbox": {
                        "email": "user@ok.test",
                        "provider": "moemail",
                        "business_key": "openai",
                    }
                },
                "stepErrors": {
                    "create-openai-account": {
                        "message": "Failed to create account. Please try again.",
                    }
                },
            }

            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_MAILBOX_EMAIL_OTP_FAILURE_BLACKLIST_THRESHOLD": "6",
                    "REGISTER_MAILBOX_EMAIL_OTP_PROVIDER_FAILURE_BLACKLIST_THRESHOLD": "12",
                    "REGISTER_MAILBOX_DOMAIN_BLACKLIST_MIN_ATTEMPTS": "50",
                    "REGISTER_MAILBOX_DOMAIN_BLACKLIST_FAILURE_RATE": "95",
                    "REGISTER_MAILBOX_PROVIDER_BLACKLIST_RECOVERY_MIN_SUCCESSES": "10",
                    "REGISTER_MAILBOX_PROVIDER_BLACKLIST_RECOVERY_MIN_SUCCESS_RATE": "20",
                },
                clear=True,
            ):
                outcome = runner_mailbox.record_business_mailbox_domain_outcome(
                    shared_root=shared_root,
                    result_payload_value=payload,
                    instance_role="main",
                )

            self.assertIsNotNone(outcome)
            assert outcome is not None
            self.assertEqual("create_account_failure", outcome["failureReason"])
            self.assertFalse(outcome["providerBlacklisted"])
            self.assertEqual("", outcome["providerBlacklistReason"])
            state_payload = json.loads(state_path.read_text(encoding="utf-8"))
            provider_stats = state_payload["businesses"]["openai"]["providers"]["moemail"]
            self.assertFalse(provider_stats["blacklisted"])
            self.assertEqual("", provider_stats["blacklistReason"])
            self.assertEqual(12, provider_stats["consecutiveFailures"])

    def test_record_business_mailbox_domain_outcome_aggregates_email_otp_failure_reasons_for_provider(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"

            def _payload(domain: str, message: str) -> dict[str, object]:
                return {
                    "ok": False,
                    "errorStep": "create-openai-account",
                    "steps": {"acquire-mailbox": "ok"},
                    "outputs": {
                        "acquire-mailbox": {
                            "email": f"user@{domain}",
                            "provider": "mixedslow",
                            "business_key": "openai",
                        }
                    },
                    "stepErrors": {
                        "create-openai-account": {
                            "message": message,
                        }
                    },
                }

            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_MAILBOX_EMAIL_OTP_FAILURE_BLACKLIST_THRESHOLD": "100",
                    "REGISTER_MAILBOX_EMAIL_OTP_PROVIDER_FAILURE_BLACKLIST_THRESHOLD": "3",
                    "REGISTER_MAILBOX_DOMAIN_BLACKLIST_MIN_ATTEMPTS": "100",
                    "REGISTER_MAILBOX_DOMAIN_CONSECUTIVE_FAILURE_BLACKLIST_THRESHOLD": "100",
                },
                clear=True,
            ):
                first = runner_mailbox.record_business_mailbox_domain_outcome(
                    shared_root=shared_root,
                    result_payload_value=_payload(
                        "one.test",
                        "timeout waiting for 6-digit code [mailbox_provider=mixedslow]",
                    ),
                    instance_role="main",
                )
                second = runner_mailbox.record_business_mailbox_domain_outcome(
                    shared_root=shared_root,
                    result_payload_value=_payload(
                        "two.test",
                        "chatgpt_login_otp_validate_failed status=401 body={\"error\":{\"code\":\"wrong_email_otp_code\"}}",
                    ),
                    instance_role="main",
                )
                third = runner_mailbox.record_business_mailbox_domain_outcome(
                    shared_root=shared_root,
                    result_payload_value=_payload(
                        "three.test",
                        "timeout waiting for 6-digit code [mailbox_provider=mixedslow]",
                    ),
                    instance_role="main",
                )

            assert first is not None and second is not None and third is not None
            self.assertFalse(first["providerBlacklisted"])
            self.assertFalse(second["providerBlacklisted"])
            self.assertEqual("provider_email_otp_failure_threshold", third["providerBlacklistReason"])
            self.assertTrue(third["providerBlacklisted"])

    def test_mailbox_domain_blacklist_reason_requires_unsupported_email(self) -> None:
        unsupported_payload = {
            "stepErrors": {
                "create-openai-account": {
                    "message": "create_account status=400 body={\"error\":{\"code\":\"unsupported_email\"}}",
                }
            }
        }
        generic_payload = {
            "stepErrors": {
                "create-openai-account": {
                    "message": "Failed to create account. Please try again.",
                }
            }
        }
        self.assertEqual(
            "unsupported_email",
            runner_mailbox.mailbox_domain_blacklist_reason(result_payload_value=unsupported_payload),
        )
        self.assertEqual(
            "",
            runner_mailbox.mailbox_domain_blacklist_reason(result_payload_value=generic_payload),
        )

    def test_mailbox_domain_blacklist_reason_treats_invalid_username_as_unsupported_email(self) -> None:
        payload = {
            "stepErrors": {
                "create-openai-account": {
                    "message": (
                        "authorize_continue status=400 body={\"error\":{\"code\":\"invalid_username\","
                        "\"message\":\"Invalid username\"}} "
                        "[mailbox_provider=etempmail email=user@example.test]"
                    ),
                }
            }
        }
        self.assertEqual(
            "unsupported_email",
            runner_mailbox.mailbox_domain_blacklist_reason(result_payload_value=payload),
        )
        self.assertEqual(
            "unsupported_email",
            runner_mailbox.mailbox_failure_reason(
                result_payload_value={
                    "ok": False,
                    "errorStep": "create-openai-account",
                    **payload,
                }
            ),
        )

    def test_record_business_mailbox_domain_outcome_tracks_non_moemail_provider(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            payload = {
                "ok": False,
                "outputs": {
                    "acquire-mailbox": {
                        "email": "user@cnmlgb.de",
                        "provider": "etempmail",
                        "business_key": "openai",
                    }
                },
                "stepErrors": {
                    "create-openai-account": {
                        "message": "create_account status=400 body={\"error\":{\"code\":\"invalid_request_error\"}}",
                    }
                },
            }
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_MAILBOX_BUSINESS_KEY": "generic",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": (
                        '{"openai":{"explicitBlacklistDomains":["coolkid.icu"],"providerBlacklist":[]}}'
                    ),
                },
                clear=True,
            ):
                outcome = runner_mailbox.record_business_mailbox_domain_outcome(
                    shared_root=shared_root,
                    result_payload_value=payload,
                    instance_role="main",
                )
            self.assertIsNotNone(outcome)
            assert outcome is not None
            self.assertEqual("etempmail", outcome["provider"])
            self.assertEqual("cnmlgb.de", outcome["domain"])

    def test_record_business_mailbox_domain_outcome_blacklists_after_consecutive_failures(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            payload = {
                "ok": False,
                "outputs": {
                    "acquire-mailbox": {
                        "email": "user@cnmlgb.de",
                        "provider": "moemail",
                        "business_key": "openai",
                    }
                },
                "stepErrors": {
                    "create-openai-account": {
                        "message": "create_account status=400 body={\"error\":{\"code\":\"invalid_request_error\"}}",
                    }
                },
            }
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_MAILBOX_BUSINESS_KEY": "generic",
                    "REGISTER_MAILBOX_DOMAIN_CONSECUTIVE_FAILURE_BLACKLIST_THRESHOLD": "3",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": (
                        '{"openai":{"explicitBlacklistDomains":["coolkid.icu"],"providerBlacklist":[]}}'
                    ),
                },
                clear=True,
            ):
                first = runner_mailbox.record_business_mailbox_domain_outcome(
                    shared_root=shared_root,
                    result_payload_value=payload,
                    instance_role="main",
                )
                second = runner_mailbox.record_business_mailbox_domain_outcome(
                    shared_root=shared_root,
                    result_payload_value=payload,
                    instance_role="main",
                )
                third = runner_mailbox.record_business_mailbox_domain_outcome(
                    shared_root=shared_root,
                    result_payload_value=payload,
                    instance_role="main",
                )
            assert first is not None and second is not None and third is not None
            self.assertEqual(1, first["consecutiveFailures"])
            self.assertFalse(first["blacklisted"])
            self.assertEqual(2, second["consecutiveFailures"])
            self.assertFalse(second["blacklisted"])
            self.assertEqual(3, third["consecutiveFailures"])
            self.assertTrue(third["blacklisted"])
            self.assertEqual("consecutive_failures_threshold", third["blacklistReason"])

    def test_record_business_mailbox_domain_outcome_blacklists_after_failure_rate_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            payload = {
                "ok": False,
                "outputs": {
                    "acquire-mailbox": {
                        "email": "user@flaky.test",
                        "provider": "mailbox-provider",
                        "business_key": "openai",
                    }
                },
                "stepErrors": {
                    "create-openai-account": {
                        "message": "create_account status=400 body={\"error\":{\"code\":\"invalid_request_error\"}}",
                    }
                },
            }
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_MAILBOX_BUSINESS_KEY": "generic",
                    "REGISTER_MAILBOX_DOMAIN_BLACKLIST_MIN_ATTEMPTS": "3",
                    "REGISTER_MAILBOX_DOMAIN_BLACKLIST_FAILURE_RATE": "90",
                    "REGISTER_MAILBOX_DOMAIN_CONSECUTIVE_FAILURE_BLACKLIST_THRESHOLD": "100",
                },
                clear=True,
            ):
                first = runner_mailbox.record_business_mailbox_domain_outcome(
                    shared_root=shared_root,
                    result_payload_value=payload,
                    instance_role="main",
                )
                second = runner_mailbox.record_business_mailbox_domain_outcome(
                    shared_root=shared_root,
                    result_payload_value=payload,
                    instance_role="main",
                )
                third = runner_mailbox.record_business_mailbox_domain_outcome(
                    shared_root=shared_root,
                    result_payload_value=payload,
                    instance_role="main",
                )
            assert first is not None and second is not None and third is not None
            self.assertFalse(first["blacklisted"])
            self.assertFalse(second["blacklisted"])
            self.assertTrue(third["blacklisted"])
            self.assertEqual("failure_rate_threshold", third["blacklistReason"])
            self.assertEqual(100.0, third["failureRate"])

    def test_record_business_mailbox_provider_outcome_blacklists_after_failure_rate_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"

            def _payload(domain: str) -> dict[str, object]:
                return {
                    "ok": False,
                    "outputs": {
                        "acquire-mailbox": {
                            "email": f"user@{domain}",
                            "provider": "badmail",
                            "business_key": "openai",
                        }
                    },
                    "stepErrors": {
                        "create-openai-account": {
                            "message": "create_account status=400 body={\"error\":{\"code\":\"invalid_request_error\"}}",
                        }
                    },
                }

            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_MAILBOX_BUSINESS_KEY": "generic",
                    "REGISTER_MAILBOX_DOMAIN_BLACKLIST_MIN_ATTEMPTS": "3",
                    "REGISTER_MAILBOX_DOMAIN_BLACKLIST_FAILURE_RATE": "90",
                    "REGISTER_MAILBOX_DOMAIN_CONSECUTIVE_FAILURE_BLACKLIST_THRESHOLD": "100",
                },
                clear=True,
            ):
                first = runner_mailbox.record_business_mailbox_domain_outcome(
                    shared_root=shared_root,
                    result_payload_value=_payload("one.test"),
                    instance_role="main",
                )
                second = runner_mailbox.record_business_mailbox_domain_outcome(
                    shared_root=shared_root,
                    result_payload_value=_payload("two.test"),
                    instance_role="main",
                )
                third = runner_mailbox.record_business_mailbox_domain_outcome(
                    shared_root=shared_root,
                    result_payload_value=_payload("three.test"),
                    instance_role="main",
                )
            assert first is not None and second is not None and third is not None
            self.assertFalse(first["providerBlacklisted"])
            self.assertFalse(second["providerBlacklisted"])
            self.assertTrue(third["providerBlacklisted"])
            self.assertEqual("provider_failure_rate_threshold", third["providerBlacklistReason"])
            state_payload = json.loads(Path(third["statePath"]).read_text(encoding="utf-8"))
            provider_stats = state_payload["businesses"]["openai"]["providers"]["badmail"]
            self.assertEqual(3, provider_stats["attempts"])
            self.assertTrue(provider_stats["blacklisted"])

    def test_mark_mailbox_capacity_failure_respects_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            with mock.patch.dict(
                os.environ,
                {"REGISTER_MAILBOX_CLEANUP_FAILURE_THRESHOLD": "3"},
                clear=True,
            ):
                result = runner_mailbox.mark_mailbox_capacity_failure(
                    shared_root=shared_root,
                    detail="mailbox capacity unavailable",
                )
            self.assertEqual("recovery_threshold_not_reached", result["status"])
            self.assertEqual(1, result["consecutiveFailures"])


class RunnerTeamCleanupTests(unittest.TestCase):
    def test_team_capacity_failure_detail_uses_structured_code(self) -> None:
        payload = {
            "errorStep": "invite-codex-member",
            "stepErrors": {
                "invite-codex-member": {
                    "code": ErrorCodes.TEAM_SEATS_FULL,
                    "message": "workspace full",
                }
            },
        }
        detail = runner_team_cleanup.team_capacity_failure_detail(result_payload_value=payload)
        self.assertIn("workspace full", detail)

    def test_capacity_cooldown_state_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            team_auth_path = str(shared_root / "mother.json")
            runner_team_cleanup.mark_team_auth_capacity_cooldown(
                shared_root=shared_root,
                team_auth_path=team_auth_path,
                cooldown_seconds=60.0,
                detail="capacity full",
            )
            self.assertTrue(
                runner_team_cleanup.team_auth_is_capacity_cooled(
                    shared_root=shared_root,
                    team_auth_path=team_auth_path,
                )
            )
            runner_team_cleanup.clear_team_auth_capacity_cooldown(
                shared_root=shared_root,
                team_auth_path=team_auth_path,
            )
            self.assertFalse(
                runner_team_cleanup.team_auth_is_capacity_cooled(
                    shared_root=shared_root,
                    team_auth_path=team_auth_path,
                )
            )


class RunnerTeamAuthTests(unittest.TestCase):
    def test_team_auth_pool_candidates_dedupes_same_identity_and_prefers_first_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            local_dir = tmp_path / "local"
            readonly_dir = tmp_path / "readonly"
            local_dir.mkdir(parents=True, exist_ok=True)
            readonly_dir.mkdir(parents=True, exist_ok=True)
            local_path = local_dir / "codex-team-mother-demo@example.com.json"
            readonly_path = readonly_dir / "codex-team-mother-demo@example.com.json"
            payload = {"email": "demo@example.com", "account_id": "acct_123"}
            local_path.write_text(json.dumps(payload), encoding="utf-8")
            readonly_path.write_text(json.dumps(payload), encoding="utf-8")
            with mock.patch.object(
                runner_team_auth_pool,
                "team_auth_payload_is_mother",
                return_value=True,
            ):
                candidates = runner_team_auth_pool.team_auth_pool_candidates(
                    candidate_dirs=[str(local_dir), str(readonly_dir)]
                )
        self.assertEqual([str(local_path.resolve())], candidates)

    def test_temp_blacklist_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            source_path = shared_root / "mother.json"
            source_path.parent.mkdir(parents=True, exist_ok=True)
            source_path.write_text(
                '{"email":"mother@example.com","account_id":"acct_123"}',
                encoding="utf-8",
            )
            team_auth_path = str(source_path)
            identity = {
                "original_name": "mother.json",
                "email": "mother@example.com",
                "account_id": "acct_123",
            }
            record = runner_team_auth.mark_team_auth_temporary_blacklist(
                shared_root=shared_root,
                team_auth_path=team_auth_path,
                identity=identity,
                reason="token invalidated",
                blacklist_seconds=120.0,
                worker_label="worker-01",
                task_index=1,
            )
            self.assertIsNotNone(record)
            blacklisted, _ = runner_team_auth.team_auth_is_temp_blacklisted(
                shared_root=shared_root,
                team_auth_path=team_auth_path,
            )
            self.assertTrue(blacklisted)
            self.assertTrue(
                runner_team_auth.clear_team_auth_temporary_blacklist(
                    shared_root=shared_root,
                    team_auth_path=team_auth_path,
                    identity=identity,
                    worker_label="worker-01",
                    task_index=1,
                )
            )
            blacklisted, _ = runner_team_auth.team_auth_is_temp_blacklisted(
                shared_root=shared_root,
                team_auth_path=team_auth_path,
            )
            self.assertFalse(blacklisted)

    def test_release_reservation_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            source_path = shared_root / "mother.json"
            source_path.parent.mkdir(parents=True, exist_ok=True)
            source_path.write_text(
                '{"email":"mother@example.com","account_id":"acct_123"}',
                encoding="utf-8",
            )
            team_auth_path = str(source_path)
            reserved, reservation, summary = runner_team_auth.try_reserve_required_team_auth_seats(
                shared_root=shared_root,
                team_auth_path=team_auth_path,
                required_codex_seats=1,
                required_chatgpt_seats=0,
                reservation_owner="worker-01",
                reservation_context="main:1",
                source_role="main",
            )
            self.assertTrue(reserved)
            self.assertIsNotNone(reservation)
            self.assertIsInstance(summary, dict)
            released = runner_team_auth.release_team_auth_seat_reservations(
                shared_root=shared_root,
                reservation=reservation,
            )
            self.assertIsNotNone(released)


class RunnerWorkerMaintenanceTests(unittest.TestCase):
    def test_resolve_worker_team_auth_falls_back_when_pinned_path_is_reserved(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            pinned_path = tmp_path / "pinned.json"
            pinned_path.write_text("{}", encoding="utf-8")
            with mock.patch.object(
                runner_worker_maintenance,
                "_resolve_team_auth_pool",
                return_value=[str(pinned_path), "fallback.json"],
            ), mock.patch.object(
                runner_worker_maintenance,
                "_prune_stale_team_auth_caches",
                return_value={},
            ), mock.patch.object(
                runner_worker_maintenance,
                "_team_auth_is_reserved_for_team_expand",
                return_value=(True, {"reason": "team-expand"}),
            ), mock.patch.object(
                runner_worker_maintenance,
                "_select_team_auth_path",
                return_value=("fallback.json", {"reservationIds": ["r1"]}),
            ) as select_team_auth_path:
                selection = runner_worker_maintenance.resolve_worker_team_auth(
                    normalized_role="main",
                    shared_root=tmp_path / "shared",
                    output_root=tmp_path / "output",
                    worker_label="worker-01",
                    task_index=1,
                    pinned_team_auth_path=str(pinned_path),
                )
        self.assertEqual([str(pinned_path), "fallback.json"], selection.team_auth_pool)
        self.assertEqual("fallback.json", selection.selected_team_auth_path)
        self.assertEqual({"reservationIds": ["r1"]}, selection.seat_reservation)
        self.assertEqual(
            [str(pinned_path), "fallback.json"],
            select_team_auth_path.call_args.kwargs["team_auth_pool"],
        )


class RunnerWorkerLoopTests(unittest.TestCase):
    def test_worker_loop_exits_before_flow_selection_when_max_runs_already_reached(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            free_oauth_pool_dir = output_root / "codex" / "free"
            spec = RunnerFlowSpec(
                name="continue-openai",
                flow_path="continue-flow.json",
                instance_role="continue",
                weight=1.0,
                team_auth_path="",
                task_max_attempts=3,
                openai_oauth_pool_dir=output_root / "openai" / "failed-once",
                mailbox_business_key="openai",
                input_source_dir="",
                input_claims_dir="",
            )
            task_counter = SimpleNamespace(value=1, get_lock=lambda: nullcontext())
            worker_state = mock.Mock()
            with mock.patch.object(runner_worker_loop, "WorkerRuntimeState", return_value=worker_state):
                with mock.patch.object(runner_worker_loop, "_process_worker_maintenance") as maintenance:
                    with mock.patch.object(runner_worker_loop, "_choose_runnable_flow_spec") as choose_flow:
                        runner_worker_loop.worker_loop(
                            worker_id=1,
                            instance_id="mixed",
                            instance_role="mixed",
                            output_root_text=str(output_root),
                            delay_seconds=0.0,
                            max_runs=1,
                            task_max_attempts=0,
                            flow_specs=(spec,),
                            stop_event=SimpleNamespace(is_set=lambda: False),
                            task_counter=task_counter,
                            free_oauth_pool_dir_text=str(free_oauth_pool_dir),
                        )
        maintenance.assert_not_called()
        choose_flow.assert_not_called()
        worker_state.exited.assert_called_once_with(local_runs=0)

    def test_worker_loop_runs_selected_flow_spec(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            free_oauth_pool_dir = output_root / "codex" / "free"
            flow_pool_dir = output_root / "openai" / "failed-once"
            flow_pool_dir.mkdir(parents=True, exist_ok=True)
            (flow_pool_dir / "seed.json").write_text("{}", encoding="utf-8")
            spec = RunnerFlowSpec(
                name="continue-openai",
                flow_path="continue-flow.json",
                instance_role="continue",
                weight=1.0,
                team_auth_path="",
                task_max_attempts=3,
                openai_oauth_pool_dir=flow_pool_dir,
                mailbox_business_key="openai",
                input_source_dir="",
                input_claims_dir="",
            )
            dummy_result = SimpleNamespace(
                ok=True,
                to_dict=lambda: {"ok": True, "steps": {}, "outputs": {}},
            )
            worker_state = mock.Mock()
            with mock.patch.object(runner_worker_loop, "WorkerRuntimeState", return_value=worker_state):
                with mock.patch.object(runner_worker_loop, "_process_worker_maintenance"):
                    with mock.patch.object(
                        runner_worker_loop,
                        "_choose_runnable_flow_spec",
                        return_value=(spec, {"selected": {"name": "continue-openai"}}),
                    ):
                        with mock.patch.object(runner_worker_loop, "claim_task_index", side_effect=[1, None]):
                            with mock.patch.object(
                                runner_worker_loop,
                                "_resolve_worker_team_auth",
                                return_value=SimpleNamespace(
                                    team_auth_pool=[],
                                    selected_team_auth_path="",
                                    seat_reservation=None,
                                ),
                            ):
                                with mock.patch.object(runner_worker_loop, "run_dst_flow_once", return_value=dummy_result) as run_once:
                                    with mock.patch.object(runner_worker_loop, "_process_worker_run_result", return_value=0.0):
                                        with mock.patch("others.runner_worker_loop.time.sleep"):
                                            runner_worker_loop.worker_loop(
                                                worker_id=1,
                                                instance_id="mixed",
                                                instance_role="mixed",
                                                output_root_text=str(output_root),
                                                delay_seconds=0.0,
                                                max_runs=1,
                                                task_max_attempts=0,
                                                flow_specs=(spec,),
                                                stop_event=SimpleNamespace(is_set=lambda: False),
                                                task_counter=SimpleNamespace(value=0),
                                                free_oauth_pool_dir_text=str(free_oauth_pool_dir),
                                            )
        run_once.assert_called_once()
        self.assertEqual("continue-flow.json", run_once.call_args.kwargs["flow_path"])
        self.assertEqual(str(flow_pool_dir.resolve()), run_once.call_args.kwargs["openai_oauth_pool_dir"])
        self.assertEqual(3, run_once.call_args.kwargs["task_max_attempts"])
        self.assertEqual("openai", run_once.call_args.kwargs["mailbox_business_key"])
        self.assertFalse(run_once.call_args.kwargs["team_invite_enabled"])

    def test_worker_loop_releases_reserved_slot_when_run_setup_crashes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            free_oauth_pool_dir = output_root / "codex" / "free"
            flow_pool_dir = output_root / "openai" / "pending"
            spec = RunnerFlowSpec(
                name="main-openai",
                flow_path="main-flow.json",
                instance_role="main",
                weight=1.0,
                team_auth_path="",
                task_max_attempts=2,
                openai_oauth_pool_dir=flow_pool_dir,
                mailbox_business_key="openai",
                input_source_dir="",
                input_claims_dir="",
            )
            worker_state = mock.Mock()
            active_counts: dict[str, int] = {}

            def ensure_directory(path: Path) -> None:
                if "run-" in str(path):
                    raise OSError("simulated setup crash")
                path.mkdir(parents=True, exist_ok=True)

            with mock.patch.object(runner_worker_loop, "WorkerRuntimeState", return_value=worker_state):
                with mock.patch.object(runner_worker_loop, "_ensure_directory", side_effect=ensure_directory):
                    with mock.patch.object(runner_worker_loop, "_process_worker_maintenance"):
                        with mock.patch.object(
                            runner_worker_loop,
                            "_choose_runnable_flow_spec",
                            return_value=(spec, {"selected": {"name": "main-openai"}}),
                        ):
                            with mock.patch.object(runner_worker_loop, "claim_task_index", return_value=1):
                                with mock.patch.object(
                                    runner_worker_loop,
                                    "_resolve_worker_team_auth",
                                    return_value=SimpleNamespace(
                                        team_auth_pool=[],
                                        selected_team_auth_path="",
                                        seat_reservation=None,
                                    ),
                                ):
                                    with self.assertRaises(OSError):
                                        runner_worker_loop.worker_loop(
                                            worker_id=1,
                                            instance_id="mixed",
                                            instance_role="mixed",
                                            output_root_text=str(output_root),
                                            delay_seconds=0.0,
                                            max_runs=1,
                                            task_max_attempts=0,
                                            flow_specs=(spec,),
                                            stop_event=SimpleNamespace(is_set=lambda: False),
                                            task_counter=SimpleNamespace(value=0),
                                            free_oauth_pool_dir_text=str(free_oauth_pool_dir),
                                            active_flow_counts=active_counts,
                                        )

        self.assertEqual({}, active_counts)

    def test_worker_loop_runs_account_audit_flow_with_input_source_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            free_oauth_pool_dir = output_root / "codex" / "free"
            account_dir = Path(tmp_dir) / "account-audit-input"
            claims_dir = Path(tmp_dir) / "account-audit-claims"
            account_dir.mkdir(parents=True, exist_ok=True)
            (account_dir / "seed.json").write_text(
                json.dumps({"email": "seed@example.com"}),
                encoding="utf-8",
            )
            flow_path = (
                "server/services/orchestration_service/flows/"
                "openai-account-availability-audit-v1.semantic-flow.json"
            )
            spec = RunnerFlowSpec(
                name="openai-account-availability-audit",
                flow_path=flow_path,
                instance_role="account-audit",
                weight=1.0,
                team_auth_path="",
                task_max_attempts=1,
                openai_oauth_pool_dir=output_root / "openai" / "unused",
                mailbox_business_key="openai-account-audit",
                input_source_dir=str(account_dir),
                input_claims_dir=str(claims_dir),
            )
            dummy_result = SimpleNamespace(
                ok=True,
                to_dict=lambda: {"ok": True, "steps": {}, "outputs": {}},
            )
            worker_state = mock.Mock()
            with mock.patch.object(runner_worker_loop, "WorkerRuntimeState", return_value=worker_state):
                with mock.patch.object(runner_worker_loop, "_process_worker_maintenance"):
                    with mock.patch.object(
                        runner_worker_loop,
                        "_choose_runnable_flow_spec",
                        return_value=(spec, {"selected": {"name": "openai-account-availability-audit"}}),
                    ):
                        with mock.patch.object(runner_worker_loop, "claim_task_index", side_effect=[1, None]):
                            with mock.patch.object(
                                runner_worker_loop,
                                "_resolve_worker_team_auth",
                                return_value=SimpleNamespace(
                                    team_auth_pool=[],
                                    selected_team_auth_path="",
                                    seat_reservation=None,
                                ),
                            ):
                                with mock.patch.object(runner_worker_loop, "run_dst_flow_once", return_value=dummy_result) as run_once:
                                    with mock.patch.object(runner_worker_loop, "_process_worker_run_result", return_value=0.0):
                                        with mock.patch("others.runner_worker_loop.time.sleep"):
                                            runner_worker_loop.worker_loop(
                                                worker_id=1,
                                                instance_id="mixed",
                                                instance_role="mixed",
                                                output_root_text=str(output_root),
                                                delay_seconds=0.0,
                                                max_runs=1,
                                                task_max_attempts=0,
                                                flow_specs=(spec,),
                                                stop_event=SimpleNamespace(is_set=lambda: False),
                                                task_counter=SimpleNamespace(value=0),
                                                free_oauth_pool_dir_text=str(free_oauth_pool_dir),
                                            )

        run_once.assert_called_once()
        self.assertEqual(flow_path, run_once.call_args.kwargs["flow_path"])
        self.assertEqual(str(account_dir), run_once.call_args.kwargs["input_source_dir"])
        self.assertEqual(str(claims_dir), run_once.call_args.kwargs["input_claims_dir"])
        self.assertEqual(1, run_once.call_args.kwargs["task_max_attempts"])
        self.assertEqual("openai-account-audit", run_once.call_args.kwargs["mailbox_business_key"])
        self.assertFalse(run_once.call_args.kwargs["team_invite_enabled"])

    def test_worker_loop_releases_continue_slot_when_pool_empties_after_selection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            free_oauth_pool_dir = output_root / "codex" / "free"
            flow_pool_dir = output_root / "openai" / "failed-once"
            flow_pool_dir.mkdir(parents=True, exist_ok=True)
            (flow_pool_dir / "seed.json").write_text("{}", encoding="utf-8")
            spec = RunnerFlowSpec(
                name="continue-openai",
                flow_path="continue-flow.json",
                instance_role="continue",
                weight=1.0,
                team_auth_path="",
                task_max_attempts=3,
                openai_oauth_pool_dir=flow_pool_dir,
                mailbox_business_key="openai",
                input_source_dir="",
                input_claims_dir="",
            )
            worker_state = mock.Mock()
            stop_event = mock.Mock()
            stop_event.is_set.side_effect = [False, True]
            task_counter = SimpleNamespace(value=0)
            active_counts: dict[str, int] = {}
            with mock.patch.object(runner_worker_loop, "WorkerRuntimeState", return_value=worker_state):
                with mock.patch.object(runner_worker_loop, "_process_worker_maintenance"):
                    with mock.patch.object(
                        runner_worker_loop,
                        "_choose_runnable_flow_spec",
                        return_value=(spec, {"selected": {"name": "continue-openai"}}),
                    ):
                        with mock.patch.object(
                            runner_worker_loop,
                            "_flow_spec_runnable_state",
                            return_value={"ready": False, "reason": "openai_oauth_pool_empty"},
                        ) as post_reserve_state:
                            with mock.patch.object(runner_worker_loop, "claim_task_index") as claim_task:
                                with mock.patch.object(runner_worker_loop, "run_dst_flow_once") as run_once:
                                    with mock.patch("others.runner_worker_loop.time.sleep"):
                                        runner_worker_loop.worker_loop(
                                            worker_id=1,
                                            instance_id="mixed",
                                            instance_role="mixed",
                                            output_root_text=str(output_root),
                                            delay_seconds=0.0,
                                            max_runs=1,
                                            task_max_attempts=0,
                                            flow_specs=(spec,),
                                            stop_event=stop_event,
                                            task_counter=task_counter,
                                            free_oauth_pool_dir_text=str(free_oauth_pool_dir),
                                            active_flow_counts=active_counts,
                                        )

        post_reserve_state.assert_called_once()
        claim_task.assert_not_called()
        run_once.assert_not_called()
        self.assertEqual({}, active_counts)
        worker_state.exited.assert_called_once_with(local_runs=0)

    def test_worker_loop_releases_continue_slot_when_reserved_slots_exceed_pool_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            free_oauth_pool_dir = output_root / "codex" / "free"
            flow_pool_dir = output_root / "openai" / "failed-once"
            flow_pool_dir.mkdir(parents=True, exist_ok=True)
            (flow_pool_dir / "seed.json").write_text("{}", encoding="utf-8")
            spec = RunnerFlowSpec(
                name="continue-openai",
                flow_path="continue-flow.json",
                instance_role="continue",
                weight=1.0,
                team_auth_path="",
                task_max_attempts=3,
                openai_oauth_pool_dir=flow_pool_dir,
                mailbox_business_key="openai",
                input_source_dir="",
                input_claims_dir="",
                concurrency_limit=2,
            )
            worker_state = mock.Mock()
            stop_event = mock.Mock()
            stop_event.is_set.side_effect = [False, True]
            task_counter = SimpleNamespace(value=0)
            active_counts: dict[str, int] = {"continue-openai": 1}
            with mock.patch.object(runner_worker_loop, "WorkerRuntimeState", return_value=worker_state):
                with mock.patch.object(runner_worker_loop, "_process_worker_maintenance"):
                    with mock.patch.object(
                        runner_worker_loop,
                        "_choose_runnable_flow_spec",
                        return_value=(spec, {"selected": {"name": "continue-openai"}}),
                    ):
                        with mock.patch.object(runner_worker_loop, "claim_task_index") as claim_task:
                            with mock.patch.object(runner_worker_loop, "run_dst_flow_once") as run_once:
                                with mock.patch("others.runner_worker_loop.time.sleep"):
                                    runner_worker_loop.worker_loop(
                                        worker_id=1,
                                        instance_id="mixed",
                                        instance_role="mixed",
                                        output_root_text=str(output_root),
                                        delay_seconds=0.0,
                                        max_runs=1,
                                        task_max_attempts=0,
                                        flow_specs=(spec,),
                                        stop_event=stop_event,
                                        task_counter=task_counter,
                                        free_oauth_pool_dir_text=str(free_oauth_pool_dir),
                                        active_flow_counts=active_counts,
                                    )

        claim_task.assert_not_called()
        run_once.assert_not_called()
        self.assertEqual({"continue-openai": 1}, active_counts)
        worker_state.exited.assert_called_once_with(local_runs=0)

    def test_worker_loop_main_continues_without_team_auth_when_pool_filtered_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            free_oauth_pool_dir = output_root / "codex" / "free"
            flow_pool_dir = output_root / "openai" / "pending"
            spec = RunnerFlowSpec(
                name="main-openai",
                flow_path="main-flow.json",
                instance_role="main",
                weight=1.0,
                team_auth_path="",
                task_max_attempts=2,
                openai_oauth_pool_dir=flow_pool_dir,
                mailbox_business_key="openai",
                input_source_dir="",
                input_claims_dir="",
            )
            dummy_result = SimpleNamespace(
                ok=True,
                to_dict=lambda: {"ok": True, "steps": {}, "outputs": {}},
            )
            worker_state = mock.Mock()
            with mock.patch.object(runner_worker_loop, "WorkerRuntimeState", return_value=worker_state):
                with mock.patch.object(runner_worker_loop, "_process_worker_maintenance"):
                    with mock.patch.object(
                        runner_worker_loop,
                        "_choose_runnable_flow_spec",
                        return_value=(spec, {"selected": {"name": "main-openai"}}),
                    ):
                        with mock.patch.object(runner_worker_loop, "claim_task_index", side_effect=[1, None]):
                            with mock.patch.object(
                                runner_worker_loop,
                                "_resolve_worker_team_auth",
                                return_value=SimpleNamespace(
                                    team_auth_pool=["mother-a.json"],
                                    selected_team_auth_path="",
                                    seat_reservation=None,
                                ),
                            ):
                                with mock.patch.object(runner_worker_loop, "run_dst_flow_once", return_value=dummy_result) as run_once:
                                    with mock.patch.object(runner_worker_loop, "_process_worker_run_result", return_value=0.0):
                                        with mock.patch("others.runner_worker_loop.time.sleep"):
                                            runner_worker_loop.worker_loop(
                                                worker_id=1,
                                                instance_id="mixed",
                                                instance_role="mixed",
                                                output_root_text=str(output_root),
                                                delay_seconds=0.0,
                                                max_runs=1,
                                                task_max_attempts=0,
                                                flow_specs=(spec,),
                                                stop_event=SimpleNamespace(is_set=lambda: False),
                                                task_counter=SimpleNamespace(value=0),
                                                free_oauth_pool_dir_text=str(free_oauth_pool_dir),
                                            )
        run_once.assert_called_once()
        self.assertFalse(run_once.call_args.kwargs["team_invite_enabled"])


class RunnerWorkerResultsTests(unittest.TestCase):
    def test_process_worker_run_result_passes_result_payload_value_to_team_auth_history(self) -> None:
        result = SimpleNamespace(
            ok=True,
            to_dict=lambda: {"ok": True, "steps": {}, "outputs": {}},
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            shared_root = output_root
            run_output_dir = output_root / "worker-01" / "run-1"
            openai_oauth_pool_dir = output_root / "openai" / "pending"
            worker_state = mock.Mock()
            with mock.patch.object(runner_worker_results, "_json_log"), mock.patch.object(
                runner_worker_results,
                "_team_auth_path_from_result_payload",
                return_value="",
            ), mock.patch.object(
                runner_worker_results,
                "_output_dict",
                return_value={},
            ), mock.patch.object(
                runner_worker_results,
                "_record_business_mailbox_domain_outcome",
                return_value=None,
            ), mock.patch.object(
                runner_worker_results,
                "_record_team_auth_recent_invite_result",
            ) as record_invite, mock.patch.object(
                runner_worker_results,
                "_record_team_auth_recent_team_expand_result",
            ) as record_expand, mock.patch.object(
                runner_worker_results,
                "_team_auth_reconcile_seat_state_from_result",
            ), mock.patch.object(
                runner_worker_results,
                "_sync_refreshed_credentials_back_to_sources",
                return_value=[],
            ) as sync_credentials, mock.patch.object(
                runner_worker_results,
                "_free_stop_after_validate_mode",
                return_value=False,
            ), mock.patch.object(
                runner_worker_results,
                "_mailbox_capacity_failure_detail",
                return_value="",
            ), mock.patch.object(
                runner_worker_results,
                "_team_capacity_failure_detail",
                return_value="",
            ), mock.patch.object(
                runner_worker_results,
                "_team_auth_blacklist_reason",
                return_value="",
            ), mock.patch.object(
                runner_worker_results,
                "_postprocess_free_success_artifact",
                return_value={"ok": True, "cleanup_run_output": False},
            ), mock.patch.object(
                runner_worker_results,
                "_extra_failure_cooldown_seconds",
                return_value=0.0,
            ):
                cooldown = runner_worker_results.process_worker_run_result(
                    result=result,
                    started_at="2026-01-01T00:00:00+00:00",
                    run_output_dir=run_output_dir,
                    output_root=output_root,
                    shared_root=shared_root,
                    openai_oauth_pool_dir=openai_oauth_pool_dir,
                    normalized_role="main",
                    worker_label="worker-01",
                    task_index=1,
                    local_run_index=1,
                    worker_state=worker_state,
                    selected_team_auth_path="",
                    free_local_selected=True,
                    team_auth_pool=[],
                )
        self.assertEqual(0.0, cooldown)
        self.assertIn("result_payload_value", record_invite.call_args.kwargs)
        self.assertIn("result_payload_value", record_expand.call_args.kwargs)
        self.assertIn("result_payload_value", sync_credentials.call_args.kwargs)


class RunnerCredentialSyncTests(unittest.TestCase):
    def test_sync_refreshed_credentials_back_to_sources_forwards_payload_to_helpers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            refreshed_path = tmp_path / "refreshed.json"
            restored_source_path = tmp_path / "restored-source.json"
            refreshed_path.write_text("{}", encoding="utf-8")
            restored_source_path.write_text("{}", encoding="utf-8")
            payload = {"outputs": {"obtain-codex-oauth": {"successPath": str(refreshed_path)}}}
            actions = [
                {
                    "kind": "generic_oauth_refresh",
                    "source_path": str(tmp_path / "missing-source.json"),
                    "refreshed_path": str(refreshed_path),
                    "force": True,
                }
            ]
            with mock.patch.object(
                runner_credential_sync,
                "credential_backwrite_actions",
                return_value=actions,
            ) as build_actions, mock.patch.object(
                runner_credential_sync,
                "restored_path_for_source",
                return_value=restored_source_path,
            ) as restored_path, mock.patch.object(
                runner_credential_sync,
                "_load_json_dict",
                side_effect=[{"email": "before@example.com"}, {"email": "after@example.com"}],
            ), mock.patch.object(
                runner_credential_sync,
                "_merge_refreshed_credential",
                return_value={"email": "after@example.com"},
            ), mock.patch.object(
                runner_credential_sync,
                "write_json_atomic",
            ), mock.patch.object(
                runner_credential_sync,
                "json_log",
            ):
                synced = runner_credential_sync.sync_refreshed_credentials_back_to_sources(
                    result_payload_value=payload,
                    worker_label="worker-01",
                    task_index=1,
                )
        self.assertEqual(1, len(synced))
        self.assertIs(build_actions.call_args.args[0], payload)
        self.assertIs(restored_path.call_args.args[0], payload)


if __name__ == "__main__":
    unittest.main()
