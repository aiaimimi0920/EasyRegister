from __future__ import annotations

import json
import errno
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock


SRC_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

PYTHON_SHARED_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "python_shared" / "src"
if str(PYTHON_SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(PYTHON_SHARED_ROOT))

from others import artifact_pool_claims, artifact_pool_common, artifact_pool_team_batch  # noqa: E402


class ArtifactPoolCommonTests(unittest.TestCase):
    def test_recover_stale_team_claims_restores_original_name(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            pool_dir = Path(tmp_dir) / "pool"
            claims_dir = Path(tmp_dir) / "claims"
            pool_dir.mkdir(parents=True, exist_ok=True)
            claims_dir.mkdir(parents=True, exist_ok=True)
            claimed_path = claims_dir / "deadbeef-original.json"
            claimed_path.write_text('{"email":"user@example.com"}', encoding="utf-8")
            stale_timestamp = time.time() - 120
            os.utime(claimed_path, (stale_timestamp, stale_timestamp))

            recovered = artifact_pool_common.recover_stale_team_claims(
                pool_dir=pool_dir,
                claims_dir=claims_dir,
                stale_after_seconds=60,
            )

            self.assertEqual(1, len(recovered))
            self.assertFalse(claimed_path.exists())
            self.assertTrue((pool_dir / "original.json").exists())

    def test_recover_stale_team_claims_strips_repeated_claim_prefixes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            pool_dir = Path(tmp_dir) / "pool"
            claims_dir = Path(tmp_dir) / "claims"
            pool_dir.mkdir(parents=True, exist_ok=True)
            claims_dir.mkdir(parents=True, exist_ok=True)
            claimed_path = claims_dir / "deadbeef-cafebabe-original.json"
            claimed_path.write_text('{"email":"user@example.com"}', encoding="utf-8")
            stale_timestamp = time.time() - 120
            os.utime(claimed_path, (stale_timestamp, stale_timestamp))

            recovered = artifact_pool_common.recover_stale_team_claims(
                pool_dir=pool_dir,
                claims_dir=claims_dir,
                stale_after_seconds=60,
            )

            self.assertEqual(1, len(recovered))
            self.assertFalse(claimed_path.exists())
            self.assertTrue((pool_dir / "original.json").exists())
            self.assertFalse((pool_dir / "cafebabe-original.json").exists())

    def test_recover_stale_team_claims_falls_back_when_restore_crosses_filesystems(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            pool_dir = Path(tmp_dir) / "pool"
            claims_dir = Path(tmp_dir) / "claims"
            pool_dir.mkdir(parents=True, exist_ok=True)
            claims_dir.mkdir(parents=True, exist_ok=True)
            claimed_path = claims_dir / "deadbeef-original.json"
            claimed_path.write_text('{"email":"user@example.com"}', encoding="utf-8")
            stale_timestamp = time.time() - 120
            os.utime(claimed_path, (stale_timestamp, stale_timestamp))
            original_replace = Path.replace

            def _replace(self: Path, target: Path) -> Path:
                if self == claimed_path:
                    raise OSError(errno.EXDEV, "Invalid cross-device link")
                return original_replace(self, target)

            with mock.patch.object(Path, "replace", _replace):
                recovered = artifact_pool_common.recover_stale_team_claims(
                    pool_dir=pool_dir,
                    claims_dir=claims_dir,
                    stale_after_seconds=60,
                )

            self.assertEqual(1, len(recovered))
            self.assertFalse(claimed_path.exists())
            restored_path = pool_dir / "original.json"
            self.assertTrue(restored_path.exists())
            self.assertEqual('{"email":"user@example.com"}', restored_path.read_text(encoding="utf-8"))

    def test_recover_stale_openai_oauth_claims_restores_original_name_and_releases_lock(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            pool_dir = output_root / "openai" / "failed-once"
            claims_dir = output_root / "others" / "openai-oauth-claims"
            pool_dir.mkdir(parents=True, exist_ok=True)
            claims_dir.mkdir(parents=True, exist_ok=True)
            claimed_path = claims_dir / "deadbeef-original.json"
            claimed_path.write_text('{"email":"user@example.com"}', encoding="utf-8")
            stale_timestamp = time.time() - 120
            os.utime(claimed_path, (stale_timestamp, stale_timestamp))
            artifact_pool_claims.acquire_conversion_lock(
                shared_root=output_root,
                email="user@example.com",
                claimed_path=claimed_path,
                source_path=claimed_path,
                stage="continue",
                worker_label="worker-01",
                task_index=1,
            )

            with mock.patch.dict(os.environ, {"REGISTER_OPENAI_OAUTH_STALE_CLAIM_SECONDS": "60"}, clear=False):
                recovered = artifact_pool_common.recover_stale_openai_oauth_claims(
                    pool_dir=pool_dir,
                    claims_dir=claims_dir,
                    stale_after_seconds=artifact_pool_common.openai_oauth_stale_claim_seconds(),
                    shared_root=output_root,
                )

            self.assertEqual(1, len(recovered))
            self.assertFalse(claimed_path.exists())
            self.assertTrue((pool_dir / "original.json").exists())
            self.assertTrue(recovered[0]["lock_released"])
            lock_dir = output_root / "others" / "openai-oauth-conversion-locks"
            self.assertEqual([], list(lock_dir.glob("*.json")))

    def test_team_expand_progress_normalizes_success_emails(self) -> None:
        progress = artifact_pool_common.team_expand_progress_from_payload(
            {
                "teamFlow": {
                    "teamExpandProgress": {
                        "targetCount": 4,
                        "successCount": 1,
                        "successfulMemberEmails": [
                            "User@One.com",
                            "user@one.com",
                            "two@example.com",
                        ],
                    }
                }
            },
            fallback_target=4,
        )

        self.assertEqual(4, progress["targetCount"])
        self.assertEqual(["user@one.com", "two@example.com"], progress["successfulMemberEmails"])
        self.assertEqual(2, progress["successCount"])
        self.assertEqual(2, progress["remainingCount"])


class ArtifactPoolClaimsTests(unittest.TestCase):
    def test_validate_free_personal_oauth_preserves_terminal_phone_rejection_as_small_success_failure(self) -> None:
        result = artifact_pool_claims.validate_free_personal_oauth(
            step_input={
                "oauth_result": {
                    "phoneVerificationAttempted": True,
                    "phoneVerificationTerminal": True,
                    "phoneVerificationTerminalCode": "phone_number_in_use",
                    "phoneVerificationTerminalMessage": "Phone number already in use.",
                    "phoneProvider": "smstome",
                    "phoneSessionId": "sms_session_123",
                }
            }
        )

        self.assertFalse(result["ok"])
        self.assertEqual("phone_number_in_use", result["code"])
        self.assertEqual("phone_verification_terminal_small_success", result["status"])
        self.assertEqual("phone_verification_terminal_small_success", result["detail"])
        self.assertTrue(result["phone_verification_attempted"])
        self.assertEqual("smstome", result["phone_provider"])
        self.assertEqual("sms_session_123", result["phone_session_id"])

    def test_validate_free_personal_oauth_preserves_phone_submission_without_code_as_small_success_failure(self) -> None:
        result = artifact_pool_claims.validate_free_personal_oauth(
            step_input={
                "oauth_result": {
                    "phoneVerificationAttempted": True,
                    "phoneVerificationSubmitted": True,
                    "phoneVerificationFailureStage": "wait_sms_code",
                    "phoneVerificationFailureDetail": "wait_code_timeout",
                    "phoneProvider": "onlinesim",
                    "phoneSessionId": "sms_session_456",
                }
            }
        )

        self.assertFalse(result["ok"])
        self.assertEqual("phone_verification_submitted_small_success", result["code"])
        self.assertEqual("phone_verification_submitted_small_success", result["status"])
        self.assertEqual("wait_sms_code", result["phone_failure_stage"])
        self.assertEqual("onlinesim", result["phone_provider"])

    def test_validate_free_personal_oauth_preserves_phone_attempt_failure_as_small_success_failure(self) -> None:
        result = artifact_pool_claims.validate_free_personal_oauth(
            step_input={
                "oauth_result": {
                    "phoneVerificationAttempted": True,
                    "phoneVerificationSubmitted": False,
                    "phoneVerificationFailureStage": "submit_phone_verification_number",
                    "phoneVerificationFailureDetail": "easyprotocol_transport_failed:timed out",
                    "phoneProvider": "smstome",
                    "phoneSessionId": "sms_session_789",
                }
            }
        )

        self.assertFalse(result["ok"])
        self.assertEqual("phone_verification_attempted_small_success", result["code"])
        self.assertEqual("phone_verification_attempted_small_success", result["status"])
        self.assertEqual("phone_verification_attempted_small_success", result["detail"])
        self.assertEqual("submit_phone_verification_number", result["phone_failure_stage"])
        self.assertEqual("smstome", result["phone_provider"])

    def test_claim_openai_oauth_artifact_skips_email_when_codex_success_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "continue-runs" / "worker-01" / "run-20260502-task000001"
            source_pool_dir = output_root / "openai" / "pending"
            codex_free_dir = output_root / "codex" / "free"
            source_pool_dir.mkdir(parents=True, exist_ok=True)
            codex_free_dir.mkdir(parents=True, exist_ok=True)
            seed_path = source_pool_dir / "seed.json"
            seed_path.write_text("{}", encoding="utf-8")
            (codex_free_dir / "already-success.json").write_text('{"email":"seed@example.com"}', encoding="utf-8")

            with mock.patch.object(
                artifact_pool_claims,
                "load_openai_oauth_seed_validation",
                return_value=(True, "", {"email": "seed@example.com"}),
            ):
                with self.assertRaisesRegex(RuntimeError, "openai_oauth_pool_empty"):
                    artifact_pool_claims.claim_openai_oauth_artifact(
                        step_input={
                            "output_dir": str(run_output_dir),
                            "pool_dir": str(source_pool_dir),
                        }
                    )

            self.assertTrue(seed_path.exists())

    def test_claim_openai_oauth_artifact_acquires_and_finalize_releases_conversion_lock(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "continue-runs" / "worker-01" / "run-20260502-task000001"
            source_pool_dir = output_root / "openai" / "pending"
            source_pool_dir.mkdir(parents=True, exist_ok=True)
            seed_path = source_pool_dir / "seed.json"
            seed_path.write_text("{}", encoding="utf-8")

            with mock.patch.object(
                artifact_pool_claims,
                "load_openai_oauth_seed_validation",
                return_value=(True, "", {"email": "seed@example.com"}),
            ):
                artifact = artifact_pool_claims.claim_openai_oauth_artifact(
                    step_input={
                        "output_dir": str(run_output_dir),
                        "pool_dir": str(source_pool_dir),
                        "worker_label": "worker-01",
                        "task_index": 1,
                    }
                )

            lock_dir = output_root / "others" / "openai-oauth-conversion-locks"
            lock_files = list(lock_dir.glob("*.json"))
            self.assertEqual(1, len(lock_files))
            self.assertTrue(Path(artifact["claimed_path"]).exists())

            finalize_result = artifact_pool_claims.finalize_openai_oauth_artifact(
                step_input={
                    "output_dir": str(run_output_dir),
                    "artifact": artifact,
                    "worker_label": "worker-01",
                    "task_index": 1,
                }
            )

            self.assertEqual("promoted_success", finalize_result["status"])
            self.assertEqual([], list(lock_dir.glob("*.json")))
            self.assertTrue((output_root / "openai" / "converted" / "seed.json").exists())

    def test_claim_openai_oauth_artifact_falls_back_when_claim_crosses_filesystems(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "continue-runs" / "worker-01" / "run-20260620-task000001"
            source_pool_dir = output_root / "openai" / "failed-once"
            source_pool_dir.mkdir(parents=True, exist_ok=True)
            seed_path = source_pool_dir / "seed.json"
            seed_path.write_text('{"email":"seed@example.com"}', encoding="utf-8")
            original_replace = Path.replace

            def _replace(self: Path, target: Path) -> Path:
                if self == seed_path:
                    raise OSError(errno.EXDEV, "Invalid cross-device link")
                return original_replace(self, target)

            with mock.patch.object(
                artifact_pool_claims,
                "load_openai_oauth_seed_validation",
                return_value=(True, "", {"email": "seed@example.com"}),
            ):
                with mock.patch.object(Path, "replace", _replace):
                    artifact = artifact_pool_claims.claim_openai_oauth_artifact(
                        step_input={
                            "output_dir": str(run_output_dir),
                            "pool_dir": str(source_pool_dir),
                            "worker_label": "worker-01",
                            "task_index": 1,
                        }
                    )

            claimed_path = Path(artifact["claimed_path"])
            self.assertFalse(seed_path.exists())
            self.assertTrue(claimed_path.exists())
            self.assertEqual('{"email":"seed@example.com"}', claimed_path.read_text(encoding="utf-8"))

    def test_claim_openai_oauth_artifact_ignores_age_for_user_layer_seed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "continue-runs" / "worker-01" / "run-20260502-task000001"
            source_pool_dir = output_root / "openai" / "failed-once"
            source_pool_dir.mkdir(parents=True, exist_ok=True)
            seed_path = source_pool_dir / "seed.json"
            seed_path.write_text(
                '{"email":"old@example.com","mailboxRef":"mailbox-ref","mailboxSessionId":"session-id","createdAt":"2026-05-01T00:00:00Z","platformOrganization":{"status":"completed"},"chatgptLogin":{"status":"completed","workspaceId":"ws_123"},"chatgptLoginDetails":{"clientBootstrap":{"authStatus":"logged_in","structure":"personal"}}}',
                encoding="utf-8",
            )

            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OPENAI_OAUTH_SEED_MAX_AGE_SECONDS": "5",
                },
                clear=False,
            ):
                artifact = artifact_pool_claims.claim_openai_oauth_artifact(
                    step_input={
                        "output_dir": str(run_output_dir),
                        "pool_dir": str(source_pool_dir),
                        "worker_label": "worker-01",
                        "task_index": 1,
                    }
                )

            self.assertEqual("old@example.com", artifact["email"])
            self.assertFalse(seed_path.exists())

    def test_claim_openai_oauth_artifact_accepts_protocol_small_success_seed_with_platform_auth(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "continue-runs" / "worker-01" / "run-20260706-task000001"
            source_pool_dir = output_root / "openai" / "pending"
            source_pool_dir.mkdir(parents=True, exist_ok=True)
            seed_path = source_pool_dir / "small-20260706-144202-goldenbadgerd86241-venf@007.hzeg.eu.org-6c6479.json"
            seed_path.write_text(
                json.dumps(
                    {
                        "outcome": "small_success",
                        "source": "protocol_small_success",
                        "email": "venf@007.hzeg.eu.org",
                        "mailboxRef": "im215:venf@007.hzeg.eu.org",
                        "mailboxSessionId": "mailbox-session-1",
                        "createdAt": "2026-07-06T14:42:02Z",
                        "platformAuth": {
                            "clientId": "app_client",
                            "redirectUri": "https://platform.openai.com/auth/callback",
                            "audience": "https://api.openai.com/v1",
                            "scope": "openid profile email offline_access",
                            "deviceId": "device-id",
                            "codeVerifier": "code-verifier",
                            "state": "state-value",
                            "nonce": "nonce-value",
                            "auth0Client": "auth0-client",
                        },
                    }
                ),
                encoding="utf-8",
            )

            artifact = artifact_pool_claims.claim_openai_oauth_artifact(
                step_input={
                    "output_dir": str(run_output_dir),
                    "pool_dir": str(source_pool_dir),
                    "worker_label": "worker-01",
                    "task_index": 1,
                }
            )

            self.assertEqual("venf@007.hzeg.eu.org", artifact["email"])
            self.assertEqual("im215:venf@007.hzeg.eu.org", artifact["mailboxRef"])
            self.assertEqual("mailbox-session-1", artifact["mailboxSessionId"])
            self.assertFalse(seed_path.exists())
            self.assertTrue(Path(artifact["claimed_path"]).exists())

    def test_claim_openai_oauth_artifact_canonicalizes_repeated_prefixed_pool_name(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "continue-runs" / "worker-01" / "run-20260502-task000001"
            source_pool_dir = output_root / "openai" / "failed-once"
            source_pool_dir.mkdir(parents=True, exist_ok=True)
            seed_name = "deadbeef-cafebabe-small-20260501-seed@example.com-a1b2c3.json"
            seed_path = source_pool_dir / seed_name
            seed_path.write_text(
                '{"email":"seed@example.com","mailboxRef":"mailbox-ref","mailboxSessionId":"session-id","createdAt":"2026-05-01T00:00:00Z","platformOrganization":{"status":"completed"},"chatgptLogin":{"status":"completed","workspaceId":"ws_123"},"chatgptLoginDetails":{"clientBootstrap":{"authStatus":"logged_in","structure":"personal"}}}',
                encoding="utf-8",
            )

            artifact = artifact_pool_claims.claim_openai_oauth_artifact(
                step_input={
                    "output_dir": str(run_output_dir),
                    "pool_dir": str(source_pool_dir),
                    "worker_label": "worker-01",
                    "task_index": 1,
                }
            )

            claimed_path = Path(artifact["claimed_path"])
            self.assertFalse(seed_path.exists())
            self.assertTrue(claimed_path.exists())
            self.assertEqual(
                "small-20260501-seed@example.com-a1b2c3.json",
                artifact["original_name"],
            )
            self.assertTrue(claimed_path.name.endswith("-small-20260501-seed@example.com-a1b2c3.json"))
            self.assertNotIn("deadbeef-cafebabe-", artifact["original_name"])

    def test_claim_openai_oauth_artifact_returns_recovery_data_credential(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "continue-runs" / "worker-01" / "run-20260502-task000001"
            source_pool_dir = output_root / "openai" / "failed-once"
            source_pool_dir.mkdir(parents=True, exist_ok=True)
            seed_path = source_pool_dir / "seed.json"
            seed_path.write_text(
                json.dumps(
                    {
                        "email": "seed@example.com",
                        "mailboxRef": "cloudflare_temp_email:old-ref",
                        "mailboxSessionId": "old-session",
                        "recoveryDataCredential": {
                            "emailAddress": "seed@example.com",
                            "providerTypeKey": "cloudflare_temp_email",
                        },
                        "createdAt": "2026-05-01T00:00:00Z",
                        "platformOrganization": {"status": "completed"},
                        "chatgptLogin": {"status": "completed", "workspaceId": "ws_123"},
                        "chatgptLoginDetails": {
                            "clientBootstrap": {"authStatus": "logged_in", "structure": "personal"}
                        },
                    }
                ),
                encoding="utf-8",
            )

            with mock.patch.dict(os.environ, {"REGISTER_OPENAI_OAUTH_SEED_MAX_AGE_SECONDS": "0"}, clear=False):
                artifact = artifact_pool_claims.claim_openai_oauth_artifact(
                    step_input={
                        "output_dir": str(run_output_dir),
                        "pool_dir": str(source_pool_dir),
                        "worker_label": "worker-01",
                    }
                )

        self.assertEqual(
            {
                "emailAddress": "seed@example.com",
                "providerTypeKey": "cloudflare_temp_email",
            },
            artifact["recoveryDataCredential"],
        )

    def test_claim_openai_oauth_artifact_prefers_oldest_seed_for_continue_pool(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "continue-runs" / "worker-01" / "run-20260502-task000001"
            source_pool_dir = output_root / "openai" / "failed-once"
            source_pool_dir.mkdir(parents=True, exist_ok=True)
            older_seed = source_pool_dir / "older.json"
            newer_seed = source_pool_dir / "newer.json"
            payload = (
                '{"mailboxRef":"mailbox-ref","mailboxSessionId":"session-id",'
                '"createdAt":"2026-05-01T00:00:00Z","platformOrganization":{"status":"completed"},'
                '"chatgptLogin":{"status":"completed","workspaceId":"ws_123"},'
                '"chatgptLoginDetails":{"clientBootstrap":{"authStatus":"logged_in","structure":"personal"}}}'
            )
            older_seed.write_text('{"email":"older@example.com",' + payload[1:], encoding="utf-8")
            newer_seed.write_text('{"email":"newer@example.com",' + payload[1:], encoding="utf-8")
            older_ts = time.time() - 3600
            newer_ts = time.time() - 60
            os.utime(older_seed, (older_ts, older_ts))
            os.utime(newer_seed, (newer_ts, newer_ts))

            artifact = artifact_pool_claims.claim_openai_oauth_artifact(
                step_input={
                    "output_dir": str(run_output_dir),
                    "pool_dir": str(source_pool_dir),
                    "worker_label": "worker-01",
                    "task_index": 1,
                }
            )

            self.assertEqual("older@example.com", artifact["email"])
            self.assertEqual("older.json", artifact["original_name"])
            self.assertFalse(older_seed.exists())
            self.assertTrue(newer_seed.exists())

    def test_claim_openai_oauth_artifact_prefers_full_seed_over_older_thin_seed_for_continue_pool(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "continue-runs" / "worker-01" / "run-20260813-task000001"
            source_pool_dir = output_root / "openai" / "failed-once"
            source_pool_dir.mkdir(parents=True, exist_ok=True)
            older_thin_seed = source_pool_dir / "older-thin.json"
            newer_full_seed = source_pool_dir / "newer-full.json"

            older_thin_seed.write_text(
                json.dumps(
                    {
                        "outcome": "small_success",
                        "source": "protocol_small_success",
                        "email": "older-thin@example.com",
                        "mailboxRef": "mailbox-ref",
                        "mailboxSessionId": "session-id",
                        "createdAt": "2026-08-13T12:00:00Z",
                        "platformAuth": {
                            "clientId": "client",
                            "redirectUri": "https://chatgpt.com/api/auth/callback/openai",
                            "codeVerifier": "verifier",
                            "state": "state",
                            "nonce": "nonce",
                        },
                    }
                ),
                encoding="utf-8",
            )
            newer_full_seed.write_text(
                json.dumps(
                    {
                        "email": "newer-full@example.com",
                        "mailboxRef": "mailbox-ref",
                        "mailboxSessionId": "session-id",
                        "createdAt": "2026-08-13T12:01:00Z",
                        "platformOrganization": {"status": "completed"},
                        "chatgptLogin": {"status": "completed", "workspaceId": "ws_123"},
                        "chatgptLoginDetails": {
                            "clientBootstrap": {"authStatus": "logged_in", "structure": "personal"}
                        },
                        "recoveryDataCredential": {"emailAddress": "newer-full@example.com"},
                    }
                ),
                encoding="utf-8",
            )
            older_ts = time.time() - 3600
            newer_ts = time.time() - 60
            os.utime(older_thin_seed, (older_ts, older_ts))
            os.utime(newer_full_seed, (newer_ts, newer_ts))

            artifact = artifact_pool_claims.claim_openai_oauth_artifact(
                step_input={
                    "output_dir": str(run_output_dir),
                    "pool_dir": str(source_pool_dir),
                    "worker_label": "worker-01",
                    "task_index": 1,
                }
            )

            self.assertEqual("newer-full@example.com", artifact["email"])
            self.assertEqual("newer-full.json", artifact["original_name"])
            self.assertTrue(older_thin_seed.exists())
            self.assertFalse(newer_full_seed.exists())

    def test_claim_openai_oauth_artifact_prefers_newer_seed_for_pending_pool(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "mixed-runs" / "worker-01" / "run-20260502-task000002"
            source_pool_dir = output_root / "openai" / "pending"
            source_pool_dir.mkdir(parents=True, exist_ok=True)
            older_seed = source_pool_dir / "older.json"
            newer_seed = source_pool_dir / "newer.json"
            payload = (
                '{"mailboxRef":"mailbox-ref","mailboxSessionId":"session-id",'
                '"createdAt":"2026-05-01T00:00:00Z","platformOrganization":{"status":"completed"},'
                '"chatgptLogin":{"status":"completed","workspaceId":"ws_123"},'
                '"chatgptLoginDetails":{"clientBootstrap":{"authStatus":"logged_in","structure":"personal"}}}'
            )
            older_seed.write_text('{"email":"older@example.com",' + payload[1:], encoding="utf-8")
            newer_seed.write_text('{"email":"newer@example.com",' + payload[1:], encoding="utf-8")
            older_ts = time.time() - 3600
            newer_ts = time.time() - 60
            os.utime(older_seed, (older_ts, older_ts))
            os.utime(newer_seed, (newer_ts, newer_ts))

            artifact = artifact_pool_claims.claim_openai_oauth_artifact(
                step_input={
                    "output_dir": str(run_output_dir),
                    "pool_dir": str(source_pool_dir),
                    "worker_label": "worker-01",
                    "task_index": 1,
                }
            )

            self.assertEqual("newer@example.com", artifact["email"])
            self.assertEqual("newer.json", artifact["original_name"])
            self.assertTrue(older_seed.exists())
            self.assertFalse(newer_seed.exists())

    def test_claim_openai_oauth_artifact_recovers_stale_continue_claim_before_reclaiming(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "continue-runs" / "worker-01" / "run-20260814-task000001"
            source_pool_dir = output_root / "openai" / "failed-once"
            claims_dir = output_root / "others" / "openai-oauth-claims"
            source_pool_dir.mkdir(parents=True, exist_ok=True)
            claims_dir.mkdir(parents=True, exist_ok=True)
            claimed_path = claims_dir / "deadbeef-stale.json"
            claimed_path.write_text(
                '{"email":"stale@example.com","mailboxRef":"mailbox-ref","mailboxSessionId":"session-id","createdAt":"2026-08-14T00:00:00Z","platformOrganization":{"status":"completed"},"chatgptLogin":{"status":"completed","workspaceId":"ws_123"},"chatgptLoginDetails":{"clientBootstrap":{"authStatus":"logged_in","structure":"personal"}}}',
                encoding="utf-8",
            )
            stale_timestamp = time.time() - 120
            os.utime(claimed_path, (stale_timestamp, stale_timestamp))
            artifact_pool_claims.acquire_conversion_lock(
                shared_root=output_root,
                email="stale@example.com",
                claimed_path=claimed_path,
                source_path=claimed_path,
                stage="continue",
                worker_label="worker-01",
                task_index=0,
            )

            with mock.patch.dict(os.environ, {"REGISTER_OPENAI_OAUTH_STALE_CLAIM_SECONDS": "60"}, clear=False):
                artifact = artifact_pool_claims.claim_openai_oauth_artifact(
                    step_input={
                        "output_dir": str(run_output_dir),
                        "pool_dir": str(source_pool_dir),
                        "worker_label": "worker-01",
                        "task_index": 1,
                    }
                )

            self.assertEqual("stale@example.com", artifact["email"])
            self.assertEqual(1, len(artifact["recovered_claims"]))
            self.assertTrue(Path(artifact["claimed_path"]).exists())
            self.assertFalse((source_pool_dir / "stale.json").exists())

    def test_fill_team_pre_pool_defaults_target_dir_under_others(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "mixed-runs" / "worker-01" / "run-20260430-task000001"
            source_pool_dir = output_root / "openai" / "pending"
            source_pool_dir.mkdir(parents=True, exist_ok=True)
            seed_path = source_pool_dir / "seed.json"
            seed_path.write_text("{}", encoding="utf-8")

            with mock.patch.object(
                artifact_pool_claims,
                "load_openai_oauth_seed_validation",
                return_value=(True, "", {"email": "seed@example.com"}),
            ):
                result = artifact_pool_claims.fill_team_pre_pool(
                    step_input={
                        "output_dir": str(run_output_dir),
                        "pool_dir": str(source_pool_dir),
                    }
                )

            expected_team_pre_pool_dir = output_root / "others" / "team-pre-pool"
            self.assertEqual("moved", result["status"])
            self.assertEqual(str(expected_team_pre_pool_dir.resolve()), result["team_pre_pool_dir"])
            self.assertFalse(seed_path.exists())
            self.assertTrue((expected_team_pre_pool_dir / "seed.json").exists())

    def test_fill_team_pre_pool_skips_email_when_codex_success_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "team-runs" / "worker-01" / "run-20260502-task000001"
            source_pool_dir = output_root / "openai" / "pending"
            codex_team_dir = output_root / "codex" / "team"
            source_pool_dir.mkdir(parents=True, exist_ok=True)
            codex_team_dir.mkdir(parents=True, exist_ok=True)
            seed_path = source_pool_dir / "seed.json"
            seed_path.write_text("{}", encoding="utf-8")
            (codex_team_dir / "already-team.json").write_text('{"email":"seed@example.com"}', encoding="utf-8")

            with mock.patch.object(
                artifact_pool_claims,
                "load_openai_oauth_seed_validation",
                return_value=(True, "", {"email": "seed@example.com"}),
            ):
                result = artifact_pool_claims.fill_team_pre_pool(
                    step_input={
                        "output_dir": str(run_output_dir),
                        "pool_dir": str(source_pool_dir),
                    }
                )

            self.assertEqual("idle", result["status"])
            self.assertEqual(1, result["skipped_existing_codex_count"])
            self.assertTrue(seed_path.exists())

    def test_finalize_openai_oauth_artifact_preserves_manual_oauth(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            claims_dir = Path(tmp_dir) / "claims"
            manual_pool_dir = Path(tmp_dir) / "manual-oauth-pool"
            claims_dir.mkdir(parents=True, exist_ok=True)
            claimed_path = claims_dir / "claimed.json"
            claimed_path.write_text('{"email":"free@example.com"}', encoding="utf-8")

            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_FREE_MANUAL_OAUTH_PRESERVE_ENABLED": "true",
                    "REGISTER_FREE_MANUAL_OAUTH_PRESERVE_ERROR_CODES": "token_invalidated",
                },
                clear=True,
            ):
                result = artifact_pool_claims.finalize_openai_oauth_artifact(
                    step_input={
                        "artifact": {
                            "claimed_path": str(claimed_path),
                            "original_name": "original.json",
                            "email": "free@example.com",
                        },
                        "task_error_code": "token_invalidated",
                        "free_manual_oauth_pool_dir": str(manual_pool_dir),
                    }
                )

            self.assertEqual("preserved_for_manual_oauth", result["status"])
            self.assertFalse(claimed_path.exists())
            self.assertTrue((manual_pool_dir / "original.json").exists())

    def test_finalize_openai_oauth_artifact_preserves_manual_oauth_strips_repeated_prefixes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            claims_dir = Path(tmp_dir) / "claims"
            manual_pool_dir = Path(tmp_dir) / "manual-oauth-pool"
            claims_dir.mkdir(parents=True, exist_ok=True)
            claimed_path = claims_dir / "claimed.json"
            claimed_path.write_text('{"email":"free@example.com"}', encoding="utf-8")

            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_FREE_MANUAL_OAUTH_PRESERVE_ENABLED": "true",
                    "REGISTER_FREE_MANUAL_OAUTH_PRESERVE_ERROR_CODES": "token_invalidated",
                },
                clear=True,
            ):
                result = artifact_pool_claims.finalize_openai_oauth_artifact(
                    step_input={
                        "artifact": {
                            "claimed_path": str(claimed_path),
                            "original_name": "deadbeef-cafebabe-original.json",
                            "email": "free@example.com",
                        },
                        "task_error_code": "token_invalidated",
                        "free_manual_oauth_pool_dir": str(manual_pool_dir),
                    }
                )

            self.assertEqual("preserved_for_manual_oauth", result["status"])
            self.assertFalse(claimed_path.exists())
            self.assertTrue((manual_pool_dir / "original.json").exists())
            self.assertFalse((manual_pool_dir / "deadbeef-cafebabe-original.json").exists())

    def test_finalize_openai_oauth_artifact_promotes_success_strips_repeated_prefixes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "continue-runs" / "worker-01" / "run-20260530-task000001"
            continue_pool_dir = output_root / "openai" / "failed-once"
            continue_pool_dir.mkdir(parents=True, exist_ok=True)
            claims_dir = output_root / "others" / "openai-oauth-claims"
            claims_dir.mkdir(parents=True, exist_ok=True)
            claimed_path = claims_dir / "claimed.json"
            claimed_path.write_text(
                '{"email":"success@example.com","mailboxRef":"mailbox-ref","mailboxSessionId":"session-id","createdAt":"2026-05-30T00:02:54Z","platformOrganization":{"status":"completed"},"chatgptLogin":{"status":"completed","workspaceId":"ws_123"},"chatgptLoginDetails":{"clientBootstrap":{"authStatus":"logged_in","structure":"personal"}}}',
                encoding="utf-8",
            )

            result = artifact_pool_claims.finalize_openai_oauth_artifact(
                step_input={
                    "output_dir": str(run_output_dir),
                    "artifact": {
                        "claimed_path": str(claimed_path),
                        "original_name": "deadbeef-cafebabe-success.json",
                        "email": "success@example.com",
                        "pool_dir": str(continue_pool_dir),
                    },
                }
            )

            self.assertEqual("promoted_success", result["status"])
            self.assertFalse(claimed_path.exists())
            self.assertTrue((output_root / "openai" / "converted" / "success.json").exists())
            self.assertFalse((output_root / "openai" / "converted" / "deadbeef-cafebabe-success.json").exists())

    def test_finalize_openai_oauth_artifact_records_routing_history_on_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "continue-runs" / "worker-03" / "run-20260807-task000042"
            continue_pool_dir = output_root / "openai" / "failed-once"
            continue_pool_dir.mkdir(parents=True, exist_ok=True)
            claims_dir = output_root / "others" / "openai-oauth-claims"
            claims_dir.mkdir(parents=True, exist_ok=True)
            claimed_path = claims_dir / "claimed.json"
            claimed_path.write_text(
                '{"email":"history-success@example.com","mailboxRef":"mailbox-ref","mailboxSessionId":"session-id","createdAt":"2026-05-30T00:02:54Z","platformOrganization":{"status":"completed"},"chatgptLogin":{"status":"completed","workspaceId":"ws_123"},"chatgptLoginDetails":{"clientBootstrap":{"authStatus":"logged_in","structure":"personal"}}}',
                encoding="utf-8",
            )

            result = artifact_pool_claims.finalize_openai_oauth_artifact(
                step_input={
                    "output_dir": str(run_output_dir),
                    "artifact": {
                        "claimed_path": str(claimed_path),
                        "original_name": "history-success.json",
                        "email": "history-success@example.com",
                        "pool_dir": str(continue_pool_dir),
                    },
                    "worker_label": "worker-03",
                    "task_index": 42,
                }
            )

            self.assertEqual("promoted_success", result["status"])
            stored_path = output_root / "openai" / "converted" / "history-success.json"
            self.assertTrue(stored_path.exists())
            payload = json.loads(stored_path.read_text(encoding="utf-8"))
            history = payload.get("routingHistory")
            self.assertIsInstance(history, list)
            self.assertEqual(1, len(history))
            entry = history[0]
            self.assertEqual("openai/converted", entry.get("pool"))
            self.assertEqual("", entry.get("errorCode"))
            self.assertEqual("worker-03", entry.get("workerLabel"))
            self.assertEqual(42, entry.get("taskIndex"))
            self.assertTrue(str(entry.get("at") or "").endswith("Z"))

    def test_finalize_openai_oauth_artifact_records_routing_history_error_code_on_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "continue-runs" / "worker-01" / "run-20260807-task000007"
            continue_pool_dir = output_root / "openai" / "failed-once"
            continue_pool_dir.mkdir(parents=True, exist_ok=True)
            claims_dir = output_root / "others" / "openai-oauth-claims"
            claims_dir.mkdir(parents=True, exist_ok=True)
            claimed_path = claims_dir / "claimed.json"
            claimed_path.write_text('{"email":"history-fail@example.com"}', encoding="utf-8")

            result = artifact_pool_claims.finalize_openai_oauth_artifact(
                step_input={
                    "output_dir": str(run_output_dir),
                    "artifact": {
                        "claimed_path": str(claimed_path),
                        "original_name": "history-fail.json",
                        "email": "history-fail@example.com",
                        "pool_dir": str(continue_pool_dir),
                    },
                    "task_error_code": "phone_verification_attempted_small_success",
                    "failure_mode": "delete",
                    "worker_label": "worker-01",
                    "task_index": 7,
                }
            )

            self.assertEqual("restored", result["status"])
            stored_path = output_root / "openai" / "failed-twice" / "history-fail.json"
            self.assertTrue(stored_path.exists())
            payload = json.loads(stored_path.read_text(encoding="utf-8"))
            history = payload.get("routingHistory")
            self.assertIsInstance(history, list)
            self.assertEqual(1, len(history))
            entry = history[0]
            self.assertEqual("openai/failed-twice", entry.get("pool"))
            self.assertEqual("phone_verification_attempted_small_success", entry.get("errorCode"))
            self.assertEqual("worker-01", entry.get("workerLabel"))
            self.assertEqual(7, entry.get("taskIndex"))

    def test_finalize_openai_oauth_artifact_appends_to_existing_routing_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "continue-runs" / "worker-02" / "run-20260807-task000009"
            continue_pool_dir = output_root / "openai" / "failed-once"
            continue_pool_dir.mkdir(parents=True, exist_ok=True)
            claims_dir = output_root / "others" / "openai-oauth-claims"
            claims_dir.mkdir(parents=True, exist_ok=True)
            claimed_path = claims_dir / "claimed.json"
            claimed_path.write_text(
                json.dumps(
                    {
                        "email": "history-append@example.com",
                        "routingHistory": [
                            {
                                "at": "2026-07-01T00:00:00Z",
                                "pool": "openai/failed-once",
                                "errorCode": "authorize_continue_blocked",
                                "workerLabel": "worker-09",
                                "taskIndex": 1,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            artifact_pool_claims.finalize_openai_oauth_artifact(
                step_input={
                    "output_dir": str(run_output_dir),
                    "artifact": {
                        "claimed_path": str(claimed_path),
                        "original_name": "history-append.json",
                        "email": "history-append@example.com",
                        "pool_dir": str(continue_pool_dir),
                    },
                    "task_error_code": "phone_verification_attempted_small_success",
                    "worker_label": "worker-02",
                    "task_index": 9,
                }
            )

            stored_path = output_root / "openai" / "failed-twice" / "history-append.json"
            self.assertTrue(stored_path.exists())
            payload = json.loads(stored_path.read_text(encoding="utf-8"))
            history = payload.get("routingHistory")
            self.assertEqual(2, len(history))
            self.assertEqual("authorize_continue_blocked", history[0].get("errorCode"))
            self.assertEqual("openai/failed-twice", history[1].get("pool"))
            self.assertEqual("phone_verification_attempted_small_success", history[1].get("errorCode"))

    def test_finalize_openai_oauth_artifact_routes_normally_when_routing_history_stamp_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "continue-runs" / "worker-04" / "run-20260807-task000011"
            continue_pool_dir = output_root / "openai" / "failed-once"
            continue_pool_dir.mkdir(parents=True, exist_ok=True)
            claims_dir = output_root / "others" / "openai-oauth-claims"
            claims_dir.mkdir(parents=True, exist_ok=True)
            claimed_path = claims_dir / "claimed.json"
            claimed_path.write_text("{ this is not valid json", encoding="utf-8")

            result = artifact_pool_claims.finalize_openai_oauth_artifact(
                step_input={
                    "output_dir": str(run_output_dir),
                    "artifact": {
                        "claimed_path": str(claimed_path),
                        "original_name": "history-broken.json",
                        "email": "history-broken@example.com",
                        "pool_dir": str(continue_pool_dir),
                    },
                    "task_error_code": "authorize_continue_blocked",
                    "worker_label": "worker-04",
                    "task_index": 11,
                }
            )

            self.assertEqual("restored", result["status"])
            stored_path = continue_pool_dir / "history-broken.json"
            self.assertTrue(stored_path.exists())
            self.assertEqual("{ this is not valid json", stored_path.read_text(encoding="utf-8"))

    def test_finalize_openai_oauth_artifact_records_routing_history_for_manual_oauth_preserve(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "mixed-runs" / "worker-05" / "run-20260807-task000013"
            pool_dir = output_root / "openai" / "pending"
            pool_dir.mkdir(parents=True, exist_ok=True)
            claims_dir = output_root / "others" / "openai-oauth-claims"
            claims_dir.mkdir(parents=True, exist_ok=True)
            claimed_path = claims_dir / "claimed.json"
            claimed_path.write_text('{"email":"manual@example.com"}', encoding="utf-8")

            manual_pool_dir = output_root / "others" / "free-manual-oauth-pool"
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_FREE_MANUAL_OAUTH_PRESERVE_ENABLED": "true",
                    "REGISTER_FREE_MANUAL_OAUTH_PRESERVE_ERROR_CODES": "token_invalidated",
                },
                clear=True,
            ):
                result = artifact_pool_claims.finalize_openai_oauth_artifact(
                    step_input={
                        "output_dir": str(run_output_dir),
                        "artifact": {
                            "claimed_path": str(claimed_path),
                            "original_name": "manual.json",
                            "email": "manual@example.com",
                            "pool_dir": str(pool_dir),
                        },
                        "task_error_code": "token_invalidated",
                        "free_manual_oauth_pool_dir": str(manual_pool_dir),
                        "worker_label": "worker-05",
                        "task_index": 13,
                    }
                )

            self.assertEqual("preserved_for_manual_oauth", result["status"])
            stored_path = Path(result["restored_path"])
            self.assertTrue(stored_path.is_file())
            payload = json.loads(stored_path.read_text(encoding="utf-8"))
            history = payload.get("routingHistory")
            self.assertIsInstance(history, list)
            self.assertEqual(1, len(history))
            self.assertEqual("others/free-manual-oauth-pool", history[0].get("pool"))
            self.assertEqual("token_invalidated", history[0].get("errorCode"))
            self.assertEqual("worker-05", history[0].get("workerLabel"))
            self.assertEqual(13, history[0].get("taskIndex"))

    def test_finalize_openai_oauth_artifact_routes_continue_phone_verification_attempt_to_failed_twice(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "continue-runs" / "worker-01" / "run-20260502-task000001"
            continue_pool_dir = output_root / "openai" / "failed-once"
            continue_pool_dir.mkdir(parents=True, exist_ok=True)
            claims_dir = output_root / "others" / "openai-oauth-claims"
            claims_dir.mkdir(parents=True, exist_ok=True)
            claimed_path = claims_dir / "claimed.json"
            claimed_path.write_text('{"email":"retry@example.com"}', encoding="utf-8")

            result = artifact_pool_claims.finalize_openai_oauth_artifact(
                step_input={
                    "output_dir": str(run_output_dir),
                    "artifact": {
                        "claimed_path": str(claimed_path),
                        "original_name": "retry.json",
                        "email": "retry@example.com",
                        "pool_dir": str(continue_pool_dir),
                    },
                    "task_error_code": "phone_verification_attempted_small_success",
                    "failure_mode": "delete",
                }
            )

            self.assertEqual("restored", result["status"])
            self.assertFalse(claimed_path.exists())
            self.assertTrue((output_root / "openai" / "failed-twice" / "retry.json").exists())

    def test_finalize_openai_oauth_artifact_restores_continue_authorize_blocked_to_failed_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "continue-runs" / "worker-01" / "run-20260726-task000001"
            continue_pool_dir = output_root / "openai" / "failed-once"
            continue_pool_dir.mkdir(parents=True, exist_ok=True)
            claims_dir = output_root / "others" / "openai-oauth-claims"
            claims_dir.mkdir(parents=True, exist_ok=True)
            claimed_path = claims_dir / "claimed.json"
            claimed_path.write_text('{"email":"retry@example.com"}', encoding="utf-8")

            result = artifact_pool_claims.finalize_openai_oauth_artifact(
                step_input={
                    "output_dir": str(run_output_dir),
                    "artifact": {
                        "claimed_path": str(claimed_path),
                        "original_name": "retry.json",
                        "email": "retry@example.com",
                        "pool_dir": str(continue_pool_dir),
                    },
                    "task_error_code": "authorize_continue_blocked",
                    "failure_mode": "delete",
                }
            )

            failed_once_path = continue_pool_dir / "retry.json"
            failed_twice_path = output_root / "openai" / "failed-twice" / "retry.json"
            self.assertEqual("restored", result["status"])
            self.assertFalse(claimed_path.exists())
            self.assertEqual(continue_pool_dir.resolve(), Path(result["restore_pool_dir"]).resolve())
            self.assertTrue(failed_once_path.exists())
            self.assertFalse(failed_twice_path.exists())

    def test_finalize_openai_oauth_artifact_deletes_account_deactivated_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "continue-runs" / "worker-01" / "run-20260726-task000004"
            continue_pool_dir = output_root / "openai" / "failed-once"
            continue_pool_dir.mkdir(parents=True, exist_ok=True)
            claims_dir = output_root / "others" / "openai-oauth-claims"
            claims_dir.mkdir(parents=True, exist_ok=True)
            claimed_path = claims_dir / "claimed.json"
            claimed_path.write_text('{"email":"retry@example.com"}', encoding="utf-8")

            result = artifact_pool_claims.finalize_openai_oauth_artifact(
                step_input={
                    "output_dir": str(run_output_dir),
                    "artifact": {
                        "claimed_path": str(claimed_path),
                        "original_name": "retry.json",
                        "email": "retry@example.com",
                        "pool_dir": str(continue_pool_dir),
                    },
                    "task_error_code": "deactivated_workspace",
                    "failure_mode": "delete",
                }
            )

            self.assertEqual("deleted_failed_artifact", result["status"])
            self.assertFalse(claimed_path.exists())
            self.assertFalse((continue_pool_dir / "retry.json").exists())

    def test_finalize_openai_oauth_artifact_deletes_continue_missing_login_session_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "continue-runs" / "worker-01" / "run-20260813-task000001"
            continue_pool_dir = output_root / "openai" / "failed-once"
            continue_pool_dir.mkdir(parents=True, exist_ok=True)
            claims_dir = output_root / "others" / "openai-oauth-claims"
            claims_dir.mkdir(parents=True, exist_ok=True)
            claimed_path = claims_dir / "claimed.json"
            claimed_path.write_text('{"email":"retry@example.com"}', encoding="utf-8")

            result = artifact_pool_claims.finalize_openai_oauth_artifact(
                step_input={
                    "output_dir": str(run_output_dir),
                    "artifact": {
                        "claimed_path": str(claimed_path),
                        "original_name": "retry.json",
                        "email": "retry@example.com",
                        "pool_dir": str(continue_pool_dir),
                    },
                    "task_error_code": "authorize_missing_login_session",
                    "failure_mode": "delete",
                }
            )

            self.assertEqual("deleted_failed_artifact", result["status"])
            self.assertFalse(claimed_path.exists())
            self.assertFalse((continue_pool_dir / "retry.json").exists())
            self.assertFalse((output_root / "openai" / "failed-twice" / "retry.json").exists())

    def test_finalize_openai_oauth_artifact_routes_transient_continue_failure_without_upload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "continue-runs" / "worker-01" / "run-20260727-task000001"
            continue_pool_dir = output_root / "openai" / "failed-once"
            continue_pool_dir.mkdir(parents=True, exist_ok=True)
            claims_dir = output_root / "others" / "openai-oauth-claims"
            claims_dir.mkdir(parents=True, exist_ok=True)
            claimed_path = claims_dir / "claimed.json"
            claimed_path.write_text('{"email":"retry@example.com"}', encoding="utf-8")

            with mock.patch.dict(os.environ, {"REGISTER_OPENAI_UPLOAD_PERCENT": "73"}, clear=True), mock.patch.object(
                artifact_pool_claims,
                "route_openai_oauth_artifact",
                return_value={
                    "ok": True,
                    "route": "local",
                    "stored_path": str(continue_pool_dir / "retry.json"),
                    "object_key": "",
                },
            ) as route_artifact:
                result = artifact_pool_claims.finalize_openai_oauth_artifact(
                    step_input={
                        "output_dir": str(run_output_dir),
                        "artifact": {
                            "claimed_path": str(claimed_path),
                            "original_name": "retry.json",
                            "email": "retry@example.com",
                            "pool_dir": str(continue_pool_dir),
                        },
                        "task_error_code": "authorize_continue_blocked",
                        "failure_mode": "delete",
                    }
                )

            self.assertEqual("restored", result["status"])
            route_artifact.assert_called_once()
            route_kwargs = route_artifact.call_args.kwargs
            self.assertEqual(continue_pool_dir.resolve(), Path(route_kwargs["destination_dir"]).resolve())
            self.assertEqual("openai/failed-once", route_kwargs["target_folder"])
            self.assertEqual(0.0, route_kwargs["upload_percent"])
            self.assertTrue(route_kwargs["move_local"])

    def test_finalize_openai_oauth_artifact_keeps_transient_route_when_continue_and_phone_paths_alias(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "continue-runs" / "worker-01" / "run-20260727-task000002"
            aliased_pool_dir = output_root / "custom" / "aliased-continue-phone"
            aliased_pool_dir.mkdir(parents=True, exist_ok=True)
            claims_dir = output_root / "others" / "openai-oauth-claims"
            claims_dir.mkdir(parents=True, exist_ok=True)
            claimed_path = claims_dir / "claimed.json"
            claimed_path.write_text('{"email":"retry@example.com"}', encoding="utf-8")

            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OPENAI_OAUTH_CONTINUE_POOL_DIR": str(aliased_pool_dir),
                    "REGISTER_OPENAI_OAUTH_NEED_PHONE_POOL_DIR": str(aliased_pool_dir),
                    "REGISTER_OPENAI_UPLOAD_PERCENT": "73",
                },
                clear=True,
            ), mock.patch.object(
                artifact_pool_claims,
                "route_openai_oauth_artifact",
                return_value={
                    "ok": True,
                    "route": "local",
                    "stored_path": str(aliased_pool_dir / "retry.json"),
                    "object_key": "",
                },
            ) as route_artifact:
                result = artifact_pool_claims.finalize_openai_oauth_artifact(
                    step_input={
                        "output_dir": str(run_output_dir),
                        "openai_oauth_continue_pool_dir": str(aliased_pool_dir),
                        "artifact": {
                            "claimed_path": str(claimed_path),
                            "original_name": "retry.json",
                            "email": "retry@example.com",
                            "pool_dir": str(aliased_pool_dir),
                        },
                        "task_error_code": "authorize_continue_blocked",
                        "failure_mode": "delete",
                    }
                )

            self.assertEqual("restored", result["status"])
            route_artifact.assert_called_once()
            route_kwargs = route_artifact.call_args.kwargs
            self.assertEqual(aliased_pool_dir.resolve(), Path(route_kwargs["destination_dir"]).resolve())
            self.assertEqual("openai/failed-once", route_kwargs["target_folder"])
            self.assertEqual(0.0, route_kwargs["upload_percent"])
            self.assertTrue(route_kwargs["move_local"])

    def test_finalize_openai_oauth_artifact_routes_phone_follow_up_to_custom_pool_with_upload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "continue-runs" / "worker-01" / "run-20260727-task000003"
            continue_pool_dir = output_root / "openai" / "failed-once"
            continue_pool_dir.mkdir(parents=True, exist_ok=True)
            custom_phone_pool_dir = output_root / "custom" / "phone-follow-up"
            claims_dir = output_root / "others" / "openai-oauth-claims"
            claims_dir.mkdir(parents=True, exist_ok=True)

            error_codes = (
                "phone_verification_attempted_small_success",
                "phone_verification_submitted_small_success",
            )
            for index, error_code in enumerate(error_codes):
                with self.subTest(error_code=error_code):
                    original_name = f"retry-{index}.json"
                    claimed_path = claims_dir / f"claimed-{index}.json"
                    claimed_path.write_text('{"email":"retry@example.com"}', encoding="utf-8")

                    with mock.patch.dict(
                        os.environ,
                        {"REGISTER_OPENAI_UPLOAD_PERCENT": "73"},
                        clear=True,
                    ), mock.patch.object(
                        artifact_pool_claims,
                        "route_openai_oauth_artifact",
                        return_value={
                            "ok": True,
                            "route": "local",
                            "stored_path": str(custom_phone_pool_dir / original_name),
                            "object_key": "",
                        },
                    ) as route_artifact:
                        result = artifact_pool_claims.finalize_openai_oauth_artifact(
                            step_input={
                                "output_dir": str(run_output_dir),
                                "openai_oauth_need_phone_pool_dir": str(custom_phone_pool_dir),
                                "artifact": {
                                    "claimed_path": str(claimed_path),
                                    "original_name": original_name,
                                    "email": "retry@example.com",
                                    "pool_dir": str(continue_pool_dir),
                                },
                                "task_error_code": error_code,
                                "failure_mode": "delete",
                            }
                        )

                    self.assertEqual("restored", result["status"])
                    route_artifact.assert_called_once()
                    route_kwargs = route_artifact.call_args.kwargs
                    self.assertEqual(custom_phone_pool_dir.resolve(), Path(route_kwargs["destination_dir"]).resolve())
                    self.assertEqual("openai/failed-twice", route_kwargs["target_folder"])
                    self.assertEqual(73.0, route_kwargs["upload_percent"])
                    self.assertTrue(route_kwargs["move_local"])

    def test_finalize_openai_oauth_artifact_falls_back_when_restore_crosses_filesystems(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "continue-runs" / "worker-01" / "run-20260620-task000001"
            continue_pool_dir = output_root / "openai" / "failed-once"
            continue_pool_dir.mkdir(parents=True, exist_ok=True)
            claims_dir = output_root / "others" / "openai-oauth-claims"
            claims_dir.mkdir(parents=True, exist_ok=True)
            claimed_path = claims_dir / "claimed.json"
            claimed_path.write_text('{"email":"retry@example.com"}', encoding="utf-8")
            failed_twice_dir = (output_root / "openai" / "failed-twice").resolve()
            original_replace = Path.replace

            def _replace(self: Path, target: Path) -> Path:
                if self == claimed_path.resolve() and target.parent.resolve() == failed_twice_dir:
                    raise OSError(errno.EXDEV, "Invalid cross-device link")
                return original_replace(self, target)

            with mock.patch.object(Path, "replace", _replace):
                result = artifact_pool_claims.finalize_openai_oauth_artifact(
                    step_input={
                        "output_dir": str(run_output_dir),
                        "artifact": {
                            "claimed_path": str(claimed_path),
                            "original_name": "retry.json",
                            "email": "retry@example.com",
                            "pool_dir": str(continue_pool_dir),
                        },
                        "task_error_code": "phone_verification_attempted_small_success",
                        "failure_mode": "delete",
                    }
                )

            self.assertEqual("restored", result["status"])
            self.assertFalse(claimed_path.exists())
            restored_path = output_root / "openai" / "failed-twice" / "retry.json"
            self.assertTrue(restored_path.exists())
            restored_payload = json.loads(restored_path.read_text(encoding="utf-8"))
            self.assertEqual("retry@example.com", restored_payload.get("email"))
            self.assertEqual(
                "openai/failed-twice",
                (restored_payload.get("routingHistory") or [{}])[-1].get("pool"),
            )

    def test_finalize_openai_oauth_artifact_normalizes_materialized_continue_failure_name(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "continue-runs" / "worker-01" / "run-20260530-task000001"
            continue_pool_dir = output_root / "openai" / "failed-once"
            continue_pool_dir.mkdir(parents=True, exist_ok=True)
            claims_dir = output_root / "others" / "openai-oauth-claims"
            claims_dir.mkdir(parents=True, exist_ok=True)
            claimed_path = claims_dir / "deadbeef-materialized-e22baa73f218.json"
            claimed_path.write_text(
                (
                    '{"email":"agnese18417@ke.for4u.net",'
                    '"mailboxRef":"mailbox-ref",'
                    '"mailboxSessionId":"session-id",'
                    '"createdAt":"2026-05-30T00:02:54Z",'
                    '"platformOrganization":{"status":"completed"},'
                    '"chatgptLogin":{"status":"completed","workspaceId":"ws_123"},'
                    '"chatgptLoginDetails":{"clientBootstrap":{"authStatus":"logged_in","structure":"personal"}}}'
                ),
                encoding="utf-8",
            )

            result = artifact_pool_claims.finalize_openai_oauth_artifact(
                step_input={
                    "output_dir": str(run_output_dir),
                    "artifact": {
                        "claimed_path": str(claimed_path),
                        "original_name": "materialized-e22baa73f218.json",
                        "email": "agnese18417@ke.for4u.net",
                        "pool_dir": str(continue_pool_dir),
                    },
                    "task_error_code": "phone_verification_attempted_small_success",
                    "failure_mode": "delete",
                }
            )

            self.assertEqual("restored", result["status"])
            self.assertFalse(claimed_path.exists())
            restored_path = Path(str(result["restored_path"]))
            self.assertTrue(restored_path.is_file())
            self.assertEqual(output_root / "openai" / "failed-twice", restored_path.parent)
            self.assertTrue(restored_path.name.startswith("small-20260530-000254-agnese18417@ke.for4u.net-"))
            self.assertTrue(restored_path.name.endswith(".json"))
            self.assertNotIn("materialized", restored_path.name)

    def test_finalize_openai_oauth_artifact_strips_repeated_claim_prefixes_from_original_name(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "continue-runs" / "worker-01" / "run-20260530-task000001"
            continue_pool_dir = output_root / "openai" / "failed-once"
            continue_pool_dir.mkdir(parents=True, exist_ok=True)
            claims_dir = output_root / "others" / "openai-oauth-claims"
            claims_dir.mkdir(parents=True, exist_ok=True)
            claimed_path = claims_dir / "feedface-small-20260530-retry@example.com-abcdef.json"
            claimed_path.write_text(
                '{"email":"retry@example.com","mailboxRef":"mailbox-ref","mailboxSessionId":"session-id","createdAt":"2026-05-30T00:02:54Z","platformOrganization":{"status":"completed"},"chatgptLogin":{"status":"completed","workspaceId":"ws_123"},"chatgptLoginDetails":{"clientBootstrap":{"authStatus":"logged_in","structure":"personal"}}}',
                encoding="utf-8",
            )

            result = artifact_pool_claims.finalize_openai_oauth_artifact(
                step_input={
                    "output_dir": str(run_output_dir),
                    "artifact": {
                        "claimed_path": str(claimed_path),
                        "original_name": "deadbeef-cafebabe-small-20260530-retry@example.com-abcdef.json",
                        "email": "retry@example.com",
                        "pool_dir": str(continue_pool_dir),
                    },
                    "task_error_code": "phone_verification_attempted_small_success",
                    "failure_mode": "delete",
                }
            )

            self.assertEqual("restored", result["status"])
            self.assertFalse(claimed_path.exists())
            restored_path = Path(str(result["restored_path"]))
            self.assertTrue(restored_path.is_file())
            self.assertEqual(
                "small-20260530-retry@example.com-abcdef.json",
                restored_path.name,
            )
            self.assertFalse(restored_path.name.startswith("deadbeef-"))
            self.assertFalse(restored_path.name.startswith("cafebabe-"))

    def test_claim_team_member_candidates_short_circuits_when_target_is_satisfied(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            team_pre_pool_dir = Path(tmp_dir) / "team-pre-pool"
            claims_dir = Path(tmp_dir) / "team-claims"
            team_pre_pool_dir.mkdir(parents=True, exist_ok=True)
            claims_dir.mkdir(parents=True, exist_ok=True)
            mother_path = Path(tmp_dir) / "mother.json"
            mother_path.write_text(
                (
                    '{"teamFlow":{"teamExpandProgress":{"targetCount":4,'
                    '"successfulMemberEmails":["a@example.com","b@example.com","c@example.com","d@example.com"],'
                    '"successCount":4,"remainingCount":0,"readyForMotherCollection":true}}}'
                ),
                encoding="utf-8",
            )

            result = artifact_pool_claims.claim_team_member_candidates(
                step_input={
                    "member_count": 4,
                    "team_pre_pool_dir": str(team_pre_pool_dir),
                    "team_member_claims_dir": str(claims_dir),
                    "mother_artifact": {
                        "source_path": str(mother_path),
                    },
                }
            )

            self.assertEqual("target_already_satisfied", result["status"])
            self.assertEqual(0, result["member_count"])
            self.assertEqual([], result["members"])

    def test_claim_team_member_candidates_acquires_and_finalize_releases_conversion_lock(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "team-runs" / "worker-01" / "run-20260502-task000001"
            team_pre_pool_dir = output_root / "others" / "team-pre-pool"
            claims_dir = output_root / "others" / "team-member-claims"
            team_pre_pool_dir.mkdir(parents=True, exist_ok=True)
            claims_dir.mkdir(parents=True, exist_ok=True)
            seed_path = team_pre_pool_dir / "member.json"
            seed_path.write_text("{}", encoding="utf-8")
            mother_path = Path(tmp_dir) / "mother.json"
            mother_path.write_text(
                (
                    '{"teamFlow":{"teamExpandProgress":{"targetCount":1,'
                    '"successfulMemberEmails":[],"successCount":0,"remainingCount":1,"readyForMotherCollection":false}}}'
                ),
                encoding="utf-8",
            )

            with mock.patch.object(
                artifact_pool_claims,
                "load_openai_oauth_seed_validation",
                return_value=(True, "", {"email": "member@example.com", "password": "pw"}),
            ):
                result = artifact_pool_claims.claim_team_member_candidates(
                    step_input={
                        "output_dir": str(run_output_dir),
                        "member_count": 1,
                        "team_pre_pool_dir": str(team_pre_pool_dir),
                        "team_member_claims_dir": str(claims_dir),
                        "mother_artifact": {
                            "source_path": str(mother_path),
                        },
                        "worker_label": "worker-01",
                        "task_index": 2,
                    }
                )

            lock_dir = output_root / "others" / "openai-oauth-conversion-locks"
            self.assertEqual(1, len(list(lock_dir.glob("*.json"))))

            finalize_result = artifact_pool_team_batch.finalize_team_batch(
                step_input={
                    "output_dir": str(run_output_dir),
                    "invite_result": {
                        "successfulMemberEmails": ["member@example.com"],
                    },
                    "member_artifacts": result["members"],
                }
            )

            self.assertEqual("restored", finalize_result["status"])
            self.assertEqual([], list(lock_dir.glob("*.json")))


class ArtifactPoolTeamBatchTests(unittest.TestCase):
    def test_collect_team_pool_artifacts_collects_ready_mother_and_reuses_staged_member(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            team_pool_dir = Path(tmp_dir) / "team-pool"
            source_dir = Path(tmp_dir) / "source"
            team_pool_dir.mkdir(parents=True, exist_ok=True)
            source_dir.mkdir(parents=True, exist_ok=True)
            mother_path = source_dir / "mother-source.json"
            mother_path.write_text(
                (
                    '{"email":"mother@example.com","account_id":"acct_1",'
                    '"auth":{"organizations":[{"id":"org_1"}]},'
                    '"teamFlow":{"teamExpandProgress":{"targetCount":4,"successCount":4,'
                    '"successfulMemberEmails":["a@example.com","b@example.com","c@example.com","d@example.com"],'
                    '"remainingCount":0,"readyForMotherCollection":true}}}'
                ),
                encoding="utf-8",
            )
            staged_member_path = team_pool_dir / "member-already-staged.json"
            staged_member_path.write_text('{"email":"member@example.com"}', encoding="utf-8")

            result = artifact_pool_team_batch.collect_team_pool_artifacts(
                step_input={
                    "team_pool_dir": str(team_pool_dir),
                    "mother_artifact": {
                        "source_path": str(mother_path),
                        "successPath": str(mother_path),
                        "email": "mother@example.com",
                    },
                    "member_artifacts": [
                        {
                            "email": "member@example.com",
                            "team_pool_path": str(staged_member_path),
                        }
                    ],
                }
            )

            self.assertEqual("collected", result["status"])
            self.assertEqual(2, result["count"])
            self.assertFalse(mother_path.exists())
            self.assertTrue(staged_member_path.exists())
            self.assertTrue(any(item["kind"] == "mother" for item in result["artifacts"]))
            self.assertTrue(any(item["team_pool_path"] == str(staged_member_path) for item in result["artifacts"]))

    def test_finalize_team_batch_restores_unsuccessful_member_and_deletes_successful_member(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            team_pre_pool_dir = Path(tmp_dir) / "team-pre-pool"
            member_claims_dir = Path(tmp_dir) / "member-claims"
            team_pre_pool_dir.mkdir(parents=True, exist_ok=True)
            member_claims_dir.mkdir(parents=True, exist_ok=True)

            success_claimed = member_claims_dir / "deadbeef-success.json"
            retry_claimed = member_claims_dir / "cafebabe-retry.json"
            success_claimed.write_text('{"email":"success@example.com"}', encoding="utf-8")
            retry_claimed.write_text('{"email":"retry@example.com"}', encoding="utf-8")

            result = artifact_pool_team_batch.finalize_team_batch(
                step_input={
                    "team_pre_pool_dir": str(team_pre_pool_dir),
                    "invite_result": {
                        "successfulMemberEmails": ["success@example.com"],
                    },
                    "member_artifacts": [
                        {
                            "claimed_path": str(success_claimed),
                            "source_path": str(success_claimed),
                            "original_name": "success.json",
                            "email": "success@example.com",
                        },
                        {
                            "claimed_path": str(retry_claimed),
                            "source_path": str(retry_claimed),
                            "original_name": "retry.json",
                            "email": "retry@example.com",
                        },
                    ],
                }
            )

            self.assertEqual("restored", result["status"])
            self.assertFalse(success_claimed.exists())
            self.assertFalse(retry_claimed.exists())
            self.assertTrue((team_pre_pool_dir / "retry.json").exists())
            self.assertEqual(2, len(result["restored"]))
            self.assertEqual(0, len(result["deleted"]))

    def test_finalize_team_batch_restores_mother_after_soft_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            team_mother_pool_dir = Path(tmp_dir) / "codex" / "team-mother-input"
            team_mother_claims_dir = Path(tmp_dir) / "team-mother-claims"
            team_mother_pool_dir.mkdir(parents=True, exist_ok=True)
            team_mother_claims_dir.mkdir(parents=True, exist_ok=True)

            mother_claimed = team_mother_claims_dir / "deadbeef-mother.json"
            mother_claimed.write_text('{"email":"mother@example.com"}', encoding="utf-8")

            result = artifact_pool_team_batch.finalize_team_batch(
                step_input={
                    "invite_result": {
                        "allInviteAttemptsFailed": True,
                        "memberOauthRequired": False,
                        "status": "mother_only_all_invites_failed",
                    },
                    "mother_artifact": {
                        "claimed_path": str(mother_claimed),
                        "source_path": str(mother_claimed),
                        "original_name": "mother.json",
                        "pool_dir": str(team_mother_pool_dir),
                    },
                    "team_mother_pool_dir": str(team_mother_pool_dir),
                }
            )

            self.assertEqual("restored", result["status"])
            self.assertFalse(mother_claimed.exists())
            self.assertTrue((team_mother_pool_dir / "mother.json").exists())
            self.assertEqual(1, len(result["restored"]))
            self.assertEqual([], result["deleted"])
