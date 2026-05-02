from __future__ import annotations

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

    def test_finalize_openai_oauth_artifact_routes_continue_failure_to_failed_twice(self) -> None:
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
                    "task_error_code": "obtain_codex_oauth_failed",
                    "failure_mode": "delete",
                }
            )

            self.assertEqual("restored", result["status"])
            self.assertFalse(claimed_path.exists())
            self.assertTrue((output_root / "openai" / "failed-twice" / "retry.json").exists())

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
