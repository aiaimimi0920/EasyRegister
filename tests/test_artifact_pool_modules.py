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
    def test_finalize_small_success_artifact_preserves_manual_oauth(self) -> None:
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
                result = artifact_pool_claims.finalize_small_success_artifact(
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

            self.assertEqual("mixed", result["status"])
            self.assertFalse(success_claimed.exists())
            self.assertFalse(retry_claimed.exists())
            self.assertTrue((team_pre_pool_dir / "retry.json").exists())
            self.assertEqual(1, len(result["restored"]))
            self.assertEqual(1, len(result["deleted"]))
