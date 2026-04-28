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

from errors import ErrorCodes  # noqa: E402
from others import runner_artifacts, runner_failures, runner_mailbox, runner_team_auth, runner_team_cleanup  # noqa: E402


class RunnerArtifactsTests(unittest.TestCase):
    def test_select_local_split_obeys_percentage(self) -> None:
        with mock.patch("others.runner_artifacts.random.random", return_value=0.20):
            self.assertTrue(runner_artifacts.select_local_split(percent=50.0))
        with mock.patch("others.runner_artifacts.random.random", return_value=0.80):
            self.assertFalse(runner_artifacts.select_local_split(percent=50.0))

    def test_small_success_failure_target_pool_dir_routes_wait_pool(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            with mock.patch.dict(os.environ, {"REGISTER_OUTPUT_ROOT": str(output_root)}, clear=True):
                target = runner_artifacts.small_success_failure_target_pool_dir(
                    output_root=output_root,
                    result_payload_value={"errorCode": "free_personal_workspace_missing"},
                )
        self.assertEqual((output_root / "others" / "small-success-wait-pool").resolve(), target)

    def test_small_success_failure_target_pool_dir_routes_manual_oauth_pool(self) -> None:
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
                target = runner_artifacts.small_success_failure_target_pool_dir(
                    output_root=output_root,
                    result_payload_value={"errorCode": "token_invalidated"},
                )
        self.assertEqual((output_root / "others" / "free-manual-oauth-pool").resolve(), target)


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
                    }
                },
            }
            outcome = runner_mailbox.record_business_mailbox_domain_outcome(
                shared_root=shared_root,
                result_payload_value=payload,
                instance_role="main",
            )
            self.assertIsNotNone(outcome)
            self.assertEqual("sall.cc", outcome["domain"])
            state_path = Path(outcome["statePath"])
            self.assertTrue(state_path.is_file())

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


if __name__ == "__main__":
    unittest.main()
