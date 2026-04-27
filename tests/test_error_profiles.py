from __future__ import annotations

import sys
import unittest
from pathlib import Path


SRC_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

import dst_flow  # noqa: E402
from errors import (  # noqa: E402
    ErrorCodes,
    ProtocolRuntimeError,
    build_error_details,
    ensure_protocol_runtime_error,
    resolve_retry_codes,
    result_error_code,
    result_error_matches,
    result_error_message,
)


class ErrorProfilesTests(unittest.TestCase):
    def test_build_error_details_classifies_team_auth_token_invalidated(self) -> None:
        details = build_error_details(
            step_type="invite_codex_member",
            message="Authentication token has been invalidated. Please try signing in again.",
            detail="invite_failed",
        )
        self.assertEqual(ErrorCodes.TEAM_AUTH_TOKEN_INVALIDATED, details["code"])
        self.assertEqual("auth_error", details["category"])

    def test_build_error_details_classifies_mailbox_unavailable(self) -> None:
        details = build_error_details(
            step_type="acquire_mailbox",
            message='code=mailbox_capacity_unavailable detail="maximum mailbox"',
        )
        self.assertEqual(ErrorCodes.MAILBOX_UNAVAILABLE, details["code"])

    def test_resolve_retry_codes_uses_profile(self) -> None:
        self.assertEqual(
            {
                ErrorCodes.TRANSPORT_ERROR,
                ErrorCodes.UPLOAD_FILE_TO_R2_FAILED,
            },
            resolve_retry_codes({"retryProfile": "step-upload-artifact"}),
        )

    def test_protocol_runtime_error_carries_inferred_code(self) -> None:
        exc = ensure_protocol_runtime_error(
            RuntimeError("mailbox capacity unavailable"),
            stage="mailbox",
            detail="create_mailbox",
        )
        self.assertIsInstance(exc, ProtocolRuntimeError)
        self.assertEqual(ErrorCodes.MAILBOX_UNAVAILABLE, exc.code)
        self.assertEqual("flow_error", exc.category)
        self.assertEqual(ErrorCodes.MAILBOX_UNAVAILABLE, exc.to_response_payload()["code"])

    def test_result_error_helpers_use_structured_payload(self) -> None:
        payload = {
            "errorStep": "invite-codex-member",
            "error": "Workspace has reached maximum number of seats",
            "stepErrors": {
                "invite-codex-member": {
                    "code": ErrorCodes.TEAM_SEATS_FULL,
                    "message": "Workspace has reached maximum number of seats",
                }
            },
        }
        self.assertEqual(
            ErrorCodes.TEAM_SEATS_FULL,
            result_error_code(payload, "invite-codex-member"),
        )
        self.assertTrue(
            result_error_matches(payload, ErrorCodes.TEAM_SEATS_FULL, step_id="invite-codex-member")
        )
        self.assertIn("maximum number of seats", result_error_message(payload, "invite-codex-member").lower())

    def test_dst_flow_step_retry_uses_retry_profile(self) -> None:
        statement = dst_flow.DstStatement(
            step_id="upload-oauth-artifact",
            step_type="upload_file_to_r2",
            metadata={
                "retry": {
                    "maxAttempts": 2,
                    "retryProfile": "step-upload-artifact",
                }
            },
        )
        self.assertTrue(
            dst_flow._should_retry_step(
                statement=statement,
                error_details={"code": ErrorCodes.UPLOAD_FILE_TO_R2_FAILED},
                attempt_index=1,
            )
        )
        self.assertFalse(
            dst_flow._should_retry_step(
                statement=statement,
                error_details={"code": ErrorCodes.TEAM_SEATS_FULL},
                attempt_index=1,
            )
        )

    def test_dst_flow_task_retry_uses_retry_profile(self) -> None:
        plan = dst_flow.DstPlan(
            steps=[],
            metadata={
                "taskRetry": {
                    "maxAttempts": 3,
                    "retryProfile": "task-team-expand-default",
                    "retryOnSteps": ["obtain-team-mother-oauth"],
                }
            },
        )
        self.assertTrue(
            dst_flow._should_retry_task(
                plan=plan,
                error_step="obtain-team-mother-oauth",
                error_details={"code": ErrorCodes.TRANSPORT_ERROR},
                attempt_index=1,
            )
        )
        self.assertFalse(
            dst_flow._should_retry_task(
                plan=plan,
                error_step="obtain-team-mother-oauth",
                error_details={"code": ErrorCodes.TEAM_SEATS_FULL},
                attempt_index=1,
            )
        )


if __name__ == "__main__":
    unittest.main()
