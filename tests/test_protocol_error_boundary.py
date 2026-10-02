from __future__ import annotations

import io
import json
import sys
import unittest
import urllib.error
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


ROOT = Path(__file__).resolve().parents[1] / "server" / "services"
for path in (ROOT / "orchestration_service" / "src", ROOT / "python_shared" / "src"):
    sys.path.insert(0, str(path))

import dst_flow
from errors import ErrorCodes, ProtocolRuntimeError, build_error_details
from others import easyprotocol_runtime, runner_failures


class ProtocolErrorBoundaryTests(unittest.TestCase):
    def response_payload(self) -> dict:
        return {
            "status": "failed",
            "error": {
                "category": "operation_error",
                "message": "browser_verification_required status=403 cf_mitigated=challenge",
                "details": {
                    "step_type": "create_openai_account",
                    "protocol_error": {
                        "error": "browser_verification_required status=403 cf_mitigated=challenge",
                        "stage": "stage_platform_login",
                        "detail": "platform_login",
                        "category": "blocked",
                    },
                },
            },
        }

    def test_http_and_failed_payload_paths_keep_remote_error_metadata(self) -> None:
        for status in (200, 502):
            with self.subTest(status=status):
                raw = json.dumps(self.response_payload()).encode("utf-8")
                if status == 200:
                    response = mock.patch.object(
                        easyprotocol_runtime.urllib.request, "urlopen", return_value=io.BytesIO(raw)
                    )
                else:
                    response = mock.patch.object(
                        easyprotocol_runtime.urllib.request, "urlopen",
                        side_effect=urllib.error.HTTPError(
                            "http://protocol.invalid/request", status, "failed", {}, io.BytesIO(raw)
                        ),
                    )
                with response, self.assertRaises(ProtocolRuntimeError) as caught:
                    easyprotocol_runtime.invoke_easyprotocol(
                        step_type="create_openai_account", step_input={}
                    )
                error = caught.exception
                self.assertEqual("stage_platform_login", error.stage)
                self.assertEqual("platform_login", error.detail)
                self.assertEqual("blocked", error.category)
                details = build_error_details(
                    step_type="create_openai_account", message=str(error),
                    stage=error.stage, detail=error.detail, category=error.category,
                )
                self.assertEqual(ErrorCodes.BROWSER_VERIFICATION_REQUIRED, details["code"])

    def test_legacy_error_object_does_not_become_a_stringified_envelope(self) -> None:
        error = easyprotocol_runtime._easyprotocol_response_error(
            {"error": {"message": "fixture failure", "category": "operation_error"}},
            fallback="http_failure",
        )
        self.assertEqual("fixture failure", str(error))

    def test_challenge_words_in_mailbox_provider_do_not_require_interactive_auth(self) -> None:
        details = build_error_details(
            step_type="create_openai_account",
            message="otp_timeout [mailbox_provider=cloudflare_temp_email]",
        )
        self.assertEqual(ErrorCodes.OTP_TIMEOUT, details["code"])

    def test_interactive_verification_never_enters_step_or_task_retry(self) -> None:
        code = ErrorCodes.BROWSER_VERIFICATION_REQUIRED
        retry = {"maxAttempts": 3, "retryOnCodes": [code, ErrorCodes.TRANSPORT_ERROR]}
        statement = dst_flow.DstStatement(
            step_id="create-openai-account", step_type="create_openai_account",
            metadata={"retry": retry},
        )
        plan = dst_flow.DstPlan(steps=[statement], metadata={"taskRetry": retry})
        for error_code, expected in ((code, False), (ErrorCodes.TRANSPORT_ERROR, True)):
            with self.subTest(error_code=error_code):
                details = {"code": error_code}
                self.assertEqual(expected, dst_flow._should_retry_step(
                    statement=statement, error_details=details, attempt_index=1,
                ))
                self.assertEqual(expected, dst_flow._should_retry_task(
                    plan=plan, error_step=statement.step_id, error_details=details, attempt_index=1,
                ))

    def test_recorded_phone_milestone_retains_priority_over_later_challenge(self) -> None:
        for code in (
            ErrorCodes.PHONE_VERIFICATION_ATTEMPTED_SMALL_SUCCESS,
            ErrorCodes.PHONE_VERIFICATION_SUBMITTED_SMALL_SUCCESS,
        ):
            with self.subTest(code=code):
                details = build_error_details(
                    step_type="obtain_codex_oauth", code=code,
                    message=f"{code}: later browser_verification_required",
                )
                self.assertEqual(code, details["code"])

    def test_typed_error_keeps_priority_over_a_secondary_message(self) -> None:
        details = build_error_details(
            step_type="obtain_codex_oauth", code=ErrorCodes.REFRESH_TOKEN_REUSED,
            message="refresh_token_reused; previous browser_verification_required",
        )
        self.assertEqual(ErrorCodes.REFRESH_TOKEN_REUSED, details["code"])

    def test_verification_required_keeps_the_existing_blocked_cooldown(self) -> None:
        config = SimpleNamespace(create_account_cooldown_seconds=17, oauth_blocked_cooldown_seconds=181)
        for code in (ErrorCodes.AUTHORIZE_CONTINUE_BLOCKED, ErrorCodes.BROWSER_VERIFICATION_REQUIRED):
            payload = {
                "errorStep": "create-openai-account",
                "stepErrors": {"create-openai-account": {"code": code, "message": "status=403"}},
            }
            with self.subTest(code=code), mock.patch.object(
                runner_failures, "result_payload", return_value=payload
            ), mock.patch.object(
                runner_failures, "_cleanup_runtime_config", return_value=config
            ), mock.patch.object(runner_failures, "_team_auth_runtime_config", return_value=None):
                self.assertEqual(181, runner_failures.extra_failure_cooldown_seconds(result=payload))

    def test_rejected_or_ambiguous_otp_resend_is_not_retried(self) -> None:
        codes = [ErrorCodes.AUTHORIZE_CONTINUE_RATE_LIMITED, ErrorCodes.TRANSPORT_ERROR]
        retry = {"maxAttempts": 3, "retryOnCodes": codes}
        statement = dst_flow.DstStatement(
            step_id="create-openai-account", step_type="create_openai_account", metadata={"retry": retry},
        )
        plan = dst_flow.DstPlan(steps=[statement], metadata={"taskRetry": retry})
        for code in codes:
            for detail, expected in (("email_otp_resend", False), ("ordinary_operation", True)):
                with self.subTest(code=code, detail=detail):
                    error = {"code": code, "detail": detail}
                    self.assertEqual(expected, dst_flow._should_retry_step(
                        statement=statement, error_details=error, attempt_index=1,
                    ))
                    self.assertEqual(expected, dst_flow._should_retry_task(
                        plan=plan, error_step=statement.step_id, error_details=error, attempt_index=1,
                    ))


if __name__ == "__main__":
    unittest.main()
