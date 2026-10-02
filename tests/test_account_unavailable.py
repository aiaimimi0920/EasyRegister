from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import unittest
import urllib.error
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
for path in (ROOT / "server/services/orchestration_service/src", ROOT / "server/services/python_shared/src"):
    sys.path.insert(0, str(path))

import dst_flow
from others import easyprotocol_runtime
from others.error_runtime import ProtocolRuntimeError, build_error_details
from others.error_catalog import ErrorCodes, RETRY_PROFILES
from others.dst_flow_runtime import should_retry_step, should_retry_task

SENTENCE = "You do not have an account because it has been deleted or deactivated. If you believe this was an error, please contact us through our help center at help.openai.com."


def failure(body, *, step="initialize_chatgpt_login_session"):
    return build_error_details(
        step_type=step, message="chatgpt_login_otp_validate_failed status=403 body=" + body,
        detail="chatgpt_login_email_otp_validate", stage="stage_otp_validate",
        category="flow_error", code="authorize_continue_blocked",
    )


class AccountUnavailableTests(unittest.TestCase):
    def test_exact_account_rejection_is_not_generic_browser_block(self):
        body = json.dumps({"error": {"message": SENTENCE, "type": "invalid_request_error"}}, indent=2)
        for value in (body, body[:220], SENTENCE):
            with self.subTest(body_length=len(value)):
                result = failure(value)
                self.assertEqual(result["code"], "account_unavailable")
                self.assertEqual(result["category"], "blocked")
                self.assertEqual(result["stage"], "stage_otp_validate")
                self.assertTrue(result["message"].endswith(value))

    def test_other_rejections_are_not_account_unavailable(self):
        for body in (
            '<html>Just a moment...</html>', '{"error":{"code":"wrong_email_otp_code"}}',
            '{"error":{"message":"Invalid session"}}',
            json.dumps({"error": {"message": "Provider reported: " + SENTENCE}}),
            json.dumps({"error": {"message": "Your account has not been deactivated."}}),
            json.dumps({"error": {"message": 'Example wording: "' + SENTENCE + '"'}}),
        ):
            with self.subTest(body=body):
                self.assertNotEqual(failure(body)["code"], "account_unavailable")

    def test_unrelated_step_and_workspace_semantics_are_preserved(self):
        self.assertNotEqual(failure(SENTENCE, step="invite_codex_member")["code"], "account_unavailable")
        result = build_error_details(step_type="obtain_codex_oauth", message="account_deactivated")
        self.assertEqual(result["code"], ErrorCodes.TEAM_WORKSPACE_DEACTIVATED)

    def test_no_default_retry_profile_includes_account_unavailable(self):
        self.assertTrue(all("account_unavailable" not in codes for codes in RETRY_PROFILES.values()))

    def test_account_body_does_not_override_other_specific_typed_errors(self):
        for code in (ErrorCodes.TEAM_WORKSPACE_DEACTIVATED, ErrorCodes.REFRESH_TOKEN_REUSED,
                     ErrorCodes.OTP_TIMEOUT, ErrorCodes.BROWSER_VERIFICATION_REQUIRED, "provider_specific_error"):
            with self.subTest(code=code):
                result = build_error_details(step_type="initialize_chatgpt_login_session",
                                             message="chatgpt_login_otp_validate_failed status=403 body=" + SENTENCE,
                                             code=code)
                self.assertEqual(result["code"], code)

    def test_step_retry_cannot_override_terminal_account_rejection(self):
        statement = SimpleNamespace(step_type="initialize_chatgpt_login_session", metadata={
            "retry": {"maxAttempts": 3, "retryOnCodes": ["account_unavailable"]},
        })
        self.assertFalse(should_retry_step(statement=statement, error_details={"code": "account_unavailable"}, attempt_index=1))

    def test_task_retry_cannot_override_terminal_account_rejection(self):
        plan = SimpleNamespace(metadata={"taskRetry": {
            "maxAttempts": 3, "retryOnCodes": ["account_unavailable"],
        }})
        self.assertFalse(should_retry_task(plan=plan, error_step="initialize-chatgpt-login-session",
                                          error_details={"code": "account_unavailable"}, attempt_index=1))

    def test_service_failure_envelope_keeps_account_rejection_and_stage(self):
        message = "chatgpt_login_otp_validate_failed status=403 body=" + json.dumps({
            "error": {"message": SENTENCE, "type": "invalid_request_error"},
        })
        payload = {"status": "failed", "error": {
            "category": "operation_error", "message": message,
            "details": {"protocol_error": {
                "error": message, "stage": "stage_otp_validate",
                "detail": "chatgpt_login_email_otp_validate", "category": "flow_error",
            }},
        }}
        raw = json.dumps(payload).encode("utf-8")
        for status in (200, 502):
            with self.subTest(status=status):
                if status == 200:
                    boundary = mock.patch.object(easyprotocol_runtime.urllib.request, "urlopen", return_value=io.BytesIO(raw))
                else:
                    boundary = mock.patch.object(easyprotocol_runtime.urllib.request, "urlopen", side_effect=urllib.error.HTTPError(
                        "http://protocol.invalid/request", status, "failed", {}, io.BytesIO(raw),
                    ))
                with boundary, self.assertRaises(ProtocolRuntimeError) as caught:
                    easyprotocol_runtime.invoke_easyprotocol(step_type="initialize_chatgpt_login_session", step_input={})
                error = caught.exception
                result = build_error_details(step_type="initialize_chatgpt_login_session", message=str(error),
                                             stage=error.stage, detail=error.detail, category=error.category, code=error.code)
                self.assertEqual(result["code"], ErrorCodes.ACCOUNT_UNAVAILABLE)
                self.assertEqual(result["category"], "blocked")
                self.assertEqual(result["stage"], "stage_otp_validate")

    def test_actual_dst_flow_stops_before_oauth_and_still_cleans_up(self):
        retry = {"maxAttempts": 3, "retryOnCodes": [ErrorCodes.ACCOUNT_UNAVAILABLE]}
        flow = {"definition": {"id": "offline-account-rejection", "platform": "codex",
            "metadata": {"taskRetry": retry}, "steps": [
                {"id": "initialize-chatgpt-login-session", "type": "initialize_chatgpt_login_session",
                 "metadata": {"owner": "easyprotocol", "retry": retry}},
                {"id": "obtain-codex-oauth", "type": "obtain_codex_oauth", "metadata": {"owner": "easyprotocol"}},
                {"id": "release-mailbox", "type": "release_mailbox", "metadata": {"owner": "easyemail", "alwaysRun": True}},
            ],
        }}
        calls = []

        def dispatch(*, step_type, step_input):
            calls.append(step_type)
            if step_type == "initialize_chatgpt_login_session":
                raise ProtocolRuntimeError("chatgpt_login_otp_validate_failed status=403 body=" + SENTENCE,
                                           stage="stage_otp_validate", detail="chatgpt_login_email_otp_validate",
                                           category="flow_error", code="authorize_continue_blocked")
            if step_type == "release_mailbox":
                return {"released": True}
            raise AssertionError("下游 OAuth/短信不应执行")

        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)
            flow_path = path / "flow.json"
            flow_path.write_text(json.dumps(flow), encoding="utf-8")
            with mock.patch.dict(dst_flow.OWNER_DISPATCHERS, {"easyprotocol": dispatch, "easyemail": dispatch}, clear=True), \
                 mock.patch.dict(os.environ, {"REGISTER_OUTPUT_ROOT": str(path / "output")}, clear=False):
                result = dst_flow.run_dst_flow_once(flow_path=flow_path, output_dir=str(path / "run"), task_max_attempts=3)
        self.assertFalse(result.ok)
        self.assertEqual(result.task_attempts, 1)
        self.assertEqual(result.step_attempts["initialize-chatgpt-login-session"], 1)
        self.assertEqual(result.step_errors["initialize-chatgpt-login-session"]["code"], ErrorCodes.ACCOUNT_UNAVAILABLE)
        self.assertEqual(result.steps["obtain-codex-oauth"], "skipped")
        self.assertEqual(calls, ["initialize_chatgpt_login_session", "release_mailbox"])



if __name__ == "__main__":
    unittest.main(verbosity=2)
