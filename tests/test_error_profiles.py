from __future__ import annotations

import sys
import tempfile
import unittest
import json
from pathlib import Path


SRC_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "src"
PYTHON_SHARED_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "python_shared" / "src"
for candidate in (SRC_ROOT, PYTHON_SHARED_ROOT):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

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
    def test_main_flow_retry_profiles_match_expected_steps(self) -> None:
        flow_path = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "flows" / "codex-openai-account-v1.semantic-flow.json"
        payload = json.loads(flow_path.read_text(encoding="utf-8"))
        task_retry = payload["definition"]["metadata"]["taskRetry"]
        steps = payload["definition"]["steps"]
        by_id = {str(step.get("id") or ""): step for step in steps}
        self.assertEqual(
            "step-create-account-recover",
            by_id["create-openai-account"]["metadata"]["retry"]["retryProfile"],
        )
        self.assertEqual(
            "step-login-init-recover",
            by_id["initialize-chatgpt-login-session"]["metadata"]["retry"]["retryProfile"],
        )
        self.assertEqual(
            3,
            by_id["initialize-chatgpt-login-session"]["metadata"]["retry"]["maxAttempts"],
        )
        self.assertEqual(
            "step-proxy-refresh",
            by_id["initialize-platform-organization"]["metadata"]["retry"]["retryProfile"],
        )
        self.assertEqual(
            "step-oauth-recover",
            by_id["obtain-codex-oauth"]["metadata"]["retry"]["retryProfile"],
        )
        self.assertEqual(
            3,
            by_id["obtain-codex-oauth"]["metadata"]["retry"]["maxAttempts"],
        )
        self.assertEqual(
            ["proxy_chain", "initialize_chatgpt_login_session"],
            by_id["obtain-codex-oauth"]["metadata"]["retry"]["refreshSavedStates"],
        )
        self.assertNotIn("obtain-codex-oauth", task_retry["retryOnSteps"])

    def test_continue_flow_obtain_oauth_retries_after_login_refresh(self) -> None:
        flow_path = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "flows" / "codex-openai-oauth-continue-v1.semantic-flow.json"
        payload = json.loads(flow_path.read_text(encoding="utf-8"))
        task_retry = payload["definition"]["metadata"]["taskRetry"]
        steps = payload["definition"]["steps"]
        by_id = {str(step.get("id") or ""): step for step in steps}

        self.assertEqual(
            "step-oauth-recover",
            by_id["obtain-codex-oauth"]["metadata"]["retry"]["retryProfile"],
        )
        self.assertEqual(
            3,
            by_id["obtain-codex-oauth"]["metadata"]["retry"]["maxAttempts"],
        )
        self.assertEqual(
            ["proxy_chain", "initialize_chatgpt_login_session"],
            by_id["obtain-codex-oauth"]["metadata"]["retry"]["refreshSavedStates"],
        )
        self.assertIn("obtain-codex-oauth", task_retry["retryOnSteps"])

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

    def test_build_error_details_classifies_mailbox_open_fetch_failed(self) -> None:
        details = build_error_details(
            step_type="acquire_mailbox",
            message=(
                "mail service POST /mail/mailboxes/open failed: "
                'HTTP 500 [code=fetch failed]: {"error":"fetch failed"}'
            ),
        )
        self.assertEqual(ErrorCodes.MAILBOX_UNAVAILABLE, details["code"])
        self.assertEqual("flow_error", details["category"])

    def test_build_error_details_classifies_im215_mailbox_provider_unavailable(self) -> None:
        for message in (
            (
                "mail service POST /mail/mailboxes/open failed: HTTP 500 "
                '[code=No available 215.im credentials for poll.]: '
                '{"error":"No available 215.im credentials for poll."}'
            ),
            (
                "mail service POST /mail/mailboxes/open failed: HTTP 500 "
                "[code=215.im createMailbox failed with status 403. This shared domain is currently "
                "restricted and not accepting new public addresses]: "
                '{"error":"215.im createMailbox failed with status 403. This shared domain is currently '
                'restricted and not accepting new public addresses"}'
            ),
        ):
            with self.subTest(message=message):
                details = build_error_details(
                    step_type="acquire_mailbox",
                    message=message,
                )
                self.assertEqual(ErrorCodes.MAILBOX_UNAVAILABLE, details["code"])
                self.assertEqual("flow_error", details["category"])

    def test_resolve_retry_codes_uses_profile(self) -> None:
        self.assertEqual(
            {
                ErrorCodes.TRANSPORT_ERROR,
                ErrorCodes.UPLOAD_FILE_TO_R2_FAILED,
            },
            resolve_retry_codes({"retryProfile": "step-upload-artifact"}),
        )

    def test_resolve_retry_codes_uses_invite_recover_profile(self) -> None:
        self.assertEqual(
            {
                ErrorCodes.TEAM_AUTH_TOKEN_INVALIDATED,
                ErrorCodes.FLOW_TIMEOUT_EXCEEDED,
                ErrorCodes.PROXY_CONNECT_FAILED,
                ErrorCodes.TRANSPORT_ERROR,
                ErrorCodes.SOURCE_ARTIFACT_MISSING,
                ErrorCodes.TEAM_INVITE_UPSTREAM_ERROR,
            },
            resolve_retry_codes({"retryProfile": "step-invite-recover"}),
        )

    def test_resolve_retry_codes_uses_create_account_recover_profile(self) -> None:
        self.assertEqual(
            {
                ErrorCodes.USER_REGISTER_400,
                ErrorCodes.UNSUPPORTED_EMAIL,
                ErrorCodes.INVALID_REQUEST_ERROR,
                ErrorCodes.OTP_TIMEOUT,
                ErrorCodes.AUTHORIZE_CONTINUE_BLOCKED,
                ErrorCodes.AUTHORIZE_CONTINUE_RATE_LIMITED,
                ErrorCodes.AUTHORIZE_MISSING_LOGIN_SESSION,
                ErrorCodes.FLOW_TIMEOUT_EXCEEDED,
                ErrorCodes.PROXY_CONNECT_FAILED,
                ErrorCodes.TRANSPORT_ERROR,
                ErrorCodes.SOURCE_ARTIFACT_MISSING,
            },
            resolve_retry_codes({"retryProfile": "step-create-account-recover"}),
        )

    def test_resolve_retry_codes_uses_login_init_recover_profile(self) -> None:
        self.assertEqual(
            {
                ErrorCodes.AUTHORIZE_CONTINUE_BLOCKED,
                ErrorCodes.AUTHORIZE_CONTINUE_RATE_LIMITED,
                ErrorCodes.AUTHORIZE_MISSING_LOGIN_SESSION,
                ErrorCodes.MAILBOX_UNAVAILABLE,
                ErrorCodes.OTP_TIMEOUT,
                ErrorCodes.FLOW_TIMEOUT_EXCEEDED,
                ErrorCodes.PROXY_CONNECT_FAILED,
                ErrorCodes.TRANSPORT_ERROR,
                ErrorCodes.SOURCE_ARTIFACT_MISSING,
            },
            resolve_retry_codes({"retryProfile": "step-login-init-recover"}),
        )

    def test_resolve_retry_codes_uses_oauth_recover_profile(self) -> None:
        self.assertEqual(
            {
                ErrorCodes.AUTHORIZE_CONTINUE_BLOCKED,
                ErrorCodes.AUTHORIZE_CONTINUE_RATE_LIMITED,
                ErrorCodes.AUTHORIZE_MISSING_LOGIN_SESSION,
                ErrorCodes.FLOW_TIMEOUT_EXCEEDED,
                ErrorCodes.PROXY_CONNECT_FAILED,
                ErrorCodes.TRANSPORT_ERROR,
                ErrorCodes.SOURCE_ARTIFACT_MISSING,
            },
            resolve_retry_codes({"retryProfile": "step-oauth-recover"}),
        )

    def test_build_error_details_classifies_phone_submitted_without_sms_code(self) -> None:
        details = build_error_details(
            step_type="obtain_codex_oauth",
            message=(
                "phone_verification_submitted_small_success: "
                "phoneVerificationFailureStage=wait_sms_code "
                "sms service GET /sms/sessions/sms_session_123/code failed: HTTP 502"
            ),
        )
        self.assertEqual(ErrorCodes.PHONE_VERIFICATION_SUBMITTED_SMALL_SUCCESS, details["code"])
        self.assertEqual("flow_error", details["category"])

    def test_build_error_details_preserves_phone_attempt_over_nested_proxy_error(self) -> None:
        details = build_error_details(
            step_type="obtain_codex_oauth",
            code=ErrorCodes.PROXY_CONNECT_FAILED,
            message=(
                "phone_verification_attempted_small_success: "
                "submit_phone_verification_number "
                "<urlopen error [SSL: UNEXPECTED_EOF_WHILE_READING] "
                "EOF occurred in violation of protocol>"
            ),
        )
        self.assertEqual(ErrorCodes.PHONE_VERIFICATION_ATTEMPTED_SMALL_SUCCESS, details["code"])
        self.assertEqual("flow_error", details["category"])

    def test_resolve_retry_codes_uses_proxy_refresh_profile(self) -> None:
        self.assertEqual(
            {
                ErrorCodes.FLOW_TIMEOUT_EXCEEDED,
                ErrorCodes.PROXY_CONNECT_FAILED,
                ErrorCodes.TRANSPORT_ERROR,
                ErrorCodes.SOURCE_ARTIFACT_MISSING,
            },
            resolve_retry_codes({"retryProfile": "step-proxy-refresh"}),
        )

    def test_build_error_details_classifies_shared_output_source_artifact_missing(self) -> None:
        details = build_error_details(
            step_type="initialize_platform_organization",
            message=(
                "[Errno 2] No such file or directory: "
                "'/shared/register-output/others/openai-oauth-claims/claimed.json'"
            ),
        )
        self.assertEqual(ErrorCodes.SOURCE_ARTIFACT_MISSING, details["code"])
        self.assertEqual("flow_error", details["category"])

    def test_build_error_details_classifies_chat_requirements_unauthorized(self) -> None:
        details = build_error_details(
            step_type="initialize_chatgpt_login_session",
            message='chat_requirements_failed status=401 body={"detail":"Unauthorized"}',
        )
        self.assertEqual(ErrorCodes.AUTHORIZE_MISSING_LOGIN_SESSION, details["code"])

    def test_build_error_details_classifies_oauth_chat_requirements_403_as_blocked(self) -> None:
        details = build_error_details(
            step_type="obtain_codex_oauth",
            message="chat_requirements_failed status=403 body=<html><title>Just a moment...</title>",
        )
        self.assertEqual(ErrorCodes.AUTHORIZE_CONTINUE_BLOCKED, details["code"])

    def test_build_error_details_classifies_chatgpt_login_authorize_init_blocked(self) -> None:
        details = build_error_details(
            step_type="initialize_chatgpt_login_session",
            message='chatgpt_login_authorize_init_failed status=403 body=<!DOCTYPE html><title>Just a moment...</title>',
        )
        self.assertEqual(ErrorCodes.AUTHORIZE_CONTINUE_BLOCKED, details["code"])

    def test_build_error_details_classifies_chatgpt_login_wrong_email_otp_code(self) -> None:
        details = build_error_details(
            step_type="initialize_chatgpt_login_session",
            message='chatgpt_login_otp_validate_failed status=401 body={"error":{"code":"wrong_email_otp_code"}}',
        )
        self.assertEqual(ErrorCodes.OTP_TIMEOUT, details["code"])

    def test_build_error_details_classifies_chatgpt_login_mailbox_poll_resource_busy(self) -> None:
        details = build_error_details(
            step_type="initialize_chatgpt_login_session",
            message=(
                "chatgpt_login_email_otp_wait_failed:mail service GET "
                "/mail/mailboxes/mailbox_123/code failed: HTTP 500: "
                "{\"error\":\"No available MoEmail credentials for poll.\"}"
            ),
        )
        self.assertEqual(ErrorCodes.MAILBOX_UNAVAILABLE, details["code"])
        self.assertEqual("flow_error", details["category"])

    def test_build_error_details_classifies_platform_login_blocked(self) -> None:
        details = build_error_details(
            step_type="create_openai_account",
            message='platform_login status=403 body=<!DOCTYPE html><title>Just a moment...</title>',
        )
        self.assertEqual(ErrorCodes.AUTHORIZE_CONTINUE_BLOCKED, details["code"])

    def test_build_error_details_classifies_unexpected_eof_as_proxy_connect_failed(self) -> None:
        details = build_error_details(
            step_type="create_openai_account",
            message="<urlopen error [SSL: UNEXPECTED_EOF_WHILE_READING] EOF occurred in violation of protocol (_ssl.c:1017)>",
        )
        self.assertEqual(ErrorCodes.PROXY_CONNECT_FAILED, details["code"])
        self.assertEqual("proxy_error", details["category"])

    def test_build_error_details_classifies_oauth_timeouts_as_retryable_flow_timeout(self) -> None:
        for message in (
            "timed out",
            "<urlopen error _ssl.c:1000: The handshake operation timed out>",
            "Failed to perform, curl: (28) Operation timed out after 30001 milliseconds with 0 bytes received.",
        ):
            with self.subTest(message=message):
                details = build_error_details(
                    step_type="obtain_codex_oauth",
                    message=message,
                )
                self.assertEqual(ErrorCodes.FLOW_TIMEOUT_EXCEEDED, details["code"])

    def test_build_error_details_refines_invalid_request_unsupported_email(self) -> None:
        details = build_error_details(
            step_type="create_openai_account",
            code=ErrorCodes.INVALID_REQUEST_ERROR,
            message=(
                "create_account status=400 body={\"error\":{\"code\":\"unsupported_email\","
                "\"message\":\"The email you provided is not supported.\"}} "
                "[mailbox_provider=etempmail email=user@example.test]"
            ),
        )
        self.assertEqual(ErrorCodes.UNSUPPORTED_EMAIL, details["code"])

    def test_build_error_details_refines_invalid_username_with_mailbox_attribution(self) -> None:
        details = build_error_details(
            step_type="create_openai_account",
            code=ErrorCodes.INVALID_REQUEST_ERROR,
            message=(
                "authorize_continue status=400 body={\"error\":{\"code\":\"invalid_username\","
                "\"message\":\"Invalid username\"}} "
                "[mailbox_provider=etempmail email=user@example.test]"
            ),
        )
        self.assertEqual(ErrorCodes.UNSUPPORTED_EMAIL, details["code"])

    def test_build_error_details_keeps_attributed_registration_disallowed_retryable(self) -> None:
        details = build_error_details(
            step_type="create_openai_account",
            code=ErrorCodes.INVALID_REQUEST_ERROR,
            message=(
                "create_account status=400 body={\"error\":{\"code\":\"registration_disallowed\","
                "\"message\":\"Sorry, we cannot create your account with the given information.\"}} "
                "[mailbox_provider=mailtm email=user@example.test]"
            ),
        )
        self.assertEqual(ErrorCodes.INVALID_REQUEST_ERROR, details["code"])

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

    def test_protocol_runtime_error_classifies_mailbox_open_fetch_failed(self) -> None:
        exc = ensure_protocol_runtime_error(
            RuntimeError(
                "mail service POST /mail/mailboxes/open failed: "
                'HTTP 500 [code=fetch failed]: {"error":"fetch failed"}'
            ),
            stage="stage_other",
            detail="create_mailbox",
            category="flow_error",
        )
        self.assertEqual(ErrorCodes.MAILBOX_UNAVAILABLE, exc.code)
        self.assertEqual("flow_error", exc.category)

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

    def test_result_error_helpers_detect_deactivated_workspace(self) -> None:
        payload = {
            "errorStep": "invite-codex-member",
            "error": "{'detail': {'code': 'deactivated_workspace'}, 'status_code': 402}",
            "stepErrors": {
                "invite-codex-member": {
                    "code": "invite_codex_member_failed",
                    "message": "{'detail': {'code': 'deactivated_workspace'}, 'status_code': 402}",
                }
            },
        }
        self.assertEqual(
            ErrorCodes.TEAM_WORKSPACE_DEACTIVATED,
            result_error_code(payload, "invite-codex-member"),
        )
        self.assertTrue(
            result_error_matches(payload, ErrorCodes.TEAM_WORKSPACE_DEACTIVATED, step_id="invite-codex-member")
        )

    def test_result_error_helpers_detect_account_deactivated_as_workspace_deactivated(self) -> None:
        payload = {
            "errorStep": "obtain-codex-oauth",
            "error": "account_deactivated",
            "stepErrors": {
                "obtain-codex-oauth": {
                    "code": "obtain_codex_oauth_failed",
                    "message": "account_deactivated",
                }
            },
        }
        self.assertEqual(
            ErrorCodes.TEAM_WORKSPACE_DEACTIVATED,
            result_error_code(payload, "obtain-codex-oauth"),
        )
        self.assertTrue(
            result_error_matches(payload, ErrorCodes.TEAM_WORKSPACE_DEACTIVATED, step_id="obtain-codex-oauth")
        )

    def test_build_error_details_refines_generic_oauth_failed_to_account_deactivated(self) -> None:
        details = build_error_details(
            step_type="obtain_codex_oauth",
            code="obtain_codex_oauth_failed",
            message="account_deactivated",
        )
        self.assertEqual(ErrorCodes.TEAM_WORKSPACE_DEACTIVATED, details["code"])

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

    def test_dst_flow_oauth_retry_stops_only_after_phone_side_effect(self) -> None:
        statement = dst_flow.DstStatement(
            step_id="obtain-codex-oauth",
            step_type="obtain_codex_oauth",
            metadata={
                "retry": {
                    "maxAttempts": 3,
                    "retryProfile": "step-oauth-recover",
                }
            },
        )
        self.assertTrue(
            dst_flow._should_retry_step(
                statement=statement,
                error_details={"code": ErrorCodes.PROXY_CONNECT_FAILED},
                attempt_index=1,
            )
        )
        for error_code in (
            ErrorCodes.PHONE_VERIFICATION_ATTEMPTED_SMALL_SUCCESS,
            ErrorCodes.PHONE_VERIFICATION_SUBMITTED_SMALL_SUCCESS,
        ):
            with self.subTest(error_code=error_code):
                self.assertFalse(
                    dst_flow._should_retry_step(
                        statement=statement,
                        error_details={"code": error_code},
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

    def test_load_dst_flow_requires_explicit_owner_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            flow_path = Path(tmp_dir) / "missing-owner.json"
            flow_path.write_text(
                (
                    '{"definition":{"steps":['
                    '{"id":"acquire-mailbox","type":"acquire_mailbox","metadata":{"stage":"mailbox-acquire"}}'
                    ']}}'
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "missing metadata.owner"):
                dst_flow.load_dst_flow(flow_path)

    def test_load_dst_flow_accepts_utf8_bom(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            flow_path = Path(tmp_dir) / "bom-flow.json"
            flow_path.write_text(
                '\ufeff{"definition":{"steps":[{"id":"acquire-mailbox","type":"acquire_mailbox","metadata":{"owner":"easyemail"}}]}}',
                encoding="utf-8",
            )
            plan = dst_flow.load_dst_flow(flow_path)
        self.assertEqual("acquire-mailbox", plan.steps[0].step_id)


if __name__ == "__main__":
    unittest.main()
