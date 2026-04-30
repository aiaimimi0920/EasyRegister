from __future__ import annotations

from typing import Any


class ErrorCodes:
    AUTHORIZE_CONTINUE_BLOCKED = "authorize_continue_blocked"
    AUTHORIZE_CONTINUE_RATE_LIMITED = "authorize_continue_rate_limited"
    AUTHORIZE_MISSING_LOGIN_SESSION = "authorize_missing_login_session"
    EXISTING_ACCOUNT_DETECTED = "existing_account_detected"
    FLOW_TIMEOUT_EXCEEDED = "flow_timeout_exceeded"
    FREE_PERSONAL_WORKSPACE_MISSING = "free_personal_workspace_missing"
    INVALID_REQUEST_ERROR = "invalid_request_error"
    MAILBOX_UNAVAILABLE = "mailbox_unavailable"
    OTP_TIMEOUT = "otp_timeout"
    PASSWORD_VERIFY_BLOCKED = "password_verify_blocked"
    PROXY_CONNECT_FAILED = "proxy_connect_failed"
    REFRESH_TOKEN_REUSED = "refresh_token_reused"
    SMALL_SUCCESS_POOL_EMPTY = "small_success_pool_empty"
    TEAM_AUTH_TOKEN_INVALIDATED = "team_auth_token_invalidated"
    TEAM_INVITE_UPSTREAM_ERROR = "team_invite_upstream_error"
    TEAM_MOTHER_TOKEN_VALIDATION_FAILED = "team_mother_token_validation_failed"
    TEAM_SEATS_FULL = "team_seats_full"
    TRANSPORT_ERROR = "transport_error"
    UPLOAD_FILE_TO_R2_FAILED = "upload_file_to_r2_failed"
    USER_REGISTER_400 = "user_register_400"


CODE_CATEGORY_MAP: dict[str, str] = {
    ErrorCodes.AUTHORIZE_CONTINUE_BLOCKED: "blocked",
    ErrorCodes.AUTHORIZE_CONTINUE_RATE_LIMITED: "blocked",
    ErrorCodes.AUTHORIZE_MISSING_LOGIN_SESSION: "auth_error",
    ErrorCodes.EXISTING_ACCOUNT_DETECTED: "flow_error",
    ErrorCodes.FLOW_TIMEOUT_EXCEEDED: "flow_error",
    ErrorCodes.FREE_PERSONAL_WORKSPACE_MISSING: "flow_error",
    ErrorCodes.INVALID_REQUEST_ERROR: "auth_error",
    ErrorCodes.MAILBOX_UNAVAILABLE: "flow_error",
    ErrorCodes.OTP_TIMEOUT: "otp_timeout",
    ErrorCodes.PASSWORD_VERIFY_BLOCKED: "blocked",
    ErrorCodes.PROXY_CONNECT_FAILED: "proxy_error",
    ErrorCodes.REFRESH_TOKEN_REUSED: "auth_error",
    ErrorCodes.SMALL_SUCCESS_POOL_EMPTY: "flow_error",
    ErrorCodes.TEAM_AUTH_TOKEN_INVALIDATED: "auth_error",
    ErrorCodes.TEAM_INVITE_UPSTREAM_ERROR: "flow_error",
    ErrorCodes.TEAM_MOTHER_TOKEN_VALIDATION_FAILED: "auth_error",
    ErrorCodes.TEAM_SEATS_FULL: "flow_error",
    ErrorCodes.TRANSPORT_ERROR: "proxy_error",
    ErrorCodes.UPLOAD_FILE_TO_R2_FAILED: "flow_error",
    ErrorCodes.USER_REGISTER_400: "blocked",
}


RETRY_PROFILES: dict[str, tuple[str, ...]] = {
    "task-openai-default": (
        ErrorCodes.USER_REGISTER_400,
        ErrorCodes.AUTHORIZE_CONTINUE_BLOCKED,
        ErrorCodes.AUTHORIZE_MISSING_LOGIN_SESSION,
        ErrorCodes.PASSWORD_VERIFY_BLOCKED,
        ErrorCodes.EXISTING_ACCOUNT_DETECTED,
        ErrorCodes.PROXY_CONNECT_FAILED,
        ErrorCodes.MAILBOX_UNAVAILABLE,
        ErrorCodes.FLOW_TIMEOUT_EXCEEDED,
        ErrorCodes.TRANSPORT_ERROR,
    ),
    "task-continue-default": (
        ErrorCodes.PROXY_CONNECT_FAILED,
        ErrorCodes.MAILBOX_UNAVAILABLE,
        ErrorCodes.PASSWORD_VERIFY_BLOCKED,
        ErrorCodes.TRANSPORT_ERROR,
        ErrorCodes.FLOW_TIMEOUT_EXCEEDED,
    ),
    "task-team-expand-default": (
        ErrorCodes.PROXY_CONNECT_FAILED,
        ErrorCodes.AUTHORIZE_MISSING_LOGIN_SESSION,
        ErrorCodes.FLOW_TIMEOUT_EXCEEDED,
        ErrorCodes.TRANSPORT_ERROR,
    ),
    "step-team-auth-refresh": (
        ErrorCodes.TEAM_AUTH_TOKEN_INVALIDATED,
    ),
    "step-invite-recover": (
        ErrorCodes.TEAM_AUTH_TOKEN_INVALIDATED,
        ErrorCodes.PROXY_CONNECT_FAILED,
        ErrorCodes.TRANSPORT_ERROR,
        ErrorCodes.TEAM_INVITE_UPSTREAM_ERROR,
    ),
    "step-create-account-recover": (
        ErrorCodes.USER_REGISTER_400,
        ErrorCodes.AUTHORIZE_CONTINUE_BLOCKED,
        ErrorCodes.AUTHORIZE_CONTINUE_RATE_LIMITED,
        ErrorCodes.AUTHORIZE_MISSING_LOGIN_SESSION,
        ErrorCodes.PROXY_CONNECT_FAILED,
        ErrorCodes.TRANSPORT_ERROR,
    ),
    "step-login-init-recover": (
        ErrorCodes.AUTHORIZE_CONTINUE_BLOCKED,
        ErrorCodes.AUTHORIZE_CONTINUE_RATE_LIMITED,
        ErrorCodes.AUTHORIZE_MISSING_LOGIN_SESSION,
        ErrorCodes.OTP_TIMEOUT,
        ErrorCodes.PROXY_CONNECT_FAILED,
        ErrorCodes.TRANSPORT_ERROR,
    ),
    "step-proxy-refresh": (
        ErrorCodes.PROXY_CONNECT_FAILED,
        ErrorCodes.TRANSPORT_ERROR,
    ),
    "step-upload-artifact": (
        ErrorCodes.TRANSPORT_ERROR,
        ErrorCodes.UPLOAD_FILE_TO_R2_FAILED,
    ),
}


def normalize_error_code(value: Any) -> str:
    return str(value or "").strip().lower()


def normalize_error_category(value: Any) -> str:
    return str(value or "").strip().lower()


def infer_category_from_message(message: str) -> str:
    msg = str(message or "").strip().lower()
    if (
        "registration_disallowed" in msg
        or "terms of use restriction on about-you page" in msg
        or "cannot create your account with the given information" in msg
    ):
        return "blocked"
    if (
        "phone_wall" in msg
        or "phone number required" in msg
        or "add-phone" in msg
        or "cloudflare" in msg
        or "cf-mitigated=challenge" in msg
        or "just a moment" in msg
        or "captcha" in msg
        or "turnstile" in msg
        or "403" in msg
        or "429" in msg
        or "blocked" in msg
    ):
        return "blocked"
    if ErrorCodes.OTP_TIMEOUT in msg or ("otp" in msg and "timeout" in msg):
        return "otp_timeout"
    if (
        "proxy" in msg
        or "network" in msg
        or "connection" in msg
        or "connect tunnel failed" in msg
        or "response 407" in msg
        or "fetch failed" in msg
        or "econnrefused" in msg
    ):
        return "proxy_error"
    if (
        "token" in msg
        or "callback" in msg
        or "invalid_state" in msg
        or "invalid client. please start over" in msg
        or "authorize_init_missing_did" in msg
        or "invalid_username_or_password" in msg
        or "missing_auth_cookie" in msg
        or "oauth error" in msg
        or "codex_session_handoff" in msg
    ):
        return "auth_error"
    return "flow_error"


def infer_category_from_code(code: str) -> str:
    return CODE_CATEGORY_MAP.get(normalize_error_code(code), "")


def resolve_retry_codes(policy: dict[str, Any] | None) -> set[str]:
    if not isinstance(policy, dict):
        return set()
    profile_name = str(policy.get("retryProfile") or "").strip()
    if profile_name:
        return {normalize_error_code(item) for item in RETRY_PROFILES.get(profile_name, ()) if normalize_error_code(item)}
    retry_codes = policy.get("retryOnCodes")
    if isinstance(retry_codes, list):
        return {normalize_error_code(item) for item in retry_codes if normalize_error_code(item)}
    return set()


def classify_error_code(
    *,
    step_type: str,
    message: str,
    detail: str = "",
    code: str = "",
) -> str:
    normalized_code = normalize_error_code(code)
    if normalized_code:
        return normalized_code

    normalized_step_type = str(step_type or "").strip().lower()
    normalized_detail = str(detail or "").strip().lower()
    lowered = str(message or "").strip().lower()
    combined = " ".join(part for part in (normalized_detail, lowered) if part)

    if ErrorCodes.FREE_PERSONAL_WORKSPACE_MISSING in combined:
        return ErrorCodes.FREE_PERSONAL_WORKSPACE_MISSING
    if (
        normalized_step_type == "invite_codex_member"
        and (
            "token_invalidated" in lowered
            or "authentication token has been invalidated" in lowered
            or ("status_code': 401" in lowered and "please try signing in again" in lowered)
            or ('"status_code": 401' in lowered and "please try signing in again" in lowered)
        )
    ):
        return ErrorCodes.TEAM_AUTH_TOKEN_INVALIDATED
    if (
        "workspace has reached maximum number of seats" in lowered
        or ErrorCodes.TEAM_SEATS_FULL in lowered
    ):
        return ErrorCodes.TEAM_SEATS_FULL
    if normalized_detail == "user_register" or "user_register" in lowered:
        return ErrorCodes.USER_REGISTER_400
    if "chat_requirements_failed" in lowered and ("status=401" in lowered or '"detail":"unauthorized"' in lowered):
        return ErrorCodes.AUTHORIZE_MISSING_LOGIN_SESSION
    if "chatgpt_login_authorize_init_failed" in lowered and ("just a moment" in lowered or "status=403" in lowered):
        return ErrorCodes.AUTHORIZE_CONTINUE_BLOCKED
    if "chatgpt_login_otp_validate_failed" in lowered and "wrong_email_otp_code" in lowered:
        return ErrorCodes.OTP_TIMEOUT
    if "authorize_continue" in lowered and ("status=429" in lowered or "rate limit exceeded" in lowered):
        return ErrorCodes.AUTHORIZE_CONTINUE_RATE_LIMITED
    if normalized_detail == "authorize_continue" or (
        "authorize_continue" in lowered and ("just a moment" in lowered or "status=403" in lowered)
    ):
        return ErrorCodes.AUTHORIZE_CONTINUE_BLOCKED
    if normalized_detail == "password_verify" or (
        "password_verify" in lowered and ("just a moment" in lowered or "status=403" in lowered)
    ):
        return ErrorCodes.PASSWORD_VERIFY_BLOCKED
    if ErrorCodes.EXISTING_ACCOUNT_DETECTED in lowered or normalized_detail == "authorize_continue_existing_account":
        return ErrorCodes.EXISTING_ACCOUNT_DETECTED
    if "authorize_init_missing_login_session" in lowered or normalized_detail == "oauth_authorize":
        return ErrorCodes.AUTHORIZE_MISSING_LOGIN_SESSION
    if (
        ErrorCodes.PROXY_CONNECT_FAILED in lowered
        or "easy_proxy_checkout_failed" in lowered
        or "recent_route_reuse" in lowered
        or "proxy connect aborted" in lowered
        or "could not connect to server" in lowered
        or "tls connect error" in lowered
        or "connection closed abruptly" in lowered
    ):
        return ErrorCodes.PROXY_CONNECT_FAILED
    if (
        "code=mailbox_capacity_unavailable" in lowered
        or '"code":"mailbox_capacity_unavailable"' in lowered
        or "mailbox_capacity_unavailable" in lowered
        or "code=mailbox_upstream_transient" in lowered
        or '"code":"mailbox_upstream_transient"' in lowered
        or "mailbox capacity unavailable" in lowered
        or "mailbox upstream transient" in lowered
        or "code=moemail_capacity_exhausted" in lowered
        or '"code":"moemail_capacity_exhausted"' in lowered
        or "moemail_capacity_exhausted" in lowered
        or "moemail upstream transient" in lowered
        or "maximum mailbox" in lowered
        or "mailbox count limit" in lowered
        or "最大邮箱数量限制" in str(message or "")
    ):
        return ErrorCodes.MAILBOX_UNAVAILABLE
    if ErrorCodes.REFRESH_TOKEN_REUSED in lowered:
        return ErrorCodes.REFRESH_TOKEN_REUSED
    if ErrorCodes.TEAM_MOTHER_TOKEN_VALIDATION_FAILED in lowered:
        return ErrorCodes.TEAM_MOTHER_TOKEN_VALIDATION_FAILED
    if ErrorCodes.INVALID_REQUEST_ERROR in lowered:
        return ErrorCodes.INVALID_REQUEST_ERROR
    if "unable to invite user due to an error" in lowered:
        return ErrorCodes.TEAM_INVITE_UPSTREAM_ERROR
    if ErrorCodes.OTP_TIMEOUT in lowered or "timeout waiting for 6-digit code" in lowered:
        return ErrorCodes.OTP_TIMEOUT
    if "r2_upload_failed" in lowered or ErrorCodes.UPLOAD_FILE_TO_R2_FAILED in lowered:
        return ErrorCodes.UPLOAD_FILE_TO_R2_FAILED
    if ErrorCodes.SMALL_SUCCESS_POOL_EMPTY in lowered:
        return ErrorCodes.SMALL_SUCCESS_POOL_EMPTY
    if ErrorCodes.FLOW_TIMEOUT_EXCEEDED in lowered:
        return ErrorCodes.FLOW_TIMEOUT_EXCEEDED
    if "curl" in lowered or "connect" in lowered or "tls" in lowered:
        return ErrorCodes.TRANSPORT_ERROR
    return normalize_error_code(f"{normalized_step_type}_failed")
