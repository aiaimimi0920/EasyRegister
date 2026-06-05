from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from easyemail_flow import dispatch_easyemail_step
from errors import ErrorCodes, result_error_matches, result_error_message
from others.bootstrap import ensure_local_bundle_imports
from others.common import ensure_directory
from others.common_runtime import validate_openai_oauth_seed_payload
from others.config import CleanupRuntimeConfig, MailboxRuntimeConfig, env_int
from others.file_lock import release_lock, try_acquire_lock
from others.result_artifacts import FREE_OPENAI_OAUTH_SOURCE_CANDIDATES, all_output_texts, output_dict

ensure_local_bundle_imports()

from shared_mailbox.easy_email_client import report_mailbox_outcome


MAILBOX_DOMAIN_STATS_SCHEMA_VERSION = 3
EMAIL_OTP_FAILURE_REASONS = {"email_otp_timeout", "email_otp_wrong_code"}
MAILBOX_OUTCOME_REPORT_REASONS = {
    "create_account_user_register_400",
    "email_otp_timeout",
    "unsupported_email",
}


def _cleanup_runtime_config() -> CleanupRuntimeConfig:
    return CleanupRuntimeConfig.from_env()


def _mailbox_runtime_config(*, shared_root: Path) -> MailboxRuntimeConfig:
    default_state_path = shared_root / "others" / "register-mailbox-domain-state.json"
    return MailboxRuntimeConfig.from_env(
        default_ttl_seconds=90,
        default_state_path=default_state_path,
        default_business_domain_pool=(
            "sall.cc",
            "cnmlgb.de",
            "zhooo.org",
            "cksa.eu.cc",
            "wqwq.eu.cc",
            "zhoo.eu.cc",
            "zhooo.ggff.net",
            "coolkidsa.ggff.net",
        ),
        default_blacklist_min_attempts=20,
        default_blacklist_failure_rate=90.0,
        default_consecutive_failure_blacklist_threshold=500,
    )


def mailbox_cleanup_state_path(*, shared_root: Path) -> Path:
    return shared_root / "others" / "mailbox-cleanup-state.json"


def mailbox_cleanup_lock_path(*, shared_root: Path) -> Path:
    return shared_root / "others" / "mailbox-cleanup.lock"


def mailbox_domain_stats_path(*, shared_root: Path) -> Path:
    return _mailbox_runtime_config(shared_root=shared_root).domain_state_path


def load_mailbox_cleanup_state(*, shared_root: Path) -> dict[str, Any]:
    path = mailbox_cleanup_state_path(shared_root=shared_root)
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def load_mailbox_domain_stats_state(*, shared_root: Path) -> dict[str, Any]:
    path = mailbox_domain_stats_path(shared_root=shared_root)
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    try:
        schema_version = int(payload.get("schemaVersion") or 0)
    except Exception:
        schema_version = 0
    if schema_version != MAILBOX_DOMAIN_STATS_SCHEMA_VERSION:
        return {}
    return payload


def write_mailbox_cleanup_state(*, shared_root: Path, payload: dict[str, Any]) -> None:
    path = mailbox_cleanup_state_path(shared_root=shared_root)
    ensure_directory(path.parent)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_mailbox_domain_stats_state(*, shared_root: Path, payload: dict[str, Any]) -> None:
    path = mailbox_domain_stats_path(shared_root=shared_root)
    ensure_directory(path.parent)
    payload["schemaVersion"] = MAILBOX_DOMAIN_STATS_SCHEMA_VERSION
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def try_acquire_mailbox_cleanup_lock(*, shared_root: Path) -> bool:
    lock_path = mailbox_cleanup_lock_path(shared_root=shared_root)
    return try_acquire_lock(
        lock_path,
        stale_after_seconds=_cleanup_runtime_config().mailbox_cleanup_lock_stale_seconds,
    )


def release_mailbox_cleanup_lock(*, shared_root: Path) -> None:
    release_lock(mailbox_cleanup_lock_path(shared_root=shared_root))


def mailbox_cleanup_recently_ran(*, shared_root: Path, cooldown_seconds: float) -> bool:
    if float(cooldown_seconds or 0.0) <= 0:
        return False
    payload = load_mailbox_cleanup_state(shared_root=shared_root)
    timestamp_text = str(payload.get("lastFinishedAt") or payload.get("lastStartedAt") or "").strip()
    if not timestamp_text:
        return False
    try:
        timestamp = datetime.fromisoformat(timestamp_text.replace("Z", "+00:00"))
    except ValueError:
        return False
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - timestamp).total_seconds() < float(cooldown_seconds)


def infer_mailbox_capacity_provider_type_key(*, detail: str) -> str:
    lowered = str(detail or "").strip().lower()
    if not lowered:
        return ""
    if "moemail" in lowered:
        return "moemail"
    return ""


def trigger_mailbox_capacity_recovery(*, shared_root: Path, detail: str) -> dict[str, Any]:
    cleanup_config = _cleanup_runtime_config()
    cooldown_seconds = cleanup_config.mailbox_cleanup_cooldown_seconds
    if mailbox_cleanup_recently_ran(shared_root=shared_root, cooldown_seconds=cooldown_seconds):
        payload = load_mailbox_cleanup_state(shared_root=shared_root)
        return {
            "ok": False,
            "status": "recovery_recently_ran",
            "lastResult": payload.get("lastResult") if isinstance(payload.get("lastResult"), dict) else {},
        }
    if not try_acquire_mailbox_cleanup_lock(shared_root=shared_root):
        return {"ok": False, "status": "recovery_locked"}
    started_at = datetime.now(timezone.utc).isoformat()
    state_payload = {
        "lastStartedAt": started_at,
        "lastFinishedAt": "",
        "triggerDetail": str(detail or "").strip(),
        "lastResult": {},
        "consecutiveFailures": 0,
    }
    write_mailbox_cleanup_state(shared_root=shared_root, payload=state_payload)
    try:
        provider_type_key = infer_mailbox_capacity_provider_type_key(detail=detail)
        try:
            result = dispatch_easyemail_step(
                step_type="recover_mailbox_capacity",
                step_input={
                    "failure_code": ErrorCodes.MAILBOX_UNAVAILABLE,
                    "detail": str(detail or "").strip(),
                    "provider_type_key": provider_type_key,
                    "force": True,
                    "stale_after_seconds": 0,
                    "max_delete_count": cleanup_config.mailbox_cleanup_max_delete_count,
                },
            )
            ok = bool(result.get("ok")) if isinstance(result, dict) else False
            status = str(result.get("status") or "recovery_finished") if isinstance(result, dict) else "recovery_finished"
        except Exception as exc:
            result = {"detail": str(exc)}
            ok = False
            status = "recovery_failed"
        finished_at = datetime.now(timezone.utc).isoformat()
        state_payload.update(
            {
                "lastFinishedAt": finished_at,
                "lastResult": {
                    "ok": ok,
                    "status": status,
                    "result": result,
                },
                "consecutiveFailures": 0,
            }
        )
        write_mailbox_cleanup_state(shared_root=shared_root, payload=state_payload)
        return {
            "ok": ok,
            "status": status,
            "result": result,
        }
    finally:
        release_mailbox_cleanup_lock(shared_root=shared_root)


def mailbox_capacity_failure_detail(*, result_payload_value: dict[str, Any]) -> str:
    if str(result_payload_value.get("errorStep") or "").strip().lower() != "acquire-mailbox":
        return ""
    if result_error_matches(result_payload_value, ErrorCodes.MAILBOX_UNAVAILABLE, step_id="acquire-mailbox"):
        return result_error_message(result_payload_value, "acquire-mailbox")
    return ""


def mailbox_domain_blacklist_min_attempts(*, shared_root: Path) -> int:
    return _mailbox_runtime_config(shared_root=shared_root).blacklist_min_attempts


def mailbox_domain_blacklist_failure_rate(*, shared_root: Path) -> float:
    return _mailbox_runtime_config(shared_root=shared_root).blacklist_failure_rate_percent


def mailbox_domain_consecutive_failure_blacklist_threshold(*, shared_root: Path) -> int:
    return _mailbox_runtime_config(shared_root=shared_root).consecutive_failure_blacklist_threshold


def mailbox_email_otp_failure_blacklist_threshold() -> int:
    return max(0, env_int("REGISTER_MAILBOX_EMAIL_OTP_FAILURE_BLACKLIST_THRESHOLD", 3))


def mailbox_email_otp_provider_failure_blacklist_threshold() -> int:
    return max(0, env_int("REGISTER_MAILBOX_EMAIL_OTP_PROVIDER_FAILURE_BLACKLIST_THRESHOLD", 3))


def mailbox_failure_reason_total(failure_reasons: Any, reasons: set[str]) -> int:
    if not isinstance(failure_reasons, dict):
        return 0
    total = 0
    for reason in reasons:
        try:
            total += max(0, int(failure_reasons.get(reason) or 0))
        except Exception:
            continue
    return total


def mailbox_domain_blacklist_reason(*, result_payload_value: dict[str, Any]) -> str:
    step_errors = result_payload_value.get("stepErrors") if isinstance(result_payload_value, dict) else {}
    if not isinstance(step_errors, dict):
        return ""
    create_error = step_errors.get("create-openai-account")
    create_error = create_error if isinstance(create_error, dict) else {}
    message = str(create_error.get("message") or result_payload_value.get("error") or "").strip().lower()
    if "unsupported_email" in message or "the email you provided is not supported" in message:
        return "unsupported_email"
    if "registration_disallowed" in message and "mailbox_provider=" in message:
        return "registration_disallowed"
    return ""


def _mailbox_result_error_text(*, result_payload_value: dict[str, Any]) -> str:
    if not isinstance(result_payload_value, dict):
        return ""
    parts: list[str] = [
        str(result_payload_value.get("errorStep") or "").strip(),
        str(result_payload_value.get("error") or "").strip(),
        str(result_payload_value.get("errorCode") or "").strip(),
    ]
    step_errors = result_payload_value.get("stepErrors")
    if isinstance(step_errors, dict):
        for raw_value in step_errors.values():
            if not isinstance(raw_value, dict):
                continue
            parts.extend(
                [
                    str(raw_value.get("code") or "").strip(),
                    str(raw_value.get("message") or "").strip(),
                    str(raw_value.get("detail") or "").strip(),
                ]
            )
    return " ".join(part for part in parts if part).strip().lower()


def mailbox_failure_ignore_reason(*, result_payload_value: dict[str, Any]) -> str:
    """Return a reason when a run failure should not degrade mailbox quality stats."""

    if bool(result_payload_value.get("ok")):
        return ""
    error_step = str(result_payload_value.get("errorStep") or "").strip().lower()
    combined = _mailbox_result_error_text(result_payload_value=result_payload_value)

    if (
        "sms_no_selection_plan_candidates" in combined
        or "sms_no_productive_selection_plan_candidates" in combined
    ):
        return "external_sms_no_selection"

    if error_step == "obtain-codex-oauth" and (
        (
            "sms service post /sms/sessions/open failed" in combined
            and any(
                marker in combined
                for marker in (
                    "no eligible public numbers",
                    "currently unavailable",
                    "synthetic activation session",
                )
            )
        )
        or ("no eligible public numbers" in combined and "synthetic activation session" in combined)
    ):
        return "external_sms_no_selection"

    if error_step == "obtain-codex-oauth" and "missing_workspace" in combined:
        return "external_oauth_workspace"

    if (
        "cannot create your account with the given information" in combined
        or "registration_disallowed" in combined
        or "terms of use restriction on about-you page" in combined
    ):
        if "mailbox_provider=" in combined:
            return ""
        return "external_registration_blocked"

    if error_step == "obtain-codex-oauth" and any(
        marker in combined
        for marker in (
            "phone_wall",
            "phone_verification",
            "phone number",
            "phone_number",
            "sms_",
            "unsupported_phone",
            "wrong_otp_code",
            "otp_incorrect",
        )
    ):
        return "external_phone_verification"

    if error_step in {"acquire-proxy-chain", "release-proxy-chain"}:
        return "external_proxy_or_auth"

    if error_step in {
        "create-openai-account",
        "initialize-chatgpt-login-session",
        "initialize-platform-organization",
        "obtain-codex-oauth",
        "validate-free-personal-oauth",
    }:
        if (
            "no execution worker became available before acquire timeout" in combined
            or (
                "service_unavailable" in combined
                and "codex.semantic.step" in combined
                and "acquire timeout" in combined
            )
        ):
            return "external_protocol_capacity"
        if result_error_matches(
            result_payload_value,
            ErrorCodes.AUTHORIZE_CONTINUE_BLOCKED,
            ErrorCodes.AUTHORIZE_CONTINUE_RATE_LIMITED,
            ErrorCodes.AUTHORIZE_MISSING_LOGIN_SESSION,
            ErrorCodes.FLOW_TIMEOUT_EXCEEDED,
            ErrorCodes.PROXY_CONNECT_FAILED,
            ErrorCodes.TRANSPORT_ERROR,
            step_id=error_step,
        ):
            return "external_proxy_or_auth"
        if any(
            marker in combined
            for marker in (
                "cf_mitigated=challenge",
                "cf-mitigated=challenge",
                "just a moment",
                "status=403",
                "status=429",
                "rate_limit_exceeded",
                "rate limit exceeded",
                "unexpected_eof_while_reading",
                "eof occurred in violation of protocol",
                "easy_proxy_checkout_failed",
                "handshake operation timed out",
                "operation timed out",
                "timed out",
                "proxy connect",
            )
        ) and not any(
            marker in combined
            for marker in (
                "timeout waiting for 6-digit code",
                "chatgpt_login_email_otp_wait_failed",
                "otp_timeout",
            )
        ):
            return "external_proxy_or_auth"

    return ""


def mailbox_failure_reason(*, result_payload_value: dict[str, Any]) -> str:
    if bool(result_payload_value.get("ok")):
        return ""
    error_step = str(result_payload_value.get("errorStep") or "").strip().lower()
    combined = _mailbox_result_error_text(result_payload_value=result_payload_value)

    explicit_reason = str(result_payload_value.get("mailboxFailureReason") or "").strip().lower()
    if explicit_reason:
        return explicit_reason
    if "unsupported_email" in combined or "the email you provided is not supported" in combined:
        return "unsupported_email"
    if "registration_disallowed" in combined and "mailbox_provider=" in combined:
        return "registration_disallowed"
    if error_step == "create-openai-account" and "user_register_400" in combined:
        return "create_account_user_register_400"
    if "wrong_email_otp_code" in combined or "chatgpt_login_otp_validate_failed" in combined:
        return "email_otp_wrong_code"
    if (
        "chatgpt_login_email_otp_wait_failed" in combined
        or "timeout waiting for 6-digit code" in combined
        or "otp_timeout" in combined
    ):
        return "email_otp_timeout"
    if error_step == "create-openai-account":
        return "create_account_failure"
    if error_step:
        return error_step.replace("-", "_")
    return "run_failure"


def mailbox_failure_rate_reaches_blacklist_threshold(
    *,
    attempts: int,
    failures: int,
    min_attempts: int,
    failure_rate_threshold: float,
) -> bool:
    normalized_attempts = max(0, int(attempts or 0))
    normalized_failures = max(0, int(failures or 0))
    normalized_min_attempts = max(1, int(min_attempts or 1))
    if normalized_attempts < normalized_min_attempts:
        return False
    failure_rate = (float(normalized_failures) / float(normalized_attempts)) * 100.0 if normalized_attempts else 0.0
    return failure_rate >= float(failure_rate_threshold or 0.0)


def mailbox_provider_from_ref(mailbox_ref: str) -> str:
    value = str(mailbox_ref or "").strip()
    if not value:
        return ""
    if ":" not in value:
        return "moemail"
    return str(value.split(":", 1)[0] or "").strip().lower()


def mailbox_session_id_from_ref(mailbox_ref: str) -> str:
    value = str(mailbox_ref or "").strip()
    if not value:
        return ""
    if ":" not in value:
        return value
    return value.split(":", 1)[1].strip()


def extract_mailbox_business_outcome_context(*, result_payload_value: dict[str, Any]) -> dict[str, str]:
    steps = result_payload_value.get("steps") if isinstance(result_payload_value, dict) else {}
    if isinstance(steps, dict) and str(steps.get("acquire-mailbox") or "").strip().lower() != "ok":
        return {}
    outputs = result_payload_value.get("outputs") if isinstance(result_payload_value, dict) else {}
    if not isinstance(outputs, dict):
        return {}
    mailbox_output = outputs.get("acquire-mailbox")
    mailbox_output = mailbox_output if isinstance(mailbox_output, dict) else {}
    create_output = outputs.get("create-openai-account")
    create_output = create_output if isinstance(create_output, dict) else {}
    email = str(
        mailbox_output.get("email")
        or mailbox_output.get("emailAddress")
        or create_output.get("email")
        or ""
    ).strip().lower()
    mailbox_ref = str(
        mailbox_output.get("mailbox_ref")
        or mailbox_output.get("mailboxRef")
        or ""
    ).strip()
    provider = str(
        mailbox_output.get("provider")
        or mailbox_output.get("providerTypeKey")
        or ""
    ).strip().lower()
    business_key = str(
        mailbox_output.get("business_key")
        or mailbox_output.get("businessKey")
        or ""
    ).strip().lower()
    if not provider and mailbox_ref:
        provider = mailbox_provider_from_ref(mailbox_ref)
    if "@" not in email:
        return {
            "business_key": business_key,
            "provider": provider,
            "mailbox_ref": mailbox_ref,
            "email": email,
            "domain": "",
        }
    return {
        "business_key": business_key,
        "provider": provider,
        "mailbox_ref": mailbox_ref,
        "email": email,
        "domain": email.rsplit("@", 1)[-1].strip().lower(),
    }


def _mailbox_outcome_report_policy(*, failure_reason: str) -> dict[str, Any]:
    normalized_reason = str(failure_reason or "").strip().lower()
    if normalized_reason == "unsupported_email":
        return {
            "attribution_strength": "strong",
            "global_blacklist": False,
        }
    if normalized_reason in {"create_account_user_register_400", "email_otp_timeout"}:
        return {
            "attribution_strength": "weak",
            "global_blacklist": False,
        }
    return {}


def _report_mailbox_failure_outcome_to_easyemail(
    *,
    context: dict[str, str],
    business_key: str,
    failure_reason: str,
) -> None:
    normalized_reason = str(failure_reason or "").strip().lower()
    if normalized_reason not in MAILBOX_OUTCOME_REPORT_REASONS:
        return
    session_id = mailbox_session_id_from_ref(str(context.get("mailbox_ref") or ""))
    if not session_id:
        return
    policy = _mailbox_outcome_report_policy(failure_reason=normalized_reason)
    if not policy:
        return
    try:
        report_mailbox_outcome(
            session_id=session_id,
            success=False,
            failure_reason=normalized_reason,
            business_flow=str(business_key or "").strip(),
            retry_layer="step",
            attribution_strength=str(policy.get("attribution_strength") or ""),
            attribution_kind="mailbox_domain_risk",
            provider_type_key=str(context.get("provider") or "").strip().lower(),
            domain=str(context.get("domain") or "").strip().lower(),
            email_address=str(context.get("email") or "").strip().lower(),
            avoid_in_current_attempt=True,
            global_blacklist=bool(policy.get("global_blacklist")),
            cooldown_seconds=0,
            source="easyregister",
        )
    except Exception:
        return


def _mailbox_artifact_matches_context(*, artifact_payload: dict[str, Any], context: dict[str, str]) -> bool:
    if not isinstance(artifact_payload, dict):
        return False
    context_email = str(context.get("email") or "").strip().lower()
    context_domain = str(context.get("domain") or "").strip().lower()
    context_ref = str(context.get("mailbox_ref") or "").strip()
    artifact_email = str(artifact_payload.get("email") or "").strip().lower()
    artifact_ref = str(artifact_payload.get("mailboxRef") or artifact_payload.get("mailbox_ref") or "").strip()
    artifact_session_id = str(
        artifact_payload.get("mailboxSessionId")
        or artifact_payload.get("mailbox_session_id")
        or ""
    ).strip()
    if context_email:
        if artifact_email != context_email:
            return False
    elif context_domain:
        if "@" not in artifact_email or artifact_email.rsplit("@", 1)[-1].strip().lower() != context_domain:
            return False
    else:
        return False
    if context_ref and artifact_ref and context_ref != artifact_ref:
        return False
    if context_ref and artifact_session_id and ":" in context_ref:
        context_session_id = context_ref.split(":", 1)[1].strip()
        if context_session_id and context_session_id != artifact_session_id:
            return False
    return True


def _mailbox_quality_success_from_existing_artifacts(
    *,
    result_payload_value: dict[str, Any],
    context: dict[str, str],
) -> str:
    for path_text in all_output_texts(result_payload_value, FREE_OPENAI_OAUTH_SOURCE_CANDIDATES):
        try:
            path = Path(path_text).resolve()
        except Exception:
            continue
        if not path.is_file():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        valid, _ = validate_openai_oauth_seed_payload(payload, enforce_max_age=False)
        if valid and _mailbox_artifact_matches_context(artifact_payload=payload, context=context):
            return "openai_oauth_artifact"
    return ""


def _output_status_is_completed(output: dict[str, Any]) -> bool:
    if not isinstance(output, dict):
        return False
    status = str(output.get("status") or "").strip().lower()
    if status:
        return status == "completed"
    return bool(output.get("ok"))


def _mailbox_quality_success_from_completed_outputs(
    *,
    result_payload_value: dict[str, Any],
    context: dict[str, str],
) -> str:
    create_output = output_dict(result_payload_value, "create-openai-account")
    if not create_output:
        create_output = output_dict(result_payload_value, "create_openai_account")
    platform_output = output_dict(result_payload_value, "initialize-platform-organization")
    login_output = output_dict(result_payload_value, "initialize-chatgpt-login-session")
    if not create_output or not platform_output or not login_output:
        return ""
    create_email = str(create_output.get("email") or context.get("email") or "").strip().lower()
    if create_email and context.get("email") and create_email != str(context.get("email") or "").strip().lower():
        return ""
    mailbox_ref = str(
        create_output.get("mailbox_ref")
        or create_output.get("mailboxRef")
        or login_output.get("mailboxRef")
        or context.get("mailbox_ref")
        or ""
    ).strip()
    mailbox_session_id = str(
        create_output.get("mailbox_session_id")
        or create_output.get("mailboxSessionId")
        or login_output.get("mailboxSessionId")
        or ""
    ).strip()
    if not mailbox_ref:
        return ""
    if not mailbox_session_id and ":" in mailbox_ref:
        mailbox_session_id = mailbox_ref.split(":", 1)[1].strip()
    if not mailbox_session_id:
        return ""
    if not _output_status_is_completed(platform_output) or not _output_status_is_completed(login_output):
        return ""
    steps = result_payload_value.get("steps")
    if isinstance(steps, dict):
        create_step_status = str(steps.get("create-openai-account") or steps.get("create_openai_account") or "").strip().lower()
        if create_step_status and create_step_status != "ok":
            return ""
    return "openai_oauth_output"


def mailbox_quality_success_reason(
    *,
    result_payload_value: dict[str, Any],
    context: dict[str, str],
) -> str:
    """Return a reason when a failed run still proves mailbox quality.

    A valid OpenAI OAuth seed / legacy small-success artifact means the mailbox
    already passed registration and email OTP. Later phone/SMS/OAuth failures
    must not poison mailbox-domain statistics.
    """

    if bool(result_payload_value.get("ok")):
        return ""
    if not context.get("domain"):
        return ""
    artifact_reason = _mailbox_quality_success_from_existing_artifacts(
        result_payload_value=result_payload_value,
        context=context,
    )
    if artifact_reason:
        return artifact_reason
    return _mailbox_quality_success_from_completed_outputs(
        result_payload_value=result_payload_value,
        context=context,
    )


def _mailbox_attempt_outcome_payload(attempt: dict[str, Any]) -> dict[str, Any]:
    email = str(attempt.get("email") or "").strip().lower()
    provider = str(attempt.get("provider") or "").strip().lower()
    mailbox_ref = str(attempt.get("mailbox_ref") or attempt.get("mailboxRef") or "").strip()
    if not provider and mailbox_ref:
        provider = mailbox_provider_from_ref(mailbox_ref)
    failure_reason = str(attempt.get("failureReason") or "").strip().lower()
    error_code = str(attempt.get("errorCode") or failure_reason or "").strip().lower()
    step_id = str(attempt.get("stepId") or "create-openai-account").strip() or "create-openai-account"
    message = " ".join(
        part
        for part in (
            error_code,
            failure_reason,
            str(attempt.get("failureClass") or "").strip(),
        )
        if part
    )
    return {
        "ok": False,
        "errorStep": step_id,
        "error": message,
        "mailboxFailureReason": failure_reason,
        "steps": {"acquire-mailbox": "ok", step_id: "failed"},
        "outputs": {
            "acquire-mailbox": {
                "email": email,
                "provider": provider,
                "mailbox_ref": mailbox_ref,
                "business_key": str(attempt.get("business_key") or attempt.get("businessKey") or "").strip().lower(),
            }
        },
        "stepErrors": {
            step_id: {
                "code": error_code,
                "message": message,
            }
        },
    }


def _record_mailbox_attempt_outcomes(
    *,
    shared_root: Path,
    result_payload_value: dict[str, Any],
    instance_role: str,
) -> list[dict[str, Any]]:
    outputs = result_payload_value.get("outputs") if isinstance(result_payload_value, dict) else {}
    if not isinstance(outputs, dict):
        return []
    raw_attempts = outputs.get("mailbox-attempt-outcomes")
    if not isinstance(raw_attempts, list):
        return []
    recorded: list[dict[str, Any]] = []
    for raw_attempt in raw_attempts:
        if not isinstance(raw_attempt, dict):
            continue
        if str(raw_attempt.get("outcome") or "").strip().lower() != "failure":
            continue
        attempt_payload = _mailbox_attempt_outcome_payload(raw_attempt)
        attempt_outcome = record_business_mailbox_domain_outcome(
            shared_root=shared_root,
            result_payload_value=attempt_payload,
            instance_role=instance_role,
            _record_attempt_outcomes=False,
        )
        if attempt_outcome:
            recorded.append(attempt_outcome)
    return recorded


def record_business_mailbox_domain_outcome(
    *,
    shared_root: Path,
    result_payload_value: dict[str, Any],
    instance_role: str,
    _record_attempt_outcomes: bool = True,
) -> dict[str, Any] | None:
    normalized_role = str(instance_role or "").strip().lower()
    if normalized_role not in {"main", "continue"}:
        return None
    attempt_outcomes = (
        _record_mailbox_attempt_outcomes(
            shared_root=shared_root,
            result_payload_value=result_payload_value,
            instance_role=instance_role,
        )
        if _record_attempt_outcomes
        else []
    )
    context = extract_mailbox_business_outcome_context(result_payload_value=result_payload_value)
    provider = str(context.get("provider") or "").strip().lower()
    domain = str(context.get("domain") or "").strip().lower()
    email = str(context.get("email") or "").strip().lower()
    if not domain:
        return None
    quality_success_reason = mailbox_quality_success_reason(
        result_payload_value=result_payload_value,
        context=context,
    )
    ok = bool(result_payload_value.get("ok")) or bool(quality_success_reason)
    ignore_reason = "" if ok else mailbox_failure_ignore_reason(result_payload_value=result_payload_value)
    if ignore_reason:
        config = _mailbox_runtime_config(shared_root=shared_root)
        business_key = config.resolve_business_key(context.get("business_key"))
        return {
            "ignored": True,
            "ignoreReason": ignore_reason,
            "businessKey": business_key,
            "provider": provider,
            "domain": domain,
            "email": email,
            "statePath": str(mailbox_domain_stats_path(shared_root=shared_root)),
        }

    payload = load_mailbox_domain_stats_state(shared_root=shared_root)
    config = _mailbox_runtime_config(shared_root=shared_root)
    business_key = config.resolve_business_key(context.get("business_key"))
    business_policy = config.resolve_business_policy(business_key)
    businesses_payload = payload.get("businesses")
    businesses = dict(businesses_payload) if isinstance(businesses_payload, dict) else {}
    business_payload = businesses.get(business_key)
    business_payload = dict(business_payload) if isinstance(business_payload, dict) else {}
    domains_payload = business_payload.get("domains")
    domains = dict(domains_payload) if isinstance(domains_payload, dict) else {}
    current = domains.get(domain)
    current = dict(current) if isinstance(current, dict) else {}
    attempts = max(0, int(current.get("attempts") or 0)) + 1
    successes = max(0, int(current.get("successes") or 0))
    failures = max(0, int(current.get("failures") or 0))
    consecutive_failures = max(0, int(current.get("consecutiveFailures") or 0))
    failure_reason = "" if ok else mailbox_failure_reason(result_payload_value=result_payload_value)
    if not ok and failure_reason:
        _report_mailbox_failure_outcome_to_easyemail(
            context=context,
            business_key=business_key,
            failure_reason=failure_reason,
        )
    failure_reasons_payload = current.get("failureReasons")
    failure_reasons = dict(failure_reasons_payload) if isinstance(failure_reasons_payload, dict) else {}
    now = datetime.now(timezone.utc).isoformat()
    if ok:
        successes += 1
        consecutive_failures = 0
        failure_reasons = {}
    else:
        failures += 1
        consecutive_failures += 1
        if failure_reason:
            failure_reasons[failure_reason] = max(0, int(failure_reasons.get(failure_reason) or 0)) + 1
    failure_rate = (float(failures) / float(attempts)) * 100.0 if attempts > 0 else 0.0
    min_attempts = mailbox_domain_blacklist_min_attempts(shared_root=shared_root)
    failure_rate_threshold = mailbox_domain_blacklist_failure_rate(shared_root=shared_root)
    blacklist_reason = mailbox_domain_blacklist_reason(result_payload_value=result_payload_value)
    prior_blacklisted = bool(current.get("blacklisted"))
    prior_blacklist_reason = str(current.get("blacklistReason") or "").strip()
    threshold = mailbox_domain_consecutive_failure_blacklist_threshold(shared_root=shared_root)
    if not ok:
        if not blacklist_reason and consecutive_failures >= threshold:
            blacklist_reason = "consecutive_failures_threshold"
        email_otp_threshold = mailbox_email_otp_failure_blacklist_threshold()
        if (
            not blacklist_reason
            and failure_reason in EMAIL_OTP_FAILURE_REASONS
            and email_otp_threshold > 0
            and mailbox_failure_reason_total(failure_reasons, EMAIL_OTP_FAILURE_REASONS) >= email_otp_threshold
        ):
            blacklist_reason = "email_otp_failure_threshold"
        if not blacklist_reason and mailbox_failure_rate_reaches_blacklist_threshold(
            attempts=attempts,
            failures=failures,
            min_attempts=min_attempts,
            failure_rate_threshold=failure_rate_threshold,
        ):
            blacklist_reason = "failure_rate_threshold"
    blacklisted = False if ok else prior_blacklisted or bool(blacklist_reason)
    stored_blacklist_reason = "" if ok else blacklist_reason or prior_blacklist_reason
    domains[domain] = {
        "provider": provider,
        "attempts": attempts,
        "successes": successes,
        "failures": failures,
        "consecutiveFailures": consecutive_failures,
        "lastOutcome": "success" if ok else "failure",
        "lastOutcomeAt": now,
        "lastEmail": email,
        "lastSuccessAt": now if ok else str(current.get("lastSuccessAt") or "").strip(),
        "lastFailureAt": now if not ok else str(current.get("lastFailureAt") or "").strip(),
        "lastFailureReason": failure_reason if not ok else str(current.get("lastFailureReason") or "").strip(),
        "failureReasons": failure_reasons,
        "blacklisted": blacklisted,
        "blacklistReason": stored_blacklist_reason,
    }

    provider_attempts = 0
    provider_successes = 0
    provider_failures = 0
    provider_consecutive_failures = 0
    provider_failure_rate = 0.0
    provider_blacklisted = False
    provider_blacklist_reason = ""
    if provider:
        providers_payload = business_payload.get("providers")
        providers = dict(providers_payload) if isinstance(providers_payload, dict) else {}
        provider_current = providers.get(provider)
        provider_current = dict(provider_current) if isinstance(provider_current, dict) else {}
        provider_attempts = max(0, int(provider_current.get("attempts") or 0)) + 1
        provider_successes = max(0, int(provider_current.get("successes") or 0))
        provider_failures = max(0, int(provider_current.get("failures") or 0))
        provider_consecutive_failures = max(0, int(provider_current.get("consecutiveFailures") or 0))
        provider_failure_reasons_payload = provider_current.get("failureReasons")
        provider_failure_reasons = (
            dict(provider_failure_reasons_payload)
            if isinstance(provider_failure_reasons_payload, dict)
            else {}
        )
        if ok:
            provider_successes += 1
            provider_consecutive_failures = 0
            provider_failure_reasons = {}
        else:
            provider_failures += 1
            provider_consecutive_failures += 1
            if failure_reason:
                provider_failure_reasons[failure_reason] = max(0, int(provider_failure_reasons.get(failure_reason) or 0)) + 1
        provider_failure_rate = (
            (float(provider_failures) / float(provider_attempts)) * 100.0
            if provider_attempts > 0
            else 0.0
        )
        prior_provider_blacklisted = bool(provider_current.get("blacklisted"))
        prior_provider_blacklist_reason = str(provider_current.get("blacklistReason") or "").strip()
        if not ok:
            provider_email_otp_threshold = mailbox_email_otp_provider_failure_blacklist_threshold()
            if (
                failure_reason in EMAIL_OTP_FAILURE_REASONS
                and provider_email_otp_threshold > 0
                and mailbox_failure_reason_total(provider_failure_reasons, EMAIL_OTP_FAILURE_REASONS)
                >= provider_email_otp_threshold
            ):
                provider_blacklist_reason = "provider_email_otp_failure_threshold"
            elif mailbox_failure_rate_reaches_blacklist_threshold(
                attempts=provider_attempts,
                failures=provider_failures,
                min_attempts=min_attempts,
                failure_rate_threshold=failure_rate_threshold,
            ):
                provider_blacklist_reason = "provider_failure_rate_threshold"
        provider_blacklisted = False if ok else prior_provider_blacklisted or bool(provider_blacklist_reason)
        provider_blacklist_reason = "" if ok else provider_blacklist_reason or prior_provider_blacklist_reason
        providers[provider] = {
            "attempts": provider_attempts,
            "successes": provider_successes,
            "failures": provider_failures,
            "consecutiveFailures": provider_consecutive_failures,
            "lastOutcome": "success" if ok else "failure",
            "lastOutcomeAt": now,
            "lastDomain": domain,
            "lastEmail": email,
            "lastSuccessAt": now if ok else str(provider_current.get("lastSuccessAt") or "").strip(),
            "lastFailureAt": now if not ok else str(provider_current.get("lastFailureAt") or "").strip(),
            "lastFailureReason": failure_reason if not ok else str(provider_current.get("lastFailureReason") or "").strip(),
            "failureReasons": provider_failure_reasons,
            "failureRate": round(provider_failure_rate, 3),
            "blacklisted": provider_blacklisted,
            "blacklistReason": provider_blacklist_reason,
        }
        business_payload["providers"] = providers

    business_payload["businessKey"] = business_key
    business_payload["updatedAt"] = now
    business_payload["explicitBlacklistDomains"] = list(business_policy.explicit_blacklist_domains)
    business_payload["explicitBlacklistProviders"] = list(business_policy.explicit_blacklist_providers)
    business_payload["domains"] = domains
    businesses[business_key] = business_payload
    payload["updatedAt"] = now
    payload["businesses"] = businesses
    write_mailbox_domain_stats_state(shared_root=shared_root, payload=payload)
    outcome = {
        "businessKey": business_key,
        "provider": provider,
        "domain": domain,
        "email": email,
        "attempts": attempts,
        "successes": successes,
        "failures": failures,
        "consecutiveFailures": consecutive_failures,
        "failureRate": round(failure_rate, 3),
        "failureReason": failure_reason,
        "lastOutcome": "success" if ok else "failure",
        "qualitySuccessReason": quality_success_reason,
        "blacklisted": blacklisted,
        "blacklistReason": stored_blacklist_reason,
        "providerAttempts": provider_attempts,
        "providerSuccesses": provider_successes,
        "providerFailures": provider_failures,
        "providerConsecutiveFailures": provider_consecutive_failures,
        "providerFailureRate": round(provider_failure_rate, 3),
        "providerBlacklisted": provider_blacklisted,
        "providerBlacklistReason": provider_blacklist_reason,
        "minAttempts": min_attempts,
        "failureRateThreshold": failure_rate_threshold,
        "consecutiveFailureThreshold": threshold,
        "statePath": str(mailbox_domain_stats_path(shared_root=shared_root)),
    }
    if attempt_outcomes:
        outcome["attemptOutcomes"] = attempt_outcomes
    return outcome


def mark_mailbox_capacity_failure(*, shared_root: Path, detail: str) -> dict[str, Any]:
    payload = load_mailbox_cleanup_state(shared_root=shared_root)
    consecutive = int(payload.get("consecutiveFailures") or 0) + 1
    now_text = datetime.now(timezone.utc).isoformat()
    payload.update(
        {
            "consecutiveFailures": consecutive,
            "lastFailureAt": now_text,
            "lastFailureDetail": str(detail or "").strip(),
        }
    )
    write_mailbox_cleanup_state(shared_root=shared_root, payload=payload)
    threshold = _cleanup_runtime_config().mailbox_cleanup_failure_threshold
    if consecutive < threshold:
        return {
            "ok": False,
            "status": "recovery_threshold_not_reached",
            "consecutiveFailures": consecutive,
            "threshold": threshold,
        }
    return trigger_mailbox_capacity_recovery(shared_root=shared_root, detail=detail)


def clear_mailbox_capacity_failures(*, shared_root: Path) -> None:
    payload = load_mailbox_cleanup_state(shared_root=shared_root)
    if not payload:
        return
    payload["consecutiveFailures"] = 0
    payload["lastRecoveredAt"] = datetime.now(timezone.utc).isoformat()
    write_mailbox_cleanup_state(shared_root=shared_root, payload=payload)
