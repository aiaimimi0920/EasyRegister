from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from easyemail_flow import dispatch_easyemail_step
from errors import ErrorCodes, result_error_matches, result_error_message
from others.common import ensure_directory
from others.config import CleanupRuntimeConfig, MailboxRuntimeConfig, env_int
from others.file_lock import release_lock, try_acquire_lock


MAILBOX_DOMAIN_STATS_SCHEMA_VERSION = 3
EMAIL_OTP_FAILURE_REASONS = {"email_otp_timeout", "email_otp_wrong_code"}


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

    if "sms_no_selection_plan_candidates" in combined:
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

    if "unsupported_email" in combined or "the email you provided is not supported" in combined:
        return "unsupported_email"
    if "registration_disallowed" in combined and "mailbox_provider=" in combined:
        return "registration_disallowed"
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


def record_business_mailbox_domain_outcome(
    *,
    shared_root: Path,
    result_payload_value: dict[str, Any],
    instance_role: str,
) -> dict[str, Any] | None:
    normalized_role = str(instance_role or "").strip().lower()
    if normalized_role not in {"main", "continue"}:
        return None
    context = extract_mailbox_business_outcome_context(result_payload_value=result_payload_value)
    provider = str(context.get("provider") or "").strip().lower()
    domain = str(context.get("domain") or "").strip().lower()
    email = str(context.get("email") or "").strip().lower()
    if not domain:
        return None
    ok = bool(result_payload_value.get("ok"))
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
    return {
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
