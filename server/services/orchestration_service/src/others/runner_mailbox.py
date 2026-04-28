from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from easyemail_flow import dispatch_easyemail_step
from errors import ErrorCodes, result_error_matches, result_error_message
from others.common import ensure_directory
from others.config import CleanupRuntimeConfig, MailboxRuntimeConfig
from others.file_lock import release_lock, try_acquire_lock


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
    return payload if isinstance(payload, dict) else {}


def write_mailbox_cleanup_state(*, shared_root: Path, payload: dict[str, Any]) -> None:
    path = mailbox_cleanup_state_path(shared_root=shared_root)
    ensure_directory(path.parent)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_mailbox_domain_stats_state(*, shared_root: Path, payload: dict[str, Any]) -> None:
    path = mailbox_domain_stats_path(shared_root=shared_root)
    ensure_directory(path.parent)
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
    if not provider and mailbox_ref:
        provider = mailbox_provider_from_ref(mailbox_ref)
    if "@" not in email:
        return {
            "provider": provider,
            "mailbox_ref": mailbox_ref,
            "email": email,
            "domain": "",
        }
    return {
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
    if provider != "moemail" or not domain:
        return None

    payload = load_mailbox_domain_stats_state(shared_root=shared_root)
    domains_payload = payload.get("domains")
    domains = dict(domains_payload) if isinstance(domains_payload, dict) else {}
    current = domains.get(domain)
    current = dict(current) if isinstance(current, dict) else {}
    attempts = max(0, int(current.get("attempts") or 0)) + 1
    successes = max(0, int(current.get("successes") or 0))
    failures = max(0, int(current.get("failures") or 0))
    ok = bool(result_payload_value.get("ok"))
    now = datetime.now(timezone.utc).isoformat()
    if ok:
        successes += 1
    else:
        failures += 1
    failure_rate = (float(failures) / float(attempts)) * 100.0 if attempts > 0 else 0.0
    blacklisted = (
        attempts >= mailbox_domain_blacklist_min_attempts(shared_root=shared_root)
        and failure_rate >= mailbox_domain_blacklist_failure_rate(shared_root=shared_root)
    )
    domains[domain] = {
        "provider": provider,
        "attempts": attempts,
        "successes": successes,
        "failures": failures,
        "lastOutcome": "success" if ok else "failure",
        "lastOutcomeAt": now,
        "lastEmail": email,
        "lastSuccessAt": now if ok else str(current.get("lastSuccessAt") or "").strip(),
        "lastFailureAt": now if not ok else str(current.get("lastFailureAt") or "").strip(),
        "blacklisted": blacklisted,
    }
    payload["updatedAt"] = now
    payload["domains"] = domains
    write_mailbox_domain_stats_state(shared_root=shared_root, payload=payload)
    return {
        "provider": provider,
        "domain": domain,
        "email": email,
        "attempts": attempts,
        "successes": successes,
        "failures": failures,
        "failureRate": round(failure_rate, 3),
        "blacklisted": blacklisted,
        "minAttempts": mailbox_domain_blacklist_min_attempts(shared_root=shared_root),
        "failureRateThreshold": mailbox_domain_blacklist_failure_rate(shared_root=shared_root),
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
