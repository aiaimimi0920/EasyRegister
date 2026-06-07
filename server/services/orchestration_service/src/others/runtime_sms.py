from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from others.common import json_log
from others.common_io import write_json_atomic
from others.config import SmsRuntimeConfig, env_text
from others.local_config import read_easysms_server_api_key
from others.paths import resolve_shared_root as _shared_root_from_output_root

from shared_sms.easy_sms_client import open_sms_session, report_sms_outcome, wait_sms_code


DEFAULT_EASY_SMS_BASE_URL = "http://localhost:18083"
DEFAULT_SMS_TERMINAL_PHONE_BLACKLIST_SECONDS = 24 * 60 * 60
DEFAULT_SMS_TERMINAL_PROVIDER_BLACKLIST_SECONDS = 30 * 60
DEFAULT_SMS_PHONE_SCOPED_PROVIDER_FAILURE_THRESHOLD = 5
DEFAULT_SMS_PHONE_SCOPED_PROVIDER_FAILURE_WINDOW_SECONDS = 60 * 60
DEFAULT_SMS_SESSION_LOCAL_RETRY_ATTEMPTS = 6
PHONE_SCOPED_TERMINAL_CODES = {
    "invalid_phone_number",
    "phone_number_in_use",
    "phone_max_usage_exceeded",
    "rate_limit_exceeded",
    "wrong_otp_code",
}
SOFT_SMS_TERMINAL_CODES = {
    "sms_code_timeout",
    "wait_code_timeout",
    "wait_sms_code_timeout",
}
PROVIDER_TERMINAL_OUTCOMES_KEY = "providerTerminalOutcomes"
SMS_DYNAMIC_PROVIDER_BLACKLIST_RELAXATION_ERRORS = (
    "sms_no_productive_selection_plan_candidates",
    "sms_no_selection_plan_candidates",
)
SMS_PROVIDER_CAPACITY_UNAVAILABLE_MARKERS = (
    "currently unavailable",
    "no eligible public numbers",
    "no available public numbers",
)


def _sms_runtime_config() -> SmsRuntimeConfig:
    output_root_text = env_text("REGISTER_OUTPUT_ROOT")
    if output_root_text:
        default_state_path = _shared_root_from_output_root(Path(output_root_text).expanduser()) / "others" / "register-sms-state.json"
    else:
        default_state_path = Path.cwd().resolve() / "others" / "register-sms-state.json"
    return SmsRuntimeConfig.from_env(default_state_path=default_state_path)


def _load_sms_state(*, config: SmsRuntimeConfig | None = None) -> dict[str, Any]:
    resolved_config = config or _sms_runtime_config()
    path = resolved_config.state_path
    if not path.is_file():
        return {"phones": {}, "providers": {}, PROVIDER_TERMINAL_OUTCOMES_KEY: {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"phones": {}, "providers": {}, PROVIDER_TERMINAL_OUTCOMES_KEY: {}}
    if not isinstance(payload, dict):
        return {"phones": {}, "providers": {}, PROVIDER_TERMINAL_OUTCOMES_KEY: {}}
    phones = payload.get("phones") if isinstance(payload.get("phones"), dict) else {}
    providers = payload.get("providers") if isinstance(payload.get("providers"), dict) else {}
    provider_outcomes = (
        payload.get(PROVIDER_TERMINAL_OUTCOMES_KEY)
        if isinstance(payload.get(PROVIDER_TERMINAL_OUTCOMES_KEY), dict)
        else {}
    )
    return {
        "phones": dict(phones),
        "providers": dict(providers),
        PROVIDER_TERMINAL_OUTCOMES_KEY: dict(provider_outcomes),
    }


def _write_sms_state(*, payload: dict[str, Any], config: SmsRuntimeConfig | None = None) -> None:
    resolved_config = config or _sms_runtime_config()
    write_json_atomic(resolved_config.state_path, payload, include_pid=True, cleanup_temp=True)


def _is_phone_scoped_terminal_code(terminal_code: str) -> bool:
    return str(terminal_code or "").strip().lower() in PHONE_SCOPED_TERMINAL_CODES


def _is_soft_sms_terminal_code(terminal_code: str) -> bool:
    return str(terminal_code or "").strip().lower() in SOFT_SMS_TERMINAL_CODES


def _parse_iso_timestamp(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


def _prune_sms_state(*, payload: dict[str, Any], now_ts: float | None = None) -> dict[str, Any]:
    effective_now_ts = float(now_ts or time.time())
    normalized = {
        "phones": {},
        "providers": {},
        PROVIDER_TERMINAL_OUTCOMES_KEY: {},
    }
    for bucket_key in ("phones", "providers"):
        bucket = payload.get(bucket_key) if isinstance(payload.get(bucket_key), dict) else {}
        target: dict[str, Any] = {}
        for raw_key, raw_value in bucket.items():
            key = str(raw_key or "").strip()
            if not key or not isinstance(raw_value, dict):
                continue
            reason = str(raw_value.get("reason") or "").strip()
            if bucket_key == "phones" and not _is_phone_scoped_terminal_code(reason):
                continue
            if bucket_key == "providers" and _is_phone_scoped_terminal_code(str(raw_value.get("reason") or "")):
                continue
            try:
                blocked_until_ts = float(raw_value.get("blockedUntilTs") or 0.0)
            except Exception:
                blocked_until_ts = 0.0
            if blocked_until_ts > effective_now_ts:
                target[key] = dict(raw_value)
        normalized[bucket_key] = target
    outcome_window_seconds = _resolve_sms_phone_scoped_provider_failure_window_seconds()
    min_outcome_ts = effective_now_ts - outcome_window_seconds
    raw_outcomes = (
        payload.get(PROVIDER_TERMINAL_OUTCOMES_KEY)
        if isinstance(payload.get(PROVIDER_TERMINAL_OUTCOMES_KEY), dict)
        else {}
    )
    for raw_provider, raw_entries in raw_outcomes.items():
        provider_key = str(raw_provider or "").strip().lower()
        if not provider_key or not isinstance(raw_entries, list):
            continue
        entries: list[dict[str, Any]] = []
        for raw_entry in raw_entries:
            if not isinstance(raw_entry, dict):
                continue
            reason = str(raw_entry.get("reason") or "").strip()
            if not _is_phone_scoped_terminal_code(reason):
                continue
            try:
                recorded_at_ts = float(raw_entry.get("atTs") or 0.0)
            except Exception:
                recorded_at_ts = 0.0
            if recorded_at_ts < min_outcome_ts:
                continue
            entries.append(dict(raw_entry))
        if entries:
            normalized[PROVIDER_TERMINAL_OUTCOMES_KEY][provider_key] = entries
    return normalized


def _provider_blacklist_from_repeated_phone_scoped_state(
    *,
    payload: dict[str, Any],
    now_ts: float | None = None,
) -> tuple[str, ...]:
    threshold = _resolve_sms_phone_scoped_provider_failure_threshold()
    if threshold <= 0:
        return ()
    effective_now_ts = float(now_ts or time.time())
    min_recorded_at_ts = effective_now_ts - _resolve_sms_phone_scoped_provider_failure_window_seconds()
    counts: dict[str, int] = {}
    phones = payload.get("phones") if isinstance(payload.get("phones"), dict) else {}
    for raw_value in phones.values():
        if not isinstance(raw_value, dict):
            continue
        reason = str(raw_value.get("reason") or "").strip()
        if not _is_phone_scoped_terminal_code(reason):
            continue
        provider_key = str(raw_value.get("providerKey") or "").strip().lower()
        if not provider_key:
            continue
        recorded_at_ts = _parse_iso_timestamp(raw_value.get("blockedAt"))
        if recorded_at_ts is None:
            try:
                recorded_at_ts = float(raw_value.get("blockedUntilTs") or 0.0) - _resolve_sms_terminal_phone_blacklist_seconds()
            except Exception:
                recorded_at_ts = 0.0
        if recorded_at_ts < min_recorded_at_ts:
            continue
        counts[provider_key] = counts.get(provider_key, 0) + 1
    return tuple(sorted(provider_key for provider_key, count in counts.items() if count >= threshold))


def _resolve_sms_terminal_phone_blacklist_seconds() -> int:
    raw = str(
        os.environ.get("REGISTER_SMS_TERMINAL_PHONE_BLACKLIST_SECONDS")
        or DEFAULT_SMS_TERMINAL_PHONE_BLACKLIST_SECONDS
    ).strip()
    try:
        return max(0, int(float(raw or DEFAULT_SMS_TERMINAL_PHONE_BLACKLIST_SECONDS)))
    except Exception:
        return DEFAULT_SMS_TERMINAL_PHONE_BLACKLIST_SECONDS


def _resolve_sms_terminal_provider_blacklist_seconds() -> int:
    raw = str(
        os.environ.get("REGISTER_SMS_TERMINAL_PROVIDER_BLACKLIST_SECONDS")
        or DEFAULT_SMS_TERMINAL_PROVIDER_BLACKLIST_SECONDS
    ).strip()
    try:
        return max(0, int(float(raw or DEFAULT_SMS_TERMINAL_PROVIDER_BLACKLIST_SECONDS)))
    except Exception:
        return DEFAULT_SMS_TERMINAL_PROVIDER_BLACKLIST_SECONDS


def _resolve_sms_phone_scoped_provider_failure_threshold() -> int:
    raw = str(
        os.environ.get("REGISTER_SMS_PHONE_SCOPED_PROVIDER_FAILURE_THRESHOLD")
        or DEFAULT_SMS_PHONE_SCOPED_PROVIDER_FAILURE_THRESHOLD
    ).strip()
    try:
        return max(0, int(float(raw or DEFAULT_SMS_PHONE_SCOPED_PROVIDER_FAILURE_THRESHOLD)))
    except Exception:
        return DEFAULT_SMS_PHONE_SCOPED_PROVIDER_FAILURE_THRESHOLD


def _resolve_sms_phone_scoped_provider_failure_window_seconds() -> int:
    raw = str(
        os.environ.get("REGISTER_SMS_PHONE_SCOPED_PROVIDER_FAILURE_WINDOW_SECONDS")
        or DEFAULT_SMS_PHONE_SCOPED_PROVIDER_FAILURE_WINDOW_SECONDS
    ).strip()
    try:
        return max(1, int(float(raw or DEFAULT_SMS_PHONE_SCOPED_PROVIDER_FAILURE_WINDOW_SECONDS)))
    except Exception:
        return DEFAULT_SMS_PHONE_SCOPED_PROVIDER_FAILURE_WINDOW_SECONDS


def _resolve_sms_session_local_retry_attempts() -> int:
    raw = str(
        os.environ.get("REGISTER_SMS_SESSION_LOCAL_RETRY_ATTEMPTS")
        or DEFAULT_SMS_SESSION_LOCAL_RETRY_ATTEMPTS
    ).strip()
    try:
        return max(1, int(float(raw or DEFAULT_SMS_SESSION_LOCAL_RETRY_ATTEMPTS)))
    except Exception:
        return DEFAULT_SMS_SESSION_LOCAL_RETRY_ATTEMPTS


def _dynamic_provider_blacklist_relaxation_reason(exc: BaseException) -> str:
    message = str(exc or "")
    for marker in SMS_DYNAMIC_PROVIDER_BLACKLIST_RELAXATION_ERRORS:
        if marker in message:
            return marker
    return ""


def _provider_capacity_unavailable_from_open_error(exc: BaseException) -> str:
    message = str(exc or "").strip()
    if not message:
        return ""
    lowered = message.lower()
    if not any(marker in lowered for marker in SMS_PROVIDER_CAPACITY_UNAVAILABLE_MARKERS):
        return ""
    marker = 'provider "'
    marker_index = lowered.find(marker)
    if marker_index < 0:
        return ""
    start = marker_index + len(marker)
    end = message.find('"', start)
    if end <= start:
        return ""
    return message[start:end].strip().lower()


def _match_phone_country_code(*, phone_number: str, country_codes: tuple[str, ...]) -> str:
    normalized_phone = str(phone_number or "").strip()
    if not normalized_phone:
        return ""
    normalized_country_codes = sorted(
        {
            str(country_code or "").strip()
            for country_code in country_codes
            if str(country_code or "").strip().startswith("+")
        },
        key=len,
        reverse=True,
    )
    for country_code in normalized_country_codes:
        if normalized_phone.startswith(country_code):
            return country_code
    return ""


def _provider_country_blacklist_from_state(
    *,
    payload: dict[str, Any],
    country_codes: tuple[str, ...],
) -> tuple[str, ...]:
    phones = payload.get("phones") if isinstance(payload.get("phones"), dict) else {}
    pairs: set[str] = set()
    for raw_phone, raw_value in phones.items():
        if not isinstance(raw_value, dict):
            continue
        provider_key = str(raw_value.get("providerKey") or "").strip().lower()
        if not provider_key:
            continue
        country_code = _match_phone_country_code(
            phone_number=str(raw_phone or "").strip(),
            country_codes=country_codes,
        )
        if country_code:
            pairs.add(f"{provider_key}|{country_code}")
    return tuple(sorted(pairs))


def record_terminal_phone_outcome(
    *,
    phone_number: str,
    provider_key: str,
    terminal_code: str,
    terminal_message: str = "",
) -> dict[str, Any]:
    normalized_phone = str(phone_number or "").strip()
    normalized_provider = str(provider_key or "").strip().lower()
    normalized_code = str(terminal_code or "").strip() or "phone_verification_terminal"
    normalized_message = str(terminal_message or "").strip()
    config = _sms_runtime_config()
    payload = _prune_sms_state(payload=_load_sms_state(config=config))
    now = datetime.now(timezone.utc)
    phone_block_recorded = False
    if normalized_phone and _is_phone_scoped_terminal_code(normalized_code):
        phone_until = now + timedelta(seconds=_resolve_sms_terminal_phone_blacklist_seconds())
        payload.setdefault("phones", {})[normalized_phone] = {
            "reason": normalized_code,
            "detail": normalized_message,
            "providerKey": normalized_provider,
            "blockedAt": now.isoformat().replace("+00:00", "Z"),
            "blockedUntil": phone_until.isoformat().replace("+00:00", "Z"),
            "blockedUntilTs": phone_until.timestamp(),
        }
        phone_block_recorded = True
    if normalized_provider and _is_phone_scoped_terminal_code(normalized_code):
        provider_outcomes = payload.setdefault(PROVIDER_TERMINAL_OUTCOMES_KEY, {})
        entries = provider_outcomes.setdefault(normalized_provider, [])
        if not isinstance(entries, list):
            entries = []
            provider_outcomes[normalized_provider] = entries
        entries.append(
            {
                "reason": normalized_code,
                "detail": normalized_message,
                "phoneNumber": normalized_phone,
                "at": now.isoformat().replace("+00:00", "Z"),
                "atTs": now.timestamp(),
            }
        )
        payload = _prune_sms_state(payload=payload, now_ts=now.timestamp())
        entries = list(payload.get(PROVIDER_TERMINAL_OUTCOMES_KEY, {}).get(normalized_provider, []))
        threshold = _resolve_sms_phone_scoped_provider_failure_threshold()
        if threshold > 0 and len(entries) >= threshold:
            provider_until = now + timedelta(seconds=_resolve_sms_terminal_provider_blacklist_seconds())
            payload.setdefault("providers", {})[normalized_provider] = {
                "reason": "repeated_phone_scoped_terminal",
                "detail": (
                    f"{len(entries)} phone-scoped terminal outcomes within "
                    f"{_resolve_sms_phone_scoped_provider_failure_window_seconds()} seconds; "
                    f"latest={normalized_code}"
                ),
                "phoneNumber": normalized_phone,
                "blockedAt": now.isoformat().replace("+00:00", "Z"),
                "blockedUntil": provider_until.isoformat().replace("+00:00", "Z"),
                "blockedUntilTs": provider_until.timestamp(),
                "terminalFailureCount": len(entries),
            }
    elif (
        normalized_provider
        and not _is_phone_scoped_terminal_code(normalized_code)
        and not _is_soft_sms_terminal_code(normalized_code)
    ):
        provider_until = now + timedelta(seconds=_resolve_sms_terminal_provider_blacklist_seconds())
        payload.setdefault("providers", {})[normalized_provider] = {
            "reason": normalized_code,
            "detail": normalized_message,
            "phoneNumber": normalized_phone,
            "blockedAt": now.isoformat().replace("+00:00", "Z"),
            "blockedUntil": provider_until.isoformat().replace("+00:00", "Z"),
            "blockedUntilTs": provider_until.timestamp(),
        }
    _write_sms_state(payload=payload, config=config)
    provider_block = (
        payload.get("providers", {}).get(normalized_provider)
        if normalized_provider and isinstance(payload.get("providers"), dict)
        else None
    )
    json_log(
        {
            "event": "register_sms_terminal_phone_outcome_recorded",
            "providerKey": normalized_provider,
            "terminalCode": normalized_code,
            "phoneRecorded": phone_block_recorded,
            "phoneScoped": _is_phone_scoped_terminal_code(normalized_code),
            "softTerminal": _is_soft_sms_terminal_code(normalized_code),
            "providerBlocked": bool(provider_block),
            "providerBlockReason": (
                str(provider_block.get("reason") or "").strip()
                if isinstance(provider_block, dict)
                else ""
            ),
            "terminalFailureCount": (
                int(provider_block.get("terminalFailureCount") or 0)
                if isinstance(provider_block, dict)
                else 0
            ),
        }
    )
    return payload


def ensure_easy_sms_env_defaults() -> None:
    if not str(os.environ.get("SMS_SERVICE_BASE_URL") or "").strip():
        os.environ["SMS_SERVICE_BASE_URL"] = DEFAULT_EASY_SMS_BASE_URL
    if not str(os.environ.get("SMS_SERVICE_API_KEY") or "").strip():
        discovered_api_key = read_easysms_server_api_key()
        if discovered_api_key:
            os.environ["SMS_SERVICE_API_KEY"] = discovered_api_key


def open_phone_session_for_business(*, business_key: str | None = None) -> dict[str, Any]:
    ensure_easy_sms_env_defaults()
    config = _sms_runtime_config()
    policy = config.resolve_business_policy(business_key)
    if not policy.enabled:
        raise RuntimeError("sms_not_enabled_for_business")
    state_payload = _prune_sms_state(payload=_load_sms_state(config=config))
    blocked_phones = {str(key or "").strip() for key in state_payload.get("phones", {}).keys() if str(key or "").strip()}
    hard_blocked_providers = {
        str(key or "").strip().lower() for key in state_payload.get("providers", {}).keys() if str(key or "").strip()
    }
    dynamic_blocked_providers = set(
        _provider_blacklist_from_repeated_phone_scoped_state(payload=state_payload)
    )
    static_blocked_providers = set(policy.explicit_blacklist_providers)
    attempt_provider_blacklist = static_blocked_providers | hard_blocked_providers | dynamic_blocked_providers
    relaxed_dynamic_provider_blacklist = False
    provider_country_blacklist = _provider_country_blacklist_from_state(
        payload=state_payload,
        country_codes=tuple(policy.country_codes),
    )
    session = None
    last_open_error: BaseException | None = None
    for _attempt in range(_resolve_sms_session_local_retry_attempts()):
        try:
            session = open_sms_session(
                business_key=policy.business_key,
                provider_blacklist=tuple(sorted(attempt_provider_blacklist)),
                allow_paid=policy.allow_paid,
                allow_reuse=policy.allow_reuse,
                max_bindings_per_phone=policy.max_bindings_per_phone,
                country_codes=policy.country_codes,
                selection_mode=policy.selection_mode,
                phone_blacklist=tuple(sorted(blocked_phones)),
                provider_country_blacklist=provider_country_blacklist,
            )
        except Exception as exc:
            last_open_error = exc
            capacity_unavailable_provider = _provider_capacity_unavailable_from_open_error(exc)
            if capacity_unavailable_provider:
                record_terminal_phone_outcome(
                    phone_number="",
                    provider_key=capacity_unavailable_provider,
                    terminal_code="provider_capacity_unavailable",
                    terminal_message=str(exc),
                )
                attempt_provider_blacklist.add(capacity_unavailable_provider)
                continue
            dynamic_relaxation_reason = _dynamic_provider_blacklist_relaxation_reason(exc)
            if (
                not relaxed_dynamic_provider_blacklist
                and dynamic_blocked_providers
                and dynamic_relaxation_reason
            ):
                relaxed_dynamic_provider_blacklist = True
                attempt_provider_blacklist = static_blocked_providers | hard_blocked_providers
                json_log(
                    {
                        "event": "register_sms_dynamic_provider_blacklist_relaxed",
                        "blockedProviderCount": len(dynamic_blocked_providers),
                        "hardProviderBlockCount": len(hard_blocked_providers),
                        "staticProviderBlockCount": len(static_blocked_providers),
                        "reason": dynamic_relaxation_reason,
                    }
                )
            continue
        normalized_phone = str(session.phone_number or "").strip()
        normalized_provider = str(session.provider_key or "").strip().lower()
        if normalized_phone and normalized_phone not in blocked_phones:
            break
        report_sms_outcome(
            session_id=session.session_id,
            outcome="failure",
            detail="blacklisted_phone_number",
        )
        if normalized_provider:
            attempt_provider_blacklist.add(normalized_provider)
    else:
        if last_open_error is not None:
            raise last_open_error
        raise RuntimeError("sms_no_usable_session_after_local_blacklist")
    if session is None:
        if last_open_error is not None:
            raise last_open_error
        raise RuntimeError("sms_no_usable_session_after_local_blacklist")
    return {
        "sessionId": session.session_id,
        "phoneNumber": session.phone_number,
        "providerKey": session.provider_key,
    }


def build_phone_verification_step_input(*, business_key: str | None = None) -> dict[str, Any]:
    policy = _sms_runtime_config().resolve_business_policy(business_key)
    return {
        "enabled": bool(policy.enabled),
        "business_key": policy.business_key,
        "provider_blacklist": list(policy.explicit_blacklist_providers),
        "allow_paid": bool(policy.allow_paid),
        "allow_reuse": bool(policy.allow_reuse),
        "max_bindings_per_phone": int(policy.max_bindings_per_phone),
        "country_codes": list(policy.country_codes),
        "selection_mode": str(policy.selection_mode or "").strip(),
    }


def wait_phone_code_for_session(*, session_id: str, timeout_seconds: int) -> str:
    ensure_easy_sms_env_defaults()
    return wait_sms_code(
        session_id=str(session_id or "").strip(),
        timeout_seconds=max(5, int(timeout_seconds)),
    )


def report_phone_outcome_for_session(*, session_id: str, outcome: str, detail: str = "") -> dict[str, Any]:
    ensure_easy_sms_env_defaults()
    return report_sms_outcome(
        session_id=str(session_id or "").strip(),
        outcome=str(outcome or "").strip(),
        detail=str(detail or "").strip(),
    )
