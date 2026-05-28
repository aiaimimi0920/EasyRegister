from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from others.common_io import write_json_atomic
from others.config import SmsRuntimeConfig, env_text
from others.local_config import read_easysms_server_api_key
from others.paths import resolve_shared_root as _shared_root_from_output_root

from shared_sms.easy_sms_client import open_sms_session, report_sms_outcome, wait_sms_code


DEFAULT_EASY_SMS_BASE_URL = "http://localhost:18083"
DEFAULT_SMS_TERMINAL_PHONE_BLACKLIST_SECONDS = 24 * 60 * 60
DEFAULT_SMS_TERMINAL_PROVIDER_BLACKLIST_SECONDS = 30 * 60
DEFAULT_SMS_SESSION_LOCAL_RETRY_ATTEMPTS = 6
PHONE_SCOPED_TERMINAL_CODES = {
    "phone_number_in_use",
    "phone_max_usage_exceeded",
}


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
        return {"phones": {}, "providers": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"phones": {}, "providers": {}}
    if not isinstance(payload, dict):
        return {"phones": {}, "providers": {}}
    phones = payload.get("phones") if isinstance(payload.get("phones"), dict) else {}
    providers = payload.get("providers") if isinstance(payload.get("providers"), dict) else {}
    return {"phones": dict(phones), "providers": dict(providers)}


def _write_sms_state(*, payload: dict[str, Any], config: SmsRuntimeConfig | None = None) -> None:
    resolved_config = config or _sms_runtime_config()
    write_json_atomic(resolved_config.state_path, payload, include_pid=True, cleanup_temp=True)


def _is_phone_scoped_terminal_code(terminal_code: str) -> bool:
    return str(terminal_code or "").strip().lower() in PHONE_SCOPED_TERMINAL_CODES


def _prune_sms_state(*, payload: dict[str, Any], now_ts: float | None = None) -> dict[str, Any]:
    effective_now_ts = float(now_ts or time.time())
    normalized = {
        "phones": {},
        "providers": {},
    }
    for bucket_key in ("phones", "providers"):
        bucket = payload.get(bucket_key) if isinstance(payload.get(bucket_key), dict) else {}
        target: dict[str, Any] = {}
        for raw_key, raw_value in bucket.items():
            key = str(raw_key or "").strip()
            if not key or not isinstance(raw_value, dict):
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
    return normalized


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


def _resolve_sms_session_local_retry_attempts() -> int:
    raw = str(
        os.environ.get("REGISTER_SMS_SESSION_LOCAL_RETRY_ATTEMPTS")
        or DEFAULT_SMS_SESSION_LOCAL_RETRY_ATTEMPTS
    ).strip()
    try:
        return max(1, int(float(raw or DEFAULT_SMS_SESSION_LOCAL_RETRY_ATTEMPTS)))
    except Exception:
        return DEFAULT_SMS_SESSION_LOCAL_RETRY_ATTEMPTS


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
    if normalized_phone:
        phone_until = now + timedelta(seconds=_resolve_sms_terminal_phone_blacklist_seconds())
        payload.setdefault("phones", {})[normalized_phone] = {
            "reason": normalized_code,
            "detail": normalized_message,
            "providerKey": normalized_provider,
            "blockedAt": now.isoformat().replace("+00:00", "Z"),
            "blockedUntil": phone_until.isoformat().replace("+00:00", "Z"),
            "blockedUntilTs": phone_until.timestamp(),
        }
    if normalized_provider and not _is_phone_scoped_terminal_code(normalized_code):
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
    blocked_providers = {
        str(key or "").strip().lower() for key in state_payload.get("providers", {}).keys() if str(key or "").strip()
    }
    attempt_provider_blacklist = set(policy.explicit_blacklist_providers) | blocked_providers
    session = None
    for _attempt in range(_resolve_sms_session_local_retry_attempts()):
        session = open_sms_session(
            business_key=policy.business_key,
            provider_blacklist=tuple(sorted(attempt_provider_blacklist)),
            allow_paid=policy.allow_paid,
            allow_reuse=policy.allow_reuse,
            max_bindings_per_phone=policy.max_bindings_per_phone,
            country_codes=policy.country_codes,
            selection_mode=policy.selection_mode,
            phone_blacklist=tuple(sorted(blocked_phones)),
        )
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
        raise RuntimeError("sms_no_usable_session_after_local_blacklist")
    if session is None:
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
