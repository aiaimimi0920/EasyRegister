from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from others.config import SmsRuntimeConfig, env_text
from others.local_config import read_easysms_server_api_key
from others.paths import resolve_shared_root as _shared_root_from_output_root

from shared_sms.easy_sms_client import open_sms_session, report_sms_outcome, wait_sms_code


DEFAULT_EASY_SMS_BASE_URL = "http://localhost:18083"


def _sms_runtime_config() -> SmsRuntimeConfig:
    output_root_text = env_text("REGISTER_OUTPUT_ROOT")
    if output_root_text:
        default_state_path = _shared_root_from_output_root(Path(output_root_text).expanduser()) / "others" / "register-sms-state.json"
    else:
        default_state_path = Path.cwd().resolve() / "others" / "register-sms-state.json"
    return SmsRuntimeConfig.from_env(default_state_path=default_state_path)


def ensure_easy_sms_env_defaults() -> None:
    if not str(os.environ.get("SMS_SERVICE_BASE_URL") or "").strip():
        os.environ["SMS_SERVICE_BASE_URL"] = DEFAULT_EASY_SMS_BASE_URL
    if not str(os.environ.get("SMS_SERVICE_API_KEY") or "").strip():
        discovered_api_key = read_easysms_server_api_key()
        if discovered_api_key:
            os.environ["SMS_SERVICE_API_KEY"] = discovered_api_key


def open_phone_session_for_business(*, business_key: str | None = None) -> dict[str, Any]:
    ensure_easy_sms_env_defaults()
    policy = _sms_runtime_config().resolve_business_policy(business_key)
    if not policy.enabled:
        raise RuntimeError("sms_not_enabled_for_business")
    session = open_sms_session(
        business_key=policy.business_key,
        provider_blacklist=policy.explicit_blacklist_providers,
        allow_paid=policy.allow_paid,
        allow_reuse=policy.allow_reuse,
        max_bindings_per_phone=policy.max_bindings_per_phone,
        country_codes=policy.country_codes,
        selection_mode=policy.selection_mode,
    )
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
