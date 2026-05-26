from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SmsSession:
    session_id: str
    phone_number: str
    provider_key: str


def _sms_service_base_url() -> str:
    value = str(os.environ.get("SMS_SERVICE_BASE_URL") or "").strip().rstrip("/")
    if not value:
        raise RuntimeError("SMS_SERVICE_BASE_URL is required")
    return value


def _sms_service_headers() -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    api_key = str(os.environ.get("SMS_SERVICE_API_KEY") or "").strip()
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers


def _post_json(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    url = _sms_service_base_url() + path
    req = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        method="POST",
        headers=_sms_service_headers(),
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"sms service POST {path} failed: HTTP {exc.code}: {raw}") from exc
    except Exception as exc:
        raise RuntimeError(f"sms service POST {path} failed: {exc}") from exc
    return json.loads(raw)


def _get_json(path: str) -> dict[str, Any]:
    url = _sms_service_base_url() + path
    req = urllib.request.Request(url, method="GET", headers=_sms_service_headers())
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"sms service GET {path} failed: HTTP {exc.code}: {raw}") from exc
    except Exception as exc:
        raise RuntimeError(f"sms service GET {path} failed: {exc}") from exc
    return json.loads(raw)


def open_sms_session(
    *,
    business_key: str,
    provider_blacklist: tuple[str, ...],
    allow_paid: bool,
    allow_reuse: bool,
    max_bindings_per_phone: int,
    country_codes: tuple[str, ...],
    selection_mode: str,
) -> SmsSession:
    response = _post_json(
        "/sms/sessions/open",
        {
            "businessKey": str(business_key or "").strip(),
            "providerBlacklist": list(provider_blacklist),
            "costTier": "paid" if allow_paid else "free",
            "allowReuse": bool(allow_reuse),
            "maxBindingsPerPhone": max(1, int(max_bindings_per_phone or 1)),
            "countryCodes": list(country_codes),
            "selectionMode": str(selection_mode or "").strip() or "available-first",
        },
    )
    session = dict((response.get("result") or {}).get("session") or {})
    session_id = str(session.get("id") or "").strip()
    phone_number = str(session.get("phoneNumberE164") or session.get("phoneNumber") or "").strip()
    provider_key = str(session.get("providerKey") or "").strip().lower()
    if not session_id or not phone_number:
        raise RuntimeError("sms service returned invalid sms session")
    return SmsSession(
        session_id=session_id,
        phone_number=phone_number,
        provider_key=provider_key,
    )


def wait_sms_code(*, session_id: str, timeout_seconds: int) -> str:
    deadline = time.time() + max(5, int(timeout_seconds))
    effective_session_id = str(session_id or "").strip()
    if not effective_session_id:
        raise RuntimeError("sms session_id is required")
    while time.time() < deadline:
        response = _get_json(f"/sms/sessions/{effective_session_id}/code")
        code_payload = dict(response.get("code") or {})
        code = str(code_payload.get("code") or code_payload.get("value") or "").strip()
        if code:
            return code
        time.sleep(3)
    raise RuntimeError("timeout waiting for sms verification code")


def report_sms_outcome(*, session_id: str, outcome: str, detail: str = "") -> dict[str, Any]:
    response = _post_json(
        "/sms/sessions/report-outcome",
        {
            "sessionId": str(session_id or "").strip(),
            "outcome": str(outcome or "").strip(),
            "detail": str(detail or "").strip(),
        },
    )
    return dict(response.get("result") or {})
