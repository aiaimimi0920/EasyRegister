from __future__ import annotations

import ipaddress
import json
import os
import socket
import time
from datetime import datetime, timezone
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


DEFAULT_SMS_SERVICE_READY_TIMEOUT_SECONDS = 90
DEFAULT_SMS_SERVICE_READY_PROBE_INTERVAL_SECONDS = 2
DEFAULT_SMS_SERVICE_REQUEST_ATTEMPTS = 3
SUPPORTED_SMS_SELECTION_MODES = {
    "price-first",
    "success-first",
    "stock-first",
    "balanced",
}


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
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    api_key = str(os.environ.get("SMS_SERVICE_API_KEY") or "").strip()
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers


def _sms_service_ready_timeout_seconds() -> int:
    raw = str(
        os.environ.get("SMS_SERVICE_READY_TIMEOUT_SECONDS")
        or DEFAULT_SMS_SERVICE_READY_TIMEOUT_SECONDS
    ).strip()
    try:
        return max(5, int(raw))
    except Exception:
        return DEFAULT_SMS_SERVICE_READY_TIMEOUT_SECONDS


def _sms_service_ready_probe_interval_seconds() -> int:
    raw = str(
        os.environ.get("SMS_SERVICE_READY_PROBE_INTERVAL_SECONDS")
        or DEFAULT_SMS_SERVICE_READY_PROBE_INTERVAL_SECONDS
    ).strip()
    try:
        return max(1, int(raw))
    except Exception:
        return DEFAULT_SMS_SERVICE_READY_PROBE_INTERVAL_SECONDS


def _sms_service_request_attempts() -> int:
    raw = str(
        os.environ.get("SMS_SERVICE_REQUEST_ATTEMPTS")
        or DEFAULT_SMS_SERVICE_REQUEST_ATTEMPTS
    ).strip()
    try:
        return max(1, int(raw))
    except Exception:
        return DEFAULT_SMS_SERVICE_REQUEST_ATTEMPTS


def _build_opener() -> urllib.request.OpenerDirector:
    base_url = _sms_service_base_url()
    parsed = urllib.parse.urlparse(base_url)
    host = parsed.hostname or ""
    should_bypass_proxy = host in ("127.0.0.1", "localhost", "::1", "0.0.0.0", "easy-sms")
    if not should_bypass_proxy and host:
        try:
            ip = ipaddress.ip_address(host)
            should_bypass_proxy = bool(ip.is_loopback or ip.is_private or ip.is_link_local)
        except ValueError:
            should_bypass_proxy = False
    if should_bypass_proxy:
        return urllib.request.build_opener(urllib.request.ProxyHandler({}))
    return urllib.request.build_opener()


def _sms_service_error_code_from_body(body: str) -> str:
    text = str(body or "").strip()
    if not text:
        return ""
    try:
        payload = json.loads(text)
    except Exception:
        return ""
    if not isinstance(payload, dict):
        return ""
    for key in ("code", "errorCode", "error"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    return ""


def _is_transient_sms_service_http_body(status_code: int, body: str) -> bool:
    normalized = str(body or "").strip().lower()
    error_code = _sms_service_error_code_from_body(body).upper()
    if int(status_code or 0) in (429, 500, 502, 503, 504):
        if error_code in {"SMS_UPSTREAM_TRANSIENT", "SMS_CAPACITY_UNAVAILABLE"}:
            return True
        return any(
            token in normalized
            for token in (
                "rate limit",
                "too many requests",
                "capacity unavailable",
                "temporarily unavailable",
                "upstream transient",
                "timed out",
                "timeout",
            )
        )
    return False


def _is_transient_sms_service_error(exc: Exception, *, path: str) -> bool:
    _ = path
    if isinstance(exc, urllib.error.HTTPError):
        body = ""
        try:
            body = exc.read().decode("utf-8", errors="replace")
        except Exception:
            body = ""
        if exc.code in (502, 503, 504):
            return True
        return _is_transient_sms_service_http_body(exc.code, body)
    if isinstance(exc, urllib.error.URLError):
        reason = getattr(exc, "reason", None)
        if isinstance(reason, (ConnectionRefusedError, TimeoutError, socket.timeout)):
            return True
        return any(
            token in str(reason or "").lower()
            for token in ("connection refused", "timed out", "actively refused")
        )
    return isinstance(exc, (ConnectionRefusedError, TimeoutError, socket.timeout))


def _sms_service_request(
    *,
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
    timeout_seconds: int = 30,
    attempts: int | None = None,
) -> dict[str, Any]:
    request_attempts = attempts or _sms_service_request_attempts()
    last_error: Exception | None = None
    for attempt_index in range(1, request_attempts + 1):
        data = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            _sms_service_base_url() + path,
            data=data,
            method=method,
            headers=_sms_service_headers(),
        )
        try:
            with _build_opener().open(req, timeout=timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8", errors="replace"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            error_code = _sms_service_error_code_from_body(body)
            message = f"sms service {method} {path} failed: HTTP {exc.code}: {body[:200]}"
            if error_code:
                message = f"sms service {method} {path} failed: HTTP {exc.code} [code={error_code}]: {body[:200]}"
            wrapped = RuntimeError(message)
            if attempt_index < request_attempts and _is_transient_sms_service_http_body(exc.code, body):
                last_error = wrapped
                time.sleep(min(5, attempt_index))
                continue
            raise wrapped from exc
        except Exception as exc:
            if attempt_index < request_attempts and _is_transient_sms_service_error(exc, path=path):
                last_error = exc
                time.sleep(min(5, attempt_index))
                continue
            raise
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"sms service {method} {path} failed without a concrete error")


def _post_json(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    return _sms_service_request(method="POST", path=path, payload=payload)


def _get_json(path: str) -> dict[str, Any]:
    return _sms_service_request(method="GET", path=path)


def _first_country_code(country_codes: tuple[str, ...]) -> str:
    for item in country_codes:
        normalized = str(item or "").strip()
        if normalized:
            return normalized
    return ""


def _country_code_candidates(country_codes: tuple[str, ...]) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()
    for item in country_codes:
        normalized = str(item or "").strip()
        if not normalized or normalized in seen:
            continue
        candidates.append(normalized)
        seen.add(normalized)
    return candidates or [""]


def _normalize_selection_mode(selection_mode: str) -> str:
    normalized = str(selection_mode or "").strip().lower()
    if normalized in SUPPORTED_SMS_SELECTION_MODES:
        return normalized
    return ""


def _normalize_provider_country_blacklist(items: tuple[str, ...]) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    for item in items:
        raw = str(item or "").strip()
        if not raw or "|" not in raw:
            continue
        provider_key, country_code = raw.split("|", 1)
        normalized_provider_key = provider_key.strip().lower()
        normalized_country_code = country_code.strip()
        if normalized_provider_key and normalized_country_code:
            pairs.add((normalized_provider_key, normalized_country_code))
    return pairs


def _is_retryable_provider_open_error(exc: Exception) -> bool:
    normalized = str(exc or "").strip().lower()
    return any(
        token in normalized
        for token in (
            "currently unavailable",
            "no eligible public numbers",
            "no available public numbers",
            "empty directory response",
            "provider temporarily unavailable",
        )
    )


def _query_provider_selection_candidates(
    *,
    provider_blacklist: tuple[str, ...],
    allow_paid: bool,
    country_codes: tuple[str, ...],
) -> list[str]:
    query = {
        "costTier": "paid" if allow_paid else "free",
        "limit": "20",
    }
    first_country_code = _first_country_code(country_codes)
    if first_country_code:
        query["countryCode"] = first_country_code
    plan_response = _get_json(
        "/sms/query/providers/selection-plan?" + urllib.parse.urlencode(query)
    )
    raw_candidates = plan_response.get("candidates") or []
    candidates: list[str] = []
    blacklist = {str(item or "").strip().lower() for item in provider_blacklist if str(item or "").strip()}
    for raw_candidate in raw_candidates:
        if not isinstance(raw_candidate, dict):
            continue
        provider_key = str(raw_candidate.get("providerKey") or "").strip().lower()
        if not provider_key or provider_key in blacklist:
            continue
        if raw_candidate.get("available") is False:
            continue
        health_state = str(raw_candidate.get("healthState") or "").strip().lower()
        if health_state == "empty":
            continue
        candidates.append(provider_key)
    if candidates:
        return candidates
    if raw_candidates:
        return []
    return _query_provider_catalog_candidates(
        provider_blacklist=provider_blacklist,
        allow_paid=allow_paid,
    )


def _query_provider_catalog_candidates(
    *,
    provider_blacklist: tuple[str, ...],
    allow_paid: bool,
    exclude_provider_keys: tuple[str, ...] = (),
) -> list[str]:
    candidates: list[str] = []
    blacklist = {str(item or "").strip().lower() for item in provider_blacklist if str(item or "").strip()}
    excluded = {str(item or "").strip().lower() for item in exclude_provider_keys if str(item or "").strip()}
    cost_tier = "paid" if allow_paid else "free"
    providers_response = _get_json(
        "/sms/query/providers?" + urllib.parse.urlencode({"costTier": cost_tier})
    )
    for raw_provider in providers_response.get("providers") or []:
        if not isinstance(raw_provider, dict):
            continue
        provider_key = str(raw_provider.get("key") or "").strip().lower()
        if not provider_key or provider_key in blacklist or provider_key in excluded:
            continue
        candidates.append(provider_key)
    return candidates


def _wait_sms_service_ready() -> None:
    deadline = time.time() + _sms_service_ready_timeout_seconds()
    interval_seconds = _sms_service_ready_probe_interval_seconds()
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            _sms_service_request(method="GET", path="/sms/catalog", timeout_seconds=10, attempts=1)
            return
        except Exception as exc:
            last_error = exc
            time.sleep(interval_seconds)
    if last_error is not None:
        raise RuntimeError(f"sms service not ready after wait: {last_error}") from last_error
    raise RuntimeError("sms service not ready after wait")


def open_sms_session(
    *,
    business_key: str,
    provider_blacklist: tuple[str, ...],
    allow_paid: bool,
    allow_reuse: bool,
    max_bindings_per_phone: int,
    country_codes: tuple[str, ...],
    selection_mode: str,
    phone_blacklist: tuple[str, ...] = (),
    provider_country_blacklist: tuple[str, ...] = (),
) -> SmsSession:
    _wait_sms_service_ready()
    selection_candidates = _query_provider_selection_candidates(
        provider_blacklist=provider_blacklist,
        allow_paid=allow_paid,
        country_codes=country_codes,
    )
    base_payload: dict[str, Any] = {
        "businessKey": str(business_key or "").strip(),
        "costTier": "paid" if allow_paid else "free",
        "allowReuse": bool(allow_reuse),
        "maxBindingsPerPhone": max(1, int(max_bindings_per_phone or 1)),
    }
    if selection_candidates:
        candidate_provider_keys = list(selection_candidates)
    else:
        candidate_provider_keys = [""]
    first_country_code = _first_country_code(country_codes)
    if first_country_code:
        base_payload["countryCode"] = first_country_code
    normalized_selection_mode = _normalize_selection_mode(selection_mode)
    blocked_phones = {str(item or "").strip() for item in phone_blacklist if str(item or "").strip()}
    blocked_provider_country_pairs = _normalize_provider_country_blacklist(provider_country_blacklist)
    country_candidates = _country_code_candidates(country_codes)
    last_error: Exception | None = None
    opened_session_attempts = 0
    provider_country_skip_count = 0

    def _try_provider_candidates(provider_keys: list[str]) -> SmsSession | None:
        nonlocal last_error, opened_session_attempts, provider_country_skip_count
        for provider_key_candidate in provider_keys:
            normalized_provider_key = str(provider_key_candidate or "").strip().lower()
            for country_code_candidate in country_candidates:
                if (
                    normalized_provider_key
                    and country_code_candidate
                    and (normalized_provider_key, country_code_candidate) in blocked_provider_country_pairs
                ):
                    provider_country_skip_count += 1
                    continue
                request_payload = dict(base_payload)
                if country_code_candidate:
                    request_payload["countryCode"] = country_code_candidate
                if normalized_provider_key:
                    request_payload["providerKey"] = normalized_provider_key
                if normalized_selection_mode and normalized_provider_key == "hero_sms":
                    request_payload["selectionMode"] = normalized_selection_mode
                opened_session_attempts += 1
                try:
                    response = _post_json("/sms/sessions/open", request_payload)
                except Exception as exc:
                    last_error = exc
                    if normalized_provider_key and _is_retryable_provider_open_error(exc):
                        continue
                    raise
                session = dict(response.get("session") or (response.get("result") or {}).get("session") or {})
                session_id = str(session.get("id") or "").strip()
                phone_number = str(session.get("phoneNumberE164") or session.get("phoneNumber") or "").strip()
                provider_key = str(session.get("providerKey") or "").strip().lower()
                if session_id and phone_number:
                    if phone_number in blocked_phones:
                        last_error = RuntimeError(f"sms service returned blacklisted phone number: {phone_number}")
                        continue
                    return SmsSession(
                        session_id=session_id,
                        phone_number=phone_number,
                        provider_key=provider_key,
                    )
                last_error = RuntimeError("sms service returned invalid sms session")
        return None

    selected_session = _try_provider_candidates(candidate_provider_keys)
    if selected_session is not None:
        return selected_session
    if selection_candidates and opened_session_attempts == 0 and provider_country_skip_count > 0:
        raise RuntimeError("sms_no_unblocked_provider_country_candidates")
    if selection_candidates:
        fallback_provider_keys = _query_provider_catalog_candidates(
            provider_blacklist=provider_blacklist,
            allow_paid=allow_paid,
            exclude_provider_keys=tuple(selection_candidates),
        )
        fallback_session = _try_provider_candidates(fallback_provider_keys)
        if fallback_session is not None:
            return fallback_session
    if last_error is not None:
        raise last_error
    raise RuntimeError("sms service returned invalid sms session")


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
    normalized_outcome = str(outcome or "").strip().lower()
    success = normalized_outcome in {"success", "ok", "completed"}
    payload: dict[str, Any] = {
        "sessionId": str(session_id or "").strip(),
        "success": success,
        "detail": str(detail or "").strip(),
        "source": "easyregister",
        "observedAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    if not success:
        payload["failureReason"] = normalized_outcome or "failure"
    response = _post_json(
        "/sms/sessions/report-outcome",
        payload,
    )
    return dict(response.get("result") or {})
