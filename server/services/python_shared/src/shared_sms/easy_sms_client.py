from __future__ import annotations

import ipaddress
import json
import os
import re
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
DEFAULT_SMS_SERVICE_OPEN_TIMEOUT_SECONDS = 120
DEFAULT_SMS_SERVICE_SELECTION_PLAN_TIMEOUT_SECONDS = 8
DEFAULT_SMS_SERVICE_SELECTION_PLAN_ATTEMPTS = 1
SUPPORTED_SMS_SELECTION_MODES = {
    "price-first",
    "success-first",
    "stock-first",
    "balanced",
}
UNPRODUCTIVE_SELECTION_HEALTH_STATES = {
    "empty",
    "challenge",
    "blocked",
}


@dataclass(frozen=True)
class SmsSession:
    session_id: str
    phone_number: str
    provider_key: str


@dataclass(frozen=True)
class _SmsProviderCountryCandidate:
    provider_key: str
    country_code: str


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


def _sms_service_open_timeout_seconds() -> int:
    raw = str(
        os.environ.get("SMS_SERVICE_OPEN_TIMEOUT_SECONDS")
        or DEFAULT_SMS_SERVICE_OPEN_TIMEOUT_SECONDS
    ).strip()
    try:
        return max(30, int(float(raw)))
    except Exception:
        return DEFAULT_SMS_SERVICE_OPEN_TIMEOUT_SECONDS


def _sms_service_selection_plan_timeout_seconds() -> int:
    raw = str(
        os.environ.get("SMS_SERVICE_SELECTION_PLAN_TIMEOUT_SECONDS")
        or DEFAULT_SMS_SERVICE_SELECTION_PLAN_TIMEOUT_SECONDS
    ).strip()
    try:
        return max(2, int(float(raw)))
    except Exception:
        return DEFAULT_SMS_SERVICE_SELECTION_PLAN_TIMEOUT_SECONDS


def _sms_service_selection_plan_attempts() -> int:
    raw = str(
        os.environ.get("SMS_SERVICE_SELECTION_PLAN_ATTEMPTS")
        or DEFAULT_SMS_SERVICE_SELECTION_PLAN_ATTEMPTS
    ).strip()
    try:
        return max(1, int(float(raw)))
    except Exception:
        return DEFAULT_SMS_SERVICE_SELECTION_PLAN_ATTEMPTS


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
    timeout_seconds = (
        _sms_service_open_timeout_seconds()
        if path == "/sms/sessions/open"
        else 30
    )
    return _sms_service_request(
        method="POST",
        path=path,
        payload=payload,
        timeout_seconds=timeout_seconds,
    )


def _get_json(path: str) -> dict[str, Any]:
    if path.startswith("/sms/query/providers/selection-plan?"):
        return _sms_service_request(
            method="GET",
            path=path,
            timeout_seconds=_sms_service_selection_plan_timeout_seconds(),
            attempts=_sms_service_selection_plan_attempts(),
        )
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
    if isinstance(exc, (TimeoutError, socket.timeout, urllib.error.URLError)):
        return True
    normalized = str(exc or "").strip().lower()
    return any(
        token in normalized
        for token in (
            "currently unavailable",
            "no eligible public numbers",
            "no available public numbers",
            "empty directory response",
            "provider temporarily unavailable",
            "capacity unavailable",
            "sms_capacity_unavailable",
            "timed out",
            "timeout",
        )
    )


def _is_transient_selection_plan_error(exc: Exception) -> bool:
    if _is_transient_sms_service_error(exc, path="/sms/query/providers/selection-plan"):
        return True
    normalized = str(exc or "").strip().lower()
    return any(
        token in normalized
        for token in (
            "http 502",
            "http 503",
            "http 504",
            "temporarily unavailable",
            "upstream transient",
            "capacity unavailable",
            "timed out",
            "timeout",
        )
    )


def _is_missing_sms_session_report_error(exc: Exception) -> bool:
    normalized = str(exc or "").strip().lower()
    return "http 404" in normalized and "sms session not found" in normalized


def _query_provider_selection_candidates_with_seen(
    *,
    provider_blacklist: tuple[str, ...],
    allow_paid: bool,
    country_codes: tuple[str, ...],
    allow_reuse: bool | None = None,
    phone_blacklist: tuple[str, ...] = (),
    provider_phone_blacklist: tuple[str, ...] = (),
) -> tuple[list[str], set[str], set[str]]:
    provider_country_candidates, seen_provider_keys, unavailable_provider_keys = (
        _query_provider_country_selection_candidates_with_seen(
            provider_blacklist=provider_blacklist,
            allow_paid=allow_paid,
            country_codes=country_codes,
            allow_reuse=allow_reuse,
            phone_blacklist=phone_blacklist,
            provider_phone_blacklist=provider_phone_blacklist,
        )
    )
    return (
        _dedupe_provider_keys(provider_country_candidates),
        seen_provider_keys,
        unavailable_provider_keys,
    )


def _query_provider_country_selection_candidates_with_seen(
    *,
    provider_blacklist: tuple[str, ...],
    allow_paid: bool,
    country_codes: tuple[str, ...],
    allow_reuse: bool | None = None,
    phone_blacklist: tuple[str, ...] = (),
    provider_phone_blacklist: tuple[str, ...] = (),
) -> tuple[list[_SmsProviderCountryCandidate], set[str], set[str]]:
    query = {
        "costTier": "paid" if allow_paid else "free",
        "limit": "20",
    }
    if allow_reuse is not None:
        query["allowReuse"] = "true" if allow_reuse else "false"
    normalized_phone_blacklist = [
        str(item or "").strip()
        for item in phone_blacklist
        if str(item or "").strip()
    ]
    if normalized_phone_blacklist:
        query["phoneBlacklist"] = ",".join(normalized_phone_blacklist)
    normalized_provider_phone_blacklist = [
        str(item or "").strip()
        for item in provider_phone_blacklist
        if str(item or "").strip()
    ]
    if normalized_provider_phone_blacklist:
        query["providerPhoneBlacklist"] = ",".join(normalized_provider_phone_blacklist)
    country_candidates = _country_code_candidates(country_codes)
    candidates: list[_SmsProviderCountryCandidate] = []
    seen_provider_keys: set[str] = set()
    unavailable_provider_keys: set[str] = set()
    blacklist = {str(item or "").strip().lower() for item in provider_blacklist if str(item or "").strip()}

    for country_code in country_candidates:
        scoped_query = dict(query)
        if country_code:
            scoped_query["countryCode"] = country_code
        selection_plan_path = "/sms/query/providers/selection-plan?" + urllib.parse.urlencode(scoped_query)
        try:
            plan_response = _get_json(selection_plan_path)
        except Exception as exc:
            if _is_transient_selection_plan_error(exc):
                continue
            raise
        if "candidates" not in plan_response:
            catalog_candidates = _query_provider_catalog_candidates(
                provider_blacklist=provider_blacklist,
                allow_paid=allow_paid,
            )
            return (
                [
                    _SmsProviderCountryCandidate(
                        provider_key=provider_key,
                        country_code=country_code,
                    )
                    for provider_key in catalog_candidates
                ],
                seen_provider_keys,
                unavailable_provider_keys,
            )
        raw_candidates = plan_response.get("candidates") or []
        for raw_candidate in raw_candidates:
            if not isinstance(raw_candidate, dict):
                continue
            provider_key = str(raw_candidate.get("providerKey") or "").strip().lower()
            if not provider_key:
                continue
            seen_provider_keys.add(provider_key)
            if raw_candidate.get("available") is False:
                unavailable_provider_keys.add(provider_key)
            if provider_key in blacklist:
                continue
            if raw_candidate.get("available") is False:
                continue
            health_state = str(raw_candidate.get("healthState") or "").strip().lower()
            if health_state in UNPRODUCTIVE_SELECTION_HEALTH_STATES:
                continue
            candidates.append(
                _SmsProviderCountryCandidate(
                    provider_key=provider_key,
                    country_code=country_code,
                )
            )
    return candidates, seen_provider_keys, unavailable_provider_keys


def _dedupe_provider_keys(candidates: list[_SmsProviderCountryCandidate]) -> list[str]:
    provider_keys: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate.provider_key in seen:
            continue
        provider_keys.append(candidate.provider_key)
        seen.add(candidate.provider_key)
    return provider_keys


def _query_provider_selection_candidates(
    *,
    provider_blacklist: tuple[str, ...],
    allow_paid: bool,
    country_codes: tuple[str, ...],
    allow_reuse: bool | None = None,
    phone_blacklist: tuple[str, ...] = (),
    provider_phone_blacklist: tuple[str, ...] = (),
) -> list[str]:
    candidates, _seen_provider_keys, _unavailable_provider_keys = _query_provider_selection_candidates_with_seen(
        provider_blacklist=provider_blacklist,
        allow_paid=allow_paid,
        country_codes=country_codes,
        allow_reuse=allow_reuse,
        phone_blacklist=phone_blacklist,
        provider_phone_blacklist=provider_phone_blacklist,
    )
    return candidates


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


def _phone_lookup_keys(phone_number: str) -> set[str]:
    text = str(phone_number or "").strip()
    if not text:
        return set()
    compact = re.sub(r"[^\d+]", "", text)
    digits_only = re.sub(r"\D", "", text)
    keys = {text}
    if compact:
        keys.add(compact)
    if digits_only:
        keys.add(digits_only)
    return keys


def _normalize_provider_phone_blacklist(items: tuple[str, ...]) -> dict[str, set[str]]:
    blocked: dict[str, set[str]] = {}
    for item in items:
        raw = str(item or "").strip()
        if not raw or "|" not in raw:
            continue
        provider_key, phone_number = raw.split("|", 1)
        normalized_provider_key = provider_key.strip().lower()
        phone_keys = _phone_lookup_keys(phone_number)
        if normalized_provider_key and phone_keys:
            blocked.setdefault(normalized_provider_key, set()).update(phone_keys)
    return blocked


def _is_provider_phone_blacklisted(
    *,
    provider_key: str,
    phone_number: str,
    blocked_provider_phone_lookup: dict[str, set[str]],
) -> bool:
    normalized_provider_key = str(provider_key or "").strip().lower()
    if not normalized_provider_key:
        return False
    blocked_phone_keys = blocked_provider_phone_lookup.get(normalized_provider_key)
    if not blocked_phone_keys:
        return False
    return bool(_phone_lookup_keys(phone_number) & blocked_phone_keys)


def _safe_report_rejected_sms_session(*, session_id: str, detail: str) -> None:
    if not str(session_id or "").strip():
        return
    try:
        report_sms_outcome(
            session_id=session_id,
            outcome="failure",
            detail=detail,
        )
    except Exception:
        return


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
    provider_phone_blacklist: tuple[str, ...] = (),
    provider_country_blacklist: tuple[str, ...] = (),
) -> SmsSession:
    _wait_sms_service_ready()
    (
        selection_plan_provider_country_candidates,
        selection_plan_seen_provider_keys,
        selection_plan_unavailable_provider_keys,
    ) = _query_provider_country_selection_candidates_with_seen(
        provider_blacklist=provider_blacklist,
        allow_paid=allow_paid,
        country_codes=country_codes,
        allow_reuse=allow_reuse,
        phone_blacklist=phone_blacklist,
        provider_phone_blacklist=provider_phone_blacklist,
    )
    base_payload: dict[str, Any] = {
        "businessKey": str(business_key or "").strip(),
        "costTier": "paid" if allow_paid else "free",
        "allowReuse": bool(allow_reuse),
        "maxBindingsPerPhone": max(1, int(max_bindings_per_phone or 1)),
    }
    candidate_provider_keys = _dedupe_provider_keys(selection_plan_provider_country_candidates)
    if not candidate_provider_keys:
        candidate_provider_keys = _query_provider_catalog_candidates(
            provider_blacklist=provider_blacklist,
            allow_paid=allow_paid,
            exclude_provider_keys=tuple(sorted(selection_plan_seen_provider_keys)),
        )
        if not candidate_provider_keys and selection_plan_seen_provider_keys:
            candidate_provider_keys = _query_provider_catalog_candidates(
                provider_blacklist=provider_blacklist,
                allow_paid=allow_paid,
                exclude_provider_keys=tuple(sorted(selection_plan_unavailable_provider_keys)),
            )
    if not candidate_provider_keys:
        raise RuntimeError("sms_no_selection_plan_candidates")
    first_country_code = _first_country_code(country_codes)
    if first_country_code:
        base_payload["countryCode"] = first_country_code
    normalized_selection_mode = _normalize_selection_mode(selection_mode)
    blocked_phones = {
        key
        for item in phone_blacklist
        for key in _phone_lookup_keys(str(item or ""))
        if key
    }
    normalized_provider_phone_blacklist = tuple(
        str(item or "").strip()
        for item in provider_phone_blacklist
        if str(item or "").strip()
    )
    blocked_provider_phone_lookup = _normalize_provider_phone_blacklist(normalized_provider_phone_blacklist)
    blocked_provider_country_pairs = _normalize_provider_country_blacklist(provider_country_blacklist)
    country_candidates = _country_code_candidates(country_codes)
    last_error: Exception | None = None
    opened_session_attempts = 0
    provider_country_skip_count = 0

    def _try_provider_country_candidates(candidates: list[_SmsProviderCountryCandidate]) -> SmsSession | None:
        nonlocal last_error, opened_session_attempts, provider_country_skip_count
        for candidate in candidates:
            normalized_provider_key = str(candidate.provider_key or "").strip().lower()
            country_code_candidate = str(candidate.country_code or "").strip()
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
            if blocked_phones:
                request_payload["phoneBlacklist"] = list(phone_blacklist)
            if normalized_provider_phone_blacklist:
                request_payload["providerPhoneBlacklist"] = list(normalized_provider_phone_blacklist)
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
            effective_provider_key = provider_key or normalized_provider_key
            if session_id and phone_number:
                if _phone_lookup_keys(phone_number) & blocked_phones:
                    _safe_report_rejected_sms_session(
                        session_id=session_id,
                        detail="blacklisted_phone_number",
                    )
                    last_error = RuntimeError(f"sms service returned blacklisted phone number: {phone_number}")
                    continue
                if _is_provider_phone_blacklisted(
                    provider_key=effective_provider_key,
                    phone_number=phone_number,
                    blocked_provider_phone_lookup=blocked_provider_phone_lookup,
                ):
                    _safe_report_rejected_sms_session(
                        session_id=session_id,
                        detail="blacklisted_provider_phone_number",
                    )
                    last_error = RuntimeError(
                        f"sms service returned blacklisted provider phone number: {effective_provider_key}"
                    )
                    continue
                return SmsSession(
                    session_id=session_id,
                    phone_number=phone_number,
                    provider_key=effective_provider_key,
                )
            last_error = RuntimeError("sms service returned invalid sms session")
        return None

    def _provider_country_candidates_from_provider_keys(provider_keys: list[str]) -> list[_SmsProviderCountryCandidate]:
        return [
            _SmsProviderCountryCandidate(
                provider_key=str(provider_key or "").strip().lower(),
                country_code=country_code,
            )
            for provider_key in provider_keys
            for country_code in country_candidates
        ]

    initial_provider_country_candidates = (
        selection_plan_provider_country_candidates
        or _provider_country_candidates_from_provider_keys(candidate_provider_keys)
    )
    selected_session = _try_provider_country_candidates(initial_provider_country_candidates)
    if selected_session is not None:
        return selected_session
    if selection_plan_provider_country_candidates and opened_session_attempts == 0 and provider_country_skip_count > 0:
        raise RuntimeError("sms_no_unblocked_provider_country_candidates")
    if selection_plan_provider_country_candidates:
        fallback_provider_keys = _query_provider_catalog_candidates(
            provider_blacklist=provider_blacklist,
            allow_paid=allow_paid,
            exclude_provider_keys=tuple(
                sorted(set(candidate_provider_keys) | selection_plan_seen_provider_keys)
            ),
        )
        if not fallback_provider_keys:
            fallback_provider_keys = _query_provider_catalog_candidates(
                provider_blacklist=tuple(sorted(set(provider_blacklist) | set(candidate_provider_keys))),
                allow_paid=allow_paid,
                exclude_provider_keys=tuple(sorted(selection_plan_unavailable_provider_keys)),
            )
        if fallback_provider_keys:
            selected_session = _try_provider_country_candidates(
                _provider_country_candidates_from_provider_keys(fallback_provider_keys)
            )
            if selected_session is not None:
                return selected_session
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
    try:
        response = _post_json(
            "/sms/sessions/report-outcome",
            payload,
        )
    except Exception as exc:
        if _is_missing_sms_session_report_error(exc):
            return {
                "accepted": False,
                "ignored": True,
                "reason": "sms_session_not_found",
            }
        raise
    return dict(response.get("result") or {})
