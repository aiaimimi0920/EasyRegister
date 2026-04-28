from __future__ import annotations

import os
import threading
import time

from others.bootstrap import ensure_local_bundle_imports
from others.runtime_proxy_env import resolve_easy_proxy_failure_window_seconds

ensure_local_bundle_imports()

from curl_cffi import requests

from shared_proxy import build_request_proxies, env_flag


DEFAULT_EASY_PROXY_PROBE_TIMEOUT_SECONDS = 20
ACTIVE_FLOW_PROXY_LOCK = threading.Lock()
ACTIVE_FLOW_PROXY_URLS: set[str] = set()
RECENT_FLOW_PROXY_URLS: dict[str, float] = {}
FAILED_FLOW_PROXY_URLS: dict[str, float] = {}


def probe_flow_proxy(
    *,
    proxy_url: str,
    probe_url: str,
    expected_statuses: set[int] | None,
) -> None:
    verify_tls = env_flag("PROTOCOL_HTTP_VERIFY_TLS", False)
    impersonate = (os.environ.get("PROTOCOL_HTTP_IMPERSONATE") or "chrome").strip() or "chrome"
    session = requests.Session(
        impersonate=impersonate,
        timeout=DEFAULT_EASY_PROXY_PROBE_TIMEOUT_SECONDS,
        verify=verify_tls,
    )
    session.headers.update(
        {
            "user-agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36 Edg/146.0.0.0"
            ),
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "accept-language": "en-US,en;q=0.9",
        }
    )
    try:
        response = session.get(
            probe_url,
            allow_redirects=False,
            proxies=build_request_proxies(proxy_url),
        )
    finally:
        try:
            session.close()
        except Exception:
            pass
    status_code = int(getattr(response, "status_code", 0) or 0)
    accepted = expected_statuses or {200}
    if status_code in accepted:
        return
    body_preview = str(getattr(response, "text", "") or "")[:180]
    raise RuntimeError(f"easy_proxy_probe_failed status={status_code} url={probe_url} body={body_preview}")


def purge_recent_flow_proxy_cache(now_monotonic: float) -> None:
    expired_keys = [key for key, expires_at in RECENT_FLOW_PROXY_URLS.items() if expires_at <= now_monotonic]
    for key in expired_keys:
        RECENT_FLOW_PROXY_URLS.pop(key, None)


def purge_failed_flow_proxy_cache(now_monotonic: float) -> None:
    expired_keys = [key for key, expires_at in FAILED_FLOW_PROXY_URLS.items() if expires_at <= now_monotonic]
    for key in expired_keys:
        FAILED_FLOW_PROXY_URLS.pop(key, None)


def mark_failed_flow_proxy(unique_key: str) -> None:
    normalized = str(unique_key or "").strip().lower()
    if not normalized:
        return
    failure_window_seconds = resolve_easy_proxy_failure_window_seconds()
    if failure_window_seconds <= 0:
        return
    with ACTIVE_FLOW_PROXY_LOCK:
        now_monotonic = time.monotonic()
        purge_failed_flow_proxy_cache(now_monotonic)
        FAILED_FLOW_PROXY_URLS[normalized] = now_monotonic + failure_window_seconds


def classify_easy_proxy_error(exc: Exception, *, probe_url: str | None = None) -> tuple[str, str, str]:
    message = str(exc or "").strip()
    normalized = message.lower()
    error_code = message or type(exc).__name__
    if "user_register status=400" in normalized or "failed to create account. please try again." in normalized:
        return ("openai_user_register_400", "route_failure", "medium")
    if "easy_proxy_probe_failed" in normalized:
        if "status=403" in normalized or "status=407" in normalized:
            target = str(probe_url or "").strip()
            return (
                f"proxy route failure blocked {target or 'probe'}",
                "route_failure",
                "high",
            )
        if "status=429" in normalized or "status=502" in normalized or "status=503" in normalized:
            return (error_code, "route_failure", "medium")
    route_markers = (
        "timeout",
        "tls",
        "connection reset",
        "connection refused",
        "network unreachable",
        "proxy route failure",
        "econnreset",
        "remote end closed",
        "unexpected eof",
    )
    if any(marker in normalized for marker in route_markers):
        return (error_code, "route_failure", "high")
    if "duplicate_active_route" in normalized or "recent_route_reuse" in normalized:
        return (error_code, "", "")
    return (error_code, "unknown", "low")


def seed_device_cookie(session: requests.Session, device_id: str) -> None:
    for domain in (
        ".openai.com",
        "openai.com",
        "platform.openai.com",
        ".auth.openai.com",
        "auth.openai.com",
    ):
        try:
            session.cookies.set("oai-did", device_id, domain=domain)
        except Exception:
            continue
