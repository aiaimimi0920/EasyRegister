from __future__ import annotations

import json
import os
import threading
import time
from urllib.parse import urlparse

from others.common_io import write_json_atomic
from others.bootstrap import ensure_local_bundle_imports
from others.config_env import resolve_shared_root_from_env
from others.runtime_proxy_env import resolve_easy_proxy_failure_window_seconds

ensure_local_bundle_imports()

from curl_cffi import requests

from shared_proxy import build_request_proxies, env_flag


DEFAULT_EASY_PROXY_PROBE_TIMEOUT_SECONDS = 20
ACTIVE_FLOW_PROXY_LOCK = threading.Lock()
ACTIVE_FLOW_PROXY_URLS: set[str] = set()
RECENT_FLOW_PROXY_URLS: dict[str, float] = {}
FAILED_FLOW_PROXY_URLS: dict[str, float] = {}
FAILED_FLOW_PROXY_STATE_SCHEMA_VERSION = 1


def _is_openai_auth_probe_url(probe_url: str) -> bool:
    try:
        parsed = urlparse(str(probe_url or "").strip())
    except Exception:
        return False
    host = str(parsed.hostname or "").strip().lower()
    path = str(parsed.path or "/").strip().lower() or "/"
    if host == "auth.openai.com":
        return path.startswith(("/log-in-or-create-account", "/authorize", "/login", "/u/login"))
    if host in {"chatgpt.com", "www.chatgpt.com"}:
        return path.startswith("/auth/")
    if host == "platform.openai.com":
        return path.startswith("/login")
    return False


def _is_openai_auth_challenge_probe_response(probe_url: str, status_code: int, body: str) -> bool:
    if int(status_code or 0) != 403:
        return False
    if not _is_openai_auth_probe_url(probe_url):
        return False
    normalized_body = str(body or "").strip().lower()
    if not normalized_body:
        return True
    challenge_markers = (
        "just a moment",
        "cf-mitigated",
        "cloudflare",
        "__cf_chl_",
        "/cdn-cgi/challenge-platform",
    )
    return any(marker in normalized_body for marker in challenge_markers)


def failed_flow_proxy_state_path():
    return resolve_shared_root_from_env() / "others" / "easy-proxy-failed-routes.json"


def read_shared_failed_flow_proxy_urls() -> dict[str, float]:
    try:
        payload = json.loads(failed_flow_proxy_state_path().read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    if int(payload.get("schemaVersion") or 0) != FAILED_FLOW_PROXY_STATE_SCHEMA_VERSION:
        return {}
    raw_failed = payload.get("failed")
    if not isinstance(raw_failed, dict):
        return {}
    failed: dict[str, float] = {}
    for key, value in raw_failed.items():
        if not isinstance(value, dict):
            continue
        try:
            until_epoch = float(value.get("untilEpoch") or 0.0)
        except (TypeError, ValueError):
            continue
        if until_epoch > 0:
            failed[str(key).strip().lower()] = until_epoch
    return failed


def write_shared_failed_flow_proxy_url(normalized: str, *, until_epoch: float) -> None:
    try:
        failed = read_shared_failed_flow_proxy_urls()
        failed[normalized] = max(float(until_epoch), float(failed.get(normalized) or 0.0))
        now_epoch = time.time()
        active_payload = {
            key: {
                "untilEpoch": value,
                "updatedAtEpoch": now_epoch,
            }
            for key, value in failed.items()
            if value > now_epoch
        }
        write_json_atomic(
            failed_flow_proxy_state_path(),
            {
                "schemaVersion": FAILED_FLOW_PROXY_STATE_SCHEMA_VERSION,
                "updatedAtEpoch": now_epoch,
                "failed": active_payload,
            },
            include_pid=True,
            cleanup_temp=True,
        )
    except Exception:
        return


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
    if _is_openai_auth_challenge_probe_response(probe_url, status_code, body_preview):
        return
    raise RuntimeError(f"easy_proxy_probe_failed status={status_code} url={probe_url} body={body_preview}")


def purge_recent_flow_proxy_cache(now_monotonic: float) -> None:
    expired_keys = [key for key, expires_at in RECENT_FLOW_PROXY_URLS.items() if expires_at <= now_monotonic]
    for key in expired_keys:
        RECENT_FLOW_PROXY_URLS.pop(key, None)


def purge_failed_flow_proxy_cache(now_monotonic: float) -> None:
    now_epoch = time.time()
    for key, until_epoch in read_shared_failed_flow_proxy_urls().items():
        remaining = max(0.0, until_epoch - now_epoch)
        if remaining > 0:
            FAILED_FLOW_PROXY_URLS[key] = max(
                float(FAILED_FLOW_PROXY_URLS.get(key) or 0.0),
                now_monotonic + remaining,
            )
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
    until_epoch = time.time() + failure_window_seconds
    with ACTIVE_FLOW_PROXY_LOCK:
        now_monotonic = time.monotonic()
        purge_failed_flow_proxy_cache(now_monotonic)
        FAILED_FLOW_PROXY_URLS[normalized] = now_monotonic + failure_window_seconds
    write_shared_failed_flow_proxy_url(normalized, until_epoch=until_epoch)


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
        "connection closed",
        "connection refused",
        "could not connect to server",
        "failed to connect",
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
