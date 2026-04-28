from __future__ import annotations

import contextlib
import os
import threading
import time
import urllib.parse
import uuid
from pathlib import Path
from typing import Iterator

from others.bootstrap import ensure_local_bundle_imports
from others.config import ProxyRuntimeConfig, env_first_text

ensure_local_bundle_imports()

from curl_cffi import requests

from shared_proxy import build_request_proxies, env_flag


DEFAULT_ORCHESTRATION_HOST_ID = "python-register-orchestration"
DEFAULT_EASY_PROXY_BASE_URL_HOST = "http://localhost:19888"
DEFAULT_EASY_PROXY_BASE_URL_DOCKER = "http://easy-proxy-service:9888"
DEFAULT_EASY_PROXY_RUNTIME_HOST_DOCKER = "easy-proxy-service"
DEFAULT_EASY_PROXY_TTL_MINUTES = 30
DEFAULT_EASY_PROXY_PROBE_TIMEOUT_SECONDS = 20
DEFAULT_EASY_PROXY_MODE = "auto"
_ACTIVE_FLOW_PROXY_LOCK = threading.Lock()
_ACTIVE_FLOW_PROXY_URLS: set[str] = set()
_RECENT_FLOW_PROXY_URLS: dict[str, float] = {}
_FAILED_FLOW_PROXY_URLS: dict[str, float] = {}


def _running_in_docker() -> bool:
    if str(os.environ.get("RUNNING_IN_DOCKER") or "").strip():
        return True
    return Path("/.dockerenv").exists()


def _default_easy_proxy_management_base_url() -> str:
    return DEFAULT_EASY_PROXY_BASE_URL_DOCKER if _running_in_docker() else DEFAULT_EASY_PROXY_BASE_URL_HOST


def _proxy_runtime_config() -> ProxyRuntimeConfig:
    return ProxyRuntimeConfig.from_env(
        default_management_base_url=_default_easy_proxy_management_base_url(),
        default_ttl_minutes=DEFAULT_EASY_PROXY_TTL_MINUTES,
        default_runtime_host=DEFAULT_EASY_PROXY_RUNTIME_HOST_DOCKER,
        default_mode=DEFAULT_EASY_PROXY_MODE,
        running_in_docker=_running_in_docker(),
    )


def ensure_easy_proxy_env_defaults() -> None:
    proxy_config = _proxy_runtime_config()
    management_base = env_first_text("EASY_PROXY_BASE_URL", "EASY_PROXY_MANAGEMENT_URL")
    if not management_base:
        os.environ["EASY_PROXY_BASE_URL"] = proxy_config.management_base_url
    ttl_value = str(os.environ.get("EASY_PROXY_TTL_MINUTES") or "").strip()
    if not ttl_value:
        os.environ["EASY_PROXY_TTL_MINUTES"] = str(proxy_config.ttl_minutes)


def resolve_easy_proxy_runtime_host() -> str:
    return _proxy_runtime_config().runtime_host


def runtime_reachable_proxy_url(proxy_url: str) -> str:
    raw = str(proxy_url or "").strip()
    if not raw:
        return ""
    try:
        parsed = urllib.parse.urlsplit(raw)
    except Exception:
        return raw
    host = str(parsed.hostname or "").strip().lower()
    runtime_host = resolve_easy_proxy_runtime_host()
    if host not in ("127.0.0.1", "localhost") or not runtime_host:
        return raw
    netloc = runtime_host
    if parsed.port:
        netloc = f"{netloc}:{parsed.port}"
    if parsed.username:
        auth = parsed.username
        if parsed.password:
            auth = f"{auth}:{parsed.password}"
        netloc = f"{auth}@{netloc}"
    return urllib.parse.urlunsplit(
        (
            parsed.scheme or "http",
            netloc,
            parsed.path or "",
            parsed.query or "",
            parsed.fragment or "",
        )
    )


def _probe_flow_proxy(
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


def _resolve_easy_proxy_unique_attempts() -> int:
    return _proxy_runtime_config().unique_attempts


def _resolve_easy_proxy_recent_window_seconds() -> int:
    return _proxy_runtime_config().recent_window_seconds


def _purge_recent_flow_proxy_cache(now_monotonic: float) -> None:
    expired_keys = [key for key, expires_at in _RECENT_FLOW_PROXY_URLS.items() if expires_at <= now_monotonic]
    for key in expired_keys:
        _RECENT_FLOW_PROXY_URLS.pop(key, None)


def _resolve_easy_proxy_failure_window_seconds() -> int:
    return _proxy_runtime_config().failure_window_seconds


def _purge_failed_flow_proxy_cache(now_monotonic: float) -> None:
    expired_keys = [key for key, expires_at in _FAILED_FLOW_PROXY_URLS.items() if expires_at <= now_monotonic]
    for key in expired_keys:
        _FAILED_FLOW_PROXY_URLS.pop(key, None)


def _mark_failed_flow_proxy(unique_key: str) -> None:
    normalized = str(unique_key or "").strip().lower()
    if not normalized:
        return
    failure_window_seconds = _resolve_easy_proxy_failure_window_seconds()
    if failure_window_seconds <= 0:
        return
    with _ACTIVE_FLOW_PROXY_LOCK:
        now_monotonic = time.monotonic()
        _purge_failed_flow_proxy_cache(now_monotonic)
        _FAILED_FLOW_PROXY_URLS[normalized] = now_monotonic + failure_window_seconds


def _resolve_easy_proxy_mode() -> str:
    return _proxy_runtime_config().mode


def _default_easy_proxy_service_key(flow_name: str) -> str:
    normalized = str(flow_name or "").strip().lower() or "flow"
    return f"register-orchestration:{normalized}"


def _default_easy_proxy_stage(flow_name: str) -> str:
    mapping = {
        "create_openai_account": "registration",
        "codex_openai_account_task": "registration",
        "obtain_codex_oauth": "oauth",
        "invite": "invite",
        "revoke": "revoke",
        "team_auth_refresh": "auth_refresh",
        "invite_codex_member": "invite",
        "revoke_codex_member": "revoke",
    }
    normalized = str(flow_name or "").strip().lower()
    return mapping.get(normalized, normalized or "request")


def _classify_easy_proxy_error(exc: Exception, *, probe_url: str | None = None) -> tuple[str, str, str]:
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


def _build_easy_proxy_host_id(flow_name: str) -> str:
    base = str(
        os.environ.get("REGISTER_PROXY_HOST_ID")
        or os.environ.get("EASY_PROXY_HOST_ID")
        or DEFAULT_ORCHESTRATION_HOST_ID
    ).strip() or DEFAULT_ORCHESTRATION_HOST_ID
    return f"{base}-{str(flow_name or 'flow').strip().lower()}-{uuid.uuid4().hex[:8]}"


@contextlib.contextmanager
def without_proxy_env() -> Iterator[None]:
    keys = (
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "NO_PROXY",
        "no_proxy",
    )
    snapshot = {key: os.environ.get(key) for key in keys}
    try:
        for key in keys:
            os.environ.pop(key, None)
        os.environ["NO_PROXY"] = "*"
        os.environ["no_proxy"] = "*"
        yield
    finally:
        for key, value in snapshot.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


@contextlib.contextmanager
def flow_network_env() -> Iterator[None]:
    if env_flag("REGISTER_ENABLE_EASY_PROXY", True):
        yield
        return
    with without_proxy_env():
        yield


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
