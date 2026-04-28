from __future__ import annotations

import contextlib
import os
import urllib.parse
import uuid
from pathlib import Path
from typing import Iterator

from others.bootstrap import ensure_local_bundle_imports
from others.config import ProxyRuntimeConfig, env_first_text

ensure_local_bundle_imports()

from shared_proxy import env_flag


DEFAULT_ORCHESTRATION_HOST_ID = "python-register-orchestration"
DEFAULT_EASY_PROXY_BASE_URL_HOST = "http://localhost:19888"
DEFAULT_EASY_PROXY_BASE_URL_DOCKER = "http://easy-proxy-service:9888"
DEFAULT_EASY_PROXY_RUNTIME_HOST_DOCKER = "easy-proxy-service"
DEFAULT_EASY_PROXY_TTL_MINUTES = 30
DEFAULT_EASY_PROXY_MODE = "auto"


def _running_in_docker() -> bool:
    if str(os.environ.get("RUNNING_IN_DOCKER") or "").strip():
        return True
    return Path("/.dockerenv").exists()


def _default_easy_proxy_management_base_url() -> str:
    return DEFAULT_EASY_PROXY_BASE_URL_DOCKER if _running_in_docker() else DEFAULT_EASY_PROXY_BASE_URL_HOST


def proxy_runtime_config() -> ProxyRuntimeConfig:
    return ProxyRuntimeConfig.from_env(
        default_management_base_url=_default_easy_proxy_management_base_url(),
        default_ttl_minutes=DEFAULT_EASY_PROXY_TTL_MINUTES,
        default_runtime_host=DEFAULT_EASY_PROXY_RUNTIME_HOST_DOCKER,
        default_mode=DEFAULT_EASY_PROXY_MODE,
        running_in_docker=_running_in_docker(),
    )


def ensure_easy_proxy_env_defaults() -> None:
    proxy_config = proxy_runtime_config()
    management_base = env_first_text("EASY_PROXY_BASE_URL", "EASY_PROXY_MANAGEMENT_URL")
    if not management_base:
        os.environ["EASY_PROXY_BASE_URL"] = proxy_config.management_base_url
    ttl_value = str(os.environ.get("EASY_PROXY_TTL_MINUTES") or "").strip()
    if not ttl_value:
        os.environ["EASY_PROXY_TTL_MINUTES"] = str(proxy_config.ttl_minutes)


def resolve_easy_proxy_runtime_host() -> str:
    return proxy_runtime_config().runtime_host


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


def resolve_easy_proxy_mode() -> str:
    return proxy_runtime_config().mode


def resolve_easy_proxy_unique_attempts() -> int:
    return proxy_runtime_config().unique_attempts


def resolve_easy_proxy_recent_window_seconds() -> int:
    return proxy_runtime_config().recent_window_seconds


def resolve_easy_proxy_failure_window_seconds() -> int:
    return proxy_runtime_config().failure_window_seconds


def default_easy_proxy_service_key(flow_name: str) -> str:
    normalized = str(flow_name or "").strip().lower() or "flow"
    return f"register-orchestration:{normalized}"


def default_easy_proxy_stage(flow_name: str) -> str:
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


def build_easy_proxy_host_id(flow_name: str) -> str:
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
