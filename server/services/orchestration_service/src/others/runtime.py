from __future__ import annotations

import contextlib
import json
import os
import random
import threading
import time
import urllib.parse
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

if __package__ in (None, "", "others"):
    import sys
    from pathlib import Path

    _CURRENT_DIR = Path(__file__).resolve().parent
    _PARENT_DIR = _CURRENT_DIR.parent
    for _candidate in (_CURRENT_DIR, _PARENT_DIR):
        candidate_text = str(_candidate)
        if candidate_text not in sys.path:
            sys.path.append(candidate_text)
    from bootstrap import ensure_local_bundle_imports
    from local_config import read_easyemail_server_api_key
    from errors import ensure_protocol_runtime_error
else:
    from .bootstrap import ensure_local_bundle_imports
    from .local_config import read_easyemail_server_api_key
    from ..errors import ensure_protocol_runtime_error

ensure_local_bundle_imports()

from curl_cffi import requests

from shared_mailbox.easy_email_client import Mailbox, create_mailbox, plan_mailbox
from shared_proxy import build_request_proxies, env_flag, mask_proxy_url
from shared_proxy.easy_proxy_client import (
    checkout_proxy,
    checkout_random_node_proxy,
    release_lease,
    report_usage,
)

DEFAULT_ORCHESTRATION_HOST_ID = "python-register-orchestration"
DEFAULT_EASY_EMAIL_BASE_URL = "http://localhost:18080"
DEFAULT_MAILBOX_TTL_SECONDS = 90
DEFAULT_REGISTER_MOEMAIL_DOMAIN_POOL = (
    "sall.cc",
    "cnmlgb.de",
    "zhooo.org",
    "cksa.eu.cc",
    "wqwq.eu.cc",
    "zhoo.eu.cc",
    "zhooo.ggff.net",
    "coolkidsa.ggff.net",
)
DEFAULT_REGISTER_MAILBOX_DOMAIN_BLACKLIST_MIN_ATTEMPTS = 20
DEFAULT_REGISTER_MAILBOX_DOMAIN_BLACKLIST_FAILURE_RATE = 90.0
DEFAULT_EASY_PROXY_BASE_URL_HOST = "http://localhost:19888"
DEFAULT_EASY_PROXY_BASE_URL_DOCKER = "http://easy-proxy-service:9888"
DEFAULT_EASY_PROXY_RUNTIME_HOST_DOCKER = "easy-proxy-service"
DEFAULT_EASY_PROXY_TTL_MINUTES = 30
DEFAULT_EASY_PROXY_UNIQUE_ATTEMPTS = 6
DEFAULT_EASY_PROXY_PROBE_TIMEOUT_SECONDS = 20
DEFAULT_EASY_PROXY_MODE = "auto"
_ACTIVE_FLOW_PROXY_LOCK = threading.Lock()
_ACTIVE_FLOW_PROXY_URLS: set[str] = set()
_RECENT_FLOW_PROXY_URLS: dict[str, float] = {}
_FAILED_FLOW_PROXY_URLS: dict[str, float] = {}


@dataclass
class FlowProxyLease:
    flow_name: str
    proxy_url: str
    raw_proxy_url: str
    lease_id: str
    host_id: str
    management_base_url: str
    unique_key: str
    started_monotonic: float
    service_key: str = ""
    stage: str = ""
    acquisition_mode: str = ""
    checked_out: bool = False
    _success: bool = False
    _error_code: str = ""
    _failure_class: str = ""
    _route_confidence: str = ""
    _finalized: bool = False

    def to_payload(self) -> dict[str, Any]:
        return {
            "flow_name": self.flow_name,
            "proxy_url": self.proxy_url,
            "raw_proxy_url": self.raw_proxy_url,
            "lease_id": self.lease_id,
            "host_id": self.host_id,
            "management_base_url": self.management_base_url,
            "unique_key": self.unique_key,
            "started_monotonic": float(self.started_monotonic or 0.0),
            "service_key": self.service_key,
            "stage": self.stage,
            "acquisition_mode": self.acquisition_mode,
            "checked_out": bool(self.checked_out),
        }

    def mark_success(self) -> None:
        self._success = True
        self._error_code = ""
        self._failure_class = ""
        self._route_confidence = ""

    def mark_error(
        self,
        error_code: str | None,
        *,
        failure_class: str = "",
        route_confidence: str = "",
    ) -> None:
        self._success = False
        normalized = str(error_code or "").strip()
        self._error_code = normalized or "flow_error"
        self._failure_class = str(failure_class or "").strip()
        self._route_confidence = str(route_confidence or "").strip()

    def finalize(self) -> None:
        if self._finalized:
            return
        self._finalized = True
        latency_ms = max(0, int((time.monotonic() - self.started_monotonic) * 1000))
        if self.checked_out and self.lease_id:
            report_usage(
                self.lease_id,
                success=self._success,
                latency_ms=latency_ms,
                error_code="" if self._success else self._error_code,
                service_key=self.service_key,
                stage=self.stage,
                failure_class="" if self._success else self._failure_class,
                route_confidence="" if self._success else self._route_confidence,
                base_url=self.management_base_url,
                api_key=str(os.environ.get("EASY_PROXY_API_KEY") or "").strip(),
            )
            release_lease(
                self.lease_id,
                base_url=self.management_base_url,
                api_key=str(os.environ.get("EASY_PROXY_API_KEY") or "").strip(),
            )
        if self.unique_key:
            with _ACTIVE_FLOW_PROXY_LOCK:
                _ACTIVE_FLOW_PROXY_URLS.discard(self.unique_key)
                recent_window_seconds = _resolve_easy_proxy_recent_window_seconds()
                if self._success and recent_window_seconds > 0:
                    _RECENT_FLOW_PROXY_URLS[self.unique_key] = time.monotonic() + recent_window_seconds

    @classmethod
    def direct(cls, *, flow_name: str) -> "FlowProxyLease":
        return cls(
            flow_name=flow_name,
            proxy_url="",
            raw_proxy_url="",
            lease_id="",
            host_id="",
            management_base_url="",
            unique_key="",
            started_monotonic=time.monotonic(),
            service_key="",
            stage="",
            acquisition_mode="direct",
            checked_out=False,
        )

    @classmethod
    def from_payload(cls, payload: dict[str, Any] | None) -> "FlowProxyLease":
        data = payload if isinstance(payload, dict) else {}
        return cls(
            flow_name=str(data.get("flow_name") or "").strip(),
            proxy_url=str(data.get("proxy_url") or "").strip(),
            raw_proxy_url=str(data.get("raw_proxy_url") or "").strip(),
            lease_id=str(data.get("lease_id") or "").strip(),
            host_id=str(data.get("host_id") or "").strip(),
            management_base_url=str(data.get("management_base_url") or "").strip(),
            unique_key=str(data.get("unique_key") or "").strip(),
            started_monotonic=float(data.get("started_monotonic") or 0.0),
            service_key=str(data.get("service_key") or "").strip(),
            stage=str(data.get("stage") or "").strip(),
            acquisition_mode=str(data.get("acquisition_mode") or "").strip(),
            checked_out=bool(data.get("checked_out")),
        )


def ensure_easy_email_env_defaults() -> None:
    base_url = str(os.environ.get("MAILBOX_SERVICE_BASE_URL") or "").strip()
    if not base_url:
        os.environ["MAILBOX_SERVICE_BASE_URL"] = DEFAULT_EASY_EMAIL_BASE_URL
    api_key = str(os.environ.get("MAILBOX_SERVICE_API_KEY") or "").strip()
    if not api_key:
        discovered_api_key = read_easyemail_server_api_key()
        if discovered_api_key:
            os.environ["MAILBOX_SERVICE_API_KEY"] = discovered_api_key


def _running_in_docker() -> bool:
    if str(os.environ.get("RUNNING_IN_DOCKER") or "").strip():
        return True
    return Path("/.dockerenv").exists()


def ensure_easy_proxy_env_defaults() -> None:
    management_base = str(
        os.environ.get("EASY_PROXY_BASE_URL")
        or os.environ.get("EASY_PROXY_MANAGEMENT_URL")
        or ""
    ).strip()
    if not management_base:
        os.environ["EASY_PROXY_BASE_URL"] = (
            DEFAULT_EASY_PROXY_BASE_URL_DOCKER if _running_in_docker() else DEFAULT_EASY_PROXY_BASE_URL_HOST
        )
    ttl_value = str(os.environ.get("EASY_PROXY_TTL_MINUTES") or "").strip()
    if not ttl_value:
        os.environ["EASY_PROXY_TTL_MINUTES"] = str(DEFAULT_EASY_PROXY_TTL_MINUTES)


def resolve_easy_proxy_runtime_host() -> str:
    runtime_host = str(os.environ.get("EASY_PROXY_RUNTIME_HOST") or "").strip()
    if runtime_host:
        return runtime_host
    # The acquired proxy URL is ultimately consumed by EasyProtocol /
    # PythonProtocol executors, which normally live on the Easy docker network
    # even when the orchestration process itself runs on the host.
    return DEFAULT_EASY_PROXY_RUNTIME_HOST_DOCKER


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
    raw = str(os.environ.get("REGISTER_PROXY_UNIQUE_ATTEMPTS") or "").strip()
    try:
        return max(1, int(raw or DEFAULT_EASY_PROXY_UNIQUE_ATTEMPTS))
    except Exception:
        return DEFAULT_EASY_PROXY_UNIQUE_ATTEMPTS


def _resolve_easy_proxy_recent_window_seconds() -> int:
    raw = str(os.environ.get("REGISTER_PROXY_RECENT_WINDOW_SECONDS") or "").strip()
    try:
        return max(0, int(raw or "180"))
    except Exception:
        return 180


def _purge_recent_flow_proxy_cache(now_monotonic: float) -> None:
    expired_keys = [key for key, expires_at in _RECENT_FLOW_PROXY_URLS.items() if expires_at <= now_monotonic]
    for key in expired_keys:
        _RECENT_FLOW_PROXY_URLS.pop(key, None)


def _resolve_easy_proxy_failure_window_seconds() -> int:
    raw = str(os.environ.get("REGISTER_PROXY_FAILURE_WINDOW_SECONDS") or "").strip()
    try:
        return max(0, int(raw or "300"))
    except Exception:
        return 300


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


def _resolve_easy_proxy_ttl_minutes() -> int:
    raw = str(os.environ.get("REGISTER_PROXY_TTL_MINUTES") or "").strip()
    try:
        return max(1, int(raw or os.environ.get("EASY_PROXY_TTL_MINUTES") or DEFAULT_EASY_PROXY_TTL_MINUTES))
    except Exception:
        return DEFAULT_EASY_PROXY_TTL_MINUTES


def _resolve_mailbox_ttl_seconds() -> int:
    raw = str(
        os.environ.get("REGISTER_MAILBOX_TTL_SECONDS")
        or ""
    ).strip()
    try:
        return max(1, int(float(raw or DEFAULT_MAILBOX_TTL_SECONDS)))
    except Exception:
        return DEFAULT_MAILBOX_TTL_SECONDS

def _resolve_easy_proxy_mode() -> str:
    raw = str(os.environ.get("REGISTER_PROXY_MODE") or "").strip().lower()
    if raw in {"lease", "compat"}:
        return "lease"
    if raw in {"random", "random-node", "random_node"}:
        return "random-node"
    return DEFAULT_EASY_PROXY_MODE


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


def acquire_flow_proxy_lease(
    *,
    flow_name: str,
    metadata: dict[str, Any] | None = None,
    required: bool | None = None,
    probe_url: str | None = None,
    probe_expected_statuses: set[int] | None = None,
) -> FlowProxyLease:
    enabled = env_flag("REGISTER_ENABLE_EASY_PROXY", True)
    required = env_flag("REGISTER_REQUIRE_EASY_PROXY", True) if required is None else bool(required)
    if not enabled:
        return FlowProxyLease.direct(flow_name=flow_name)

    ensure_easy_proxy_env_defaults()
    management_base = str(os.environ.get("EASY_PROXY_BASE_URL") or "").strip()
    api_key = str(os.environ.get("EASY_PROXY_API_KEY") or "").strip()
    ttl_minutes = _resolve_easy_proxy_ttl_minutes()
    mode = _resolve_easy_proxy_mode()
    service_key = _default_easy_proxy_service_key(flow_name)
    stage = _default_easy_proxy_stage(flow_name)
    lease: FlowProxyLease | None = None
    last_error: Exception | None = None
    host_id = ""
    metadata_text = {
        str(key): str(value)
        for key, value in (metadata or {}).items()
        if str(key or "").strip() and str(value or "").strip()
    }
    metadata_text.setdefault("source", DEFAULT_ORCHESTRATION_HOST_ID)
    metadata_text.setdefault("flow", str(flow_name or "").strip() or "flow")
    metadata_text.setdefault("pid", str(os.getpid()))
    metadata_text.setdefault("serviceKey", service_key)
    metadata_text.setdefault("stage", stage)
    metadata_text.setdefault("avoidRecentSuccessReuse", "true")
    metadata_text.setdefault("recentSuccessReuseThreshold", "1")
    metadata_text.setdefault("recentSuccessReuseWindowMinutes", "30")

    unique_attempts = _resolve_easy_proxy_unique_attempts()
    tried_random = False

    def _try_random_nodes() -> FlowProxyLease | None:
        nonlocal last_error, tried_random
        tried_random = True
        attempted_proxy_urls: set[str] = set()
        for attempt in range(unique_attempts):
            candidate = None
            try:
                with _ACTIVE_FLOW_PROXY_LOCK:
                    _purge_recent_flow_proxy_cache(time.monotonic())
                    _purge_failed_flow_proxy_cache(time.monotonic())
                    excluded = (
                        set(_ACTIVE_FLOW_PROXY_URLS)
                        | set(_RECENT_FLOW_PROXY_URLS.keys())
                        | set(_FAILED_FLOW_PROXY_URLS.keys())
                        | set(attempted_proxy_urls)
                    )
                candidate = checkout_random_node_proxy(
                    base_url=management_base,
                    api_key=api_key,
                    excluded_proxy_urls=excluded,
                )
                raw_proxy_url = str(candidate.get("proxyUrl") or "").strip()
                proxy_url = runtime_reachable_proxy_url(raw_proxy_url)
                unique_key = proxy_url.lower()
                attempted_proxy_urls.add(unique_key)
                if not proxy_url:
                    raise RuntimeError("easy_proxy_random_node_missing_proxy_url")
                if probe_url:
                    _probe_flow_proxy(
                        proxy_url=raw_proxy_url,
                        probe_url=str(probe_url).strip(),
                        expected_statuses=probe_expected_statuses,
                    )
                with _ACTIVE_FLOW_PROXY_LOCK:
                    _purge_recent_flow_proxy_cache(time.monotonic())
                    if unique_key in _ACTIVE_FLOW_PROXY_URLS:
                        raise RuntimeError(f"easy_proxy_duplicate_active_route: {proxy_url}")
                    if unique_key in _RECENT_FLOW_PROXY_URLS:
                        raise RuntimeError(f"easy_proxy_recent_route_reuse: {proxy_url}")
                    _ACTIVE_FLOW_PROXY_URLS.add(unique_key)
                node_tag = str((candidate.get("metadata") or {}).get("selectedNodeTag") or "").strip()
                node_port = str((candidate.get("metadata") or {}).get("selectedNodePort") or "").strip()
                selected = FlowProxyLease(
                    flow_name=flow_name,
                    proxy_url=proxy_url,
                    raw_proxy_url=raw_proxy_url,
                    lease_id="",
                    host_id="",
                    management_base_url=management_base,
                    unique_key=unique_key,
                    started_monotonic=time.monotonic(),
                    service_key=service_key,
                    stage=stage,
                    acquisition_mode="random-node",
                    checked_out=False,
                )
                print(
                    "[register-orchestration] easy proxy random-node selected "
                    f"flow={flow_name} node={node_tag or 'unknown'} port={node_port or 'unknown'} "
                    f"proxy={mask_proxy_url(proxy_url)}"
                )
                return selected
            except Exception as exc:
                last_error = exc
                node_tag = str(((candidate or {}).get("metadata") or {}).get("selectedNodeTag") or "").strip()
                node_port = str(((candidate or {}).get("metadata") or {}).get("selectedNodePort") or "").strip()
                candidate_proxy_url = runtime_reachable_proxy_url(str((candidate or {}).get("proxyUrl") or "").strip())
                candidate_unique_key = str(candidate_proxy_url or "").strip().lower()
                error_code, failure_class, route_confidence = _classify_easy_proxy_error(exc, probe_url=probe_url)
                if failure_class == "route_failure" and candidate_unique_key:
                    _mark_failed_flow_proxy(candidate_unique_key)
                print(
                    "[register-orchestration] easy proxy random-node failed "
                    f"flow={flow_name} attempt={attempt + 1} "
                    f"node={node_tag or 'unknown'} port={node_port or 'unknown'} err={exc}"
                )
                time.sleep(0.1 * (attempt + 1))
        return None

    def _try_compat_checkout() -> FlowProxyLease | None:
        nonlocal last_error, host_id
        for attempt in range(unique_attempts):
            candidate = None
            try:
                host_id = _build_easy_proxy_host_id(flow_name)
                candidate = checkout_proxy(
                    host_id=host_id,
                    ttl_minutes=ttl_minutes,
                    base_url=management_base,
                    api_key=api_key,
                    metadata=metadata_text,
                    require_dedicated_node=True,
                )
                raw_proxy_url = str(candidate.get("proxyUrl") or "").strip()
                proxy_url = runtime_reachable_proxy_url(raw_proxy_url)
                unique_key = proxy_url.lower()
                if not proxy_url:
                    raise RuntimeError("easy_proxy_checkout_missing_proxy_url")
                if probe_url:
                    _probe_flow_proxy(
                        proxy_url=raw_proxy_url,
                        probe_url=str(probe_url).strip(),
                        expected_statuses=probe_expected_statuses,
                    )
                with _ACTIVE_FLOW_PROXY_LOCK:
                    _purge_recent_flow_proxy_cache(time.monotonic())
                    if unique_key in _ACTIVE_FLOW_PROXY_URLS:
                        raise RuntimeError(f"easy_proxy_duplicate_active_route: {proxy_url}")
                    if unique_key in _RECENT_FLOW_PROXY_URLS:
                        raise RuntimeError(f"easy_proxy_recent_route_reuse: {proxy_url}")
                    _ACTIVE_FLOW_PROXY_URLS.add(unique_key)
                selected = FlowProxyLease(
                    flow_name=flow_name,
                    proxy_url=proxy_url,
                    raw_proxy_url=raw_proxy_url,
                    lease_id=str(candidate.get("id") or "").strip(),
                    host_id=host_id,
                    management_base_url=management_base,
                    unique_key=unique_key,
                    started_monotonic=time.monotonic(),
                    service_key=service_key,
                    stage=stage,
                    acquisition_mode="lease",
                    checked_out=True,
                )
                print(
                    "[register-orchestration] easy proxy checkout "
                    f"flow={flow_name} lease={selected.lease_id or 'unknown'} proxy={mask_proxy_url(proxy_url)}"
                )
                return selected
            except Exception as exc:
                last_error = exc
                candidate_lease_id = str((candidate or {}).get("id") or "").strip()
                candidate_proxy_url = runtime_reachable_proxy_url(str((candidate or {}).get("proxyUrl") or "").strip())
                print(
                    "[register-orchestration] easy proxy checkout failed "
                    f"flow={flow_name} attempt={attempt + 1} proxy={mask_proxy_url(candidate_proxy_url)} err={exc}"
                )
                if candidate_lease_id:
                    error_code, failure_class, route_confidence = _classify_easy_proxy_error(exc, probe_url=probe_url)
                    report_usage(
                        candidate_lease_id,
                        success=False,
                        latency_ms=0,
                        error_code=error_code,
                        service_key=service_key,
                        stage=stage,
                        failure_class=failure_class,
                        route_confidence=route_confidence,
                        base_url=management_base,
                        api_key=api_key,
                    )
                    release_lease(candidate_lease_id, base_url=management_base, api_key=api_key)
                time.sleep(0.1 * (attempt + 1))
        return None

    # Prefer compat leases first so EasyProxy can receive feedback and learn
    # from failures before we fall back to stateless random-node selection.
    if mode in {"auto", "lease"}:
        lease = _try_compat_checkout()
    if lease is None and mode in {"auto", "random-node"}:
        lease = _try_random_nodes()
    if lease is None and mode == "random-node" and not tried_random:
        lease = _try_random_nodes()

    if lease is None:
        if required:
            raise RuntimeError(f"easy_proxy_checkout_failed flow={flow_name}: {last_error}") from last_error
        lease = FlowProxyLease.direct(flow_name=flow_name)

    return lease


def release_flow_proxy_lease(
    lease: FlowProxyLease,
    *,
    success: bool = True,
    error: Exception | None = None,
    error_code: str | None = None,
    failure_class: str = "",
    route_confidence: str = "",
) -> None:
    if error is not None:
        resolved_error_code, resolved_failure_class, resolved_route_confidence = _classify_easy_proxy_error(error)
        lease.mark_error(
            resolved_error_code,
            failure_class=resolved_failure_class,
            route_confidence=resolved_route_confidence,
        )
        if resolved_failure_class == "route_failure" and lease.unique_key:
            _mark_failed_flow_proxy(lease.unique_key)
    elif success:
        lease.mark_success()
    else:
        normalized_error_code = str(error_code or "").strip() or "flow_error"
        normalized_failure_class = str(failure_class or "").strip()
        normalized_route_confidence = str(route_confidence or "").strip()
        lease.mark_error(
            normalized_error_code,
            failure_class=normalized_failure_class,
            route_confidence=normalized_route_confidence,
        )
        if normalized_failure_class == "route_failure" and lease.unique_key:
            _mark_failed_flow_proxy(lease.unique_key)
    lease.finalize()


@contextlib.contextmanager
def lease_flow_proxy(
    *,
    flow_name: str,
    metadata: dict[str, Any] | None = None,
    required: bool | None = None,
    probe_url: str | None = None,
    probe_expected_statuses: set[int] | None = None,
) -> Iterator[FlowProxyLease]:
    lease = acquire_flow_proxy_lease(
        flow_name=flow_name,
        metadata=metadata,
        required=required,
        probe_url=probe_url,
        probe_expected_statuses=probe_expected_statuses,
    )
    try:
        yield lease
    except Exception as exc:
        release_flow_proxy_lease(lease, error=exc)
        raise
    else:
        release_flow_proxy_lease(lease, success=True)


@contextlib.contextmanager
def flow_network_env() -> Iterator[None]:
    # When EasyProxy is enabled, curl_cffi explicit-proxy requests behave
    # differently if the process proxy environment is stripped. Preserve the
    # ambient proxy environment for EasyProxy-backed flows, and only hard-clear
    # proxy env for true direct/no-proxy runs.
    if env_flag("REGISTER_ENABLE_EASY_PROXY", True):
        yield
        return
    with without_proxy_env():
        yield


def _normalize_mailbox_provider(provider: str) -> str:
    value = str(provider or "").strip().lower()
    alias_map = {
        "cloudflare-temp-email": "cloudflare_temp_email",
        "cloudflaretempemail": "cloudflare_temp_email",
        "mail-to-you": "m2u",
        "mailtoyou": "m2u",
        "tempmaillol": "tempmail-lol",
        "tempmail.lol": "tempmail-lol",
    }
    return alias_map.get(value, value)


def resolve_mailbox_provider_selections() -> tuple[str, ...]:
    raw = (
        os.environ.get("REGISTER_MAILBOX_PROVIDERS")
        or os.environ.get("MAILBOX_PROVIDER_CANDIDATES")
        or ""
    ).strip()
    if not raw:
        # Business default: keep the main registration flow on providers that
        # still pass OpenAI email acceptance. etempmail remains available only
        # when explicitly opted in via env.
        return ("m2u", "moemail")
    normalized: list[str] = []
    seen: set[str] = set()
    for part in raw.split(","):
        normalized_provider = _normalize_mailbox_provider(part)
        if not normalized_provider or normalized_provider in seen:
            continue
        normalized.append(normalized_provider)
        seen.add(normalized_provider)
    return tuple(normalized)


def resolve_mailbox_strategy_mode_id() -> str:
    return str(
        os.environ.get("REGISTER_MAILBOX_STRATEGY_MODE_ID")
        or os.environ.get("MAILBOX_PROVIDER_STRATEGY_MODE_ID")
        or ""
    ).strip()


def resolve_mailbox_routing_profile_id() -> str:
    return str(
        os.environ.get("REGISTER_MAILBOX_ROUTING_PROFILE_ID")
        or os.environ.get("MAILBOX_PROVIDER_ROUTING_PROFILE_ID")
        or "high-availability"
    ).strip()


def _provider_from_mailbox_ref(mailbox_ref: str) -> str:
    value = str(mailbox_ref or "").strip()
    if not value:
        return ""
    if ":" not in value:
        return "moemail"
    provider = value.split(":", 1)[0]
    return _normalize_mailbox_provider(provider)


def _normalize_requested_email_address(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    if not normalized or "@" not in normalized:
        return ""
    local_part, _, domain = normalized.partition("@")
    local_part = local_part.strip()
    domain = domain.strip().lower()
    if not local_part or not domain:
        return ""
    return f"{local_part}@{domain}"


def _shared_root_from_output_root(output_root: Path) -> Path:
    resolved = output_root.resolve()
    if resolved.name.lower().endswith("-runs"):
        if resolved.parent.name.lower() == "others":
            return resolved.parent.parent
        return resolved.parent
    if resolved.name.lower() == "others":
        return resolved.parent
    return resolved


def _resolve_mailbox_domain_state_path() -> Path:
    explicit = str(os.environ.get("REGISTER_MAILBOX_DOMAIN_STATE_PATH") or "").strip()
    if explicit:
        return Path(explicit).expanduser().resolve()
    output_root_text = str(os.environ.get("REGISTER_OUTPUT_ROOT") or "").strip()
    if output_root_text:
        return _shared_root_from_output_root(Path(output_root_text).expanduser()) / "others" / "register-mailbox-domain-state.json"
    return Path.cwd().resolve() / "tmp" / "register-mailbox-domain-state.json"


def _load_mailbox_domain_state() -> dict[str, Any]:
    state_path = _resolve_mailbox_domain_state_path()
    if not state_path.is_file():
        return {}
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _resolve_business_mailbox_domain_pool() -> tuple[str, ...]:
    raw = str(os.environ.get("REGISTER_MAILBOX_DOMAIN_POOL") or "").strip()
    if not raw:
        return DEFAULT_REGISTER_MOEMAIL_DOMAIN_POOL
    normalized = raw.replace(";", ",").replace("|", ",").replace("\r", ",").replace("\n", ",")
    domains: list[str] = []
    seen: set[str] = set()
    for item in normalized.split(","):
        domain = str(item or "").strip().lower()
        if not domain or domain in seen:
            continue
        seen.add(domain)
        domains.append(domain)
    return tuple(domains)


def _resolve_mailbox_domain_blacklist_min_attempts() -> int:
    raw = str(os.environ.get("REGISTER_MAILBOX_DOMAIN_BLACKLIST_MIN_ATTEMPTS") or "").strip()
    try:
        return max(1, int(raw or DEFAULT_REGISTER_MAILBOX_DOMAIN_BLACKLIST_MIN_ATTEMPTS))
    except Exception:
        return DEFAULT_REGISTER_MAILBOX_DOMAIN_BLACKLIST_MIN_ATTEMPTS


def _resolve_mailbox_domain_blacklist_failure_rate() -> float:
    raw = str(os.environ.get("REGISTER_MAILBOX_DOMAIN_BLACKLIST_FAILURE_RATE") or "").strip()
    try:
        value = float(raw or DEFAULT_REGISTER_MAILBOX_DOMAIN_BLACKLIST_FAILURE_RATE)
    except Exception:
        value = DEFAULT_REGISTER_MAILBOX_DOMAIN_BLACKLIST_FAILURE_RATE
    if 0.0 < value <= 1.0:
        value *= 100.0
    return max(0.0, min(100.0, value))


def _mailbox_domain_stats(domain: str, state_payload: dict[str, Any]) -> dict[str, Any]:
    domains = state_payload.get("domains")
    if not isinstance(domains, dict):
        return {}
    stats = domains.get(domain)
    return stats if isinstance(stats, dict) else {}


def _mailbox_domain_is_business_blacklisted(domain: str, state_payload: dict[str, Any]) -> bool:
    stats = _mailbox_domain_stats(domain, state_payload)
    attempts = int(stats.get("attempts") or 0)
    failures = int(stats.get("failures") or 0)
    if attempts < _resolve_mailbox_domain_blacklist_min_attempts():
        return False
    if attempts <= 0:
        return False
    failure_rate = (float(failures) / float(attempts)) * 100.0
    return failure_rate >= _resolve_mailbox_domain_blacklist_failure_rate()


def _select_business_mailbox_domain() -> tuple[str, dict[str, Any]]:
    domain_pool = list(_resolve_business_mailbox_domain_pool())
    if not domain_pool:
        return "", {"reason": "empty_pool", "pool_size": 0, "eligible_count": 0, "blacklisted": []}

    state_payload = _load_mailbox_domain_state()
    blacklisted = [domain for domain in domain_pool if _mailbox_domain_is_business_blacklisted(domain, state_payload)]
    eligible = [domain for domain in domain_pool if domain not in blacklisted]
    effective_pool = eligible or domain_pool
    selected_domain = random.SystemRandom().choice(effective_pool)
    return selected_domain, {
        "reason": "eligible_pool" if eligible else "all_blacklisted_fallback",
        "pool_size": len(domain_pool),
        "eligible_count": len(eligible),
        "blacklisted": blacklisted,
    }


def _resolve_mailbox_strategy_kwargs() -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    routing_profile_id = resolve_mailbox_routing_profile_id()
    if routing_profile_id:
        kwargs["provider_routing_profile_id"] = routing_profile_id
    strategy_mode_id = resolve_mailbox_strategy_mode_id()
    if strategy_mode_id:
        kwargs["provider_strategy_mode_id"] = strategy_mode_id
    provider_selections = resolve_mailbox_provider_selections()
    if provider_selections:
        kwargs["provider_group_selections"] = provider_selections
    return kwargs


def _resolve_planned_mailbox_provider(*, ttl_seconds: int, strategy_kwargs: dict[str, Any]) -> str:
    try:
        plan = plan_mailbox(
            provider="auto",
            default_host_id=DEFAULT_ORCHESTRATION_HOST_ID,
            ttl_seconds=ttl_seconds,
            **strategy_kwargs,
        )
    except Exception as exc:
        print(f"[register-orchestration] mailbox plan skipped err={exc}")
        return ""
    if not isinstance(plan, dict):
        return ""
    instance = plan.get("instance")
    provider_type = plan.get("providerType")
    return _normalize_mailbox_provider(
        str(
            (instance.get("providerTypeKey") if isinstance(instance, dict) else "")
            or (provider_type.get("key") if isinstance(provider_type, dict) else "")
            or ""
        ).strip()
    )


def resolve_mailbox(
    *,
    preallocated_email: str | None,
    preallocated_session_id: str | None,
    preallocated_mailbox_ref: str | None,
    recreate_preallocated_email: bool = False,
) -> Mailbox:
    ensure_easy_email_env_defaults()
    normalized_preallocated_email = _normalize_requested_email_address(preallocated_email)
    if normalized_preallocated_email and recreate_preallocated_email:
        ttl_seconds = _resolve_mailbox_ttl_seconds()
        requested_local_part, _, requested_domain = normalized_preallocated_email.partition("@")
        preferred_provider = _provider_from_mailbox_ref(preallocated_mailbox_ref or "")
        try:
            return create_mailbox(
                provider=preferred_provider or "auto",
                default_host_id=DEFAULT_ORCHESTRATION_HOST_ID,
                prefer_raw_self_hosted_ref=True,
                ttl_seconds=ttl_seconds,
                requested_email_address=normalized_preallocated_email,
                requested_local_part=requested_local_part,
                mailcreate_domain=requested_domain,
                **_resolve_mailbox_strategy_kwargs(),
            )
        except Exception as exc:
            raise ensure_protocol_runtime_error(
                exc,
                stage="stage_other",
                detail="recreate_mailbox",
                category="flow_error",
            ) from exc
    if preallocated_email and preallocated_mailbox_ref:
        ref = str(preallocated_mailbox_ref).strip()
        session_id = str(preallocated_session_id or "").strip()
        if not session_id:
            if ":" in ref:
                session_id = ref.split(":", 1)[1].strip()
            else:
                session_id = ref
        return Mailbox(
            provider=_provider_from_mailbox_ref(ref),
            email=str(preallocated_email).strip(),
            ref=ref,
            session_id=session_id,
        )
    if preallocated_email and preallocated_session_id:
        session_id = str(preallocated_session_id).strip()
        return Mailbox(
            provider="moemail",
            email=str(preallocated_email).strip(),
            ref=f"moemail:{session_id}",
            session_id=session_id,
        )
    ttl_seconds = _resolve_mailbox_ttl_seconds()
    strategy_kwargs = _resolve_mailbox_strategy_kwargs()
    planned_provider = _resolve_planned_mailbox_provider(
        ttl_seconds=ttl_seconds,
        strategy_kwargs=strategy_kwargs,
    )
    try:
        if planned_provider == "moemail":
            selected_domain, domain_selection = _select_business_mailbox_domain()
            if selected_domain:
                print(
                    "[register-orchestration] mailbox business domain selected "
                    f"provider=moemail domain={selected_domain} "
                    f"reason={domain_selection.get('reason')} "
                    f"eligible={domain_selection.get('eligible_count')} "
                    f"blacklisted={len(domain_selection.get('blacklisted') or [])}"
                )
                return create_mailbox(
                    provider="moemail",
                    default_host_id=DEFAULT_ORCHESTRATION_HOST_ID,
                    prefer_raw_self_hosted_ref=True,
                    ttl_seconds=ttl_seconds,
                    mailcreate_domain=selected_domain,
                )
        return create_mailbox(
            provider="auto",
            default_host_id=DEFAULT_ORCHESTRATION_HOST_ID,
            prefer_raw_self_hosted_ref=True,
            ttl_seconds=ttl_seconds,
            **strategy_kwargs,
        )
    except Exception as exc:
        raise ensure_protocol_runtime_error(
            exc,
            stage="stage_other",
            detail="create_mailbox",
            category="flow_error",
        ) from exc


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
