from __future__ import annotations

import contextlib
import os
import time
from dataclasses import dataclass
from typing import Any, Iterator

from others.runtime_proxy_support import (
    DEFAULT_ORCHESTRATION_HOST_ID,
    _ACTIVE_FLOW_PROXY_LOCK,
    _ACTIVE_FLOW_PROXY_URLS,
    _FAILED_FLOW_PROXY_URLS,
    _RECENT_FLOW_PROXY_URLS,
    _build_easy_proxy_host_id,
    _classify_easy_proxy_error,
    _default_easy_proxy_service_key,
    _default_easy_proxy_stage,
    _mark_failed_flow_proxy,
    _probe_flow_proxy,
    _proxy_runtime_config,
    _purge_failed_flow_proxy_cache,
    _purge_recent_flow_proxy_cache,
    _resolve_easy_proxy_mode,
    _resolve_easy_proxy_recent_window_seconds,
    _resolve_easy_proxy_unique_attempts,
    ensure_easy_proxy_env_defaults,
    runtime_reachable_proxy_url,
)

from shared_proxy import mask_proxy_url
from shared_proxy.easy_proxy_client import (
    checkout_proxy,
    checkout_random_node_proxy,
    release_lease,
    report_usage,
)


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
            proxy_config = _proxy_runtime_config()
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
                api_key=proxy_config.api_key,
            )
            release_lease(
                self.lease_id,
                base_url=self.management_base_url,
                api_key=proxy_config.api_key,
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


def acquire_flow_proxy_lease(
    *,
    flow_name: str,
    metadata: dict[str, Any] | None = None,
    required: bool | None = None,
    probe_url: str | None = None,
    probe_expected_statuses: set[int] | None = None,
) -> FlowProxyLease:
    proxy_config = _proxy_runtime_config()
    enabled = proxy_config.enabled
    required = proxy_config.required_by_default if required is None else bool(required)
    if not enabled:
        return FlowProxyLease.direct(flow_name=flow_name)

    ensure_easy_proxy_env_defaults()
    management_base = proxy_config.management_base_url
    api_key = proxy_config.api_key
    ttl_minutes = proxy_config.ttl_minutes
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

    def _try_random_nodes() -> FlowProxyLease | None:
        nonlocal last_error
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
                _, failure_class, _ = _classify_easy_proxy_error(exc, probe_url=probe_url)
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

    if mode in {"auto", "lease"}:
        lease = _try_compat_checkout()
    if lease is None and mode in {"auto", "random-node"}:
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
