from __future__ import annotations

import os
import time
from typing import Any

from others.common import json_log
from others.runtime_proxy_model import FlowProxyLease
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
    _resolve_easy_proxy_unique_attempts,
    ensure_easy_proxy_env_defaults,
    runtime_reachable_proxy_url,
)

from shared_proxy import mask_proxy_url
from shared_proxy.easy_proxy_client import checkout_proxy, checkout_random_node_proxy, release_lease, report_usage


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

    def _is_local_route_reuse_error(error_text: str) -> bool:
        normalized = str(error_text or "").strip().lower()
        return "duplicate_active_route" in normalized or "recent_route_reuse" in normalized

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
                json_log(
                    {
                        "event": "register_easy_proxy_random_node_selected",
                        "flowName": flow_name,
                        "nodeTag": node_tag or "unknown",
                        "nodePort": node_port or "unknown",
                        "proxy": mask_proxy_url(proxy_url),
                    }
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
                json_log(
                    {
                        "event": "register_easy_proxy_random_node_failed",
                        "flowName": flow_name,
                        "attempt": attempt + 1,
                        "nodeTag": node_tag or "unknown",
                        "nodePort": node_port or "unknown",
                        "error": str(exc),
                    }
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
                json_log(
                    {
                        "event": "register_easy_proxy_checkout_selected",
                        "flowName": flow_name,
                        "leaseId": selected.lease_id or "unknown",
                        "proxy": mask_proxy_url(proxy_url),
                    }
                )
                return selected
            except Exception as exc:
                last_error = exc
                candidate_lease_id = str((candidate or {}).get("id") or "").strip()
                candidate_proxy_url = runtime_reachable_proxy_url(str((candidate or {}).get("proxyUrl") or "").strip())
                local_route_reuse = _is_local_route_reuse_error(str(exc))
                json_log(
                    {
                        "event": "register_easy_proxy_checkout_failed",
                        "flowName": flow_name,
                        "attempt": attempt + 1,
                        "proxy": mask_proxy_url(candidate_proxy_url),
                        "error": str(exc),
                    }
                )
                if candidate_lease_id:
                    error_code, failure_class, route_confidence = _classify_easy_proxy_error(exc, probe_url=probe_url)
                    if not local_route_reuse:
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
                    if local_route_reuse:
                        break
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
