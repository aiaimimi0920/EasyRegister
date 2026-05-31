from __future__ import annotations

import json
import os
import time
from typing import Any

from others.config import _resolve_shared_root, env_float
from others.common import json_log, write_json_atomic
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


_COMPAT_CHECKOUT_COOLDOWN_UNTIL: dict[str, float] = {}
_COMPAT_CHECKOUT_COOLDOWN_STATE_SCHEMA_VERSION = 1


def _normalize_probe_urls(*, probe_url: str | None, probe_urls: object) -> list[str]:
    targets: list[str] = []
    seen: set[str] = set()

    def _append(raw: object) -> None:
        normalized = str(raw or "").strip()
        key = normalized.lower()
        if normalized and key not in seen:
            targets.append(normalized)
            seen.add(key)

    _append(probe_url)
    if isinstance(probe_urls, str):
        _append(probe_urls)
    elif isinstance(probe_urls, (list, tuple, set)):
        for item in probe_urls:
            _append(item)
    return targets


def _resolve_compat_checkout_failure_cooldown_seconds() -> float:
    return max(0.0, env_float("REGISTER_PROXY_LEASE_FAILURE_COOLDOWN_SECONDS", 120.0))


def _compat_checkout_cooldown_state_path():
    return _resolve_shared_root() / "others" / "easy-proxy-checkout-cooldowns.json"


def _read_shared_compat_checkout_cooldowns() -> dict[str, float]:
    path = _compat_checkout_cooldown_state_path()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    if int(payload.get("schemaVersion") or 0) != _COMPAT_CHECKOUT_COOLDOWN_STATE_SCHEMA_VERSION:
        return {}
    raw_cooldowns = payload.get("cooldowns")
    if not isinstance(raw_cooldowns, dict):
        return {}
    cooldowns: dict[str, float] = {}
    for key, value in raw_cooldowns.items():
        if not isinstance(value, dict):
            continue
        try:
            until_epoch = float(value.get("untilEpoch") or 0.0)
        except (TypeError, ValueError):
            continue
        if until_epoch > 0:
            cooldowns[str(key)] = until_epoch
    return cooldowns


def _write_shared_compat_checkout_cooldown(*, key: str, until_epoch: float) -> None:
    try:
        cooldowns = _read_shared_compat_checkout_cooldowns()
        cooldowns[str(key)] = max(float(until_epoch), float(cooldowns.get(str(key)) or 0.0))
        now_epoch = time.time()
        active_payload = {
            cooldown_key: {
                "untilEpoch": cooldown_until,
                "updatedAtEpoch": now_epoch,
            }
            for cooldown_key, cooldown_until in cooldowns.items()
            if cooldown_until > now_epoch
        }
        write_json_atomic(
            _compat_checkout_cooldown_state_path(),
            {
                "schemaVersion": _COMPAT_CHECKOUT_COOLDOWN_STATE_SCHEMA_VERSION,
                "updatedAtEpoch": now_epoch,
                "cooldowns": active_payload,
            },
            include_pid=True,
            cleanup_temp=True,
        )
    except Exception as exc:
        json_log(
            {
                "event": "register_easy_proxy_checkout_cooldown_state_write_failed",
                "error": str(exc),
            }
        )


def acquire_flow_proxy_lease(
    *,
    flow_name: str,
    metadata: dict[str, Any] | None = None,
    required: bool | None = None,
    probe_url: str | None = None,
    probe_urls: object = None,
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
    probe_targets = _normalize_probe_urls(probe_url=probe_url, probe_urls=probe_urls)
    primary_probe_url = probe_targets[0] if probe_targets else probe_url
    compat_cooldown_key = "|".join((management_base.lower(), service_key, stage))
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

    def _should_abort_compat_retry(exc: Exception) -> bool:
        normalized = str(exc or "").strip().lower()
        if not normalized:
            return False
        if _is_local_route_reuse_error(normalized):
            return True
        abort_markers = (
            "initial_proxy_probe_pending",
            "no_proxy_provider_route",
            "provider_instance_unavailable",
            "proxy route failure",
            "easy_proxy_probe_failed",
            "connection reset",
            "connection refused",
            "remote end closed",
            "unexpected eof",
            "timed out",
            "i/o timeout",
            "ssl handshake",
            "tls handshake",
            "econnreset",
            "eof",
        )
        return any(marker in normalized for marker in abort_markers)

    def _compat_checkout_cooldown_remaining(now: float) -> float:
        with _ACTIVE_FLOW_PROXY_LOCK:
            until = float(_COMPAT_CHECKOUT_COOLDOWN_UNTIL.get(compat_cooldown_key) or 0.0)
            if until <= now:
                _COMPAT_CHECKOUT_COOLDOWN_UNTIL.pop(compat_cooldown_key, None)
            else:
                return max(0.0, until - now)
        shared_until_epoch = float(_read_shared_compat_checkout_cooldowns().get(compat_cooldown_key) or 0.0)
        shared_remaining = max(0.0, shared_until_epoch - time.time())
        if shared_remaining > 0:
            with _ACTIVE_FLOW_PROXY_LOCK:
                _COMPAT_CHECKOUT_COOLDOWN_UNTIL[compat_cooldown_key] = max(
                    float(_COMPAT_CHECKOUT_COOLDOWN_UNTIL.get(compat_cooldown_key) or 0.0),
                    time.monotonic() + shared_remaining,
                )
        return shared_remaining

    def _mark_compat_checkout_cooldown(exc: Exception) -> None:
        cooldown_seconds = _resolve_compat_checkout_failure_cooldown_seconds()
        if cooldown_seconds <= 0:
            return
        now = time.monotonic()
        until = now + cooldown_seconds
        with _ACTIVE_FLOW_PROXY_LOCK:
            _COMPAT_CHECKOUT_COOLDOWN_UNTIL[compat_cooldown_key] = max(
                until,
                float(_COMPAT_CHECKOUT_COOLDOWN_UNTIL.get(compat_cooldown_key) or 0.0),
            )
        _write_shared_compat_checkout_cooldown(
            key=compat_cooldown_key,
            until_epoch=time.time() + cooldown_seconds,
        )
        json_log(
            {
                "event": "register_easy_proxy_checkout_cooldown_started",
                "flowName": flow_name,
                "seconds": round(cooldown_seconds, 3),
                "errorClass": _classify_easy_proxy_error(exc, probe_url=primary_probe_url)[1],
            }
        )

    def _probe_candidate(raw_proxy_url: str) -> None:
        last_probe_error: Exception | None = None
        for target in probe_targets:
            try:
                _probe_flow_proxy(
                    proxy_url=raw_proxy_url,
                    probe_url=target,
                    expected_statuses=probe_expected_statuses,
                )
                return
            except Exception as exc:
                last_probe_error = exc
        if last_probe_error is not None:
            raise last_probe_error

    def _try_random_nodes() -> FlowProxyLease | None:
        nonlocal last_error
        attempted_proxy_urls: set[str] = set()
        for attempt in range(unique_attempts + 1):
            candidate = None
            try:
                allow_recent_reuse = attempt >= unique_attempts
                with _ACTIVE_FLOW_PROXY_LOCK:
                    _purge_recent_flow_proxy_cache(time.monotonic())
                    _purge_failed_flow_proxy_cache(time.monotonic())
                    excluded = (
                        set(_ACTIVE_FLOW_PROXY_URLS)
                        | set(_FAILED_FLOW_PROXY_URLS.keys())
                        | set(attempted_proxy_urls)
                    )
                    if not allow_recent_reuse:
                        excluded |= set(_RECENT_FLOW_PROXY_URLS.keys())
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
                _probe_candidate(raw_proxy_url)
                with _ACTIVE_FLOW_PROXY_LOCK:
                    _purge_recent_flow_proxy_cache(time.monotonic())
                    if unique_key in _ACTIVE_FLOW_PROXY_URLS:
                        raise RuntimeError(f"easy_proxy_duplicate_active_route: {proxy_url}")
                    if not allow_recent_reuse and unique_key in _RECENT_FLOW_PROXY_URLS:
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
                _, failure_class, _ = _classify_easy_proxy_error(exc, probe_url=primary_probe_url)
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
                _probe_candidate(raw_proxy_url)
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
                    error_code, failure_class, route_confidence = _classify_easy_proxy_error(exc, probe_url=primary_probe_url)
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
                if _should_abort_compat_retry(exc):
                    if not local_route_reuse and not candidate_lease_id and not candidate_proxy_url:
                        _mark_compat_checkout_cooldown(exc)
                    break
                time.sleep(0.1 * (attempt + 1))
        return None

    if mode in {"auto", "lease"}:
        cooldown_remaining = _compat_checkout_cooldown_remaining(time.monotonic())
        if cooldown_remaining > 0:
            last_error = RuntimeError(f"easy_proxy_checkout_cooldown_active seconds={cooldown_remaining:.1f}")
            json_log(
                {
                    "event": "register_easy_proxy_checkout_cooldown_active",
                    "flowName": flow_name,
                    "remainingSeconds": round(cooldown_remaining, 3),
                }
            )
        else:
            lease = _try_compat_checkout()
    if lease is None and mode in {"auto", "random-node"}:
        lease = _try_random_nodes()

    if lease is None:
        if required:
            raise RuntimeError(f"easy_proxy_checkout_failed flow={flow_name}: {last_error}") from last_error
        lease = FlowProxyLease.direct(flow_name=flow_name)

    return lease
