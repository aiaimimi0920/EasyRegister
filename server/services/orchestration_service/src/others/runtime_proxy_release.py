from __future__ import annotations

import contextlib
from typing import Iterator

from others.runtime_proxy_acquire import acquire_flow_proxy_lease
from others.runtime_proxy_model import FlowProxyLease
from others.runtime_proxy_support import _classify_easy_proxy_error, _mark_failed_flow_proxy


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
    metadata: dict[str, str] | None = None,
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
