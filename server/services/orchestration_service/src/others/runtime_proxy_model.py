from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from others.runtime_proxy_support import (
    _ACTIVE_FLOW_PROXY_LOCK,
    _ACTIVE_FLOW_PROXY_URLS,
    _RECENT_FLOW_PROXY_URLS,
    _proxy_runtime_config,
    _resolve_easy_proxy_recent_window_seconds,
)

from shared_proxy.easy_proxy_client import release_lease, report_usage


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
