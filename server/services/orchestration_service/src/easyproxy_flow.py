from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    _CURRENT_DIR = Path(__file__).resolve().parent
    _SRC_DIR = _CURRENT_DIR.parent
    for _candidate in (_CURRENT_DIR, _SRC_DIR):
        candidate_text = str(_candidate)
        if candidate_text not in sys.path:
            sys.path.append(candidate_text)
    from others.bootstrap import ensure_local_bundle_imports

    ensure_local_bundle_imports()
    from others.runtime import FlowProxyLease, acquire_flow_proxy_lease, release_flow_proxy_lease
else:
    from .others.bootstrap import ensure_local_bundle_imports

    ensure_local_bundle_imports()
    from .others.runtime import FlowProxyLease, acquire_flow_proxy_lease, release_flow_proxy_lease


def _proxy_failure_class_from_code(code: str) -> tuple[str, str]:
    normalized = str(code or "").strip().lower()
    if normalized in {
        "proxy_connect_failed",
        "user_register_400",
        "authorize_missing_login_session",
        "transport_error",
    }:
        return ("route_failure", "medium")
    if normalized in {"flow_timeout_exceeded", "existing_account_detected"}:
        return ("unknown", "low")
    return ("", "")


def dispatch_easyproxy_step(*, step_type: str, step_input: dict[str, Any]) -> dict[str, Any]:
    normalized_step_type = str(step_type or "").strip()

    if normalized_step_type == "acquire_proxy_chain":
        metadata = step_input.get("metadata")
        avoid_proxy_urls = {
            str(item or "").strip().lower()
            for item in (step_input.get("avoid_proxy_urls") or [])
            if str(item or "").strip()
        }
        try:
            max_acquire_attempts = max(1, int(step_input.get("max_acquire_attempts") or 4))
        except Exception:
            max_acquire_attempts = 4
        expected_statuses = step_input.get("probe_expected_statuses")
        if isinstance(expected_statuses, list):
            try:
                expected_status_set = {int(item) for item in expected_statuses}
            except Exception:
                expected_status_set = None
        else:
            expected_status_set = None
        last_error: Exception | None = None
        for _ in range(max_acquire_attempts):
            lease = acquire_flow_proxy_lease(
                flow_name=str(step_input.get("flow_name") or "").strip() or "codex_openai_account_task",
                metadata=metadata if isinstance(metadata, dict) else None,
                required=bool(step_input.get("required", True)),
                probe_url=str(step_input.get("probe_url") or "").strip() or None,
                probe_expected_statuses=expected_status_set,
            )
            if str(lease.proxy_url or "").strip().lower() not in avoid_proxy_urls:
                return lease.to_payload()
            release_flow_proxy_lease(
                lease,
                success=False,
                error_code="task_retry_avoid_proxy_reuse",
                failure_class="route_failure",
                route_confidence="medium",
            )
            last_error = RuntimeError(f"easy_proxy_avoided_previous_route: {lease.proxy_url}")
        raise RuntimeError(str(last_error or "easy_proxy_acquire_exhausted"))

    if normalized_step_type == "release_proxy_chain":
        lease_payload = step_input.get("proxy_chain")
        if not isinstance(lease_payload, dict):
            return {
                "released": True,
                "detail": "skipped_missing_proxy_chain",
            }
        lease = FlowProxyLease.from_payload(lease_payload)
        error_code = str(step_input.get("error_code") or "").strip()
        if error_code:
            failure_class, route_confidence = _proxy_failure_class_from_code(error_code)
            release_flow_proxy_lease(
                lease,
                success=False,
                error_code=error_code,
                failure_class=failure_class,
                route_confidence=route_confidence,
            )
            return {
                "released": True,
                "proxy_url": lease.proxy_url,
                "lease_id": lease.lease_id,
                "detail": "released_after_error",
                "errorCode": error_code,
            }
        release_flow_proxy_lease(lease, success=True)
        return {
            "released": True,
            "proxy_url": lease.proxy_url,
            "lease_id": lease.lease_id,
            "detail": "released",
        }

    raise RuntimeError(f"unsupported_easyproxy_step:{normalized_step_type}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dispatch a medium EasyProxy business step.")
    parser.add_argument("--step-type", required=True, help="Generic DST step type.")
    parser.add_argument("--input-json", default="{}", help="JSON object passed as step input.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    payload = json.loads(str(args.input_json or "{}"))
    if not isinstance(payload, dict):
        raise RuntimeError("input_json_must_be_object")
    result = dispatch_easyproxy_step(
        step_type=str(args.step_type or "").strip(),
        step_input=payload,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
