from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
import uuid
from typing import Any


DEFAULT_EASY_PROTOCOL_BASE_URL = "http://127.0.0.1:19788"
DEFAULT_EASY_PROTOCOL_OPERATION = "codex.semantic.step"
DEFAULT_EASY_PROTOCOL_MODE = "strategy"
DEFAULT_EASY_PROTOCOL_REQUESTED_SERVICE = ""
DEFAULT_EASY_PROTOCOL_TIMEOUT_SECONDS = 900


def normalize_easyprotocol_request_url(base_url: str) -> str:
    normalized = str(base_url or "").strip().rstrip("/")
    if not normalized:
        normalized = DEFAULT_EASY_PROTOCOL_BASE_URL
    if normalized.endswith("/api/public/request"):
        return normalized
    return normalized + "/api/public/request"


def easyprotocol_timeout_seconds() -> int:
    raw = str(os.environ.get("EASY_PROTOCOL_TIMEOUT_SECONDS") or "").strip()
    if not raw:
        return DEFAULT_EASY_PROTOCOL_TIMEOUT_SECONDS
    try:
        return max(1, int(float(raw)))
    except Exception:
        return DEFAULT_EASY_PROTOCOL_TIMEOUT_SECONDS


def build_easyprotocol_request(*, step_type: str, step_input: dict[str, Any]) -> dict[str, Any]:
    request_mode = str(
        os.environ.get("EASY_PROTOCOL_REQUEST_MODE") or DEFAULT_EASY_PROTOCOL_MODE
    ).strip() or DEFAULT_EASY_PROTOCOL_MODE
    requested_service = str(os.environ.get("EASY_PROTOCOL_REQUESTED_SERVICE") or "").strip()
    payload: dict[str, Any] = {
        "request_id": f"register-{uuid.uuid4()}",
        "mode": request_mode,
        "operation": DEFAULT_EASY_PROTOCOL_OPERATION,
        "payload": {
            "step_type": str(step_type or "").strip(),
            "step_input": dict(step_input or {}),
        },
    }
    if requested_service:
        payload["requested_service"] = requested_service
    return payload


def is_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def normalize_preserve_codes(value: Any) -> set[str]:
    if isinstance(value, list):
        return {str(item or "").strip() for item in value if str(item or "").strip()}
    if isinstance(value, tuple):
        return {str(item or "").strip() for item in value if str(item or "").strip()}
    raw = str(value or "").strip()
    if not raw:
        return set()
    return {item.strip() for item in raw.split(",") if item.strip()}


def invoke_easyprotocol(*, step_type: str, step_input: dict[str, Any]) -> dict[str, Any]:
    base_url = str(os.environ.get("EASY_PROTOCOL_BASE_URL") or "").strip() or DEFAULT_EASY_PROTOCOL_BASE_URL
    request_url = normalize_easyprotocol_request_url(base_url)
    request_payload = build_easyprotocol_request(step_type=step_type, step_input=step_input)
    body = json.dumps(request_payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        request_url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=easyprotocol_timeout_seconds()) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(raw)
        except Exception:
            raise RuntimeError(f"easyprotocol_http_{exc.code}")
        message = str(parsed.get("error") or parsed.get("message") or f"easyprotocol_http_{exc.code}").strip()
        raise RuntimeError(message or f"easyprotocol_http_{exc.code}")
    except Exception as exc:
        raise RuntimeError(f"easyprotocol_transport_failed:{exc}") from exc

    try:
        payload = json.loads(raw)
    except Exception as exc:
        raise RuntimeError(f"easyprotocol_invalid_json:{exc}") from exc

    if str(payload.get("status") or "").strip().lower() == "failed":
        error = payload.get("error") if isinstance(payload.get("error"), dict) else {}
        message = str(error.get("message") or "").strip() or "easyprotocol_failed"
        raise RuntimeError(message)

    result = payload.get("result")
    if not isinstance(result, dict):
        raise RuntimeError("easyprotocol_result_missing")
    step_result = result.get("step_result")
    if not isinstance(step_result, dict):
        raise RuntimeError("easyprotocol_step_result_missing")
    return step_result


def dispatch_easyprotocol_step(*, step_type: str, step_input: dict[str, Any]) -> dict[str, Any]:
    normalized_step_type = str(step_type or "").strip()
    if not normalized_step_type:
        raise RuntimeError("easyprotocol_step_type_missing")
    if not isinstance(step_input, dict):
        raise RuntimeError("easyprotocol_step_input_invalid")
    if normalized_step_type == "revoke_codex_member":
        error_code = str(step_input.get("error_code") or "").strip()
        preserve_enabled = is_truthy(step_input.get("preserve_enabled"))
        preserve_codes = normalize_preserve_codes(step_input.get("preserve_on_error_codes"))
        if preserve_enabled and error_code and error_code in preserve_codes:
            return {
                "ok": True,
                "status": "skipped_preserved_for_manual_oauth",
                "detail": "preserved_for_manual_oauth",
                "invite_email": str(step_input.get("invite_email") or "").strip(),
                "team_account_id": "",
                "team_email": "",
                "status_code": 0,
                "response": None,
            }
    return invoke_easyprotocol(step_type=normalized_step_type, step_input=step_input)
