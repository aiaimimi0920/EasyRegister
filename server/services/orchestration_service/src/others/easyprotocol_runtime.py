from __future__ import annotations

import json
import os
import shutil
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any

from others import runtime_sms


DEFAULT_EASY_PROTOCOL_BASE_URL = "http://127.0.0.1:19788"
DEFAULT_EASY_PROTOCOL_OPERATION = "codex.semantic.step"
DEFAULT_EASY_PROTOCOL_MODE = "strategy"
DEFAULT_EASY_PROTOCOL_REQUESTED_SERVICE = ""
DEFAULT_EASY_PROTOCOL_TIMEOUT_SECONDS = 900
DEFAULT_PHONE_VERIFICATION_TERMINAL_RETRY_ATTEMPTS = 5
PHONE_VERIFICATION_RETRYABLE_TERMINAL_CODES = {
    "phone_number_in_use",
    "phone_max_usage_exceeded",
    "rate_limit_exceeded",
}


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


def phone_verification_terminal_retry_attempts() -> int:
    raw = str(os.environ.get("REGISTER_PHONE_VERIFICATION_TERMINAL_RETRY_ATTEMPTS") or "").strip()
    if not raw:
        return DEFAULT_PHONE_VERIFICATION_TERMINAL_RETRY_ATTEMPTS
    try:
        return max(1, int(float(raw)))
    except Exception:
        return DEFAULT_PHONE_VERIFICATION_TERMINAL_RETRY_ATTEMPTS


def _is_retryable_phone_terminal_code(terminal_code: str) -> bool:
    return str(terminal_code or "").strip().lower() in PHONE_VERIFICATION_RETRYABLE_TERMINAL_CODES


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


def maybe_bridge_step_artifact(*, step_type: str, step_result: dict[str, Any]) -> dict[str, Any]:
    bridge_root_text = str(os.environ.get("REGISTER_PROTOCOL_BRIDGE_DIR") or "").strip()
    storage_path_text = str(step_result.get("storage_path") or "").strip()
    if (
        str(step_type or "").strip() != "create_openai_account"
        or not bridge_root_text
        or not storage_path_text
    ):
        return step_result

    source_path = Path(storage_path_text).expanduser()
    if not source_path.is_file():
        return step_result

    bridge_root = Path(bridge_root_text).expanduser()
    bridge_root.mkdir(parents=True, exist_ok=True)
    target_path = (bridge_root / source_path.name).resolve()
    if source_path.resolve() != target_path:
        shutil.copy2(source_path, target_path)

    bridged = dict(step_result)
    bridged.setdefault("original_storage_path", storage_path_text)
    bridged["storage_path"] = str(target_path)
    bridged["bridged_storage_path"] = str(target_path)
    return bridged


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
    normalized_step_input = dict(step_input)
    if normalized_step_type == "revoke_codex_member":
        invite_email = str(normalized_step_input.get("invite_email") or "").strip()
        invite_id = str(normalized_step_input.get("invite_id") or normalized_step_input.get("inviteId") or "").strip()
        member_user_id = str(normalized_step_input.get("member_user_id") or normalized_step_input.get("memberUserId") or "").strip()
        if not invite_email and not invite_id and not member_user_id:
            return {
                "ok": True,
                "status": "skipped_missing_revoke_target",
                "detail": "missing_revoke_target",
                "invite_email": "",
                "team_account_id": "",
                "team_email": "",
                "status_code": 0,
                "response": None,
            }
        error_code = str(normalized_step_input.get("error_code") or "").strip()
        preserve_enabled = is_truthy(normalized_step_input.get("preserve_enabled"))
        preserve_codes = normalize_preserve_codes(normalized_step_input.get("preserve_on_error_codes"))
        if preserve_enabled and error_code and error_code in preserve_codes:
            return {
                "ok": True,
                "status": "skipped_preserved_for_manual_oauth",
                "detail": "preserved_for_manual_oauth",
                "invite_email": str(normalized_step_input.get("invite_email") or "").strip(),
                "team_account_id": "",
                "team_email": "",
                "status_code": 0,
                "response": None,
            }
    if normalized_step_type == "obtain_codex_oauth":
        normalized_step_input.setdefault(
            "sms_verification",
            runtime_sms.build_phone_verification_step_input(
                business_key=str(
                    normalized_step_input.get("business_key")
                    or normalized_step_input.get("mailbox_business_key")
                    or "openai"
                )
            ),
        )
    result = invoke_easyprotocol(step_type=normalized_step_type, step_input=normalized_step_input)
    if normalized_step_type == "obtain_codex_oauth" and isinstance(result, dict):
        result = _maybe_complete_phone_verification_for_oauth(
            initial_result=result,
            step_input=normalized_step_input,
        )
    if isinstance(result, dict):
        return maybe_bridge_step_artifact(
            step_type=normalized_step_type,
            step_result=result,
        )
    return result


def _maybe_complete_phone_verification_for_oauth(*, initial_result: dict[str, Any], step_input: dict[str, Any]) -> dict[str, Any]:
    if not bool(initial_result.get("phoneVerificationRequired")):
        return initial_result

    resume_context = dict(initial_result.get("resumeContext") or {})
    business_key = str(step_input.get("business_key") or step_input.get("mailbox_business_key") or "openai")
    max_phone_attempts = phone_verification_terminal_retry_attempts()
    for phone_attempt_index in range(max_phone_attempts):
        phone_session = runtime_sms.open_phone_session_for_business(
            business_key=business_key,
        )
        phone_number_submitted = False
        phone_failure_stage = "submit_phone_verification_number"
        try:
            phone_number_result = invoke_easyprotocol(
                step_type="submit_phone_verification_number",
                step_input={
                    "source_path": step_input.get("source_path"),
                    "resume_context": resume_context,
                    "phone_number": phone_session["phoneNumber"],
                    "phone_session_id": phone_session["sessionId"],
                },
            )
            if isinstance(phone_number_result, dict):
                updated_resume_context = phone_number_result.get("resumeContext")
                if isinstance(updated_resume_context, dict) and updated_resume_context:
                    resume_context = dict(updated_resume_context)
                if bool(phone_number_result.get("phoneVerificationTerminal")):
                    terminal_code = str(phone_number_result.get("phoneVerificationTerminalCode") or "").strip()
                    terminal_message = str(phone_number_result.get("phoneVerificationTerminalMessage") or "").strip()
                    runtime_sms.record_terminal_phone_outcome(
                        phone_number=phone_session["phoneNumber"],
                        provider_key=phone_session["providerKey"],
                        terminal_code=terminal_code,
                        terminal_message=terminal_message,
                    )
                    runtime_sms.report_phone_outcome_for_session(
                        session_id=phone_session["sessionId"],
                        outcome="failure",
                        detail=terminal_code or terminal_message or "phone_verification_terminal",
                    )
                    if (
                        _is_retryable_phone_terminal_code(terminal_code)
                        and phone_attempt_index + 1 < max_phone_attempts
                    ):
                        continue
                    return {
                        "ok": True,
                        "status": str(phone_number_result.get("status") or "phone_verification_terminal").strip()
                        or "phone_verification_terminal",
                        "successPath": str(
                            initial_result.get("successPath")
                            or initial_result.get("sourcePath")
                            or step_input.get("source_path")
                            or ""
                        ).strip(),
                        "sourcePath": str(
                            step_input.get("source_path")
                            or initial_result.get("sourcePath")
                            or initial_result.get("successPath")
                            or ""
                        ).strip(),
                        "pageType": str(phone_number_result.get("pageType") or "").strip() or "add_phone",
                        "resumeContext": dict(resume_context),
                        "phoneVerificationAttempted": True,
                        "phoneVerificationAccepted": False,
                        "phoneVerificationTerminal": True,
                        "phoneVerificationTerminalCode": terminal_code,
                        "phoneVerificationTerminalMessage": terminal_message,
                        "phoneVerificationTerminalStatusCode": phone_number_result.get("phoneVerificationTerminalStatusCode"),
                        "phoneProvider": phone_session["providerKey"],
                        "phoneSessionId": phone_session["sessionId"],
                        "phoneNumber": phone_session["phoneNumber"],
                    }
                phone_number_submitted = True
                phone_failure_stage = "wait_sms_code"
            sms_code = runtime_sms.wait_phone_code_for_session(
                session_id=phone_session["sessionId"],
                timeout_seconds=180,
            )
            phone_failure_stage = "submit_phone_verification_code"
            final_result = invoke_easyprotocol(
                step_type="submit_phone_verification_code",
                step_input={
                    "source_path": step_input.get("source_path"),
                    "resume_context": resume_context,
                    "sms_code": sms_code,
                    "phone_session_id": phone_session["sessionId"],
                },
            )
            runtime_sms.report_phone_outcome_for_session(
                session_id=phone_session["sessionId"],
                outcome="success",
                detail="codex_oauth_completed",
            )
        except Exception as exc:
            runtime_sms.report_phone_outcome_for_session(
                session_id=phone_session["sessionId"],
                outcome="failure",
                detail=str(exc),
            )
            if phone_number_submitted:
                return {
                    "ok": True,
                    "status": "phone_verification_submitted_small_success",
                    "successPath": str(
                        initial_result.get("successPath")
                        or initial_result.get("sourcePath")
                        or step_input.get("source_path")
                        or ""
                    ).strip(),
                    "sourcePath": str(
                        step_input.get("source_path")
                        or initial_result.get("sourcePath")
                        or initial_result.get("successPath")
                        or ""
                    ).strip(),
                    "pageType": str((resume_context or {}).get("pageType") or "").strip() or "add_phone",
                    "resumeContext": dict(resume_context),
                    "phoneVerificationAttempted": True,
                    "phoneVerificationSubmitted": True,
                    "phoneVerificationAccepted": False,
                    "phoneVerificationFailureStage": phone_failure_stage,
                    "phoneVerificationFailureDetail": str(exc),
                    "phoneProvider": phone_session["providerKey"],
                    "phoneSessionId": phone_session["sessionId"],
                    "phoneNumber": phone_session["phoneNumber"],
                }
            raise
        if isinstance(final_result, dict):
            final_result["phoneVerificationAttempted"] = True
            final_result["phoneProvider"] = phone_session["providerKey"]
            final_result["phoneSessionId"] = phone_session["sessionId"]
        return final_result
    return initial_result
