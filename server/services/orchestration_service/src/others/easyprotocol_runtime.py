from __future__ import annotations

import json
import os
import shutil
import time
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
DEFAULT_EASY_PROTOCOL_OAUTH_TIMEOUT_SECONDS = 240
DEFAULT_EASY_PROTOCOL_PHONE_TIMEOUT_SECONDS = 120
DEFAULT_PROTOCOL_OUTPUT_TARGET_DIR = "/shared/register-output"
DEFAULT_PROTOCOL_BRIDGE_SUBDIR = "easyregister-bridge"
DEFAULT_PHONE_VERIFICATION_TERMINAL_RETRY_ATTEMPTS = 5
PHONE_VERIFICATION_RETRYABLE_TERMINAL_CODES = {
    "invalid_phone_number",
    "phone_number_in_use",
    "phone_max_usage_exceeded",
    "rate_limit_exceeded",
    "wrong_otp_code",
}
PHONE_VERIFICATION_SUBMIT_EXCEPTION_TERMINAL_MARKERS = (
    ("invalid_phone_number", ("invalid_phone_number", "invalid phone number")),
    ("phone_number_in_use", ("phone_number_in_use", "already used", "already in use", "number in use")),
    ("phone_max_usage_exceeded", ("phone_max_usage_exceeded", "max usage", "maximum usage")),
    ("rate_limit_exceeded", ("rate_limit_exceeded", "rate limit", "too many phone verification", "429", "status=403")),
)
PHONE_WALL_RECOVERY_ERROR_MARKERS = (
    "flow_timeout_exceeded",
    "timed out",
    "timeout",
    "easyprotocol_transport_failed",
)


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


def easyprotocol_oauth_timeout_seconds() -> int:
    raw = str(os.environ.get("EASY_PROTOCOL_OAUTH_TIMEOUT_SECONDS") or "").strip()
    if raw:
        try:
            return max(1, int(float(raw)))
        except Exception:
            return DEFAULT_EASY_PROTOCOL_OAUTH_TIMEOUT_SECONDS
    return min(easyprotocol_timeout_seconds(), DEFAULT_EASY_PROTOCOL_OAUTH_TIMEOUT_SECONDS)


def easyprotocol_phone_timeout_seconds() -> int:
    raw = str(os.environ.get("EASY_PROTOCOL_PHONE_TIMEOUT_SECONDS") or "").strip()
    if raw:
        try:
            return max(1, int(float(raw)))
        except Exception:
            return DEFAULT_EASY_PROTOCOL_PHONE_TIMEOUT_SECONDS
    return min(easyprotocol_timeout_seconds(), DEFAULT_EASY_PROTOCOL_PHONE_TIMEOUT_SECONDS)


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


def _phone_submit_terminal_code_from_exception(exc: BaseException) -> str:
    message = str(exc or "").strip().lower()
    if not message:
        return ""
    for terminal_code, markers in PHONE_VERIFICATION_SUBMIT_EXCEPTION_TERMINAL_MARKERS:
        if any(marker in message for marker in markers):
            return terminal_code
    return ""


def _is_retryable_phone_code_submission_error(exc: BaseException) -> bool:
    message = str(exc or "").strip().lower()
    if not message:
        return False
    return any(
        marker in message
        for marker in (
            "wrong_email_otp_code",
            "otp_incorrect",
            "wrong otp",
            "wrong code",
            "incorrect code",
            "invalid verification code",
            "phone_code_submit_failed",
        )
    )


def _is_retryable_phone_code_wait_error(exc: BaseException) -> bool:
    message = str(exc or "").strip().lower()
    if not message:
        return False
    return any(
        marker in message
        for marker in (
            "timeout waiting for sms verification code",
            "wait_code_timeout",
            "wait_sms_code_timeout",
            "sms_code_timeout",
        )
    )


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

    source_path = resolve_protocol_artifact_path(storage_path_text)
    if source_path is None or not source_path.is_file():
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


def _split_relative_path_from_root(*, path_text: str, root_text: str) -> str | None:
    path_value = str(path_text or "").strip()
    root_value = str(root_text or "").strip().rstrip("/\\")
    if not path_value or not root_value:
        return None
    if path_value == root_value:
        return ""
    for separator in ("/", "\\"):
        prefix = f"{root_value}{separator}"
        if path_value.startswith(prefix):
            return path_value[len(prefix) :]
    return None


def _join_path_text(*, root_text: str, relative_text: str) -> str:
    root_value = str(root_text or "").strip().rstrip("/\\")
    relative_value = str(relative_text or "").strip().strip("/\\")
    if not relative_value:
        return root_value
    relative_parts = [part for part in relative_value.replace("\\", "/").split("/") if part]
    if "\\" in root_value and "/" not in root_value:
        return str(Path(root_value).joinpath(*relative_parts))
    return f"{root_value}/{'/'.join(relative_parts)}"


def resolve_protocol_artifact_path(path_text: str) -> Path | None:
    direct_path = Path(str(path_text or "").strip()).expanduser()
    if direct_path.is_file():
        return direct_path

    protocol_target_dir = str(
        os.environ.get("REGISTER_PROTOCOL_OUTPUT_TARGET_DIR") or DEFAULT_PROTOCOL_OUTPUT_TARGET_DIR
    ).strip()
    protocol_mirror_dir = str(os.environ.get("REGISTER_PROTOCOL_OUTPUT_MIRROR_DIR") or "").strip()
    relative_path = _split_relative_path_from_root(
        path_text=str(path_text or "").strip(),
        root_text=protocol_target_dir,
    )
    if relative_path is None or not protocol_mirror_dir:
        return None

    mirrored_path = Path(
        _join_path_text(
            root_text=protocol_mirror_dir,
            relative_text=relative_path,
        )
    ).expanduser()
    if mirrored_path.is_file():
        return mirrored_path
    return None


def _protocol_bridge_target_dir_text(*, bridge_root: Path) -> str:
    target_dir_text = str(os.environ.get("REGISTER_PROTOCOL_BRIDGE_TARGET_DIR") or "").strip()
    if target_dir_text:
        return target_dir_text.rstrip("/\\")
    return str(bridge_root.resolve())


def _protocol_bridge_root_text_for_source(*, source_path_text: str) -> str:
    bridge_root_text = str(os.environ.get("REGISTER_PROTOCOL_BRIDGE_DIR") or "").strip()
    if bridge_root_text:
        return bridge_root_text

    protocol_target_dir = str(
        os.environ.get("REGISTER_PROTOCOL_OUTPUT_TARGET_DIR") or DEFAULT_PROTOCOL_OUTPUT_TARGET_DIR
    ).strip()
    if not protocol_target_dir:
        return ""
    relative_path = _split_relative_path_from_root(
        path_text=source_path_text,
        root_text=protocol_target_dir,
    )
    if relative_path is None:
        return ""
    return _join_path_text(
        root_text=protocol_target_dir,
        relative_text=DEFAULT_PROTOCOL_BRIDGE_SUBDIR,
    )


def _join_bridge_target_path(*, target_dir_text: str, file_name: str) -> str:
    if "\\" in target_dir_text and "/" not in target_dir_text:
        return str(Path(target_dir_text) / file_name)
    return f"{target_dir_text.rstrip('/')}/{file_name}"


def maybe_bridge_source_path_for_protocol(*, step_input: dict[str, Any]) -> tuple[dict[str, Any], dict[str, str]]:
    source_path_text = str(step_input.get("source_path") or "").strip()
    if not source_path_text:
        return step_input, {}
    bridge_root_text = _protocol_bridge_root_text_for_source(source_path_text=source_path_text)
    if not bridge_root_text:
        return step_input, {}

    source_path = Path(source_path_text).expanduser()
    if not source_path.is_file():
        return step_input, {}

    bridge_root = Path(bridge_root_text).expanduser()
    bridge_root.mkdir(parents=True, exist_ok=True)
    local_bridge_path = (bridge_root / source_path.name).resolve()
    if source_path.resolve() != local_bridge_path:
        shutil.copy2(source_path, local_bridge_path)

    target_dir_text = _protocol_bridge_target_dir_text(bridge_root=bridge_root)
    target_source_path = _join_bridge_target_path(
        target_dir_text=target_dir_text,
        file_name=source_path.name,
    )
    bridged_input = dict(step_input)
    bridged_input["source_path"] = target_source_path
    return bridged_input, {
        "original_source_path": str(source_path.resolve()),
        "local_bridge_path": str(local_bridge_path),
        "target_source_path": target_source_path,
    }


def sync_protocol_source_bridge_back(*, bridge_info: dict[str, str]) -> None:
    if not bridge_info:
        return
    original_source_path = Path(str(bridge_info.get("original_source_path") or "")).expanduser()
    local_bridge_path = Path(str(bridge_info.get("local_bridge_path") or "")).expanduser()
    if not original_source_path or not local_bridge_path.is_file():
        return
    original_source_path.parent.mkdir(parents=True, exist_ok=True)
    if original_source_path.resolve() != local_bridge_path.resolve():
        shutil.copy2(local_bridge_path, original_source_path)


def rewrite_protocol_source_bridge_result_paths(
    *,
    step_result: dict[str, Any],
    bridge_info: dict[str, str],
) -> dict[str, Any]:
    if not bridge_info:
        return step_result
    original_source_path = str(bridge_info.get("original_source_path") or "").strip()
    target_source_path = str(bridge_info.get("target_source_path") or "").strip()
    local_bridge_path = str(bridge_info.get("local_bridge_path") or "").strip()
    if not original_source_path or not target_source_path:
        return step_result

    rewritten = dict(step_result)
    for key in (
        "sourcePath",
        "source_path",
        "successPath",
        "success_path",
        "storage_path",
        "output_path",
    ):
        value = str(rewritten.get(key) or "").strip()
        if value and value in {target_source_path, local_bridge_path}:
            rewritten[key] = original_source_path
    return rewritten


def validate_login_session_handoff_for_oauth(step_input: dict[str, Any]) -> None:
    if "login_session" not in step_input:
        return
    login_session = step_input.get("login_session")
    if not isinstance(login_session, dict) or not login_session:
        raise RuntimeError("authorize_missing_login_session:login_session_handoff_missing")
    if login_session.get("ok") is False:
        detail = str(login_session.get("detail") or login_session.get("status") or "login_session_not_ok").strip()
        raise RuntimeError(f"authorize_missing_login_session:{detail}")


def _looks_like_phone_wall_recovery_error(exc: BaseException) -> bool:
    message = str(exc or "").strip().lower()
    return bool(message) and any(marker in message for marker in PHONE_WALL_RECOVERY_ERROR_MARKERS)


def _looks_like_phone_wall_result(result: dict[str, Any]) -> bool:
    if bool(result.get("phoneVerificationRequired")):
        return True
    resume_context = result.get("resumeContext")
    if not isinstance(resume_context, dict) or not resume_context:
        return False
    outcome = str(result.get("outcome") or "").strip().lower()
    status = str(result.get("status") or "").strip().lower()
    page_type = str(result.get("pageType") or result.get("page_type") or "").strip().lower()
    return outcome == "phone_wall" or status == "phone_verification_required" or "phone" in page_type


def _normalize_phone_wall_result(result: dict[str, Any]) -> dict[str, Any]:
    if not _looks_like_phone_wall_result(result):
        return result
    normalized = dict(result)
    normalized["ok"] = True
    normalized["status"] = str(normalized.get("status") or "").strip() or "phone_verification_required"
    normalized["phoneVerificationRequired"] = True
    normalized["pageType"] = str(normalized.get("pageType") or normalized.get("page_type") or "").strip() or "add_phone"
    return normalized


def _phone_wall_artifact_base_dirs(step_input: dict[str, Any]) -> list[Path]:
    bases: list[Path] = []
    for key in ("output_dir", "outputDir"):
        value = str(step_input.get(key) or "").strip()
        if value:
            bases.append(Path(value).expanduser())
    source_path_text = str(step_input.get("source_path") or step_input.get("sourcePath") or "").strip()
    if source_path_text:
        source_path = resolve_protocol_artifact_path(source_path_text) or Path(source_path_text).expanduser()
        if source_path.parent:
            bases.append(source_path.parent.parent if source_path.parent.name == "small_success" else source_path.parent)

    deduped: list[Path] = []
    seen: set[str] = set()
    for base in bases:
        try:
            key = str(base.resolve())
        except Exception:
            key = str(base)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(base)
    return deduped


def _load_latest_phone_wall_artifact_payload(step_input: dict[str, Any]) -> dict[str, Any] | None:
    candidates: list[Path] = []
    for base in _phone_wall_artifact_base_dirs(step_input):
        first_phone_dir = base / "first_phone"
        if not first_phone_dir.is_dir():
            continue
        for path in first_phone_dir.glob("*.json"):
            if path.is_file():
                candidates.append(path)
    if not candidates:
        return None

    candidates.sort(key=lambda item: item.stat().st_mtime, reverse=True)
    for path in candidates:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        resume_context = payload.get("resumeContext")
        if not isinstance(resume_context, dict) or not resume_context:
            continue
        outcome = str(payload.get("outcome") or "").strip().lower()
        page_type = str(payload.get("pageType") or payload.get("page_type") or "").strip().lower()
        if outcome != "phone_wall" and "phone" not in page_type:
            continue
        return {
            "ok": True,
            "status": "phone_verification_required",
            "phoneVerificationRequired": True,
            "pageType": str(payload.get("pageType") or payload.get("page_type") or "").strip() or "add_phone",
            "finalUrl": str(payload.get("finalUrl") or payload.get("final_url") or "").strip(),
            "resumeContext": dict(resume_context),
            "successPath": str(path),
            "sourcePath": str(step_input.get("source_path") or step_input.get("sourcePath") or "").strip(),
            "recoveredFromPhoneWallArtifact": True,
            "phoneWallArtifactPath": str(path),
            "phoneWallArtifactMtime": int(path.stat().st_mtime),
            "phoneWallArtifactRecoveredAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
    return None


def invoke_easyprotocol(
    *,
    step_type: str,
    step_input: dict[str, Any],
    timeout_seconds: int | None = None,
) -> dict[str, Any]:
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
        normalized_step_type = str(step_type or "").strip()
        if timeout_seconds is not None:
            request_timeout = max(1, int(timeout_seconds))
        elif normalized_step_type == "obtain_codex_oauth":
            request_timeout = easyprotocol_oauth_timeout_seconds()
        elif normalized_step_type in {
            "submit_phone_verification_number",
            "submit_phone_verification_code",
        }:
            request_timeout = easyprotocol_phone_timeout_seconds()
        else:
            request_timeout = easyprotocol_timeout_seconds()
        with urllib.request.urlopen(req, timeout=request_timeout) as resp:
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
        validate_login_session_handoff_for_oauth(normalized_step_input)
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
    bridged_step_input, source_bridge_info = maybe_bridge_source_path_for_protocol(
        step_input=normalized_step_input,
    )
    try:
        result = invoke_easyprotocol(
            step_type=normalized_step_type,
            step_input=bridged_step_input,
        )
        if normalized_step_type == "obtain_codex_oauth" and isinstance(result, dict):
            result = _maybe_complete_phone_verification_for_oauth(
                initial_result=result,
                step_input=bridged_step_input,
            )
    except Exception as exc:
        if normalized_step_type != "obtain_codex_oauth" or not _looks_like_phone_wall_recovery_error(exc):
            raise
        recovered_result = _load_latest_phone_wall_artifact_payload(bridged_step_input)
        if not isinstance(recovered_result, dict):
            raise
        result = _maybe_complete_phone_verification_for_oauth(
            initial_result=recovered_result,
            step_input=bridged_step_input,
        )
    finally:
        sync_protocol_source_bridge_back(bridge_info=source_bridge_info)
    if isinstance(result, dict):
        result = rewrite_protocol_source_bridge_result_paths(
            step_result=result,
            bridge_info=source_bridge_info,
        )
        return maybe_bridge_step_artifact(
            step_type=normalized_step_type,
            step_result=result,
        )
    return result


def _maybe_complete_phone_verification_for_oauth(*, initial_result: dict[str, Any], step_input: dict[str, Any]) -> dict[str, Any]:
    initial_result = _normalize_phone_wall_result(initial_result)
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
                        business_key=business_key,
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
            if not phone_number_submitted and phone_failure_stage == "submit_phone_verification_number":
                terminal_code = _phone_submit_terminal_code_from_exception(exc)
                if terminal_code:
                    runtime_sms.record_terminal_phone_outcome(
                        phone_number=phone_session["phoneNumber"],
                        provider_key=phone_session["providerKey"],
                        terminal_code=terminal_code,
                        terminal_message=str(exc),
                        business_key=business_key,
                    )
                    if (
                        _is_retryable_phone_terminal_code(terminal_code)
                        and phone_attempt_index + 1 < max_phone_attempts
                    ):
                        continue
                    return {
                        "ok": True,
                        "status": "phone_verification_terminal",
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
                        "phoneVerificationSubmitted": False,
                        "phoneVerificationAccepted": False,
                        "phoneVerificationTerminal": True,
                        "phoneVerificationTerminalCode": terminal_code,
                        "phoneVerificationTerminalMessage": str(exc),
                        "phoneProvider": phone_session["providerKey"],
                        "phoneSessionId": phone_session["sessionId"],
                        "phoneNumber": phone_session["phoneNumber"],
                    }
            if (
                phone_number_submitted
                and phone_failure_stage == "wait_sms_code"
                and _is_retryable_phone_code_wait_error(exc)
            ):
                runtime_sms.record_terminal_phone_outcome(
                    phone_number=phone_session["phoneNumber"],
                    provider_key=phone_session["providerKey"],
                    terminal_code="sms_code_timeout",
                    terminal_message=str(exc),
                    business_key=business_key,
                )
                if phone_attempt_index + 1 < max_phone_attempts:
                    continue
            if (
                phone_number_submitted
                and phone_failure_stage == "submit_phone_verification_code"
                and _is_retryable_phone_code_submission_error(exc)
            ):
                runtime_sms.record_terminal_phone_outcome(
                    phone_number=phone_session["phoneNumber"],
                    provider_key=phone_session["providerKey"],
                    terminal_code="wrong_otp_code",
                    terminal_message=str(exc),
                    business_key=business_key,
                )
                if phone_attempt_index + 1 < max_phone_attempts:
                    continue
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
            return {
                "ok": True,
                "status": "phone_verification_attempted_small_success",
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
                "phoneVerificationSubmitted": False,
                "phoneVerificationAccepted": False,
                "phoneVerificationFailureStage": phone_failure_stage,
                "phoneVerificationFailureDetail": str(exc),
                "phoneProvider": phone_session["providerKey"],
                "phoneSessionId": phone_session["sessionId"],
                "phoneNumber": phone_session["phoneNumber"],
            }
        if isinstance(final_result, dict):
            final_result["phoneVerificationAttempted"] = True
            final_result["phoneProvider"] = phone_session["providerKey"]
            final_result["phoneSessionId"] = phone_session["sessionId"]
        return final_result
    return initial_result
