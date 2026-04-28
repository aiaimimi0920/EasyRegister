from __future__ import annotations

from typing import Any

from others.error_catalog import classify_error_code
from others.error_catalog import infer_category_from_code
from others.error_catalog import infer_category_from_message
from others.error_catalog import normalize_error_category
from others.error_catalog import normalize_error_code


class ProtocolRuntimeError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        stage: str = "stage_other",
        detail: str = "runtime_error",
        category: str | None = None,
        code: str | None = None,
    ) -> None:
        super().__init__(message)
        self.stage = str(stage or "stage_other").strip() or "stage_other"
        self.detail = str(detail or "runtime_error").strip() or "runtime_error"
        normalized_code = normalize_error_code(code)
        self.code = normalized_code or None
        normalized_category = normalize_error_category(
            category or infer_category_from_code(normalized_code)
        )
        self.category = normalized_category or None

    def to_response_payload(self) -> dict[str, str]:
        payload = {
            "error": str(self),
            "stage": self.stage,
            "detail": self.detail,
        }
        if self.code:
            payload["code"] = self.code
        if self.category:
            payload["category"] = self.category
        return payload


def build_error_details(
    *,
    step_type: str,
    message: str,
    detail: str = "",
    stage: str = "",
    category: str = "",
    code: str = "",
) -> dict[str, str]:
    final_code = classify_error_code(
        step_type=step_type,
        message=message,
        detail=detail,
        code=code,
    )
    final_category = normalize_error_category(category) or infer_category_from_code(final_code) or infer_category_from_message(message)
    return {
        "code": final_code,
        "message": str(message or "").strip(),
        "detail": str(detail or "").strip(),
        "stage": str(stage or "").strip(),
        "category": final_category,
    }


def result_error_step(result_payload: dict[str, Any]) -> str:
    return str(result_payload.get("errorStep") or "").strip()


def result_step_error(result_payload: dict[str, Any], step_id: str | None = None) -> dict[str, Any]:
    if not isinstance(result_payload, dict):
        return {}
    target_step = str(step_id or result_error_step(result_payload) or "").strip()
    if not target_step:
        return {}
    step_errors = result_payload.get("stepErrors")
    if not isinstance(step_errors, dict):
        return {}
    candidate = step_errors.get(target_step)
    return candidate if isinstance(candidate, dict) else {}


def result_error_code(result_payload: dict[str, Any], step_id: str | None = None) -> str:
    top_level_code = normalize_error_code(result_payload.get("errorCode") if isinstance(result_payload, dict) else "")
    if top_level_code and (step_id is None or str(step_id or "").strip() == result_error_step(result_payload)):
        return top_level_code
    return normalize_error_code(result_step_error(result_payload, step_id).get("code"))


def result_error_message(result_payload: dict[str, Any], step_id: str | None = None) -> str:
    if not isinstance(result_payload, dict):
        return ""
    target_step = str(step_id or "").strip()
    message_parts: list[str] = []
    if not target_step or target_step == result_error_step(result_payload):
        message_parts.append(str(result_payload.get("error") or "").strip())
    step_error = result_step_error(result_payload, step_id)
    message_parts.append(str(step_error.get("message") or "").strip())
    message_parts.append(str(step_error.get("code") or "").strip())
    return " ".join(part for part in message_parts if part).strip()


def result_error_matches(result_payload: dict[str, Any], *codes: str, step_id: str | None = None) -> bool:
    normalized_codes = {normalize_error_code(item) for item in codes if normalize_error_code(item)}
    if not normalized_codes:
        return False
    return result_error_code(result_payload, step_id) in normalized_codes


def ensure_protocol_runtime_error(
    exc: BaseException,
    *,
    stage: str,
    detail: str,
    category: str | None = None,
    code: str | None = None,
) -> ProtocolRuntimeError:
    if isinstance(exc, ProtocolRuntimeError):
        if not exc.stage:
            exc.stage = str(stage or "stage_other").strip() or "stage_other"
        if not exc.detail:
            exc.detail = str(detail or "runtime_error").strip() or "runtime_error"
        if not exc.code:
            exc.code = classify_error_code(
                step_type="",
                message=str(exc),
                detail=exc.detail,
                code=str(code or ""),
            ) or None
        if not exc.category:
            normalized_category = normalize_error_category(category) or infer_category_from_code(str(exc.code or "")) or infer_category_from_message(str(exc))
            exc.category = normalized_category or None
        return exc

    message = str(exc or detail or "protocol_runtime_error").strip() or "protocol_runtime_error"
    final_code = classify_error_code(
        step_type="",
        message=message,
        detail=detail,
        code=str(code or ""),
    )
    final_category = normalize_error_category(category) or infer_category_from_code(final_code) or infer_category_from_message(message)
    return ProtocolRuntimeError(
        message,
        stage=stage,
        detail=detail,
        category=final_category,
        code=final_code,
    )
