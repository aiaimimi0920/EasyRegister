from __future__ import annotations

import re
from typing import Any, Callable

from artifact_pool_flow import dispatch_orchestration_step
from easyemail_flow import dispatch_easyemail_step
from easyprotocol_flow import dispatch_easyprotocol_step
from easyproxy_flow import dispatch_easyproxy_step
from errors import ErrorCodes, build_error_details
from others.dst_flow_models import DstStatement


PLACEHOLDER_RE = re.compile(r"^\{\{\s*([a-zA-Z0-9_]+(?:\.[a-zA-Z0-9_]+)*)\s*\}\}$")

OWNER_DISPATCHERS: dict[str, Callable[..., dict[str, Any]]] = {
    "orchestration": dispatch_orchestration_step,
    "easyemail": dispatch_easyemail_step,
    "easyproxy": dispatch_easyproxy_step,
    "easyprotocol": dispatch_easyprotocol_step,
}


def step_output_ok(*, step_type: str, step_output: Any) -> tuple[bool, str]:
    normalized_step_type = str(step_type or "").strip()
    if normalized_step_type == "upload_file_to_r2":
        if isinstance(step_output, dict) and bool(step_output.get("ok")):
            return True, ""
        return False, str((step_output or {}).get("detail") or "upload_file_to_r2_failed").strip()
    if normalized_step_type == "obtain_team_mother_oauth":
        if isinstance(step_output, dict) and (
            bool(step_output.get("ok")) or bool(str(step_output.get("successPath") or "").strip())
        ):
            return True, ""
        return False, str(
            (step_output or {}).get("detail")
            or (step_output or {}).get("status")
            or "obtain_team_mother_oauth_failed"
        ).strip()
    if normalized_step_type in {
        "initialize_platform_organization",
        "initialize_chatgpt_login_session",
        "invite_codex_member",
        "revoke_codex_member",
        "invite_team_members",
        "obtain_team_member_oauth_batch",
        "revoke_team_members",
    }:
        if isinstance(step_output, dict) and bool(step_output.get("ok")):
            return True, ""
        return False, str(
            (step_output or {}).get("detail")
            or (step_output or {}).get("status")
            or f"{normalized_step_type}_failed"
        ).strip()
    if normalized_step_type == "release_proxy_chain":
        if isinstance(step_output, dict) and bool(step_output.get("released")):
            return True, ""
        return False, str((step_output or {}).get("detail") or "release_proxy_chain_failed").strip()
    if normalized_step_type == "release_mailbox":
        if isinstance(step_output, dict):
            released = step_output.get("released")
            detail = str(step_output.get("detail") or "").strip().lower()
            if released is True:
                return True, ""
            if detail in {
                "deleted",
                "not_found",
                "already_deleted",
                "provider_does_not_support_release",
                "skipped_non_moemail",
                "skipped_preserved_for_manual_oauth",
            }:
                return True, ""
            return False, str(step_output.get("detail") or "release_mailbox_failed").strip()
        return False, "release_mailbox_invalid_output"
    if normalized_step_type in {
        "fill_team_pre_pool",
        "acquire_team_mother_artifact",
        "acquire_team_member_candidates",
        "collect_team_pool_artifacts",
        "finalize_team_batch",
        "acquire_small_success_artifact",
        "finalize_small_success_artifact",
        "validate_free_personal_oauth",
        "sleep_seconds",
    }:
        if isinstance(step_output, dict) and bool(step_output.get("ok")):
            return True, ""
        return False, str(
            (step_output or {}).get("code")
            or (step_output or {}).get("detail")
            or (step_output or {}).get("status")
            or f"{normalized_step_type}_failed"
        ).strip()
    return True, ""


def step_always_run(statement: DstStatement) -> bool:
    return bool(statement.metadata.get("alwaysRun"))


def step_retry_policy(statement: DstStatement) -> dict[str, Any]:
    retry = statement.metadata.get("retry")
    return retry if isinstance(retry, dict) else {}


def step_error_details(*, step_type: str, exc: BaseException) -> dict[str, Any]:
    return build_error_details(
        step_type=step_type,
        message=str(exc or "").strip(),
        detail=str(getattr(exc, "detail", "") or "").strip(),
        stage=str(getattr(exc, "stage", "") or "").strip(),
        category=str(getattr(exc, "category", "") or "").strip(),
        code=str(getattr(exc, "code", "") or "").strip(),
    )


def resolve_placeholder(path_text: str, state: dict[str, Any]) -> Any:
    current: Any = state
    for part in path_text.split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return ""
    return current


def resolve_value(value: Any, state: dict[str, Any]) -> Any:
    if isinstance(value, str):
        match = PLACEHOLDER_RE.match(value.strip())
        if match:
            return resolve_placeholder(match.group(1), state)
        return value
    if isinstance(value, dict):
        return {key: resolve_value(inner, state) for key, inner in value.items()}
    if isinstance(value, list):
        return [resolve_value(item, state) for item in value]
    return value
