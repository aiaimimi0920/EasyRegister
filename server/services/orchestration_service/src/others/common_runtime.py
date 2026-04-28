from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_FREE_MANUAL_OAUTH_PRESERVE_ERROR_CODES = (
    "free_personal_workspace_missing,obtain_codex_oauth_failed"
)


def env_flag(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name) or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "on"}


def ensure_directory(path: Path) -> None:
    target = Path(path)
    for _ in range(5):
        try:
            os.makedirs(target, exist_ok=True)
            if target.is_dir():
                return
        except FileExistsError:
            if target.is_dir():
                return
            raise
        except FileNotFoundError:
            time.sleep(0.02)
            continue
        time.sleep(0.02)
    os.makedirs(target, exist_ok=True)


def json_log(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False), flush=True)


def env_flag_value(value: Any, default: bool | None = False) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off", ""}:
        return False
    return default


def free_manual_oauth_preserve_enabled(step_input: dict[str, Any] | None = None) -> bool:
    raw = str((step_input or {}).get("free_manual_oauth_preserve_enabled") or "").strip()
    if raw:
        return bool(env_flag_value(raw, default=False))
    return env_flag("REGISTER_FREE_MANUAL_OAUTH_PRESERVE_ENABLED", False)


def free_manual_oauth_preserve_codes(step_input: dict[str, Any] | None = None) -> set[str]:
    raw = str(
        (step_input or {}).get("free_manual_oauth_preserve_error_codes")
        or os.environ.get("REGISTER_FREE_MANUAL_OAUTH_PRESERVE_ERROR_CODES")
        or DEFAULT_FREE_MANUAL_OAUTH_PRESERVE_ERROR_CODES
    ).strip()
    if not raw:
        return set()
    return {item.strip() for item in raw.split(",") if item.strip()}


def validate_small_success_seed_payload(payload: dict[str, Any]) -> tuple[bool, str]:
    if not isinstance(payload, dict):
        return False, "payload_not_object"
    platform_org = payload.get("platformOrganization")
    if not isinstance(platform_org, dict):
        return False, "missing_platform_organization"
    if str(platform_org.get("status") or "").strip().lower() != "completed":
        return False, "platform_organization_not_completed"

    chatgpt_login = payload.get("chatgptLogin")
    if not isinstance(chatgpt_login, dict):
        return False, "missing_chatgpt_login"
    if str(chatgpt_login.get("status") or "").strip().lower() != "completed":
        return False, "chatgpt_login_not_completed"

    personal_workspace_id = str(
        chatgpt_login.get("personalWorkspaceId")
        or chatgpt_login.get("workspaceId")
        or ""
    ).strip()
    if not personal_workspace_id:
        return False, "missing_personal_workspace_id"

    login_details = payload.get("chatgptLoginDetails")
    if isinstance(login_details, dict):
        client_bootstrap = login_details.get("clientBootstrap")
        if isinstance(client_bootstrap, dict):
            auth_status = str(client_bootstrap.get("authStatus") or "").strip().lower()
            structure = str(client_bootstrap.get("structure") or "").strip().lower()
            if auth_status and auth_status != "logged_in":
                return False, "chatgpt_login_not_logged_in"
            if structure and structure != "personal":
                return False, "chatgpt_login_not_personal"

    mailbox_ref = str(payload.get("mailboxRef") or "").strip()
    if not mailbox_ref:
        return False, "missing_mailbox_ref"

    mailbox_session_id = str(payload.get("mailboxSessionId") or "").strip()
    if not mailbox_session_id:
        return False, "missing_mailbox_session_id"

    created_at_text = str(payload.get("createdAt") or "").strip()
    if not created_at_text:
        return False, "missing_created_at"
    try:
        parsed_created_at = datetime.fromisoformat(created_at_text.replace("Z", "+00:00"))
        if parsed_created_at.tzinfo is None:
            parsed_created_at = parsed_created_at.replace(tzinfo=timezone.utc)
        parsed_created_at = parsed_created_at.astimezone(timezone.utc)
    except Exception:
        return False, "invalid_created_at"

    max_age_raw = str(
        os.environ.get("REGISTER_SMALL_SUCCESS_SEED_MAX_AGE_SECONDS")
        or os.environ.get("REGISTER_TEAM_MEMBER_SEED_MAX_AGE_SECONDS")
        or "900"
    ).strip()
    try:
        max_age_seconds = max(0, int(float(max_age_raw)))
    except Exception:
        max_age_seconds = 900
    if max_age_seconds > 0:
        age_seconds = max(0.0, (datetime.now(timezone.utc) - parsed_created_at).total_seconds())
        if age_seconds > max_age_seconds:
            return False, f"small_success_seed_too_old:{int(age_seconds)}"

    return True, ""
