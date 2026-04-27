from __future__ import annotations

import base64
import json
import os
import time
import uuid
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


def sanitize_filename_component(value: str, *, fallback: str) -> str:
    text = str(value or "").strip()
    if not text:
        return fallback
    for bad in ('<', '>', ':', '"', '/', "\\", "|", "?", "*"):
        text = text.replace(bad, "_")
    text = text.strip().strip(".")
    return text or fallback


def short_account_id_segment(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    for separator in ("-", "_"):
        if separator in text:
            head = text.split(separator, 1)[0].strip()
            if head:
                return head
    return text[:8].strip()


def decode_jwt_payload(token: str) -> dict[str, Any]:
    raw = str(token or "").strip()
    if not raw:
        return {}
    parts = raw.split(".")
    if len(parts) < 2:
        return {}
    payload = parts[1].strip()
    if not payload:
        return {}
    padding = "=" * (-len(payload) % 4)
    try:
        decoded = base64.urlsafe_b64decode((payload + padding).encode("utf-8"))
        claims = json.loads(decoded.decode("utf-8"))
    except Exception:
        return {}
    return dict(claims) if isinstance(claims, dict) else {}


def extract_auth_claims(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    auth_sources: list[dict[str, Any]] = []
    auth_payload = payload.get("auth")
    if isinstance(auth_payload, dict):
        auth_sources.append(auth_payload)
    auth_sources.append(payload)
    for source in auth_sources:
        nested = source.get("https://api.openai.com/auth")
        if isinstance(nested, dict):
            return dict(nested)
    for source in auth_sources:
        for token_key in ("id_token", "access_token"):
            token = source.get(token_key)
            if not isinstance(token, str) or not token.strip():
                continue
            claims = decode_jwt_payload(token)
            nested = claims.get("https://api.openai.com/auth")
            if isinstance(nested, dict):
                return dict(nested)
    return {}


def extract_profile_claims(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    auth_sources: list[dict[str, Any]] = []
    auth_payload = payload.get("auth")
    if isinstance(auth_payload, dict):
        auth_sources.append(auth_payload)
    auth_sources.append(payload)
    for source in auth_sources:
        nested = source.get("https://api.openai.com/profile")
        if isinstance(nested, dict):
            return dict(nested)
    for source in auth_sources:
        for token_key in ("id_token", "access_token"):
            token = source.get(token_key)
            if not isinstance(token, str) or not token.strip():
                continue
            claims = decode_jwt_payload(token)
            nested = claims.get("https://api.openai.com/profile")
            if isinstance(nested, dict):
                return dict(nested)
    return {}


def extract_string_field(payload: dict[str, Any], *keys: str) -> str:
    sources: list[dict[str, Any]] = []
    auth_payload = payload.get("auth")
    if isinstance(auth_payload, dict):
        sources.append(auth_payload)
    sources.append(payload)
    for key in keys:
        for source in sources:
            value = source.get(key)
            if value is None:
                continue
            text = str(value).strip()
            if text:
                return text
    return ""


def extract_bool_field(payload: dict[str, Any], key: str, default: bool = False) -> bool:
    sources: list[dict[str, Any]] = []
    auth_payload = payload.get("auth")
    if isinstance(auth_payload, dict):
        sources.append(auth_payload)
    sources.append(payload)
    for source in sources:
        if key not in source:
            continue
        value = source.get(key)
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        parsed = env_flag_value(value, default=None)
        if parsed is not None:
            return parsed
    return default


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


def extract_account_id(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    direct = str(
        payload.get("accountId")
        or payload.get("account_id")
        or payload.get("chatgpt_account_id")
        or ""
    ).strip()
    if direct:
        return direct

    auth_payload = payload.get("auth")
    if isinstance(auth_payload, dict):
        nested_direct = str(
            auth_payload.get("account_id")
            or auth_payload.get("chatgpt_account_id")
            or ((auth_payload.get("https://api.openai.com/auth") or {}).get("chatgpt_account_id"))
            or ((auth_payload.get("https://api.openai.com/auth") or {}).get("account_id"))
            or ""
        ).strip()
        if nested_direct:
            return nested_direct

    nested = payload.get("https://api.openai.com/auth")
    if isinstance(nested, dict):
        nested_direct = str(
            nested.get("chatgpt_account_id")
            or nested.get("account_id")
            or ""
        ).strip()
        if nested_direct:
            return nested_direct

    return ""


def extract_organizations(payload: Any) -> list[dict[str, Any]]:
    auth_claims = extract_auth_claims(payload)
    organizations = auth_claims.get("organizations") if isinstance(auth_claims, dict) else None
    if isinstance(organizations, list):
        return [item for item in organizations if isinstance(item, dict)]
    if isinstance(payload, dict):
        auth_payload = payload.get("auth")
        if isinstance(auth_payload, dict):
            nested_orgs = auth_payload.get("organizations")
            if isinstance(nested_orgs, list):
                return [item for item in nested_orgs if isinstance(item, dict)]
        direct_orgs = payload.get("organizations")
        if isinstance(direct_orgs, list):
            return [item for item in direct_orgs if isinstance(item, dict)]
    return []


def extract_email(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    return str(payload.get("email") or "").strip()


def extract_org_id(payload: Any) -> str:
    account_id = extract_account_id(payload)
    if account_id:
        return account_id
    for organization in extract_organizations(payload):
        org_id = str(organization.get("id") or "").strip()
        if org_id:
            return org_id
    return ""


def canonical_free_artifact_name(payload: dict[str, Any]) -> str:
    org_id = sanitize_filename_component(
        short_account_id_segment(extract_org_id(payload)),
        fallback="unknown-org",
    )
    email = sanitize_filename_component(extract_email(payload), fallback="unknown-email")
    return f"codex-free-{org_id}-{email}.json"


def canonical_team_artifact_name(payload: dict[str, Any], *, is_mother: bool) -> str:
    org_id = sanitize_filename_component(
        short_account_id_segment(extract_org_id(payload)),
        fallback="unknown-org",
    )
    email = sanitize_filename_component(extract_email(payload), fallback="unknown-email")
    prefix = "codex-team-mother" if is_mother else "codex-team"
    return f"{prefix}-{org_id}-{email}.json"


def standardize_export_credential_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}

    auth_claims = extract_auth_claims(payload)
    profile_claims = extract_profile_claims(payload)
    account_id = extract_account_id(payload)
    email = extract_string_field(payload, "email")
    if not email and profile_claims:
        email = str(profile_claims.get("email") or "").strip()

    standardized: dict[str, Any] = {
        "type": extract_string_field(payload, "type") or "codex",
        "email": email,
        "account_id": account_id,
        "access_token": extract_string_field(payload, "access_token"),
        "refresh_token": extract_string_field(payload, "refresh_token"),
        "id_token": extract_string_field(payload, "id_token"),
        "expired": extract_string_field(payload, "expired"),
        "disabled": extract_bool_field(payload, "disabled", False),
        "last_refresh": extract_string_field(payload, "last_refresh"),
    }

    if auth_claims:
        standardized["https://api.openai.com/auth"] = auth_claims
    if profile_claims:
        standardized["https://api.openai.com/profile"] = profile_claims

    return standardized


def team_mother_cooldown_key(*, original_name: str, email: str, account_id: str) -> str:
    normalized_email = sanitize_filename_component(
        str(email or "").strip().lower(),
        fallback="unknown-email",
    )
    normalized_account = sanitize_filename_component(
        short_account_id_segment(account_id),
        fallback="unknown-account",
    )
    if normalized_email != "unknown-email" or normalized_account != "unknown-account":
        return f"{normalized_account}-{normalized_email}"
    return sanitize_filename_component(
        str(original_name or "").strip().lower(),
        fallback="unknown-mother",
    )


def write_json_atomic(
    path: Path,
    payload: dict[str, Any],
    *,
    json_default: Any | None = None,
    sort_keys: bool = False,
    include_pid: bool = False,
    cleanup_temp: bool = False,
) -> None:
    ensure_directory(path.parent)
    temp_name = f"{path.name}."
    if include_pid:
        temp_name += f"{os.getpid()}."
    temp_name += f"{uuid.uuid4().hex}.tmp"
    tmp_path = path.parent / temp_name
    try:
        tmp_path.write_text(
            json.dumps(
                payload,
                ensure_ascii=False,
                indent=2,
                default=json_default,
                sort_keys=sort_keys,
            ),
            encoding="utf-8",
        )
        os.replace(tmp_path, path)
    finally:
        if cleanup_temp and tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
