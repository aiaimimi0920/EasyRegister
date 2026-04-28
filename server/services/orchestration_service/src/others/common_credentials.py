from __future__ import annotations

import base64
import json
from typing import Any

from others.common_runtime import env_flag_value


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
