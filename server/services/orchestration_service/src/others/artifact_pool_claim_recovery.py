from __future__ import annotations

import time
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from others.artifact_transfer import move_artifact_to_dir
from others.artifact_pool_paths import team_auth_runtime_config_for_step_input
from others.common import validate_openai_oauth_seed_payload
from others.storage import load_json_payload


def strip_generated_claim_prefixes(name: str) -> str:
    current_name = str(name or "").strip()
    while True:
        prefix, separator, remainder = current_name.partition("-")
        if not separator or len(prefix) != 8:
            return current_name
        try:
            int(prefix, 16)
        except ValueError:
            return current_name
        current_name = remainder


def load_openai_oauth_seed_validation(
    path: Path,
    *,
    enforce_max_age: bool = False,
    allow_protocol_small_seed: bool = False,
) -> tuple[bool, str, dict[str, Any]]:
    try:
        payload = load_json_payload(path)
    except Exception as exc:
        return False, f"load_failed:{exc}", {}
    ok, reason = validate_openai_oauth_seed_payload(
        payload,
        enforce_max_age=enforce_max_age,
    )
    if (not ok) and allow_protocol_small_seed:
        ok, reason = _validate_protocol_small_success_seed_payload(
            payload,
            enforce_max_age=enforce_max_age,
        )
    return ok, reason, payload


def _validate_protocol_small_success_seed_payload(
    payload: dict[str, Any],
    *,
    enforce_max_age: bool = False,
) -> tuple[bool, str]:
    if not isinstance(payload, dict):
        return False, "payload_not_object"
    if str(payload.get("outcome") or "").strip().lower() != "small_success":
        return False, "protocol_small_success_missing_outcome"
    if str(payload.get("source") or "").strip().lower() != "protocol_small_success":
        return False, "protocol_small_success_missing_source"
    if not str(payload.get("email") or "").strip():
        return False, "missing_email"
    if not str(payload.get("mailboxRef") or "").strip():
        return False, "missing_mailbox_ref"
    if not str(payload.get("mailboxSessionId") or "").strip():
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
    if enforce_max_age:
        max_age_raw = str(
            os.environ.get("REGISTER_OPENAI_OAUTH_SEED_MAX_AGE_SECONDS")
            or os.environ.get("REGISTER_SMALL_SUCCESS_SEED_MAX_AGE_SECONDS")
            or "900"
        ).strip()
        try:
            max_age_seconds = max(0, int(float(max_age_raw)))
        except Exception:
            max_age_seconds = 900
        if max_age_seconds > 0:
            age_seconds = max(0.0, (datetime.now(timezone.utc) - parsed_created_at).total_seconds())
            if age_seconds > max_age_seconds:
                return False, f"openai_oauth_seed_too_old:{int(age_seconds)}"
    platform_auth = payload.get("platformAuth")
    if not isinstance(platform_auth, dict):
        return False, "missing_platform_auth"
    required_platform_auth_fields = (
        "clientId",
        "redirectUri",
        "codeVerifier",
        "state",
        "nonce",
    )
    for field_name in required_platform_auth_fields:
        if not str(platform_auth.get(field_name) or "").strip():
            return False, f"missing_platform_auth_{field_name.lower()}"
    return True, ""


def restore_to_pool(*, claimed_path: Path, pool_dir: Path, preferred_name: str) -> str:
    return move_artifact_to_dir(
        source_path=claimed_path,
        destination_dir=pool_dir,
        preferred_name=preferred_name,
    )


def derive_original_name_from_claim(path: Path) -> str:
    return strip_generated_claim_prefixes(path.name)


def recover_stale_team_claims(
    *,
    pool_dir: Path,
    claims_dir: Path,
    stale_after_seconds: int,
) -> list[dict[str, Any]]:
    if stale_after_seconds <= 0:
        return []

    recovered: list[dict[str, Any]] = []
    now = time.time()
    for claimed_path in sorted(claims_dir.glob("*.json"), key=lambda path: path.name.lower()):
        try:
            age_seconds = max(0.0, now - claimed_path.stat().st_mtime)
        except FileNotFoundError:
            continue
        if age_seconds < stale_after_seconds:
            continue
        original_name = derive_original_name_from_claim(claimed_path)
        try:
            restored_path = restore_to_pool(
                claimed_path=claimed_path,
                pool_dir=pool_dir,
                preferred_name=original_name,
            )
        except FileNotFoundError:
            continue
        recovered.append(
            {
                "claimed_path": str(claimed_path),
                "restored_path": restored_path,
                "age_seconds": round(age_seconds, 3),
            }
        )
    return recovered


def safe_count(value: Any, default: int) -> int:
    try:
        return max(0, int(value))
    except Exception:
        return default


def team_stale_claim_seconds() -> int:
    return team_auth_runtime_config_for_step_input().stale_claim_seconds


def sort_paths_newest_first(paths: list[Path]) -> list[Path]:
    def _sort_key(path: Path) -> tuple[float, str]:
        try:
            modified_at = float(path.stat().st_mtime)
        except FileNotFoundError:
            modified_at = 0.0
        return (-modified_at, path.name.lower())

    return sorted(paths, key=_sort_key)


def choose_random_files(*, directory: Path, pattern: str, limit: int) -> list[Path]:
    candidates = [path for path in directory.glob(pattern) if path.is_file()]
    if not candidates or limit <= 0:
        return []
    return sort_paths_newest_first(candidates)[:limit]
