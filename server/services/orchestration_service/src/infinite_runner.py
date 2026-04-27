from __future__ import annotations

import errno
import hashlib
import json
import multiprocessing as mp
import os
import random
import shutil
import signal
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    _CURRENT_DIR = Path(__file__).resolve().parent
    if str(_CURRENT_DIR) not in sys.path:
        sys.path.append(str(_CURRENT_DIR))
    from dashboard_server import ServiceRuntimeState, WorkerRuntimeState, start_dashboard_server_if_enabled
    from dst_flow import run_dst_flow_once
    from easyemail_flow import dispatch_easyemail_step
    from easyprotocol_flow import dispatch_easyprotocol_step
    from others.common import (
        canonical_free_artifact_name as _canonical_free_artifact_name_from_payload,
        canonical_team_artifact_name as _canonical_team_artifact_name_from_payload,
        decode_jwt_payload as _decode_jwt_payload,
        ensure_directory as _ensure_directory,
        env_flag as _env_bool,
        extract_auth_claims as _extract_artifact_auth_claims,
        free_manual_oauth_preserve_codes as _free_manual_oauth_preserve_codes,
        free_manual_oauth_preserve_enabled as _free_manual_oauth_preserve_enabled,
        standardize_export_credential_payload as _standardize_export_credential_payload,
        team_mother_cooldown_key as _team_mother_cooldown_key,
        validate_small_success_seed_payload as _validate_small_success_seed_payload,
        write_json_atomic as _write_json_atomic,
    )
    from others.file_lock import release_lock, try_acquire_lock
    from others.paths import (
        resolve_shared_root as _shared_root_from_output_root,
        resolve_team_mother_claims_dir,
        resolve_team_mother_cooldowns_dir,
        resolve_team_mother_pool_dir,
    )
    from others.storage import load_json_payload
else:
    from .dashboard_server import ServiceRuntimeState, WorkerRuntimeState, start_dashboard_server_if_enabled
    from .dst_flow import run_dst_flow_once
    from .easyemail_flow import dispatch_easyemail_step
    from .easyprotocol_flow import dispatch_easyprotocol_step
    from .others.common import (
        canonical_free_artifact_name as _canonical_free_artifact_name_from_payload,
        canonical_team_artifact_name as _canonical_team_artifact_name_from_payload,
        decode_jwt_payload as _decode_jwt_payload,
        ensure_directory as _ensure_directory,
        env_flag as _env_bool,
        extract_auth_claims as _extract_artifact_auth_claims,
        free_manual_oauth_preserve_codes as _free_manual_oauth_preserve_codes,
        free_manual_oauth_preserve_enabled as _free_manual_oauth_preserve_enabled,
        standardize_export_credential_payload as _standardize_export_credential_payload,
        team_mother_cooldown_key as _team_mother_cooldown_key,
        validate_small_success_seed_payload as _validate_small_success_seed_payload,
        write_json_atomic as _write_json_atomic,
    )
    from .others.file_lock import release_lock, try_acquire_lock
    from .others.paths import (
        resolve_shared_root as _shared_root_from_output_root,
        resolve_team_mother_claims_dir,
        resolve_team_mother_cooldowns_dir,
        resolve_team_mother_pool_dir,
    )
    from .others.storage import load_json_payload


def _env_text(name: str, default: str = "") -> str:
    return str(os.environ.get(name) or default).strip()


def _env_int(name: str, default: int) -> int:
    raw = _env_text(name, str(default))
    try:
        return int(raw)
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    raw = _env_text(name, str(default))
    try:
        return float(raw)
    except Exception:
        return default


def _split_path_list(raw: str) -> list[str]:
    normalized = str(raw or "").strip()
    if not normalized:
        return []
    return [item.strip() for item in normalized.split(os.pathsep) if str(item or "").strip()]


def _sort_file_paths_newest_first(paths: list[Path]) -> list[Path]:
    def _sort_key(path: Path) -> tuple[float, str]:
        try:
            modified_at = float(path.stat().st_mtime)
        except FileNotFoundError:
            modified_at = 0.0
        return (-modified_at, path.name.lower())

    return sorted(paths, key=_sort_key)

def _team_auth_path_is_explicit_mother(path: Path) -> bool:
    return str(path.name or "").strip().lower().startswith("codex-team-mother-")


def _team_auth_identity_claims(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    auth_sources: list[dict[str, Any]] = []
    auth_payload = payload.get("auth")
    if isinstance(auth_payload, dict):
        auth_sources.append(auth_payload)
    auth_sources.append(payload)
    for source in auth_sources:
        for token_key in ("id_token", "access_token"):
            token = source.get(token_key)
            if not isinstance(token, str) or not token.strip():
                continue
            claims = _decode_jwt_payload(token)
            if claims:
                return claims
    return {}


def _team_auth_payload_is_mother(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    auth_claims = _extract_artifact_auth_claims(payload)
    plan_type = str(auth_claims.get("chatgpt_plan_type") or "").strip().lower()
    if plan_type and plan_type != "team":
        return False
    identity_claims = _team_auth_identity_claims(payload)
    if not identity_claims:
        return False
    auth_provider = str(identity_claims.get("auth_provider") or "").strip().lower()
    if auth_provider != "passwordless":
        return False
    amr = identity_claims.get("amr")
    amr_values: list[str] = []
    if isinstance(amr, list):
        amr_values = [str(value).strip().lower() for value in amr if str(value or "").strip()]
    elif isinstance(amr, str):
        amr_values = [part.strip().lower() for part in amr.split(",") if part.strip()]
    if not amr_values:
        return False
    return any(value == "otp" or "otp_email" in value for value in amr_values)


def _team_auth_pool_candidates(*, candidate_dirs: list[str]) -> list[str]:
    glob_pattern = _env_text("REGISTER_TEAM_AUTH_GLOB", "*-team.json") or "*-team.json"
    explicit: list[str] = []
    inferred: list[str] = []
    seen: set[str] = set()
    for raw_dir in candidate_dirs:
        candidate = Path(raw_dir).expanduser()
        if candidate.is_file():
            directory_paths = [candidate]
        elif candidate.is_dir():
            directory_paths = []
            for pattern in ("*.json", glob_pattern):
                for path in candidate.glob(pattern):
                    if path.is_file():
                        directory_paths.append(path)
        else:
            continue
        for path in directory_paths:
            resolved = str(path.resolve())
            lowered = resolved.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            try:
                payload = load_json_payload(path)
            except Exception:
                continue
            if not _team_auth_payload_is_mother(payload):
                continue
            if _team_auth_path_is_explicit_mother(path):
                explicit.append(resolved)
            else:
                inferred.append(resolved)
    return explicit + inferred


def _resolve_team_auth_pool(*, instance_role: str) -> list[str]:
    normalized_role = str(instance_role or "").strip().lower()
    if normalized_role == "team":
        explicit_pool_dir = _env_text("REGISTER_TEAM_MOTHER_POOL_DIR") or "/shared/register-output/team-mother-pool"
        pool_dir = Path(explicit_pool_dir).expanduser().resolve()
        if not pool_dir.exists():
            return []
        return _team_auth_pool_candidates(candidate_dirs=[str(pool_dir)])

    explicit_paths = _split_path_list(_env_text("REGISTER_TEAM_AUTH_PATHS"))
    if explicit_paths:
        return _team_auth_pool_candidates(candidate_dirs=explicit_paths)

    explicit_path = _env_text("REGISTER_TEAM_AUTH_PATH")
    if explicit_path:
        candidate = Path(explicit_path).expanduser()
        if candidate.exists():
            return _team_auth_pool_candidates(candidate_dirs=[str(candidate.resolve())])
        return []

    candidate_dirs = _split_path_list(_env_text("REGISTER_TEAM_AUTH_DIRS"))
    if not candidate_dirs:
        preferred_local_dir = _env_text("REGISTER_TEAM_AUTH_LOCAL_DIR") or _env_text("REGISTER_TEAM_LOCAL_DIR")
        fallback_default_dir = _env_text("REGISTER_TEAM_AUTH_DEFAULT_DIR") or _env_text("REGISTER_TEAM_AUTH_DIR")
        deduped_dirs: list[str] = []
        seen_dirs: set[str] = set()
        for raw_dir in (preferred_local_dir, fallback_default_dir):
            normalized = str(raw_dir or "").strip()
            if not normalized:
                continue
            lowered = normalized.lower()
            if lowered in seen_dirs:
                continue
            seen_dirs.add(lowered)
            deduped_dirs.append(normalized)
        candidate_dirs = deduped_dirs
    return _team_auth_pool_candidates(candidate_dirs=candidate_dirs)


def _team_auth_state_dir(*, shared_root: Path) -> Path:
    return shared_root / "others" / "team-auth-state"


def _team_auth_identity_keys_from_paths(team_auth_paths: list[str]) -> set[str]:
    identity_keys: set[str] = set()
    for team_auth_path in team_auth_paths:
        normalized_path = str(team_auth_path or "").strip()
        if not normalized_path:
            continue
        identity = _team_mother_identity_from_team_auth_path(normalized_path)
        identity_key = _team_mother_identity_key(
            original_name=str(identity.get("original_name") or "").strip(),
            email=str(identity.get("email") or "").strip(),
            account_id=str(identity.get("account_id") or "").strip(),
        )
        if identity_key:
            identity_keys.add(identity_key)
    return identity_keys


def _prune_stale_team_auth_caches(
    *,
    shared_root: Path,
    active_team_auth_paths: list[str],
) -> dict[str, Any]:
    active_paths = {
        str(Path(candidate).resolve()).strip().lower()
        for candidate in active_team_auth_paths
        if str(candidate or "").strip()
    }
    active_identity_keys = _team_auth_identity_keys_from_paths(active_team_auth_paths)
    reserved_identity_keys = _team_mother_reserved_identity_keys_for_shared_root(shared_root=shared_root)
    allowed_identity_keys = active_identity_keys | reserved_identity_keys

    removed_state_paths: list[str] = []
    state_dir = _team_auth_state_dir(shared_root=shared_root)
    if state_dir.is_dir():
        cleanup_state_name = _team_cleanup_state_path(shared_root=shared_root).name.lower()
        for state_path in state_dir.glob("*.json"):
            if not state_path.is_file():
                continue
            if state_path.name.lower() == cleanup_state_name:
                continue
            payload = _load_json_dict(state_path)
            team_auth_path = str(payload.get("teamAuthPath") or "").strip()
            resolved_team_auth_path = str(Path(team_auth_path).resolve()).strip().lower() if team_auth_path else ""
            if not resolved_team_auth_path or resolved_team_auth_path not in active_paths or not Path(team_auth_path).is_file():
                state_path.unlink(missing_ok=True)
                removed_state_paths.append(str(state_path))

    removed_availability_paths: list[str] = []
    cooldown_dir = _team_mother_cooldowns_dir_for_shared_root(shared_root=shared_root)
    if cooldown_dir.is_dir():
        for state_path in cooldown_dir.glob("*.json"):
            if not state_path.is_file():
                continue
            payload = _load_json_dict(state_path)
            if not payload:
                state_path.unlink(missing_ok=True)
                removed_availability_paths.append(str(state_path))
                continue
            normalized = _prune_team_mother_availability_state(state_path=state_path, payload=payload)
            if not normalized:
                removed_availability_paths.append(str(state_path))
                continue
            identity_key = _team_mother_identity_key(
                original_name=str(normalized.get("original_name") or "").strip(),
                email=str(normalized.get("email") or "").strip(),
                account_id=str(normalized.get("account_id") or "").strip(),
            )
            if identity_key and identity_key not in allowed_identity_keys:
                state_path.unlink(missing_ok=True)
                removed_availability_paths.append(str(state_path))

    return {
        "removedTeamAuthStatePaths": removed_state_paths,
        "removedAvailabilityStatePaths": removed_availability_paths,
    }


def _team_mother_identity_key(*, original_name: str, email: str, account_id: str) -> str:
    return _team_mother_cooldown_key(
        original_name=str(original_name or "").strip(),
        email=str(email or "").strip(),
        account_id=str(account_id or "").strip(),
    )


def _team_mother_reserved_identity_keys_for_shared_root(*, shared_root: Path) -> set[str]:
    reserved: set[str] = set()
    for directory in (
        Path(resolve_team_mother_pool_dir(str(shared_root))).resolve(),
        Path(resolve_team_mother_claims_dir(str(shared_root))).resolve(),
    ):
        if not directory.is_dir():
            continue
        for candidate in directory.glob("*.json"):
            if not candidate.is_file():
                continue
            try:
                payload = load_json_payload(candidate)
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            identity_key = _team_mother_identity_key(
                original_name=candidate.name,
                email=str(payload.get("email") or "").strip(),
                account_id=str(
                    payload.get("account_id")
                    or ((payload.get("https://api.openai.com/auth") or {}).get("chatgpt_account_id"))
                    or ""
                ).strip(),
            )
            if identity_key:
                reserved.add(identity_key)
    return reserved


def _team_auth_is_reserved_for_team_expand(
    *,
    shared_root: Path,
    team_auth_path: str,
    reserved_keys: set[str] | None = None,
) -> tuple[bool, dict[str, Any]]:
    normalized_path = str(team_auth_path or "").strip()
    if not normalized_path:
        return False, {}
    identity = _team_mother_identity_from_team_auth_path(normalized_path)
    identity_key = _team_mother_identity_key(
        original_name=str(identity.get("original_name") or "").strip(),
        email=str(identity.get("email") or "").strip(),
        account_id=str(identity.get("account_id") or "").strip(),
    )
    if not identity_key:
        return False, {}
    active_reserved_keys = reserved_keys if reserved_keys is not None else _team_mother_reserved_identity_keys_for_shared_root(shared_root=shared_root)
    if identity_key not in active_reserved_keys:
        return False, {}
    return True, {
        "teamAuthPath": normalized_path,
        "identityKey": identity_key,
        "original_name": str(identity.get("original_name") or "").strip(),
        "email": str(identity.get("email") or "").strip(),
        "account_id": str(identity.get("account_id") or "").strip(),
    }


def _team_auth_state_path(*, shared_root: Path, team_auth_path: str) -> Path:
    normalized = str(Path(team_auth_path).resolve()).strip().lower()
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:16]
    return _team_auth_state_dir(shared_root=shared_root) / f"{digest}.json"


def _team_cleanup_state_path(*, shared_root: Path) -> Path:
    return _team_auth_state_dir(shared_root=shared_root) / "codex-capacity-cleanup.json"


def _team_cleanup_lock_path(*, shared_root: Path) -> Path:
    return _team_auth_state_dir(shared_root=shared_root) / "codex-capacity-cleanup.lock"


def _mailbox_cleanup_state_path(*, shared_root: Path) -> Path:
    return shared_root / "others" / "mailbox-cleanup-state.json"


def _mailbox_cleanup_lock_path(*, shared_root: Path) -> Path:
    return shared_root / "others" / "mailbox-cleanup.lock"


def _mailbox_domain_stats_path(*, shared_root: Path) -> Path:
    explicit = _env_text("REGISTER_MAILBOX_DOMAIN_STATE_PATH")
    if explicit:
        return Path(explicit).expanduser().resolve()
    return shared_root / "others" / "register-mailbox-domain-state.json"


def _load_team_auth_state(*, shared_root: Path, team_auth_path: str) -> dict[str, Any]:
    path = _team_auth_state_path(shared_root=shared_root, team_auth_path=team_auth_path)
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_team_cleanup_state(*, shared_root: Path) -> dict[str, Any]:
    path = _team_cleanup_state_path(shared_root=shared_root)
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_mailbox_cleanup_state(*, shared_root: Path) -> dict[str, Any]:
    path = _mailbox_cleanup_state_path(shared_root=shared_root)
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_mailbox_domain_stats_state(*, shared_root: Path) -> dict[str, Any]:
    path = _mailbox_domain_stats_path(shared_root=shared_root)
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_team_auth_state(*, shared_root: Path, team_auth_path: str, payload: dict[str, Any]) -> None:
    state_dir = _team_auth_state_dir(shared_root=shared_root)
    _ensure_directory(state_dir)
    path = _team_auth_state_path(shared_root=shared_root, team_auth_path=team_auth_path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_team_cleanup_state(*, shared_root: Path, payload: dict[str, Any]) -> None:
    state_dir = _team_auth_state_dir(shared_root=shared_root)
    _ensure_directory(state_dir)
    path = _team_cleanup_state_path(shared_root=shared_root)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_mailbox_cleanup_state(*, shared_root: Path, payload: dict[str, Any]) -> None:
    path = _mailbox_cleanup_state_path(shared_root=shared_root)
    _ensure_directory(path.parent)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_mailbox_domain_stats_state(*, shared_root: Path, payload: dict[str, Any]) -> None:
    path = _mailbox_domain_stats_path(shared_root=shared_root)
    _ensure_directory(path.parent)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _mark_team_auth_capacity_cooldown(
    *,
    shared_root: Path,
    team_auth_path: str,
    cooldown_seconds: float,
    detail: str,
) -> None:
    now = datetime.now(timezone.utc)
    payload = _load_team_auth_state(shared_root=shared_root, team_auth_path=team_auth_path)
    payload.update(
        {
            "teamAuthPath": str(Path(team_auth_path).resolve()),
            "capacityCooldownUntil": (now + timedelta(seconds=max(0.0, float(cooldown_seconds or 0.0)))).isoformat(),
            "lastCapacityErrorAt": now.isoformat(),
            "lastCapacityDetail": str(detail or "").strip(),
        }
    )
    _write_team_auth_state(shared_root=shared_root, team_auth_path=team_auth_path, payload=payload)


def _clear_team_auth_capacity_cooldown(*, shared_root: Path, team_auth_path: str) -> None:
    payload = _load_team_auth_state(shared_root=shared_root, team_auth_path=team_auth_path)
    if not payload:
        return
    payload["capacityCooldownUntil"] = ""
    payload["lastCapacityRecoveredAt"] = datetime.now(timezone.utc).isoformat()
    _write_team_auth_state(shared_root=shared_root, team_auth_path=team_auth_path, payload=payload)


def _team_auth_is_capacity_cooled(*, shared_root: Path, team_auth_path: str) -> bool:
    payload = _load_team_auth_state(shared_root=shared_root, team_auth_path=team_auth_path)
    until_text = str(payload.get("capacityCooldownUntil") or "").strip()
    if not until_text:
        return False
    try:
        until = datetime.fromisoformat(until_text.replace("Z", "+00:00"))
    except ValueError:
        return False
    if until.tzinfo is None:
        until = until.replace(tzinfo=timezone.utc)
    return until > datetime.now(timezone.utc)


def _all_team_auth_capacity_cooled(*, shared_root: Path, team_auth_pool: list[str]) -> bool:
    normalized_pool = [candidate for candidate in team_auth_pool if str(candidate or "").strip()]
    if not normalized_pool:
        return False
    return all(
        _team_auth_is_capacity_cooled(shared_root=shared_root, team_auth_path=candidate)
        for candidate in normalized_pool
    )


def _team_mother_cooldowns_dir_for_shared_root(*, shared_root: Path) -> Path:
    return Path(resolve_team_mother_cooldowns_dir(str(shared_root))).resolve()


def _team_mother_identity_from_team_auth_path(team_auth_path: str) -> dict[str, str]:
    normalized_path = str(team_auth_path or "").strip()
    if not normalized_path:
        return {
            "original_name": "",
            "email": "",
            "account_id": "",
        }
    path = Path(normalized_path).resolve()
    try:
        payload = load_json_payload(path)
    except Exception:
        payload = {}
    auth_claims = _extract_artifact_auth_claims(payload)
    return {
        "original_name": path.name,
        "email": str(payload.get("email") or "").strip(),
        "account_id": str(
            payload.get("account_id")
            or auth_claims.get("chatgpt_account_id")
            or ""
        ).strip(),
    }


def _team_auth_email_domain(*, team_auth_path: str) -> str:
    identity = _team_mother_identity_from_team_auth_path(team_auth_path)
    email = str(identity.get("email") or "").strip().lower()
    if "@" not in email:
        return ""
    return email.rsplit("@", 1)[-1].strip().lower()


def _team_auth_sall_cc_weight() -> float:
    return max(0.0, min(1.0, _env_percent("REGISTER_TEAM_AUTH_SALL_CC_WEIGHT", 5.0) / 100.0))


def _team_auth_zero_success_window_seconds() -> float:
    return max(0.0, _env_float("REGISTER_TEAM_AUTH_ZERO_SUCCESS_WINDOW_SECONDS", 1800.0))


def _team_auth_zero_success_min_attempts() -> int:
    return max(0, _env_int("REGISTER_TEAM_AUTH_ZERO_SUCCESS_MIN_ATTEMPTS", 10))


def _team_auth_team_expand_window_seconds() -> float:
    return max(0.0, _env_float("REGISTER_TEAM_AUTH_TEAM_EXPAND_WINDOW_SECONDS", 21600.0))


def _team_auth_team_expand_failure_weight_step() -> float:
    return max(0.0, min(1.0, _env_float("REGISTER_TEAM_AUTH_TEAM_EXPAND_FAILURE_WEIGHT_STEP", 0.25)))


def _team_auth_team_expand_floor_weight() -> float:
    return max(0.05, min(1.0, _env_float("REGISTER_TEAM_AUTH_TEAM_EXPAND_FLOOR_WEIGHT", 0.2)))


def _team_auth_team_expand_success_credit() -> float:
    return max(0.0, _env_float("REGISTER_TEAM_AUTH_TEAM_EXPAND_SUCCESS_CREDIT", 0.5))


def _team_auth_total_seat_limit() -> int:
    return max(1, _env_int("REGISTER_TEAM_TOTAL_SEAT_LIMIT", 9))


def _team_auth_chatgpt_seat_limit() -> int:
    return max(0, min(_team_auth_total_seat_limit(), _env_int("REGISTER_TEAM_CHATGPT_SEAT_LIMIT", 4)))


def _team_auth_codex_seat_limit() -> int:
    return max(0, min(_team_auth_total_seat_limit(), _env_int("REGISTER_TEAM_CODEX_SEAT_LIMIT", _team_auth_total_seat_limit())))


def _team_auth_reservation_ttl_seconds() -> float:
    return max(30.0, _env_float("REGISTER_TEAM_AUTH_RESERVATION_TTL_SECONDS", 300.0))


def _team_auth_state_lock_timeout_seconds() -> float:
    return max(1.0, _env_float("REGISTER_TEAM_AUTH_STATE_LOCK_TIMEOUT_SECONDS", 5.0))


def _team_auth_team_member_chatgpt_seat_request() -> int:
    return max(1, _env_int("REGISTER_TEAM_MEMBER_COUNT", 4))


def _team_auth_codex_seat_types() -> set[str]:
    return {
        str(item or "").strip().lower()
        for item in str(_env_text("REGISTER_TEAM_CODEX_SEAT_TYPES", "usage_based,codex")).split(",")
        if str(item or "").strip()
    }


def _normalize_team_auth_seat_type(value: Any) -> str:
    return str(value or "").strip().lower()


def _team_auth_seat_category_for_type(seat_type: Any) -> str:
    normalized = _normalize_team_auth_seat_type(seat_type)
    if normalized in _team_auth_codex_seat_types():
        return "codex"
    return "chatgpt"


def _team_auth_seat_request_for_role(*, instance_role: str) -> dict[str, int]:
    normalized_role = str(instance_role or "").strip().lower()
    if normalized_role in {"main", "continue"}:
        return {"codex": 1, "chatgpt": 0}
    if normalized_role == "team":
        return {"codex": 0, "chatgpt": 0}
    return {"codex": 0, "chatgpt": 0}


def _team_auth_team_expand_penalty_weight(
    *,
    shared_root: Path,
    team_auth_path: str,
) -> float:
    return float(
        _team_auth_recent_team_expand_weight_info(
            shared_root=shared_root,
            team_auth_path=team_auth_path,
        ).get("weight")
        or 1.0
    )


def _team_auth_selection_weight(
    *,
    team_auth_path: str,
    shared_root: Path,
    instance_role: str,
) -> float:
    weight = 1.0
    if _team_auth_email_domain(team_auth_path=team_auth_path) == "sall.cc":
        weight *= _team_auth_sall_cc_weight()
    if str(instance_role or "").strip().lower() == "team":
        weight *= _team_auth_team_expand_penalty_weight(
            shared_root=shared_root,
            team_auth_path=team_auth_path,
        )
    return max(0.0, float(weight))


def _team_auth_normalize_seat_allocations(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    normalized_items: list[dict[str, Any]] = []
    dedupe_index: dict[str, int] = {}
    for item in raw:
        if not isinstance(item, dict):
            continue
        invite_email = str(
            item.get("invite_email")
            or item.get("email")
            or item.get("inviteEmail")
            or ""
        ).strip()
        invite_id = str(item.get("invite_id") or item.get("inviteId") or "").strip()
        member_user_id = str(item.get("member_user_id") or item.get("memberUserId") or item.get("user_id") or "").strip()
        seat_type = _normalize_team_auth_seat_type(
            item.get("seat_type")
            or item.get("seatType")
            or item.get("effectiveSeatType")
            or item.get("requestedSeatType")
            or ""
        )
        seat_category = str(item.get("seat_category") or item.get("seatCategory") or "").strip().lower() or _team_auth_seat_category_for_type(seat_type)
        reservation_id = str(item.get("reservation_id") or item.get("reservationId") or "").strip()
        reservation_owner = str(item.get("reservation_owner") or item.get("reservationOwner") or "").strip()
        reservation_context = str(item.get("reservation_context") or item.get("reservationContext") or "").strip()
        pending_until = str(item.get("pending_until") or item.get("pendingUntil") or "").strip()
        try:
            pending_until_ts = float(item.get("pending_until_ts") or item.get("pendingUntilTs") or 0.0)
        except Exception:
            pending_until_ts = 0.0
        normalized_item = {
            "seat_category": seat_category,
            "seat_type": seat_type,
            "invite_email": invite_email,
            "invite_id": invite_id,
            "member_user_id": member_user_id,
            "source_role": str(item.get("source_role") or item.get("sourceRole") or "").strip().lower(),
            "source_step": str(item.get("source_step") or item.get("sourceStep") or "").strip().lower(),
            "status": str(item.get("status") or "").strip().lower() or "active",
            "created_at": str(item.get("created_at") or item.get("createdAt") or "").strip(),
            "updated_at": str(item.get("updated_at") or item.get("updatedAt") or "").strip(),
            "reservation_id": reservation_id,
            "reservation_owner": reservation_owner,
            "reservation_context": reservation_context,
            "pending_until": pending_until,
            "pending_until_ts": pending_until_ts,
        }
        dedupe_key = ""
        if reservation_id:
            dedupe_key = f"reservation:{reservation_id.lower()}"
        elif invite_id:
            dedupe_key = f"invite:{invite_id.lower()}"
        elif member_user_id:
            dedupe_key = f"user:{member_user_id.lower()}"
        elif invite_email:
            dedupe_key = f"email:{invite_email.lower()}"
        else:
            dedupe_key = f"anon:{len(normalized_items)}"
        existing_index = dedupe_index.get(dedupe_key)
        if existing_index is not None:
            existing = normalized_items[existing_index]
            created_at = str(existing.get("created_at") or "").strip() or normalized_item["created_at"]
            normalized_item["created_at"] = created_at
            normalized_items[existing_index] = normalized_item
            continue
        dedupe_index[dedupe_key] = len(normalized_items)
        normalized_items.append(normalized_item)
    return normalized_items


def _team_auth_seat_summary_from_allocations(seat_allocations: list[dict[str, Any]]) -> dict[str, Any]:
    total_limit = _team_auth_total_seat_limit()
    chatgpt_limit = _team_auth_chatgpt_seat_limit()
    codex_limit = _team_auth_codex_seat_limit()
    used_chatgpt = sum(
        1
        for item in seat_allocations
        if isinstance(item, dict) and str(item.get("seat_category") or "").strip().lower() == "chatgpt"
    )
    used_codex = sum(
        1
        for item in seat_allocations
        if isinstance(item, dict) and str(item.get("seat_category") or "").strip().lower() == "codex"
    )
    used_total = max(0, used_chatgpt + used_codex)
    available_total = max(0, total_limit - used_total)
    available_chatgpt = max(0, min(chatgpt_limit - used_chatgpt, available_total))
    available_codex = max(0, min(codex_limit - used_codex, available_total))
    return {
        "total_limit": total_limit,
        "chatgpt_limit": chatgpt_limit,
        "codex_limit": codex_limit,
        "used_total": used_total,
        "used_chatgpt": used_chatgpt,
        "used_codex": used_codex,
        "available_total": available_total,
        "available_chatgpt": available_chatgpt,
        "available_codex": available_codex,
    }


def _team_auth_seat_summary_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return _team_auth_seat_summary_from_allocations(
        _team_auth_normalize_seat_allocations(payload.get("seat_allocations"))
    )


def _team_auth_prune_expired_seat_allocations(
    seat_allocations: list[dict[str, Any]],
    *,
    now_ts: float | None = None,
) -> tuple[list[dict[str, Any]], bool]:
    normalized_allocations = _team_auth_normalize_seat_allocations(seat_allocations)
    current_ts = float(now_ts if now_ts is not None else time.time())
    filtered: list[dict[str, Any]] = []
    changed = False
    for allocation in normalized_allocations:
        status = str(allocation.get("status") or "").strip().lower()
        if status == "pending":
            try:
                pending_until_ts = float(allocation.get("pending_until_ts") or 0.0)
            except Exception:
                pending_until_ts = 0.0
            if pending_until_ts > 0.0 and pending_until_ts <= current_ts:
                changed = True
                continue
        filtered.append(allocation)
    return _team_auth_normalize_seat_allocations(filtered), changed


def _team_auth_allocation_matches(
    allocation: dict[str, Any],
    *,
    invite_email: str = "",
    invite_id: str = "",
    member_user_id: str = "",
    reservation_id: str = "",
) -> bool:
    normalized_email = str(invite_email or "").strip().lower()
    normalized_invite_id = str(invite_id or "").strip().lower()
    normalized_member_user_id = str(member_user_id or "").strip().lower()
    normalized_reservation_id = str(reservation_id or "").strip().lower()
    allocation_email = str(allocation.get("invite_email") or "").strip().lower()
    allocation_invite_id = str(allocation.get("invite_id") or "").strip().lower()
    allocation_member_user_id = str(allocation.get("member_user_id") or "").strip().lower()
    allocation_reservation_id = str(allocation.get("reservation_id") or "").strip().lower()
    if normalized_reservation_id and allocation_reservation_id and normalized_reservation_id == allocation_reservation_id:
        return True
    if normalized_invite_id and allocation_invite_id and normalized_invite_id == allocation_invite_id:
        return True
    if normalized_member_user_id and allocation_member_user_id and normalized_member_user_id == allocation_member_user_id:
        return True
    if normalized_email and allocation_email and normalized_email == allocation_email:
        return True
    return False


def _team_auth_upsert_seat_allocations(
    *,
    seat_allocations: list[dict[str, Any]],
    additions: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], bool]:
    changed = False
    now_iso = datetime.now(timezone.utc).isoformat()
    normalized_allocations = _team_auth_normalize_seat_allocations(seat_allocations)
    for item in additions:
        if not isinstance(item, dict):
            continue
        normalized_item = _team_auth_normalize_seat_allocations([item])
        if not normalized_item:
            continue
        allocation = normalized_item[0]
        allocation["updated_at"] = now_iso
        if not str(allocation.get("created_at") or "").strip():
            allocation["created_at"] = now_iso
        matched_index = None
        for index, existing in enumerate(normalized_allocations):
            if _team_auth_allocation_matches(
                existing,
                invite_email=str(allocation.get("invite_email") or "").strip(),
                invite_id=str(allocation.get("invite_id") or "").strip(),
                member_user_id=str(allocation.get("member_user_id") or "").strip(),
            ):
                matched_index = index
                break
        if matched_index is None:
            normalized_allocations.append(allocation)
            changed = True
            continue
        existing = dict(normalized_allocations[matched_index])
        allocation["created_at"] = str(existing.get("created_at") or "").strip() or allocation["created_at"]
        if existing != allocation:
            normalized_allocations[matched_index] = allocation
            changed = True
    return _team_auth_normalize_seat_allocations(normalized_allocations), changed


def _team_auth_remove_seat_allocations(
    *,
    seat_allocations: list[dict[str, Any]],
    invite_email: str = "",
    invite_id: str = "",
    member_user_id: str = "",
    reservation_id: str = "",
    seat_category: str = "",
    clear_all: bool = False,
) -> tuple[list[dict[str, Any]], bool]:
    normalized_allocations = _team_auth_normalize_seat_allocations(seat_allocations)
    if clear_all:
        return [], bool(normalized_allocations)
    normalized_category = str(seat_category or "").strip().lower()
    filtered: list[dict[str, Any]] = []
    changed = False
    has_filters = any(str(value or "").strip() for value in (invite_email, invite_id, member_user_id, reservation_id))
    for allocation in normalized_allocations:
        matches_identity = _team_auth_allocation_matches(
            allocation,
            invite_email=invite_email,
            invite_id=invite_id,
            member_user_id=member_user_id,
            reservation_id=reservation_id,
        )
        matches_category = not normalized_category or str(allocation.get("seat_category") or "").strip().lower() == normalized_category
        should_remove = False
        if has_filters:
            should_remove = matches_identity and matches_category
        elif normalized_category:
            should_remove = matches_category
        if should_remove:
            changed = True
            continue
        filtered.append(allocation)
    return _team_auth_normalize_seat_allocations(filtered), changed


def _team_auth_identity_from_path_or_override(
    *,
    team_auth_path: str,
    identity: dict[str, str] | None,
) -> dict[str, str]:
    return {
        **_team_mother_identity_from_team_auth_path(team_auth_path),
        **(identity or {}),
    }


def _team_auth_update_seat_state(
    *,
    shared_root: Path,
    team_auth_path: str,
    identity: dict[str, str] | None,
    updater: Any,
) -> dict[str, Any] | None:
    resolved_identity = _team_auth_identity_from_path_or_override(
        team_auth_path=team_auth_path,
        identity=identity,
    )
    original_name = str(resolved_identity.get("original_name") or "").strip()
    email = str(resolved_identity.get("email") or "").strip()
    account_id = str(resolved_identity.get("account_id") or "").strip()
    if not any((original_name, email, account_id)):
        return None
    state_path, payload = _load_team_mother_availability_state(
        shared_root=shared_root,
        original_name=original_name,
        email=email,
        account_id=account_id,
    )
    normalized = _prune_team_mother_availability_state(state_path=state_path, payload=payload)
    current_allocations = _team_auth_normalize_seat_allocations(normalized.get("seat_allocations"))
    updated_allocations, changed = updater(current_allocations)
    if not changed:
        return None
    normalized.update(
        {
            "original_name": original_name,
            "email": email,
            "account_id": account_id,
            "seat_allocations": updated_allocations,
            "last_seat_sync_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    if not updated_allocations:
        normalized.pop("seat_allocations", None)
    _write_team_mother_availability_state(state_path=state_path, payload=normalized)
    return _team_auth_seat_summary_from_allocations(updated_allocations)


def _team_auth_replace_seat_allocations(
    *,
    shared_root: Path,
    team_auth_path: str,
    identity: dict[str, str] | None,
    allocations: list[dict[str, Any]],
) -> dict[str, Any] | None:
    normalized_target = _team_auth_normalize_seat_allocations(allocations)
    return _team_auth_update_seat_state(
        shared_root=shared_root,
        team_auth_path=team_auth_path,
        identity=identity,
        updater=lambda seat_allocations: (
            normalized_target,
            normalized_target != _team_auth_normalize_seat_allocations(seat_allocations),
        ),
    )


def _team_auth_get_seat_summary(
    *,
    shared_root: Path,
    team_auth_path: str,
) -> dict[str, Any]:
    identity = _team_mother_identity_from_team_auth_path(team_auth_path)
    if not any(str(identity.get(key) or "").strip() for key in ("original_name", "email", "account_id")):
        return _team_auth_seat_summary_from_allocations([])
    state_path, payload = _load_team_mother_availability_state(
        shared_root=shared_root,
        original_name=str(identity.get("original_name") or "").strip(),
        email=str(identity.get("email") or "").strip(),
        account_id=str(identity.get("account_id") or "").strip(),
    )
    normalized = _prune_team_mother_availability_state(state_path=state_path, payload=payload)
    return _team_auth_seat_summary_from_payload(normalized)


def _team_auth_has_required_seats(
    *,
    shared_root: Path,
    team_auth_path: str,
    required_codex_seats: int,
    required_chatgpt_seats: int,
) -> tuple[bool, dict[str, Any]]:
    summary = _team_auth_get_seat_summary(
        shared_root=shared_root,
        team_auth_path=team_auth_path,
    )
    ok = (
        int(summary.get("available_codex") or 0) >= max(0, int(required_codex_seats or 0))
        and int(summary.get("available_chatgpt") or 0) >= max(0, int(required_chatgpt_seats or 0))
        and int(summary.get("available_total") or 0)
        >= max(0, int(required_codex_seats or 0)) + max(0, int(required_chatgpt_seats or 0))
    )
    return ok, summary


def _team_auth_build_pending_reservation_allocations(
    *,
    required_codex_seats: int,
    required_chatgpt_seats: int,
    reservation_owner: str,
    reservation_context: str,
    source_role: str,
) -> list[dict[str, Any]]:
    additions: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    pending_until_ts = time.time() + _team_auth_reservation_ttl_seconds()
    pending_until_iso = datetime.fromtimestamp(pending_until_ts, timezone.utc).isoformat()

    def _build_item(*, seat_category: str, seat_type: str) -> dict[str, Any]:
        return {
            "seat_category": seat_category,
            "seat_type": seat_type,
            "invite_email": "",
            "invite_id": "",
            "member_user_id": "",
            "source_role": str(source_role or "").strip().lower(),
            "source_step": "seat-reservation",
            "status": "pending",
            "created_at": now_iso,
            "updated_at": now_iso,
            "reservation_id": uuid.uuid4().hex,
            "reservation_owner": str(reservation_owner or "").strip(),
            "reservation_context": str(reservation_context or "").strip(),
            "pending_until": pending_until_iso,
            "pending_until_ts": pending_until_ts,
        }

    for _ in range(max(0, int(required_codex_seats or 0))):
        additions.append(_build_item(seat_category="codex", seat_type="usage_based"))
    for _ in range(max(0, int(required_chatgpt_seats or 0))):
        additions.append(_build_item(seat_category="chatgpt", seat_type="default"))
    return additions


def _team_auth_try_reserve_required_seats(
    *,
    shared_root: Path,
    team_auth_path: str,
    required_codex_seats: int,
    required_chatgpt_seats: int,
    reservation_owner: str,
    reservation_context: str,
    source_role: str,
) -> tuple[bool, dict[str, Any] | None, dict[str, Any]]:
    required_codex = max(0, int(required_codex_seats or 0))
    required_chatgpt = max(0, int(required_chatgpt_seats or 0))
    if required_codex <= 0 and required_chatgpt <= 0:
        return True, None, _team_auth_get_seat_summary(shared_root=shared_root, team_auth_path=team_auth_path)

    identity = _team_mother_identity_from_team_auth_path(team_auth_path)
    if not any(str(identity.get(key) or "").strip() for key in ("original_name", "email", "account_id")):
        return False, None, _team_auth_seat_summary_from_allocations([])
    state_path = _team_mother_availability_state_path(
        shared_root=shared_root,
        original_name=str(identity.get("original_name") or "").strip(),
        email=str(identity.get("email") or "").strip(),
        account_id=str(identity.get("account_id") or "").strip(),
    )
    lock_path: Path | None = None
    try:
        lock_path = _acquire_team_mother_availability_state_lock(state_path=state_path)
        payload = _load_json_dict(state_path) if state_path.is_file() else {}
        normalized = _prune_team_mother_availability_state(state_path=state_path, payload=payload)
        summary = _team_auth_seat_summary_from_payload(normalized)
        has_capacity = (
            int(summary.get("available_codex") or 0) >= required_codex
            and int(summary.get("available_chatgpt") or 0) >= required_chatgpt
            and int(summary.get("available_total") or 0) >= (required_codex + required_chatgpt)
        )
        if not has_capacity:
            return False, None, summary
        current_allocations = _team_auth_normalize_seat_allocations(normalized.get("seat_allocations"))
        additions = _team_auth_build_pending_reservation_allocations(
            required_codex_seats=required_codex,
            required_chatgpt_seats=required_chatgpt,
            reservation_owner=reservation_owner,
            reservation_context=reservation_context,
            source_role=source_role,
        )
        updated_allocations, _ = _team_auth_upsert_seat_allocations(
            seat_allocations=current_allocations,
            additions=additions,
        )
        normalized.update(
            {
                "original_name": str(identity.get("original_name") or "").strip(),
                "email": str(identity.get("email") or "").strip(),
                "account_id": str(identity.get("account_id") or "").strip(),
                "seat_allocations": updated_allocations,
                "last_seat_sync_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        _write_team_mother_availability_state(state_path=state_path, payload=normalized)
        reservation = {
            "teamAuthPath": str(team_auth_path or "").strip(),
            "reservationIds": [
                str(item.get("reservation_id") or "").strip()
                for item in additions
                if str(item.get("reservation_id") or "").strip()
            ],
            "owner": str(reservation_owner or "").strip(),
            "context": str(reservation_context or "").strip(),
        }
        return True, reservation, _team_auth_seat_summary_from_allocations(updated_allocations)
    finally:
        _release_team_mother_availability_state_lock(lock_path=lock_path)


def _team_auth_release_seat_reservations(
    *,
    shared_root: Path,
    reservation: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(reservation, dict):
        return None
    team_auth_path = str(reservation.get("teamAuthPath") or "").strip()
    reservation_ids = [
        str(item or "").strip()
        for item in (reservation.get("reservationIds") or [])
        if str(item or "").strip()
    ]
    if not team_auth_path or not reservation_ids:
        return None
    identity = _team_mother_identity_from_team_auth_path(team_auth_path)
    if not any(str(identity.get(key) or "").strip() for key in ("original_name", "email", "account_id")):
        return None
    state_path = _team_mother_availability_state_path(
        shared_root=shared_root,
        original_name=str(identity.get("original_name") or "").strip(),
        email=str(identity.get("email") or "").strip(),
        account_id=str(identity.get("account_id") or "").strip(),
    )
    lock_path: Path | None = None
    try:
        lock_path = _acquire_team_mother_availability_state_lock(state_path=state_path)
        payload = _load_json_dict(state_path) if state_path.is_file() else {}
        normalized = _prune_team_mother_availability_state(state_path=state_path, payload=payload)
        current_allocations = _team_auth_normalize_seat_allocations(normalized.get("seat_allocations"))
        updated_allocations = current_allocations
        changed = False
        for reservation_id in reservation_ids:
            updated_allocations, removed = _team_auth_remove_seat_allocations(
                seat_allocations=updated_allocations,
                reservation_id=reservation_id,
            )
            changed = changed or removed
        if not changed:
            return None
        normalized.update(
            {
                "original_name": str(identity.get("original_name") or "").strip(),
                "email": str(identity.get("email") or "").strip(),
                "account_id": str(identity.get("account_id") or "").strip(),
                "seat_allocations": updated_allocations,
                "last_seat_sync_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        _write_team_mother_availability_state(state_path=state_path, payload=normalized)
        return _team_auth_seat_summary_from_allocations(updated_allocations)
    finally:
        _release_team_mother_availability_state_lock(lock_path=lock_path)


def _team_auth_invite_payload_to_seat_allocation(
    *,
    invite_payload: dict[str, Any],
    source_role: str,
    source_step: str,
) -> dict[str, Any] | None:
    if not isinstance(invite_payload, dict) or not bool(invite_payload.get("ok")):
        return None
    response = invite_payload.get("response")
    response_invite = {}
    if isinstance(response, dict):
        account_invites = response.get("account_invites")
        if isinstance(account_invites, list) and account_invites:
            response_invite = account_invites[0] if isinstance(account_invites[0], dict) else {}
    seat_type = (
        str(invite_payload.get("effectiveSeatType") or "").strip()
        or str(invite_payload.get("requestedSeatType") or "").strip()
        or str((response_invite or {}).get("seat_type") or (response_invite or {}).get("seatType") or "").strip()
        or str(invite_payload.get("seat_type") or "").strip()
    )
    invite_email = (
        str(invite_payload.get("invite_email") or "").strip()
        or str((response_invite or {}).get("email_address") or (response_invite or {}).get("email") or "").strip()
    )
    invite_id = (
        str(invite_payload.get("invite_id") or "").strip()
        or str((response_invite or {}).get("id") or "").strip()
    )
    if not invite_email and not invite_id:
        return None
    return {
        "seat_category": _team_auth_seat_category_for_type(seat_type),
        "seat_type": _normalize_team_auth_seat_type(seat_type),
        "invite_email": invite_email,
        "invite_id": invite_id,
        "member_user_id": str(invite_payload.get("member_user_id") or invite_payload.get("user_id") or "").strip(),
        "source_role": str(source_role or "").strip().lower(),
        "source_step": str(source_step or "").strip().lower(),
        "status": "active",
    }


def _team_auth_extract_team_member_invite_allocations(invite_team_members_output: dict[str, Any]) -> list[dict[str, Any]]:
    results = invite_team_members_output.get("results")
    if not isinstance(results, list):
        return []
    allocations: list[dict[str, Any]] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        invite_payload = item.get("result")
        if not isinstance(invite_payload, dict):
            continue
        allocation = _team_auth_invite_payload_to_seat_allocation(
            invite_payload=invite_payload,
            source_role="team",
            source_step="invite-team-members",
        )
        if allocation:
            allocations.append(allocation)
    return allocations


def _team_auth_reconcile_seat_state_from_result(
    *,
    shared_root: Path,
    team_auth_path: str,
    result_payload: dict[str, Any],
    instance_role: str,
    worker_label: str,
    task_index: int,
) -> None:
    normalized_path = str(team_auth_path or "").strip()
    if not normalized_path or not isinstance(result_payload, dict):
        return
    steps = result_payload.get("steps")
    outputs = result_payload.get("outputs")
    if not isinstance(steps, dict) or not isinstance(outputs, dict):
        return
    identity = (
        _team_mother_identity_from_result_payload(result_payload)
        if str(instance_role or "").strip().lower() == "team"
        else _team_mother_identity_from_team_auth_path(normalized_path)
    )

    def _apply(mutator: Any) -> dict[str, Any] | None:
        return _team_auth_update_seat_state(
            shared_root=shared_root,
            team_auth_path=normalized_path,
            identity=identity,
            updater=mutator,
        )

    seat_state_changed = False

    cleanup_output = outputs.get("cleanup-team-all-seats") or {}
    cleanup_response = cleanup_output.get("response") if isinstance(cleanup_output, dict) and isinstance(cleanup_output.get("response"), dict) else {}
    if str(steps.get("cleanup-team-all-seats") or "").strip().lower() == "ok":
        projected_snapshot = cleanup_response.get("seatSnapshotAfterProjected") if isinstance(cleanup_response, dict) else None
        if isinstance(projected_snapshot, dict) and isinstance(projected_snapshot.get("allocations"), list):
            summary = _team_auth_replace_seat_allocations(
                shared_root=shared_root,
                team_auth_path=normalized_path,
                identity=identity,
                allocations=projected_snapshot.get("allocations") or [],
            )
        else:
            summary = _apply(
                lambda seat_allocations: _team_auth_remove_seat_allocations(
                    seat_allocations=seat_allocations,
                    clear_all=True,
                )
            )
        if summary is not None:
            seat_state_changed = True

    if str(steps.get("invite-team-members") or "").strip().lower() == "ok":
        team_member_allocations = _team_auth_extract_team_member_invite_allocations(outputs.get("invite-team-members") or {})
        if team_member_allocations:
            summary = _apply(
                lambda seat_allocations: _team_auth_upsert_seat_allocations(
                    seat_allocations=seat_allocations,
                    additions=team_member_allocations,
                )
            )
            if summary is not None:
                seat_state_changed = True

    if str(steps.get("invite-codex-member") or "").strip().lower() == "ok":
        codex_allocation = _team_auth_invite_payload_to_seat_allocation(
            invite_payload=outputs.get("invite-codex-member") or {},
            source_role=str(instance_role or "").strip().lower(),
            source_step="invite-codex-member",
        )
        if codex_allocation:
            summary = _apply(
                lambda seat_allocations: _team_auth_upsert_seat_allocations(
                    seat_allocations=seat_allocations,
                    additions=[codex_allocation],
                )
            )
            if summary is not None:
                seat_state_changed = True

    if str(steps.get("revoke-team-members") or "").strip().lower() == "ok":
        revoke_output = outputs.get("revoke-team-members") or {}
        if isinstance(revoke_output, dict):
            revoke_results = revoke_output.get("results")
            if isinstance(revoke_results, list) and revoke_results:
                for item in revoke_results:
                    if not isinstance(item, dict):
                        continue
                    summary = _apply(
                        lambda seat_allocations, item=item: _team_auth_remove_seat_allocations(
                            seat_allocations=seat_allocations,
                            invite_email=str(item.get("email") or "").strip(),
                            member_user_id=str(item.get("userId") or "").strip(),
                        )
                    )
                    if summary is not None:
                        seat_state_changed = True

    if str(steps.get("revoke-codex-member") or "").strip().lower() == "ok":
        revoke_output = outputs.get("revoke-codex-member") or {}
        if isinstance(revoke_output, dict) and bool(revoke_output.get("ok")):
            invite_email = str(revoke_output.get("invite_email") or "").strip()
            invite_id = str(revoke_output.get("invite_id") or "").strip()
            member_user_id = str((revoke_output.get("response") or {}).get("user_id") or "").strip()
            if not any((invite_email, invite_id, member_user_id)):
                invite_output = outputs.get("invite-codex-member") or {}
                if isinstance(invite_output, dict):
                    invite_email = invite_email or str(invite_output.get("invite_email") or "").strip()
                    invite_id = invite_id or str(invite_output.get("invite_id") or "").strip()
            summary = _apply(
                lambda seat_allocations: _team_auth_remove_seat_allocations(
                    seat_allocations=seat_allocations,
                    invite_email=invite_email,
                    invite_id=invite_id,
                    member_user_id=member_user_id,
                )
            )
            if summary is not None:
                seat_state_changed = True

    if seat_state_changed:
        summary = _team_auth_get_seat_summary(
            shared_root=shared_root,
            team_auth_path=normalized_path,
        )
        _json_log(
            {
                "event": "register_team_auth_seat_cache_synced",
                "workerId": worker_label,
                "taskIndex": task_index,
                "instanceRole": str(instance_role or "").strip().lower(),
                "teamAuthPath": normalized_path,
                "seatSummary": summary,
            }
        )


def _team_auth_sync_codex_seats_from_cleanup_result(
    *,
    shared_root: Path,
    cleanup_result: dict[str, Any],
    worker_label: str,
    task_index: int,
) -> None:
    results = cleanup_result.get("results")
    if not isinstance(results, list) or not results:
        return
    changed_records: list[dict[str, Any]] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        team_auth_path = str(item.get("teamAuthPath") or "").strip()
        if not team_auth_path:
            continue
        response_payload = item.get("response") if isinstance(item.get("response"), dict) else {}
        projected_snapshot = response_payload.get("seatSnapshotAfterProjected") if isinstance(response_payload, dict) else None
        if isinstance(projected_snapshot, dict) and isinstance(projected_snapshot.get("allocations"), list):
            summary = _team_auth_replace_seat_allocations(
                shared_root=shared_root,
                team_auth_path=team_auth_path,
                identity=None,
                allocations=projected_snapshot.get("allocations") or [],
            )
            if summary is not None:
                changed_records.append(
                    {
                        "teamAuthPath": team_auth_path,
                        "seatSummary": summary,
                        "mode": "projected_snapshot_replace",
                    }
                )
            continue
        operations = response_payload.get("operations") if isinstance(response_payload, dict) else None
        if bool(item.get("ok")):
            summary = _team_auth_update_seat_state(
                shared_root=shared_root,
                team_auth_path=team_auth_path,
                identity=None,
                updater=lambda seat_allocations: _team_auth_remove_seat_allocations(
                    seat_allocations=seat_allocations,
                    seat_category="codex",
                ),
            )
            if summary is not None:
                changed_records.append(
                    {
                        "teamAuthPath": team_auth_path,
                        "seatSummary": summary,
                        "mode": "clear_codex_category",
                    }
                )
            continue
        if not isinstance(operations, list):
            continue
        for operation in operations:
            if not isinstance(operation, dict) or not bool(operation.get("ok")):
                continue
            seat_category = _team_auth_seat_category_for_type(operation.get("seat_type"))
            if seat_category != "codex":
                continue
            summary = _team_auth_update_seat_state(
                shared_root=shared_root,
                team_auth_path=team_auth_path,
                identity=None,
                updater=lambda seat_allocations, operation=operation: _team_auth_remove_seat_allocations(
                    seat_allocations=seat_allocations,
                    invite_email=str(operation.get("email") or "").strip(),
                    invite_id=str(operation.get("id") or "").strip() if str(operation.get("kind") or "").strip().lower() == "invite" else "",
                    member_user_id=str(operation.get("id") or "").strip() if str(operation.get("kind") or "").strip().lower() == "user" else "",
                    seat_category="codex",
                ),
            )
            if summary is not None:
                changed_records.append(
                    {
                        "teamAuthPath": team_auth_path,
                        "seatSummary": summary,
                        "mode": "operation_match",
                    }
                )
    if changed_records:
        _json_log(
            {
                "event": "register_team_auth_cleanup_seat_cache_synced",
                "workerId": worker_label,
                "taskIndex": task_index,
                "results": changed_records,
            }
        )


def _choose_weighted_team_auth_candidate(
    *,
    candidates: list[str],
    shared_root: Path,
    instance_role: str,
    required_codex_seats: int = 0,
    required_chatgpt_seats: int = 0,
) -> str:
    if not candidates:
        return ""
    rng = random.SystemRandom()
    weighted_candidates: list[tuple[str, float]] = []
    total_weight = 0.0
    for candidate in candidates:
        base_weight = max(
            0.0,
            float(
                _team_auth_selection_weight(
                    team_auth_path=candidate,
                    shared_root=shared_root,
                    instance_role=instance_role,
                )
            ),
        )
        seat_multiplier = 1.0
        if required_chatgpt_seats > 0 or required_codex_seats > 0:
            seat_summary = _team_auth_get_seat_summary(
                shared_root=shared_root,
                team_auth_path=candidate,
            )
            if required_chatgpt_seats > 0:
                seat_focus = int(seat_summary.get("available_chatgpt") or 0)
            else:
                seat_focus = int(seat_summary.get("available_codex") or 0)
            seat_multiplier = max(1.0, min(5.0, float(max(0, seat_focus))))
        weight = max(0.0, float(base_weight * seat_multiplier))
        if weight <= 0.0:
            continue
        weighted_candidates.append((candidate, weight))
        total_weight += weight
    if total_weight <= 0.0 or not weighted_candidates:
        return rng.choice(candidates)
    threshold = rng.uniform(0.0, total_weight)
    running_weight = 0.0
    for candidate, weight in weighted_candidates:
        running_weight += weight
        if threshold <= running_weight:
            return candidate
    return weighted_candidates[-1][0]


def _team_mother_availability_state_path(
    *,
    shared_root: Path,
    original_name: str,
    email: str,
    account_id: str,
) -> Path:
    cooldown_dir = _team_mother_cooldowns_dir_for_shared_root(shared_root=shared_root)
    _ensure_directory(cooldown_dir)
    return cooldown_dir / f"{_team_mother_cooldown_key(original_name=original_name, email=email, account_id=account_id)}.json"


def _load_team_mother_availability_state(
    *,
    shared_root: Path,
    original_name: str,
    email: str,
    account_id: str,
) -> tuple[Path, dict[str, Any]]:
    state_path = _team_mother_availability_state_path(
        shared_root=shared_root,
        original_name=original_name,
        email=email,
        account_id=account_id,
    )
    payload = _load_json_dict(state_path) if state_path.is_file() else {}
    return state_path, payload


def _team_mother_availability_state_lock_path(*, state_path: Path) -> Path:
    return state_path.with_suffix(state_path.suffix + ".lock")


def _acquire_team_mother_availability_state_lock(*, state_path: Path) -> Path:
    lock_path = _team_mother_availability_state_lock_path(state_path=state_path)
    _ensure_directory(lock_path.parent)
    deadline = time.time() + _team_auth_state_lock_timeout_seconds()
    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(str(os.getpid()))
            return lock_path
        except FileExistsError:
            if time.time() >= deadline:
                raise RuntimeError(f"team_auth_state_lock_timeout:{state_path}")
            time.sleep(0.05)


def _release_team_mother_availability_state_lock(*, lock_path: Path | None) -> None:
    if lock_path is None:
        return
    lock_path.unlink(missing_ok=True)


def _write_team_mother_availability_state(*, state_path: Path, payload: dict[str, Any]) -> None:
    identity_payload = {
        "original_name": str(payload.get("original_name") or "").strip(),
        "email": str(payload.get("email") or "").strip(),
        "account_id": str(payload.get("account_id") or "").strip(),
    }
    cooldown_keys = {
        "reason",
        "cooldown_seconds",
        "cooldown_started_at",
        "cooldown_until",
        "cooldown_until_ts",
    }
    blacklist_keys = {
        "blacklist_reason",
        "blacklist_seconds",
        "blacklist_started_at",
        "blacklist_until",
        "blacklist_until_ts",
    }
    stats_keys = {
        "recent_invite_results",
        "recent_team_expand_results",
        "seat_allocations",
        "seat_summary",
        "last_seat_sync_at",
    }
    seat_allocations = _team_auth_normalize_seat_allocations(payload.get("seat_allocations"))
    seat_summary = _team_auth_seat_summary_from_allocations(seat_allocations)
    normalized_payload = {
        **identity_payload,
        **{key: payload.get(key) for key in cooldown_keys if key in payload},
        **{key: payload.get(key) for key in blacklist_keys if key in payload},
        **{key: payload.get(key) for key in stats_keys if key in payload},
        "seat_summary": seat_summary,
    }
    if seat_allocations:
        normalized_payload["seat_allocations"] = seat_allocations
    else:
        normalized_payload.pop("seat_allocations", None)
    has_active_window = any(
        str(normalized_payload.get(key) or "").strip()
        for key in ("cooldown_until", "blacklist_until")
    )
    has_recent_history = bool(normalized_payload.get("recent_invite_results")) or bool(
        normalized_payload.get("recent_team_expand_results")
    )
    has_seat_allocations = bool(seat_allocations)
    if not has_active_window and not has_recent_history and not has_seat_allocations:
        state_path.unlink(missing_ok=True)
        return
    _write_json_atomic(
        state_path,
        normalized_payload,
        include_pid=True,
        cleanup_temp=True,
    )


def _prune_team_mother_availability_state(
    *,
    state_path: Path,
    payload: dict[str, Any],
) -> dict[str, Any]:
    if not payload:
        return {}
    now_ts = time.time()
    normalized = dict(payload)
    changed = False

    try:
        cooldown_until_ts = float(normalized.get("cooldown_until_ts") or 0.0)
    except Exception:
        cooldown_until_ts = 0.0
    if cooldown_until_ts > 0 and cooldown_until_ts <= now_ts:
        for key in (
            "reason",
            "cooldown_seconds",
            "cooldown_started_at",
            "cooldown_until",
            "cooldown_until_ts",
        ):
            if key in normalized:
                normalized.pop(key, None)
                changed = True

    try:
        blacklist_until_ts = float(normalized.get("blacklist_until_ts") or 0.0)
    except Exception:
        blacklist_until_ts = 0.0
    if blacklist_until_ts > 0 and blacklist_until_ts <= now_ts:
        for key in (
            "blacklist_reason",
            "blacklist_seconds",
            "blacklist_started_at",
            "blacklist_until",
            "blacklist_until_ts",
        ):
            if key in normalized:
                normalized.pop(key, None)
                changed = True

    recent_results = normalized.get("recent_invite_results")
    if isinstance(recent_results, list):
        cutoff_ts = now_ts - _team_auth_zero_success_window_seconds()
        filtered_results: list[dict[str, Any]] = []
        for item in recent_results:
            if not isinstance(item, dict):
                continue
            try:
                result_ts = float(item.get("ts") or 0.0)
            except Exception:
                result_ts = 0.0
            if result_ts <= 0.0 or result_ts < cutoff_ts:
                continue
            filtered_results.append(
                {
                    "ts": result_ts,
                    "at": str(item.get("at") or "").strip(),
                    "ok": bool(item.get("ok")),
                    "step": str(item.get("step") or "").strip(),
                }
            )
        filtered_results = filtered_results[-200:]
        if filtered_results != recent_results:
            changed = True
        if filtered_results:
            normalized["recent_invite_results"] = filtered_results
        elif "recent_invite_results" in normalized:
            normalized.pop("recent_invite_results", None)
            changed = True

    recent_team_expand_results = normalized.get("recent_team_expand_results")
    if isinstance(recent_team_expand_results, list):
        cutoff_ts = now_ts - _team_auth_team_expand_window_seconds()
        filtered_expand_results: list[dict[str, Any]] = []
        for item in recent_team_expand_results:
            if not isinstance(item, dict):
                continue
            try:
                result_ts = float(item.get("ts") or 0.0)
            except Exception:
                result_ts = 0.0
            if result_ts <= 0.0 or result_ts < cutoff_ts:
                continue
            filtered_expand_results.append(
                {
                    "ts": result_ts,
                    "at": str(item.get("at") or "").strip(),
                    "all_failed": bool(item.get("all_failed")),
                    "status": str(item.get("status") or "").strip(),
                }
            )
        filtered_expand_results = filtered_expand_results[-200:]
        if filtered_expand_results != recent_team_expand_results:
            changed = True
        if filtered_expand_results:
            normalized["recent_team_expand_results"] = filtered_expand_results
        elif "recent_team_expand_results" in normalized:
            normalized.pop("recent_team_expand_results", None)
            changed = True

    seat_allocations = _team_auth_normalize_seat_allocations(normalized.get("seat_allocations"))
    seat_allocations, pruned_expired_reservations = _team_auth_prune_expired_seat_allocations(
        seat_allocations,
        now_ts=now_ts,
    )
    if pruned_expired_reservations:
        changed = True
    existing_seat_allocations = normalized.get("seat_allocations")
    if seat_allocations != existing_seat_allocations:
        changed = True
    if seat_allocations:
        normalized["seat_allocations"] = seat_allocations
    elif "seat_allocations" in normalized:
        normalized.pop("seat_allocations", None)
        changed = True
    normalized["seat_summary"] = _team_auth_seat_summary_from_allocations(seat_allocations)

    has_active_window = any(
        float(normalized.get(key) or 0.0) > now_ts
        for key in ("cooldown_until_ts", "blacklist_until_ts")
        if str(normalized.get(key) or "").strip()
    )
    has_recent_history = bool(normalized.get("recent_invite_results")) or bool(normalized.get("recent_team_expand_results"))
    has_seat_allocations = bool(seat_allocations)
    if not has_active_window and not has_recent_history and not has_seat_allocations:
        state_path.unlink(missing_ok=True)
        return {}
    if changed:
        _write_team_mother_availability_state(state_path=state_path, payload=normalized)
    return normalized


def _invite_step_outcome(result_payload: dict[str, Any]) -> tuple[str, bool | None]:
    steps = result_payload.get("steps") if isinstance(result_payload, dict) else {}
    if not isinstance(steps, dict):
        return "", None
    for step_name in ("invite-codex-member", "invite-team-members"):
        status = str(steps.get(step_name) or "").strip().lower()
        if status == "ok":
            return step_name, True
        if status == "failed":
            return step_name, False
    return "", None


def _record_team_auth_recent_invite_result(
    *,
    shared_root: Path,
    team_auth_path: str,
    result_payload: dict[str, Any],
    identity: dict[str, str] | None = None,
) -> None:
    normalized_path = str(team_auth_path or "").strip()
    if not normalized_path:
        return
    step_name, invite_ok = _invite_step_outcome(result_payload)
    if invite_ok is None:
        return
    resolved_identity = {
        **_team_mother_identity_from_team_auth_path(normalized_path),
        **(identity or {}),
    }
    if not any(str(resolved_identity.get(key) or "").strip() for key in ("original_name", "email", "account_id")):
        return
    state_path, payload = _load_team_mother_availability_state(
        shared_root=shared_root,
        original_name=str(resolved_identity.get("original_name") or "").strip(),
        email=str(resolved_identity.get("email") or "").strip(),
        account_id=str(resolved_identity.get("account_id") or "").strip(),
    )
    normalized = _prune_team_mother_availability_state(state_path=state_path, payload=payload)
    recent_results = normalized.get("recent_invite_results")
    if not isinstance(recent_results, list):
        recent_results = []
    now = datetime.now(timezone.utc)
    recent_results.append(
        {
            "ts": now.timestamp(),
            "at": now.isoformat(),
            "ok": bool(invite_ok),
            "step": step_name,
        }
    )
    normalized.update(
        {
            "original_name": str(resolved_identity.get("original_name") or "").strip(),
            "email": str(resolved_identity.get("email") or "").strip(),
            "account_id": str(resolved_identity.get("account_id") or "").strip(),
            "recent_invite_results": recent_results[-200:],
        }
    )
    _write_team_mother_availability_state(state_path=state_path, payload=normalized)


def _record_team_auth_recent_team_expand_result(
    *,
    shared_root: Path,
    team_auth_path: str,
    result_payload: dict[str, Any],
    instance_role: str,
    identity: dict[str, str] | None = None,
) -> None:
    if str(instance_role or "").strip().lower() != "team":
        return
    normalized_path = str(team_auth_path or "").strip()
    if not normalized_path:
        return
    steps = result_payload.get("steps") if isinstance(result_payload, dict) else {}
    outputs = result_payload.get("outputs") if isinstance(result_payload, dict) else {}
    if not isinstance(steps, dict) or not isinstance(outputs, dict):
        return
    invite_status = str(steps.get("invite-team-members") or "").strip().lower()
    invite_output = outputs.get("invite-team-members")
    if invite_status != "ok" or not isinstance(invite_output, dict):
        return
    if "allInviteAttemptsFailed" not in invite_output:
        return
    resolved_identity = {
        **_team_mother_identity_from_team_auth_path(normalized_path),
        **(identity or {}),
    }
    if not any(str(resolved_identity.get(key) or "").strip() for key in ("original_name", "email", "account_id")):
        return
    state_path, payload = _load_team_mother_availability_state(
        shared_root=shared_root,
        original_name=str(resolved_identity.get("original_name") or "").strip(),
        email=str(resolved_identity.get("email") or "").strip(),
        account_id=str(resolved_identity.get("account_id") or "").strip(),
    )
    normalized = _prune_team_mother_availability_state(state_path=state_path, payload=payload)
    recent_results = normalized.get("recent_team_expand_results")
    if not isinstance(recent_results, list):
        recent_results = []
    now = datetime.now(timezone.utc)
    recent_results.append(
        {
            "ts": now.timestamp(),
            "at": now.isoformat(),
            "all_failed": bool(invite_output.get("allInviteAttemptsFailed")),
            "status": str(invite_output.get("status") or "").strip(),
        }
    )
    normalized.update(
        {
            "original_name": str(resolved_identity.get("original_name") or "").strip(),
            "email": str(resolved_identity.get("email") or "").strip(),
            "account_id": str(resolved_identity.get("account_id") or "").strip(),
            "recent_team_expand_results": recent_results[-200:],
        }
    )
    _write_team_mother_availability_state(state_path=state_path, payload=normalized)


def _team_auth_is_recent_zero_success(
    *,
    shared_root: Path,
    team_auth_path: str,
) -> tuple[bool, dict[str, Any]]:
    window_seconds = _team_auth_zero_success_window_seconds()
    min_attempts = _team_auth_zero_success_min_attempts()
    if window_seconds <= 0.0 or min_attempts <= 0:
        return False, {}
    identity = _team_mother_identity_from_team_auth_path(team_auth_path)
    if not any(str(identity.get(key) or "").strip() for key in ("original_name", "email", "account_id")):
        return False, {}
    state_path, payload = _load_team_mother_availability_state(
        shared_root=shared_root,
        original_name=str(identity.get("original_name") or "").strip(),
        email=str(identity.get("email") or "").strip(),
        account_id=str(identity.get("account_id") or "").strip(),
    )
    normalized = _prune_team_mother_availability_state(state_path=state_path, payload=payload)
    recent_results = normalized.get("recent_invite_results")
    if not isinstance(recent_results, list) or not recent_results:
        return False, {}
    attempts = len(recent_results)
    success_count = sum(1 for item in recent_results if isinstance(item, dict) and bool(item.get("ok")))
    if attempts < min_attempts or success_count > 0:
        return False, {}
    return True, {
        "state_path": str(state_path),
        "original_name": str(identity.get("original_name") or "").strip(),
        "email": str(identity.get("email") or "").strip(),
        "account_id": str(identity.get("account_id") or "").strip(),
        "window_seconds": window_seconds,
        "attempts": attempts,
        "successes": success_count,
    }


def _team_auth_recent_team_expand_weight_info(
    *,
    shared_root: Path,
    team_auth_path: str,
) -> dict[str, Any]:
    identity = _team_mother_identity_from_team_auth_path(team_auth_path)
    if not any(str(identity.get(key) or "").strip() for key in ("original_name", "email", "account_id")):
        return {
            "weight": 1.0,
            "failures": 0,
            "successes": 0,
            "penaltyUnits": 0.0,
            "windowSeconds": _team_auth_team_expand_window_seconds(),
        }
    state_path, payload = _load_team_mother_availability_state(
        shared_root=shared_root,
        original_name=str(identity.get("original_name") or "").strip(),
        email=str(identity.get("email") or "").strip(),
        account_id=str(identity.get("account_id") or "").strip(),
    )
    normalized = _prune_team_mother_availability_state(state_path=state_path, payload=payload)
    recent_results = normalized.get("recent_team_expand_results")
    if not isinstance(recent_results, list) or not recent_results:
        return {
            "weight": 1.0,
            "failures": 0,
            "successes": 0,
            "penaltyUnits": 0.0,
            "windowSeconds": _team_auth_team_expand_window_seconds(),
        }
    failure_count = sum(
        1
        for item in recent_results
        if isinstance(item, dict) and bool(item.get("all_failed"))
    )
    success_count = sum(
        1
        for item in recent_results
        if isinstance(item, dict) and not bool(item.get("all_failed"))
    )
    penalty_units = max(
        0.0,
        float(failure_count) - (float(success_count) * _team_auth_team_expand_success_credit()),
    )
    if penalty_units <= 0.0:
        weight = 1.0
    else:
        weight = max(
            _team_auth_team_expand_floor_weight(),
            1.0 - (penalty_units * _team_auth_team_expand_failure_weight_step()),
        )
    return {
        "weight": float(weight),
        "failures": int(failure_count),
        "successes": int(success_count),
        "penaltyUnits": float(round(penalty_units, 6)),
        "windowSeconds": _team_auth_team_expand_window_seconds(),
        "statePath": str(state_path),
    }


def _team_auth_is_temp_blacklisted(
    *,
    shared_root: Path,
    team_auth_path: str,
) -> tuple[bool, dict[str, Any]]:
    identity = _team_mother_identity_from_team_auth_path(team_auth_path)
    if not any(str(identity.get(key) or "").strip() for key in ("original_name", "email", "account_id")):
        return False, {}
    state_path, payload = _load_team_mother_availability_state(
        shared_root=shared_root,
        original_name=str(identity.get("original_name") or "").strip(),
        email=str(identity.get("email") or "").strip(),
        account_id=str(identity.get("account_id") or "").strip(),
    )
    normalized = _prune_team_mother_availability_state(state_path=state_path, payload=payload)
    if not normalized:
        return False, {}
    try:
        until_ts = float(normalized.get("blacklist_until_ts") or 0.0)
    except Exception:
        until_ts = 0.0
    now_ts = time.time()
    if until_ts <= now_ts:
        return False, {}
    return True, {
        "state_path": str(state_path),
        "original_name": str(identity.get("original_name") or "").strip(),
        "email": str(identity.get("email") or "").strip(),
        "account_id": str(identity.get("account_id") or "").strip(),
        "blacklist_until": str(normalized.get("blacklist_until") or "").strip(),
        "blacklist_until_ts": until_ts,
        "remaining_seconds": round(max(0.0, until_ts - now_ts), 3),
        "reason": str(normalized.get("blacklist_reason") or "").strip(),
    }


def _mark_team_auth_temporary_blacklist(
    *,
    shared_root: Path,
    team_auth_path: str,
    identity: dict[str, str] | None,
    reason: str,
    blacklist_seconds: float,
    worker_label: str,
    task_index: int,
) -> dict[str, Any] | None:
    resolved_identity = {
        **_team_mother_identity_from_team_auth_path(team_auth_path),
        **(identity or {}),
    }
    original_name = str(resolved_identity.get("original_name") or "").strip()
    email = str(resolved_identity.get("email") or "").strip()
    account_id = str(resolved_identity.get("account_id") or "").strip()
    if not any((original_name, email, account_id)):
        return None
    state_path, payload = _load_team_mother_availability_state(
        shared_root=shared_root,
        original_name=original_name,
        email=email,
        account_id=account_id,
    )
    normalized = _prune_team_mother_availability_state(state_path=state_path, payload=payload)
    now = datetime.now(timezone.utc)
    blacklist_seconds = max(0.0, float(blacklist_seconds or 0.0))
    blacklist_until = now + timedelta(seconds=blacklist_seconds)
    normalized.update(
        {
            "original_name": original_name,
            "email": email,
            "account_id": account_id,
            "blacklist_reason": str(reason or "").strip(),
            "blacklist_seconds": blacklist_seconds,
            "blacklist_started_at": now.isoformat(),
            "blacklist_until": blacklist_until.isoformat(),
            "blacklist_until_ts": blacklist_until.timestamp(),
        }
    )
    _write_team_mother_availability_state(state_path=state_path, payload=normalized)
    result = {
        "state_path": str(state_path),
        "original_name": original_name,
        "email": email,
        "account_id": account_id,
        "blacklist_reason": str(reason or "").strip(),
        "blacklist_seconds": blacklist_seconds,
        "blacklist_started_at": now.isoformat(),
        "blacklist_until": blacklist_until.isoformat(),
        "blacklist_until_ts": blacklist_until.timestamp(),
        "team_auth_path": str(team_auth_path or "").strip(),
    }
    _json_log(
        {
            "event": "register_team_auth_temp_blacklist_marked",
            "workerId": worker_label,
            "taskIndex": task_index,
            **result,
        }
    )
    return result


def _clear_team_auth_temporary_blacklist(
    *,
    shared_root: Path,
    team_auth_path: str,
    identity: dict[str, str] | None,
    worker_label: str,
    task_index: int,
) -> bool:
    resolved_identity = {
        **_team_mother_identity_from_team_auth_path(team_auth_path),
        **(identity or {}),
    }
    original_name = str(resolved_identity.get("original_name") or "").strip()
    email = str(resolved_identity.get("email") or "").strip()
    account_id = str(resolved_identity.get("account_id") or "").strip()
    if not any((original_name, email, account_id)):
        return False
    state_path, payload = _load_team_mother_availability_state(
        shared_root=shared_root,
        original_name=original_name,
        email=email,
        account_id=account_id,
    )
    normalized = _prune_team_mother_availability_state(state_path=state_path, payload=payload)
    if not normalized or not any(key in normalized for key in ("blacklist_until", "blacklist_until_ts")):
        return False
    for key in (
        "blacklist_reason",
        "blacklist_seconds",
        "blacklist_started_at",
        "blacklist_until",
        "blacklist_until_ts",
    ):
        normalized.pop(key, None)
    _write_team_mother_availability_state(state_path=state_path, payload=normalized)
    _json_log(
        {
            "event": "register_team_auth_temp_blacklist_cleared",
            "workerId": worker_label,
            "taskIndex": task_index,
            "statePath": str(state_path),
            "original_name": original_name,
            "email": email,
            "account_id": account_id,
            "team_auth_path": str(team_auth_path or "").strip(),
        }
    )
    return True


def _team_cleanup_recently_ran(*, shared_root: Path, cooldown_seconds: float) -> bool:
    if float(cooldown_seconds or 0.0) <= 0:
        return False
    payload = _load_team_cleanup_state(shared_root=shared_root)
    timestamp_text = str(payload.get("lastFinishedAt") or payload.get("lastStartedAt") or "").strip()
    if not timestamp_text:
        return False
    try:
        timestamp = datetime.fromisoformat(timestamp_text.replace("Z", "+00:00"))
    except ValueError:
        return False
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - timestamp).total_seconds() < float(cooldown_seconds)


def _try_acquire_team_cleanup_lock(*, shared_root: Path) -> bool:
    state_dir = _team_auth_state_dir(shared_root=shared_root)
    _ensure_directory(state_dir)
    lock_path = _team_cleanup_lock_path(shared_root=shared_root)
    return try_acquire_lock(
        lock_path,
        stale_after_seconds=max(0.0, _env_float("REGISTER_TEAM_CLEANUP_LOCK_STALE_SECONDS", 600.0)),
    )


def _release_team_cleanup_lock(*, shared_root: Path) -> None:
    release_lock(_team_cleanup_lock_path(shared_root=shared_root))


def _try_acquire_mailbox_cleanup_lock(*, shared_root: Path) -> bool:
    lock_path = _mailbox_cleanup_lock_path(shared_root=shared_root)
    return try_acquire_lock(
        lock_path,
        stale_after_seconds=max(0.0, _env_float("REGISTER_MAILBOX_CLEANUP_LOCK_STALE_SECONDS", 600.0)),
    )


def _release_mailbox_cleanup_lock(*, shared_root: Path) -> None:
    release_lock(_mailbox_cleanup_lock_path(shared_root=shared_root))


def _mailbox_cleanup_recently_ran(*, shared_root: Path, cooldown_seconds: float) -> bool:
    if float(cooldown_seconds or 0.0) <= 0:
        return False
    payload = _load_mailbox_cleanup_state(shared_root=shared_root)
    timestamp_text = str(payload.get("lastFinishedAt") or payload.get("lastStartedAt") or "").strip()
    if not timestamp_text:
        return False
    try:
        timestamp = datetime.fromisoformat(timestamp_text.replace("Z", "+00:00"))
    except ValueError:
        return False
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - timestamp).total_seconds() < float(cooldown_seconds)


def _trigger_mailbox_capacity_recovery(*, shared_root: Path, detail: str) -> dict[str, Any]:
    cooldown_seconds = _env_float("REGISTER_MAILBOX_CLEANUP_COOLDOWN_SECONDS", 120.0)
    if _mailbox_cleanup_recently_ran(shared_root=shared_root, cooldown_seconds=cooldown_seconds):
        payload = _load_mailbox_cleanup_state(shared_root=shared_root)
        return {
            "ok": False,
            "status": "recovery_recently_ran",
            "lastResult": payload.get("lastResult") if isinstance(payload.get("lastResult"), dict) else {},
        }
    if not _try_acquire_mailbox_cleanup_lock(shared_root=shared_root):
        return {"ok": False, "status": "recovery_locked"}
    started_at = datetime.now(timezone.utc).isoformat()
    state_payload = {
        "lastStartedAt": started_at,
        "lastFinishedAt": "",
        "triggerDetail": str(detail or "").strip(),
        "lastResult": {},
        "consecutiveFailures": 0,
    }
    _write_mailbox_cleanup_state(shared_root=shared_root, payload=state_payload)
    try:
        provider_type_key = _infer_mailbox_capacity_provider_type_key(detail=detail)
        try:
            result = dispatch_easyemail_step(
                step_type="recover_mailbox_capacity",
                step_input={
                    "failure_code": "mailbox_unavailable",
                    "detail": str(detail or "").strip(),
                    "provider_type_key": provider_type_key,
                    "force": True,
                    "stale_after_seconds": 0,
                    "max_delete_count": _env_int("REGISTER_MAILBOX_CLEANUP_MAX_DELETE_COUNT", 30),
                },
            )
            ok = bool(result.get("ok")) if isinstance(result, dict) else False
            status = str(result.get("status") or "recovery_finished") if isinstance(result, dict) else "recovery_finished"
        except Exception as exc:
            result = {"detail": str(exc)}
            ok = False
            status = "recovery_failed"
        finished_at = datetime.now(timezone.utc).isoformat()
        state_payload.update(
            {
                "lastFinishedAt": finished_at,
                "lastResult": {
                    "ok": ok,
                    "status": status,
                    "result": result,
                },
                "consecutiveFailures": 0,
            }
        )
        _write_mailbox_cleanup_state(shared_root=shared_root, payload=state_payload)
        return {
            "ok": ok,
            "status": status,
            "result": result,
        }
    finally:
        _release_mailbox_cleanup_lock(shared_root=shared_root)


def _infer_mailbox_capacity_provider_type_key(*, detail: str) -> str:
    lowered = str(detail or "").strip().lower()
    if not lowered:
        return ""
    if "moemail" in lowered:
        return "moemail"
    return ""


def _mailbox_capacity_failure_detail(*, result_payload: dict[str, Any]) -> str:
    error_step = str(result_payload.get("errorStep") or "").strip().lower()
    if error_step != "acquire-mailbox":
        return ""
    combined_parts = [str(result_payload.get("error") or "").strip()]
    step_errors = result_payload.get("stepErrors") or {}
    mailbox_error = step_errors.get("acquire-mailbox") or {}
    mailbox_error_code = str(mailbox_error.get("code") or "").strip().lower()
    combined_parts.append(str(mailbox_error.get("message") or "").strip())
    combined_parts.append(str(mailbox_error.get("code") or "").strip())
    combined = " ".join(part for part in combined_parts if part).strip().lower()
    if mailbox_error_code in {
        "mailbox_unavailable",
        "moemail_capacity_exhausted",
        "moemail_upstream_transient",
    } or any(
        marker in combined
        for marker in (
            "mailbox_unavailable",
            "code=mailbox_capacity_unavailable",
            "code=mailbox_upstream_transient",
            "moemail_capacity_exhausted",
            "moemail_upstream_transient",
            "maximum mailbox",
            "mailbox count limit",
            "最大邮箱数量限制",
        )
    ):
        return " ".join(part for part in combined_parts if part).strip()
    return ""


def _mailbox_domain_blacklist_min_attempts() -> int:
    return max(1, _env_int("REGISTER_MAILBOX_DOMAIN_BLACKLIST_MIN_ATTEMPTS", 20))


def _mailbox_domain_blacklist_failure_rate() -> float:
    return _env_percent("REGISTER_MAILBOX_DOMAIN_BLACKLIST_FAILURE_RATE", 90.0)


def _mailbox_provider_from_ref(mailbox_ref: str) -> str:
    value = str(mailbox_ref or "").strip()
    if not value:
        return ""
    if ":" not in value:
        return "moemail"
    return str(value.split(":", 1)[0] or "").strip().lower()


def _extract_mailbox_business_outcome_context(*, result_payload: dict[str, Any]) -> dict[str, str]:
    steps = result_payload.get("steps") if isinstance(result_payload, dict) else {}
    if isinstance(steps, dict) and str(steps.get("acquire-mailbox") or "").strip().lower() != "ok":
        return {}
    outputs = result_payload.get("outputs") if isinstance(result_payload, dict) else {}
    if not isinstance(outputs, dict):
        return {}
    mailbox_output = outputs.get("acquire-mailbox")
    mailbox_output = mailbox_output if isinstance(mailbox_output, dict) else {}
    create_output = outputs.get("create-openai-account")
    create_output = create_output if isinstance(create_output, dict) else {}
    email = str(
        mailbox_output.get("email")
        or mailbox_output.get("emailAddress")
        or create_output.get("email")
        or ""
    ).strip().lower()
    mailbox_ref = str(
        mailbox_output.get("mailbox_ref")
        or mailbox_output.get("mailboxRef")
        or ""
    ).strip()
    provider = str(
        mailbox_output.get("provider")
        or mailbox_output.get("providerTypeKey")
        or ""
    ).strip().lower()
    if not provider and mailbox_ref:
        provider = _mailbox_provider_from_ref(mailbox_ref)
    if "@" not in email:
        return {
            "provider": provider,
            "mailbox_ref": mailbox_ref,
            "email": email,
            "domain": "",
        }
    return {
        "provider": provider,
        "mailbox_ref": mailbox_ref,
        "email": email,
        "domain": email.rsplit("@", 1)[-1].strip().lower(),
    }


def _record_business_mailbox_domain_outcome(
    *,
    shared_root: Path,
    result_payload: dict[str, Any],
    instance_role: str,
) -> dict[str, Any] | None:
    normalized_role = str(instance_role or "").strip().lower()
    if normalized_role not in {"main", "continue"}:
        return None
    context = _extract_mailbox_business_outcome_context(result_payload=result_payload)
    provider = str(context.get("provider") or "").strip().lower()
    domain = str(context.get("domain") or "").strip().lower()
    email = str(context.get("email") or "").strip().lower()
    if provider != "moemail" or not domain:
        return None

    payload = _load_mailbox_domain_stats_state(shared_root=shared_root)
    domains_payload = payload.get("domains")
    domains = dict(domains_payload) if isinstance(domains_payload, dict) else {}
    current = domains.get(domain)
    current = dict(current) if isinstance(current, dict) else {}
    attempts = max(0, int(current.get("attempts") or 0)) + 1
    successes = max(0, int(current.get("successes") or 0))
    failures = max(0, int(current.get("failures") or 0))
    ok = bool(result_payload.get("ok"))
    now = datetime.now(timezone.utc).isoformat()
    if ok:
        successes += 1
    else:
        failures += 1
    failure_rate = (float(failures) / float(attempts)) * 100.0 if attempts > 0 else 0.0
    blacklisted = (
        attempts >= _mailbox_domain_blacklist_min_attempts()
        and failure_rate >= _mailbox_domain_blacklist_failure_rate()
    )
    domains[domain] = {
        "provider": provider,
        "attempts": attempts,
        "successes": successes,
        "failures": failures,
        "lastOutcome": "success" if ok else "failure",
        "lastOutcomeAt": now,
        "lastEmail": email,
        "lastSuccessAt": now if ok else str(current.get("lastSuccessAt") or "").strip(),
        "lastFailureAt": now if not ok else str(current.get("lastFailureAt") or "").strip(),
        "blacklisted": blacklisted,
    }
    payload["updatedAt"] = now
    payload["domains"] = domains
    _write_mailbox_domain_stats_state(shared_root=shared_root, payload=payload)
    return {
        "provider": provider,
        "domain": domain,
        "email": email,
        "attempts": attempts,
        "successes": successes,
        "failures": failures,
        "failureRate": round(failure_rate, 3),
        "blacklisted": blacklisted,
        "minAttempts": _mailbox_domain_blacklist_min_attempts(),
        "failureRateThreshold": _mailbox_domain_blacklist_failure_rate(),
        "statePath": str(_mailbox_domain_stats_path(shared_root=shared_root)),
    }


def _mark_mailbox_capacity_failure(*, shared_root: Path, detail: str) -> dict[str, Any]:
    payload = _load_mailbox_cleanup_state(shared_root=shared_root)
    consecutive = int(payload.get("consecutiveFailures") or 0) + 1
    now_text = datetime.now(timezone.utc).isoformat()
    payload.update(
        {
            "consecutiveFailures": consecutive,
            "lastFailureAt": now_text,
            "lastFailureDetail": str(detail or "").strip(),
        }
    )
    _write_mailbox_cleanup_state(shared_root=shared_root, payload=payload)
    threshold = max(1, _env_int("REGISTER_MAILBOX_CLEANUP_FAILURE_THRESHOLD", 3))
    if consecutive < threshold:
        return {
            "ok": False,
            "status": "recovery_threshold_not_reached",
            "consecutiveFailures": consecutive,
            "threshold": threshold,
        }
    return _trigger_mailbox_capacity_recovery(shared_root=shared_root, detail=detail)


def _clear_mailbox_capacity_failures(*, shared_root: Path) -> None:
    payload = _load_mailbox_cleanup_state(shared_root=shared_root)
    if not payload:
        return
    payload["consecutiveFailures"] = 0
    payload["lastRecoveredAt"] = datetime.now(timezone.utc).isoformat()
    _write_mailbox_cleanup_state(shared_root=shared_root, payload=payload)


def _trigger_codex_capacity_cleanup(*, shared_root: Path, team_auth_pool: list[str]) -> dict[str, Any]:
    normalized_pool = [candidate for candidate in team_auth_pool if str(candidate or "").strip()]
    if not normalized_pool:
        return {"ok": False, "status": "cleanup_skipped_empty_pool", "results": []}
    cooldown_seconds = _env_float("REGISTER_TEAM_CLEANUP_COOLDOWN_SECONDS", 180.0)
    if _team_cleanup_recently_ran(shared_root=shared_root, cooldown_seconds=cooldown_seconds):
        payload = _load_team_cleanup_state(shared_root=shared_root)
        return {
            "ok": False,
            "status": "cleanup_recently_ran",
            "results": payload.get("results") if isinstance(payload.get("results"), list) else [],
        }
    if not _try_acquire_team_cleanup_lock(shared_root=shared_root):
        return {"ok": False, "status": "cleanup_locked", "results": []}
    now = datetime.now(timezone.utc)
    summary_payload: dict[str, Any] = {
        "lastStartedAt": now.isoformat(),
        "lastFinishedAt": "",
        "results": [],
    }
    _write_team_cleanup_state(shared_root=shared_root, payload=summary_payload)
    try:
        results: list[dict[str, Any]] = []
        total_revoked_invites = 0
        total_removed_users = 0
        successful_workspaces = 0
        for team_auth_path in normalized_pool:
            try:
                step_result = dispatch_easyprotocol_step(
                    step_type="cleanup_codex_capacity",
                    step_input={"team_auth_path": team_auth_path},
                )
            except Exception as exc:
                step_result = {
                    "ok": False,
                    "status": "cleanup_transport_failed",
                    "detail": str(exc),
                }
            step_payload = step_result if isinstance(step_result, dict) else {"ok": False, "status": "cleanup_invalid_result"}
            results.append({"teamAuthPath": team_auth_path, **step_payload})
            if bool(step_payload.get("ok")):
                successful_workspaces += 1
                total_revoked_invites += int(step_payload.get("revoked_invites") or 0)
                total_removed_users += int(step_payload.get("removed_users") or 0)
                _clear_team_auth_capacity_cooldown(shared_root=shared_root, team_auth_path=team_auth_path)
        finished_at = datetime.now(timezone.utc).isoformat()
        summary_payload = {
            "lastStartedAt": summary_payload["lastStartedAt"],
            "lastFinishedAt": finished_at,
            "successfulWorkspaces": successful_workspaces,
            "totalRevokedInvites": total_revoked_invites,
            "totalRemovedUsers": total_removed_users,
            "results": results,
        }
        _write_team_cleanup_state(shared_root=shared_root, payload=summary_payload)
        return {
            "ok": successful_workspaces > 0,
            "status": "cleanup_finished",
            "successfulWorkspaces": successful_workspaces,
            "totalRevokedInvites": total_revoked_invites,
            "totalRemovedUsers": total_removed_users,
            "results": results,
        }
    finally:
        _release_team_cleanup_lock(shared_root=shared_root)


def _team_capacity_failure_detail(*, result_payload: dict[str, Any]) -> str:
    error_step = str(result_payload.get("errorStep") or "").strip().lower()
    if error_step != "invite-codex-member":
        return ""
    combined_parts = [str(result_payload.get("error") or "").strip()]
    step_errors = result_payload.get("stepErrors") or {}
    invite_error = step_errors.get("invite-codex-member") or {}
    combined_parts.append(str(invite_error.get("message") or "").strip())
    combined_parts.append(str(invite_error.get("code") or "").strip())
    combined = " ".join(part for part in combined_parts if part).lower()
    if "workspace has reached maximum number of seats" in combined or "team_seats_full" in combined:
        return " ".join(part for part in combined_parts if part).strip()
    return ""


def _select_team_auth_path(
    *,
    team_auth_pool: list[str],
    task_index: int,
    shared_root: Path,
    instance_role: str,
    worker_label: str = "",
) -> tuple[str, dict[str, Any] | None]:
    if not team_auth_pool:
        return "", None
    normalized_role = str(instance_role or "").strip().lower()
    seat_request = _team_auth_seat_request_for_role(instance_role=instance_role)
    required_codex_seats = int(seat_request.get("codex") or 0)
    required_chatgpt_seats = int(seat_request.get("chatgpt") or 0)
    reservation_context = f"{str(instance_role or '').strip().lower()}:{task_index}"
    reserved_for_team_expand = (
        _team_mother_reserved_identity_keys_for_shared_root(shared_root=shared_root)
        if normalized_role in {"main", "continue"}
        else set()
    )
    explicit_candidates: list[str] = []
    inferred_candidates: list[str] = []
    seen: set[str] = set()
    for candidate in team_auth_pool:
        normalized = str(candidate or "").strip()
        if not normalized:
            continue
        lowered = normalized.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        if _team_auth_path_is_explicit_mother(Path(normalized)):
            explicit_candidates.append(normalized)
        else:
            inferred_candidates.append(normalized)
    for candidate_pool in (explicit_candidates, inferred_candidates):
        eligible = [
            candidate
            for candidate in candidate_pool
            if not _team_auth_is_capacity_cooled(shared_root=shared_root, team_auth_path=candidate)
            and not _team_auth_is_temp_blacklisted(shared_root=shared_root, team_auth_path=candidate)[0]
            and not _team_auth_is_reserved_for_team_expand(
                shared_root=shared_root,
                team_auth_path=candidate,
                reserved_keys=reserved_for_team_expand,
            )[0]
        ]
        if eligible:
            seat_eligible = [
                candidate
                for candidate in eligible
                if _team_auth_has_required_seats(
                    shared_root=shared_root,
                    team_auth_path=candidate,
                    required_codex_seats=required_codex_seats,
                    required_chatgpt_seats=required_chatgpt_seats,
                )[0]
            ]
            if not seat_eligible:
                continue
            recent_success_filtered = [
                candidate
                for candidate in seat_eligible
                if not _team_auth_is_recent_zero_success(shared_root=shared_root, team_auth_path=candidate)[0]
            ]
            selectable_candidates = list(recent_success_filtered or seat_eligible)
            if required_codex_seats <= 0 and required_chatgpt_seats <= 0:
                return (
                    _choose_weighted_team_auth_candidate(
                        candidates=selectable_candidates,
                        shared_root=shared_root,
                        instance_role=normalized_role,
                        required_codex_seats=required_codex_seats,
                        required_chatgpt_seats=required_chatgpt_seats,
                    ),
                    None,
                )
            remaining_candidates = list(selectable_candidates)
            while remaining_candidates:
                selected_candidate = _choose_weighted_team_auth_candidate(
                    candidates=remaining_candidates,
                    shared_root=shared_root,
                    instance_role=normalized_role,
                    required_codex_seats=required_codex_seats,
                    required_chatgpt_seats=required_chatgpt_seats,
                )
                if not selected_candidate:
                    break
                reserved, reservation, _ = _team_auth_try_reserve_required_seats(
                    shared_root=shared_root,
                    team_auth_path=selected_candidate,
                    required_codex_seats=required_codex_seats,
                    required_chatgpt_seats=required_chatgpt_seats,
                    reservation_owner=worker_label,
                    reservation_context=reservation_context,
                    source_role=str(instance_role or "").strip().lower(),
                )
                if reserved:
                    return selected_candidate, reservation
                remaining_candidates = [
                    candidate
                    for candidate in remaining_candidates
                    if str(candidate or "").strip().lower() != str(selected_candidate or "").strip().lower()
                ]
    return "", None


def _team_auth_blacklist_reason(*, result_payload: dict[str, Any]) -> str:
    error_step = str(result_payload.get("errorStep") or "").strip().lower()
    if error_step not in {
        "invite-codex-member",
        "invite-team-members",
        "cleanup-team-all-seats",
        "revoke-team-members",
        "refresh-team-auth-on-demand",
        "obtain-team-mother-oauth",
    }:
        return ""

    step_errors = result_payload.get("stepErrors") or {}
    step_error = step_errors.get(error_step) or {}
    combined = " ".join(
        part
        for part in (
            str(result_payload.get("error") or "").strip(),
            str(step_error.get("message") or "").strip(),
            str(step_error.get("code") or "").strip(),
        )
        if part
    ).lower()
    if not (
        "token_invalidated" in combined
        or "authentication token has been invalidated" in combined
        or ("status_code': 401" in combined and "please try signing in again" in combined)
        or ('"status_code": 401' in combined and "please try signing in again" in combined)
    ):
        return ""

    step_attempts = result_payload.get("stepAttempts") or {}
    refresh_attempts = int(step_attempts.get("refresh-team-auth-on-demand") or 0)
    mother_refresh_attempts = int(step_attempts.get("obtain-team-mother-oauth") or 0)

    if error_step == "invite-codex-member":
        invite_attempts = int(step_attempts.get("invite-codex-member") or 0)
        if refresh_attempts >= 1 and invite_attempts >= 2:
            return combined.strip()
        return ""

    if error_step in {"invite-team-members", "cleanup-team-all-seats", "revoke-team-members", "refresh-team-auth-on-demand", "obtain-team-mother-oauth"}:
        if refresh_attempts >= 1 or mother_refresh_attempts >= 1:
            return combined.strip()

    return ""


def _json_log(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False), flush=True)


def _build_worker_output_root(*, output_root: Path, worker_id: int) -> Path:
    return output_root / f"worker-{worker_id:02d}"


def _build_run_output_dir(*, worker_output_root: Path, task_index: int) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return worker_output_root / f"run-{timestamp}-task{task_index:06d}"

def _cleanup_dashboard_worker_state_files(*, shared_root: Path, instance_id: str) -> None:
    workers_dir = shared_root / "others" / "dashboard-state" / str(instance_id or "default").strip() / "workers"
    if not workers_dir.is_dir():
        return
    for path in workers_dir.glob("*.json"):
        try:
            path.unlink()
        except FileNotFoundError:
            continue


def _should_cleanup_successful_run_output(result: Any) -> bool:
    try:
        if not bool(getattr(result, "ok", False)):
            return False
        steps = getattr(result, "steps", {}) or {}
        outputs = getattr(result, "outputs", {}) or {}
        if str(steps.get("upload-oauth-artifact") or "").strip().lower() != "ok":
            return False
        upload_output = outputs.get("upload-oauth-artifact")
        if isinstance(upload_output, dict) and bool(upload_output.get("ok")):
            return True
        return False
    except Exception:
        return False


def _resolve_small_success_pool_dir(*, output_root: Path) -> Path:
    explicit = _env_text("REGISTER_SMALL_SUCCESS_POOL_DIR")
    if explicit:
        return Path(explicit).expanduser().resolve()
    return _shared_root_from_output_root(output_root) / "small-success-pool"


def _resolve_small_success_wait_pool_dir(*, output_root: Path) -> Path:
    explicit = _env_text("REGISTER_SMALL_SUCCESS_WAIT_POOL_DIR")
    if explicit:
        return Path(explicit).expanduser().resolve()
    return _shared_root_from_output_root(output_root) / "others" / "small-success-wait-pool"


def _resolve_small_success_continue_pool_dir(*, output_root: Path) -> Path:
    explicit = _env_text("REGISTER_SMALL_SUCCESS_CONTINUE_POOL_DIR")
    if explicit:
        return Path(explicit).expanduser().resolve()
    return _shared_root_from_output_root(output_root) / "others" / "small-success-continue-pool"


def _resolve_free_oauth_pool_dir(*, output_root: Path) -> Path:
    explicit = _env_text("REGISTER_FREE_OAUTH_POOL_DIR")
    if explicit:
        return Path(explicit).expanduser().resolve()
    return _shared_root_from_output_root(output_root) / "free-oauth-pool"


def _resolve_free_manual_oauth_pool_dir(*, output_root: Path) -> Path:
    explicit = _env_text("REGISTER_FREE_MANUAL_OAUTH_POOL_DIR")
    if explicit:
        return Path(explicit).expanduser().resolve()
    return _shared_root_from_output_root(output_root) / "others" / "free-manual-oauth-pool"


def _resolve_free_local_dir(*, output_root: Path) -> Path:
    explicit = _env_text("REGISTER_FREE_LOCAL_DIR")
    if explicit:
        return Path(explicit).expanduser().resolve()
    return _shared_root_from_output_root(output_root) / "others" / "free-local-store"


def _resolve_team_local_dir(*, output_root: Path) -> Path:
    explicit = _env_text("REGISTER_TEAM_LOCAL_DIR")
    if explicit:
        return Path(explicit).expanduser().resolve()
    return _shared_root_from_output_root(output_root) / "others" / "team-local-store"


def _env_percent(name: str, default: float = 0.0) -> float:
    raw = str(os.environ.get(name) or "").strip()
    if not raw:
        return max(0.0, min(100.0, float(default)))
    try:
        value = float(raw)
    except Exception:
        return max(0.0, min(100.0, float(default)))
    if 0.0 < value <= 1.0:
        value *= 100.0
    return max(0.0, min(100.0, value))

def _select_local_split(*, percent: float) -> bool:
    if float(percent or 0.0) <= 0.0:
        return False
    if float(percent) >= 100.0:
        return True
    return random.random() * 100.0 < float(percent)


def _small_success_wait_seconds() -> float:
    return max(0.0, _env_float("REGISTER_SMALL_SUCCESS_WAIT_SECONDS", 600.0))


def _small_success_continue_prefill_count() -> int:
    return max(0, _env_int("REGISTER_SMALL_SUCCESS_CONTINUE_PREFILL_COUNT", 1))


def _small_success_continue_prefill_target_count() -> int:
    return max(0, _env_int("REGISTER_SMALL_SUCCESS_CONTINUE_PREFILL_TARGET_COUNT", 2))


def _small_success_continue_prefill_min_age_seconds() -> float:
    return max(0.0, _env_float("REGISTER_SMALL_SUCCESS_CONTINUE_PREFILL_MIN_AGE_SECONDS", 0.0))

def _free_stop_after_validate_mode() -> bool:
    return _env_bool("REGISTER_FREE_STOP_AFTER_VALIDATE", False)

def _iter_small_success_artifacts(*, run_output_dir: Path) -> list[Path]:
    small_success_dir = run_output_dir / "small_success"
    if not small_success_dir.is_dir():
        return []
    return sorted(
        [
            path
            for path in small_success_dir.glob("*.json")
            if path.is_file()
        ],
        key=lambda item: item.name.lower(),
    )


def _copy_small_success_artifacts_to_pool(
    *,
    run_output_dir: Path,
    pool_dir: Path,
    worker_label: str,
    task_index: int,
) -> list[str]:
    source_paths = _iter_small_success_artifacts(run_output_dir=run_output_dir)
    if not source_paths:
        return []
    _ensure_directory(pool_dir)
    copied_paths: list[str] = []
    discarded_paths: list[dict[str, str]] = []
    for source_path in source_paths:
        try:
            payload = load_json_payload(source_path)
        except Exception as exc:
            discarded_paths.append({"source_path": str(source_path), "reason": f"load_failed:{exc}"})
            continue
        valid, reason = _validate_small_success_seed_payload(payload)
        if not valid:
            discarded_paths.append({"source_path": str(source_path), "reason": reason})
            continue
        destination = pool_dir / source_path.name
        if destination.exists():
            destination = pool_dir / f"{source_path.stem}-{uuid.uuid4().hex[:6]}{source_path.suffix}"
        shutil.copy2(source_path, destination)
        copied_paths.append(str(destination))
    _json_log(
        {
            "event": "register_small_success_collected",
            "workerId": worker_label,
            "taskIndex": task_index,
            "outputDir": str(run_output_dir),
            "poolDir": str(pool_dir),
            "count": len(copied_paths),
            "artifacts": copied_paths,
            "discardedCount": len(discarded_paths),
            "discarded": discarded_paths,
        }
    )
    return copied_paths


def _small_success_failure_target_pool_dir(*, output_root: Path, result_payload: dict[str, Any]) -> Path:
    error_code = str(result_payload.get("errorCode") or "").strip()
    if not error_code:
        error_code = str(((result_payload.get("stepErrors") or {}).get(str(result_payload.get("errorStep") or "")) or {}).get("code") or "").strip()
    if _free_manual_oauth_preserve_enabled() and error_code in _free_manual_oauth_preserve_codes():
        return _resolve_free_manual_oauth_pool_dir(output_root=output_root)
    if error_code == "free_personal_workspace_missing":
        return _resolve_small_success_wait_pool_dir(output_root=output_root)
    return _resolve_small_success_pool_dir(output_root=output_root)


def _drain_small_success_wait_pool(
    *,
    wait_pool_dir: Path,
    continue_pool_dir: Path,
    min_age_seconds: float,
) -> dict[str, Any]:
    _ensure_directory(wait_pool_dir)
    _ensure_directory(continue_pool_dir)
    moved: list[dict[str, Any]] = []
    now = time.time()

    for source_path in sorted(wait_pool_dir.glob("*.json"), key=lambda item: item.name.lower()):
        if not source_path.is_file():
            continue
        try:
            age_seconds = max(0.0, now - source_path.stat().st_mtime)
        except FileNotFoundError:
            continue
        if age_seconds < min_age_seconds:
            continue
        destination = continue_pool_dir / source_path.name
        if destination.exists():
            destination = continue_pool_dir / f"{source_path.stem}-{uuid.uuid4().hex[:6]}{source_path.suffix}"
        try:
            source_path.replace(destination)
        except FileNotFoundError:
            continue
        moved.append(
            {
                "source_path": str(source_path),
                "destination_path": str(destination),
                "age_seconds": round(age_seconds, 3),
            }
        )

    return {
        "ok": True,
        "status": "moved" if moved else "idle",
        "count": len(moved),
        "wait_pool_dir": str(wait_pool_dir),
        "continue_pool_dir": str(continue_pool_dir),
        "artifacts": moved,
    }


def _backfill_small_success_continue_pool(
    *,
    source_pool_dir: Path,
    continue_pool_dir: Path,
    max_move_count: int,
    target_count: int,
    min_age_seconds: float,
) -> dict[str, Any]:
    _ensure_directory(source_pool_dir)
    _ensure_directory(continue_pool_dir)
    normalized_source = str(source_pool_dir.resolve()).lower()
    normalized_continue = str(continue_pool_dir.resolve()).lower()
    if normalized_source == normalized_continue:
        return {
            "ok": True,
            "status": "skipped_same_pool",
            "count": 0,
            "source_pool_dir": str(source_pool_dir),
            "continue_pool_dir": str(continue_pool_dir),
            "artifacts": [],
            "discarded": [],
        }

    if max_move_count <= 0:
        return {
            "ok": True,
            "status": "disabled",
            "count": 0,
            "source_pool_dir": str(source_pool_dir),
            "continue_pool_dir": str(continue_pool_dir),
            "artifacts": [],
            "discarded": [],
        }

    current_continue_count = len(list(continue_pool_dir.glob("*.json")))
    move_budget = max_move_count
    if target_count > 0:
        move_budget = min(move_budget, max(0, target_count - current_continue_count))
    if move_budget <= 0:
        return {
            "ok": True,
            "status": "target_satisfied",
            "count": 0,
            "current_continue_count": current_continue_count,
            "target_count": target_count,
            "source_pool_dir": str(source_pool_dir),
            "continue_pool_dir": str(continue_pool_dir),
            "artifacts": [],
            "discarded": [],
        }

    moved: list[dict[str, Any]] = []
    discarded: list[dict[str, Any]] = []
    now = time.time()
    candidates = _sort_file_paths_newest_first([path for path in source_pool_dir.glob("*.json") if path.is_file()])
    for source_path in candidates:
        if len(moved) >= move_budget:
            break
        try:
            age_seconds = max(0.0, now - source_path.stat().st_mtime)
        except FileNotFoundError:
            continue
        if min_age_seconds > 0 and age_seconds < min_age_seconds:
            continue
        try:
            payload = load_json_payload(source_path)
        except Exception as exc:
            source_path.unlink(missing_ok=True)
            discarded.append({"source_path": str(source_path), "reason": f"load_failed:{exc}"})
            continue
        valid, reason = _validate_small_success_seed_payload(payload)
        if not valid:
            source_path.unlink(missing_ok=True)
            discarded.append({"source_path": str(source_path), "reason": reason})
            continue
        destination = continue_pool_dir / source_path.name
        if destination.exists():
            destination = continue_pool_dir / f"{source_path.stem}-{uuid.uuid4().hex[:6]}{source_path.suffix}"
        try:
            source_path.replace(destination)
        except FileNotFoundError:
            continue
        moved.append(
            {
                "source_path": str(source_path),
                "destination_path": str(destination),
                "email": str(payload.get("email") or "").strip(),
                "age_seconds": round(age_seconds, 3),
            }
        )

    return {
        "ok": True,
        "status": "moved" if moved else "idle",
        "count": len(moved),
        "current_continue_count": current_continue_count,
        "target_count": target_count,
        "source_pool_dir": str(source_pool_dir),
        "continue_pool_dir": str(continue_pool_dir),
        "artifacts": moved,
        "discarded": discarded,
    }


def _iter_free_oauth_artifacts(*, result: Any) -> list[Path]:
    payload = _result_payload(result)
    outputs = payload.get("outputs") if isinstance(payload, dict) else {}
    if not isinstance(outputs, dict):
        return []
    oauth_output = outputs.get("obtain-codex-oauth")
    if not isinstance(oauth_output, dict):
        return []
    success_path_text = str(oauth_output.get("successPath") or "").strip()
    if not success_path_text:
        return []
    success_path = Path(success_path_text).resolve()
    if not success_path.is_file():
        return []
    return [success_path]


def _free_oauth_account_id(*, result_payload: dict[str, Any]) -> str:
    outputs = result_payload.get("outputs") if isinstance(result_payload, dict) else {}
    oauth_output = outputs.get("obtain-codex-oauth") if isinstance(outputs, dict) else {}
    if not isinstance(oauth_output, dict):
        return ""
    direct = str(
        oauth_output.get("accountId")
        or oauth_output.get("account_id")
        or oauth_output.get("chatgpt_account_id")
        or ""
    ).strip()
    if direct:
        return direct
    auth_payload = oauth_output.get("auth")
    if isinstance(auth_payload, dict):
        nested = str(
            auth_payload.get("account_id")
            or auth_payload.get("chatgpt_account_id")
            or ((auth_payload.get("https://api.openai.com/auth") or {}).get("chatgpt_account_id"))
            or ""
        ).strip()
        if nested:
            return nested
    return ""


def _free_invite_team_account_id(*, result_payload: dict[str, Any]) -> str:
    outputs = result_payload.get("outputs") if isinstance(result_payload, dict) else {}
    invite_output = outputs.get("invite-codex-member") if isinstance(outputs, dict) else {}
    if not isinstance(invite_output, dict):
        return ""
    return str(invite_output.get("team_account_id") or "").strip()


def _free_personal_oauth_confirmed(*, result_payload: dict[str, Any]) -> bool:
    steps = result_payload.get("steps") if isinstance(result_payload, dict) else {}
    if isinstance(steps, dict) and str(steps.get("validate-free-personal-oauth") or "").strip().lower() == "ok":
        return True
    oauth_account_id = _free_oauth_account_id(result_payload=result_payload)
    team_account_id = _free_invite_team_account_id(result_payload=result_payload)
    return bool(oauth_account_id and team_account_id and oauth_account_id != team_account_id)


def _copy_free_oauth_artifacts_to_pool(
    *,
    result: Any,
    pool_dir: Path,
    worker_label: str,
    task_index: int,
    run_output_dir: Path,
) -> list[str]:
    source_paths = _iter_free_oauth_artifacts(result=result)
    if not source_paths:
        return []
    _ensure_directory(pool_dir)
    copied_paths: list[str] = []
    for source_path in source_paths:
        destination = pool_dir / source_path.name
        if destination.exists():
            destination = pool_dir / f"{source_path.stem}-{uuid.uuid4().hex[:6]}{source_path.suffix}"
        shutil.copy2(source_path, destination)
        copied_paths.append(str(destination))
    _json_log(
        {
            "event": "register_free_oauth_collected",
            "workerId": worker_label,
            "taskIndex": task_index,
            "outputDir": str(run_output_dir),
            "poolDir": str(pool_dir),
            "count": len(copied_paths),
            "artifacts": copied_paths,
        }
    )
    return copied_paths


def _build_unique_destination(*, destination_dir: Path, preferred_name: str) -> Path:
    destination = destination_dir / preferred_name
    if destination.exists():
        destination = destination_dir / f"{destination.stem}-{uuid.uuid4().hex[:6]}{destination.suffix}"
    return destination


def _copy_artifact_to_dir(
    *,
    source_path: Path,
    destination_dir: Path,
    preferred_name: str | None = None,
    overwrite_existing: bool = False,
) -> str:
    _ensure_directory(destination_dir)
    target_name = str(preferred_name or "").strip() or source_path.name
    if overwrite_existing:
        destination = destination_dir / target_name
        destination.unlink(missing_ok=True)
    else:
        destination = _build_unique_destination(destination_dir=destination_dir, preferred_name=target_name)
    shutil.copy2(source_path, destination)
    return str(destination)


def _move_artifact_to_dir(
    *,
    source_path: Path,
    destination_dir: Path,
    preferred_name: str | None = None,
    overwrite_existing: bool = False,
) -> str:
    _ensure_directory(destination_dir)
    target_name = str(preferred_name or "").strip() or source_path.name
    if overwrite_existing:
        destination = destination_dir / target_name
        destination.unlink(missing_ok=True)
    else:
        destination = _build_unique_destination(destination_dir=destination_dir, preferred_name=target_name)
    try:
        source_path.replace(destination)
    except OSError as exc:
        if exc.errno != errno.EXDEV and "Invalid cross-device link" not in str(exc):
            raise
        shutil.copy2(source_path, destination)
        source_path.unlink(missing_ok=True)
    return str(destination)


def _upload_artifact_to_r2(*, source_path: Path, target_folder: str, object_name: str | None = None) -> dict[str, Any]:
    step_input = {
        "source_path": str(source_path),
        "bucket": _env_text("REGISTER_R2_BUCKET") or _env_text("R2_BUCKET"),
        "target_folder": str(target_folder or "").strip(),
        "object_name": str(object_name or "").strip() or source_path.name,
        "account_id": _env_text("REGISTER_R2_ACCOUNT_ID") or _env_text("R2_ACCOUNT_ID"),
        "endpoint_url": _env_text("REGISTER_R2_ENDPOINT_URL") or _env_text("R2_ENDPOINT_URL"),
        "access_key_id": _env_text("REGISTER_R2_ACCESS_KEY_ID") or _env_text("R2_ACCESS_KEY_ID"),
        "secret_access_key": _env_text("REGISTER_R2_SECRET_ACCESS_KEY") or _env_text("R2_SECRET_ACCESS_KEY"),
        "region": _env_text("REGISTER_R2_REGION") or _env_text("R2_REGION"),
        "public_base_url": _env_text("REGISTER_R2_PUBLIC_BASE_URL") or _env_text("R2_PUBLIC_BASE_URL"),
        "overwrite": True,
    }
    return dispatch_easyprotocol_step(step_type="upload_file_to_r2", step_input=step_input)


def _load_artifact_json_quiet(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}
def _free_success_artifact_path(*, result: Any) -> Path | None:
    source_paths = _iter_free_oauth_artifacts(result=result)
    if not source_paths:
        return None
    return source_paths[0]


def _team_success_artifacts(*, result: Any) -> list[dict[str, Any]]:
    payload = _result_payload(result)
    outputs = payload.get("outputs") if isinstance(payload, dict) else {}
    if not isinstance(outputs, dict):
        return []
    collected = outputs.get("collect-team-pool-artifacts")
    if not isinstance(collected, dict):
        return []
    artifacts = collected.get("artifacts")
    if not isinstance(artifacts, list):
        return []
    normalized: list[dict[str, Any]] = []
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            continue
        path_text = str(artifact.get("team_pool_path") or "").strip()
        if not path_text:
            continue
        normalized.append(
            {
                "kind": str(artifact.get("kind") or "").strip(),
                "email": str(artifact.get("email") or "").strip(),
                "preferred_name": str(artifact.get("preferred_name") or "").strip(),
                "path": str(Path(path_text).resolve()),
            }
        )
    return normalized


def _team_has_collectable_artifacts(*, result: Any) -> bool:
    return len(_team_success_artifacts(result=result)) > 0


def _free_small_success_seed_path(*, result_payload: dict[str, Any]) -> Path | None:
    outputs = result_payload.get("outputs") if isinstance(result_payload, dict) else {}
    if not isinstance(outputs, dict):
        return None
    for output_key, field_name in (
        ("create-openai-account", "storage_path"),
        ("create_openai_account", "storage_path"),
        ("acquire-small-success-artifact", "source_path"),
        ("acquire-small-success-artifact", "claimed_path"),
    ):
        payload = outputs.get(output_key)
        if not isinstance(payload, dict):
            continue
        candidate = str(payload.get(field_name) or "").strip()
        if not candidate:
            continue
        path = Path(candidate).resolve()
        if path.is_file():
            return path
    return None


def _postprocess_free_success_artifact(
    *,
    result: Any,
    output_root: Path,
    worker_label: str,
    task_index: int,
    free_local_selected: bool,
) -> dict[str, Any]:
    result_payload = _result_payload(result)
    if not _free_personal_oauth_confirmed(result_payload=result_payload):
        seed_path = _free_small_success_seed_path(result_payload=result_payload)
        if seed_path is None or not seed_path.is_file():
            return {
                "ok": False,
                "status": "free_personal_workspace_missing_seed_unavailable",
                "cleanup_run_output": False,
            }
        if _free_manual_oauth_preserve_enabled():
            handoff_dir = _resolve_free_manual_oauth_pool_dir(output_root=output_root)
            stored_path = _copy_artifact_to_dir(source_path=seed_path, destination_dir=handoff_dir)
            seed_payload = load_json_payload(seed_path)
            return {
                "ok": True,
                "status": "free_personal_workspace_missing_preserved_for_manual_oauth",
                "cleanup_run_output": True,
                "stored_path": stored_path,
                "target_dir": str(handoff_dir),
                "email": str(seed_payload.get("email") or "").strip(),
            }
        wait_pool_dir = _resolve_small_success_wait_pool_dir(output_root=output_root)
        stored_path = _copy_artifact_to_dir(source_path=seed_path, destination_dir=wait_pool_dir)
        return {
            "ok": True,
            "status": "free_personal_workspace_missing_routed_to_wait_pool",
            "cleanup_run_output": True,
            "stored_path": stored_path,
            "target_dir": str(wait_pool_dir),
        }

    artifact_path = _free_success_artifact_path(result=result)
    if artifact_path is None or not artifact_path.is_file():
        return {"ok": False, "status": "missing_free_artifact", "cleanup_run_output": False}
    artifact_payload = _standardize_export_credential_payload(_load_artifact_json_quiet(artifact_path))
    preferred_name = _canonical_free_artifact_name_from_payload(artifact_payload)

    if free_local_selected:
        target_dir = _resolve_free_local_dir(output_root=output_root)
        stored_path = _copy_artifact_to_dir(
            source_path=artifact_path,
            destination_dir=target_dir,
            preferred_name=preferred_name,
            overwrite_existing=True,
        )
        _write_json_atomic(
            Path(stored_path),
            artifact_payload,
            include_pid=True,
            cleanup_temp=True,
        )
        return {
            "ok": True,
            "status": "stored_local",
            "cleanup_run_output": True,
            "stored_path": stored_path,
            "target_dir": str(target_dir),
        }

    if not _should_cleanup_successful_run_output(result):
        return {"ok": False, "status": "free_upload_not_confirmed", "cleanup_run_output": False}

    pool_dir = _resolve_free_oauth_pool_dir(output_root=output_root)
    pooled_path = _copy_artifact_to_dir(
        source_path=artifact_path,
        destination_dir=pool_dir,
        preferred_name=preferred_name,
        overwrite_existing=True,
    )
    _write_json_atomic(
        Path(pooled_path),
        artifact_payload,
        include_pid=True,
        cleanup_temp=True,
    )
    upload_result = _upload_artifact_to_r2(
        source_path=Path(pooled_path),
        target_folder="codex",
        object_name=preferred_name,
    )
    if not bool(upload_result.get("ok")):
        return {
            "ok": False,
            "status": "free_upload_failed",
            "cleanup_run_output": False,
            "detail": str(upload_result.get("detail") or upload_result.get("status") or "upload_failed"),
            "transient_path": pooled_path,
        }

    try:
        Path(pooled_path).unlink(missing_ok=True)
    except Exception:
        pass
    return {
        "ok": True,
        "status": "uploaded_deleted",
        "cleanup_run_output": True,
        "pool_dir": str(pool_dir),
        "transient_path": pooled_path,
    }


def _postprocess_team_success_artifacts(
    *,
    result: Any,
    output_root: Path,
) -> dict[str, Any]:
    artifacts = _team_success_artifacts(result=result)
    if not artifacts:
        return {"ok": True, "status": "idle", "cleanup_run_output": True, "artifacts": []}

    local_percent = _env_percent("REGISTER_TEAM_LOCAL_SPLIT_PERCENT", 0.0)
    local_dir = _resolve_team_local_dir(output_root=output_root)
    processed: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    for artifact in artifacts:
        source_path = Path(str(artifact.get("path") or "")).resolve()
        if not source_path.is_file():
            failures.append({"path": str(source_path), "status": "missing"})
            continue
        artifact_payload = _standardize_export_credential_payload(_load_artifact_json_quiet(source_path))
        is_mother = str(artifact.get("kind") or "").strip().lower() == "mother"
        preferred_name = _canonical_team_artifact_name_from_payload(artifact_payload, is_mother=is_mother)
        overwrite_existing = True
        route_local = _select_local_split(percent=local_percent)
        if route_local:
            stored_path = _move_artifact_to_dir(
                source_path=source_path,
                destination_dir=local_dir,
                preferred_name=preferred_name,
                overwrite_existing=overwrite_existing,
            )
            _write_json_atomic(
                Path(stored_path),
                artifact_payload,
                include_pid=True,
                cleanup_temp=True,
            )
            processed.append(
                {
                    **artifact,
                    "route": "local",
                    "stored_path": stored_path,
                }
            )
            continue
        _write_json_atomic(
            source_path,
            artifact_payload,
            include_pid=True,
            cleanup_temp=True,
        )
        try:
            upload_result = _upload_artifact_to_r2(
                source_path=source_path,
                target_folder="codex-team",
                object_name=preferred_name,
            )
            upload_ok = bool(upload_result.get("ok"))
        except Exception as exc:
            upload_result = {"ok": False, "detail": str(exc)}
            upload_ok = False
        if upload_ok:
            source_path.unlink(missing_ok=True)
            processed.append(
                {
                    **artifact,
                    "route": "uploaded",
                    "object_key": str(upload_result.get("object_key") or ""),
                }
            )
            continue
        failures.append(
            {
                **artifact,
                "route": "upload_failed",
                "detail": str(upload_result.get("detail") or upload_result.get("status") or "upload_failed"),
            }
        )

    return {
        "ok": not failures,
        "status": "processed" if processed and not failures else "partial_failure" if processed else "failed",
        "cleanup_run_output": True,
        "artifacts": processed,
        "failures": failures,
        "local_dir": str(local_dir),
    }


def _sync_team_member_artifacts_from_active_claims(
    *,
    output_root: Path,
) -> dict[str, Any]:
    local_percent = _env_percent("REGISTER_TEAM_LOCAL_SPLIT_PERCENT", 0.0)
    if local_percent < 100.0:
        return {"ok": True, "status": "disabled", "localized": [], "failures": []}

    claims_dir = _shared_root_from_output_root(output_root) / "others" / "team-mother-claims"
    if not claims_dir.is_dir():
        return {"ok": True, "status": "idle", "localized": [], "failures": []}

    local_dir = _resolve_team_local_dir(output_root=output_root)
    localized: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    for claim_path in sorted(claims_dir.glob("*.json"), key=lambda item: item.name.lower()):
        if not claim_path.is_file():
            continue
        claim_payload = load_json_payload(claim_path)
        if not isinstance(claim_payload, dict):
            continue
        team_flow = claim_payload.get("teamFlow")
        if not isinstance(team_flow, dict):
            continue
        progress = team_flow.get("teamExpandProgress")
        if not isinstance(progress, dict):
            continue
        successful_artifacts = progress.get("successfulArtifacts")
        if not isinstance(successful_artifacts, list):
            continue
        for artifact in successful_artifacts:
            if not isinstance(artifact, dict):
                continue
            success_path_text = str(artifact.get("successPath") or "").strip()
            if not success_path_text:
                continue
            success_path = Path(success_path_text).resolve()
            if not success_path.is_file():
                continue
            try:
                artifact_payload = _standardize_export_credential_payload(_load_artifact_json_quiet(success_path))
                preferred_name = _canonical_team_artifact_name_from_payload(artifact_payload, is_mother=False)
                stored_path = _copy_artifact_to_dir(
                    source_path=success_path,
                    destination_dir=local_dir,
                    preferred_name=preferred_name,
                    overwrite_existing=True,
                )
                _write_json_atomic(
                    Path(stored_path),
                    artifact_payload,
                    include_pid=True,
                    cleanup_temp=True,
                )
                localized.append(
                    {
                        "claim_path": str(claim_path),
                        "source_path": str(success_path),
                        "stored_path": str(stored_path),
                        "email": str(artifact.get("email") or "").strip(),
                    }
                )
            except Exception as exc:
                failures.append(
                    {
                        "claim_path": str(claim_path),
                        "source_path": str(success_path),
                        "email": str(artifact.get("email") or "").strip(),
                        "detail": str(exc),
                    }
                )

    return {
        "ok": not failures,
        "status": (
            "processed"
            if localized and not failures
            else "partial_failure"
            if localized
            else "failed"
            if failures
            else "idle"
        ),
        "localized": localized,
        "failures": failures,
        "local_dir": str(local_dir),
    }


def _team_live_local_sync_loop(
    *,
    stop_event: Any,
    output_root: Path,
    worker_label: str,
) -> None:
    while not stop_event.is_set():
        try:
            sync_result = _sync_team_member_artifacts_from_active_claims(output_root=output_root)
            if sync_result.get("localized") or sync_result.get("failures"):
                _json_log(
                    {
                        "event": "register_team_live_local_sync",
                        "workerId": worker_label,
                        "result": sync_result,
                    }
                )
        except Exception as exc:
            _json_log(
                {
                    "event": "register_team_live_local_sync_failed",
                    "workerId": worker_label,
                    "detail": str(exc),
                }
            )
        try:
            if stop_event.wait(2.0):
                break
        except Exception:
            time.sleep(2.0)


def _drain_oauth_pool_backlog(
    *,
    pool_dir: Path,
    target_folder: str,
    local_percent: float = 100.0,
    local_dir: Path | None = None,
) -> dict[str, Any]:
    if not pool_dir.is_dir():
        return {"ok": True, "status": "idle", "uploaded": [], "failures": []}

    uploaded: list[dict[str, Any]] = []
    localized: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for source_path in sorted(pool_dir.glob("*.json"), key=lambda item: item.name.lower()):
        if not source_path.is_file():
            continue
        artifact_payload = _standardize_export_credential_payload(_load_artifact_json_quiet(source_path))
        is_team = str(target_folder or "").strip().lower() == "codex-team"
        is_mother = source_path.name.lower().startswith("mother-") or source_path.name.lower().startswith("codex-team-mother-")
        preferred_name = (
            _canonical_team_artifact_name_from_payload(artifact_payload, is_mother=is_mother)
            if is_team
            else _canonical_free_artifact_name_from_payload(artifact_payload)
        )
        overwrite_existing = True
        _write_json_atomic(
            source_path,
            artifact_payload,
            include_pid=True,
            cleanup_temp=True,
        )
        if local_dir is not None and _select_local_split(percent=local_percent):
            try:
                stored_path = _move_artifact_to_dir(
                    source_path=source_path,
                    destination_dir=local_dir,
                    preferred_name=preferred_name,
                    overwrite_existing=overwrite_existing,
                )
                _write_json_atomic(
                    Path(stored_path),
                    artifact_payload,
                    include_pid=True,
                    cleanup_temp=True,
                )
                localized.append(
                    {
                        "path": str(source_path),
                        "stored_path": stored_path,
                    }
                )
                continue
            except Exception as exc:
                failures.append(
                    {
                        "path": str(source_path),
                        "detail": str(exc),
                    }
                )
                continue
        try:
            upload_result = _upload_artifact_to_r2(
                source_path=source_path,
                target_folder=target_folder,
                object_name=preferred_name,
            )
            upload_ok = bool(upload_result.get("ok"))
        except Exception as exc:
            upload_result = {"ok": False, "detail": str(exc)}
            upload_ok = False
        if upload_ok:
            source_path.unlink(missing_ok=True)
            uploaded.append(
                {
                    "path": str(source_path),
                    "object_key": str(upload_result.get("object_key") or ""),
                }
            )
            continue
        failures.append(
            {
                "path": str(source_path),
                "detail": str(upload_result.get("detail") or upload_result.get("status") or "upload_failed"),
            }
        )
    return {
        "ok": not failures,
        "status": (
            "processed"
            if (uploaded or localized) and not failures
            else "partial_failure"
            if (uploaded or localized)
            else "failed"
            if failures
            else "idle"
        ),
        "uploaded": uploaded,
        "localized": localized,
        "failures": failures,
    }


def _load_json_dict(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _payload_looks_like_oauth_credential(payload: dict[str, Any]) -> bool:
    if not isinstance(payload, dict):
        return False
    if any(str(payload.get(key) or "").strip() for key in ("access_token", "refresh_token", "id_token")):
        return True
    auth = payload.get("auth")
    if isinstance(auth, dict) and any(str(auth.get(key) or "").strip() for key in ("access_token", "refresh_token", "id_token")):
        return True
    return False


def _resolve_restored_path_for_source(*, result_payload: dict[str, Any], source_path: Path) -> Path | None:
    outputs = result_payload.get("outputs") if isinstance(result_payload, dict) else {}
    if not isinstance(outputs, dict):
        return None

    finalize_small = outputs.get("finalize-small-success-artifact")
    if isinstance(finalize_small, dict):
        claimed_path = str(finalize_small.get("claimed_path") or "").strip()
        restored_path = str(finalize_small.get("restored_path") or "").strip()
        if claimed_path and restored_path and str(source_path).lower() == str(Path(claimed_path).resolve()).lower():
            candidate = Path(restored_path).resolve()
            if candidate.exists():
                return candidate

    finalize_team = outputs.get("finalize-team-batch")
    if isinstance(finalize_team, dict):
        restored = finalize_team.get("restored")
        if isinstance(restored, list):
            for item in restored:
                if not isinstance(item, dict):
                    continue
                claimed_path = str(item.get("claimed_path") or "").strip()
                restored_path = str(item.get("restored_path") or "").strip()
                if not claimed_path or not restored_path:
                    continue
                if str(source_path).lower() == str(Path(claimed_path).resolve()).lower():
                    candidate = Path(restored_path).resolve()
                    if candidate.exists():
                        return candidate
    return None

def _team_mother_identity_from_result_payload(result_payload: dict[str, Any]) -> dict[str, str]:
    outputs = result_payload.get("outputs") if isinstance(result_payload, dict) else {}
    mother_artifact = outputs.get("acquire-team-mother-artifact") if isinstance(outputs, dict) else {}
    if not isinstance(mother_artifact, dict):
        return {
            "original_name": "",
            "email": "",
            "account_id": "",
        }
    return {
        "original_name": str(mother_artifact.get("original_name") or "").strip(),
        "email": str(mother_artifact.get("email") or "").strip(),
        "account_id": str(mother_artifact.get("account_id") or "").strip(),
    }


def _team_auth_path_from_result_payload(
    *,
    result_payload: dict[str, Any],
    fallback_path: str,
) -> str:
    outputs = result_payload.get("outputs") if isinstance(result_payload, dict) else {}
    if not isinstance(outputs, dict):
        return str(fallback_path or "").strip()
    for candidate in (
        ((outputs.get("obtain-team-mother-oauth") or {}).get("successPath") if isinstance(outputs.get("obtain-team-mother-oauth"), dict) else ""),
        ((outputs.get("acquire-team-mother-artifact") or {}).get("source_path") if isinstance(outputs.get("acquire-team-mother-artifact"), dict) else ""),
        ((outputs.get("acquire-team-mother-artifact") or {}).get("claimed_path") if isinstance(outputs.get("acquire-team-mother-artifact"), dict) else ""),
    ):
        normalized = str(candidate or "").strip()
        if normalized:
            return normalized
    return str(fallback_path or "").strip()


def _team_mother_failure_cooldown_seconds(*, result: Any) -> float:
    payload = _result_payload(result)
    error_step = str(payload.get("errorStep") or "").strip().lower()
    error_text = str(payload.get("error") or "").strip().lower()
    step_errors = payload.get("stepErrors") or {}

    if error_step == "obtain-team-mother-oauth":
        oauth_error = step_errors.get("obtain-team-mother-oauth") or {}
        combined = " ".join(
            part
            for part in (
                error_text,
                str(oauth_error.get("message") or "").strip().lower(),
                str(oauth_error.get("code") or "").strip().lower(),
            )
            if part
        )
        if any(
            marker in combined
            for marker in (
                "refresh_token_reused",
                "team_mother_token_validation_failed",
                "invalid_request_error",
                "authorize_continue_rate_limited",
                "rate limit exceeded",
                "status=429",
                "timeout waiting for 6-digit code",
                "mailbox_unavailable",
            )
        ):
            return float(max(0, _env_float("REGISTER_TEAM_OAUTH_FAILURE_COOLDOWN_SECONDS", 300.0)))

    if error_step == "invite-team-members":
        invite_error = step_errors.get("invite-team-members") or {}
        combined = " ".join(
            part
            for part in (
                error_text,
                str(invite_error.get("message") or "").strip().lower(),
                str(invite_error.get("code") or "").strip().lower(),
            )
            if part
        )
        if any(
            marker in combined
            for marker in (
                "workspace has reached maximum number of seats",
                "team_seats_full",
                "unable to invite user due to an error",
            )
        ):
            return float(max(0, _env_float("REGISTER_TEAM_INVITE_FAILURE_COOLDOWN_SECONDS", 300.0)))

    return 0.0


def _mark_team_mother_failure_cooldown(
    *,
    shared_root: Path,
    result_payload: dict[str, Any],
    cooldown_seconds: float,
    reason: str,
    worker_label: str,
    task_index: int,
) -> dict[str, Any] | None:
    if float(cooldown_seconds or 0.0) <= 0:
        return None
    identity = _team_mother_identity_from_result_payload(result_payload)
    original_name = str(identity.get("original_name") or "").strip()
    email = str(identity.get("email") or "").strip()
    account_id = str(identity.get("account_id") or "").strip()
    if not original_name and not email and not account_id:
        return None

    cooldown_dir = resolve_team_mother_cooldowns_dir(str(shared_root))
    _ensure_directory(cooldown_dir)
    state_path = cooldown_dir / f"{_team_mother_cooldown_key(original_name=original_name, email=email, account_id=account_id)}.json"
    now = datetime.now(timezone.utc)
    cooldown_until = now + timedelta(seconds=max(0.0, float(cooldown_seconds or 0.0)))
    payload = {
        "original_name": original_name,
        "email": email,
        "account_id": account_id,
        "reason": str(reason or "").strip(),
        "cooldown_seconds": float(max(0.0, float(cooldown_seconds or 0.0))),
        "cooldown_started_at": now.isoformat(),
        "cooldown_until": cooldown_until.isoformat(),
        "cooldown_until_ts": cooldown_until.timestamp(),
    }
    _write_json_atomic(
        state_path,
        payload,
        include_pid=True,
        cleanup_temp=True,
    )
    _json_log(
        {
            "event": "register_team_mother_cooldown_marked",
            "workerId": worker_label,
            "taskIndex": task_index,
            "statePath": str(state_path),
            **payload,
        }
    )
    return {
        "state_path": str(state_path),
        **payload,
    }


def _merge_refreshed_credential(*, original_payload: dict[str, Any], refreshed_payload: dict[str, Any]) -> dict[str, Any]:
    merged = dict(original_payload or {})
    merged.update(refreshed_payload or {})
    if isinstance(original_payload.get("auth"), dict) or isinstance(refreshed_payload.get("auth"), dict):
        merged["auth"] = {
            **(dict(original_payload.get("auth") or {}) if isinstance(original_payload, dict) else {}),
            **(dict(refreshed_payload.get("auth") or {}) if isinstance(refreshed_payload, dict) else {}),
        }
    return _standardize_export_credential_payload(merged)


def _collect_credential_backwrite_actions(*, result_payload: dict[str, Any]) -> list[dict[str, Any]]:
    outputs = result_payload.get("outputs") if isinstance(result_payload, dict) else {}
    if not isinstance(outputs, dict):
        return []

    actions: list[dict[str, Any]] = []

    mother_source = str(((outputs.get("acquire-team-mother-artifact") or {}).get("source_path")) or "").strip()
    mother_success = str(((outputs.get("obtain-team-mother-oauth") or {}).get("successPath")) or "").strip()
    if mother_source and mother_success:
        actions.append(
            {
                "kind": "team_mother",
                "source_path": mother_source,
                "refreshed_path": mother_success,
                "force": True,
            }
        )

    codex_success = str(((outputs.get("obtain-codex-oauth") or {}).get("successPath")) or "").strip()
    if codex_success:
        for source_key, field_name in (
            ("acquire-small-success-artifact", "source_path"),
            ("acquire-small-success-artifact", "claimed_path"),
            ("acquire_small_success_artifact", "source_path"),
            ("small_success_artifact", "source_path"),
            ("create-openai-account", "storage_path"),
            ("create_openai_account", "storage_path"),
        ):
            candidate = outputs.get(source_key)
            if not isinstance(candidate, dict):
                continue
            source_path = str(candidate.get(field_name) or "").strip()
            if not source_path:
                continue
            actions.append(
                {
                    "kind": "generic_oauth_refresh",
                    "source_path": source_path,
                    "refreshed_path": codex_success,
                    "force": False,
                }
            )

    members = ((outputs.get("acquire-team-member-candidates") or {}).get("members")) or []
    oauth_batch = ((outputs.get("obtain-team-member-oauth-batch") or {}).get("artifacts")) or []
    if isinstance(members, list) and isinstance(oauth_batch, list):
        for index, item in enumerate(oauth_batch):
            if not isinstance(item, dict) or index >= len(members) or not isinstance(members[index], dict):
                continue
            refreshed_path = str(item.get("successPath") or "").strip()
            source_path = str(members[index].get("source_path") or members[index].get("claimed_path") or "").strip()
            if not refreshed_path or not source_path:
                continue
            actions.append(
                {
                    "kind": "team_member_oauth_refresh",
                    "source_path": source_path,
                    "refreshed_path": refreshed_path,
                    "force": False,
                }
            )

    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in actions:
        key = (str(item.get("source_path") or "").strip().lower(), str(item.get("refreshed_path") or "").strip().lower())
        if not key[0] or not key[1] or key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _sync_refreshed_credentials_back_to_sources(*, result_payload: dict[str, Any], worker_label: str, task_index: int) -> list[dict[str, Any]]:
    actions = _collect_credential_backwrite_actions(result_payload=result_payload)
    if not actions:
        return []

    synced: list[dict[str, Any]] = []
    for action in actions:
        refreshed_path = Path(str(action.get("refreshed_path") or "")).resolve()
        if not refreshed_path.is_file():
            continue
        source_path = Path(str(action.get("source_path") or "")).resolve()
        live_source_path = source_path if source_path.exists() else _resolve_restored_path_for_source(
            result_payload=result_payload,
            source_path=source_path,
        )
        if live_source_path is None or not live_source_path.exists():
            continue

        original_payload = _load_json_dict(live_source_path)
        refreshed_payload = _load_json_dict(refreshed_path)
        if not refreshed_payload:
            continue
        if not bool(action.get("force")) and not _payload_looks_like_oauth_credential(original_payload):
            continue

        merged_payload = _merge_refreshed_credential(
            original_payload=original_payload,
            refreshed_payload=refreshed_payload,
        )
        _write_json_atomic(
            live_source_path,
            merged_payload,
            include_pid=True,
            cleanup_temp=True,
        )
        synced.append(
            {
                "kind": str(action.get("kind") or "").strip(),
                "source_path": str(live_source_path),
                "refreshed_path": str(refreshed_path),
            }
        )

    if synced:
        _json_log(
            {
                "event": "register_credential_source_synced",
                "workerId": worker_label,
                "taskIndex": task_index,
                "count": len(synced),
                "artifacts": synced,
            }
        )
    return synced


def _cleanup_run_output_dir(*, run_output_dir: Path, worker_label: str, task_index: int) -> None:
    if not run_output_dir.exists():
        return
    shutil.rmtree(run_output_dir, ignore_errors=False)
    _json_log(
        {
            "event": "register_run_output_deleted",
            "workerId": worker_label,
            "taskIndex": task_index,
            "outputDir": str(run_output_dir),
        }
    )


def _result_payload(result: Any) -> dict[str, Any]:
    try:
        payload = result.to_dict()
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass
    return {}


def _extra_failure_cooldown_seconds(*, result: Any) -> float:
    payload = _result_payload(result)
    error_step = str(payload.get("errorStep") or "").strip().lower()
    error_text = str(payload.get("error") or "").strip().lower()
    step_errors = payload.get("stepErrors") or {}

    if error_step == "create-openai-account":
        create_error = step_errors.get("create-openai-account") or {}
        combined = " ".join(
            part
            for part in (
                error_text,
                str(create_error.get("message") or "").strip().lower(),
                str(create_error.get("code") or "").strip().lower(),
            )
            if part
        )
        if any(
            marker in combined
            for marker in (
                "rate limit exceeded",
                "status=429",
                "status=403",
                "platform_login status=403",
                "authorize_continue status=429",
                "unexpected_eof_while_reading",
                "eof occurred in violation of protocol",
            )
        ):
            return float(
                max(
                    0,
                    _env_float("REGISTER_CREATE_ACCOUNT_COOLDOWN_SECONDS", 60.0),
                )
            )

    if error_step == "acquire-mailbox":
        mailbox_error = step_errors.get("acquire-mailbox") or {}
        combined = " ".join(
            part
            for part in (
                error_text,
                str(mailbox_error.get("message") or "").strip().lower(),
                str(mailbox_error.get("code") or "").strip().lower(),
            )
            if part
        )
        if combined:
            return float(max(0, _env_float("REGISTER_MAILBOX_FAILURE_COOLDOWN_SECONDS", 15.0)))

    if error_step == "invite-codex-member":
        invite_error = step_errors.get("invite-codex-member") or {}
        combined = " ".join(
            part
            for part in (
                error_text,
                str(invite_error.get("message") or "").strip().lower(),
                str(invite_error.get("code") or "").strip().lower(),
            )
            if part
        )
        if "workspace has reached maximum number of seats" in combined or "team_seats_full" in combined:
            return float(max(0, _env_float("REGISTER_TEAM_CAPACITY_COOLDOWN_SECONDS", 30.0)))

    return 0.0


def _claim_task_index(
    *,
    task_counter: Any,
    max_runs: int,
) -> int | None:
    with task_counter.get_lock():
        current = int(task_counter.value or 0)
        if max_runs > 0 and current >= max_runs:
            return None
        current += 1
        task_counter.value = current
        return current


def _worker_loop(
    *,
    worker_id: int,
    instance_id: str,
    instance_role: str,
    output_root_text: str,
    delay_seconds: float,
    max_runs: int,
    task_max_attempts: int,
    team_auth_path: str,
    flow_path: str,
    stop_event: Any,
    task_counter: Any,
    small_success_pool_dir_text: str,
    free_oauth_pool_dir_text: str,
) -> None:
    output_root = Path(output_root_text).resolve()
    shared_root = _shared_root_from_output_root(output_root)
    worker_output_root = _build_worker_output_root(output_root=output_root, worker_id=worker_id)
    _ensure_directory(worker_output_root)
    small_success_pool_dir = Path(small_success_pool_dir_text).resolve()
    _ensure_directory(small_success_pool_dir)
    free_oauth_pool_dir = Path(free_oauth_pool_dir_text).resolve()
    _ensure_directory(free_oauth_pool_dir)
    worker_label = f"worker-{worker_id:02d}"
    os.environ["REGISTER_WORKER_ID"] = worker_label
    local_run_index = 0
    worker_state = WorkerRuntimeState(
        shared_root=shared_root,
        instance_id=instance_id,
        instance_role=instance_role,
        worker_id=worker_label,
    )
    worker_state.started(
        pid=os.getpid(),
        output_root=str(worker_output_root),
        team_auth_pinned=bool(str(team_auth_path or "").strip()),
    )

    _json_log(
        {
            "event": "register_worker_started",
            "workerId": worker_label,
            "pid": os.getpid(),
            "outputRoot": str(worker_output_root),
            "teamAuthPinned": bool(str(team_auth_path or "").strip()),
        }
    )

    normalized_role = str(instance_role or "").strip().lower()
    if normalized_role == "team":
        threading.Thread(
            target=_team_live_local_sync_loop,
            kwargs={
                "stop_event": stop_event,
                "output_root": output_root,
                "worker_label": worker_label,
            },
            daemon=True,
            name=f"{worker_label}-team-live-local-sync",
        ).start()

    while not stop_event.is_set():
        wait_pool_result: dict[str, Any] | None = None
        if normalized_role in {"main", "continue"}:
            wait_pool_result = _drain_small_success_wait_pool(
                wait_pool_dir=_resolve_small_success_wait_pool_dir(output_root=output_root),
                continue_pool_dir=_resolve_small_success_continue_pool_dir(output_root=output_root),
                min_age_seconds=_small_success_wait_seconds(),
            )
        if isinstance(wait_pool_result, dict) and wait_pool_result.get("artifacts"):
            _json_log(
                {
                    "event": "register_small_success_wait_pool_processed",
                    "workerId": worker_label,
                    "instanceRole": normalized_role,
                    "result": wait_pool_result,
                }
            )
        continue_prefill_result: dict[str, Any] | None = None
        if normalized_role == "continue":
            continue_prefill_result = _backfill_small_success_continue_pool(
                source_pool_dir=_shared_root_from_output_root(output_root) / "small-success-pool",
                continue_pool_dir=_resolve_small_success_continue_pool_dir(output_root=output_root),
                max_move_count=_small_success_continue_prefill_count(),
                target_count=_small_success_continue_prefill_target_count(),
                min_age_seconds=_small_success_continue_prefill_min_age_seconds(),
            )
        if isinstance(continue_prefill_result, dict) and (
            continue_prefill_result.get("artifacts") or continue_prefill_result.get("discarded")
        ):
            _json_log(
                {
                    "event": "register_small_success_continue_pool_prefilled",
                    "workerId": worker_label,
                    "instanceRole": normalized_role,
                    "result": continue_prefill_result,
                }
            )
        backlog_result: dict[str, Any] | None = None
        if normalized_role in {"main", "continue"}:
            backlog_result = _drain_oauth_pool_backlog(
                pool_dir=free_oauth_pool_dir,
                target_folder="codex",
                local_percent=_env_percent("REGISTER_FREE_LOCAL_SPLIT_PERCENT", 100.0),
                local_dir=_resolve_free_local_dir(output_root=output_root),
            )
        elif normalized_role == "team":
            backlog_result = _drain_oauth_pool_backlog(
                pool_dir=_shared_root_from_output_root(output_root) / "team-oauth-pool",
                target_folder="codex-team",
                local_percent=_env_percent("REGISTER_TEAM_LOCAL_SPLIT_PERCENT", 0.0),
                local_dir=_resolve_team_local_dir(output_root=output_root),
            )
        if isinstance(backlog_result, dict) and (
            backlog_result.get("uploaded") or backlog_result.get("failures")
        ):
            _json_log(
                {
                    "event": "register_oauth_pool_backlog_processed",
                    "workerId": worker_label,
                    "instanceRole": normalized_role,
                    "result": backlog_result,
                }
            )
        task_index = _claim_task_index(task_counter=task_counter, max_runs=max_runs)
        if task_index is None:
            break
        free_local_selected = False
        if normalized_role in {"main", "continue"}:
            free_local_selected = _select_local_split(
                percent=_env_percent("REGISTER_FREE_LOCAL_SPLIT_PERCENT", 100.0)
            )
        team_auth_pool = _resolve_team_auth_pool(instance_role=normalized_role)
        stale_team_auth_cleanup = _prune_stale_team_auth_caches(
            shared_root=shared_root,
            active_team_auth_paths=team_auth_pool,
        )
        if stale_team_auth_cleanup.get("removedTeamAuthStatePaths") or stale_team_auth_cleanup.get("removedAvailabilityStatePaths"):
            _json_log(
                {
                    "event": "register_team_auth_cache_pruned",
                    "workerId": worker_label,
                    "instanceRole": normalized_role,
                    "removedTeamAuthStateCount": len(stale_team_auth_cleanup.get("removedTeamAuthStatePaths") or []),
                    "removedAvailabilityStateCount": len(stale_team_auth_cleanup.get("removedAvailabilityStatePaths") or []),
                    "removedTeamAuthStatePaths": stale_team_auth_cleanup.get("removedTeamAuthStatePaths") or [],
                    "removedAvailabilityStatePaths": stale_team_auth_cleanup.get("removedAvailabilityStatePaths") or [],
                }
            )
        pinned_team_auth_path = str(team_auth_path or "").strip()
        seat_reservation: dict[str, Any] | None = None
        selected_team_auth_path = ""
        if pinned_team_auth_path:
            if not Path(pinned_team_auth_path).is_file():
                pinned_team_auth_path = ""
            else:
                reserved_for_team, reserved_state = _team_auth_is_reserved_for_team_expand(
                    shared_root=shared_root,
                    team_auth_path=pinned_team_auth_path,
                )
                if reserved_for_team and normalized_role in {"main", "continue"}:
                    _json_log(
                        {
                            "event": "register_team_auth_pinned_reserved_for_team_expand",
                            "workerId": worker_label,
                            "taskIndex": task_index,
                            "instanceRole": normalized_role,
                            "teamAuthPath": pinned_team_auth_path,
                            "reserved": reserved_state,
                        }
                    )
                    pinned_team_auth_path = ""
        if pinned_team_auth_path:
            pinned_blacklisted, _ = _team_auth_is_temp_blacklisted(
                shared_root=shared_root,
                team_auth_path=pinned_team_auth_path,
            )
            if not pinned_blacklisted:
                selected_team_auth_path, seat_reservation = _select_team_auth_path(
                    team_auth_pool=[pinned_team_auth_path],
                    task_index=task_index,
                    shared_root=shared_root,
                    instance_role=normalized_role,
                    worker_label=worker_label,
                )
            if pinned_blacklisted:
                selected_team_auth_path, seat_reservation = _select_team_auth_path(
                    team_auth_pool=team_auth_pool,
                    task_index=task_index,
                    shared_root=shared_root,
                    instance_role=normalized_role,
                    worker_label=worker_label,
                )
        else:
            selected_team_auth_path, seat_reservation = _select_team_auth_path(
                team_auth_pool=team_auth_pool,
                task_index=task_index,
                shared_root=shared_root,
                instance_role=normalized_role,
                worker_label=worker_label,
            )
        if normalized_role in {"main", "continue", "team"} and team_auth_pool and not selected_team_auth_path:
            _json_log(
                {
                    "event": "register_team_auth_pool_filtered_empty",
                    "workerId": worker_label,
                    "pid": os.getpid(),
                    "taskIndex": task_index,
                    "teamAuthPoolSize": len(team_auth_pool),
                }
            )
            worker_state.sleeping(task_index=task_index, seconds=max(float(delay_seconds or 0.0), 1.0))
            time.sleep(max(float(delay_seconds or 0.0), 1.0))
            continue
        local_run_index += 1
        run_output_dir = _build_run_output_dir(
            worker_output_root=worker_output_root,
            task_index=task_index,
        )
        _ensure_directory(run_output_dir)
        started_at = datetime.now(timezone.utc).isoformat()
        _json_log(
            {
                "event": "register_run_started",
                "workerId": worker_label,
                "pid": os.getpid(),
                "taskIndex": task_index,
                "localRunIndex": local_run_index,
                "startedAt": started_at,
                "outputDir": str(run_output_dir),
                "teamAuthPath": selected_team_auth_path,
                "teamAuthPoolSize": len(team_auth_pool),
                "freeLocalSelected": free_local_selected,
            }
        )
        worker_state.run_started(
            task_index=task_index,
            local_run_index=local_run_index,
            started_at=started_at,
            output_dir=str(run_output_dir),
            team_auth_path=selected_team_auth_path,
            team_auth_pool_size=len(team_auth_pool),
        )
        try:
            result = run_dst_flow_once(
                output_dir=str(run_output_dir),
                team_auth_path=selected_team_auth_path or None,
                small_success_pool_dir=str(small_success_pool_dir),
                flow_path=flow_path or None,
                task_max_attempts=task_max_attempts or None,
                r2_upload_enabled=(not free_local_selected) if str(instance_role or "").strip().lower() in {"main", "continue"} else None,
            )
            _json_log(
                {
                    "event": "register_run_finished",
                    "workerId": worker_label,
                    "pid": os.getpid(),
                    "taskIndex": task_index,
                    "localRunIndex": local_run_index,
                    "startedAt": started_at,
                    "finishedAt": datetime.now(timezone.utc).isoformat(),
                    "ok": bool(result.ok),
                    "outputDir": str(run_output_dir),
                    "result": result.to_dict(),
                }
            )
            worker_state.run_finished(
                task_index=task_index,
                result=result.to_dict(),
                output_dir=str(run_output_dir),
                finished_at=datetime.now(timezone.utc).isoformat(),
            )
            result_payload = result.to_dict()
            effective_team_auth_path = selected_team_auth_path
            if normalized_role == "team":
                effective_team_auth_path = _team_auth_path_from_result_payload(
                    result_payload=result_payload,
                    fallback_path=selected_team_auth_path,
                )
            invite_capacity_cleanup_output = (
                (result_payload.get("outputs") or {}).get("invite-codex-member-capacity-cleanup")
                if isinstance(result_payload, dict)
                else None
            )
            if effective_team_auth_path and isinstance(invite_capacity_cleanup_output, dict):
                _team_auth_sync_codex_seats_from_cleanup_result(
                    shared_root=shared_root,
                    cleanup_result={
                        "results": [
                            {
                                "teamAuthPath": effective_team_auth_path,
                                **invite_capacity_cleanup_output,
                            }
                        ]
                    },
                    worker_label=worker_label,
                    task_index=task_index,
                )
            mailbox_domain_outcome = _record_business_mailbox_domain_outcome(
                shared_root=shared_root,
                result_payload=result_payload,
                instance_role=normalized_role,
            )
            if mailbox_domain_outcome:
                _json_log(
                    {
                        "event": "register_mailbox_domain_outcome_recorded",
                        "workerId": worker_label,
                        "taskIndex": task_index,
                        "instanceRole": normalized_role,
                        "result": mailbox_domain_outcome,
                    }
                )
            team_result_identity = (
                _team_mother_identity_from_result_payload(result_payload)
                if normalized_role == "team"
                else None
            )
            _record_team_auth_recent_invite_result(
                shared_root=shared_root,
                team_auth_path=effective_team_auth_path,
                result_payload=result_payload,
                identity=team_result_identity,
            )
            _record_team_auth_recent_team_expand_result(
                shared_root=shared_root,
                team_auth_path=effective_team_auth_path,
                result_payload=result_payload,
                instance_role=normalized_role,
                identity=team_result_identity,
            )
            _team_auth_reconcile_seat_state_from_result(
                shared_root=shared_root,
                team_auth_path=effective_team_auth_path,
                result_payload=result_payload,
                instance_role=normalized_role,
                worker_label=worker_label,
                task_index=task_index,
            )
            synced_credentials = _sync_refreshed_credentials_back_to_sources(
                result_payload=result_payload,
                worker_label=worker_label,
                task_index=task_index,
            )
            if bool(result.ok):
                success_steps = result_payload.get("steps") if isinstance(result_payload, dict) else {}
                if isinstance(success_steps, dict):
                    if normalized_role in {"main", "continue"} and str(success_steps.get("invite-codex-member") or "").strip().lower() == "ok":
                        _clear_team_auth_temporary_blacklist(
                            shared_root=shared_root,
                            team_auth_path=effective_team_auth_path,
                            identity=_team_mother_identity_from_team_auth_path(effective_team_auth_path),
                            worker_label=worker_label,
                            task_index=task_index,
                        )
                    elif normalized_role == "team" and str(success_steps.get("invite-team-members") or "").strip().lower() == "ok":
                        _clear_team_auth_temporary_blacklist(
                            shared_root=shared_root,
                            team_auth_path=str((result_payload.get("outputs") or {}).get("obtain-team-mother-oauth", {}).get("successPath") or selected_team_auth_path),
                            identity=_team_mother_identity_from_result_payload(result_payload),
                            worker_label=worker_label,
                            task_index=task_index,
                        )
            stop_after_validate_mode = _free_stop_after_validate_mode() and normalized_role in {"main", "continue"}
            if stop_after_validate_mode:
                outputs = result_payload.get("outputs") if isinstance(result_payload, dict) else {}
                create_output = outputs.get("create-openai-account") if isinstance(outputs, dict) else {}
                validate_output = outputs.get("validate-free-personal-oauth") if isinstance(outputs, dict) else {}
                obtain_output = outputs.get("obtain-codex-oauth") if isinstance(outputs, dict) else {}
                _json_log(
                    {
                        "event": "register_free_stop_after_validate_handoff",
                        "workerId": worker_label,
                        "taskIndex": task_index,
                        "instanceRole": normalized_role,
                        "email": str((create_output or {}).get("email") or "").strip(),
                        "smallSuccessPath": str((create_output or {}).get("storage_path") or "").strip(),
                        "validateStatus": str((validate_output or {}).get("status") or "").strip(),
                        "validateCode": str((validate_output or {}).get("code") or "").strip(),
                        "oauthSuccessPath": str((obtain_output or {}).get("successPath") or "").strip(),
                    }
                )
            mailbox_capacity_detail = _mailbox_capacity_failure_detail(result_payload=result_payload)
            if mailbox_capacity_detail:
                recovery_result = _mark_mailbox_capacity_failure(
                    shared_root=shared_root,
                    detail=mailbox_capacity_detail,
                )
                _json_log(
                    {
                        "event": "register_mailbox_capacity_recovery_evaluated",
                        "workerId": worker_label,
                        "taskIndex": task_index,
                        "recoveryResult": recovery_result,
                    }
                )
            elif str((result_payload.get("steps") or {}).get("acquire-mailbox") or "").strip().lower() == "ok":
                _clear_mailbox_capacity_failures(shared_root=shared_root)

            capacity_detail = _team_capacity_failure_detail(result_payload=result_payload)
            if effective_team_auth_path and capacity_detail:
                _mark_team_auth_capacity_cooldown(
                    shared_root=shared_root,
                    team_auth_path=effective_team_auth_path,
                    cooldown_seconds=_env_float("REGISTER_TEAM_CAPACITY_COOLDOWN_SECONDS", 180.0),
                    detail=capacity_detail,
                )
                if _all_team_auth_capacity_cooled(shared_root=shared_root, team_auth_pool=team_auth_pool):
                    cleanup_result = _trigger_codex_capacity_cleanup(
                        shared_root=shared_root,
                        team_auth_pool=team_auth_pool,
                    )
                    _json_log(
                        {
                            "event": "register_team_codex_cleanup_triggered",
                            "workerId": worker_label,
                            "taskIndex": task_index,
                            "teamAuthPoolSize": len(team_auth_pool),
                            "cleanupResult": cleanup_result,
                        }
                    )
                    if isinstance(cleanup_result, dict):
                        _team_auth_sync_codex_seats_from_cleanup_result(
                            shared_root=shared_root,
                            cleanup_result=cleanup_result,
                            worker_label=worker_label,
                            task_index=task_index,
                        )
            elif (
                effective_team_auth_path
                and str((result_payload.get("steps") or {}).get("invite-codex-member") or "").strip().lower() == "ok"
            ):
                _clear_team_auth_capacity_cooldown(
                    shared_root=shared_root,
                    team_auth_path=effective_team_auth_path,
                )

            blacklist_reason = _team_auth_blacklist_reason(result_payload=result_payload)
            if blacklist_reason:
                blacklist_identity = (
                    _team_mother_identity_from_result_payload(result_payload)
                    if normalized_role == "team"
                    else _team_mother_identity_from_team_auth_path(selected_team_auth_path)
                )
                blacklist_record = _mark_team_auth_temporary_blacklist(
                    shared_root=shared_root,
                    team_auth_path=effective_team_auth_path,
                    identity=blacklist_identity,
                    reason=blacklist_reason,
                    blacklist_seconds=_env_float("REGISTER_TEAM_AUTH_TEMP_BLACKLIST_SECONDS", 3600.0),
                    worker_label=worker_label,
                    task_index=task_index,
                )
                if blacklist_record:
                    _json_log(
                        {
                            "event": "register_team_auth_temporary_blacklist_evaluated",
                            "workerId": worker_label,
                            "taskIndex": task_index,
                            "result": blacklist_record,
                        }
                    )
            if stop_after_validate_mode:
                pass
            elif bool(result.ok):
                postprocess_result: dict[str, Any] = {
                    "ok": True,
                    "status": "no_success_postprocess",
                    "cleanup_run_output": False,
                }
                if normalized_role in {"main", "continue"}:
                    postprocess_result = _postprocess_free_success_artifact(
                        result=result,
                        output_root=output_root,
                        worker_label=worker_label,
                        task_index=task_index,
                        free_local_selected=free_local_selected,
                    )
                elif normalized_role == "team":
                    postprocess_result = _postprocess_team_success_artifacts(
                        result=result,
                        output_root=output_root,
                    )
                _json_log(
                    {
                        "event": "register_success_postprocess",
                        "workerId": worker_label,
                        "taskIndex": task_index,
                        "instanceRole": normalized_role,
                        "syncedCredentialCount": len(synced_credentials),
                        "result": postprocess_result,
                    }
                )
                if bool(postprocess_result.get("cleanup_run_output")):
                    _cleanup_run_output_dir(
                        run_output_dir=run_output_dir,
                        worker_label=worker_label,
                        task_index=task_index,
                    )
            elif not bool(result.ok):
                if normalized_role == "team" and _team_has_collectable_artifacts(result=result):
                    postprocess_result = _postprocess_team_success_artifacts(
                        result=result,
                        output_root=output_root,
                    )
                    _json_log(
                        {
                            "event": "register_success_postprocess",
                            "workerId": worker_label,
                            "taskIndex": task_index,
                            "instanceRole": normalized_role,
                            "syncedCredentialCount": len(synced_credentials),
                            "result": postprocess_result,
                        }
                    )
                _copy_small_success_artifacts_to_pool(
                    run_output_dir=run_output_dir,
                    pool_dir=_small_success_failure_target_pool_dir(
                        output_root=output_root,
                        result_payload=result_payload,
                    ),
                    worker_label=worker_label,
                    task_index=task_index,
                )
                _cleanup_run_output_dir(
                    run_output_dir=run_output_dir,
                    worker_label=worker_label,
                    task_index=task_index,
                )
            extra_cooldown_seconds = _extra_failure_cooldown_seconds(result=result)
            if normalized_role == "team" and not bool(result.ok):
                mother_cooldown_seconds = _team_mother_failure_cooldown_seconds(result=result)
                if mother_cooldown_seconds > 0:
                    _mark_team_mother_failure_cooldown(
                        shared_root=shared_root,
                        result_payload=result_payload,
                        cooldown_seconds=mother_cooldown_seconds,
                        reason=str(result_payload.get("errorStep") or "").strip() or str(result_payload.get("error") or "").strip(),
                        worker_label=worker_label,
                        task_index=task_index,
                    )
                    extra_cooldown_seconds = 0.0
        except Exception as exc:
            extra_cooldown_seconds = float(
                max(0, _env_float("REGISTER_CRASH_COOLDOWN_SECONDS", 20.0))
            )
            _json_log(
                {
                    "event": "register_run_crashed",
                    "workerId": worker_label,
                    "pid": os.getpid(),
                    "taskIndex": task_index,
                    "localRunIndex": local_run_index,
                    "startedAt": started_at,
                    "finishedAt": datetime.now(timezone.utc).isoformat(),
                    "outputDir": str(run_output_dir),
                    "error": str(exc),
                }
            )
            worker_state.run_crashed(
                task_index=task_index,
                output_dir=str(run_output_dir),
                error=str(exc),
                finished_at=datetime.now(timezone.utc).isoformat(),
            )
            _copy_small_success_artifacts_to_pool(
                run_output_dir=run_output_dir,
                pool_dir=small_success_pool_dir,
                worker_label=worker_label,
                task_index=task_index,
            )
            _cleanup_run_output_dir(
                run_output_dir=run_output_dir,
                worker_label=worker_label,
                task_index=task_index,
            )
        finally:
            reservation_summary = _team_auth_release_seat_reservations(
                shared_root=shared_root,
                reservation=seat_reservation,
            )
            if reservation_summary is not None and selected_team_auth_path:
                _json_log(
                    {
                        "event": "register_team_auth_seat_reservation_released",
                        "workerId": worker_label,
                        "taskIndex": task_index,
                        "teamAuthPath": selected_team_auth_path,
                        "seatSummary": reservation_summary,
                    }
                )
        if stop_event.is_set():
            break
        sleep_seconds = max(float(delay_seconds or 0.0), float(extra_cooldown_seconds or 0.0))
        if sleep_seconds > 0:
            _json_log(
                {
                    "event": "register_worker_sleep",
                    "workerId": worker_label,
                    "taskIndex": task_index,
                    "seconds": sleep_seconds,
                }
            )
            worker_state.sleeping(task_index=task_index, seconds=sleep_seconds)
            time.sleep(sleep_seconds)

    worker_state.exited(local_runs=local_run_index)
    _json_log(
        {
            "event": "register_worker_exited",
            "workerId": worker_label,
            "pid": os.getpid(),
            "localRuns": local_run_index,
        }
    )


def _install_signal_handlers(*, stop_event: Any) -> None:
    def _handler(signum: int, _frame: Any) -> None:
        _json_log(
            {
                "event": "register_supervisor_signal",
                "pid": os.getpid(),
                "signal": signum,
            }
        )
        stop_event.set()

    signal.signal(signal.SIGINT, _handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handler)


def _start_worker(
    *,
    ctx: Any,
    worker_id: int,
    instance_id: str,
    instance_role: str,
    output_root_text: str,
    delay_seconds: float,
    max_runs: int,
    task_max_attempts: int,
    team_auth_path: str,
    flow_path: str,
    stop_event: Any,
    task_counter: Any,
    small_success_pool_dir_text: str,
    free_oauth_pool_dir_text: str,
) -> Any:
    process = ctx.Process(
        target=_worker_loop,
        kwargs={
            "worker_id": worker_id,
            "instance_id": instance_id,
            "instance_role": instance_role,
            "output_root_text": output_root_text,
            "delay_seconds": delay_seconds,
            "max_runs": max_runs,
            "task_max_attempts": task_max_attempts,
            "team_auth_path": team_auth_path,
            "flow_path": flow_path,
            "stop_event": stop_event,
            "task_counter": task_counter,
            "small_success_pool_dir_text": small_success_pool_dir_text,
            "free_oauth_pool_dir_text": free_oauth_pool_dir_text,
        },
        name=f"register-worker-{worker_id:02d}",
    )
    process.start()
    _json_log(
        {
            "event": "register_worker_spawned",
            "workerId": f"worker-{worker_id:02d}",
            "pid": process.pid,
        }
    )
    return process


def _task_slots_exhausted(*, task_counter: Any, max_runs: int) -> bool:
    if max_runs <= 0:
        return False
    with task_counter.get_lock():
        return int(task_counter.value or 0) >= max_runs


def main() -> int:
    output_root = Path(_env_text("REGISTER_OUTPUT_ROOT", "/shared/register-output")).resolve()
    _ensure_directory(output_root)
    shared_root = _shared_root_from_output_root(output_root)

    delay_seconds = max(0.0, _env_float("REGISTER_LOOP_DELAY_SECONDS", 5.0))
    worker_count = max(1, _env_int("REGISTER_WORKER_COUNT", 10))
    worker_stagger_seconds = max(0.0, _env_float("REGISTER_WORKER_STAGGER_SECONDS", 2.0))
    max_runs = max(0, _env_int("REGISTER_INFINITE_MAX_RUNS", 0))
    task_max_attempts = _env_int("REGISTER_TASK_MAX_ATTEMPTS", 0)
    team_auth_path = _env_text("REGISTER_TEAM_AUTH_PATH")
    flow_path = _env_text("REGISTER_FLOW_PATH")
    small_success_pool_dir = _resolve_small_success_pool_dir(output_root=output_root)
    _ensure_directory(small_success_pool_dir)
    free_oauth_pool_dir = _resolve_free_oauth_pool_dir(output_root=output_root)
    _ensure_directory(free_oauth_pool_dir)
    instance_id = _env_text("REGISTER_INSTANCE_ID", "main") or "main"
    instance_role = _env_text("REGISTER_INSTANCE_ROLE", instance_id) or instance_id
    easy_protocol_base_url = _env_text("EASY_PROTOCOL_BASE_URL", "http://easy-protocol-service:9788")
    easy_protocol_control_token = _env_text("EASY_PROTOCOL_CONTROL_TOKEN", "")
    easy_protocol_control_actor = _env_text("EASY_PROTOCOL_CONTROL_ACTOR", "register-dashboard")

    ctx = mp.get_context("spawn")
    stop_event = ctx.Event()
    task_counter = ctx.Value("i", 0)
    processes: dict[int, Any] = {}
    dashboard_server = None
    _cleanup_dashboard_worker_state_files(shared_root=shared_root, instance_id=instance_id)
    service_state = ServiceRuntimeState(
        shared_root=shared_root,
        instance_id=instance_id,
        instance_role=instance_role,
        flow_path=flow_path,
        output_root=str(output_root),
        worker_count=worker_count,
        delay_seconds=delay_seconds,
        worker_stagger_seconds=worker_stagger_seconds,
        small_success_pool_dir=str(small_success_pool_dir),
    )

    _install_signal_handlers(stop_event=stop_event)
    service_state.started(pid=os.getpid(), max_runs=max_runs)
    dashboard_server = start_dashboard_server_if_enabled(
        output_root=output_root,
        easy_protocol_base_url=easy_protocol_base_url,
        easy_protocol_token=easy_protocol_control_token,
        easy_protocol_actor=easy_protocol_control_actor,
    )
    _json_log(
        {
            "event": "register_supervisor_started",
            "pid": os.getpid(),
            "instanceId": instance_id,
            "instanceRole": instance_role,
            "workerCount": worker_count,
            "delaySeconds": delay_seconds,
            "workerStaggerSeconds": worker_stagger_seconds,
            "maxRuns": max_runs,
            "outputRoot": str(output_root),
            "smallSuccessPoolDir": str(small_success_pool_dir),
            "freeOauthPoolDir": str(free_oauth_pool_dir),
        }
    )

    try:
        for worker_id in range(1, worker_count + 1):
            if stop_event.is_set():
                break
            processes[worker_id] = _start_worker(
                ctx=ctx,
                worker_id=worker_id,
                instance_id=instance_id,
                instance_role=instance_role,
                output_root_text=str(output_root),
                delay_seconds=delay_seconds,
                max_runs=max_runs,
                task_max_attempts=task_max_attempts,
                team_auth_path=team_auth_path,
                flow_path=flow_path,
                stop_event=stop_event,
                task_counter=task_counter,
                small_success_pool_dir_text=str(small_success_pool_dir),
                free_oauth_pool_dir_text=str(free_oauth_pool_dir),
            )
            if worker_stagger_seconds > 0 and worker_id < worker_count:
                time.sleep(worker_stagger_seconds)

        while processes:
            if stop_event.is_set():
                break
            for worker_id, process in list(processes.items()):
                if process.is_alive():
                    continue
                exit_code = int(process.exitcode or 0)
                processes.pop(worker_id, None)
                _json_log(
                    {
                        "event": "register_worker_stopped",
                        "workerId": f"worker-{worker_id:02d}",
                        "pid": process.pid,
                        "exitCode": exit_code,
                    }
                )
                if stop_event.is_set():
                    continue
                if _task_slots_exhausted(task_counter=task_counter, max_runs=max_runs):
                    continue
                _json_log(
                    {
                        "event": "register_worker_restarting",
                        "workerId": f"worker-{worker_id:02d}",
                    }
                )
                processes[worker_id] = _start_worker(
                    ctx=ctx,
                    worker_id=worker_id,
                    instance_id=instance_id,
                    instance_role=instance_role,
                    output_root_text=str(output_root),
                    delay_seconds=delay_seconds,
                    max_runs=max_runs,
                    task_max_attempts=task_max_attempts,
                    team_auth_path=team_auth_path,
                    flow_path=flow_path,
                    stop_event=stop_event,
                    task_counter=task_counter,
                    small_success_pool_dir_text=str(small_success_pool_dir),
                    free_oauth_pool_dir_text=str(free_oauth_pool_dir),
                )
                if worker_stagger_seconds > 0:
                    time.sleep(worker_stagger_seconds)
            if processes:
                time.sleep(1.0)
    finally:
        stop_event.set()
        shutdown_deadline = time.monotonic() + 15.0
        for process in processes.values():
            remaining = max(0.0, shutdown_deadline - time.monotonic())
            if remaining <= 0:
                break
            process.join(timeout=min(remaining, 2.0))
        for process in processes.values():
            if process.is_alive():
                process.terminate()
        for process in processes.values():
            process.join(timeout=1.0)
        _json_log(
            {
                "event": "register_supervisor_stopped",
                "pid": os.getpid(),
                "instanceId": instance_id,
                "taskCount": int(task_counter.value or 0),
            }
        )
        service_state.stopped(pid=os.getpid(), task_count=int(task_counter.value or 0))
        if dashboard_server is not None:
            dashboard_server.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
