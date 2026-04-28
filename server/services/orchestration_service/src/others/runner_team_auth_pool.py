from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

from others.common import (
    decode_jwt_payload,
    ensure_directory,
    extract_auth_claims,
    team_mother_cooldown_key,
)
from others.config import TeamAuthRuntimeConfig
from others.paths import (
    resolve_team_mother_claims_dir,
    resolve_team_mother_cooldowns_dir,
    resolve_team_mother_pool_dir,
)
from others.storage import load_json_payload


def team_auth_runtime_config(
    *,
    output_root: Path | None = None,
    shared_root: Path | None = None,
) -> TeamAuthRuntimeConfig:
    return TeamAuthRuntimeConfig.from_env(output_root=output_root, shared_root=shared_root)


def _split_path_list(raw: str) -> list[str]:
    normalized = str(raw or "").strip()
    if not normalized:
        return []
    return [item.strip() for item in normalized.split(os.pathsep) if str(item or "").strip()]


def team_auth_path_is_explicit_mother(path: Path) -> bool:
    return str(path.name or "").strip().lower().startswith("codex-team-mother-")


def team_auth_identity_claims(payload: Any) -> dict[str, Any]:
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
            claims = decode_jwt_payload(token)
            if claims:
                return claims
    return {}


def team_auth_payload_is_mother(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    auth_claims = extract_auth_claims(payload)
    plan_type = str(auth_claims.get("chatgpt_plan_type") or "").strip().lower()
    if plan_type and plan_type != "team":
        return False
    identity_claims = team_auth_identity_claims(payload)
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


def team_auth_pool_candidates(*, candidate_dirs: list[str]) -> list[str]:
    glob_pattern = team_auth_runtime_config().auth_glob or "*-team.json"
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
            if not team_auth_payload_is_mother(payload):
                continue
            if team_auth_path_is_explicit_mother(path):
                explicit.append(resolved)
            else:
                inferred.append(resolved)
    return explicit + inferred


def resolve_team_auth_pool(*, instance_role: str) -> list[str]:
    config = team_auth_runtime_config()
    normalized_role = str(instance_role or "").strip().lower()
    if normalized_role == "team":
        pool_dir = config.mother_pool_dir
        if not pool_dir.exists():
            return []
        return team_auth_pool_candidates(candidate_dirs=[str(pool_dir)])

    explicit_paths = list(config.auth_paths)
    if explicit_paths:
        return team_auth_pool_candidates(candidate_dirs=explicit_paths)

    explicit_path = config.auth_path
    if explicit_path:
        candidate = Path(explicit_path).expanduser()
        if candidate.exists():
            return team_auth_pool_candidates(candidate_dirs=[str(candidate.resolve())])
        return []

    candidate_dirs = list(config.auth_dirs)
    if not candidate_dirs:
        preferred_local_dir = config.auth_local_dir or config.local_dir
        fallback_default_dir = config.auth_default_dir
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
    return team_auth_pool_candidates(candidate_dirs=candidate_dirs)


def team_mother_identity_from_team_auth_path(team_auth_path: str) -> dict[str, str]:
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
    auth_claims = extract_auth_claims(payload)
    return {
        "original_name": path.name,
        "email": str(payload.get("email") or "").strip(),
        "account_id": str(
            payload.get("account_id")
            or auth_claims.get("chatgpt_account_id")
            or ""
        ).strip(),
    }


def team_mother_identity_key(*, original_name: str, email: str, account_id: str) -> str:
    return team_mother_cooldown_key(
        original_name=str(original_name or "").strip(),
        email=str(email or "").strip(),
        account_id=str(account_id or "").strip(),
    )


def team_auth_identity_keys_from_paths(team_auth_paths: list[str]) -> set[str]:
    identity_keys: set[str] = set()
    for team_auth_path in team_auth_paths:
        normalized_path = str(team_auth_path or "").strip()
        if not normalized_path:
            continue
        identity = team_mother_identity_from_team_auth_path(normalized_path)
        identity_key = team_mother_identity_key(
            original_name=str(identity.get("original_name") or "").strip(),
            email=str(identity.get("email") or "").strip(),
            account_id=str(identity.get("account_id") or "").strip(),
        )
        if identity_key:
            identity_keys.add(identity_key)
    return identity_keys


def team_mother_cooldowns_dir_for_shared_root(*, shared_root: Path) -> Path:
    return Path(resolve_team_mother_cooldowns_dir(str(shared_root))).resolve()


def team_mother_reserved_identity_keys_for_shared_root(*, shared_root: Path) -> set[str]:
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
            identity_key = team_mother_identity_key(
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


def team_auth_is_reserved_for_team_expand(
    *,
    shared_root: Path,
    team_auth_path: str,
    reserved_keys: set[str] | None = None,
) -> tuple[bool, dict[str, Any]]:
    normalized_path = str(team_auth_path or "").strip()
    if not normalized_path:
        return False, {}
    identity = team_mother_identity_from_team_auth_path(normalized_path)
    identity_key = team_mother_identity_key(
        original_name=str(identity.get("original_name") or "").strip(),
        email=str(identity.get("email") or "").strip(),
        account_id=str(identity.get("account_id") or "").strip(),
    )
    if not identity_key:
        return False, {}
    active_reserved_keys = reserved_keys if reserved_keys is not None else team_mother_reserved_identity_keys_for_shared_root(shared_root=shared_root)
    if identity_key not in active_reserved_keys:
        return False, {}
    return True, {
        "teamAuthPath": normalized_path,
        "identityKey": identity_key,
        "original_name": str(identity.get("original_name") or "").strip(),
        "email": str(identity.get("email") or "").strip(),
        "account_id": str(identity.get("account_id") or "").strip(),
    }


def _load_json_dict(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def team_mother_availability_state_path(
    *,
    shared_root: Path,
    original_name: str,
    email: str,
    account_id: str,
) -> Path:
    cooldown_dir = team_mother_cooldowns_dir_for_shared_root(shared_root=shared_root)
    ensure_directory(cooldown_dir)
    return cooldown_dir / f"{team_mother_cooldown_key(original_name=original_name, email=email, account_id=account_id)}.json"


def load_team_mother_availability_state(
    *,
    shared_root: Path,
    original_name: str,
    email: str,
    account_id: str,
) -> tuple[Path, dict[str, Any]]:
    state_path = team_mother_availability_state_path(
        shared_root=shared_root,
        original_name=original_name,
        email=email,
        account_id=account_id,
    )
    payload = _load_json_dict(state_path) if state_path.is_file() else {}
    return state_path, payload


def team_mother_availability_state_lock_path(*, state_path: Path) -> Path:
    return state_path.with_suffix(state_path.suffix + ".lock")


def _team_auth_state_lock_timeout_seconds() -> float:
    return team_auth_runtime_config().state_lock_timeout_seconds


def acquire_team_mother_availability_state_lock(*, state_path: Path) -> Path:
    lock_path = team_mother_availability_state_lock_path(state_path=state_path)
    ensure_directory(lock_path.parent)
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


def release_team_mother_availability_state_lock(*, lock_path: Path | None) -> None:
    if lock_path is None:
        return
    lock_path.unlink(missing_ok=True)


def _team_auth_total_seat_limit() -> int:
    return team_auth_runtime_config().total_seat_limit


def _team_auth_chatgpt_seat_limit() -> int:
    return team_auth_runtime_config().chatgpt_seat_limit


def _team_auth_codex_seat_limit() -> int:
    return team_auth_runtime_config().codex_seat_limit


def _team_auth_reservation_ttl_seconds() -> float:
    return team_auth_runtime_config().reservation_ttl_seconds


def _team_auth_team_member_chatgpt_seat_request() -> int:
    return team_auth_runtime_config().team_member_count


def _team_auth_codex_seat_types() -> set[str]:
    return set(team_auth_runtime_config().codex_seat_types)


def _team_auth_sall_cc_weight() -> float:
    return team_auth_runtime_config().sall_cc_weight


def _team_auth_zero_success_window_seconds() -> float:
    return team_auth_runtime_config().zero_success_window_seconds


def _team_auth_zero_success_min_attempts() -> int:
    return team_auth_runtime_config().zero_success_min_attempts


def _team_auth_team_expand_window_seconds() -> float:
    return team_auth_runtime_config().team_expand_window_seconds


def _team_auth_team_expand_failure_weight_step() -> float:
    return team_auth_runtime_config().team_expand_failure_weight_step


def _team_auth_team_expand_floor_weight() -> float:
    return team_auth_runtime_config().team_expand_floor_weight


def _team_auth_team_expand_success_credit() -> float:
    return team_auth_runtime_config().team_expand_success_credit
