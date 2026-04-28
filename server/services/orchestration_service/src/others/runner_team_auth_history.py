from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from others.common import json_log
from others.runner_team_auth_state import (
    _load_json_dict,
    load_team_mother_availability_state,
    prune_team_mother_availability_state,
    team_auth_identity_keys_from_paths,
    team_mother_cooldowns_dir_for_shared_root,
    team_mother_identity_from_team_auth_path,
    team_mother_identity_key,
    team_mother_reserved_identity_keys_for_shared_root,
    write_team_mother_availability_state,
)
from others.runner_team_cleanup import team_auth_state_dir, team_cleanup_state_path


def record_team_auth_recent_invite_result(
    *,
    shared_root: Path,
    team_auth_path: str,
    result_payload_value: dict[str, object],
    identity: dict[str, str] | None = None,
) -> None:
    normalized_path = str(team_auth_path or "").strip()
    if not normalized_path:
        return
    steps = result_payload_value.get("steps") if isinstance(result_payload_value, dict) else {}
    if not isinstance(steps, dict):
        return
    step_name = ""
    invite_ok: bool | None = None
    for candidate in ("invite-codex-member", "invite-team-members"):
        status = str(steps.get(candidate) or "").strip().lower()
        if status == "ok":
            step_name = candidate
            invite_ok = True
            break
        if status == "failed":
            step_name = candidate
            invite_ok = False
            break
    if invite_ok is None:
        return
    resolved_identity = {
        **team_mother_identity_from_team_auth_path(normalized_path),
        **(identity or {}),
    }
    if not any(str(resolved_identity.get(key) or "").strip() for key in ("original_name", "email", "account_id")):
        return
    state_path, payload = load_team_mother_availability_state(
        shared_root=shared_root,
        original_name=str(resolved_identity.get("original_name") or "").strip(),
        email=str(resolved_identity.get("email") or "").strip(),
        account_id=str(resolved_identity.get("account_id") or "").strip(),
    )
    normalized = prune_team_mother_availability_state(state_path=state_path, payload=payload)
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
    write_team_mother_availability_state(state_path=state_path, payload=normalized)


def record_team_auth_recent_team_expand_result(
    *,
    shared_root: Path,
    team_auth_path: str,
    result_payload_value: dict[str, object],
    instance_role: str,
    identity: dict[str, str] | None = None,
) -> None:
    if str(instance_role or "").strip().lower() != "team":
        return
    normalized_path = str(team_auth_path or "").strip()
    if not normalized_path:
        return
    steps = result_payload_value.get("steps") if isinstance(result_payload_value, dict) else {}
    outputs = result_payload_value.get("outputs") if isinstance(result_payload_value, dict) else {}
    if not isinstance(steps, dict) or not isinstance(outputs, dict):
        return
    invite_status = str(steps.get("invite-team-members") or "").strip().lower()
    invite_output = outputs.get("invite-team-members")
    if invite_status != "ok" or not isinstance(invite_output, dict):
        return
    if "allInviteAttemptsFailed" not in invite_output:
        return
    resolved_identity = {
        **team_mother_identity_from_team_auth_path(normalized_path),
        **(identity or {}),
    }
    if not any(str(resolved_identity.get(key) or "").strip() for key in ("original_name", "email", "account_id")):
        return
    state_path, payload = load_team_mother_availability_state(
        shared_root=shared_root,
        original_name=str(resolved_identity.get("original_name") or "").strip(),
        email=str(resolved_identity.get("email") or "").strip(),
        account_id=str(resolved_identity.get("account_id") or "").strip(),
    )
    normalized = prune_team_mother_availability_state(state_path=state_path, payload=payload)
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
    write_team_mother_availability_state(state_path=state_path, payload=normalized)


def team_auth_is_temp_blacklisted(
    *,
    shared_root: Path,
    team_auth_path: str,
) -> tuple[bool, dict[str, object]]:
    identity = team_mother_identity_from_team_auth_path(team_auth_path)
    if not any(str(identity.get(key) or "").strip() for key in ("original_name", "email", "account_id")):
        return False, {}
    state_path, payload = load_team_mother_availability_state(
        shared_root=shared_root,
        original_name=str(identity.get("original_name") or "").strip(),
        email=str(identity.get("email") or "").strip(),
        account_id=str(identity.get("account_id") or "").strip(),
    )
    normalized = prune_team_mother_availability_state(state_path=state_path, payload=payload)
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


def mark_team_auth_temporary_blacklist(
    *,
    shared_root: Path,
    team_auth_path: str,
    identity: dict[str, str] | None,
    reason: str,
    blacklist_seconds: float,
    worker_label: str,
    task_index: int,
) -> dict[str, object] | None:
    resolved_identity = {
        **team_mother_identity_from_team_auth_path(team_auth_path),
        **(identity or {}),
    }
    original_name = str(resolved_identity.get("original_name") or "").strip()
    email = str(resolved_identity.get("email") or "").strip()
    account_id = str(resolved_identity.get("account_id") or "").strip()
    if not any((original_name, email, account_id)):
        return None
    state_path, payload = load_team_mother_availability_state(
        shared_root=shared_root,
        original_name=original_name,
        email=email,
        account_id=account_id,
    )
    normalized = prune_team_mother_availability_state(state_path=state_path, payload=payload)
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
    write_team_mother_availability_state(state_path=state_path, payload=normalized)
    result: dict[str, object] = {
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
    json_log(
        {
            "event": "register_team_auth_temp_blacklist_marked",
            "workerId": worker_label,
            "taskIndex": task_index,
            **result,
        }
    )
    return result


def clear_team_auth_temporary_blacklist(
    *,
    shared_root: Path,
    team_auth_path: str,
    identity: dict[str, str] | None,
    worker_label: str,
    task_index: int,
) -> bool:
    resolved_identity = {
        **team_mother_identity_from_team_auth_path(team_auth_path),
        **(identity or {}),
    }
    original_name = str(resolved_identity.get("original_name") or "").strip()
    email = str(resolved_identity.get("email") or "").strip()
    account_id = str(resolved_identity.get("account_id") or "").strip()
    if not any((original_name, email, account_id)):
        return False
    state_path, payload = load_team_mother_availability_state(
        shared_root=shared_root,
        original_name=original_name,
        email=email,
        account_id=account_id,
    )
    normalized = prune_team_mother_availability_state(state_path=state_path, payload=payload)
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
    write_team_mother_availability_state(state_path=state_path, payload=normalized)
    json_log(
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


def prune_stale_team_auth_caches(
    *,
    shared_root: Path,
    active_team_auth_paths: list[str],
) -> dict[str, list[str]]:
    active_paths = {
        str(Path(candidate).resolve()).strip().lower()
        for candidate in active_team_auth_paths
        if str(candidate or "").strip()
    }
    active_identity_keys = team_auth_identity_keys_from_paths(active_team_auth_paths)
    reserved_identity_keys = team_mother_reserved_identity_keys_for_shared_root(shared_root=shared_root)
    allowed_identity_keys = active_identity_keys | reserved_identity_keys

    removed_state_paths: list[str] = []
    state_dir = team_auth_state_dir(shared_root=shared_root)
    if state_dir.is_dir():
        cleanup_state_name = team_cleanup_state_path(shared_root=shared_root).name.lower()
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
    cooldown_dir = team_mother_cooldowns_dir_for_shared_root(shared_root=shared_root)
    if cooldown_dir.is_dir():
        for state_path in cooldown_dir.glob("*.json"):
            if not state_path.is_file():
                continue
            payload = _load_json_dict(state_path)
            if not payload:
                state_path.unlink(missing_ok=True)
                removed_availability_paths.append(str(state_path))
                continue
            normalized = prune_team_mother_availability_state(state_path=state_path, payload=payload)
            if not normalized:
                removed_availability_paths.append(str(state_path))
                continue
            identity_key = team_mother_identity_key(
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
