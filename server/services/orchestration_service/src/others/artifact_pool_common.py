from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from others.common import (
    extract_auth_claims,
    team_mother_cooldown_key,
    validate_small_success_seed_payload,
)
from others.paths import (
    resolve_free_manual_oauth_pool_dir,
    resolve_shared_root,
    resolve_small_success_claims_dir,
    resolve_small_success_continue_pool_dir,
    resolve_small_success_pool_dir,
    resolve_small_success_wait_pool_dir,
    resolve_team_member_claims_dir,
    resolve_team_mother_claims_dir,
    resolve_team_mother_cooldowns_dir,
    resolve_team_mother_pool_dir,
    resolve_team_pool_dir,
    resolve_team_post_pool_dir,
    resolve_team_pre_pool_dir,
)
from others.storage import load_json_payload


def derive_output_root_from_run_dir(output_dir: str | None) -> Path:
    if str(output_dir or "").strip():
        run_dir = Path(str(output_dir)).resolve()
        if run_dir.name.startswith("run-") and run_dir.parent.name.startswith("worker-"):
            return resolve_shared_root(str(run_dir.parents[1]))
        if run_dir.name.startswith("run-"):
            return resolve_shared_root(str(run_dir.parent))
        return resolve_shared_root(str(run_dir))
    return resolve_shared_root(str(Path.cwd()))


def resolve_small_success_pool(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("pool_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return resolve_small_success_pool_dir(str(derive_output_root_from_run_dir(step_input.get("output_dir"))))


def resolve_small_success_claims(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("claims_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return resolve_small_success_claims_dir(str(derive_output_root_from_run_dir(step_input.get("output_dir"))))


def resolve_small_success_wait_pool(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("wait_pool_dir") or step_input.get("small_success_wait_pool_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    env_explicit = str(os.environ.get("REGISTER_SMALL_SUCCESS_WAIT_POOL_DIR") or "").strip()
    if env_explicit:
        return Path(env_explicit).resolve()
    return resolve_small_success_wait_pool_dir(str(derive_output_root_from_run_dir(step_input.get("output_dir"))))


def resolve_small_success_continue_pool(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("continue_pool_dir") or step_input.get("small_success_continue_pool_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    env_explicit = str(os.environ.get("REGISTER_SMALL_SUCCESS_CONTINUE_POOL_DIR") or "").strip()
    if env_explicit:
        return Path(env_explicit).resolve()
    return resolve_small_success_continue_pool_dir(str(derive_output_root_from_run_dir(step_input.get("output_dir"))))


def resolve_free_manual_oauth_pool(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("free_manual_oauth_pool_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    env_explicit = str(os.environ.get("REGISTER_FREE_MANUAL_OAUTH_POOL_DIR") or "").strip()
    if env_explicit:
        return Path(env_explicit).resolve()
    return resolve_free_manual_oauth_pool_dir(str(derive_output_root_from_run_dir(step_input.get("output_dir"))))


def resolve_team_pre_pool(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("team_pre_pool_dir") or step_input.get("pool_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return resolve_team_pre_pool_dir(str(derive_output_root_from_run_dir(step_input.get("output_dir"))))


def resolve_team_mother_pool(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("team_mother_pool_dir") or step_input.get("pool_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return resolve_team_mother_pool_dir(str(derive_output_root_from_run_dir(step_input.get("output_dir"))))


def resolve_team_mother_claims(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("team_mother_claims_dir") or step_input.get("claims_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return resolve_team_mother_claims_dir(str(derive_output_root_from_run_dir(step_input.get("output_dir"))))


def resolve_team_mother_cooldowns(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("team_mother_cooldowns_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return resolve_team_mother_cooldowns_dir(str(derive_output_root_from_run_dir(step_input.get("output_dir"))))


def resolve_team_member_claims(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("team_member_claims_dir") or step_input.get("claims_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return resolve_team_member_claims_dir(str(derive_output_root_from_run_dir(step_input.get("output_dir"))))


def resolve_team_post_pool(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("team_post_pool_dir") or step_input.get("pool_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return resolve_team_post_pool_dir(str(derive_output_root_from_run_dir(step_input.get("output_dir"))))


def resolve_team_pool(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("team_pool_dir") or step_input.get("pool_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return resolve_team_pool_dir(str(derive_output_root_from_run_dir(step_input.get("output_dir"))))


def load_small_success_seed_validation(path: Path) -> tuple[bool, str, dict[str, Any]]:
    try:
        payload = load_json_payload(path)
    except Exception as exc:
        return False, f"load_failed:{exc}", {}
    ok, reason = validate_small_success_seed_payload(payload)
    return ok, reason, payload


def restore_to_pool(*, claimed_path: Path, pool_dir: Path, preferred_name: str) -> str:
    destination = pool_dir / preferred_name
    if destination.exists():
        destination = pool_dir / f"{destination.stem}-{uuid.uuid4().hex[:6]}{destination.suffix}"
    claimed_path.replace(destination)
    return str(destination)


def derive_original_name_from_claim(path: Path) -> str:
    name = path.name
    prefix, separator, remainder = name.partition("-")
    if separator and len(prefix) == 8:
        return remainder
    return name


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
    return safe_count(os.environ.get("REGISTER_TEAM_STALE_CLAIM_SECONDS") or 60, 60)


def sort_paths_newest_first(paths: list[Path]) -> list[Path]:
    def _sort_key(path: Path) -> tuple[float, str]:
        try:
            modified_at = float(path.stat().st_mtime)
        except FileNotFoundError:
            modified_at = 0.0
        return (-modified_at, path.name.lower())

    return sorted(paths, key=_sort_key)


def team_mother_cooldown_path(
    *,
    cooldown_dir: Path,
    original_name: str,
    email: str,
    account_id: str,
) -> Path:
    return cooldown_dir / f"{team_mother_cooldown_key(original_name=original_name, email=email, account_id=account_id)}.json"


def team_mother_cooldown_state(
    *,
    cooldown_dir: Path,
    original_name: str,
    email: str,
    account_id: str,
) -> dict[str, Any]:
    state_path = team_mother_cooldown_path(
        cooldown_dir=cooldown_dir,
        original_name=original_name,
        email=email,
        account_id=account_id,
    )
    if not state_path.exists():
        return {}
    try:
        state = load_json_payload(state_path)
    except Exception:
        return {}
    return state if isinstance(state, dict) else {}


def team_mother_availability_state_prune(
    *,
    state_path: Path,
    state: dict[str, Any],
) -> dict[str, Any]:
    if not state:
        return {}
    normalized = dict(state)
    changed = False
    now_ts = time.time()

    def _drop_window(prefix: str) -> None:
        nonlocal changed
        try:
            until_ts = float(normalized.get(f"{prefix}_until_ts") or 0.0)
        except Exception:
            until_ts = 0.0
        if until_ts <= 0 or until_ts > now_ts:
            return
        for key in (
            "reason" if prefix == "cooldown" else None,
            "cooldown_seconds",
            "cooldown_started_at",
            "cooldown_until",
            "cooldown_until_ts",
            "blacklist_reason",
            "blacklist_seconds",
            "blacklist_started_at",
            "blacklist_until",
            "blacklist_until_ts",
        ):
            if key is None:
                continue
            if prefix == "cooldown" and key.startswith("blacklist_"):
                continue
            if prefix == "blacklist" and key.startswith("cooldown_"):
                continue
            if key in normalized:
                normalized.pop(key, None)
                changed = True

    _drop_window("cooldown")
    _drop_window("blacklist")

    seat_allocations = normalized.get("seat_allocations")
    if isinstance(seat_allocations, list):
        filtered_allocations: list[dict[str, Any]] = []
        for item in seat_allocations:
            if not isinstance(item, dict):
                changed = True
                continue
            status = str(item.get("status") or "").strip().lower()
            try:
                pending_until_ts = float(item.get("pending_until_ts") or 0.0)
            except Exception:
                pending_until_ts = 0.0
            if status == "pending" and pending_until_ts > 0.0 and pending_until_ts <= now_ts:
                changed = True
                continue
            filtered_allocations.append(item)
        if filtered_allocations != seat_allocations:
            normalized["seat_allocations"] = filtered_allocations
            changed = True
        if not filtered_allocations and "seat_allocations" in normalized:
            normalized.pop("seat_allocations", None)
            changed = True

    recent_invite_results = normalized.get("recent_invite_results")
    if not isinstance(recent_invite_results, list) and "recent_invite_results" in normalized:
        normalized.pop("recent_invite_results", None)
        changed = True
    recent_team_expand_results = normalized.get("recent_team_expand_results")
    if not isinstance(recent_team_expand_results, list) and "recent_team_expand_results" in normalized:
        normalized.pop("recent_team_expand_results", None)
        changed = True

    has_active_window = any(
        float(normalized.get(key) or 0.0) > now_ts
        for key in ("cooldown_until_ts", "blacklist_until_ts")
        if str(normalized.get(key) or "").strip()
    )
    has_recent_history = bool(normalized.get("recent_invite_results")) or bool(normalized.get("recent_team_expand_results"))
    has_seat_allocations = bool(normalized.get("seat_allocations"))
    if not has_active_window and not has_recent_history and not has_seat_allocations:
        state_path.unlink(missing_ok=True)
        return {}
    if changed:
        state_path.write_text(json.dumps(normalized, ensure_ascii=False, indent=2), encoding="utf-8")
    return normalized


def _team_mother_identity_from_payload(candidate: Path, payload: dict[str, Any]) -> dict[str, str]:
    return {
        "email": str(payload.get("email") or "").strip(),
        "account_id": str(
            payload.get("account_id")
            or ((payload.get("https://api.openai.com/auth") or {}).get("chatgpt_account_id"))
            or ""
        ).strip(),
        "original_name": candidate.name,
    }


def team_mother_is_cooling(
    *,
    cooldown_dir: Path,
    candidate: Path,
    payload: dict[str, Any],
) -> tuple[bool, dict[str, Any]]:
    identity = _team_mother_identity_from_payload(candidate, payload)
    state_path = team_mother_cooldown_path(
        cooldown_dir=cooldown_dir,
        original_name=identity["original_name"],
        email=identity["email"],
        account_id=identity["account_id"],
    )
    state = team_mother_cooldown_state(
        cooldown_dir=cooldown_dir,
        original_name=identity["original_name"],
        email=identity["email"],
        account_id=identity["account_id"],
    )
    if not state:
        return False, {}
    state = team_mother_availability_state_prune(state_path=state_path, state=state)
    if not state:
        return False, {}
    try:
        until_ts = float(state.get("cooldown_until_ts") or 0.0)
    except Exception:
        until_ts = 0.0
    now = time.time()
    if until_ts > now:
        return True, {
            "state_path": str(state_path),
            "cooldown_until": str(state.get("cooldown_until") or "").strip(),
            "cooldown_until_ts": until_ts,
            "remaining_seconds": round(max(0.0, until_ts - now), 3),
            "reason": str(state.get("reason") or "").strip(),
            **identity,
            "window": "cooldown",
        }
    try:
        blacklist_until_ts = float(state.get("blacklist_until_ts") or 0.0)
    except Exception:
        blacklist_until_ts = 0.0
    if blacklist_until_ts > now:
        return True, {
            "state_path": str(state_path),
            "blacklist_until": str(state.get("blacklist_until") or "").strip(),
            "blacklist_until_ts": blacklist_until_ts,
            "remaining_seconds": round(max(0.0, blacklist_until_ts - now), 3),
            "reason": str(state.get("blacklist_reason") or "").strip(),
            **identity,
            "window": "blacklist",
        }
    state_path.unlink(missing_ok=True)
    return False, {}


def team_mother_has_inflight_primary_usage(
    *,
    cooldown_dir: Path,
    candidate: Path,
    payload: dict[str, Any],
) -> tuple[bool, dict[str, Any]]:
    identity = _team_mother_identity_from_payload(candidate, payload)
    state = team_mother_cooldown_state(
        cooldown_dir=cooldown_dir,
        original_name=identity["original_name"],
        email=identity["email"],
        account_id=identity["account_id"],
    )
    state_path = team_mother_cooldown_path(
        cooldown_dir=cooldown_dir,
        original_name=identity["original_name"],
        email=identity["email"],
        account_id=identity["account_id"],
    )
    if not state:
        return False, {}
    state = team_mother_availability_state_prune(state_path=state_path, state=state)
    if not state:
        return False, {}
    inflight: list[dict[str, Any]] = []
    now_ts = time.time()
    for item in state.get("seat_allocations") or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("status") or "").strip().lower() != "pending":
            continue
        source_role = str(item.get("source_role") or "").strip().lower()
        if source_role not in {"main", "continue"}:
            continue
        try:
            pending_until_ts = float(item.get("pending_until_ts") or 0.0)
        except Exception:
            pending_until_ts = 0.0
        if pending_until_ts > 0.0 and pending_until_ts <= now_ts:
            continue
        inflight.append(
            {
                "source_role": source_role,
                "reservation_owner": str(item.get("reservation_owner") or "").strip(),
                "reservation_context": str(item.get("reservation_context") or "").strip(),
                "pending_until": str(item.get("pending_until") or "").strip(),
                "invite_email": str(item.get("invite_email") or "").strip(),
            }
        )
    if not inflight:
        return False, {}
    return True, {
        "state_path": str(state_path),
        **identity,
        "inflight": inflight,
        "pending_count": len(inflight),
    }


def choose_random_files(*, directory: Path, pattern: str, limit: int) -> list[Path]:
    candidates = [path for path in directory.glob(pattern) if path.is_file()]
    if not candidates or limit <= 0:
        return []
    return sort_paths_newest_first(candidates)[:limit]


def extract_free_oauth_plan_type(payload: Any) -> str:
    auth_claims = extract_auth_claims(payload)
    if auth_claims:
        plan_type = str(auth_claims.get("chatgpt_plan_type") or "").strip()
        if plan_type:
            return plan_type
    if isinstance(payload, dict):
        direct = str(payload.get("chatgpt_plan_type") or "").strip()
        if direct:
            return direct
        auth_payload = payload.get("auth")
        if isinstance(auth_payload, dict):
            nested_direct = str(auth_payload.get("chatgpt_plan_type") or "").strip()
            if nested_direct:
                return nested_direct
    return ""


def extract_free_oauth_organizations(payload: Any) -> list[dict[str, Any]]:
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


def has_free_personal_oauth_claims(payload: Any) -> bool:
    plan_type = extract_free_oauth_plan_type(payload).strip().lower()
    if plan_type != "free":
        return False
    organizations = extract_free_oauth_organizations(payload)
    for organization in organizations:
        title = str(organization.get("title") or "").strip().lower()
        role = str(organization.get("role") or "").strip().lower()
        is_default = organization.get("is_default")
        if title == "personal" and role == "owner":
            return True
        if title == "personal" and is_default is True:
            return True
    return False


def team_expand_target_count(step_input: dict[str, Any] | None = None, default: int = 4) -> int:
    candidate = ""
    if isinstance(step_input, dict):
        candidate = str(step_input.get("member_count") or "").strip()
    if not candidate:
        candidate = str(os.environ.get("REGISTER_TEAM_MEMBER_COUNT") or default).strip()
    try:
        return max(1, int(candidate or default))
    except Exception:
        return max(1, int(default))


def team_expand_progress_from_payload(payload: Any, *, fallback_target: int) -> dict[str, Any]:
    progress = {}
    if isinstance(payload, dict):
        team_flow = payload.get("teamFlow")
        if isinstance(team_flow, dict):
            raw_progress = team_flow.get("teamExpandProgress")
            if isinstance(raw_progress, dict):
                progress = dict(raw_progress)
    target_count = max(1, safe_count(progress.get("targetCount") or fallback_target, fallback_target))
    emails: list[str] = []
    raw_emails = progress.get("successfulMemberEmails")
    if isinstance(raw_emails, list):
        for item in raw_emails:
            email = str(item or "").strip().lower()
            if email and email not in emails:
                emails.append(email)
    success_count = max(len(emails), safe_count(progress.get("successCount") or len(emails), len(emails)))
    remaining_count = max(0, target_count - success_count)
    ready = bool(progress.get("readyForMotherCollection")) or success_count >= target_count
    return {
        "targetCount": target_count,
        "successfulMemberEmails": emails,
        "successCount": success_count,
        "remainingCount": remaining_count,
        "readyForMotherCollection": ready,
    }


def team_expand_progress_is_in_progress(progress: Any) -> bool:
    if not isinstance(progress, dict):
        return False
    target_count = max(1, safe_count(progress.get("targetCount") or 4, 4))
    success_count = max(0, safe_count(progress.get("successCount") or 0, 0))
    ready = bool(progress.get("readyForMotherCollection")) or success_count >= target_count
    return success_count > 0 and not ready


def team_expand_progress_is_completed(progress: Any) -> bool:
    if not isinstance(progress, dict):
        return False
    target_count = max(1, safe_count(progress.get("targetCount") or 4, 4))
    success_count = max(0, safe_count(progress.get("successCount") or 0, 0))
    return bool(progress.get("readyForMotherCollection")) or success_count >= target_count


def reset_claimed_team_expand_cycle_payload(
    payload: dict[str, Any],
    *,
    target_count: int,
    reason: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    normalized_target_count = max(1, int(target_count or 4))
    team_flow = dict(payload.get("teamFlow") or {})
    previous_progress = team_expand_progress_from_payload(payload, fallback_target=normalized_target_count)
    previous_raw_progress = team_flow.get("teamExpandProgress")
    previous_progress_payload = (
        dict(previous_raw_progress)
        if isinstance(previous_raw_progress, dict)
        else {
            **previous_progress,
            "successfulArtifacts": [],
        }
    )
    has_previous_cycle_state = (
        bool(previous_progress.get("successCount"))
        or bool(previous_progress.get("successfulMemberEmails"))
        or bool(team_flow.get("memberInviteBatch"))
        or bool(team_flow.get("teamSeatCleanup"))
        or bool(team_flow.get("collectedArtifacts"))
    )
    if has_previous_cycle_state:
        team_flow["previousClaimedTeamExpandProgress"] = previous_progress_payload
    team_flow["teamExpandProgress"] = {
        "targetCount": normalized_target_count,
        "successfulMemberEmails": [],
        "successfulArtifacts": [],
        "successCount": 0,
        "remainingCount": normalized_target_count,
        "readyForMotherCollection": False,
        "resetAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "resetReason": reason,
        "previousCycleSuccessCount": int(previous_progress.get("successCount") or 0),
        "previousCycleSuccessfulMemberEmails": list(previous_progress.get("successfulMemberEmails") or []),
    }
    for stale_key in (
        "teamSeatCleanup",
        "memberInviteBatch",
        "collectedArtifacts",
    ):
        team_flow.pop(stale_key, None)
    updated_payload = {
        **payload,
        "teamFlow": team_flow,
    }
    reset_summary = {
        "reset": has_previous_cycle_state,
        "reason": reason,
        "previous_success_count": int(previous_progress.get("successCount") or 0),
        "new_target_count": normalized_target_count,
    }
    return updated_payload, reset_summary


def load_team_expand_progress_from_artifact(
    artifact: Any,
    *,
    fallback_target: int,
) -> dict[str, Any]:
    if not isinstance(artifact, dict):
        return team_expand_progress_from_payload({}, fallback_target=fallback_target)
    source_path_text = str(artifact.get("source_path") or artifact.get("claimed_path") or "").strip()
    if not source_path_text:
        return team_expand_progress_from_payload({}, fallback_target=fallback_target)
    source_path = Path(source_path_text).resolve()
    if not source_path.exists():
        return team_expand_progress_from_payload({}, fallback_target=fallback_target)
    try:
        payload = load_json_payload(source_path)
    except Exception:
        payload = {}
    return team_expand_progress_from_payload(payload, fallback_target=fallback_target)


def path_is_inside_directory(*, path: Path, directory: Path) -> bool:
    try:
        resolved_path = path.resolve()
        resolved_dir = directory.resolve()
    except Exception:
        return False
    try:
        resolved_path.relative_to(resolved_dir)
        return True
    except Exception:
        return False
