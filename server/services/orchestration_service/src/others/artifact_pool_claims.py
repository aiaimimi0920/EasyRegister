from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any

if __package__ in (None, "", "others"):
    import sys

    _CURRENT_DIR = Path(__file__).resolve().parent
    _PARENT_DIR = _CURRENT_DIR.parent
    for _candidate in (_CURRENT_DIR, _PARENT_DIR):
        candidate_text = str(_candidate)
        if candidate_text not in sys.path:
            sys.path.append(candidate_text)
    from others.artifact_pool_common import (
        choose_random_files,
        extract_free_oauth_organizations,
        extract_free_oauth_plan_type,
        has_free_personal_oauth_claims,
        load_small_success_seed_validation,
        load_team_expand_progress_from_artifact,
        recover_stale_team_claims,
        reset_claimed_team_expand_cycle_payload,
        resolve_free_manual_oauth_pool,
        resolve_small_success_claims,
        resolve_small_success_pool,
        resolve_small_success_wait_pool,
        resolve_team_member_claims,
        resolve_team_mother_claims,
        resolve_team_mother_cooldowns,
        resolve_team_mother_pool,
        resolve_team_pre_pool,
        restore_to_pool,
        safe_count,
        sort_paths_newest_first,
        team_expand_progress_from_payload,
        team_expand_progress_is_in_progress,
        team_expand_target_count,
        team_mother_has_inflight_primary_usage,
        team_mother_is_cooling,
        team_stale_claim_seconds,
    )
    from others.common import (
        ensure_directory,
        extract_account_id,
        free_manual_oauth_preserve_codes,
        free_manual_oauth_preserve_enabled,
    )
    from others.storage import load_json_payload
else:
    from .artifact_pool_common import (
        choose_random_files,
        extract_free_oauth_organizations,
        extract_free_oauth_plan_type,
        has_free_personal_oauth_claims,
        load_small_success_seed_validation,
        load_team_expand_progress_from_artifact,
        recover_stale_team_claims,
        reset_claimed_team_expand_cycle_payload,
        resolve_free_manual_oauth_pool,
        resolve_small_success_claims,
        resolve_small_success_pool,
        resolve_small_success_wait_pool,
        resolve_team_member_claims,
        resolve_team_mother_claims,
        resolve_team_mother_cooldowns,
        resolve_team_mother_pool,
        resolve_team_pre_pool,
        restore_to_pool,
        safe_count,
        sort_paths_newest_first,
        team_expand_progress_from_payload,
        team_expand_progress_is_in_progress,
        team_expand_target_count,
        team_mother_has_inflight_primary_usage,
        team_mother_is_cooling,
        team_stale_claim_seconds,
    )
    from .common import (
        ensure_directory,
        extract_account_id,
        free_manual_oauth_preserve_codes,
        free_manual_oauth_preserve_enabled,
    )
    from .storage import load_json_payload


def sleep_seconds(*, step_input: dict[str, Any]) -> dict[str, Any]:
    try:
        seconds = max(0.0, float(step_input.get("seconds") or 0.0))
    except Exception:
        seconds = 0.0
    reason = str(step_input.get("reason") or "").strip()
    if seconds > 0:
        time.sleep(seconds)
    return {
        "ok": True,
        "status": "slept" if seconds > 0 else "skipped_zero_seconds",
        "slept_seconds": seconds,
        "reason": reason,
    }


def claim_small_success_artifact(*, step_input: dict[str, Any]) -> dict[str, Any]:
    pool_dir = resolve_small_success_pool(step_input)
    claims_dir = resolve_small_success_claims(step_input)
    ensure_directory(pool_dir)
    ensure_directory(claims_dir)

    for candidate in sort_paths_newest_first([path for path in pool_dir.glob("*.json") if path.is_file()]):
        claim_name = f"{uuid.uuid4().hex[:8]}-{candidate.name}"
        claimed_path = claims_dir / claim_name
        try:
            candidate.replace(claimed_path)
        except FileNotFoundError:
            continue
        valid, _, payload = load_small_success_seed_validation(claimed_path)
        if not valid:
            claimed_path.unlink(missing_ok=True)
            continue
        return {
            "ok": True,
            "source_path": str(claimed_path),
            "claimed_path": str(claimed_path),
            "pool_dir": str(pool_dir),
            "claims_dir": str(claims_dir),
            "original_name": candidate.name,
            "email": str(payload.get("email") or "").strip(),
        }

    raise RuntimeError("small_success_pool_empty")


def finalize_small_success_artifact(*, step_input: dict[str, Any]) -> dict[str, Any]:
    artifact = step_input.get("artifact")
    if not isinstance(artifact, dict):
        return {
            "ok": True,
            "status": "skipped_missing_artifact",
        }

    claimed_path_text = str(artifact.get("claimed_path") or artifact.get("source_path") or "").strip()
    if not claimed_path_text:
        return {
            "ok": True,
            "status": "skipped_missing_artifact",
        }

    claimed_path = Path(claimed_path_text).resolve()
    if not claimed_path.exists():
        return {
            "ok": True,
            "status": "skipped_missing_artifact",
        }

    task_error_code = str(step_input.get("task_error_code") or "").strip()
    failure_mode = str(step_input.get("failure_mode") or "").strip().lower()
    if (
        task_error_code
        and free_manual_oauth_preserve_enabled(step_input)
        and task_error_code in free_manual_oauth_preserve_codes(step_input)
    ):
        pool_dir = resolve_free_manual_oauth_pool(step_input)
        ensure_directory(pool_dir)
        restored_path = restore_to_pool(
            claimed_path=claimed_path,
            pool_dir=pool_dir,
            preferred_name=str(artifact.get("original_name") or claimed_path.name).strip() or claimed_path.name,
        )
        return {
            "ok": True,
            "status": "preserved_for_manual_oauth",
            "restored_path": restored_path,
            "claimed_path": str(claimed_path),
            "restore_pool_dir": str(pool_dir),
            "task_error_code": task_error_code,
            "email": str(artifact.get("email") or "").strip(),
        }
    if task_error_code == "free_personal_workspace_missing":
        pool_dir = resolve_small_success_wait_pool(step_input)
    else:
        pool_dir = resolve_small_success_pool(
            {
                "pool_dir": artifact.get("pool_dir"),
                "output_dir": step_input.get("output_dir"),
            }
        )
    ensure_directory(pool_dir)

    if not task_error_code:
        claimed_path.unlink(missing_ok=True)
        return {
            "ok": True,
            "status": "deleted",
            "claimed_path": str(claimed_path),
        }

    if failure_mode == "delete":
        claimed_path.unlink(missing_ok=True)
        return {
            "ok": True,
            "status": "deleted_failed_artifact",
            "claimed_path": str(claimed_path),
            "task_error_code": task_error_code,
        }

    restored_path = restore_to_pool(
        claimed_path=claimed_path,
        pool_dir=pool_dir,
        preferred_name=str(artifact.get("original_name") or claimed_path.name).strip() or claimed_path.name,
    )
    return {
        "ok": True,
        "status": "restored",
        "restored_path": restored_path,
        "claimed_path": str(claimed_path),
        "restore_pool_dir": str(pool_dir),
    }


def validate_free_personal_oauth(*, step_input: dict[str, Any]) -> dict[str, Any]:
    oauth_result = step_input.get("oauth_result")
    invite_result = step_input.get("invite_result")

    oauth_account_id = extract_account_id(oauth_result)
    team_account_id = str((invite_result or {}).get("team_account_id") or "").strip() if isinstance(invite_result, dict) else ""
    plan_type = extract_free_oauth_plan_type(oauth_result)
    organizations = extract_free_oauth_organizations(oauth_result)

    has_personal_claims = has_free_personal_oauth_claims(oauth_result)
    if has_personal_claims:
        return {
            "ok": True,
            "status": "personal_oauth_confirmed",
            "oauth_account_id": oauth_account_id,
            "team_account_id": team_account_id,
            "validation_mode": "claim_based",
            "chatgpt_plan_type": plan_type,
            "organizations": organizations,
        }

    return {
        "ok": False,
        "status": "free_personal_workspace_missing",
        "code": "free_personal_workspace_missing",
        "detail": "free_personal_workspace_missing",
        "oauth_account_id": oauth_account_id,
        "team_account_id": team_account_id,
        "validation_mode": "missing_personal_claims",
        "chatgpt_plan_type": plan_type,
        "organizations": organizations,
    }


def fill_team_pre_pool(*, step_input: dict[str, Any]) -> dict[str, Any]:
    source_pool_dir = resolve_small_success_pool(step_input)
    team_pre_pool_dir = resolve_team_pre_pool(step_input)
    ensure_directory(source_pool_dir)
    ensure_directory(team_pre_pool_dir)

    max_move_count = safe_count(step_input.get("max_move_count") or 1, 1) or 1
    selected_paths = choose_random_files(directory=source_pool_dir, pattern="*.json", limit=max_move_count)
    moved: list[dict[str, Any]] = []
    discarded: list[dict[str, Any]] = []
    for candidate in selected_paths:
        valid, reason, payload = load_small_success_seed_validation(candidate)
        if not valid:
            candidate.unlink(missing_ok=True)
            discarded.append(
                {
                    "source_path": str(candidate),
                    "reason": reason,
                }
            )
            continue
        destination = team_pre_pool_dir / candidate.name
        if destination.exists():
            destination = team_pre_pool_dir / f"{candidate.stem}-{uuid.uuid4().hex[:6]}{candidate.suffix}"
        try:
            candidate.replace(destination)
        except FileNotFoundError:
            continue
        moved.append(
            {
                "source_path": str(candidate),
                "destination_path": str(destination),
                "email": str(payload.get("email") or "").strip(),
                "name": destination.name,
            }
        )

    return {
        "ok": True,
        "status": "moved" if moved else "idle",
        "moved_count": len(moved),
        "moved": moved,
        "discarded_count": len(discarded),
        "discarded": discarded,
        "source_pool_dir": str(source_pool_dir),
        "team_pre_pool_dir": str(team_pre_pool_dir),
    }


def claim_team_mother_artifact(*, step_input: dict[str, Any]) -> dict[str, Any]:
    pool_dir = resolve_team_mother_pool(step_input)
    claims_dir = resolve_team_mother_claims(step_input)
    cooldown_dir = resolve_team_mother_cooldowns(step_input)
    ensure_directory(pool_dir)
    ensure_directory(claims_dir)
    ensure_directory(cooldown_dir)
    recovered = recover_stale_team_claims(
        pool_dir=pool_dir,
        claims_dir=claims_dir,
        stale_after_seconds=team_stale_claim_seconds(),
    )
    cooling: list[dict[str, Any]] = []
    busy: list[dict[str, Any]] = []

    target_count = team_expand_target_count(step_input)
    in_progress_candidates: list[tuple[Path, dict[str, Any], dict[str, Any]]] = []
    other_candidates: list[tuple[Path, dict[str, Any], dict[str, Any]]] = []
    for candidate in sort_paths_newest_first([path for path in pool_dir.glob("*.json") if path.is_file()]):
        payload = load_json_payload(candidate)
        progress = team_expand_progress_from_payload(payload, fallback_target=target_count)
        if team_expand_progress_is_in_progress(progress):
            in_progress_candidates.append((candidate, payload, progress))
        else:
            other_candidates.append((candidate, payload, progress))

    for candidate, payload, _ in [*in_progress_candidates, *other_candidates]:
        is_cooling, cooling_state = team_mother_is_cooling(
            cooldown_dir=cooldown_dir,
            candidate=candidate,
            payload=payload,
        )
        if is_cooling:
            cooling.append(cooling_state)
            continue
        is_busy, busy_state = team_mother_has_inflight_primary_usage(
            cooldown_dir=cooldown_dir,
            candidate=candidate,
            payload=payload,
        )
        if is_busy:
            busy.append(busy_state)
            continue
        claim_name = f"{uuid.uuid4().hex[:8]}-{candidate.name}"
        claimed_path = claims_dir / claim_name
        try:
            candidate.replace(claimed_path)
        except FileNotFoundError:
            continue
        claimed_payload, progress_reset = reset_claimed_team_expand_cycle_payload(
            payload,
            target_count=target_count,
            reason="claimed_from_team_mother_pool",
        )
        if progress_reset:
            claimed_path.write_text(json.dumps(claimed_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return {
            "ok": True,
            "status": "claimed",
            "source_path": str(claimed_path),
            "claimed_path": str(claimed_path),
            "pool_dir": str(pool_dir),
            "claims_dir": str(claims_dir),
            "cooldowns_dir": str(cooldown_dir),
            "original_name": candidate.name,
            "recovered_claims": recovered,
            "cooling": cooling,
            "busy": busy,
            "email": str(payload.get("email") or "").strip(),
            "password": str(payload.get("password") or "").strip(),
            "account_id": str(
                payload.get("account_id")
                or ((payload.get("https://api.openai.com/auth") or {}).get("chatgpt_account_id"))
                or ""
            ).strip(),
            "mother_progress": team_expand_progress_from_payload(claimed_payload, fallback_target=target_count),
            "progress_reset": progress_reset,
        }

    return {
        "ok": True,
        "status": "idle",
        "source_path": "",
        "claimed_path": "",
        "pool_dir": str(pool_dir),
        "claims_dir": str(claims_dir),
        "cooldowns_dir": str(cooldown_dir),
        "recovered_claims": recovered,
        "cooling": cooling,
        "busy": busy,
        "original_name": "",
        "email": "",
        "password": "",
        "account_id": "",
    }


def claim_team_member_candidates(*, step_input: dict[str, Any]) -> dict[str, Any]:
    team_pre_pool_dir = resolve_team_pre_pool(step_input)
    claims_dir = resolve_team_member_claims(step_input)
    ensure_directory(team_pre_pool_dir)
    ensure_directory(claims_dir)
    recovered = recover_stale_team_claims(
        pool_dir=team_pre_pool_dir,
        claims_dir=claims_dir,
        stale_after_seconds=team_stale_claim_seconds(),
    )

    requested_member_count = safe_count(step_input.get("member_count") or 4, 4) or 4
    mother_progress = load_team_expand_progress_from_artifact(
        step_input.get("mother_artifact"),
        fallback_target=requested_member_count,
    )
    remaining_member_count = max(0, int(mother_progress.get("remainingCount") or 0))
    member_count = min(requested_member_count, remaining_member_count) if remaining_member_count > 0 else 0
    if member_count <= 0:
        return {
            "ok": True,
            "status": "target_already_satisfied",
            "member_count": 0,
            "requested_member_count": requested_member_count,
            "remaining_member_count": remaining_member_count,
            "members": [],
            "team_pre_pool_dir": str(team_pre_pool_dir),
            "claims_dir": str(claims_dir),
            "recovered_claims": recovered,
            "member_emails": [],
            "mother_progress": mother_progress,
        }

    selected_paths = choose_random_files(directory=team_pre_pool_dir, pattern="*.json", limit=member_count)
    if len(selected_paths) < member_count:
        raise RuntimeError("team_pre_pool_insufficient_members")

    claimed_members: list[dict[str, Any]] = []
    for candidate in selected_paths:
        claim_name = f"{uuid.uuid4().hex[:8]}-{candidate.name}"
        claimed_path = claims_dir / claim_name
        try:
            candidate.replace(claimed_path)
        except FileNotFoundError:
            continue
        payload = load_json_payload(claimed_path)
        claimed_members.append(
            {
                "source_path": str(claimed_path),
                "claimed_path": str(claimed_path),
                "pool_dir": str(team_pre_pool_dir),
                "claims_dir": str(claims_dir),
                "original_name": candidate.name,
                "email": str(payload.get("email") or "").strip(),
                "password": str(payload.get("password") or "").strip(),
            }
        )

    if len(claimed_members) < member_count:
        for artifact in claimed_members:
            claimed_path = Path(str(artifact.get("claimed_path") or "")).resolve()
            if claimed_path.exists():
                restore_to_pool(
                    claimed_path=claimed_path,
                    pool_dir=team_pre_pool_dir,
                    preferred_name=str(artifact.get("original_name") or claimed_path.name).strip() or claimed_path.name,
                )
        raise RuntimeError("team_pre_pool_claim_race")

    return {
        "ok": True,
        "status": "claimed",
        "member_count": len(claimed_members),
        "requested_member_count": requested_member_count,
        "remaining_member_count": remaining_member_count,
        "members": claimed_members,
        "team_pre_pool_dir": str(team_pre_pool_dir),
        "claims_dir": str(claims_dir),
        "recovered_claims": recovered,
        "member_emails": [str(item.get("email") or "").strip() for item in claimed_members],
        "mother_progress": mother_progress,
    }
