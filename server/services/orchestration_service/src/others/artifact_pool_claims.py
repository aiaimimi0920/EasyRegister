from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any

from errors import ErrorCodes
from others.artifact_pool_common import (
    artifact_routing_config_for_step_input,
    choose_random_files,
    derive_output_root_from_run_dir,
    extract_free_oauth_organizations,
    extract_free_oauth_plan_type,
    has_free_personal_oauth_claims,
    load_openai_oauth_seed_validation,
    load_team_expand_progress_from_artifact,
    recover_stale_team_claims,
    reset_claimed_team_expand_cycle_payload,
    resolve_free_manual_oauth_pool,
    resolve_openai_oauth_claims,
    resolve_openai_oauth_continue_pool,
    resolve_openai_oauth_pool,
    resolve_openai_oauth_need_phone_pool,
    resolve_openai_oauth_success_pool,
    resolve_openai_oauth_wait_pool,
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
    extract_email,
    free_manual_oauth_preserve_codes,
    free_manual_oauth_preserve_enabled,
    json_log,
)
from others.openai_oauth_conversion_guard import (
    acquire_conversion_lock,
    codex_success_lookup,
    conversion_lock_path,
    prune_stale_conversion_lock,
    release_conversion_lock,
)
from others.runner_openai_oauth import route_openai_oauth_artifact
from others.storage import load_json_payload


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


def _resolve_configured_input_source_dir(step_input: dict[str, Any]) -> Path:
    source_dir_text = str(
        step_input.get("input_source_dir")
        or step_input.get("inputSourceDir")
        or ""
    ).strip()
    if not source_dir_text:
        raise RuntimeError("configured_input_source_dir_missing")
    return Path(source_dir_text).expanduser().resolve()


def _resolve_configured_input_claims_dir(*, step_input: dict[str, Any], source_dir: Path) -> Path:
    claims_dir_text = str(
        step_input.get("input_claims_dir")
        or step_input.get("inputClaimsDir")
        or ""
    ).strip()
    if claims_dir_text:
        return Path(claims_dir_text).expanduser().resolve()
    return (source_dir / "_claims").resolve()


def _extract_configured_input_email(payload: dict[str, Any]) -> str:
    direct_email = str(payload.get("email") or "").strip()
    if direct_email:
        return direct_email
    extracted_email = str(extract_email(payload) or "").strip()
    if extracted_email:
        return extracted_email
    profile_claims = payload.get("https://api.openai.com/profile")
    if isinstance(profile_claims, dict):
        return str(profile_claims.get("email") or "").strip()
    return ""


def claim_configured_input_file(*, step_input: dict[str, Any]) -> dict[str, Any]:
    source_dir = _resolve_configured_input_source_dir(step_input)
    claims_dir = _resolve_configured_input_claims_dir(step_input=step_input, source_dir=source_dir)
    ensure_directory(source_dir)
    ensure_directory(claims_dir)

    invalid_candidates: list[dict[str, Any]] = []
    missing_email_candidates: list[str] = []
    for candidate in sort_paths_newest_first([path for path in source_dir.glob("*.json") if path.is_file()]):
        try:
            payload = load_json_payload(candidate)
        except Exception as exc:
            invalid_candidates.append(
                {
                    "source_path": str(candidate),
                    "reason": f"load_failed:{exc}",
                }
            )
            continue
        email = _extract_configured_input_email(payload)
        if not email:
            missing_email_candidates.append(str(candidate))
            continue
        claim_name = f"{uuid.uuid4().hex[:8]}-{candidate.name}"
        claimed_path = claims_dir / claim_name
        try:
            candidate.replace(claimed_path)
        except FileNotFoundError:
            continue
        return {
            "ok": True,
            "status": "claimed",
            "source_path": str(claimed_path),
            "claimed_path": str(claimed_path),
            "input_source_dir": str(source_dir),
            "input_claims_dir": str(claims_dir),
            "original_name": candidate.name,
            "email": email,
            "mailbox_ref": str(payload.get("mailbox_ref") or payload.get("mailboxRef") or "").strip(),
            "session_id": str(payload.get("session_id") or payload.get("sessionId") or "").strip(),
            "account_id": str(
                payload.get("account_id")
                or payload.get("accountId")
                or extract_account_id(payload)
                or ""
            ).strip(),
            "payload": payload,
        }

    if invalid_candidates or missing_email_candidates:
        json_log(
            {
                "event": "configured_input_candidates_skipped",
                "inputSourceDir": str(source_dir),
                "invalidCandidateCount": len(invalid_candidates),
                "missingEmailCandidateCount": len(missing_email_candidates),
                "invalidCandidates": invalid_candidates,
                "missingEmailCandidates": missing_email_candidates,
            }
        )
    raise RuntimeError("configured_input_pool_empty")


def claim_openai_oauth_artifact(*, step_input: dict[str, Any]) -> dict[str, Any]:
    shared_root = derive_output_root_from_run_dir(step_input.get("output_dir"))
    pool_dir = resolve_openai_oauth_pool(step_input)
    claims_dir = resolve_openai_oauth_claims(step_input)
    ensure_directory(pool_dir)
    ensure_directory(claims_dir)
    skipped_existing_codex: list[dict[str, Any]] = []
    skipped_locked: list[dict[str, Any]] = []

    for candidate in sort_paths_newest_first([path for path in pool_dir.glob("*.json") if path.is_file()]):
        valid, _, payload = load_openai_oauth_seed_validation(candidate)
        if not valid:
            candidate.unlink(missing_ok=True)
            continue
        email = str(payload.get("email") or "").strip()
        existing_codex = codex_success_lookup(
            shared_root=shared_root,
            output_root=shared_root,
            email=email,
        )
        if bool(existing_codex.get("exists")):
            skipped_existing_codex.append(
                {
                    "source_path": str(candidate),
                    "email": email,
                    "matches": existing_codex.get("matches") or [],
                }
            )
            continue
        prune_stale_conversion_lock(shared_root=shared_root, email=email)
        email_lock_path = conversion_lock_path(shared_root=shared_root, email=email)
        if email and email_lock_path.is_file():
            skipped_locked.append(
                {
                    "source_path": str(candidate),
                    "email": email,
                    "lock_path": str(email_lock_path),
                }
            )
            continue
        claim_name = f"{uuid.uuid4().hex[:8]}-{candidate.name}"
        claimed_path = claims_dir / claim_name
        try:
            candidate.replace(claimed_path)
        except FileNotFoundError:
            continue
        conversion_claim = acquire_conversion_lock(
            shared_root=shared_root,
            email=email,
            claimed_path=claimed_path,
            source_path=candidate,
            stage="continue",
            worker_label=str(step_input.get("worker_label") or "").strip(),
            task_index=int(step_input.get("task_index") or 0),
        )
        if email and conversion_claim is None:
            restore_to_pool(
                claimed_path=claimed_path,
                pool_dir=pool_dir,
                preferred_name=candidate.name,
            )
            skipped_locked.append(
                {
                    "source_path": str(candidate),
                    "email": email,
                    "lock_path": str(email_lock_path),
                }
            )
            continue
        return {
            "ok": True,
            "source_path": str(claimed_path),
            "claimed_path": str(claimed_path),
            "pool_dir": str(pool_dir),
            "claims_dir": str(claims_dir),
            "original_name": candidate.name,
            "email": email,
            "conversion_claim": conversion_claim or {},
        }

    if skipped_existing_codex or skipped_locked:
        json_log(
            {
                "event": "register_openai_oauth_candidates_skipped",
                "workerId": str(step_input.get("worker_label") or "").strip(),
                "taskIndex": int(step_input.get("task_index") or 0),
                "poolDir": str(pool_dir),
                "skippedExistingCodexCount": len(skipped_existing_codex),
                "skippedLockedCount": len(skipped_locked),
                "skippedExistingCodex": skipped_existing_codex,
                "skippedLocked": skipped_locked,
            }
        )
    raise RuntimeError("openai_oauth_pool_empty")


def finalize_openai_oauth_artifact(*, step_input: dict[str, Any]) -> dict[str, Any]:
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
    shared_root = derive_output_root_from_run_dir(step_input.get("output_dir"))
    artifact_email = str(artifact.get("email") or "").strip()
    routing_config = artifact_routing_config_for_step_input(step_input)
    if not claimed_path.exists():
        release_conversion_lock(
            shared_root=shared_root,
            email=artifact_email,
            claimed_path=claimed_path,
            worker_label=str(step_input.get("worker_label") or "").strip(),
            task_index=int(step_input.get("task_index") or 0),
        )
        return {
            "ok": True,
            "status": "skipped_missing_artifact",
        }

    task_error_code = str(step_input.get("task_error_code") or "").strip()
    failure_mode = str(step_input.get("failure_mode") or "").strip().lower()
    original_pool_dir = resolve_openai_oauth_pool(
        {
            "pool_dir": artifact.get("pool_dir"),
            "output_dir": step_input.get("output_dir"),
        }
    )
    continue_source = original_pool_dir.resolve() == resolve_openai_oauth_continue_pool(
        {
            "output_dir": step_input.get("output_dir"),
            "openai_oauth_continue_pool_dir": step_input.get("openai_oauth_continue_pool_dir"),
        }
    ).resolve()
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
        release_conversion_lock(
            shared_root=shared_root,
            email=artifact_email,
            claimed_path=claimed_path,
            worker_label=str(step_input.get("worker_label") or "").strip(),
            task_index=int(step_input.get("task_index") or 0),
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
    if continue_source:
        pool_dir = resolve_openai_oauth_need_phone_pool(step_input)
    elif task_error_code == ErrorCodes.FREE_PERSONAL_WORKSPACE_MISSING:
        pool_dir = resolve_openai_oauth_wait_pool(step_input)
    else:
        pool_dir = original_pool_dir
    ensure_directory(pool_dir)

    if not task_error_code:
        success_pool_dir = resolve_openai_oauth_success_pool(step_input)
        route_result = route_openai_oauth_artifact(
            source_path=claimed_path,
            destination_dir=success_pool_dir,
            output_root=shared_root,
            target_folder="openai/converted",
            upload_percent=routing_config.openai_upload_percent,
            preferred_name=str(artifact.get("original_name") or claimed_path.name).strip() or claimed_path.name,
            move_local=True,
        )
        if not bool(route_result.get("ok")):
            return {
                "ok": False,
                "status": "openai_upload_failed",
                "claimed_path": str(claimed_path),
                "detail": str(route_result.get("detail") or "upload_failed"),
            }
        release_conversion_lock(
            shared_root=shared_root,
            email=artifact_email,
            claimed_path=claimed_path,
            worker_label=str(step_input.get("worker_label") or "").strip(),
            task_index=int(step_input.get("task_index") or 0),
        )
        return {
            "ok": True,
            "status": "uploaded" if str(route_result.get("route") or "") == "uploaded" else "promoted_success",
            "claimed_path": str(claimed_path),
            "restored_path": str(route_result.get("stored_path") or ""),
            "restore_pool_dir": str(success_pool_dir),
            "route": str(route_result.get("route") or ""),
            "object_key": str(route_result.get("object_key") or ""),
        }

    if failure_mode == "delete" and not continue_source:
        claimed_path.unlink(missing_ok=True)
        release_conversion_lock(
            shared_root=shared_root,
            email=artifact_email,
            claimed_path=claimed_path,
            worker_label=str(step_input.get("worker_label") or "").strip(),
            task_index=int(step_input.get("task_index") or 0),
        )
        return {
            "ok": True,
            "status": "deleted_failed_artifact",
            "claimed_path": str(claimed_path),
            "task_error_code": task_error_code,
        }

    route_result = route_openai_oauth_artifact(
        source_path=claimed_path,
        destination_dir=pool_dir,
        output_root=shared_root,
        target_folder="openai/failed-twice" if continue_source else "openai/failed-once",
        upload_percent=routing_config.openai_upload_percent if continue_source else 0.0,
        preferred_name=str(artifact.get("original_name") or claimed_path.name).strip() or claimed_path.name,
        move_local=True,
    )
    if not bool(route_result.get("ok")):
        return {
            "ok": False,
            "status": "openai_upload_failed",
            "claimed_path": str(claimed_path),
            "detail": str(route_result.get("detail") or "upload_failed"),
        }
    release_conversion_lock(
        shared_root=shared_root,
        email=artifact_email,
        claimed_path=claimed_path,
        worker_label=str(step_input.get("worker_label") or "").strip(),
        task_index=int(step_input.get("task_index") or 0),
    )
    return {
        "ok": True,
        "status": "uploaded" if str(route_result.get("route") or "") == "uploaded" else "restored",
        "restored_path": str(route_result.get("stored_path") or ""),
        "claimed_path": str(claimed_path),
        "restore_pool_dir": str(pool_dir),
        "route": str(route_result.get("route") or ""),
        "object_key": str(route_result.get("object_key") or ""),
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
        "status": ErrorCodes.FREE_PERSONAL_WORKSPACE_MISSING,
        "code": ErrorCodes.FREE_PERSONAL_WORKSPACE_MISSING,
        "detail": ErrorCodes.FREE_PERSONAL_WORKSPACE_MISSING,
        "oauth_account_id": oauth_account_id,
        "team_account_id": team_account_id,
        "validation_mode": "missing_personal_claims",
        "chatgpt_plan_type": plan_type,
        "organizations": organizations,
    }


def fill_team_pre_pool(*, step_input: dict[str, Any]) -> dict[str, Any]:
    shared_root = derive_output_root_from_run_dir(step_input.get("output_dir"))
    source_pool_dir = resolve_openai_oauth_pool(step_input)
    retry_source_pool_dir = resolve_openai_oauth_need_phone_pool(
        {
            "output_dir": step_input.get("output_dir"),
            "openai_oauth_need_phone_pool_dir": step_input.get("openai_oauth_need_phone_pool_dir"),
        }
    )
    team_pre_pool_dir = resolve_team_pre_pool(step_input)
    ensure_directory(source_pool_dir)
    ensure_directory(retry_source_pool_dir)
    ensure_directory(team_pre_pool_dir)

    max_move_count = safe_count(step_input.get("max_move_count") or 1, 1) or 1
    selected_paths = sort_paths_newest_first(
        [
            *[path for path in source_pool_dir.glob("*.json") if path.is_file()],
            *[path for path in retry_source_pool_dir.glob("*.json") if path.is_file()],
        ]
    )[:max_move_count]
    moved: list[dict[str, Any]] = []
    discarded: list[dict[str, Any]] = []
    skipped_existing_codex: list[dict[str, Any]] = []
    skipped_locked: list[dict[str, Any]] = []
    for candidate in selected_paths:
        valid, reason, payload = load_openai_oauth_seed_validation(candidate)
        if not valid:
            candidate.unlink(missing_ok=True)
            discarded.append(
                {
                    "source_path": str(candidate),
                    "reason": reason,
                }
            )
            continue
        email = str(payload.get("email") or "").strip()
        existing_codex = codex_success_lookup(
            shared_root=shared_root,
            output_root=shared_root,
            email=email,
        )
        if bool(existing_codex.get("exists")):
            skipped_existing_codex.append(
                {
                    "source_path": str(candidate),
                    "email": email,
                    "matches": existing_codex.get("matches") or [],
                }
            )
            continue
        prune_stale_conversion_lock(shared_root=shared_root, email=email)
        email_lock_path = conversion_lock_path(shared_root=shared_root, email=email)
        if email and email_lock_path.is_file():
            skipped_locked.append(
                {
                    "source_path": str(candidate),
                    "email": email,
                    "lock_path": str(email_lock_path),
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
        "skipped_existing_codex_count": len(skipped_existing_codex),
        "skipped_existing_codex": skipped_existing_codex,
        "skipped_locked_count": len(skipped_locked),
        "skipped_locked": skipped_locked,
        "source_pool_dir": str(source_pool_dir),
        "retry_source_pool_dir": str(retry_source_pool_dir),
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
    shared_root = derive_output_root_from_run_dir(step_input.get("output_dir"))
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

    claimed_members: list[dict[str, Any]] = []
    skipped_existing_codex: list[dict[str, Any]] = []
    skipped_locked: list[dict[str, Any]] = []
    selected_paths = sort_paths_newest_first([path for path in team_pre_pool_dir.glob("*.json") if path.is_file()])
    for candidate in selected_paths:
        if len(claimed_members) >= member_count:
            break
        valid, reason, payload = load_openai_oauth_seed_validation(candidate)
        if not valid:
            candidate.unlink(missing_ok=True)
            continue
        email = str(payload.get("email") or "").strip()
        existing_codex = codex_success_lookup(
            shared_root=shared_root,
            output_root=shared_root,
            email=email,
        )
        if bool(existing_codex.get("exists")):
            skipped_existing_codex.append(
                {
                    "source_path": str(candidate),
                    "email": email,
                    "matches": existing_codex.get("matches") or [],
                }
            )
            continue
        prune_stale_conversion_lock(shared_root=shared_root, email=email)
        email_lock_path = conversion_lock_path(shared_root=shared_root, email=email)
        if email and email_lock_path.is_file():
            skipped_locked.append(
                {
                    "source_path": str(candidate),
                    "email": email,
                    "lock_path": str(email_lock_path),
                }
            )
            continue
        claim_name = f"{uuid.uuid4().hex[:8]}-{candidate.name}"
        claimed_path = claims_dir / claim_name
        try:
            candidate.replace(claimed_path)
        except FileNotFoundError:
            continue
        conversion_claim = acquire_conversion_lock(
            shared_root=shared_root,
            email=email,
            claimed_path=claimed_path,
            source_path=candidate,
            stage="team",
            worker_label=str(step_input.get("worker_label") or "").strip(),
            task_index=int(step_input.get("task_index") or 0),
        )
        if email and conversion_claim is None:
            restore_to_pool(
                claimed_path=claimed_path,
                pool_dir=team_pre_pool_dir,
                preferred_name=candidate.name,
            )
            skipped_locked.append(
                {
                    "source_path": str(candidate),
                    "email": email,
                    "lock_path": str(email_lock_path),
                }
            )
            continue
        claimed_members.append(
            {
                "source_path": str(claimed_path),
                "claimed_path": str(claimed_path),
                "pool_dir": str(team_pre_pool_dir),
                "claims_dir": str(claims_dir),
                "original_name": candidate.name,
                "email": email,
                "password": str(payload.get("password") or "").strip(),
                "conversion_claim": conversion_claim or {},
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
            release_conversion_lock(
                shared_root=shared_root,
                email=str(artifact.get("email") or "").strip(),
                claimed_path=claimed_path,
                worker_label=str(step_input.get("worker_label") or "").strip(),
                task_index=int(step_input.get("task_index") or 0),
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
        "skipped_existing_codex_count": len(skipped_existing_codex),
        "skipped_existing_codex": skipped_existing_codex,
        "skipped_locked_count": len(skipped_locked),
        "skipped_locked": skipped_locked,
    }
