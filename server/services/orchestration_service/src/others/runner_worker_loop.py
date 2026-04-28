from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dashboard_server import WorkerRuntimeState
from dst_flow import run_dst_flow_once
from others.common import (
    ensure_directory as _ensure_directory,
    json_log as _json_log,
)
from others.config import CleanupRuntimeConfig
from others.paths import resolve_shared_root as _shared_root_from_output_root
from others.runner_artifacts import (
    artifact_routing_config as _artifact_routing_config,
    backfill_small_success_continue_pool as _backfill_small_success_continue_pool,
    cleanup_run_output_dir as _cleanup_run_output_dir,
    copy_small_success_artifacts_to_pool as _copy_small_success_artifacts_to_pool,
    drain_oauth_pool_backlog as _drain_oauth_pool_backlog,
    drain_small_success_wait_pool as _drain_small_success_wait_pool,
    free_stop_after_validate_mode as _free_stop_after_validate_mode,
    postprocess_free_success_artifact as _postprocess_free_success_artifact,
    postprocess_team_success_artifacts as _postprocess_team_success_artifacts,
    resolve_small_success_continue_pool_dir as _resolve_small_success_continue_pool_dir,
    resolve_small_success_wait_pool_dir as _resolve_small_success_wait_pool_dir,
    select_local_split as _select_local_split,
    small_success_continue_prefill_count as _small_success_continue_prefill_count,
    small_success_continue_prefill_min_age_seconds as _small_success_continue_prefill_min_age_seconds,
    small_success_continue_prefill_target_count as _small_success_continue_prefill_target_count,
    small_success_failure_target_pool_dir as _small_success_failure_target_pool_dir,
    small_success_wait_seconds as _small_success_wait_seconds,
    sync_refreshed_credentials_back_to_sources as _sync_refreshed_credentials_back_to_sources,
    team_has_collectable_artifacts as _team_has_collectable_artifacts,
    team_live_local_sync_loop as _team_live_local_sync_loop,
)
from others.runner_mailbox import (
    clear_mailbox_capacity_failures as _clear_mailbox_capacity_failures,
    mailbox_capacity_failure_detail as _mailbox_capacity_failure_detail,
    mark_mailbox_capacity_failure as _mark_mailbox_capacity_failure,
    record_business_mailbox_domain_outcome as _record_business_mailbox_domain_outcome,
)
from others.runner_team_auth import (
    clear_team_auth_temporary_blacklist as _clear_team_auth_temporary_blacklist,
    mark_team_auth_temporary_blacklist as _mark_team_auth_temporary_blacklist,
    prune_stale_team_auth_caches as _prune_stale_team_auth_caches,
    record_team_auth_recent_invite_result as _record_team_auth_recent_invite_result,
    record_team_auth_recent_team_expand_result as _record_team_auth_recent_team_expand_result,
    reconcile_team_auth_seat_state_from_result as _team_auth_reconcile_seat_state_from_result,
    release_team_auth_seat_reservations as _team_auth_release_seat_reservations,
    resolve_team_auth_pool as _resolve_team_auth_pool,
    select_team_auth_path as _select_team_auth_path,
    team_auth_is_reserved_for_team_expand as _team_auth_is_reserved_for_team_expand,
    team_auth_is_temp_blacklisted as _team_auth_is_temp_blacklisted,
    team_auth_runtime_config as _team_auth_runtime_config,
    team_mother_identity_from_team_auth_path as _team_mother_identity_from_team_auth_path,
    sync_team_auth_codex_seats_from_cleanup_result as _team_auth_sync_codex_seats_from_cleanup_result,
)
from others.runner_team_cleanup import (
    all_team_auth_capacity_cooled as _all_team_auth_capacity_cooled,
    clear_team_auth_capacity_cooldown as _clear_team_auth_capacity_cooldown,
    mark_team_auth_capacity_cooldown as _mark_team_auth_capacity_cooldown,
    team_capacity_failure_detail as _team_capacity_failure_detail,
    trigger_codex_capacity_cleanup as _trigger_codex_capacity_cleanup,
)
from others.runner_failures import (
    extra_failure_cooldown_seconds as _extra_failure_cooldown_seconds,
    mark_team_mother_failure_cooldown as _mark_team_mother_failure_cooldown,
    team_auth_blacklist_reason as _team_auth_blacklist_reason,
    team_mother_failure_cooldown_seconds as _team_mother_failure_cooldown_seconds,
)
from others.result_artifacts import (
    output_dict as _output_dict,
    team_auth_path as _team_auth_path_from_result_payload,
    team_mother_identity as _team_mother_identity_from_result_payload,
)


def _cleanup_runtime_config() -> CleanupRuntimeConfig:
    return CleanupRuntimeConfig.from_env()


def build_worker_output_root(*, output_root: Path, worker_id: int) -> Path:
    return output_root / f"worker-{worker_id:02d}"


def build_run_output_dir(*, worker_output_root: Path, task_index: int) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return worker_output_root / f"run-{timestamp}-task{task_index:06d}"


def claim_task_index(
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


def worker_loop(
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
    worker_output_root = build_worker_output_root(output_root=output_root, worker_id=worker_id)
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
            artifact_config = _artifact_routing_config(output_root=output_root)
            backlog_result = _drain_oauth_pool_backlog(
                pool_dir=free_oauth_pool_dir,
                target_folder="codex",
                local_percent=artifact_config.free_local_split_percent,
                local_dir=artifact_config.free_local_dir,
            )
        elif normalized_role == "team":
            artifact_config = _artifact_routing_config(output_root=output_root)
            backlog_result = _drain_oauth_pool_backlog(
                pool_dir=_shared_root_from_output_root(output_root) / "team-oauth-pool",
                target_folder="codex-team",
                local_percent=artifact_config.team_local_split_percent,
                local_dir=artifact_config.team_local_dir,
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
        task_index = claim_task_index(task_counter=task_counter, max_runs=max_runs)
        if task_index is None:
            break
        free_local_selected = False
        if normalized_role in {"main", "continue"}:
            artifact_config = _artifact_routing_config(output_root=output_root)
            free_local_selected = _select_local_split(
                percent=artifact_config.free_local_split_percent
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
        run_output_dir = build_run_output_dir(
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
                    result_payload,
                    selected_team_auth_path,
                )
            invite_capacity_cleanup_output = _output_dict(
                result_payload,
                "invite-codex-member-capacity-cleanup",
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
                            team_auth_path=_team_auth_path_from_result_payload(
                                result_payload,
                                selected_team_auth_path,
                            ),
                            identity=_team_mother_identity_from_result_payload(result_payload),
                            worker_label=worker_label,
                            task_index=task_index,
                        )
            stop_after_validate_mode = _free_stop_after_validate_mode() and normalized_role in {"main", "continue"}
            if stop_after_validate_mode:
                create_output = _output_dict(result_payload, "create-openai-account")
                validate_output = _output_dict(result_payload, "validate-free-personal-oauth")
                obtain_output = _output_dict(result_payload, "obtain-codex-oauth")
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
                team_auth_config = _team_auth_runtime_config(output_root=output_root, shared_root=shared_root)
                _mark_team_auth_capacity_cooldown(
                    shared_root=shared_root,
                    team_auth_path=effective_team_auth_path,
                    cooldown_seconds=team_auth_config.capacity_cooldown_seconds,
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
                team_auth_config = _team_auth_runtime_config(output_root=output_root, shared_root=shared_root)
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
                    blacklist_seconds=team_auth_config.temp_blacklist_seconds,
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
            extra_cooldown_seconds = _cleanup_runtime_config().crash_cooldown_seconds
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

