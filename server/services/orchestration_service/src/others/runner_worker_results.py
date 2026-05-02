from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from others.common import json_log as _json_log
from others.config import CleanupRuntimeConfig
from others.runner_artifacts import (
    cleanup_run_output_dir as _cleanup_run_output_dir,
    copy_openai_oauth_artifacts_to_pool as _copy_openai_oauth_artifacts_to_pool,
    free_stop_after_validate_mode as _free_stop_after_validate_mode,
    postprocess_free_success_artifact as _postprocess_free_success_artifact,
    postprocess_team_success_artifacts as _postprocess_team_success_artifacts,
    openai_oauth_failure_target_pool_dir as _openai_oauth_failure_target_pool_dir,
    sync_refreshed_credentials_back_to_sources as _sync_refreshed_credentials_back_to_sources,
    team_has_collectable_artifacts as _team_has_collectable_artifacts,
)
from others.runner_failures import (
    extra_failure_cooldown_seconds as _extra_failure_cooldown_seconds,
    mark_team_mother_failure_cooldown as _mark_team_mother_failure_cooldown,
    team_auth_blacklist_reason as _team_auth_blacklist_reason,
    team_mother_failure_cooldown_seconds as _team_mother_failure_cooldown_seconds,
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
    record_team_auth_recent_invite_result as _record_team_auth_recent_invite_result,
    record_team_auth_recent_team_expand_result as _record_team_auth_recent_team_expand_result,
    reconcile_team_auth_seat_state_from_result as _team_auth_reconcile_seat_state_from_result,
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
from others.result_artifacts import (
    output_dict as _output_dict,
    team_auth_path as _team_auth_path_from_result_payload,
    team_mother_identity as _team_mother_identity_from_result_payload,
)


def process_worker_run_result(
    *,
    result: Any,
    started_at: str,
    run_output_dir: Path,
    output_root: Path,
    shared_root: Path,
    openai_oauth_pool_dir: Path,
    normalized_role: str,
    worker_label: str,
    task_index: int,
    local_run_index: int,
    worker_state: Any,
    selected_team_auth_path: str,
    free_local_selected: bool,
    team_auth_pool: list[str],
) -> float:
    finished_at = datetime.now(timezone.utc).isoformat()
    _json_log(
        {
            "event": "register_run_finished",
            "workerId": worker_label,
            "pid": os.getpid(),
            "taskIndex": task_index,
            "localRunIndex": local_run_index,
            "startedAt": started_at,
            "finishedAt": finished_at,
            "ok": bool(result.ok),
            "outputDir": str(run_output_dir),
            "result": result.to_dict(),
        }
    )
    worker_state.run_finished(
        task_index=task_index,
        result=result.to_dict(),
        output_dir=str(run_output_dir),
        finished_at=finished_at,
    )
    result_payload = result.to_dict()
    if isinstance(result_payload, dict):
        result_payload.setdefault("instanceRole", normalized_role)
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
        result_payload_value=result_payload,
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
        result_payload_value=result_payload,
        identity=team_result_identity,
    )
    _record_team_auth_recent_team_expand_result(
        shared_root=shared_root,
        team_auth_path=effective_team_auth_path,
        result_payload_value=result_payload,
        instance_role=normalized_role,
        identity=team_result_identity,
    )
    _team_auth_reconcile_seat_state_from_result(
        shared_root=shared_root,
        team_auth_path=effective_team_auth_path,
        result_payload_value=result_payload,
        instance_role=normalized_role,
        worker_label=worker_label,
        task_index=task_index,
    )
    synced_credentials = _sync_refreshed_credentials_back_to_sources(
        result_payload_value=result_payload,
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
                "openaiOauthPath": str((create_output or {}).get("storage_path") or "").strip(),
                "smallSuccessPath": str((create_output or {}).get("storage_path") or "").strip(),
                "validateStatus": str((validate_output or {}).get("status") or "").strip(),
                "validateCode": str((validate_output or {}).get("code") or "").strip(),
                "oauthSuccessPath": str((obtain_output or {}).get("successPath") or "").strip(),
            }
        )
    mailbox_capacity_detail = _mailbox_capacity_failure_detail(result_payload_value=result_payload)
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

    capacity_detail = _team_capacity_failure_detail(result_payload_value=result_payload)
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

    blacklist_reason = _team_auth_blacklist_reason(result_payload_value=result_payload)
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
        _copy_openai_oauth_artifacts_to_pool(
            run_output_dir=run_output_dir,
            pool_dir=_openai_oauth_failure_target_pool_dir(
                output_root=output_root,
                result_payload_value=result_payload,
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
    return float(extra_cooldown_seconds or 0.0)


def process_worker_run_crash(
    *,
    exc: Exception,
    started_at: str,
    run_output_dir: Path,
    openai_oauth_pool_dir: Path,
    normalized_role: str,
    worker_label: str,
    task_index: int,
    local_run_index: int,
    worker_state: Any,
) -> float:
    crash_cooldown_seconds = CleanupRuntimeConfig.from_env().crash_cooldown_seconds
    finished_at = datetime.now(timezone.utc).isoformat()
    _json_log(
        {
            "event": "register_run_crashed",
            "workerId": worker_label,
            "pid": os.getpid(),
            "taskIndex": task_index,
            "localRunIndex": local_run_index,
            "startedAt": started_at,
            "finishedAt": finished_at,
            "outputDir": str(run_output_dir),
            "error": str(exc),
        }
    )
    worker_state.run_crashed(
        task_index=task_index,
        output_dir=str(run_output_dir),
        error=str(exc),
        finished_at=finished_at,
    )
    _copy_openai_oauth_artifacts_to_pool(
        run_output_dir=run_output_dir,
        pool_dir=(
            _openai_oauth_failure_target_pool_dir(
                output_root=openai_oauth_pool_dir.parent.parent if openai_oauth_pool_dir.parent.name == "openai" else openai_oauth_pool_dir.parent,
                result_payload_value={"error": str(exc), "instanceRole": normalized_role},
            )
            if normalized_role in {"main", "continue"}
            else openai_oauth_pool_dir
        ),
        worker_label=worker_label,
        task_index=task_index,
    )
    _cleanup_run_output_dir(
        run_output_dir=run_output_dir,
        worker_label=worker_label,
        task_index=task_index,
    )
    return float(crash_cooldown_seconds or 0.0)
