from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from others.common import json_log as _json_log
from others.paths import resolve_shared_root as _shared_root_from_output_root
from others.paths import resolve_team_pool_dir as _resolve_team_pool_dir
from others.openai_oauth_conversion_guard import prune_stale_conversion_locks as _prune_stale_conversion_locks
from others.runner_artifacts import (
    artifact_routing_config as _artifact_routing_config,
    backfill_openai_oauth_continue_pool as _backfill_openai_oauth_continue_pool,
    drain_oauth_pool_backlog as _drain_oauth_pool_backlog,
    drain_openai_oauth_wait_pool as _drain_openai_oauth_wait_pool,
    resolve_openai_oauth_continue_pool_dir as _resolve_openai_oauth_continue_pool_dir,
    resolve_openai_oauth_wait_pool_dir as _resolve_openai_oauth_wait_pool_dir,
    openai_oauth_continue_prefill_count as _openai_oauth_continue_prefill_count,
    openai_oauth_continue_prefill_min_age_seconds as _openai_oauth_continue_prefill_min_age_seconds,
    openai_oauth_continue_prefill_target_count as _openai_oauth_continue_prefill_target_count,
    openai_oauth_wait_seconds as _openai_oauth_wait_seconds,
)
from others.runner_team_auth import (
    prune_stale_team_auth_caches as _prune_stale_team_auth_caches,
    resolve_team_auth_pool as _resolve_team_auth_pool,
    select_team_auth_path as _select_team_auth_path,
    team_auth_is_reserved_for_team_expand as _team_auth_is_reserved_for_team_expand,
    team_auth_is_temp_blacklisted as _team_auth_is_temp_blacklisted,
)


@dataclass(frozen=True)
class WorkerTeamAuthSelection:
    team_auth_pool: list[str]
    selected_team_auth_path: str
    seat_reservation: dict[str, Any] | None


def process_worker_maintenance(
    *,
    active_roles: set[str],
    output_root: Path,
    free_oauth_pool_dir: Path,
    worker_label: str,
) -> None:
    stale_conversion_locks = _prune_stale_conversion_locks(shared_root=_shared_root_from_output_root(output_root))
    if stale_conversion_locks:
        _json_log(
            {
                "event": "register_openai_oauth_conversion_locks_pruned",
                "workerId": worker_label,
                "instanceRoles": sorted(active_roles),
                "removedCount": len(stale_conversion_locks),
                "removedStatePaths": stale_conversion_locks,
            }
        )
    wait_pool_result: dict[str, Any] | None = None
    if active_roles & {"main", "continue"}:
        wait_pool_result = _drain_openai_oauth_wait_pool(
            wait_pool_dir=_resolve_openai_oauth_wait_pool_dir(output_root=output_root),
            continue_pool_dir=_resolve_openai_oauth_continue_pool_dir(output_root=output_root),
            min_age_seconds=_openai_oauth_wait_seconds(),
        )
    if isinstance(wait_pool_result, dict) and wait_pool_result.get("artifacts"):
        _json_log(
            {
                "event": "register_openai_oauth_wait_pool_processed",
                "workerId": worker_label,
                "instanceRoles": sorted(active_roles),
                "result": wait_pool_result,
            }
        )

    continue_prefill_result: dict[str, Any] | None = None
    if "continue" in active_roles:
        continue_prefill_result = _backfill_openai_oauth_continue_pool(
            source_pool_dir=_resolve_openai_oauth_continue_pool_dir(output_root=output_root),
            continue_pool_dir=_resolve_openai_oauth_continue_pool_dir(output_root=output_root),
            max_move_count=_openai_oauth_continue_prefill_count(),
            target_count=_openai_oauth_continue_prefill_target_count(),
            min_age_seconds=_openai_oauth_continue_prefill_min_age_seconds(),
        )
    if isinstance(continue_prefill_result, dict) and (
        continue_prefill_result.get("artifacts") or continue_prefill_result.get("discarded")
    ):
        _json_log(
            {
                "event": "register_openai_oauth_continue_pool_prefilled",
                "workerId": worker_label,
                "instanceRoles": sorted(active_roles),
                "result": continue_prefill_result,
            }
        )

    artifact_config = _artifact_routing_config(output_root=output_root)
    backlog_jobs: list[tuple[Path, str, float, Path]] = []
    if active_roles & {"main", "continue"}:
        backlog_jobs.append(
            (
                free_oauth_pool_dir,
                "codex",
                artifact_config.free_local_split_percent,
                artifact_config.free_local_dir,
            )
        )
    if "team" in active_roles:
        backlog_jobs.append(
            (
                _resolve_team_pool_dir(str(output_root)),
                "codex-team",
                artifact_config.team_local_split_percent,
                artifact_config.team_local_dir,
            )
        )
    for pool_dir, target_folder, local_percent, local_dir in backlog_jobs:
        backlog_result = _drain_oauth_pool_backlog(
            pool_dir=pool_dir,
            target_folder=target_folder,
            local_percent=local_percent,
            local_dir=local_dir,
        )
        if not (isinstance(backlog_result, dict) and (backlog_result.get("uploaded") or backlog_result.get("failures"))):
            continue
        _json_log(
            {
                "event": "register_oauth_pool_backlog_processed",
                "workerId": worker_label,
                "instanceRoles": sorted(active_roles),
                "targetFolder": target_folder,
                "poolDir": str(pool_dir),
                "result": backlog_result,
            }
        )


def resolve_worker_team_auth(
    *,
    normalized_role: str,
    shared_root: Path,
    output_root: Path,
    worker_label: str,
    task_index: int,
    pinned_team_auth_path: str,
) -> WorkerTeamAuthSelection:
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

    selected_team_auth_path = ""
    seat_reservation: dict[str, Any] | None = None
    normalized_pinned_path = str(pinned_team_auth_path or "").strip()

    if normalized_pinned_path:
        if not Path(normalized_pinned_path).is_file():
            normalized_pinned_path = ""
        else:
            reserved_for_team, reserved_state = _team_auth_is_reserved_for_team_expand(
                shared_root=shared_root,
                team_auth_path=normalized_pinned_path,
            )
            if reserved_for_team and normalized_role in {"main", "continue"}:
                _json_log(
                    {
                        "event": "register_team_auth_pinned_reserved_for_team_expand",
                        "workerId": worker_label,
                        "taskIndex": task_index,
                        "instanceRole": normalized_role,
                        "teamAuthPath": normalized_pinned_path,
                        "reserved": reserved_state,
                    }
                )
                normalized_pinned_path = ""

    if normalized_pinned_path:
        pinned_blacklisted, _ = _team_auth_is_temp_blacklisted(
            shared_root=shared_root,
            team_auth_path=normalized_pinned_path,
        )
        if not pinned_blacklisted:
            selected_team_auth_path, seat_reservation = _select_team_auth_path(
                team_auth_pool=[normalized_pinned_path],
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
    else:
        selected_team_auth_path, seat_reservation = _select_team_auth_path(
            team_auth_pool=team_auth_pool,
            task_index=task_index,
            shared_root=shared_root,
            instance_role=normalized_role,
            worker_label=worker_label,
        )

    return WorkerTeamAuthSelection(
        team_auth_pool=team_auth_pool,
        selected_team_auth_path=selected_team_auth_path,
        seat_reservation=seat_reservation,
    )


def team_auth_unavailable_sleep_seconds(*, delay_seconds: float) -> float:
    return max(float(delay_seconds or 0.0), 1.0)
