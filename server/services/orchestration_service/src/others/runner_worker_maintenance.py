from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from others.common import json_log as _json_log
from others.paths import resolve_shared_root as _shared_root_from_output_root
from others.runner_artifacts import (
    artifact_routing_config as _artifact_routing_config,
    backfill_small_success_continue_pool as _backfill_small_success_continue_pool,
    drain_oauth_pool_backlog as _drain_oauth_pool_backlog,
    drain_small_success_wait_pool as _drain_small_success_wait_pool,
    resolve_small_success_continue_pool_dir as _resolve_small_success_continue_pool_dir,
    resolve_small_success_wait_pool_dir as _resolve_small_success_wait_pool_dir,
    small_success_continue_prefill_count as _small_success_continue_prefill_count,
    small_success_continue_prefill_min_age_seconds as _small_success_continue_prefill_min_age_seconds,
    small_success_continue_prefill_target_count as _small_success_continue_prefill_target_count,
    small_success_wait_seconds as _small_success_wait_seconds,
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
    normalized_role: str,
    output_root: Path,
    free_oauth_pool_dir: Path,
    worker_label: str,
) -> None:
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

    artifact_config = _artifact_routing_config(output_root=output_root)
    backlog_result: dict[str, Any] | None = None
    if normalized_role in {"main", "continue"}:
        backlog_result = _drain_oauth_pool_backlog(
            pool_dir=free_oauth_pool_dir,
            target_folder="codex",
            local_percent=artifact_config.free_local_split_percent,
            local_dir=artifact_config.free_local_dir,
        )
    elif normalized_role == "team":
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
