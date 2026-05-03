from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dashboard_server import WorkerRuntimeState
from dst_flow import run_dst_flow_once
from others.common import ensure_directory as _ensure_directory
from others.common import json_log as _json_log
from others.config import CleanupRuntimeConfig
from others.paths import resolve_shared_root as _shared_root_from_output_root
from others.runner_artifacts import (
    artifact_routing_config as _artifact_routing_config,
    select_local_split as _select_local_split,
    team_live_local_sync_loop as _team_live_local_sync_loop,
)
from others.runner_flow_scheduler import (
    choose_runnable_flow_spec as _choose_runnable_flow_spec,
    configured_flow_roles as _configured_flow_roles,
    release_flow_slot as _release_flow_slot,
    reserve_flow_slot as _reserve_flow_slot,
    snapshot_active_flow_counts as _snapshot_active_flow_counts,
)
from others.runner_team_auth import (
    release_team_auth_seat_reservations as _team_auth_release_seat_reservations,
)
from others.runner_worker_maintenance import (
    process_worker_maintenance as _process_worker_maintenance,
    resolve_worker_team_auth as _resolve_worker_team_auth,
    team_auth_unavailable_sleep_seconds as _team_auth_unavailable_sleep_seconds,
)
from others.runner_worker_results import (
    process_worker_run_crash as _process_worker_run_crash,
    process_worker_run_result as _process_worker_run_result,
)


def cleanup_runtime_config() -> CleanupRuntimeConfig:
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


def task_slots_exhausted(
    *,
    task_counter: Any,
    max_runs: int,
) -> bool:
    if max_runs <= 0:
        return False
    lock_factory = getattr(task_counter, "get_lock", None)
    if callable(lock_factory):
        with lock_factory():
            return int(getattr(task_counter, "value", 0) or 0) >= max_runs
    return int(getattr(task_counter, "value", 0) or 0) >= max_runs


def worker_loop(
    *,
    worker_id: int,
    instance_id: str,
    instance_role: str,
    output_root_text: str,
    delay_seconds: float,
    max_runs: int,
    task_max_attempts: int,
    flow_specs: tuple[Any, ...],
    stop_event: Any,
    task_counter: Any,
    free_oauth_pool_dir_text: str,
    active_flow_counts: Any | None = None,
    active_flow_lock: Any | None = None,
) -> None:
    output_root = Path(output_root_text).resolve()
    shared_root = _shared_root_from_output_root(output_root)
    worker_output_root = build_worker_output_root(output_root=output_root, worker_id=worker_id)
    _ensure_directory(worker_output_root)
    free_oauth_pool_dir = Path(free_oauth_pool_dir_text).resolve()
    _ensure_directory(free_oauth_pool_dir)
    worker_label = f"worker-{worker_id:02d}"
    os.environ["REGISTER_WORKER_ID"] = worker_label
    local_run_index = 0
    configured_roles = _configured_flow_roles(flow_specs) or {str(instance_role or "").strip().lower()}
    any_team_auth_pinned = any(bool(str(getattr(spec, "team_auth_path", "") or "").strip()) for spec in flow_specs)

    worker_state = WorkerRuntimeState(
        shared_root=shared_root,
        instance_id=instance_id,
        instance_role=instance_role,
        worker_id=worker_label,
    )
    worker_state.started(
        pid=os.getpid(),
        output_root=str(worker_output_root),
        team_auth_pinned=any_team_auth_pinned,
    )
    _json_log(
        {
            "event": "register_worker_started",
            "workerId": worker_label,
            "pid": os.getpid(),
            "outputRoot": str(worker_output_root),
            "teamAuthPinned": any_team_auth_pinned,
            "configuredRoles": sorted(configured_roles),
        }
    )

    if "team" in configured_roles:
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
        if task_slots_exhausted(task_counter=task_counter, max_runs=max_runs):
            break
        _process_worker_maintenance(
            active_roles=configured_roles,
            output_root=output_root,
            free_oauth_pool_dir=free_oauth_pool_dir,
            worker_label=worker_label,
        )
        selected_flow_spec, flow_selection = _choose_runnable_flow_spec(
            flow_specs=flow_specs,
            output_root=output_root,
            shared_root=shared_root,
            active_flow_counts=_snapshot_active_flow_counts(
                active_flow_counts=active_flow_counts,
                active_flow_lock=active_flow_lock,
            ),
        )
        if selected_flow_spec is None:
            sleep_seconds = max(float(delay_seconds or 0.0), 1.0)
            _json_log(
                {
                    "event": "register_flow_selection_idle",
                    "workerId": worker_label,
                    "pid": os.getpid(),
                    "configuredRoles": sorted(configured_roles),
                    "selection": flow_selection,
                    "seconds": sleep_seconds,
                }
            )
            worker_state.sleeping(task_index=int(task_counter.value or 0), seconds=sleep_seconds)
            time.sleep(sleep_seconds)
            continue
        reserved_flow_slot = _reserve_flow_slot(
            spec=selected_flow_spec,
            active_flow_counts=active_flow_counts,
            active_flow_lock=active_flow_lock,
        )
        if not reserved_flow_slot:
            _json_log(
                {
                    "event": "register_flow_slot_busy",
                    "workerId": worker_label,
                    "pid": os.getpid(),
                    "selection": flow_selection,
                }
            )
            worker_state.sleeping(task_index=int(task_counter.value or 0), seconds=1.0)
            time.sleep(1.0)
            continue
        task_index = claim_task_index(task_counter=task_counter, max_runs=max_runs)
        if task_index is None:
            _release_flow_slot(
                spec=selected_flow_spec,
                active_flow_counts=active_flow_counts,
                active_flow_lock=active_flow_lock,
            )
            break

        normalized_role = str(selected_flow_spec.instance_role or "").strip().lower()
        openai_oauth_pool_dir = Path(selected_flow_spec.openai_oauth_pool_dir).resolve()
        _ensure_directory(openai_oauth_pool_dir)
        input_source_dir = str(selected_flow_spec.input_source_dir or "").strip()
        input_claims_dir = str(selected_flow_spec.input_claims_dir or "").strip()
        free_local_selected = False
        if normalized_role in {"main", "continue"}:
            artifact_config = _artifact_routing_config(output_root=output_root)
            free_local_selected = _select_local_split(
                percent=artifact_config.free_local_split_percent
            )

        team_auth_selection = _resolve_worker_team_auth(
            normalized_role=normalized_role,
            shared_root=shared_root,
            output_root=output_root,
            worker_label=worker_label,
            task_index=task_index,
            pinned_team_auth_path=str(selected_flow_spec.team_auth_path or "").strip(),
        )
        team_auth_pool = team_auth_selection.team_auth_pool
        selected_team_auth_path = team_auth_selection.selected_team_auth_path
        seat_reservation = team_auth_selection.seat_reservation

        if normalized_role == "team" and team_auth_pool and not selected_team_auth_path:
            _json_log(
                {
                    "event": "register_team_auth_pool_filtered_empty",
                    "workerId": worker_label,
                    "pid": os.getpid(),
                    "taskIndex": task_index,
                    "teamAuthPoolSize": len(team_auth_pool),
                }
            )
            sleep_seconds = _team_auth_unavailable_sleep_seconds(delay_seconds=delay_seconds)
            worker_state.sleeping(task_index=task_index, seconds=sleep_seconds)
            time.sleep(sleep_seconds)
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
                "flowName": str(selected_flow_spec.name or "").strip(),
                "flowPath": str(selected_flow_spec.flow_path or "").strip(),
                "taskRole": normalized_role,
                "teamAuthPath": selected_team_auth_path,
                "teamAuthPoolSize": len(team_auth_pool),
                "teamInviteEnabled": bool(selected_team_auth_path),
                "openaiOauthPoolDir": str(openai_oauth_pool_dir),
                "smallSuccessPoolDir": str(openai_oauth_pool_dir),
                "inputSourceDir": input_source_dir,
                "inputClaimsDir": input_claims_dir,
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
            flow_name=str(selected_flow_spec.name or "").strip(),
            flow_path=str(selected_flow_spec.flow_path or "").strip(),
            task_role=normalized_role,
        )

        try:
            result = run_dst_flow_once(
                output_dir=str(run_output_dir),
                team_auth_path=selected_team_auth_path or None,
                team_invite_enabled=bool(selected_team_auth_path),
                input_source_dir=input_source_dir or None,
                input_claims_dir=input_claims_dir or None,
                openai_oauth_pool_dir=str(openai_oauth_pool_dir),
                flow_path=str(selected_flow_spec.flow_path or "").strip() or None,
                task_max_attempts=selected_flow_spec.task_max_attempts or task_max_attempts or None,
                mailbox_business_key=str(selected_flow_spec.mailbox_business_key or "").strip() or None,
                r2_upload_enabled=(not free_local_selected)
                if normalized_role in {"main", "continue"}
                else None,
            )
            extra_cooldown_seconds = _process_worker_run_result(
                result=result,
                started_at=started_at,
                run_output_dir=run_output_dir,
                output_root=output_root,
                shared_root=shared_root,
                openai_oauth_pool_dir=openai_oauth_pool_dir,
                normalized_role=normalized_role,
                worker_label=worker_label,
                task_index=task_index,
                local_run_index=local_run_index,
                worker_state=worker_state,
                selected_team_auth_path=selected_team_auth_path,
                free_local_selected=free_local_selected,
                team_auth_pool=team_auth_pool,
            )
        except Exception as exc:
            extra_cooldown_seconds = _process_worker_run_crash(
                exc=exc,
                started_at=started_at,
                run_output_dir=run_output_dir,
                openai_oauth_pool_dir=openai_oauth_pool_dir,
                normalized_role=normalized_role,
                worker_label=worker_label,
                task_index=task_index,
                local_run_index=local_run_index,
                worker_state=worker_state,
            )
        finally:
            _release_flow_slot(
                spec=selected_flow_spec,
                active_flow_counts=active_flow_counts,
                active_flow_lock=active_flow_lock,
            )
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
