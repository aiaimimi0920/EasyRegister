from __future__ import annotations

from datetime import datetime, timezone
import json
import math
import multiprocessing as mp
import os
import signal
import time
from pathlib import Path
from typing import Any, Callable

from dashboard_server import ServiceRuntimeState, start_dashboard_server_if_enabled
from others.common import ensure_directory as _ensure_directory
from others.common import json_log as _json_log
from others.config import RunnerMainConfig
from others.config_env import (
    account_audit_worker_hard_timeout_seconds as _account_audit_worker_hard_timeout_seconds,
    continue_worker_hard_timeout_seconds as _continue_worker_hard_timeout_seconds,
)
from others.runner_flow_scheduler import flow_spec_summary
from others.runner_flow_scheduler import release_flow_slot_for_owner
from others.preflight import validate_runtime_preflight as _validate_runtime_preflight
from others.runner_worker_loop import worker_loop


DEFAULT_ACCOUNT_AUDIT_WORKER_HARD_TIMEOUT_SECONDS = 420.0
DEFAULT_FLOW_SLOT_UNINTERRUPTIBLE_CONFIRM_SECONDS = 30.0
DEFAULT_ROLE_WORKER_TERMINATE_GRACE_SECONDS = 1.0
_UNINTERRUPTIBLE_SINCE_ATTRIBUTE = "_easyregister_uninterruptible_since_utc"


def flow_slot_uninterruptible_stale_seconds() -> float:
    raw_value = str(os.getenv("REGISTER_FLOW_SLOT_UNINTERRUPTIBLE_STALE_SECONDS", "900") or "").strip()
    try:
        return max(0.0, float(raw_value))
    except ValueError:
        return 900.0


def _env_float(*, name: str, default: float) -> float:
    raw_value = str(os.getenv(name) or "").strip()
    if not raw_value:
        return float(default)
    try:
        return max(0.0, float(raw_value))
    except ValueError:
        return float(default)


def flow_slot_uninterruptible_confirm_seconds() -> float:
    return _env_float(
        name="REGISTER_FLOW_SLOT_UNINTERRUPTIBLE_CONFIRM_SECONDS",
        default=DEFAULT_FLOW_SLOT_UNINTERRUPTIBLE_CONFIRM_SECONDS,
    )


def _worker_uninterruptible_stale_threshold_seconds(
    *,
    worker_state: dict[str, Any],
    default_seconds: float,
) -> float:
    role = str(worker_state.get("currentTaskRole") or "").strip().lower()
    flow_name = str(worker_state.get("currentFlowName") or "").strip().lower()
    if role == "account-audit" or flow_name == "openai-account-availability-audit":
        return _env_float(
            name="REGISTER_ACCOUNT_AUDIT_FLOW_SLOT_UNINTERRUPTIBLE_STALE_SECONDS",
            default=max(float(default_seconds or 0.0), 3600.0),
        )
    return max(0.0, float(default_seconds or 0.0))


def account_audit_worker_hard_timeout_seconds() -> float:
    # Single source of truth lives in config_env so preflight can validate this
    # against the protocol HTTP budget without importing this module.
    return _account_audit_worker_hard_timeout_seconds()


def continue_worker_hard_timeout_seconds() -> float:
    return _continue_worker_hard_timeout_seconds()


def _worker_state_is_account_audit(worker_state: dict[str, Any]) -> bool:
    role = str(worker_state.get("currentTaskRole") or "").strip().lower()
    flow_name = str(worker_state.get("currentFlowName") or "").strip().lower()
    return role == "account-audit" or flow_name == "openai-account-availability-audit"


def _worker_state_is_continue(worker_state: dict[str, Any]) -> bool:
    role = str(worker_state.get("currentTaskRole") or "").strip().lower()
    flow_name = str(worker_state.get("currentFlowName") or "").strip().lower()
    return role == "continue" or flow_name == "openai-continue"


def cleanup_dashboard_worker_state_files(*, shared_root: Path, instance_id: str) -> None:
    workers_dir = shared_root / "others" / "dashboard-state" / str(instance_id or "default").strip() / "workers"
    if not workers_dir.is_dir():
        return
    for path in workers_dir.glob("*.json"):
        try:
            path.unlink()
        except FileNotFoundError:
            continue


def install_signal_handlers(*, stop_event: Any) -> None:
    def _handler(signum: int, _frame: Any) -> None:
        _json_log(
            {
                "event": "register_supervisor_signal",
                "pid": os.getpid(),
                "signal": signum,
            }
        )
        stop_event.set()

    signal.signal(signal.SIGINT, _handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handler)


def start_worker(
    *,
    ctx: Any,
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
    active_flow_counts: Any,
    active_flow_lock: Any,
    active_flow_owners: Any,
) -> Any:
    process = ctx.Process(
        target=worker_loop,
        kwargs={
            "worker_id": worker_id,
            "instance_id": instance_id,
            "instance_role": instance_role,
            "output_root_text": output_root_text,
            "delay_seconds": delay_seconds,
            "max_runs": max_runs,
            "task_max_attempts": task_max_attempts,
            "flow_specs": flow_specs,
            "stop_event": stop_event,
            "task_counter": task_counter,
            "free_oauth_pool_dir_text": free_oauth_pool_dir_text,
            "active_flow_counts": active_flow_counts,
            "active_flow_lock": active_flow_lock,
            "active_flow_owners": active_flow_owners,
        },
        name=f"register-worker-{worker_id:02d}",
    )
    process.start()
    _json_log(
        {
            "event": "register_worker_spawned",
            "workerId": f"worker-{worker_id:02d}",
            "pid": process.pid,
        }
    )
    return process


def task_slots_exhausted(*, task_counter: Any, max_runs: int) -> bool:
    if max_runs <= 0:
        return False
    return task_counter_value(task_counter) >= max_runs


def task_counter_value(task_counter: Any) -> int:
    get_obj = getattr(task_counter, "get_obj", None)
    if callable(get_obj):
        try:
            return int(getattr(get_obj(), "value", 0) or 0)
        except Exception:
            pass
    return int(getattr(task_counter, "value", 0) or 0)


def _worker_label_from_id(worker_id: Any) -> str:
    try:
        return f"worker-{int(worker_id):02d}"
    except (TypeError, ValueError):
        text = str(worker_id or "").strip()
        return text if text.startswith("worker-") else f"worker-{text}"


def _read_linux_process_state(*, pid: int, proc_root: Path = Path("/proc")) -> str:
    if pid <= 0:
        return ""
    status_path = proc_root / str(pid) / "status"
    try:
        for line in status_path.read_text(encoding="utf-8", errors="replace").splitlines():
            if not line.startswith("State:"):
                continue
            parts = line.split()
            return str(parts[1] if len(parts) > 1 else "").strip()
    except OSError:
        return ""
    return ""


def _clear_uninterruptible_observation(process: Any) -> None:
    try:
        delattr(process, _UNINTERRUPTIBLE_SINCE_ATTRIBUTE)
    except (AttributeError, TypeError):
        pass


def _parse_utc_timestamp(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _read_worker_state(*, shared_root: Path, instance_id: str, worker_label: str) -> dict[str, Any]:
    state_path = (
        shared_root
        / "others"
        / "dashboard-state"
        / str(instance_id or "default").strip()
        / "workers"
        / f"{worker_label}.json"
    )
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def recover_stale_uninterruptible_worker_slots(
    *,
    processes: dict[int, Any],
    shared_root: Path,
    instance_id: str,
    active_flow_counts: Any,
    active_flow_owners: Any,
    active_flow_lock: Any,
    stale_seconds: float,
    now: datetime | None = None,
    proc_root: Path = Path("/proc"),
) -> list[dict[str, Any]]:
    threshold_seconds = max(0.0, float(stale_seconds or 0.0))
    if threshold_seconds <= 0.0:
        return []
    now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    recovered: list[dict[str, Any]] = []
    for worker_id, process in list(processes.items()):
        is_alive = getattr(process, "is_alive", None)
        try:
            if callable(is_alive) and not is_alive():
                continue
        except Exception:
            continue
        try:
            pid = int(getattr(process, "pid", 0) or 0)
        except (TypeError, ValueError):
            pid = 0
        if _read_linux_process_state(pid=pid, proc_root=proc_root) != "D":
            _clear_uninterruptible_observation(process)
            continue
        worker_label = _worker_label_from_id(worker_id)
        try:
            owned_slot_key = str(active_flow_owners.get(worker_label, "") or "").strip()
        except Exception:
            owned_slot_key = ""
        if not owned_slot_key:
            continue
        worker_state = _read_worker_state(
            shared_root=shared_root,
            instance_id=instance_id,
            worker_label=worker_label,
        )
        timestamp = _parse_utc_timestamp(worker_state.get("updatedAt")) or _parse_utc_timestamp(
            worker_state.get("startedAt")
        )
        if timestamp is None:
            continue
        age_seconds = (now_utc - timestamp).total_seconds()
        effective_threshold_seconds = _worker_uninterruptible_stale_threshold_seconds(
            worker_state=worker_state,
            default_seconds=threshold_seconds,
        )
        if age_seconds < effective_threshold_seconds:
            continue
        first_uninterruptible_at = getattr(process, _UNINTERRUPTIBLE_SINCE_ATTRIBUTE, None)
        if not isinstance(first_uninterruptible_at, datetime):
            try:
                setattr(process, _UNINTERRUPTIBLE_SINCE_ATTRIBUTE, now_utc)
            except (AttributeError, TypeError):
                continue
            uninterruptible_seconds = 0.0
        else:
            if first_uninterruptible_at.tzinfo is None:
                first_uninterruptible_at = first_uninterruptible_at.replace(tzinfo=timezone.utc)
            else:
                first_uninterruptible_at = first_uninterruptible_at.astimezone(timezone.utc)
            uninterruptible_seconds = max(0.0, (now_utc - first_uninterruptible_at).total_seconds())
        confirm_seconds = flow_slot_uninterruptible_confirm_seconds()
        if uninterruptible_seconds < confirm_seconds:
            continue
        released_slot_key = release_flow_slot_for_owner(
            owner_id=worker_label,
            active_flow_counts=active_flow_counts,
            active_flow_owners=active_flow_owners,
            active_flow_lock=active_flow_lock,
        )
        if not released_slot_key:
            continue
        terminate_signal_sent = False
        terminate = getattr(process, "terminate", None)
        if callable(terminate):
            try:
                terminate()
                terminate_signal_sent = True
            except Exception:
                terminate_signal_sent = False
        recovery = {
            "workerId": worker_label,
            "pid": pid,
            "slotKey": released_slot_key,
            "processState": "D",
            "staleSeconds": age_seconds,
            "thresholdSeconds": effective_threshold_seconds,
            "defaultThresholdSeconds": threshold_seconds,
            "uninterruptibleSeconds": uninterruptible_seconds,
            "confirmSeconds": confirm_seconds,
            "terminateSignalSent": terminate_signal_sent,
            "currentTaskRole": str(worker_state.get("currentTaskRole") or ""),
            "currentFlowName": str(worker_state.get("currentFlowName") or ""),
            "currentOutputDir": str(worker_state.get("currentOutputDir") or ""),
            "updatedAt": str(worker_state.get("updatedAt") or ""),
        }
        _clear_uninterruptible_observation(process)
        recovered.append(recovery)
        _json_log({"event": "register_worker_flow_slot_recovered_from_uninterruptible_state", **recovery})
    return recovered


def _recover_stale_role_workers(
    *,
    processes: dict[int, Any],
    shared_root: Path,
    instance_id: str,
    active_flow_counts: Any,
    active_flow_owners: Any,
    active_flow_lock: Any,
    threshold_seconds: float,
    worker_state_matches: Callable[[dict[str, Any]], bool],
    event_name: str,
    termination_failed_event_name: str,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    try:
        threshold_seconds = float(threshold_seconds or 0.0)
    except (TypeError, ValueError):
        return []
    if not math.isfinite(threshold_seconds) or threshold_seconds <= 0.0:
        return []
    now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    recovered: list[dict[str, Any]] = []
    for worker_id, process in list(processes.items()):
        is_alive = getattr(process, "is_alive", None)
        try:
            if callable(is_alive) and not is_alive():
                continue
        except Exception:
            continue
        worker_label = _worker_label_from_id(worker_id)
        worker_state = _read_worker_state(
            shared_root=shared_root,
            instance_id=instance_id,
            worker_label=worker_label,
        )
        if not worker_state_matches(worker_state):
            continue
        timestamp = _parse_utc_timestamp(worker_state.get("updatedAt")) or _parse_utc_timestamp(
            worker_state.get("startedAt")
        )
        if timestamp is None:
            continue
        age_seconds = (now_utc - timestamp).total_seconds()
        if age_seconds < threshold_seconds:
            continue
        try:
            owned_slot_key = str(active_flow_owners.get(worker_label, "") or "").strip()
        except Exception:
            owned_slot_key = ""
        if not owned_slot_key:
            continue
        try:
            pid = int(getattr(process, "pid", 0) or 0)
        except (TypeError, ValueError):
            pid = 0
        terminate_signal_sent = False
        kill_signal_sent = False
        terminate = getattr(process, "terminate", None)
        if callable(terminate):
            try:
                terminate()
                terminate_signal_sent = True
            except Exception:
                terminate_signal_sent = False
        join = getattr(process, "join", None)
        if callable(join):
            try:
                join(timeout=DEFAULT_ROLE_WORKER_TERMINATE_GRACE_SECONDS)
            except Exception:
                pass
        try:
            process_alive = bool(process.is_alive())
        except Exception:
            process_alive = True
        if process_alive:
            kill = getattr(process, "kill", None)
            if callable(kill):
                try:
                    kill()
                    kill_signal_sent = True
                except Exception:
                    kill_signal_sent = False
            if callable(join):
                try:
                    join(timeout=DEFAULT_ROLE_WORKER_TERMINATE_GRACE_SECONDS)
                except Exception:
                    pass
            try:
                process_alive = bool(process.is_alive())
            except Exception:
                process_alive = True
        termination_confirmed = not process_alive
        common_details = {
            "workerId": worker_label,
            "pid": pid,
            "slotKey": owned_slot_key,
            "staleSeconds": age_seconds,
            "thresholdSeconds": threshold_seconds,
            "terminateSignalSent": terminate_signal_sent,
            "killSignalSent": kill_signal_sent,
            "terminationConfirmed": termination_confirmed,
            "currentTaskRole": str(worker_state.get("currentTaskRole") or ""),
            "currentFlowName": str(worker_state.get("currentFlowName") or ""),
            "currentOutputDir": str(worker_state.get("currentOutputDir") or ""),
            "updatedAt": str(worker_state.get("updatedAt") or ""),
        }
        if not termination_confirmed:
            _json_log({"event": termination_failed_event_name, **common_details})
            continue
        released_slot_key = release_flow_slot_for_owner(
            owner_id=worker_label,
            active_flow_counts=active_flow_counts,
            active_flow_owners=active_flow_owners,
            active_flow_lock=active_flow_lock,
        )
        recovery = {
            **common_details,
            "slotKey": released_slot_key or owned_slot_key,
            "slotReleased": bool(released_slot_key),
        }
        recovered.append(recovery)
        _json_log({"event": event_name, **recovery})
    return recovered


def recover_stale_continue_workers(
    *,
    processes: dict[int, Any],
    shared_root: Path,
    instance_id: str,
    active_flow_counts: Any,
    active_flow_owners: Any,
    active_flow_lock: Any,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    return _recover_stale_role_workers(
        processes=processes,
        shared_root=shared_root,
        instance_id=instance_id,
        active_flow_counts=active_flow_counts,
        active_flow_owners=active_flow_owners,
        active_flow_lock=active_flow_lock,
        threshold_seconds=continue_worker_hard_timeout_seconds(),
        worker_state_matches=_worker_state_is_continue,
        event_name="register_continue_worker_hard_timeout_recovered",
        termination_failed_event_name="register_continue_worker_hard_timeout_termination_failed",
        now=now,
    )


def recover_stale_account_audit_workers(
    *,
    processes: dict[int, Any],
    shared_root: Path,
    instance_id: str,
    active_flow_counts: Any,
    active_flow_owners: Any,
    active_flow_lock: Any,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    return _recover_stale_role_workers(
        processes=processes,
        shared_root=shared_root,
        instance_id=instance_id,
        active_flow_counts=active_flow_counts,
        active_flow_owners=active_flow_owners,
        active_flow_lock=active_flow_lock,
        threshold_seconds=account_audit_worker_hard_timeout_seconds(),
        worker_state_matches=_worker_state_is_account_audit,
        event_name="register_account_audit_worker_hard_timeout_recovered",
        termination_failed_event_name="register_account_audit_worker_hard_timeout_termination_failed",
        now=now,
    )


def cleanup_process_handle(*, process: Any, join_timeout: float = 0.0, terminate_if_alive: bool = False) -> None:
    join = getattr(process, "join", None)
    if callable(join):
        try:
            join(timeout=max(0.0, float(join_timeout or 0.0)))
        except Exception:
            pass
    if terminate_if_alive:
        is_alive = getattr(process, "is_alive", None)
        terminate = getattr(process, "terminate", None)
        if callable(is_alive) and callable(terminate):
            try:
                if is_alive():
                    terminate()
            except Exception:
                pass
        if callable(join):
            try:
                join(timeout=1.0)
            except Exception:
                pass
    close = getattr(process, "close", None)
    if callable(close):
        try:
            close()
        except Exception:
            pass


def should_stop_supervisor_after_worker_stop(
    *,
    processes: dict[int, Any],
    task_counter: Any,
    max_runs: int,
) -> bool:
    if processes:
        return False
    return task_slots_exhausted(task_counter=task_counter, max_runs=max_runs)


def main() -> int:
    preflight_summary = _validate_runtime_preflight()
    _json_log({"event": "register_runtime_preflight_ok", **preflight_summary})
    config = RunnerMainConfig.from_env()
    output_root = config.output_root
    _ensure_directory(output_root)
    shared_root = config.shared_root
    _ensure_directory(config.openai_oauth_pool_dir)
    _ensure_directory(config.free_oauth_pool_dir)

    ctx = mp.get_context("spawn")
    manager = ctx.Manager()
    stop_event = ctx.Event()
    task_counter = ctx.Value("i", 0)
    active_flow_counts = manager.dict()
    active_flow_owners = manager.dict()
    active_flow_lock = ctx.Lock()
    processes: dict[int, Any] = {}
    dashboard_server = None
    shutdown_requested = False
    cleanup_dashboard_worker_state_files(shared_root=shared_root, instance_id=config.instance_id)
    service_state = ServiceRuntimeState(
        shared_root=shared_root,
        instance_id=config.instance_id,
        instance_role=config.instance_role,
        flow_path=config.flow_path,
        output_root=str(output_root),
        worker_count=config.worker_count,
        delay_seconds=config.delay_seconds,
        worker_stagger_seconds=config.worker_stagger_seconds,
        openai_oauth_pool_dir=str(config.openai_oauth_pool_dir),
        flow_specs=[flow_spec_summary(spec) for spec in config.flow_specs],
    )

    install_signal_handlers(stop_event=stop_event)
    service_state.started(pid=os.getpid(), max_runs=config.max_runs)
    dashboard_server = start_dashboard_server_if_enabled(
        output_root=output_root,
        easy_protocol_base_url=config.easy_protocol_base_url,
        easy_protocol_token=config.easy_protocol_control_token,
        easy_protocol_actor=config.easy_protocol_control_actor,
    )
    _json_log(
        {
            "event": "register_supervisor_started",
            "pid": os.getpid(),
            "instanceId": config.instance_id,
            "instanceRole": config.instance_role,
            "workerCount": config.worker_count,
            "delaySeconds": config.delay_seconds,
            "workerStaggerSeconds": config.worker_stagger_seconds,
            "maxRuns": config.max_runs,
            "outputRoot": str(output_root),
            "flowSpecs": [flow_spec_summary(spec) for spec in config.flow_specs],
            "openaiOauthPoolDir": str(config.openai_oauth_pool_dir),
            "smallSuccessPoolDir": str(config.openai_oauth_pool_dir),
            "freeOauthPoolDir": str(config.free_oauth_pool_dir),
        }
    )

    try:
        for worker_id in range(1, config.worker_count + 1):
            if stop_event.is_set():
                break
            processes[worker_id] = start_worker(
                ctx=ctx,
                worker_id=worker_id,
                instance_id=config.instance_id,
                instance_role=config.instance_role,
                output_root_text=str(output_root),
                delay_seconds=config.delay_seconds,
                max_runs=config.max_runs,
                task_max_attempts=config.task_max_attempts,
                flow_specs=config.flow_specs,
                stop_event=stop_event,
                task_counter=task_counter,
                free_oauth_pool_dir_text=str(config.free_oauth_pool_dir),
                active_flow_counts=active_flow_counts,
                active_flow_lock=active_flow_lock,
                active_flow_owners=active_flow_owners,
            )
            if config.worker_stagger_seconds > 0 and worker_id < config.worker_count:
                time.sleep(config.worker_stagger_seconds)

        while processes:
            if shutdown_requested:
                break
            if stop_event.is_set():
                break
            recover_stale_uninterruptible_worker_slots(
                processes=processes,
                shared_root=shared_root,
                instance_id=config.instance_id,
                active_flow_counts=active_flow_counts,
                active_flow_owners=active_flow_owners,
                active_flow_lock=active_flow_lock,
                stale_seconds=flow_slot_uninterruptible_stale_seconds(),
            )
            recover_stale_account_audit_workers(
                processes=processes,
                shared_root=shared_root,
                instance_id=config.instance_id,
                active_flow_counts=active_flow_counts,
                active_flow_owners=active_flow_owners,
                active_flow_lock=active_flow_lock,
            )
            recover_stale_continue_workers(
                processes=processes,
                shared_root=shared_root,
                instance_id=config.instance_id,
                active_flow_counts=active_flow_counts,
                active_flow_owners=active_flow_owners,
                active_flow_lock=active_flow_lock,
            )
            for worker_id, process in list(processes.items()):
                if process.is_alive():
                    continue
                exit_code = int(process.exitcode or 0)
                processes.pop(worker_id, None)
                _json_log(
                    {
                        "event": "register_worker_stopped",
                        "workerId": f"worker-{worker_id:02d}",
                        "pid": process.pid,
                        "exitCode": exit_code,
                    }
                )
                recovered_slot_key = release_flow_slot_for_owner(
                    owner_id=f"worker-{worker_id:02d}",
                    active_flow_counts=active_flow_counts,
                    active_flow_owners=active_flow_owners,
                    active_flow_lock=active_flow_lock,
                )
                if recovered_slot_key:
                    _json_log(
                        {
                            "event": "register_worker_flow_slot_recovered",
                            "workerId": f"worker-{worker_id:02d}",
                            "pid": process.pid,
                            "exitCode": exit_code,
                            "slotKey": recovered_slot_key,
                        }
                    )
                cleanup_process_handle(process=process, join_timeout=0.0)
                if should_stop_supervisor_after_worker_stop(
                    processes=processes,
                    task_counter=task_counter,
                    max_runs=config.max_runs,
                ):
                    _json_log(
                        {
                            "event": "register_supervisor_max_runs_reached",
                            "pid": os.getpid(),
                            "instanceId": config.instance_id,
                            "taskCount": task_counter_value(task_counter),
                        }
                    )
                    shutdown_requested = True
                    break
                if stop_event.is_set():
                    continue
                if task_slots_exhausted(task_counter=task_counter, max_runs=config.max_runs):
                    continue
                _json_log(
                    {
                        "event": "register_worker_restarting",
                        "workerId": f"worker-{worker_id:02d}",
                    }
                )
                processes[worker_id] = start_worker(
                    ctx=ctx,
                    worker_id=worker_id,
                    instance_id=config.instance_id,
                    instance_role=config.instance_role,
                    output_root_text=str(output_root),
                    delay_seconds=config.delay_seconds,
                    max_runs=config.max_runs,
                    task_max_attempts=config.task_max_attempts,
                    flow_specs=config.flow_specs,
                    stop_event=stop_event,
                    task_counter=task_counter,
                    free_oauth_pool_dir_text=str(config.free_oauth_pool_dir),
                    active_flow_counts=active_flow_counts,
                    active_flow_lock=active_flow_lock,
                    active_flow_owners=active_flow_owners,
                )
                if config.worker_stagger_seconds > 0:
                    time.sleep(config.worker_stagger_seconds)
            if shutdown_requested:
                break
            if processes:
                time.sleep(1.0)
    finally:
        _json_log(
            {
                "event": "register_supervisor_finally_entered",
                "pid": os.getpid(),
                "instanceId": config.instance_id,
                "remainingProcesses": len(processes),
                "taskCount": task_counter_value(task_counter),
                "stopEventSet": bool(stop_event.is_set()),
            }
        )
        if processes:
            stop_event.set()
        shutdown_deadline = time.monotonic() + 15.0
        for process in processes.values():
            remaining = max(0.0, shutdown_deadline - time.monotonic())
            if remaining <= 0:
                break
            process.join(timeout=min(remaining, 2.0))
        for process in processes.values():
            cleanup_process_handle(process=process, join_timeout=0.0, terminate_if_alive=True)
        _json_log(
            {
                "event": "register_supervisor_stopped",
                "pid": os.getpid(),
                "instanceId": config.instance_id,
                "taskCount": task_counter_value(task_counter),
            }
        )
        service_state.stopped(pid=os.getpid(), task_count=task_counter_value(task_counter))
        if dashboard_server is not None:
            dashboard_server.stop()
        try:
            manager.shutdown()
        except Exception:
            pass
    return 0
