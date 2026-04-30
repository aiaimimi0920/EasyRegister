from __future__ import annotations

import multiprocessing as mp
import os
import signal
import time
from pathlib import Path
from typing import Any

from dashboard_server import ServiceRuntimeState, start_dashboard_server_if_enabled
from others.common import ensure_directory as _ensure_directory
from others.common import json_log as _json_log
from others.config import RunnerMainConfig
from others.runner_flow_scheduler import flow_spec_summary
from others.preflight import validate_runtime_preflight as _validate_runtime_preflight
from others.runner_worker_loop import worker_loop


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
    _ensure_directory(config.small_success_pool_dir)
    _ensure_directory(config.free_oauth_pool_dir)

    ctx = mp.get_context("spawn")
    stop_event = ctx.Event()
    task_counter = ctx.Value("i", 0)
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
        small_success_pool_dir=str(config.small_success_pool_dir),
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
            "smallSuccessPoolDir": str(config.small_success_pool_dir),
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
            )
            if config.worker_stagger_seconds > 0 and worker_id < config.worker_count:
                time.sleep(config.worker_stagger_seconds)

        while processes:
            if shutdown_requested:
                break
            if stop_event.is_set():
                break
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
    return 0
