from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from others.common import write_json_atomic as _write_json_atomic


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso8601(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        normalized = text.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    except Exception:
        return None


def json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def dashboard_state_root(shared_root: Path) -> Path:
    return shared_root / "others" / "dashboard-state"


def instance_root(shared_root: Path, instance_id: str) -> Path:
    return dashboard_state_root(shared_root) / str(instance_id or "default").strip()


def worker_state_path(shared_root: Path, instance_id: str, worker_id: str) -> Path:
    return instance_root(shared_root, instance_id) / "workers" / f"{worker_id}.json"


def service_state_path(shared_root: Path, instance_id: str) -> Path:
    return instance_root(shared_root, instance_id) / "service.json"


def prune_recent_uploads(items: list[dict[str, Any]], *, keep: int = 50) -> list[dict[str, Any]]:
    out = []
    for item in items:
        if not isinstance(item, dict):
            continue
        out.append(item)
    if len(out) <= keep:
        return out
    return out[-keep:]


class WorkerRuntimeState:
    def __init__(self, *, shared_root: Path, instance_id: str, instance_role: str, worker_id: str) -> None:
        self._path = worker_state_path(shared_root, instance_id, worker_id)
        self._state = read_json(self._path)
        self._state.setdefault("instanceId", instance_id)
        self._state.setdefault("instanceRole", instance_role)
        self._state.setdefault("workerId", worker_id)
        self._state.setdefault("recentUploads", [])

    def _save(self) -> None:
        _write_json_atomic(self._path, self._state, json_default=json_default)

    def started(self, *, pid: int, output_root: str, team_auth_pinned: bool) -> None:
        self._state.update(
            {
                "pid": int(pid),
                "outputRoot": str(output_root),
                "teamAuthPinned": bool(team_auth_pinned),
                "status": "idle",
                "updatedAt": utcnow().isoformat(),
            }
        )
        self._save()

    def run_started(self, *, task_index: int, local_run_index: int, started_at: str, output_dir: str, team_auth_path: str, team_auth_pool_size: int) -> None:
        self._state.update(
            {
                "status": "running",
                "taskIndex": int(task_index),
                "localRunIndex": int(local_run_index),
                "startedAt": str(started_at),
                "currentOutputDir": str(output_dir),
                "teamAuthPath": str(team_auth_path or ""),
                "teamAuthPoolSize": int(team_auth_pool_size),
                "updatedAt": utcnow().isoformat(),
            }
        )
        self._save()

    def run_finished(self, *, task_index: int, result: dict[str, Any], output_dir: str, finished_at: str) -> None:
        upload_output = {}
        outputs = result.get("outputs") if isinstance(result, dict) else {}
        if isinstance(outputs, dict):
            upload_candidate = outputs.get("upload-oauth-artifact")
            if isinstance(upload_candidate, dict):
                upload_output = upload_candidate

        recent_uploads = list(self._state.get("recentUploads") or [])
        if bool(result.get("ok")) and bool(upload_output.get("ok")):
            recent_uploads.append(
                {
                    "finishedAt": str(finished_at),
                    "taskIndex": int(task_index),
                    "objectKey": str(upload_output.get("object_key") or ""),
                    "bucket": str(upload_output.get("bucket") or ""),
                    "targetFolder": str(upload_output.get("target_folder") or ""),
                }
            )
        self._state.update(
            {
                "status": "idle" if bool(result.get("ok")) else "failed",
                "taskIndex": int(task_index),
                "currentOutputDir": "",
                "lastFinishedAt": str(finished_at),
                "lastResult": {
                    "ok": bool(result.get("ok")),
                    "errorStep": str(result.get("errorStep") or ""),
                    "error": str(result.get("error") or ""),
                    "stepAttempts": result.get("stepAttempts") if isinstance(result.get("stepAttempts"), dict) else {},
                    "uploadOk": bool(upload_output.get("ok")),
                    "uploadObjectKey": str(upload_output.get("object_key") or ""),
                },
                "recentUploads": prune_recent_uploads(recent_uploads),
                "updatedAt": utcnow().isoformat(),
            }
        )
        self._save()

    def run_crashed(self, *, task_index: int, output_dir: str, error: str, finished_at: str) -> None:
        self._state.update(
            {
                "status": "crashed",
                "taskIndex": int(task_index),
                "currentOutputDir": "",
                "lastFinishedAt": str(finished_at),
                "lastResult": {
                    "ok": False,
                    "errorStep": "",
                    "error": str(error or ""),
                    "uploadOk": False,
                    "uploadObjectKey": "",
                },
                "updatedAt": utcnow().isoformat(),
            }
        )
        self._save()

    def sleeping(self, *, task_index: int, seconds: float) -> None:
        self._state.update(
            {
                "status": "sleeping",
                "taskIndex": int(task_index),
                "sleepSeconds": float(seconds),
                "updatedAt": utcnow().isoformat(),
            }
        )
        self._save()

    def exited(self, *, local_runs: int) -> None:
        self._state.update(
            {
                "status": "exited",
                "localRuns": int(local_runs),
                "updatedAt": utcnow().isoformat(),
            }
        )
        self._save()


class ServiceRuntimeState:
    def __init__(
        self,
        *,
        shared_root: Path,
        instance_id: str,
        instance_role: str,
        flow_path: str,
        output_root: str,
        worker_count: int,
        delay_seconds: float,
        worker_stagger_seconds: float,
        small_success_pool_dir: str,
    ) -> None:
        self._path = service_state_path(shared_root, instance_id)
        self._state = read_json(self._path)
        self._state.update(
            {
                "instanceId": instance_id,
                "instanceRole": instance_role,
                "flowPath": str(flow_path or ""),
                "outputRoot": str(output_root),
                "workerCountConfigured": int(worker_count),
                "delaySeconds": float(delay_seconds),
                "workerStaggerSeconds": float(worker_stagger_seconds),
                "smallSuccessPoolDir": str(small_success_pool_dir),
            }
        )

    def _save(self) -> None:
        _write_json_atomic(self._path, self._state, json_default=json_default)

    def started(self, *, pid: int, max_runs: int) -> None:
        self._state.update(
            {
                "pid": int(pid),
                "status": "running",
                "maxRuns": int(max_runs),
                "startedAt": utcnow().isoformat(),
                "updatedAt": utcnow().isoformat(),
            }
        )
        self._save()

    def stopped(self, *, pid: int, task_count: int) -> None:
        self._state.update(
            {
                "pid": int(pid),
                "status": "stopped",
                "taskCount": int(task_count),
                "stoppedAt": utcnow().isoformat(),
                "updatedAt": utcnow().isoformat(),
            }
        )
        self._save()
