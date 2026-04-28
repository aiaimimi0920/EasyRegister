from __future__ import annotations

import json
import shutil
from pathlib import Path

from others.common import json_log, standardize_export_credential_payload, write_json_atomic
from others.result_artifacts import credential_backwrite_actions, restored_path_for_source


def _load_json_dict(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _payload_looks_like_oauth_credential(payload: dict[str, object]) -> bool:
    if not isinstance(payload, dict):
        return False
    if any(str(payload.get(key) or "").strip() for key in ("access_token", "refresh_token", "id_token")):
        return True
    auth = payload.get("auth")
    if isinstance(auth, dict) and any(str(auth.get(key) or "").strip() for key in ("access_token", "refresh_token", "id_token")):
        return True
    return False


def _merge_refreshed_credential(*, original_payload: dict[str, object], refreshed_payload: dict[str, object]) -> dict[str, object]:
    merged = dict(original_payload or {})
    merged.update(refreshed_payload or {})
    if isinstance(original_payload.get("auth"), dict) or isinstance(refreshed_payload.get("auth"), dict):
        merged["auth"] = {
            **(dict(original_payload.get("auth") or {}) if isinstance(original_payload, dict) else {}),
            **(dict(refreshed_payload.get("auth") or {}) if isinstance(refreshed_payload, dict) else {}),
        }
    return standardize_export_credential_payload(merged)


def sync_refreshed_credentials_back_to_sources(
    *,
    result_payload_value: dict[str, object],
    worker_label: str,
    task_index: int,
) -> list[dict[str, str]]:
    actions = credential_backwrite_actions(result_payload=result_payload_value)
    if not actions:
        return []

    synced: list[dict[str, str]] = []
    for action in actions:
        refreshed_path = Path(str(action.get("refreshed_path") or "")).resolve()
        if not refreshed_path.is_file():
            continue
        source_path = Path(str(action.get("source_path") or "")).resolve()
        live_source_path = source_path if source_path.exists() else restored_path_for_source(
            result_payload=result_payload_value,
            source_path=source_path,
        )
        if live_source_path is None or not live_source_path.exists():
            continue

        original_payload = _load_json_dict(live_source_path)
        refreshed_payload = _load_json_dict(refreshed_path)
        if not refreshed_payload:
            continue
        if not bool(action.get("force")) and not _payload_looks_like_oauth_credential(original_payload):
            continue

        merged_payload = _merge_refreshed_credential(
            original_payload=original_payload,
            refreshed_payload=refreshed_payload,
        )
        write_json_atomic(live_source_path, merged_payload, include_pid=True, cleanup_temp=True)
        synced.append(
            {
                "kind": str(action.get("kind") or "").strip(),
                "source_path": str(live_source_path),
                "refreshed_path": str(refreshed_path),
            }
        )

    if synced:
        json_log(
            {
                "event": "register_credential_source_synced",
                "workerId": worker_label,
                "taskIndex": task_index,
                "count": len(synced),
                "artifacts": synced,
            }
        )
    return synced


def cleanup_run_output_dir(*, run_output_dir: Path, worker_label: str, task_index: int) -> None:
    if not run_output_dir.exists():
        return
    shutil.rmtree(run_output_dir, ignore_errors=False)
    json_log(
        {
            "event": "register_run_output_deleted",
            "workerId": worker_label,
            "taskIndex": task_index,
            "outputDir": str(run_output_dir),
        }
    )
