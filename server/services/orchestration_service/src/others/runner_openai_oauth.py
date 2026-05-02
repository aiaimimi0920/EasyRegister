from __future__ import annotations

import shutil
import time
import uuid
from pathlib import Path
from typing import Any

from errors import ErrorCodes
from others.artifact_transfer import copy_artifact_to_dir
from others.common import (
    ensure_directory,
    extract_account_id,
    free_manual_oauth_preserve_codes,
    free_manual_oauth_preserve_enabled,
    json_log,
    validate_openai_oauth_seed_payload,
)
from others.prepared_artifacts import (
    delete_artifact_quiet,
    prepare_free_artifact,
    prepare_free_artifact_from_payload,
    route_prepared_artifact,
    write_prepared_artifact,
)
from others.result_artifacts import (
    FREE_OPENAI_OAUTH_SOURCE_CANDIDATES,
    first_existing_output_path,
    output_dict,
    output_text,
    result_payload,
)
from others.runner_artifact_settings import (
    artifact_routing_config,
    resolve_free_local_dir,
    resolve_free_manual_oauth_pool_dir,
    resolve_free_oauth_pool_dir,
    resolve_openai_oauth_continue_pool_dir,
    resolve_openai_oauth_need_phone_pool_dir,
    resolve_openai_oauth_pool_dir,
    resolve_openai_oauth_success_pool_dir,
    resolve_openai_oauth_wait_pool_dir,
    select_upload_split,
    should_cleanup_successful_run_output,
    upload_artifact_to_r2,
)
from others.storage import load_json_payload


def _sort_file_paths_newest_first(paths: list[Path]) -> list[Path]:
    def _sort_key(path: Path) -> tuple[float, str]:
        try:
            modified_at = float(path.stat().st_mtime)
        except FileNotFoundError:
            modified_at = 0.0
        return (-modified_at, path.name.lower())

    return sorted(paths, key=_sort_key)


def _iter_openai_oauth_artifacts(*, run_output_dir: Path) -> list[Path]:
    openai_oauth_dir = run_output_dir / "openai_oauth"
    if not openai_oauth_dir.is_dir():
        return []
    return sorted(
        [path for path in openai_oauth_dir.glob("*.json") if path.is_file()],
        key=lambda item: item.name.lower(),
    )


def copy_openai_oauth_artifacts_to_pool(
    *,
    run_output_dir: Path,
    pool_dir: Path,
    worker_label: str,
    task_index: int,
) -> list[str]:
    source_paths = _iter_openai_oauth_artifacts(run_output_dir=run_output_dir)
    if not source_paths:
        return []
    ensure_directory(pool_dir)
    copied_paths: list[str] = []
    discarded_paths: list[dict[str, str]] = []
    for source_path in source_paths:
        try:
            payload = load_json_payload(source_path)
        except Exception as exc:
            discarded_paths.append({"source_path": str(source_path), "reason": f"load_failed:{exc}"})
            continue
        valid, reason = validate_openai_oauth_seed_payload(payload)
        if not valid:
            discarded_paths.append({"source_path": str(source_path), "reason": reason})
            continue
        destination = pool_dir / source_path.name
        if destination.exists():
            destination = pool_dir / f"{source_path.stem}-{uuid.uuid4().hex[:6]}{source_path.suffix}"
        shutil.copy2(source_path, destination)
        copied_paths.append(str(destination))
    json_log(
        {
            "event": "register_openai_oauth_collected",
            "workerId": worker_label,
            "taskIndex": task_index,
            "outputDir": str(run_output_dir),
            "poolDir": str(pool_dir),
            "count": len(copied_paths),
            "artifacts": copied_paths,
            "discardedCount": len(discarded_paths),
            "discarded": discarded_paths,
        }
    )
    return copied_paths


def route_openai_oauth_artifact(
    *,
    source_path: Path,
    destination_dir: Path,
    output_root: Path,
    target_folder: str,
    upload_percent: float,
    preferred_name: str | None = None,
    move_local: bool = False,
) -> dict[str, Any]:
    resolved_source = Path(source_path).resolve()
    artifact_name = str(preferred_name or resolved_source.name).strip() or resolved_source.name
    if select_upload_split(percent=upload_percent):
        upload_result = upload_artifact_to_r2(
            source_path=resolved_source,
            target_folder=target_folder,
            object_name=artifact_name,
        )
        if bool(upload_result.get("ok")):
            if move_local:
                delete_artifact_quiet(resolved_source)
            return {
                "ok": True,
                "route": "uploaded",
                "object_key": str(upload_result.get("object_key") or ""),
                "stored_path": "",
                "target_dir": str(destination_dir),
            }
        return {
            "ok": False,
            "route": "upload_failed",
            "detail": str(upload_result.get("detail") or upload_result.get("status") or "upload_failed"),
            "stored_path": "",
            "target_dir": str(destination_dir),
        }
    ensure_directory(destination_dir)
    if move_local:
        destination = destination_dir / artifact_name
        if destination.exists():
            destination = destination_dir / f"{destination.stem}-{uuid.uuid4().hex[:6]}{destination.suffix}"
        resolved_source.replace(destination)
        return {
            "ok": True,
            "route": "local",
            "object_key": "",
            "stored_path": str(destination),
            "target_dir": str(destination_dir),
        }
    stored_path = copy_artifact_to_dir(
        source_path=resolved_source,
        destination_dir=destination_dir,
        preferred_name=artifact_name,
        overwrite_existing=True,
    )
    return {
        "ok": True,
        "route": "local",
        "object_key": "",
        "stored_path": stored_path,
        "target_dir": str(destination_dir),
    }


def openai_oauth_failure_target_pool_dir(*, output_root: Path, result_payload_value: dict[str, Any]) -> Path:
    instance_role = str(result_payload_value.get("instanceRole") or "").strip().lower()
    error_code = str(result_payload_value.get("errorCode") or "").strip()
    if not error_code:
        error_code = str(
            (
                (result_payload_value.get("stepErrors") or {}).get(str(result_payload_value.get("errorStep") or ""))
                or {}
            ).get("code")
            or ""
        ).strip()
    if free_manual_oauth_preserve_enabled() and error_code in free_manual_oauth_preserve_codes():
        return resolve_free_manual_oauth_pool_dir(output_root=output_root)
    if instance_role == "continue":
        return resolve_openai_oauth_need_phone_pool_dir(output_root=output_root)
    return resolve_openai_oauth_continue_pool_dir(output_root=output_root)


def drain_openai_oauth_wait_pool(
    *,
    wait_pool_dir: Path,
    continue_pool_dir: Path,
    min_age_seconds: float,
) -> dict[str, Any]:
    ensure_directory(wait_pool_dir)
    ensure_directory(continue_pool_dir)
    moved: list[dict[str, Any]] = []
    now = time.time()

    for source_path in sorted(wait_pool_dir.glob("*.json"), key=lambda item: item.name.lower()):
        if not source_path.is_file():
            continue
        try:
            age_seconds = max(0.0, now - source_path.stat().st_mtime)
        except FileNotFoundError:
            continue
        if age_seconds < min_age_seconds:
            continue
        destination = continue_pool_dir / source_path.name
        if destination.exists():
            destination = continue_pool_dir / f"{source_path.stem}-{uuid.uuid4().hex[:6]}{source_path.suffix}"
        try:
            source_path.replace(destination)
        except FileNotFoundError:
            continue
        moved.append(
            {
                "source_path": str(source_path),
                "destination_path": str(destination),
                "age_seconds": round(age_seconds, 3),
            }
        )

    return {
        "ok": True,
        "status": "moved" if moved else "idle",
        "count": len(moved),
        "wait_pool_dir": str(wait_pool_dir),
        "continue_pool_dir": str(continue_pool_dir),
        "artifacts": moved,
    }


def backfill_openai_oauth_continue_pool(
    *,
    source_pool_dir: Path,
    continue_pool_dir: Path,
    max_move_count: int,
    target_count: int,
    min_age_seconds: float,
) -> dict[str, Any]:
    ensure_directory(source_pool_dir)
    ensure_directory(continue_pool_dir)
    normalized_source = str(source_pool_dir.resolve()).lower()
    normalized_continue = str(continue_pool_dir.resolve()).lower()
    if normalized_source == normalized_continue:
        return {
            "ok": True,
            "status": "skipped_same_pool",
            "count": 0,
            "source_pool_dir": str(source_pool_dir),
            "continue_pool_dir": str(continue_pool_dir),
            "artifacts": [],
            "discarded": [],
        }

    if max_move_count <= 0:
        return {
            "ok": True,
            "status": "disabled",
            "count": 0,
            "source_pool_dir": str(source_pool_dir),
            "continue_pool_dir": str(continue_pool_dir),
            "artifacts": [],
            "discarded": [],
        }

    current_continue_count = len(list(continue_pool_dir.glob("*.json")))
    move_budget = max_move_count
    if target_count > 0:
        move_budget = min(move_budget, max(0, target_count - current_continue_count))
    if move_budget <= 0:
        return {
            "ok": True,
            "status": "target_satisfied",
            "count": 0,
            "current_continue_count": current_continue_count,
            "target_count": target_count,
            "source_pool_dir": str(source_pool_dir),
            "continue_pool_dir": str(continue_pool_dir),
            "artifacts": [],
            "discarded": [],
        }

    moved: list[dict[str, Any]] = []
    discarded: list[dict[str, Any]] = []
    now = time.time()
    candidates = _sort_file_paths_newest_first([path for path in source_pool_dir.glob("*.json") if path.is_file()])
    for source_path in candidates:
        if len(moved) >= move_budget:
            break
        try:
            age_seconds = max(0.0, now - source_path.stat().st_mtime)
        except FileNotFoundError:
            continue
        if min_age_seconds > 0 and age_seconds < min_age_seconds:
            continue
        try:
            payload = load_json_payload(source_path)
        except Exception as exc:
            source_path.unlink(missing_ok=True)
            discarded.append({"source_path": str(source_path), "reason": f"load_failed:{exc}"})
            continue
        valid, reason = validate_openai_oauth_seed_payload(payload)
        if not valid:
            source_path.unlink(missing_ok=True)
            discarded.append({"source_path": str(source_path), "reason": reason})
            continue
        destination = continue_pool_dir / source_path.name
        if destination.exists():
            destination = continue_pool_dir / f"{source_path.stem}-{uuid.uuid4().hex[:6]}{source_path.suffix}"
        try:
            source_path.replace(destination)
        except FileNotFoundError:
            continue
        moved.append(
            {
                "source_path": str(source_path),
                "destination_path": str(destination),
                "email": str(payload.get("email") or "").strip(),
                "age_seconds": round(age_seconds, 3),
            }
        )

    return {
        "ok": True,
        "status": "moved" if moved else "idle",
        "count": len(moved),
        "current_continue_count": current_continue_count,
        "target_count": target_count,
        "source_pool_dir": str(source_pool_dir),
        "continue_pool_dir": str(continue_pool_dir),
        "artifacts": moved,
        "discarded": discarded,
    }


def collect_openai_oauth_success_artifacts(
    *,
    run_output_dir: Path,
    output_root: Path,
    worker_label: str,
    task_index: int,
) -> list[str]:
    return copy_openai_oauth_artifacts_to_pool(
        run_output_dir=run_output_dir,
        pool_dir=resolve_openai_oauth_success_pool_dir(output_root=output_root),
        worker_label=worker_label,
        task_index=task_index,
    )


def _iter_free_oauth_artifacts(*, result: Any) -> list[Path]:
    success_path = first_existing_output_path(result, (("obtain-codex-oauth", "successPath"),))
    if success_path is None:
        return []
    return [success_path]


def _free_oauth_account_id(*, result_payload_value: dict[str, Any]) -> str:
    return extract_account_id(output_dict(result_payload_value, "obtain-codex-oauth"))


def _free_invite_team_account_id(*, result_payload_value: dict[str, Any]) -> str:
    return output_text(result_payload_value, "invite-codex-member", "team_account_id")


def _free_personal_oauth_confirmed(*, result_payload_value: dict[str, Any]) -> bool:
    steps = result_payload_value.get("steps") if isinstance(result_payload_value, dict) else {}
    if isinstance(steps, dict) and str(steps.get("validate-free-personal-oauth") or "").strip().lower() == "ok":
        return True
    oauth_account_id = _free_oauth_account_id(result_payload_value=result_payload_value)
    team_account_id = _free_invite_team_account_id(result_payload_value=result_payload_value)
    return bool(oauth_account_id and team_account_id and oauth_account_id != team_account_id)


def _free_success_artifact_path(*, result: Any) -> Path | None:
    source_paths = _iter_free_oauth_artifacts(result=result)
    if not source_paths:
        return None
    return source_paths[0]


def _materialize_free_success_artifact_from_output(*, result: Any, output_root: Path) -> Path | None:
    oauth_output = output_dict(result, "obtain-codex-oauth")
    if not oauth_output:
        return None
    staging_dir = resolve_free_oauth_pool_dir(output_root=output_root) / "_materialized"
    ensure_directory(staging_dir)
    materialized_path = staging_dir / f"materialized-{uuid.uuid4().hex[:12]}.json"
    prepared = prepare_free_artifact_from_payload(
        source_path=materialized_path,
        payload=oauth_output,
    )
    prepared.source_path.parent.mkdir(parents=True, exist_ok=True)
    prepared.source_path.write_text("{}", encoding="utf-8")

    write_prepared_artifact(prepared)
    return prepared.source_path


def postprocess_free_success_artifact(
    *,
    result: Any,
    output_root: Path,
    worker_label: str,
    task_index: int,
    free_local_selected: bool,
) -> dict[str, Any]:
    result_payload_value = result_payload(result)
    routing_config = artifact_routing_config(output_root=output_root)
    if not _free_personal_oauth_confirmed(result_payload_value=result_payload_value):
        seed_path = first_existing_output_path(result_payload_value, FREE_OPENAI_OAUTH_SOURCE_CANDIDATES)
        if seed_path is None or not seed_path.is_file():
            return {
                "ok": False,
                "status": "free_personal_workspace_missing_seed_unavailable",
                "cleanup_run_output": False,
            }
        if free_manual_oauth_preserve_enabled():
            handoff_dir = resolve_free_manual_oauth_pool_dir(output_root=output_root)
            stored_path = copy_artifact_to_dir(source_path=seed_path, destination_dir=handoff_dir)
            seed_payload = load_json_payload(seed_path)
            return {
                "ok": True,
                "status": "free_personal_workspace_missing_preserved_for_manual_oauth",
                "cleanup_run_output": True,
                "stored_path": stored_path,
                "target_dir": str(handoff_dir),
                "email": str(seed_payload.get("email") or "").strip(),
            }
        wait_pool_dir = resolve_openai_oauth_wait_pool_dir(output_root=output_root)
        stored_path = copy_artifact_to_dir(source_path=seed_path, destination_dir=wait_pool_dir)
        return {
            "ok": True,
            "status": "free_personal_workspace_missing_routed_to_wait_pool",
            "cleanup_run_output": True,
            "stored_path": stored_path,
            "target_dir": str(wait_pool_dir),
        }

    artifact_path = _free_success_artifact_path(result=result)
    materialized_artifact_path: Path | None = None
    openai_source_path = first_existing_output_path(result_payload_value, FREE_OPENAI_OAUTH_SOURCE_CANDIDATES)
    if artifact_path is None or not artifact_path.is_file():
        materialized_artifact_path = _materialize_free_success_artifact_from_output(
            result=result,
            output_root=output_root,
        )
        artifact_path = materialized_artifact_path
    if artifact_path is None or not artifact_path.is_file() or openai_source_path is None or not openai_source_path.is_file():
        return {"ok": False, "status": "missing_free_artifact", "cleanup_run_output": False}
    prepared_artifact = prepare_free_artifact(source_path=artifact_path)
    openai_target_dir = resolve_openai_oauth_success_pool_dir(output_root=output_root)
    openai_upload_selected = select_upload_split(percent=routing_config.openai_upload_percent)
    codex_upload_selected = select_upload_split(percent=routing_config.codex_free_upload_percent)
    openai_should_upload = openai_upload_selected or codex_upload_selected

    openai_route_result = route_openai_oauth_artifact(
        source_path=openai_source_path,
        destination_dir=openai_target_dir,
        output_root=output_root,
        target_folder="openai/converted",
        upload_percent=100.0 if openai_should_upload else 0.0,
        preferred_name=openai_source_path.name,
        move_local=False,
    )
    if not bool(openai_route_result.get("ok")):
        return {
            "ok": False,
            "status": "openai_upload_failed",
            "cleanup_run_output": False,
            "detail": str(openai_route_result.get("detail") or "upload_failed"),
        }

    codex_target_dir = resolve_free_oauth_pool_dir(output_root=output_root)
    if codex_upload_selected:
        route_result = route_prepared_artifact(
            prepared_artifact,
            local_dir=None,
            move_local=False,
            overwrite_existing=True,
            target_folder="codex/free",
            upload_fn=upload_artifact_to_r2,
            staging_dir=(output_root / "others" / "codex-free-upload-staging"),
        )
        if not bool(route_result.get("ok")):
            return {
                "ok": False,
                "status": "free_upload_failed",
                "cleanup_run_output": False,
                "detail": str(route_result.get("detail") or "upload_failed"),
                "transient_path": str(route_result.get("staged_path") or ""),
            }
        if materialized_artifact_path is not None:
            delete_artifact_quiet(materialized_artifact_path)
        return {
            "ok": True,
            "status": "uploaded_deleted",
            "cleanup_run_output": True,
            "pool_dir": str(codex_target_dir),
            "openaiRoute": openai_route_result,
            "transient_path": str(route_result.get("staged_path") or ""),
        }

    route_result = route_prepared_artifact(
        prepared_artifact,
        local_dir=codex_target_dir,
        move_local=False,
        overwrite_existing=True,
        target_folder="codex/free",
        upload_fn=upload_artifact_to_r2,
    )
    if materialized_artifact_path is not None:
        delete_artifact_quiet(materialized_artifact_path)
    return {
        "ok": True,
        "status": "stored_local",
        "cleanup_run_output": True,
        "stored_path": str(route_result.get("stored_path") or ""),
        "target_dir": str(codex_target_dir),
        "openaiRoute": openai_route_result,
    }
