from __future__ import annotations

import json
import random
import shutil
import time
import uuid
from pathlib import Path
from typing import Any, Callable

if __package__ in (None, "", "others"):
    import sys

    _CURRENT_DIR = Path(__file__).resolve().parent
    _PARENT_DIR = _CURRENT_DIR.parent
    for _candidate in (_CURRENT_DIR, _PARENT_DIR):
        candidate_text = str(_candidate)
        if candidate_text not in sys.path:
            sys.path.append(candidate_text)
    from easyprotocol_flow import dispatch_easyprotocol_step
    from errors import ErrorCodes
    from others.artifact_transfer import copy_artifact_to_dir
    from others.common import (
        ensure_directory,
        extract_account_id,
        free_manual_oauth_preserve_codes,
        free_manual_oauth_preserve_enabled,
        json_log,
        standardize_export_credential_payload,
        validate_small_success_seed_payload,
        write_json_atomic,
    )
    from others.config import ArtifactRoutingConfig, DstTaskEnvConfig
    from others.paths import resolve_shared_root as shared_root_from_output_root
    from others.prepared_artifacts import (
        merge_route_result,
        prepare_artifact_for_folder,
        prepare_free_artifact,
        prepare_team_artifact,
        route_prepared_artifact,
        summarize_route_collections,
    )
    from others.result_artifacts import (
        FREE_SMALL_SUCCESS_SOURCE_CANDIDATES,
        credential_backwrite_actions,
        first_existing_output_path,
        normalized_team_pool_artifacts,
        output_dict,
        output_text,
        result_payload,
        restored_path_for_source,
    )
    from others.storage import load_json_payload
else:
    from ..easyprotocol_flow import dispatch_easyprotocol_step
    from ..errors import ErrorCodes
    from .artifact_transfer import copy_artifact_to_dir
    from .common import (
        ensure_directory,
        extract_account_id,
        free_manual_oauth_preserve_codes,
        free_manual_oauth_preserve_enabled,
        json_log,
        standardize_export_credential_payload,
        validate_small_success_seed_payload,
        write_json_atomic,
    )
    from .config import ArtifactRoutingConfig, DstTaskEnvConfig
    from .paths import resolve_shared_root as shared_root_from_output_root
    from .prepared_artifacts import (
        merge_route_result,
        prepare_artifact_for_folder,
        prepare_free_artifact,
        prepare_team_artifact,
        route_prepared_artifact,
        summarize_route_collections,
    )
    from .result_artifacts import (
        FREE_SMALL_SUCCESS_SOURCE_CANDIDATES,
        credential_backwrite_actions,
        first_existing_output_path,
        normalized_team_pool_artifacts,
        output_dict,
        output_text,
        result_payload,
        restored_path_for_source,
    )
    from .storage import load_json_payload


def artifact_routing_config(*, output_root: Path | None = None) -> ArtifactRoutingConfig:
    return ArtifactRoutingConfig.from_env(output_root=output_root)


def should_cleanup_successful_run_output(result: Any) -> bool:
    try:
        if not bool(getattr(result, "ok", False)):
            return False
        steps = getattr(result, "steps", {}) or {}
        outputs = getattr(result, "outputs", {}) or {}
        if str(steps.get("upload-oauth-artifact") or "").strip().lower() != "ok":
            return False
        upload_output = outputs.get("upload-oauth-artifact")
        if isinstance(upload_output, dict) and bool(upload_output.get("ok")):
            return True
        return False
    except Exception:
        return False


def resolve_small_success_pool_dir(*, output_root: Path) -> Path:
    return artifact_routing_config(output_root=output_root).small_success_pool_dir


def resolve_small_success_wait_pool_dir(*, output_root: Path) -> Path:
    return artifact_routing_config(output_root=output_root).small_success_wait_pool_dir


def resolve_small_success_continue_pool_dir(*, output_root: Path) -> Path:
    return artifact_routing_config(output_root=output_root).small_success_continue_pool_dir


def resolve_free_oauth_pool_dir(*, output_root: Path) -> Path:
    return artifact_routing_config(output_root=output_root).free_oauth_pool_dir


def resolve_free_manual_oauth_pool_dir(*, output_root: Path) -> Path:
    return artifact_routing_config(output_root=output_root).free_manual_oauth_pool_dir


def resolve_free_local_dir(*, output_root: Path) -> Path:
    return artifact_routing_config(output_root=output_root).free_local_dir


def resolve_team_local_dir(*, output_root: Path) -> Path:
    return artifact_routing_config(output_root=output_root).team_local_dir


def select_local_split(*, percent: float) -> bool:
    if float(percent or 0.0) <= 0.0:
        return False
    if float(percent) >= 100.0:
        return True
    return random.random() * 100.0 < float(percent)


def small_success_wait_seconds() -> float:
    return artifact_routing_config().small_success_wait_seconds


def small_success_continue_prefill_count() -> int:
    return artifact_routing_config().small_success_continue_prefill_count


def small_success_continue_prefill_target_count() -> int:
    return artifact_routing_config().small_success_continue_prefill_target_count


def small_success_continue_prefill_min_age_seconds() -> float:
    return artifact_routing_config().small_success_continue_prefill_min_age_seconds


def free_stop_after_validate_mode() -> bool:
    return DstTaskEnvConfig.from_env().free_stop_after_validate


def _sort_file_paths_newest_first(paths: list[Path]) -> list[Path]:
    def _sort_key(path: Path) -> tuple[float, str]:
        try:
            modified_at = float(path.stat().st_mtime)
        except FileNotFoundError:
            modified_at = 0.0
        return (-modified_at, path.name.lower())

    return sorted(paths, key=_sort_key)


def _iter_small_success_artifacts(*, run_output_dir: Path) -> list[Path]:
    small_success_dir = run_output_dir / "small_success"
    if not small_success_dir.is_dir():
        return []
    return sorted(
        [path for path in small_success_dir.glob("*.json") if path.is_file()],
        key=lambda item: item.name.lower(),
    )


def copy_small_success_artifacts_to_pool(
    *,
    run_output_dir: Path,
    pool_dir: Path,
    worker_label: str,
    task_index: int,
) -> list[str]:
    source_paths = _iter_small_success_artifacts(run_output_dir=run_output_dir)
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
        valid, reason = validate_small_success_seed_payload(payload)
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
            "event": "register_small_success_collected",
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


def small_success_failure_target_pool_dir(*, output_root: Path, result_payload_value: dict[str, Any]) -> Path:
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
    if error_code == ErrorCodes.FREE_PERSONAL_WORKSPACE_MISSING:
        return resolve_small_success_wait_pool_dir(output_root=output_root)
    return resolve_small_success_pool_dir(output_root=output_root)


def drain_small_success_wait_pool(
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


def backfill_small_success_continue_pool(
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
        valid, reason = validate_small_success_seed_payload(payload)
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


def upload_artifact_to_r2(*, source_path: Path, target_folder: str, object_name: str | None = None) -> dict[str, Any]:
    config = artifact_routing_config()
    step_input = {
        "source_path": str(source_path),
        "bucket": config.r2_bucket,
        "target_folder": str(target_folder or "").strip(),
        "object_name": str(object_name or "").strip() or source_path.name,
        "account_id": config.r2_account_id,
        "endpoint_url": config.r2_endpoint_url,
        "access_key_id": config.r2_access_key_id,
        "secret_access_key": config.r2_secret_access_key,
        "region": config.r2_region,
        "public_base_url": config.r2_public_base_url,
        "overwrite": True,
    }
    return dispatch_easyprotocol_step(step_type="upload_file_to_r2", step_input=step_input)


def _free_success_artifact_path(*, result: Any) -> Path | None:
    source_paths = _iter_free_oauth_artifacts(result=result)
    if not source_paths:
        return None
    return source_paths[0]


def team_has_collectable_artifacts(*, result: Any) -> bool:
    return len(normalized_team_pool_artifacts(result=result)) > 0


def postprocess_free_success_artifact(
    *,
    result: Any,
    output_root: Path,
    worker_label: str,
    task_index: int,
    free_local_selected: bool,
) -> dict[str, Any]:
    result_payload_value = result_payload(result)
    if not _free_personal_oauth_confirmed(result_payload_value=result_payload_value):
        seed_path = first_existing_output_path(result_payload_value, FREE_SMALL_SUCCESS_SOURCE_CANDIDATES)
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
        wait_pool_dir = resolve_small_success_wait_pool_dir(output_root=output_root)
        stored_path = copy_artifact_to_dir(source_path=seed_path, destination_dir=wait_pool_dir)
        return {
            "ok": True,
            "status": "free_personal_workspace_missing_routed_to_wait_pool",
            "cleanup_run_output": True,
            "stored_path": stored_path,
            "target_dir": str(wait_pool_dir),
        }

    artifact_path = _free_success_artifact_path(result=result)
    if artifact_path is None or not artifact_path.is_file():
        return {"ok": False, "status": "missing_free_artifact", "cleanup_run_output": False}
    prepared_artifact = prepare_free_artifact(source_path=artifact_path)

    if free_local_selected:
        target_dir = resolve_free_local_dir(output_root=output_root)
        route_result = route_prepared_artifact(
            prepared_artifact,
            local_dir=target_dir,
            move_local=False,
            overwrite_existing=True,
            target_folder="codex",
            upload_fn=upload_artifact_to_r2,
        )
        return {
            "ok": True,
            "status": "stored_local",
            "cleanup_run_output": True,
            "stored_path": str(route_result.get("stored_path") or ""),
            "target_dir": str(target_dir),
        }

    if not should_cleanup_successful_run_output(result):
        return {"ok": False, "status": "free_upload_not_confirmed", "cleanup_run_output": False}

    pool_dir = resolve_free_oauth_pool_dir(output_root=output_root)
    route_result = route_prepared_artifact(
        prepared_artifact,
        local_dir=None,
        move_local=False,
        overwrite_existing=True,
        target_folder="codex",
        upload_fn=upload_artifact_to_r2,
        staging_dir=pool_dir,
    )
    if not bool(route_result.get("ok")):
        return {
            "ok": False,
            "status": "free_upload_failed",
            "cleanup_run_output": False,
            "detail": str(route_result.get("detail") or "upload_failed"),
            "transient_path": str(route_result.get("staged_path") or ""),
        }

    return {
        "ok": True,
        "status": "uploaded_deleted",
        "cleanup_run_output": True,
        "pool_dir": str(pool_dir),
        "transient_path": str(route_result.get("staged_path") or ""),
    }


def postprocess_team_success_artifacts(
    *,
    result: Any,
    output_root: Path,
) -> dict[str, Any]:
    artifacts = normalized_team_pool_artifacts(result=result)
    if not artifacts:
        return {"ok": True, "status": "idle", "cleanup_run_output": True, "artifacts": []}

    local_percent = artifact_routing_config(output_root=output_root).team_local_split_percent
    local_dir = resolve_team_local_dir(output_root=output_root)
    processed: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    for artifact in artifacts:
        source_path = Path(str(artifact.get("path") or "")).resolve()
        if not source_path.is_file():
            failures.append({"path": str(source_path), "status": "missing"})
            continue
        is_mother = str(artifact.get("kind") or "").strip().lower() == "mother"
        prepared_artifact = prepare_team_artifact(source_path=source_path, is_mother=is_mother)
        route_local = select_local_split(percent=local_percent)
        if route_local:
            route_result = route_prepared_artifact(
                prepared_artifact,
                local_dir=local_dir,
                move_local=True,
                overwrite_existing=True,
                target_folder="codex-team",
                upload_fn=upload_artifact_to_r2,
            )
            processed.append(merge_route_result(artifact, route_result))
            continue
        route_result = route_prepared_artifact(
            prepared_artifact,
            local_dir=None,
            move_local=True,
            overwrite_existing=True,
            target_folder="codex-team",
            upload_fn=upload_artifact_to_r2,
        )
        if bool(route_result.get("ok")):
            processed.append(merge_route_result(artifact, route_result))
            continue
        failures.append(merge_route_result(artifact, route_result))

    return summarize_route_collections(
        failures=failures,
        artifacts=processed,
        extra={
            "cleanup_run_output": True,
            "local_dir": str(local_dir),
        },
    )


def sync_team_member_artifacts_from_active_claims(
    *,
    output_root: Path,
) -> dict[str, Any]:
    local_percent = artifact_routing_config(output_root=output_root).team_local_split_percent
    if local_percent < 100.0:
        return {"ok": True, "status": "disabled", "localized": [], "failures": []}

    claims_dir = shared_root_from_output_root(output_root) / "others" / "team-mother-claims"
    if not claims_dir.is_dir():
        return {"ok": True, "status": "idle", "localized": [], "failures": []}

    local_dir = resolve_team_local_dir(output_root=output_root)
    localized: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    for claim_path in sorted(claims_dir.glob("*.json"), key=lambda item: item.name.lower()):
        if not claim_path.is_file():
            continue
        claim_payload = load_json_payload(claim_path)
        if not isinstance(claim_payload, dict):
            continue
        team_flow = claim_payload.get("teamFlow")
        if not isinstance(team_flow, dict):
            continue
        progress = team_flow.get("teamExpandProgress")
        if not isinstance(progress, dict):
            continue
        successful_artifacts = progress.get("successfulArtifacts")
        if not isinstance(successful_artifacts, list):
            continue
        for artifact in successful_artifacts:
            if not isinstance(artifact, dict):
                continue
            success_path_text = str(artifact.get("successPath") or "").strip()
            if not success_path_text:
                continue
            success_path = Path(success_path_text).resolve()
            if not success_path.is_file():
                continue
            try:
                prepared_artifact = prepare_team_artifact(source_path=success_path, is_mother=False)
                route_result = route_prepared_artifact(
                    prepared_artifact,
                    local_dir=local_dir,
                    move_local=False,
                    overwrite_existing=True,
                    target_folder="codex-team",
                    upload_fn=upload_artifact_to_r2,
                )
                localized.append(
                    merge_route_result(
                        {
                            "claim_path": str(claim_path),
                            "source_path": str(success_path),
                            "email": str(artifact.get("email") or "").strip(),
                        },
                        route_result,
                        include_route=False,
                        include_object_key=False,
                        include_detail=False,
                    )
                )
            except Exception as exc:
                failures.append(
                    {
                        "claim_path": str(claim_path),
                        "source_path": str(success_path),
                        "email": str(artifact.get("email") or "").strip(),
                        "detail": str(exc),
                    }
                )

    return summarize_route_collections(
        failures=failures,
        localized=localized,
        extra={"local_dir": str(local_dir)},
    )


def team_live_local_sync_loop(
    *,
    stop_event: Any,
    output_root: Path,
    worker_label: str,
) -> None:
    while not stop_event.is_set():
        try:
            sync_result = sync_team_member_artifacts_from_active_claims(output_root=output_root)
            if sync_result.get("localized") or sync_result.get("failures"):
                json_log(
                    {
                        "event": "register_team_live_local_sync",
                        "workerId": worker_label,
                        "result": sync_result,
                    }
                )
        except Exception as exc:
            json_log(
                {
                    "event": "register_team_live_local_sync_failed",
                    "workerId": worker_label,
                    "detail": str(exc),
                }
            )
        try:
            if stop_event.wait(2.0):
                break
        except Exception:
            time.sleep(2.0)


def drain_oauth_pool_backlog(
    *,
    pool_dir: Path,
    target_folder: str,
    local_percent: float = 100.0,
    local_dir: Path | None = None,
    upload_fn: Callable[..., dict[str, Any]] = upload_artifact_to_r2,
) -> dict[str, Any]:
    if not pool_dir.is_dir():
        return {"ok": True, "status": "idle", "uploaded": [], "failures": []}

    uploaded: list[dict[str, Any]] = []
    localized: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for source_path in sorted(pool_dir.glob("*.json"), key=lambda item: item.name.lower()):
        if not source_path.is_file():
            continue
        is_mother = source_path.name.lower().startswith("mother-") or source_path.name.lower().startswith("codex-team-mother-")
        prepared_artifact = prepare_artifact_for_folder(
            source_path=source_path,
            target_folder=target_folder,
            is_mother=is_mother,
        )
        if local_dir is not None and select_local_split(percent=local_percent):
            try:
                route_result = route_prepared_artifact(
                    prepared_artifact,
                    local_dir=local_dir,
                    move_local=True,
                    overwrite_existing=True,
                    target_folder=target_folder,
                    upload_fn=upload_fn,
                )
                localized.append(
                    merge_route_result(
                        {"path": str(source_path)},
                        route_result,
                        include_route=False,
                        include_object_key=False,
                        include_detail=False,
                    )
                )
                continue
            except Exception as exc:
                failures.append({"path": str(source_path), "detail": str(exc)})
                continue
        route_result = route_prepared_artifact(
            prepared_artifact,
            local_dir=None,
            move_local=True,
            overwrite_existing=True,
            target_folder=target_folder,
            upload_fn=upload_fn,
        )
        if bool(route_result.get("ok")):
            uploaded.append(
                merge_route_result(
                    {"path": str(source_path)},
                    route_result,
                    include_route=False,
                    include_stored_path=False,
                    include_detail=False,
                )
            )
            continue
        failures.append(
            merge_route_result(
                {"path": str(source_path)},
                route_result,
                include_route=False,
                include_stored_path=False,
                include_object_key=False,
            )
        )
    return summarize_route_collections(
        failures=failures,
        uploaded=uploaded,
        localized=localized,
    )


def _load_json_dict(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _payload_looks_like_oauth_credential(payload: dict[str, Any]) -> bool:
    if not isinstance(payload, dict):
        return False
    if any(str(payload.get(key) or "").strip() for key in ("access_token", "refresh_token", "id_token")):
        return True
    auth = payload.get("auth")
    if isinstance(auth, dict) and any(str(auth.get(key) or "").strip() for key in ("access_token", "refresh_token", "id_token")):
        return True
    return False


def _merge_refreshed_credential(*, original_payload: dict[str, Any], refreshed_payload: dict[str, Any]) -> dict[str, Any]:
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
    result_payload_value: dict[str, Any],
    worker_label: str,
    task_index: int,
) -> list[dict[str, Any]]:
    actions = credential_backwrite_actions(result_payload=result_payload_value)
    if not actions:
        return []

    synced: list[dict[str, Any]] = []
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
