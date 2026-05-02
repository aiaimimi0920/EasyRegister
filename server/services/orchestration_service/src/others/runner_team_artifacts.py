from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Callable

from others.common import json_log
from others.paths import resolve_shared_root as shared_root_from_output_root
from others.prepared_artifacts import (
    delete_artifact_quiet,
    merge_route_result,
    prepare_artifact_for_folder,
    prepare_team_artifact,
    route_prepared_artifact,
    summarize_route_collections,
)
from others.result_artifacts import normalized_team_pool_artifacts, output_dict
from others.runner_artifact_settings import (
    artifact_routing_config,
    resolve_openai_oauth_success_pool_dir,
    resolve_team_local_dir,
    select_local_split,
    select_upload_split,
    upload_artifact_to_r2,
)
from others.runner_openai_oauth import route_openai_oauth_artifact
from others.storage import load_json_payload


def team_has_collectable_artifacts(*, result: Any) -> bool:
    return len(normalized_team_pool_artifacts(result)) > 0


def postprocess_team_success_artifacts(
    *,
    result: Any,
    output_root: Path,
) -> dict[str, Any]:
    artifacts = normalized_team_pool_artifacts(result)
    if not artifacts:
        return {"ok": True, "status": "idle", "cleanup_run_output": True, "artifacts": []}

    local_percent = artifact_routing_config(output_root=output_root).team_local_split_percent
    upload_percent = artifact_routing_config(output_root=output_root).codex_team_upload_percent
    openai_upload_percent = artifact_routing_config(output_root=output_root).openai_upload_percent
    local_dir = resolve_team_local_dir(output_root=output_root)
    openai_converted_dir = resolve_openai_oauth_success_pool_dir(output_root=output_root)
    finalize_output = output_dict(result, "finalize-team-batch")
    restored_items = finalize_output.get("restored") if isinstance(finalize_output, dict) else []
    openai_by_email: dict[str, Path] = {}
    if isinstance(restored_items, list):
        for item in restored_items:
            if not isinstance(item, dict):
                continue
            restored_path_text = str(item.get("restored_path") or "").strip()
            email = str(item.get("email") or "").strip().lower()
            if not restored_path_text or not email:
                continue
            restored_path = Path(restored_path_text).resolve()
            if not restored_path.is_file():
                continue
            if restored_path.parent.resolve() != openai_converted_dir.resolve():
                continue
            openai_by_email[email] = restored_path
    processed: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    for artifact in artifacts:
        source_path = Path(str(artifact.get("path") or "")).resolve()
        if not source_path.is_file():
            failures.append({"path": str(source_path), "status": "missing"})
            continue
        is_mother = str(artifact.get("kind") or "").strip().lower() == "mother"
        prepared_artifact = prepare_team_artifact(source_path=source_path, is_mother=is_mother)
        artifact_email = str(artifact.get("email") or "").strip().lower()
        openai_source_path = openai_by_email.get(artifact_email)
        openai_should_upload = False
        if openai_source_path is not None:
            openai_should_upload = select_upload_split(percent=openai_upload_percent)
        codex_should_upload = select_upload_split(percent=upload_percent)
        route_local = not codex_should_upload and not openai_should_upload and select_local_split(percent=local_percent)
        if route_local:
            if source_path.parent.resolve() == local_dir.resolve():
                route_result = {
                    "ok": True,
                    "route": "local",
                    "stored_path": str(source_path),
                }
            else:
                route_result = route_prepared_artifact(
                    prepared_artifact,
                    local_dir=local_dir,
                    move_local=True,
                    overwrite_existing=True,
                    target_folder="codex/team",
                    upload_fn=upload_artifact_to_r2,
                )
            processed.append(merge_route_result(artifact, route_result))
            continue
        if openai_source_path is not None and (openai_should_upload or codex_should_upload):
            openai_route_result = route_openai_oauth_artifact(
                source_path=openai_source_path,
                destination_dir=openai_converted_dir,
                output_root=output_root,
                target_folder="openai/converted",
                upload_percent=100.0,
                preferred_name=openai_source_path.name,
                move_local=True,
            )
            if not bool(openai_route_result.get("ok")):
                failures.append(
                    {
                        "path": str(source_path),
                        "email": artifact_email,
                        "detail": str(openai_route_result.get("detail") or "openai_upload_failed"),
                    }
                )
                continue
        if codex_should_upload:
            route_result = route_prepared_artifact(
                prepared_artifact,
                local_dir=None,
                move_local=False,
                overwrite_existing=True,
                target_folder="codex/team",
                upload_fn=upload_artifact_to_r2,
                staging_dir=(output_root / "others" / "codex-team-upload-staging"),
            )
            if bool(route_result.get("ok")):
                delete_artifact_quiet(source_path)
        else:
            if source_path.parent.resolve() == local_dir.resolve():
                route_result = {
                    "ok": True,
                    "route": "local",
                    "stored_path": str(source_path),
                }
            else:
                route_result = route_prepared_artifact(
                    prepared_artifact,
                    local_dir=local_dir,
                    move_local=True,
                    overwrite_existing=True,
                    target_folder="codex/team",
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
