from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .artifact_transfer import (
    copy_artifact_to_dir,
    copy_delete_artifact_to_dir,
    move_artifact_to_dir,
)
from .common import (
    canonical_free_artifact_name,
    canonical_team_artifact_name,
    standardize_export_credential_payload,
    write_json_atomic,
)


@dataclass(frozen=True)
class PreparedArtifact:
    source_path: Path
    payload: dict[str, Any]
    preferred_name: str


def load_artifact_json_quiet(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def prepare_named_artifact(*, source_path: Path, preferred_name: str) -> PreparedArtifact:
    resolved_source = Path(source_path).resolve()
    payload = standardize_export_credential_payload(load_artifact_json_quiet(resolved_source))
    return PreparedArtifact(
        source_path=resolved_source,
        payload=payload,
        preferred_name=str(preferred_name or "").strip() or resolved_source.name,
    )


def prepare_free_artifact(*, source_path: Path) -> PreparedArtifact:
    resolved_source = Path(source_path).resolve()
    payload = standardize_export_credential_payload(load_artifact_json_quiet(resolved_source))
    return PreparedArtifact(
        source_path=resolved_source,
        payload=payload,
        preferred_name=canonical_free_artifact_name(payload),
    )


def prepare_team_artifact(*, source_path: Path, is_mother: bool) -> PreparedArtifact:
    resolved_source = Path(source_path).resolve()
    payload = standardize_export_credential_payload(load_artifact_json_quiet(resolved_source))
    return PreparedArtifact(
        source_path=resolved_source,
        payload=payload,
        preferred_name=canonical_team_artifact_name(payload, is_mother=is_mother),
    )


def prepare_artifact_for_folder(
    *,
    source_path: Path,
    target_folder: str,
    is_mother: bool = False,
) -> PreparedArtifact:
    if str(target_folder or "").strip().lower() == "codex-team":
        return prepare_team_artifact(source_path=source_path, is_mother=is_mother)
    return prepare_free_artifact(source_path=source_path)


def write_prepared_artifact(
    prepared: PreparedArtifact,
    *,
    path: Path | None = None,
) -> None:
    write_json_atomic(
        Path(path or prepared.source_path),
        prepared.payload,
        include_pid=True,
        cleanup_temp=True,
    )


def copy_prepared_artifact_to_dir(
    prepared: PreparedArtifact,
    *,
    destination_dir: Path,
    overwrite_existing: bool = False,
) -> str:
    stored_path = copy_artifact_to_dir(
        source_path=prepared.source_path,
        destination_dir=destination_dir,
        preferred_name=prepared.preferred_name,
        overwrite_existing=overwrite_existing,
    )
    write_prepared_artifact(prepared, path=Path(stored_path))
    return stored_path


def move_prepared_artifact_to_dir(
    prepared: PreparedArtifact,
    *,
    destination_dir: Path,
    overwrite_existing: bool = False,
) -> str:
    stored_path = move_artifact_to_dir(
        source_path=prepared.source_path,
        destination_dir=destination_dir,
        preferred_name=prepared.preferred_name,
        overwrite_existing=overwrite_existing,
    )
    write_prepared_artifact(prepared, path=Path(stored_path))
    return stored_path


def copy_delete_prepared_artifact_to_dir(
    prepared: PreparedArtifact,
    *,
    destination_dir: Path,
    overwrite_existing: bool = False,
) -> str:
    return copy_delete_artifact_to_dir(
        source_path=prepared.source_path,
        destination_dir=destination_dir,
        preferred_name=prepared.preferred_name,
        overwrite_existing=overwrite_existing,
        payload_override=prepared.payload,
    )


def store_local_prepared_artifact(
    prepared: PreparedArtifact,
    *,
    destination_dir: Path,
    overwrite_existing: bool = False,
    move: bool = False,
) -> str:
    if move:
        return move_prepared_artifact_to_dir(
            prepared,
            destination_dir=destination_dir,
            overwrite_existing=overwrite_existing,
        )
    return copy_prepared_artifact_to_dir(
        prepared,
        destination_dir=destination_dir,
        overwrite_existing=overwrite_existing,
    )


def stage_prepared_artifact_for_upload(
    prepared: PreparedArtifact,
    *,
    staging_dir: Path | None = None,
    overwrite_existing: bool = False,
) -> Path:
    if staging_dir is None:
        write_prepared_artifact(prepared)
        return prepared.source_path
    staged_path = copy_prepared_artifact_to_dir(
        prepared,
        destination_dir=staging_dir,
        overwrite_existing=overwrite_existing,
    )
    return Path(staged_path)


def delete_artifact_quiet(path: Path) -> None:
    try:
        Path(path).unlink(missing_ok=True)
    except Exception:
        pass


def route_prepared_artifact(
    prepared: PreparedArtifact,
    *,
    local_dir: Path | None,
    move_local: bool,
    overwrite_existing: bool,
    target_folder: str,
    upload_fn: Callable[..., dict[str, Any]],
    staging_dir: Path | None = None,
) -> dict[str, Any]:
    if local_dir is not None:
        stored_path = store_local_prepared_artifact(
            prepared,
            destination_dir=local_dir,
            overwrite_existing=overwrite_existing,
            move=move_local,
        )
        return {
            "ok": True,
            "route": "local",
            "stored_path": stored_path,
        }

    staged_path = stage_prepared_artifact_for_upload(
        prepared,
        staging_dir=staging_dir,
        overwrite_existing=overwrite_existing,
    )
    try:
        upload_result = upload_fn(
            source_path=staged_path,
            target_folder=target_folder,
            object_name=prepared.preferred_name,
        )
        upload_ok = bool(upload_result.get("ok"))
    except Exception as exc:
        upload_result = {"ok": False, "detail": str(exc)}
        upload_ok = False
    if upload_ok:
        delete_artifact_quiet(staged_path)
        return {
            "ok": True,
            "route": "uploaded",
            "object_key": str(upload_result.get("object_key") or ""),
            "staged_path": str(staged_path),
        }
    return {
        "ok": False,
        "route": "upload_failed",
        "detail": str(upload_result.get("detail") or upload_result.get("status") or "upload_failed"),
        "staged_path": str(staged_path),
    }
