from __future__ import annotations

import errno
import json
import shutil
import uuid
from pathlib import Path
from typing import Any

from .common import ensure_directory


def build_unique_destination(*, destination_dir: Path, preferred_name: str) -> Path:
    destination = destination_dir / preferred_name
    if destination.exists():
        destination = destination_dir / f"{destination.stem}-{uuid.uuid4().hex[:6]}{destination.suffix}"
    return destination


def _resolve_destination_path(
    *,
    destination_dir: Path,
    target_name: str,
    overwrite_existing: bool,
) -> Path:
    ensure_directory(destination_dir)
    if overwrite_existing:
        destination = destination_dir / target_name
        destination.unlink(missing_ok=True)
        return destination
    return build_unique_destination(destination_dir=destination_dir, preferred_name=target_name)


def copy_artifact_to_dir(
    *,
    source_path: Path,
    destination_dir: Path,
    preferred_name: str | None = None,
    overwrite_existing: bool = False,
) -> str:
    target_name = str(preferred_name or "").strip() or source_path.name
    destination = _resolve_destination_path(
        destination_dir=destination_dir,
        target_name=target_name,
        overwrite_existing=overwrite_existing,
    )
    shutil.copy2(source_path, destination)
    return str(destination)


def move_artifact_to_dir(
    *,
    source_path: Path,
    destination_dir: Path,
    preferred_name: str | None = None,
    overwrite_existing: bool = False,
) -> str:
    target_name = str(preferred_name or "").strip() or source_path.name
    destination = _resolve_destination_path(
        destination_dir=destination_dir,
        target_name=target_name,
        overwrite_existing=overwrite_existing,
    )
    try:
        source_path.replace(destination)
    except OSError as exc:
        if exc.errno != errno.EXDEV and "Invalid cross-device link" not in str(exc):
            raise
        shutil.copy2(source_path, destination)
        source_path.unlink(missing_ok=True)
    return str(destination)


def copy_delete_artifact_to_dir(
    *,
    source_path: Path,
    destination_dir: Path,
    preferred_name: str | None = None,
    overwrite_existing: bool = False,
    payload_override: dict[str, Any] | None = None,
) -> str:
    target_name = str(preferred_name or "").strip() or source_path.name
    destination = _resolve_destination_path(
        destination_dir=destination_dir,
        target_name=target_name,
        overwrite_existing=overwrite_existing,
    )
    if payload_override is not None:
        destination.write_text(
            json.dumps(payload_override, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        source_path.unlink(missing_ok=True)
        return str(destination)
    shutil.copy2(source_path, destination)
    source_path.unlink(missing_ok=True)
    return str(destination)
