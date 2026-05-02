from __future__ import annotations

import time
import uuid
from pathlib import Path
from typing import Any

from others.artifact_pool_paths import team_auth_runtime_config_for_step_input
from others.common import validate_openai_oauth_seed_payload
from others.storage import load_json_payload


def load_openai_oauth_seed_validation(
    path: Path,
    *,
    enforce_max_age: bool = False,
) -> tuple[bool, str, dict[str, Any]]:
    try:
        payload = load_json_payload(path)
    except Exception as exc:
        return False, f"load_failed:{exc}", {}
    ok, reason = validate_openai_oauth_seed_payload(
        payload,
        enforce_max_age=enforce_max_age,
    )
    return ok, reason, payload


def restore_to_pool(*, claimed_path: Path, pool_dir: Path, preferred_name: str) -> str:
    destination = pool_dir / preferred_name
    if destination.exists():
        destination = pool_dir / f"{destination.stem}-{uuid.uuid4().hex[:6]}{destination.suffix}"
    claimed_path.replace(destination)
    return str(destination)


def derive_original_name_from_claim(path: Path) -> str:
    name = path.name
    prefix, separator, remainder = name.partition("-")
    if separator and len(prefix) == 8:
        return remainder
    return name


def recover_stale_team_claims(
    *,
    pool_dir: Path,
    claims_dir: Path,
    stale_after_seconds: int,
) -> list[dict[str, Any]]:
    if stale_after_seconds <= 0:
        return []

    recovered: list[dict[str, Any]] = []
    now = time.time()
    for claimed_path in sorted(claims_dir.glob("*.json"), key=lambda path: path.name.lower()):
        try:
            age_seconds = max(0.0, now - claimed_path.stat().st_mtime)
        except FileNotFoundError:
            continue
        if age_seconds < stale_after_seconds:
            continue
        original_name = derive_original_name_from_claim(claimed_path)
        try:
            restored_path = restore_to_pool(
                claimed_path=claimed_path,
                pool_dir=pool_dir,
                preferred_name=original_name,
            )
        except FileNotFoundError:
            continue
        recovered.append(
            {
                "claimed_path": str(claimed_path),
                "restored_path": restored_path,
                "age_seconds": round(age_seconds, 3),
            }
        )
    return recovered


def safe_count(value: Any, default: int) -> int:
    try:
        return max(0, int(value))
    except Exception:
        return default


def team_stale_claim_seconds() -> int:
    return team_auth_runtime_config_for_step_input().stale_claim_seconds


def sort_paths_newest_first(paths: list[Path]) -> list[Path]:
    def _sort_key(path: Path) -> tuple[float, str]:
        try:
            modified_at = float(path.stat().st_mtime)
        except FileNotFoundError:
            modified_at = 0.0
        return (-modified_at, path.name.lower())

    return sorted(paths, key=_sort_key)


def choose_random_files(*, directory: Path, pattern: str, limit: int) -> list[Path]:
    candidates = [path for path in directory.glob(pattern) if path.is_file()]
    if not candidates or limit <= 0:
        return []
    return sort_paths_newest_first(candidates)[:limit]
