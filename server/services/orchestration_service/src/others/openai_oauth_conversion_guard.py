from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from others.common import ensure_directory, extract_email, json_log, standardize_export_credential_payload, write_json_atomic
from others.config import ArtifactRoutingConfig
from others.paths import resolve_others_root, resolve_team_mother_pool_dir, resolve_team_pool_dir
from others.storage import load_json_payload


OPENAI_OAUTH_CONVERSION_LOCKS_DIRNAME = "openai-oauth-conversion-locks"


def _normalize_email(value: Any) -> str:
    return str(value or "").strip().lower()


def _email_lock_key(email: str) -> str:
    normalized = _normalize_email(email)
    if not normalized:
        return "unknown-email"
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:12]
    safe = "".join(char if char.isalnum() or char in {"@", ".", "_", "-"} else "_" for char in normalized)
    return f"{digest}-{safe}"


def conversion_locks_dir(*, shared_root: Path) -> Path:
    return resolve_others_root(str(shared_root)) / OPENAI_OAUTH_CONVERSION_LOCKS_DIRNAME


def conversion_lock_path(*, shared_root: Path, email: str) -> Path:
    return conversion_locks_dir(shared_root=shared_root) / f"{_email_lock_key(email)}.json"


def _load_json_quiet(path: Path) -> dict[str, Any]:
    try:
        payload = load_json_payload(path)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _payload_email(path: Path) -> str:
    payload = _load_json_quiet(path)
    if not payload:
        return ""
    standardized = standardize_export_credential_payload(payload)
    return _normalize_email(extract_email(standardized or payload))


def _candidate_codex_success_dirs(*, shared_root: Path, output_root: Path) -> tuple[Path, ...]:
    artifact_config = ArtifactRoutingConfig.from_env(output_root=output_root)
    current_dirs = (
        artifact_config.free_oauth_pool_dir,
        artifact_config.free_local_dir,
        artifact_config.team_local_dir,
        Path(resolve_team_pool_dir(str(shared_root))).resolve(),
        Path(resolve_team_mother_pool_dir(str(shared_root))).resolve(),
    )
    future_dirs = (
        (shared_root / "codex" / "free").resolve(),
        (shared_root / "codex" / "team").resolve(),
        (shared_root / "codex" / "plus").resolve(),
    )
    deduped: list[Path] = []
    seen: set[str] = set()
    for candidate in (*current_dirs, *future_dirs):
        key = str(candidate.resolve()).lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate.resolve())
    return tuple(deduped)


def codex_success_lookup(
    *,
    shared_root: Path,
    output_root: Path,
    email: str,
) -> dict[str, Any]:
    normalized_email = _normalize_email(email)
    if not normalized_email:
        return {"exists": False, "email": "", "matches": []}

    matches: list[dict[str, str]] = []
    for directory in _candidate_codex_success_dirs(shared_root=shared_root, output_root=output_root):
        if not directory.is_dir():
            continue
        for candidate in directory.glob("*.json"):
            if not candidate.is_file():
                continue
            candidate_email = _payload_email(candidate)
            if candidate_email != normalized_email:
                continue
            matches.append(
                {
                    "email": normalized_email,
                    "path": str(candidate.resolve()),
                    "directory": str(directory),
                }
            )
    return {
        "exists": bool(matches),
        "email": normalized_email,
        "matches": matches,
    }


def prune_stale_conversion_lock(
    *,
    shared_root: Path,
    email: str,
) -> bool:
    state_path = conversion_lock_path(shared_root=shared_root, email=email)
    if not state_path.is_file():
        return False
    payload = _load_json_quiet(state_path)
    claimed_path_text = str(payload.get("claimed_path") or "").strip()
    if claimed_path_text and Path(claimed_path_text).is_file():
        return False
    state_path.unlink(missing_ok=True)
    return True


def prune_stale_conversion_locks(*, shared_root: Path) -> list[str]:
    locks_dir = conversion_locks_dir(shared_root=shared_root)
    if not locks_dir.is_dir():
        return []
    removed: list[str] = []
    for state_path in sorted(locks_dir.glob("*.json"), key=lambda item: item.name.lower()):
        payload = _load_json_quiet(state_path)
        claimed_path_text = str(payload.get("claimed_path") or "").strip()
        if claimed_path_text and Path(claimed_path_text).is_file():
            continue
        state_path.unlink(missing_ok=True)
        removed.append(str(state_path))
    return removed


def acquire_conversion_lock(
    *,
    shared_root: Path,
    email: str,
    claimed_path: Path,
    source_path: Path,
    stage: str,
    worker_label: str,
    task_index: int,
) -> dict[str, Any] | None:
    normalized_email = _normalize_email(email)
    if not normalized_email:
        return None
    ensure_directory(conversion_locks_dir(shared_root=shared_root))
    prune_stale_conversion_lock(shared_root=shared_root, email=normalized_email)
    state_path = conversion_lock_path(shared_root=shared_root, email=normalized_email)
    if state_path.exists():
        return None
    payload = {
        "email": normalized_email,
        "claimed_path": str(claimed_path.resolve()),
        "source_path": str(source_path.resolve()),
        "stage": str(stage or "").strip(),
        "worker_label": str(worker_label or "").strip(),
        "task_index": int(task_index or 0),
    }
    write_json_atomic(state_path, payload, include_pid=True, cleanup_temp=True)
    json_log(
        {
            "event": "register_openai_oauth_conversion_lock_acquired",
            "workerId": worker_label,
            "taskIndex": task_index,
            "email": normalized_email,
            "stage": payload["stage"],
            "statePath": str(state_path),
            "claimedPath": payload["claimed_path"],
        }
    )
    return {
        "state_path": str(state_path),
        **payload,
    }


def release_conversion_lock(
    *,
    shared_root: Path,
    email: str,
    claimed_path: str | Path | None = None,
    worker_label: str = "",
    task_index: int = 0,
) -> bool:
    normalized_email = _normalize_email(email)
    if not normalized_email:
        return False
    state_path = conversion_lock_path(shared_root=shared_root, email=normalized_email)
    if not state_path.is_file():
        return False
    payload = _load_json_quiet(state_path)
    locked_claimed_path = str(payload.get("claimed_path") or "").strip()
    requested_claimed_path = str(claimed_path or "").strip()
    if requested_claimed_path and locked_claimed_path and Path(locked_claimed_path).resolve() != Path(requested_claimed_path).resolve():
        return False
    state_path.unlink(missing_ok=True)
    json_log(
        {
            "event": "register_openai_oauth_conversion_lock_released",
            "workerId": str(worker_label or "").strip(),
            "taskIndex": int(task_index or 0),
            "email": normalized_email,
            "statePath": str(state_path),
            "claimedPath": requested_claimed_path or locked_claimed_path,
        }
    )
    return True
