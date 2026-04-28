from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from easyprotocol_flow import dispatch_easyprotocol_step
from errors import ErrorCodes, result_error_matches, result_error_message
from others.common import ensure_directory
from others.config import CleanupRuntimeConfig
from others.file_lock import release_lock, try_acquire_lock


def cleanup_runtime_config() -> CleanupRuntimeConfig:
    return CleanupRuntimeConfig.from_env()


def team_auth_state_dir(*, shared_root: Path) -> Path:
    return shared_root / "others" / "team-auth-state"


def team_auth_state_path(*, shared_root: Path, team_auth_path: str) -> Path:
    import hashlib

    normalized = str(Path(team_auth_path).resolve()).strip().lower()
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:16]
    return team_auth_state_dir(shared_root=shared_root) / f"{digest}.json"


def team_cleanup_state_path(*, shared_root: Path) -> Path:
    return team_auth_state_dir(shared_root=shared_root) / "codex-capacity-cleanup.json"


def team_cleanup_lock_path(*, shared_root: Path) -> Path:
    return team_auth_state_dir(shared_root=shared_root) / "codex-capacity-cleanup.lock"


def load_team_auth_state(*, shared_root: Path, team_auth_path: str) -> dict[str, Any]:
    path = team_auth_state_path(shared_root=shared_root, team_auth_path=team_auth_path)
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def load_team_cleanup_state(*, shared_root: Path) -> dict[str, Any]:
    path = team_cleanup_state_path(shared_root=shared_root)
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def write_team_auth_state(*, shared_root: Path, team_auth_path: str, payload: dict[str, Any]) -> None:
    state_dir = team_auth_state_dir(shared_root=shared_root)
    ensure_directory(state_dir)
    path = team_auth_state_path(shared_root=shared_root, team_auth_path=team_auth_path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_team_cleanup_state(*, shared_root: Path, payload: dict[str, Any]) -> None:
    state_dir = team_auth_state_dir(shared_root=shared_root)
    ensure_directory(state_dir)
    path = team_cleanup_state_path(shared_root=shared_root)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def mark_team_auth_capacity_cooldown(
    *,
    shared_root: Path,
    team_auth_path: str,
    cooldown_seconds: float,
    detail: str,
) -> None:
    now = datetime.now(timezone.utc)
    payload = load_team_auth_state(shared_root=shared_root, team_auth_path=team_auth_path)
    payload.update(
        {
            "teamAuthPath": str(Path(team_auth_path).resolve()),
            "capacityCooldownUntil": (
                now + timedelta(seconds=max(0.0, float(cooldown_seconds or 0.0)))
            ).isoformat(),
            "lastCapacityErrorAt": now.isoformat(),
            "lastCapacityDetail": str(detail or "").strip(),
        }
    )
    write_team_auth_state(shared_root=shared_root, team_auth_path=team_auth_path, payload=payload)


def clear_team_auth_capacity_cooldown(*, shared_root: Path, team_auth_path: str) -> None:
    payload = load_team_auth_state(shared_root=shared_root, team_auth_path=team_auth_path)
    if not payload:
        return
    payload["capacityCooldownUntil"] = ""
    payload["lastCapacityRecoveredAt"] = datetime.now(timezone.utc).isoformat()
    write_team_auth_state(shared_root=shared_root, team_auth_path=team_auth_path, payload=payload)


def team_auth_is_capacity_cooled(*, shared_root: Path, team_auth_path: str) -> bool:
    payload = load_team_auth_state(shared_root=shared_root, team_auth_path=team_auth_path)
    until_text = str(payload.get("capacityCooldownUntil") or "").strip()
    if not until_text:
        return False
    try:
        until = datetime.fromisoformat(until_text.replace("Z", "+00:00"))
    except ValueError:
        return False
    if until.tzinfo is None:
        until = until.replace(tzinfo=timezone.utc)
    return until > datetime.now(timezone.utc)


def all_team_auth_capacity_cooled(*, shared_root: Path, team_auth_pool: list[str]) -> bool:
    normalized_pool = [candidate for candidate in team_auth_pool if str(candidate or "").strip()]
    if not normalized_pool:
        return False
    return all(
        team_auth_is_capacity_cooled(shared_root=shared_root, team_auth_path=candidate)
        for candidate in normalized_pool
    )


def team_cleanup_recently_ran(*, shared_root: Path, cooldown_seconds: float) -> bool:
    if float(cooldown_seconds or 0.0) <= 0:
        return False
    payload = load_team_cleanup_state(shared_root=shared_root)
    timestamp_text = str(payload.get("lastFinishedAt") or payload.get("lastStartedAt") or "").strip()
    if not timestamp_text:
        return False
    try:
        timestamp = datetime.fromisoformat(timestamp_text.replace("Z", "+00:00"))
    except ValueError:
        return False
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - timestamp).total_seconds() < float(cooldown_seconds)


def try_acquire_team_cleanup_lock(*, shared_root: Path) -> bool:
    state_dir = team_auth_state_dir(shared_root=shared_root)
    ensure_directory(state_dir)
    lock_path = team_cleanup_lock_path(shared_root=shared_root)
    return try_acquire_lock(
        lock_path,
        stale_after_seconds=cleanup_runtime_config().team_cleanup_lock_stale_seconds,
    )


def release_team_cleanup_lock(*, shared_root: Path) -> None:
    release_lock(team_cleanup_lock_path(shared_root=shared_root))


def trigger_codex_capacity_cleanup(
    *,
    shared_root: Path,
    team_auth_pool: list[str],
) -> dict[str, Any]:
    normalized_pool = [candidate for candidate in team_auth_pool if str(candidate or "").strip()]
    if not normalized_pool:
        return {"ok": False, "status": "cleanup_skipped_empty_pool", "results": []}
    cooldown_seconds = cleanup_runtime_config().team_cleanup_cooldown_seconds
    if team_cleanup_recently_ran(shared_root=shared_root, cooldown_seconds=cooldown_seconds):
        payload = load_team_cleanup_state(shared_root=shared_root)
        return {
            "ok": False,
            "status": "cleanup_recently_ran",
            "results": payload.get("results") if isinstance(payload.get("results"), list) else [],
        }
    if not try_acquire_team_cleanup_lock(shared_root=shared_root):
        return {"ok": False, "status": "cleanup_locked", "results": []}
    now = datetime.now(timezone.utc)
    summary_payload: dict[str, Any] = {
        "lastStartedAt": now.isoformat(),
        "lastFinishedAt": "",
        "results": [],
    }
    write_team_cleanup_state(shared_root=shared_root, payload=summary_payload)
    try:
        results: list[dict[str, Any]] = []
        total_revoked_invites = 0
        total_removed_users = 0
        successful_workspaces = 0
        for team_auth_path in normalized_pool:
            try:
                step_result = dispatch_easyprotocol_step(
                    step_type="cleanup_codex_capacity",
                    step_input={"team_auth_path": team_auth_path},
                )
            except Exception as exc:
                step_result = {
                    "ok": False,
                    "status": "cleanup_transport_failed",
                    "detail": str(exc),
                }
            step_payload = step_result if isinstance(step_result, dict) else {"ok": False, "status": "cleanup_invalid_result"}
            results.append({"teamAuthPath": team_auth_path, **step_payload})
            if bool(step_payload.get("ok")):
                successful_workspaces += 1
                total_revoked_invites += int(step_payload.get("revoked_invites") or 0)
                total_removed_users += int(step_payload.get("removed_users") or 0)
                clear_team_auth_capacity_cooldown(shared_root=shared_root, team_auth_path=team_auth_path)
        finished_at = datetime.now(timezone.utc).isoformat()
        summary_payload = {
            "lastStartedAt": summary_payload["lastStartedAt"],
            "lastFinishedAt": finished_at,
            "successfulWorkspaces": successful_workspaces,
            "totalRevokedInvites": total_revoked_invites,
            "totalRemovedUsers": total_removed_users,
            "results": results,
        }
        write_team_cleanup_state(shared_root=shared_root, payload=summary_payload)
        return {
            "ok": successful_workspaces > 0,
            "status": "cleanup_finished",
            "successfulWorkspaces": successful_workspaces,
            "totalRevokedInvites": total_revoked_invites,
            "totalRemovedUsers": total_removed_users,
            "results": results,
        }
    finally:
        release_team_cleanup_lock(shared_root=shared_root)


def team_capacity_failure_detail(*, result_payload_value: dict[str, Any]) -> str:
    if str(result_payload_value.get("errorStep") or "").strip().lower() != "invite-codex-member":
        return ""
    if result_error_matches(result_payload_value, ErrorCodes.TEAM_SEATS_FULL, step_id="invite-codex-member"):
        return result_error_message(result_payload_value, "invite-codex-member")
    return ""
