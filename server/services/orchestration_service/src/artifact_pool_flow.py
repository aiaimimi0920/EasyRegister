from __future__ import annotations

import json
import shutil
import os
import time
import uuid
import base64
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from others.paths import (
    resolve_free_manual_oauth_pool_dir,
    resolve_shared_root,
    resolve_small_success_claims_dir,
    resolve_small_success_continue_pool_dir,
    resolve_small_success_pool_dir,
    resolve_small_success_wait_pool_dir,
    resolve_team_member_claims_dir,
    resolve_team_mother_cooldowns_dir,
    resolve_team_mother_claims_dir,
    resolve_team_mother_pool_dir,
    resolve_team_pool_dir,
    resolve_team_post_pool_dir,
    resolve_team_pre_pool_dir,
)
from others.storage import load_json_payload


def _ensure_directory(path: Path) -> None:
    target = Path(path)
    for _ in range(5):
        try:
            os.makedirs(target, exist_ok=True)
            if target.is_dir():
                return
        except FileExistsError:
            if target.is_dir():
                return
            raise
        except FileNotFoundError:
            time.sleep(0.02)
            continue
        time.sleep(0.02)
    os.makedirs(target, exist_ok=True)


def _derive_output_root_from_run_dir(output_dir: str | None) -> Path:
    if str(output_dir or "").strip():
        run_dir = Path(str(output_dir)).resolve()
        if run_dir.name.startswith("run-") and run_dir.parent.name.startswith("worker-"):
            return resolve_shared_root(str(run_dir.parents[1]))
        if run_dir.name.startswith("run-"):
            return resolve_shared_root(str(run_dir.parent))
        return resolve_shared_root(str(run_dir))
    return resolve_shared_root(str(Path.cwd()))


def _resolve_small_success_pool(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("pool_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return resolve_small_success_pool_dir(str(_derive_output_root_from_run_dir(step_input.get("output_dir"))))


def _resolve_small_success_claims(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("claims_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return resolve_small_success_claims_dir(str(_derive_output_root_from_run_dir(step_input.get("output_dir"))))


def _resolve_small_success_wait_pool(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("wait_pool_dir") or step_input.get("small_success_wait_pool_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    env_explicit = str(os.environ.get("REGISTER_SMALL_SUCCESS_WAIT_POOL_DIR") or "").strip()
    if env_explicit:
        return Path(env_explicit).resolve()
    return resolve_small_success_wait_pool_dir(str(_derive_output_root_from_run_dir(step_input.get("output_dir"))))


def _resolve_small_success_continue_pool(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("continue_pool_dir") or step_input.get("small_success_continue_pool_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    env_explicit = str(os.environ.get("REGISTER_SMALL_SUCCESS_CONTINUE_POOL_DIR") or "").strip()
    if env_explicit:
        return Path(env_explicit).resolve()
    return resolve_small_success_continue_pool_dir(str(_derive_output_root_from_run_dir(step_input.get("output_dir"))))


def _resolve_free_manual_oauth_pool(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("free_manual_oauth_pool_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    env_explicit = str(os.environ.get("REGISTER_FREE_MANUAL_OAUTH_POOL_DIR") or "").strip()
    if env_explicit:
        return Path(env_explicit).resolve()
    return resolve_free_manual_oauth_pool_dir(str(_derive_output_root_from_run_dir(step_input.get("output_dir"))))


def _validate_small_success_seed_payload(payload: dict[str, Any]) -> tuple[bool, str]:
    if not isinstance(payload, dict):
        return False, "payload_not_object"
    platform_org = payload.get("platformOrganization")
    if not isinstance(platform_org, dict):
        return False, "missing_platform_organization"
    if str(platform_org.get("status") or "").strip().lower() != "completed":
        return False, "platform_organization_not_completed"

    chatgpt_login = payload.get("chatgptLogin")
    if not isinstance(chatgpt_login, dict):
        return False, "missing_chatgpt_login"
    if str(chatgpt_login.get("status") or "").strip().lower() != "completed":
        return False, "chatgpt_login_not_completed"

    personal_workspace_id = str(
        chatgpt_login.get("personalWorkspaceId")
        or chatgpt_login.get("workspaceId")
        or ""
    ).strip()
    if not personal_workspace_id:
        return False, "missing_personal_workspace_id"

    login_details = payload.get("chatgptLoginDetails")
    if isinstance(login_details, dict):
        client_bootstrap = login_details.get("clientBootstrap")
        if isinstance(client_bootstrap, dict):
            auth_status = str(client_bootstrap.get("authStatus") or "").strip().lower()
            structure = str(client_bootstrap.get("structure") or "").strip().lower()
            if auth_status and auth_status != "logged_in":
                return False, "chatgpt_login_not_logged_in"
            if structure and structure != "personal":
                return False, "chatgpt_login_not_personal"

    mailbox_ref = str(payload.get("mailboxRef") or "").strip()
    if not mailbox_ref:
        return False, "missing_mailbox_ref"

    mailbox_session_id = str(payload.get("mailboxSessionId") or "").strip()
    if not mailbox_session_id:
        return False, "missing_mailbox_session_id"

    created_at_text = str(payload.get("createdAt") or "").strip()
    if not created_at_text:
        return False, "missing_created_at"
    try:
        parsed_created_at = datetime.fromisoformat(created_at_text.replace("Z", "+00:00"))
        if parsed_created_at.tzinfo is None:
            parsed_created_at = parsed_created_at.replace(tzinfo=timezone.utc)
        parsed_created_at = parsed_created_at.astimezone(timezone.utc)
    except Exception:
        return False, "invalid_created_at"

    max_age_raw = str(
        os.environ.get("REGISTER_SMALL_SUCCESS_SEED_MAX_AGE_SECONDS")
        or os.environ.get("REGISTER_TEAM_MEMBER_SEED_MAX_AGE_SECONDS")
        or "900"
    ).strip()
    try:
        max_age_seconds = max(0, int(float(max_age_raw)))
    except Exception:
        max_age_seconds = 900
    if max_age_seconds > 0:
        age_seconds = max(0.0, (datetime.now(timezone.utc) - parsed_created_at).total_seconds())
        if age_seconds > max_age_seconds:
            return False, f"small_success_seed_too_old:{int(age_seconds)}"

    return True, ""


def _load_small_success_seed_validation(path: Path) -> tuple[bool, str, dict[str, Any]]:
    try:
        payload = load_json_payload(path)
    except Exception as exc:
        return False, f"load_failed:{exc}", {}
    ok, reason = _validate_small_success_seed_payload(payload)
    return ok, reason, payload


def _free_manual_oauth_preserve_enabled(step_input: dict[str, Any]) -> bool:
    raw = str(
        step_input.get("free_manual_oauth_preserve_enabled")
        or os.environ.get("REGISTER_FREE_MANUAL_OAUTH_PRESERVE_ENABLED")
        or ""
    ).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _free_manual_oauth_preserve_codes(step_input: dict[str, Any]) -> set[str]:
    raw = str(
        step_input.get("free_manual_oauth_preserve_error_codes")
        or os.environ.get("REGISTER_FREE_MANUAL_OAUTH_PRESERVE_ERROR_CODES")
        or "free_personal_workspace_missing,obtain_codex_oauth_failed"
    ).strip()
    if not raw:
        return set()
    return {item.strip() for item in raw.split(",") if item.strip()}


def _resolve_team_pre_pool(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("team_pre_pool_dir") or step_input.get("pool_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return resolve_team_pre_pool_dir(str(_derive_output_root_from_run_dir(step_input.get("output_dir"))))


def _resolve_team_mother_pool(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("team_mother_pool_dir") or step_input.get("pool_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return resolve_team_mother_pool_dir(str(_derive_output_root_from_run_dir(step_input.get("output_dir"))))


def _resolve_team_mother_claims(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("team_mother_claims_dir") or step_input.get("claims_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return resolve_team_mother_claims_dir(str(_derive_output_root_from_run_dir(step_input.get("output_dir"))))


def _resolve_team_mother_cooldowns(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("team_mother_cooldowns_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return resolve_team_mother_cooldowns_dir(str(_derive_output_root_from_run_dir(step_input.get("output_dir"))))


def _resolve_team_member_claims(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("team_member_claims_dir") or step_input.get("claims_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return resolve_team_member_claims_dir(str(_derive_output_root_from_run_dir(step_input.get("output_dir"))))


def _resolve_team_post_pool(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("team_post_pool_dir") or step_input.get("pool_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return resolve_team_post_pool_dir(str(_derive_output_root_from_run_dir(step_input.get("output_dir"))))


def _resolve_team_pool(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("team_pool_dir") or step_input.get("pool_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return resolve_team_pool_dir(str(_derive_output_root_from_run_dir(step_input.get("output_dir"))))


def _restore_to_pool(*, claimed_path: Path, pool_dir: Path, preferred_name: str) -> str:
    destination = pool_dir / preferred_name
    if destination.exists():
        destination = pool_dir / f"{destination.stem}-{uuid.uuid4().hex[:6]}{destination.suffix}"
    claimed_path.replace(destination)
    return str(destination)


def _derive_original_name_from_claim(path: Path) -> str:
    name = path.name
    prefix, separator, remainder = name.partition("-")
    if separator and len(prefix) == 8:
        return remainder
    return name


def _recover_stale_team_claims(
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
        original_name = _derive_original_name_from_claim(claimed_path)
        try:
            restored_path = _restore_to_pool(
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


def _safe_count(value: Any, default: int) -> int:
    try:
        return max(0, int(value))
    except Exception:
        return default


def _team_stale_claim_seconds() -> int:
    return _safe_count(os.environ.get("REGISTER_TEAM_STALE_CLAIM_SECONDS") or 60, 60)


def _sort_paths_newest_first(paths: list[Path]) -> list[Path]:
    def _sort_key(path: Path) -> tuple[float, str]:
        try:
            modified_at = float(path.stat().st_mtime)
        except FileNotFoundError:
            modified_at = 0.0
        return (-modified_at, path.name.lower())

    return sorted(paths, key=_sort_key)


def _team_mother_cooldown_key(*, original_name: str, email: str, account_id: str) -> str:
    normalized_email = _sanitize_filename_component(str(email or "").strip().lower(), fallback="unknown-email")
    normalized_account = _sanitize_filename_component(_short_account_id_segment(account_id), fallback="unknown-account")
    if normalized_email != "unknown-email" or normalized_account != "unknown-account":
        return f"{normalized_account}-{normalized_email}"
    return _sanitize_filename_component(str(original_name or "").strip().lower(), fallback="unknown-mother")


def _team_mother_cooldown_path(
    *,
    cooldown_dir: Path,
    original_name: str,
    email: str,
    account_id: str,
) -> Path:
    return cooldown_dir / f"{_team_mother_cooldown_key(original_name=original_name, email=email, account_id=account_id)}.json"


def _team_mother_cooldown_state(
    *,
    cooldown_dir: Path,
    original_name: str,
    email: str,
    account_id: str,
) -> dict[str, Any]:
    state_path = _team_mother_cooldown_path(
        cooldown_dir=cooldown_dir,
        original_name=original_name,
        email=email,
        account_id=account_id,
    )
    if not state_path.exists():
        return {}
    try:
        state = load_json_payload(state_path)
    except Exception:
        return {}
    return state if isinstance(state, dict) else {}


def _team_mother_availability_state_prune(
    *,
    state_path: Path,
    state: dict[str, Any],
) -> dict[str, Any]:
    if not state:
        return {}
    normalized = dict(state)
    changed = False
    now_ts = time.time()

    def _drop_window(prefix: str) -> None:
        nonlocal changed
        try:
            until_ts = float(normalized.get(f"{prefix}_until_ts") or 0.0)
        except Exception:
            until_ts = 0.0
        if until_ts <= 0 or until_ts > now_ts:
            return
        for key in (
            "reason" if prefix == "cooldown" else None,
            "cooldown_seconds",
            "cooldown_started_at",
            "cooldown_until",
            "cooldown_until_ts",
            "blacklist_reason",
            "blacklist_seconds",
            "blacklist_started_at",
            "blacklist_until",
            "blacklist_until_ts",
        ):
            if key is None:
                continue
            if prefix == "cooldown" and key.startswith("blacklist_"):
                continue
            if prefix == "blacklist" and key.startswith("cooldown_"):
                continue
            if key in normalized:
                normalized.pop(key, None)
                changed = True

    _drop_window("cooldown")
    _drop_window("blacklist")

    seat_allocations = normalized.get("seat_allocations")
    if isinstance(seat_allocations, list):
        filtered_allocations: list[dict[str, Any]] = []
        for item in seat_allocations:
            if not isinstance(item, dict):
                changed = True
                continue
            status = str(item.get("status") or "").strip().lower()
            try:
                pending_until_ts = float(item.get("pending_until_ts") or 0.0)
            except Exception:
                pending_until_ts = 0.0
            if status == "pending" and pending_until_ts > 0.0 and pending_until_ts <= now_ts:
                changed = True
                continue
            filtered_allocations.append(item)
        if filtered_allocations != seat_allocations:
            normalized["seat_allocations"] = filtered_allocations
            changed = True
        if not filtered_allocations and "seat_allocations" in normalized:
            normalized.pop("seat_allocations", None)
            changed = True

    recent_invite_results = normalized.get("recent_invite_results")
    if not isinstance(recent_invite_results, list) and "recent_invite_results" in normalized:
        normalized.pop("recent_invite_results", None)
        changed = True
    recent_team_expand_results = normalized.get("recent_team_expand_results")
    if not isinstance(recent_team_expand_results, list) and "recent_team_expand_results" in normalized:
        normalized.pop("recent_team_expand_results", None)
        changed = True

    has_active_window = any(
        float(normalized.get(key) or 0.0) > now_ts
        for key in ("cooldown_until_ts", "blacklist_until_ts")
        if str(normalized.get(key) or "").strip()
    )
    has_recent_history = bool(normalized.get("recent_invite_results")) or bool(normalized.get("recent_team_expand_results"))
    has_seat_allocations = bool(normalized.get("seat_allocations"))
    if not has_active_window and not has_recent_history and not has_seat_allocations:
        state_path.unlink(missing_ok=True)
        return {}
    if changed:
        state_path.write_text(json.dumps(normalized, ensure_ascii=False, indent=2), encoding="utf-8")
    return normalized


def _team_mother_is_cooling(
    *,
    cooldown_dir: Path,
    candidate: Path,
    payload: dict[str, Any],
) -> tuple[bool, dict[str, Any]]:
    state_path = _team_mother_cooldown_path(
        cooldown_dir=cooldown_dir,
        original_name=candidate.name,
        email=str(payload.get("email") or "").strip(),
        account_id=str(
            payload.get("account_id")
            or ((payload.get("https://api.openai.com/auth") or {}).get("chatgpt_account_id"))
            or ""
        ).strip(),
    )
    state = _team_mother_cooldown_state(
        cooldown_dir=cooldown_dir,
        original_name=candidate.name,
        email=str(payload.get("email") or "").strip(),
        account_id=str(
            payload.get("account_id")
            or ((payload.get("https://api.openai.com/auth") or {}).get("chatgpt_account_id"))
            or ""
        ).strip(),
    )
    if not state:
        return False, {}
    state = _team_mother_availability_state_prune(state_path=state_path, state=state)
    if not state:
        return False, {}
    try:
        until_ts = float(state.get("cooldown_until_ts") or 0.0)
    except Exception:
        until_ts = 0.0
    now = time.time()
    if until_ts > now:
        return True, {
            "state_path": str(state_path),
            "cooldown_until": str(state.get("cooldown_until") or "").strip(),
            "cooldown_until_ts": until_ts,
            "remaining_seconds": round(max(0.0, until_ts - now), 3),
            "reason": str(state.get("reason") or "").strip(),
            "email": str(payload.get("email") or "").strip(),
            "account_id": str(
                payload.get("account_id")
                or ((payload.get("https://api.openai.com/auth") or {}).get("chatgpt_account_id"))
                or ""
            ).strip(),
            "original_name": candidate.name,
            "window": "cooldown",
        }
    try:
        blacklist_until_ts = float(state.get("blacklist_until_ts") or 0.0)
    except Exception:
        blacklist_until_ts = 0.0
    if blacklist_until_ts > now:
        return True, {
            "state_path": str(state_path),
            "blacklist_until": str(state.get("blacklist_until") or "").strip(),
            "blacklist_until_ts": blacklist_until_ts,
            "remaining_seconds": round(max(0.0, blacklist_until_ts - now), 3),
            "reason": str(state.get("blacklist_reason") or "").strip(),
            "email": str(payload.get("email") or "").strip(),
            "account_id": str(
                payload.get("account_id")
                or ((payload.get("https://api.openai.com/auth") or {}).get("chatgpt_account_id"))
                or ""
            ).strip(),
            "original_name": candidate.name,
            "window": "blacklist",
        }
    state_path.unlink(missing_ok=True)
    return False, {}


def _team_mother_has_inflight_primary_usage(
    *,
    cooldown_dir: Path,
    candidate: Path,
    payload: dict[str, Any],
) -> tuple[bool, dict[str, Any]]:
    state = _team_mother_cooldown_state(
        cooldown_dir=cooldown_dir,
        original_name=candidate.name,
        email=str(payload.get("email") or "").strip(),
        account_id=str(
            payload.get("account_id")
            or ((payload.get("https://api.openai.com/auth") or {}).get("chatgpt_account_id"))
            or ""
        ).strip(),
    )
    state_path = _team_mother_cooldown_path(
        cooldown_dir=cooldown_dir,
        original_name=candidate.name,
        email=str(payload.get("email") or "").strip(),
        account_id=str(
            payload.get("account_id")
            or ((payload.get("https://api.openai.com/auth") or {}).get("chatgpt_account_id"))
            or ""
        ).strip(),
    )
    if not state:
        return False, {}
    state = _team_mother_availability_state_prune(state_path=state_path, state=state)
    if not state:
        return False, {}
    inflight: list[dict[str, Any]] = []
    now_ts = time.time()
    for item in state.get("seat_allocations") or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("status") or "").strip().lower() != "pending":
            continue
        source_role = str(item.get("source_role") or "").strip().lower()
        if source_role not in {"main", "continue"}:
            continue
        try:
            pending_until_ts = float(item.get("pending_until_ts") or 0.0)
        except Exception:
            pending_until_ts = 0.0
        if pending_until_ts > 0.0 and pending_until_ts <= now_ts:
            continue
        inflight.append(
            {
                "source_role": source_role,
                "reservation_owner": str(item.get("reservation_owner") or "").strip(),
                "reservation_context": str(item.get("reservation_context") or "").strip(),
                "pending_until": str(item.get("pending_until") or "").strip(),
                "invite_email": str(item.get("invite_email") or "").strip(),
            }
        )
    if not inflight:
        return False, {}
    return True, {
        "state_path": str(state_path),
        "email": str(payload.get("email") or "").strip(),
        "account_id": str(
            payload.get("account_id")
            or ((payload.get("https://api.openai.com/auth") or {}).get("chatgpt_account_id"))
            or ""
        ).strip(),
        "original_name": candidate.name,
        "inflight": inflight,
        "pending_count": len(inflight),
    }


def _choose_random_files(*, directory: Path, pattern: str, limit: int) -> list[Path]:
    candidates = [path for path in directory.glob(pattern) if path.is_file()]
    if not candidates or limit <= 0:
        return []
    return _sort_paths_newest_first(candidates)[:limit]


def _claim_small_success_artifact(*, step_input: dict[str, Any]) -> dict[str, Any]:
    pool_dir = _resolve_small_success_pool(step_input)
    claims_dir = _resolve_small_success_claims(step_input)
    _ensure_directory(pool_dir)
    _ensure_directory(claims_dir)

    for candidate in _sort_paths_newest_first([path for path in pool_dir.glob("*.json") if path.is_file()]):
        claim_name = f"{uuid.uuid4().hex[:8]}-{candidate.name}"
        claimed_path = claims_dir / claim_name
        try:
            candidate.replace(claimed_path)
        except FileNotFoundError:
            continue
        valid, reason, payload = _load_small_success_seed_validation(claimed_path)
        if not valid:
            claimed_path.unlink(missing_ok=True)
            continue
        return {
            "ok": True,
            "source_path": str(claimed_path),
            "claimed_path": str(claimed_path),
            "pool_dir": str(pool_dir),
            "claims_dir": str(claims_dir),
            "original_name": candidate.name,
            "email": str(payload.get("email") or "").strip(),
        }

    raise RuntimeError("small_success_pool_empty")


def _finalize_small_success_artifact(*, step_input: dict[str, Any]) -> dict[str, Any]:
    artifact = step_input.get("artifact")
    if not isinstance(artifact, dict):
        return {
            "ok": True,
            "status": "skipped_missing_artifact",
        }

    claimed_path_text = str(artifact.get("claimed_path") or artifact.get("source_path") or "").strip()
    if not claimed_path_text:
        return {
            "ok": True,
            "status": "skipped_missing_artifact",
        }

    claimed_path = Path(claimed_path_text).resolve()
    if not claimed_path.exists():
        return {
            "ok": True,
            "status": "skipped_missing_artifact",
        }

    task_error_code = str(step_input.get("task_error_code") or "").strip()
    failure_mode = str(step_input.get("failure_mode") or "").strip().lower()
    if (
        task_error_code
        and _free_manual_oauth_preserve_enabled(step_input)
        and task_error_code in _free_manual_oauth_preserve_codes(step_input)
    ):
        pool_dir = _resolve_free_manual_oauth_pool(step_input)
        _ensure_directory(pool_dir)
        restored_path = _restore_to_pool(
            claimed_path=claimed_path,
            pool_dir=pool_dir,
            preferred_name=str(artifact.get("original_name") or claimed_path.name).strip() or claimed_path.name,
        )
        return {
            "ok": True,
            "status": "preserved_for_manual_oauth",
            "restored_path": restored_path,
            "claimed_path": str(claimed_path),
            "restore_pool_dir": str(pool_dir),
            "task_error_code": task_error_code,
            "email": str(artifact.get("email") or "").strip(),
        }
    if task_error_code == "free_personal_workspace_missing":
        pool_dir = _resolve_small_success_wait_pool(step_input)
    else:
        pool_dir = _resolve_small_success_pool(
            {
                "pool_dir": artifact.get("pool_dir"),
                "output_dir": step_input.get("output_dir"),
            }
        )
    _ensure_directory(pool_dir)

    if not task_error_code:
        claimed_path.unlink(missing_ok=True)
        return {
            "ok": True,
            "status": "deleted",
            "claimed_path": str(claimed_path),
        }

    if failure_mode == "delete":
        claimed_path.unlink(missing_ok=True)
        return {
            "ok": True,
            "status": "deleted_failed_artifact",
            "claimed_path": str(claimed_path),
            "task_error_code": task_error_code,
        }

    restored_path = _restore_to_pool(
        claimed_path=claimed_path,
        pool_dir=pool_dir,
        preferred_name=str(artifact.get("original_name") or claimed_path.name).strip() or claimed_path.name,
    )
    return {
        "ok": True,
        "status": "restored",
        "restored_path": restored_path,
        "claimed_path": str(claimed_path),
        "restore_pool_dir": str(pool_dir),
    }


def _extract_free_oauth_account_id(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    direct = str(
        payload.get("accountId")
        or payload.get("account_id")
        or payload.get("chatgpt_account_id")
        or ""
    ).strip()
    if direct:
        return direct
    auth_payload = payload.get("auth")
    if isinstance(auth_payload, dict):
        nested_direct = str(
            auth_payload.get("account_id")
            or auth_payload.get("chatgpt_account_id")
            or ((auth_payload.get("https://api.openai.com/auth") or {}).get("chatgpt_account_id"))
            or ""
        ).strip()
        if nested_direct:
            return nested_direct
    return ""


def _extract_free_oauth_auth_claims(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    auth_payload = payload.get("auth")
    if isinstance(auth_payload, dict):
        nested = auth_payload.get("https://api.openai.com/auth")
        if isinstance(nested, dict):
            return nested
    nested = payload.get("https://api.openai.com/auth")
    return nested if isinstance(nested, dict) else {}


def _extract_free_oauth_plan_type(payload: Any) -> str:
    auth_claims = _extract_free_oauth_auth_claims(payload)
    if auth_claims:
        plan_type = str(auth_claims.get("chatgpt_plan_type") or "").strip()
        if plan_type:
            return plan_type
    if isinstance(payload, dict):
        direct = str(payload.get("chatgpt_plan_type") or "").strip()
        if direct:
            return direct
        auth_payload = payload.get("auth")
        if isinstance(auth_payload, dict):
            nested_direct = str(auth_payload.get("chatgpt_plan_type") or "").strip()
            if nested_direct:
                return nested_direct
    return ""


def _extract_free_oauth_organizations(payload: Any) -> list[dict[str, Any]]:
    auth_claims = _extract_free_oauth_auth_claims(payload)
    organizations = auth_claims.get("organizations") if isinstance(auth_claims, dict) else None
    if isinstance(organizations, list):
        return [item for item in organizations if isinstance(item, dict)]
    if isinstance(payload, dict):
        auth_payload = payload.get("auth")
        if isinstance(auth_payload, dict):
            nested_orgs = auth_payload.get("organizations")
            if isinstance(nested_orgs, list):
                return [item for item in nested_orgs if isinstance(item, dict)]
        direct_orgs = payload.get("organizations")
        if isinstance(direct_orgs, list):
            return [item for item in direct_orgs if isinstance(item, dict)]
    return []


def _extract_credential_email(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    return str(payload.get("email") or "").strip()


def _extract_credential_org_id(payload: Any) -> str:
    account_id = _extract_free_oauth_account_id(payload)
    if account_id:
        return account_id
    organizations = _extract_free_oauth_organizations(payload)
    for organization in organizations:
        if not isinstance(organization, dict):
            continue
        org_id = str(organization.get("id") or "").strip()
        if org_id:
            return org_id
    return ""


def _sanitize_filename_component(value: str, *, fallback: str) -> str:
    text = str(value or "").strip()
    if not text:
        return fallback
    for bad in ('<', '>', ':', '"', '/', '\\', '|', '?', '*'):
        text = text.replace(bad, "_")
    text = text.strip().strip(".")
    return text or fallback


def _short_account_id_segment(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    for separator in ("-", "_"):
        if separator in text:
            head = text.split(separator, 1)[0].strip()
            if head:
                return head
    return text[:8].strip()


def _decode_jwt_payload(token: str) -> dict[str, Any]:
    raw = str(token or "").strip()
    if not raw:
        return {}
    parts = raw.split(".")
    if len(parts) < 2:
        return {}
    payload = parts[1].strip()
    if not payload:
        return {}
    padding = "=" * (-len(payload) % 4)
    try:
        decoded = base64.urlsafe_b64decode((payload + padding).encode("utf-8"))
        claims = json.loads(decoded.decode("utf-8"))
    except Exception:
        return {}
    return dict(claims) if isinstance(claims, dict) else {}


def _extract_credential_auth_claims(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    auth_sources: list[dict[str, Any]] = []
    auth_payload = payload.get("auth")
    if isinstance(auth_payload, dict):
        auth_sources.append(auth_payload)
    auth_sources.append(payload)
    for source in auth_sources:
        nested = source.get("https://api.openai.com/auth")
        if isinstance(nested, dict):
            return dict(nested)
    for source in auth_sources:
        for token_key in ("id_token", "access_token"):
            token = source.get(token_key)
            if not isinstance(token, str) or not token.strip():
                continue
            claims = _decode_jwt_payload(token)
            nested = claims.get("https://api.openai.com/auth")
            if isinstance(nested, dict):
                return dict(nested)
    return {}


def _extract_credential_profile_claims(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    auth_sources: list[dict[str, Any]] = []
    auth_payload = payload.get("auth")
    if isinstance(auth_payload, dict):
        auth_sources.append(auth_payload)
    auth_sources.append(payload)
    for source in auth_sources:
        nested = source.get("https://api.openai.com/profile")
        if isinstance(nested, dict):
            return dict(nested)
    for source in auth_sources:
        for token_key in ("id_token", "access_token"):
            token = source.get(token_key)
            if not isinstance(token, str) or not token.strip():
                continue
            claims = _decode_jwt_payload(token)
            nested = claims.get("https://api.openai.com/profile")
            if isinstance(nested, dict):
                return dict(nested)
    return {}


def _extract_credential_string_field(payload: dict[str, Any], *keys: str) -> str:
    sources: list[dict[str, Any]] = []
    auth_payload = payload.get("auth")
    if isinstance(auth_payload, dict):
        sources.append(auth_payload)
    sources.append(payload)
    for key in keys:
        for source in sources:
            value = source.get(key)
            if value is None:
                continue
            text = str(value).strip()
            if text:
                return text
    return ""


def _extract_credential_bool_field(payload: dict[str, Any], key: str, default: bool = False) -> bool:
    sources: list[dict[str, Any]] = []
    auth_payload = payload.get("auth")
    if isinstance(auth_payload, dict):
        sources.append(auth_payload)
    sources.append(payload)
    for source in sources:
        if key not in source:
            continue
        value = source.get(key)
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        text = str(value or "").strip().lower()
        if text in {"1", "true", "yes", "on"}:
            return True
        if text in {"0", "false", "no", "off", ""}:
            return False
    return default


def _standardize_export_credential_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    auth_claims = _extract_credential_auth_claims(payload)
    profile_claims = _extract_credential_profile_claims(payload)
    account_id = _extract_free_oauth_account_id(payload)
    email = _extract_credential_string_field(payload, "email")
    if not email and profile_claims:
        email = str(profile_claims.get("email") or "").strip()

    standardized: dict[str, Any] = {
        "type": _extract_credential_string_field(payload, "type") or "codex",
        "email": email,
        "account_id": account_id,
        "access_token": _extract_credential_string_field(payload, "access_token"),
        "refresh_token": _extract_credential_string_field(payload, "refresh_token"),
        "id_token": _extract_credential_string_field(payload, "id_token"),
        "expired": _extract_credential_string_field(payload, "expired"),
        "disabled": _extract_credential_bool_field(payload, "disabled", False),
        "last_refresh": _extract_credential_string_field(payload, "last_refresh"),
    }
    if auth_claims:
        standardized["https://api.openai.com/auth"] = auth_claims
    if profile_claims:
        standardized["https://api.openai.com/profile"] = profile_claims
    return standardized


def _has_free_personal_oauth_claims(payload: Any) -> bool:
    plan_type = _extract_free_oauth_plan_type(payload).strip().lower()
    if plan_type != "free":
        return False
    organizations = _extract_free_oauth_organizations(payload)
    for organization in organizations:
        title = str(organization.get("title") or "").strip().lower()
        role = str(organization.get("role") or "").strip().lower()
        is_default = organization.get("is_default")
        if title == "personal" and role == "owner":
            return True
        if title == "personal" and is_default is True:
            return True
    return False


def _validate_free_personal_oauth(*, step_input: dict[str, Any]) -> dict[str, Any]:
    oauth_result = step_input.get("oauth_result")
    invite_result = step_input.get("invite_result")

    oauth_account_id = _extract_free_oauth_account_id(oauth_result)
    team_account_id = str((invite_result or {}).get("team_account_id") or "").strip() if isinstance(invite_result, dict) else ""
    plan_type = _extract_free_oauth_plan_type(oauth_result)
    organizations = _extract_free_oauth_organizations(oauth_result)

    has_personal_claims = _has_free_personal_oauth_claims(oauth_result)
    if has_personal_claims:
        return {
            "ok": True,
            "status": "personal_oauth_confirmed",
            "oauth_account_id": oauth_account_id,
            "team_account_id": team_account_id,
            "validation_mode": "claim_based",
            "chatgpt_plan_type": plan_type,
            "organizations": organizations,
        }

    return {
        "ok": False,
        "status": "free_personal_workspace_missing",
        "code": "free_personal_workspace_missing",
        "detail": "free_personal_workspace_missing",
        "oauth_account_id": oauth_account_id,
        "team_account_id": team_account_id,
        "validation_mode": "missing_personal_claims",
        "chatgpt_plan_type": plan_type,
        "organizations": organizations,
    }


def _sleep_seconds(*, step_input: dict[str, Any]) -> dict[str, Any]:
    try:
        seconds = max(0.0, float(step_input.get("seconds") or 0.0))
    except Exception:
        seconds = 0.0
    reason = str(step_input.get("reason") or "").strip()
    if seconds > 0:
        time.sleep(seconds)
    return {
        "ok": True,
        "status": "slept" if seconds > 0 else "skipped_zero_seconds",
        "slept_seconds": seconds,
        "reason": reason,
    }


def _fill_team_pre_pool(*, step_input: dict[str, Any]) -> dict[str, Any]:
    source_pool_dir = _resolve_small_success_pool(step_input)
    team_pre_pool_dir = _resolve_team_pre_pool(step_input)
    _ensure_directory(source_pool_dir)
    _ensure_directory(team_pre_pool_dir)

    max_move_count = _safe_count(step_input.get("max_move_count") or 1, 1) or 1
    selected_paths = _choose_random_files(directory=source_pool_dir, pattern="*.json", limit=max_move_count)
    moved: list[dict[str, Any]] = []
    discarded: list[dict[str, Any]] = []
    for candidate in selected_paths:
        valid, reason, payload = _load_small_success_seed_validation(candidate)
        if not valid:
            candidate.unlink(missing_ok=True)
            discarded.append(
                {
                    "source_path": str(candidate),
                    "reason": reason,
                }
            )
            continue
        destination = team_pre_pool_dir / candidate.name
        if destination.exists():
            destination = team_pre_pool_dir / f"{candidate.stem}-{uuid.uuid4().hex[:6]}{candidate.suffix}"
        try:
            candidate.replace(destination)
        except FileNotFoundError:
            continue
        moved.append(
            {
                "source_path": str(candidate),
                "destination_path": str(destination),
                "email": str(payload.get("email") or "").strip(),
                "name": destination.name,
            }
        )

    return {
        "ok": True,
        "status": "moved" if moved else "idle",
        "moved_count": len(moved),
        "moved": moved,
        "discarded_count": len(discarded),
        "discarded": discarded,
        "source_pool_dir": str(source_pool_dir),
        "team_pre_pool_dir": str(team_pre_pool_dir),
    }


def _claim_team_mother_artifact(*, step_input: dict[str, Any]) -> dict[str, Any]:
    pool_dir = _resolve_team_mother_pool(step_input)
    claims_dir = _resolve_team_mother_claims(step_input)
    cooldown_dir = _resolve_team_mother_cooldowns(step_input)
    _ensure_directory(pool_dir)
    _ensure_directory(claims_dir)
    _ensure_directory(cooldown_dir)
    recovered = _recover_stale_team_claims(
        pool_dir=pool_dir,
        claims_dir=claims_dir,
        stale_after_seconds=_team_stale_claim_seconds(),
    )
    cooling: list[dict[str, Any]] = []
    busy: list[dict[str, Any]] = []

    target_count = _team_expand_target_count(step_input)
    in_progress_candidates: list[tuple[Path, dict[str, Any], dict[str, Any]]] = []
    other_candidates: list[tuple[Path, dict[str, Any], dict[str, Any]]] = []
    for candidate in _sort_paths_newest_first([path for path in pool_dir.glob("*.json") if path.is_file()]):
        payload = load_json_payload(candidate)
        progress = _team_expand_progress_from_payload(payload, fallback_target=target_count)
        if _team_expand_progress_is_in_progress(progress):
            in_progress_candidates.append((candidate, payload, progress))
        else:
            other_candidates.append((candidate, payload, progress))

    for candidate, payload, progress in [*in_progress_candidates, *other_candidates]:
        is_cooling, cooling_state = _team_mother_is_cooling(
            cooldown_dir=cooldown_dir,
            candidate=candidate,
            payload=payload,
        )
        if is_cooling:
            cooling.append(cooling_state)
            continue
        is_busy, busy_state = _team_mother_has_inflight_primary_usage(
            cooldown_dir=cooldown_dir,
            candidate=candidate,
            payload=payload,
        )
        if is_busy:
            busy.append(busy_state)
            continue
        claim_name = f"{uuid.uuid4().hex[:8]}-{candidate.name}"
        claimed_path = claims_dir / claim_name
        try:
            candidate.replace(claimed_path)
        except FileNotFoundError:
            continue
        claimed_payload = payload
        progress_reset = {}
        claimed_payload, progress_reset = _reset_claimed_team_expand_cycle_payload(
            payload,
            target_count=target_count,
            reason="claimed_from_team_mother_pool",
        )
        if progress_reset:
            claimed_path.write_text(json.dumps(claimed_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return {
            "ok": True,
            "status": "claimed",
            "source_path": str(claimed_path),
            "claimed_path": str(claimed_path),
            "pool_dir": str(pool_dir),
            "claims_dir": str(claims_dir),
            "cooldowns_dir": str(cooldown_dir),
            "original_name": candidate.name,
            "recovered_claims": recovered,
            "cooling": cooling,
            "busy": busy,
            "email": str(payload.get("email") or "").strip(),
            "password": str(payload.get("password") or "").strip(),
            "account_id": str(
                payload.get("account_id")
                or ((payload.get("https://api.openai.com/auth") or {}).get("chatgpt_account_id"))
                or ""
            ).strip(),
            "mother_progress": _team_expand_progress_from_payload(claimed_payload, fallback_target=target_count),
            "progress_reset": progress_reset,
        }

    return {
        "ok": True,
        "status": "idle",
        "source_path": "",
        "claimed_path": "",
        "pool_dir": str(pool_dir),
        "claims_dir": str(claims_dir),
        "cooldowns_dir": str(cooldown_dir),
        "recovered_claims": recovered,
        "cooling": cooling,
        "busy": busy,
        "original_name": "",
        "email": "",
        "password": "",
        "account_id": "",
    }


def _claim_team_member_candidates(*, step_input: dict[str, Any]) -> dict[str, Any]:
    team_pre_pool_dir = _resolve_team_pre_pool(step_input)
    claims_dir = _resolve_team_member_claims(step_input)
    _ensure_directory(team_pre_pool_dir)
    _ensure_directory(claims_dir)
    recovered = _recover_stale_team_claims(
        pool_dir=team_pre_pool_dir,
        claims_dir=claims_dir,
        stale_after_seconds=_team_stale_claim_seconds(),
    )

    requested_member_count = _safe_count(step_input.get("member_count") or 4, 4) or 4
    mother_progress = _load_team_expand_progress_from_artifact(
        step_input.get("mother_artifact"),
        fallback_target=requested_member_count,
    )
    remaining_member_count = max(0, int(mother_progress.get("remainingCount") or 0))
    member_count = min(requested_member_count, remaining_member_count) if remaining_member_count > 0 else 0
    if member_count <= 0:
        return {
            "ok": True,
            "status": "target_already_satisfied",
            "member_count": 0,
            "requested_member_count": requested_member_count,
            "remaining_member_count": remaining_member_count,
            "members": [],
            "team_pre_pool_dir": str(team_pre_pool_dir),
            "claims_dir": str(claims_dir),
            "recovered_claims": recovered,
            "member_emails": [],
            "mother_progress": mother_progress,
        }

    selected_paths = _choose_random_files(directory=team_pre_pool_dir, pattern="*.json", limit=member_count)
    if len(selected_paths) < member_count:
        raise RuntimeError("team_pre_pool_insufficient_members")

    claimed_members: list[dict[str, Any]] = []
    for candidate in selected_paths:
        claim_name = f"{uuid.uuid4().hex[:8]}-{candidate.name}"
        claimed_path = claims_dir / claim_name
        try:
            candidate.replace(claimed_path)
        except FileNotFoundError:
            continue
        payload = load_json_payload(claimed_path)
        claimed_members.append(
            {
                "source_path": str(claimed_path),
                "claimed_path": str(claimed_path),
                "pool_dir": str(team_pre_pool_dir),
                "claims_dir": str(claims_dir),
                "original_name": candidate.name,
                "email": str(payload.get("email") or "").strip(),
                "password": str(payload.get("password") or "").strip(),
            }
        )

    if len(claimed_members) < member_count:
        for artifact in claimed_members:
            claimed_path = Path(str(artifact.get("claimed_path") or "")).resolve()
            if claimed_path.exists():
                _restore_to_pool(
                    claimed_path=claimed_path,
                    pool_dir=team_pre_pool_dir,
                    preferred_name=str(artifact.get("original_name") or claimed_path.name).strip() or claimed_path.name,
                )
        raise RuntimeError("team_pre_pool_claim_race")

    return {
        "ok": True,
        "status": "claimed",
        "member_count": len(claimed_members),
        "requested_member_count": requested_member_count,
        "remaining_member_count": remaining_member_count,
        "members": claimed_members,
        "team_pre_pool_dir": str(team_pre_pool_dir),
        "claims_dir": str(claims_dir),
        "recovered_claims": recovered,
        "member_emails": [str(item.get("email") or "").strip() for item in claimed_members],
        "mother_progress": mother_progress,
    }


def _move_artifact_file(
    *,
    source_path: Path,
    destination_dir: Path,
    preferred_name: str | None = None,
    overwrite_existing: bool = False,
    payload_override: dict[str, Any] | None = None,
) -> str:
    _ensure_directory(destination_dir)
    destination = destination_dir / (str(preferred_name or "").strip() or source_path.name)
    if destination.exists() and not overwrite_existing:
        destination = destination_dir / f"{destination.stem}-{uuid.uuid4().hex[:6]}{destination.suffix}"
    elif destination.exists():
        destination.unlink(missing_ok=True)
    if payload_override is not None:
        destination.write_text(json.dumps(payload_override, ensure_ascii=False, indent=2), encoding="utf-8")
        source_path.unlink(missing_ok=True)
        return str(destination)
    shutil.copy2(source_path, destination)
    source_path.unlink(missing_ok=True)
    return str(destination)


def _mother_team_pool_name(source_path: Path, mother_artifact: dict[str, Any] | None = None) -> str:
    payload: dict[str, Any] = {}
    try:
        payload = load_json_payload(source_path)
    except Exception:
        payload = {}
    payload = _standardize_export_credential_payload(payload)

    email = str(
        ((mother_artifact or {}).get("email") if isinstance(mother_artifact, dict) else "")
        or payload.get("email")
        or ""
    ).strip()
    org_id = _extract_credential_org_id(payload)
    if email and org_id:
        normalized_email = _sanitize_filename_component(email, fallback="unknown-email")
        normalized_org_id = _sanitize_filename_component(_short_account_id_segment(org_id), fallback="unknown-org")
        return f"codex-team-mother-{normalized_org_id}-{normalized_email}.json"

    current_name = str(source_path.name or "").strip() or "mother-team.json"
    while True:
        parts = current_name.split("-", 2)
        if len(parts) >= 2 and len(parts[0]) == 8:
            try:
                int(parts[0], 16)
                current_name = parts[1] if len(parts) == 2 else f"{parts[1]}-{parts[2]}"
                continue
            except ValueError:
                pass
        break
    while current_name.lower().startswith("mother-"):
        current_name = current_name[7:]
    current_name = current_name.strip() or "team.json"
    if current_name.lower().endswith(".json"):
        return f"mother-{current_name}"
    return f"mother-{current_name}.json"


def _member_team_pool_name(source_path: Path, member_artifact: dict[str, Any] | None = None) -> str:
    payload: dict[str, Any] = {}
    try:
        payload = load_json_payload(source_path)
    except Exception:
        payload = {}
    payload = _standardize_export_credential_payload(payload)
    email = str(
        ((member_artifact or {}).get("email") if isinstance(member_artifact, dict) else "")
        or payload.get("email")
        or ""
    ).strip()
    org_id = _extract_credential_org_id(payload)
    if email and org_id:
        normalized_email = _sanitize_filename_component(email, fallback="unknown-email")
        normalized_org_id = _sanitize_filename_component(_short_account_id_segment(org_id), fallback="unknown-org")
        return f"codex-team-{normalized_org_id}-{normalized_email}.json"
    return str(source_path.name or "").strip() or "codex-team-unknown-org-unknown-email.json"


def _restore_team_members_on_success(invite_result: Any) -> bool:
    if not isinstance(invite_result, dict):
        return False
    if bool(invite_result.get("restoreMembersToTeamPrePool")):
        return True
    return str(invite_result.get("status") or "").strip().lower() == "mother_only_all_invites_failed"


def _preserve_mother_after_invite_result(invite_result: Any) -> bool:
    if not isinstance(invite_result, dict):
        return False
    if bool(invite_result.get("allInviteAttemptsFailed")):
        return True
    if not bool(invite_result.get("memberOauthRequired", True)):
        return True
    return str(invite_result.get("status") or "").strip().lower() == "mother_only_all_invites_failed"


def _team_member_success_emails(invite_result: Any, oauth_result: Any) -> set[str]:
    normalized: set[str] = set()
    if isinstance(oauth_result, dict):
        artifacts = oauth_result.get("artifacts")
        if isinstance(artifacts, list):
            for item in artifacts:
                if not isinstance(item, dict):
                    continue
                email = str(item.get("email") or "").strip().lower()
                if email:
                    normalized.add(email)
    if isinstance(invite_result, dict):
        oauth_artifacts = invite_result.get("oauthArtifacts")
        if isinstance(oauth_artifacts, list):
            for item in oauth_artifacts:
                if not isinstance(item, dict):
                    continue
                email = str(item.get("email") or "").strip().lower()
                if email:
                    normalized.add(email)
        successful_emails = invite_result.get("successfulMemberEmails")
        if isinstance(successful_emails, list):
            for item in successful_emails:
                email = str(item or "").strip().lower()
                if email:
                    normalized.add(email)
    return normalized


def _team_member_discard_emails(invite_result: Any) -> set[str]:
    discard: set[str] = set()
    if not isinstance(invite_result, dict):
        return discard
    results = invite_result.get("results")
    if not isinstance(results, list):
        return discard
    for item in results:
        if not isinstance(item, dict):
            continue
        email = str(item.get("email") or "").strip().lower()
        result_payload = item.get("result")
        if not email or not isinstance(result_payload, dict):
            continue
        if bool(result_payload.get("discardMemberArtifact")):
            discard.add(email)
            continue
        status_text = str(result_payload.get("status") or "").strip().lower()
        detail_text = " ".join(
            part
            for part in (
                str(result_payload.get("detail") or "").strip().lower(),
                str(result_payload.get("oauthError") or "").strip().lower(),
            )
            if part
        )
        if status_text == "member_oauth_failed_after_invite" and (
            "phone_wall" in detail_text or "page_type=add_phone" in detail_text or "add_phone" in detail_text
        ):
            discard.add(email)
    return discard


def _collect_result_has_mother(collect_result: Any) -> bool:
    if not isinstance(collect_result, dict):
        return False
    artifacts = collect_result.get("artifacts")
    if not isinstance(artifacts, list):
        return False
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            continue
        if str(artifact.get("kind") or "").strip().lower() == "mother":
            return True
    return False


def _team_expand_target_count(step_input: dict[str, Any] | None = None, default: int = 4) -> int:
    candidate = ""
    if isinstance(step_input, dict):
        candidate = str(step_input.get("member_count") or "").strip()
    if not candidate:
        candidate = str(os.environ.get("REGISTER_TEAM_MEMBER_COUNT") or default).strip()
    try:
        return max(1, int(candidate or default))
    except Exception:
        return max(1, int(default))


def _team_expand_progress_from_payload(payload: Any, *, fallback_target: int) -> dict[str, Any]:
    progress = {}
    if isinstance(payload, dict):
        team_flow = payload.get("teamFlow")
        if isinstance(team_flow, dict):
            raw_progress = team_flow.get("teamExpandProgress")
            if isinstance(raw_progress, dict):
                progress = dict(raw_progress)
    target_count = max(1, _safe_count(progress.get("targetCount") or fallback_target, fallback_target))
    emails: list[str] = []
    raw_emails = progress.get("successfulMemberEmails")
    if isinstance(raw_emails, list):
        for item in raw_emails:
            email = str(item or "").strip().lower()
            if email and email not in emails:
                emails.append(email)
    success_count = max(len(emails), _safe_count(progress.get("successCount") or len(emails), len(emails)))
    remaining_count = max(0, target_count - success_count)
    ready = bool(progress.get("readyForMotherCollection")) or success_count >= target_count
    return {
        "targetCount": target_count,
        "successfulMemberEmails": emails,
        "successCount": success_count,
        "remainingCount": remaining_count,
        "readyForMotherCollection": ready,
    }


def _team_expand_progress_is_in_progress(progress: Any) -> bool:
    if not isinstance(progress, dict):
        return False
    target_count = max(1, _safe_count(progress.get("targetCount") or 4, 4))
    success_count = max(0, _safe_count(progress.get("successCount") or 0, 0))
    ready = bool(progress.get("readyForMotherCollection")) or success_count >= target_count
    return success_count > 0 and not ready


def _team_expand_progress_is_completed(progress: Any) -> bool:
    if not isinstance(progress, dict):
        return False
    target_count = max(1, _safe_count(progress.get("targetCount") or 4, 4))
    success_count = max(0, _safe_count(progress.get("successCount") or 0, 0))
    return bool(progress.get("readyForMotherCollection")) or success_count >= target_count


def _reset_claimed_team_expand_cycle_payload(
    payload: dict[str, Any],
    *,
    target_count: int,
    reason: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    normalized_target_count = max(1, int(target_count or 4))
    team_flow = dict(payload.get("teamFlow") or {})
    previous_progress = _team_expand_progress_from_payload(payload, fallback_target=normalized_target_count)
    previous_raw_progress = team_flow.get("teamExpandProgress")
    previous_progress_payload = (
        dict(previous_raw_progress)
        if isinstance(previous_raw_progress, dict)
        else {
            **previous_progress,
            "successfulArtifacts": [],
        }
    )
    has_previous_cycle_state = (
        bool(previous_progress.get("successCount"))
        or bool(previous_progress.get("successfulMemberEmails"))
        or bool(team_flow.get("memberInviteBatch"))
        or bool(team_flow.get("teamSeatCleanup"))
        or bool(team_flow.get("collectedArtifacts"))
    )
    if has_previous_cycle_state:
        team_flow["previousClaimedTeamExpandProgress"] = previous_progress_payload
    team_flow["teamExpandProgress"] = {
        "targetCount": normalized_target_count,
        "successfulMemberEmails": [],
        "successfulArtifacts": [],
        "successCount": 0,
        "remainingCount": normalized_target_count,
        "readyForMotherCollection": False,
        "resetAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "resetReason": reason,
        "previousCycleSuccessCount": int(previous_progress.get("successCount") or 0),
        "previousCycleSuccessfulMemberEmails": list(previous_progress.get("successfulMemberEmails") or []),
    }
    for stale_key in (
        "teamSeatCleanup",
        "memberInviteBatch",
        "collectedArtifacts",
    ):
        team_flow.pop(stale_key, None)
    updated_payload = {
        **payload,
        "teamFlow": team_flow,
    }
    reset_summary = {
        "reset": has_previous_cycle_state,
        "reason": reason,
        "previous_success_count": int(previous_progress.get("successCount") or 0),
        "new_target_count": normalized_target_count,
    }
    return updated_payload, reset_summary


def _load_team_expand_progress_from_artifact(
    artifact: Any,
    *,
    fallback_target: int,
) -> dict[str, Any]:
    if not isinstance(artifact, dict):
        return _team_expand_progress_from_payload({}, fallback_target=fallback_target)
    source_path_text = str(artifact.get("source_path") or artifact.get("claimed_path") or "").strip()
    if not source_path_text:
        return _team_expand_progress_from_payload({}, fallback_target=fallback_target)
    source_path = Path(source_path_text).resolve()
    if not source_path.exists():
        return _team_expand_progress_from_payload({}, fallback_target=fallback_target)
    try:
        payload = load_json_payload(source_path)
    except Exception:
        payload = {}
    return _team_expand_progress_from_payload(payload, fallback_target=fallback_target)


def _path_is_inside_directory(*, path: Path, directory: Path) -> bool:
    try:
        resolved_path = path.resolve()
        resolved_dir = directory.resolve()
    except Exception:
        return False
    try:
        resolved_path.relative_to(resolved_dir)
        return True
    except Exception:
        return False


def _collect_team_pool_artifacts(*, step_input: dict[str, Any]) -> dict[str, Any]:
    team_pool_dir = _resolve_team_pool(step_input)
    _ensure_directory(team_pool_dir)

    collected: list[dict[str, Any]] = []
    target_count = _team_expand_target_count(step_input)
    mother_progress = _load_team_expand_progress_from_artifact(
        step_input.get("mother_claim_artifact") or step_input.get("mother_artifact"),
        fallback_target=target_count,
    )
    mother_ready_for_collection = bool(mother_progress.get("readyForMotherCollection"))

    mother_artifact = step_input.get("mother_artifact")
    if isinstance(mother_artifact, dict) and mother_ready_for_collection:
        mother_path_text = str(
            mother_artifact.get("successPath")
            or mother_artifact.get("source_path")
            or mother_artifact.get("claimed_path")
            or ""
        ).strip()
        if mother_path_text:
            mother_path = Path(mother_path_text).resolve()
            if mother_path.exists():
                standardized_payload = _standardize_export_credential_payload(load_json_payload(mother_path))
                team_pool_path = _move_artifact_file(
                    source_path=mother_path,
                    destination_dir=team_pool_dir,
                    preferred_name=_mother_team_pool_name(mother_path, mother_artifact),
                    overwrite_existing=True,
                    payload_override=standardized_payload,
                )
                collected.append(
                    {
                        "kind": "mother",
                        "email": str(mother_artifact.get("email") or "").strip(),
                        "preferred_name": Path(team_pool_path).name,
                        "team_pool_path": team_pool_path,
                    }
                )

    member_artifacts = step_input.get("member_artifacts")
    if isinstance(member_artifacts, list):
        for index, artifact in enumerate(member_artifacts, start=1):
            if not isinstance(artifact, dict):
                continue
            staged_path_text = str(
                artifact.get("teamPoolPath")
                or artifact.get("team_pool_path")
                or ""
            ).strip()
            team_pool_path = ""
            if staged_path_text:
                staged_path = Path(staged_path_text).resolve()
                if staged_path.exists() and _path_is_inside_directory(path=staged_path, directory=team_pool_dir):
                    team_pool_path = str(staged_path)
            if not team_pool_path:
                member_path_text = str(
                    artifact.get("successPath")
                    or artifact.get("source_path")
                    or artifact.get("claimed_path")
                    or ""
                ).strip()
                if not member_path_text:
                    continue
                member_path = Path(member_path_text).resolve()
                if not member_path.exists():
                    continue
                if _path_is_inside_directory(path=member_path, directory=team_pool_dir):
                    team_pool_path = str(member_path)
                else:
                    standardized_payload = _standardize_export_credential_payload(load_json_payload(member_path))
                    team_pool_path = _move_artifact_file(
                        source_path=member_path,
                        destination_dir=team_pool_dir,
                        preferred_name=_member_team_pool_name(member_path, artifact),
                        overwrite_existing=True,
                        payload_override=standardized_payload,
                    )
            collected.append(
                {
                    "kind": "member",
                    "index": index,
                    "email": str(artifact.get("email") or "").strip(),
                    "preferred_name": Path(team_pool_path).name,
                    "team_pool_path": team_pool_path,
                }
            )

    return {
        "ok": True,
        "status": "collected" if collected else "idle",
        "count": len(collected),
        "artifacts": collected,
        "team_pool_dir": str(team_pool_dir),
        "motherProgress": mother_progress,
        "motherReadyForCollection": mother_ready_for_collection,
    }


def _finalize_team_batch(*, step_input: dict[str, Any]) -> dict[str, Any]:
    task_error_code = str(step_input.get("task_error_code") or "").strip()
    invite_result = step_input.get("invite_result")
    oauth_result = step_input.get("oauth_result")
    restore_members_on_success = _restore_team_members_on_success(invite_result)
    target_count = _team_expand_target_count(step_input)
    mother_progress = _load_team_expand_progress_from_artifact(
        step_input.get("mother_artifact"),
        fallback_target=target_count,
    )
    mother_success_count = int(mother_progress.get("successCount") or 0)
    mother_ready_for_collection = bool(mother_progress.get("readyForMotherCollection"))
    preserve_mother_on_failure = _collect_result_has_mother(step_input.get("collect_result")) or _preserve_mother_after_invite_result(
        invite_result
    )
    restore_mother_for_iteration = (
        not bool(task_error_code)
        and not mother_ready_for_collection
        and mother_success_count > 0
    )
    successful_member_emails = _team_member_success_emails(invite_result, oauth_result)
    discarded_member_emails = _team_member_discard_emails(invite_result)
    restored: list[dict[str, Any]] = []
    deleted: list[str] = []

    def _finalize_one(
        *,
        artifact: dict[str, Any] | None,
        pool_dir: Path | None = None,
        restore_on_success: bool = False,
        preserve_on_failure: bool = False,
        force_delete: bool = False,
    ) -> None:
        nonlocal restored, deleted
        if not isinstance(artifact, dict):
            return
        claimed_path_text = str(artifact.get("claimed_path") or artifact.get("source_path") or "").strip()
        if not claimed_path_text:
            return
        claimed_path = Path(claimed_path_text).resolve()
        if not claimed_path.exists():
            return
        if force_delete:
            claimed_path.unlink(missing_ok=True)
            deleted.append(str(claimed_path))
            return
        should_restore = bool(task_error_code) or bool(restore_on_success)
        if bool(task_error_code) and bool(preserve_on_failure):
            should_restore = False
        if not should_restore:
            claimed_path.unlink(missing_ok=True)
            deleted.append(str(claimed_path))
            return
        target_pool_dir = pool_dir or Path(str(artifact.get("pool_dir") or "")).resolve()
        if not str(target_pool_dir or "").strip():
            return
        _ensure_directory(target_pool_dir)
        restored_path = _restore_to_pool(
            claimed_path=claimed_path,
            pool_dir=target_pool_dir,
            preferred_name=str(artifact.get("original_name") or claimed_path.name).strip() or claimed_path.name,
        )
        restored.append(
            {
                "claimed_path": str(claimed_path),
                "restored_path": restored_path,
            }
        )

    mother_artifact = step_input.get("mother_artifact")
    _finalize_one(
        artifact=mother_artifact,
        pool_dir=_resolve_team_mother_pool(
            {
                "team_mother_pool_dir": (mother_artifact or {}).get("pool_dir") if isinstance(mother_artifact, dict) else "",
                "output_dir": step_input.get("output_dir"),
            }
        ),
        restore_on_success=restore_mother_for_iteration,
        preserve_on_failure=preserve_mother_on_failure,
    )

    member_artifacts = step_input.get("member_artifacts")
    if isinstance(member_artifacts, list):
        team_pre_pool_dir = _resolve_team_pre_pool(
            {
                "team_pre_pool_dir": str(step_input.get("team_pre_pool_dir") or "").strip(),
                "output_dir": step_input.get("output_dir"),
            }
        )
        for artifact in member_artifacts:
            member_email = str((artifact or {}).get("email") or "").strip().lower() if isinstance(artifact, dict) else ""
            member_restore_on_success = restore_members_on_success
            member_force_delete = member_email in discarded_member_emails
            if not task_error_code and not restore_members_on_success:
                member_restore_on_success = member_email not in successful_member_emails
            _finalize_one(
                artifact=artifact,
                pool_dir=team_pre_pool_dir,
                restore_on_success=member_restore_on_success,
                force_delete=member_force_delete,
            )

    return {
        "ok": True,
        "status": (
            "mixed"
            if restored and deleted
            else "restored"
            if restored
            else "deleted"
            if deleted
            else "skipped_missing_artifact"
        ),
        "task_error_code": task_error_code,
        "restore_members_on_success": restore_members_on_success,
        "preserve_mother_on_failure": preserve_mother_on_failure,
        "restore_mother_for_iteration": restore_mother_for_iteration,
        "mother_progress": mother_progress,
        "restored": restored,
        "deleted": deleted,
    }


def dispatch_orchestration_step(*, step_type: str, step_input: dict[str, Any]) -> dict[str, Any]:
    normalized_step_type = str(step_type or "").strip()
    if normalized_step_type == "sleep_seconds":
        return _sleep_seconds(step_input=step_input)
    if normalized_step_type == "acquire_small_success_artifact":
        return _claim_small_success_artifact(step_input=step_input)
    if normalized_step_type == "finalize_small_success_artifact":
        return _finalize_small_success_artifact(step_input=step_input)
    if normalized_step_type == "validate_free_personal_oauth":
        return _validate_free_personal_oauth(step_input=step_input)
    if normalized_step_type == "fill_team_pre_pool":
        return _fill_team_pre_pool(step_input=step_input)
    if normalized_step_type == "acquire_team_mother_artifact":
        return _claim_team_mother_artifact(step_input=step_input)
    if normalized_step_type == "acquire_team_member_candidates":
        return _claim_team_member_candidates(step_input=step_input)
    if normalized_step_type == "collect_team_pool_artifacts":
        return _collect_team_pool_artifacts(step_input=step_input)
    if normalized_step_type == "finalize_team_batch":
        return _finalize_team_batch(step_input=step_input)
    raise RuntimeError(f"unsupported_orchestration_step:{normalized_step_type}")
