from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ in (None, "", "others"):
    import sys

    _CURRENT_DIR = Path(__file__).resolve().parent
    _PARENT_DIR = _CURRENT_DIR.parent
    for _candidate in (_CURRENT_DIR, _PARENT_DIR):
        candidate_text = str(_candidate)
        if candidate_text not in sys.path:
            sys.path.append(candidate_text)
    from others.common import (
        decode_jwt_payload,
        ensure_directory,
        extract_auth_claims,
        json_log,
        team_mother_cooldown_key,
        write_json_atomic,
    )
    from others.config import TeamAuthRuntimeConfig
    from others.paths import (
        resolve_team_mother_claims_dir,
        resolve_team_mother_cooldowns_dir,
        resolve_team_mother_pool_dir,
    )
    from others.result_artifacts import team_mother_identity as team_mother_identity_from_result_payload
    from others.runner_team_cleanup import team_auth_is_capacity_cooled
    from others.storage import load_json_payload
else:
    from .common import (
        decode_jwt_payload,
        ensure_directory,
        extract_auth_claims,
        json_log,
        team_mother_cooldown_key,
        write_json_atomic,
    )
    from .config import TeamAuthRuntimeConfig
    from .paths import (
        resolve_team_mother_claims_dir,
        resolve_team_mother_cooldowns_dir,
        resolve_team_mother_pool_dir,
    )
    from .result_artifacts import team_mother_identity as team_mother_identity_from_result_payload
    from .runner_team_cleanup import team_auth_is_capacity_cooled
    from .storage import load_json_payload


def team_auth_runtime_config(
    *,
    output_root: Path | None = None,
    shared_root: Path | None = None,
) -> TeamAuthRuntimeConfig:
    return TeamAuthRuntimeConfig.from_env(output_root=output_root, shared_root=shared_root)


def _split_path_list(raw: str) -> list[str]:
    normalized = str(raw or "").strip()
    if not normalized:
        return []
    return [item.strip() for item in normalized.split(os.pathsep) if str(item or "").strip()]


def team_auth_path_is_explicit_mother(path: Path) -> bool:
    return str(path.name or "").strip().lower().startswith("codex-team-mother-")


def team_auth_identity_claims(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    auth_sources: list[dict[str, Any]] = []
    auth_payload = payload.get("auth")
    if isinstance(auth_payload, dict):
        auth_sources.append(auth_payload)
    auth_sources.append(payload)
    for source in auth_sources:
        for token_key in ("id_token", "access_token"):
            token = source.get(token_key)
            if not isinstance(token, str) or not token.strip():
                continue
            claims = decode_jwt_payload(token)
            if claims:
                return claims
    return {}


def team_auth_payload_is_mother(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    auth_claims = extract_auth_claims(payload)
    plan_type = str(auth_claims.get("chatgpt_plan_type") or "").strip().lower()
    if plan_type and plan_type != "team":
        return False
    identity_claims = team_auth_identity_claims(payload)
    if not identity_claims:
        return False
    auth_provider = str(identity_claims.get("auth_provider") or "").strip().lower()
    if auth_provider != "passwordless":
        return False
    amr = identity_claims.get("amr")
    amr_values: list[str] = []
    if isinstance(amr, list):
        amr_values = [str(value).strip().lower() for value in amr if str(value or "").strip()]
    elif isinstance(amr, str):
        amr_values = [part.strip().lower() for part in amr.split(",") if part.strip()]
    if not amr_values:
        return False
    return any(value == "otp" or "otp_email" in value for value in amr_values)


def team_auth_pool_candidates(*, candidate_dirs: list[str]) -> list[str]:
    glob_pattern = team_auth_runtime_config().auth_glob or "*-team.json"
    explicit: list[str] = []
    inferred: list[str] = []
    seen: set[str] = set()
    for raw_dir in candidate_dirs:
        candidate = Path(raw_dir).expanduser()
        if candidate.is_file():
            directory_paths = [candidate]
        elif candidate.is_dir():
            directory_paths = []
            for pattern in ("*.json", glob_pattern):
                for path in candidate.glob(pattern):
                    if path.is_file():
                        directory_paths.append(path)
        else:
            continue
        for path in directory_paths:
            resolved = str(path.resolve())
            lowered = resolved.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            try:
                payload = load_json_payload(path)
            except Exception:
                continue
            if not team_auth_payload_is_mother(payload):
                continue
            if team_auth_path_is_explicit_mother(path):
                explicit.append(resolved)
            else:
                inferred.append(resolved)
    return explicit + inferred


def resolve_team_auth_pool(*, instance_role: str) -> list[str]:
    config = team_auth_runtime_config()
    normalized_role = str(instance_role or "").strip().lower()
    if normalized_role == "team":
        pool_dir = config.mother_pool_dir
        if not pool_dir.exists():
            return []
        return team_auth_pool_candidates(candidate_dirs=[str(pool_dir)])

    explicit_paths = list(config.auth_paths)
    if explicit_paths:
        return team_auth_pool_candidates(candidate_dirs=explicit_paths)

    explicit_path = config.auth_path
    if explicit_path:
        candidate = Path(explicit_path).expanduser()
        if candidate.exists():
            return team_auth_pool_candidates(candidate_dirs=[str(candidate.resolve())])
        return []

    candidate_dirs = list(config.auth_dirs)
    if not candidate_dirs:
        preferred_local_dir = config.auth_local_dir or config.local_dir
        fallback_default_dir = config.auth_default_dir
        deduped_dirs: list[str] = []
        seen_dirs: set[str] = set()
        for raw_dir in (preferred_local_dir, fallback_default_dir):
            normalized = str(raw_dir or "").strip()
            if not normalized:
                continue
            lowered = normalized.lower()
            if lowered in seen_dirs:
                continue
            seen_dirs.add(lowered)
            deduped_dirs.append(normalized)
        candidate_dirs = deduped_dirs
    return team_auth_pool_candidates(candidate_dirs=candidate_dirs)


def team_mother_identity_from_team_auth_path(team_auth_path: str) -> dict[str, str]:
    normalized_path = str(team_auth_path or "").strip()
    if not normalized_path:
        return {
            "original_name": "",
            "email": "",
            "account_id": "",
        }
    path = Path(normalized_path).resolve()
    try:
        payload = load_json_payload(path)
    except Exception:
        payload = {}
    auth_claims = extract_auth_claims(payload)
    return {
        "original_name": path.name,
        "email": str(payload.get("email") or "").strip(),
        "account_id": str(
            payload.get("account_id")
            or auth_claims.get("chatgpt_account_id")
            or ""
        ).strip(),
    }


def team_mother_identity_key(*, original_name: str, email: str, account_id: str) -> str:
    return team_mother_cooldown_key(
        original_name=str(original_name or "").strip(),
        email=str(email or "").strip(),
        account_id=str(account_id or "").strip(),
    )


def team_auth_identity_keys_from_paths(team_auth_paths: list[str]) -> set[str]:
    identity_keys: set[str] = set()
    for team_auth_path in team_auth_paths:
        normalized_path = str(team_auth_path or "").strip()
        if not normalized_path:
            continue
        identity = team_mother_identity_from_team_auth_path(normalized_path)
        identity_key = team_mother_identity_key(
            original_name=str(identity.get("original_name") or "").strip(),
            email=str(identity.get("email") or "").strip(),
            account_id=str(identity.get("account_id") or "").strip(),
        )
        if identity_key:
            identity_keys.add(identity_key)
    return identity_keys


def team_mother_cooldowns_dir_for_shared_root(*, shared_root: Path) -> Path:
    return Path(resolve_team_mother_cooldowns_dir(str(shared_root))).resolve()


def team_mother_reserved_identity_keys_for_shared_root(*, shared_root: Path) -> set[str]:
    reserved: set[str] = set()
    for directory in (
        Path(resolve_team_mother_pool_dir(str(shared_root))).resolve(),
        Path(resolve_team_mother_claims_dir(str(shared_root))).resolve(),
    ):
        if not directory.is_dir():
            continue
        for candidate in directory.glob("*.json"):
            if not candidate.is_file():
                continue
            try:
                payload = load_json_payload(candidate)
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            identity_key = team_mother_identity_key(
                original_name=candidate.name,
                email=str(payload.get("email") or "").strip(),
                account_id=str(
                    payload.get("account_id")
                    or ((payload.get("https://api.openai.com/auth") or {}).get("chatgpt_account_id"))
                    or ""
                ).strip(),
            )
            if identity_key:
                reserved.add(identity_key)
    return reserved


def team_auth_is_reserved_for_team_expand(
    *,
    shared_root: Path,
    team_auth_path: str,
    reserved_keys: set[str] | None = None,
) -> tuple[bool, dict[str, Any]]:
    normalized_path = str(team_auth_path or "").strip()
    if not normalized_path:
        return False, {}
    identity = team_mother_identity_from_team_auth_path(normalized_path)
    identity_key = team_mother_identity_key(
        original_name=str(identity.get("original_name") or "").strip(),
        email=str(identity.get("email") or "").strip(),
        account_id=str(identity.get("account_id") or "").strip(),
    )
    if not identity_key:
        return False, {}
    active_reserved_keys = reserved_keys if reserved_keys is not None else team_mother_reserved_identity_keys_for_shared_root(shared_root=shared_root)
    if identity_key not in active_reserved_keys:
        return False, {}
    return True, {
        "teamAuthPath": normalized_path,
        "identityKey": identity_key,
        "original_name": str(identity.get("original_name") or "").strip(),
        "email": str(identity.get("email") or "").strip(),
        "account_id": str(identity.get("account_id") or "").strip(),
    }


def _load_json_dict(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def team_mother_availability_state_path(
    *,
    shared_root: Path,
    original_name: str,
    email: str,
    account_id: str,
) -> Path:
    cooldown_dir = team_mother_cooldowns_dir_for_shared_root(shared_root=shared_root)
    ensure_directory(cooldown_dir)
    return cooldown_dir / f"{team_mother_cooldown_key(original_name=original_name, email=email, account_id=account_id)}.json"


def load_team_mother_availability_state(
    *,
    shared_root: Path,
    original_name: str,
    email: str,
    account_id: str,
) -> tuple[Path, dict[str, Any]]:
    state_path = team_mother_availability_state_path(
        shared_root=shared_root,
        original_name=original_name,
        email=email,
        account_id=account_id,
    )
    payload = _load_json_dict(state_path) if state_path.is_file() else {}
    return state_path, payload


def team_mother_availability_state_lock_path(*, state_path: Path) -> Path:
    return state_path.with_suffix(state_path.suffix + ".lock")


def _team_auth_state_lock_timeout_seconds() -> float:
    return team_auth_runtime_config().state_lock_timeout_seconds


def acquire_team_mother_availability_state_lock(*, state_path: Path) -> Path:
    lock_path = team_mother_availability_state_lock_path(state_path=state_path)
    ensure_directory(lock_path.parent)
    deadline = time.time() + _team_auth_state_lock_timeout_seconds()
    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(str(os.getpid()))
            return lock_path
        except FileExistsError:
            if time.time() >= deadline:
                raise RuntimeError(f"team_auth_state_lock_timeout:{state_path}")
            time.sleep(0.05)


def release_team_mother_availability_state_lock(*, lock_path: Path | None) -> None:
    if lock_path is None:
        return
    lock_path.unlink(missing_ok=True)


def _team_auth_total_seat_limit() -> int:
    return team_auth_runtime_config().total_seat_limit


def _team_auth_chatgpt_seat_limit() -> int:
    return team_auth_runtime_config().chatgpt_seat_limit


def _team_auth_codex_seat_limit() -> int:
    return team_auth_runtime_config().codex_seat_limit


def _team_auth_reservation_ttl_seconds() -> float:
    return team_auth_runtime_config().reservation_ttl_seconds


def _team_auth_team_member_chatgpt_seat_request() -> int:
    return team_auth_runtime_config().team_member_count


def _team_auth_codex_seat_types() -> set[str]:
    return set(team_auth_runtime_config().codex_seat_types)


def _team_auth_sall_cc_weight() -> float:
    return team_auth_runtime_config().sall_cc_weight


def _team_auth_zero_success_window_seconds() -> float:
    return team_auth_runtime_config().zero_success_window_seconds


def _team_auth_zero_success_min_attempts() -> int:
    return team_auth_runtime_config().zero_success_min_attempts


def _team_auth_team_expand_window_seconds() -> float:
    return team_auth_runtime_config().team_expand_window_seconds


def _team_auth_team_expand_failure_weight_step() -> float:
    return team_auth_runtime_config().team_expand_failure_weight_step


def _team_auth_team_expand_floor_weight() -> float:
    return team_auth_runtime_config().team_expand_floor_weight


def _team_auth_team_expand_success_credit() -> float:
    return team_auth_runtime_config().team_expand_success_credit


def normalize_team_auth_seat_type(value: Any) -> str:
    return str(value or "").strip().lower()


def team_auth_seat_category_for_type(seat_type: Any) -> str:
    normalized = normalize_team_auth_seat_type(seat_type)
    if normalized in _team_auth_codex_seat_types():
        return "codex"
    return "chatgpt"


def team_auth_seat_request_for_role(*, instance_role: str) -> dict[str, int]:
    normalized_role = str(instance_role or "").strip().lower()
    if normalized_role in {"main", "continue"}:
        return {"codex": 1, "chatgpt": 0}
    if normalized_role == "team":
        return {"codex": 0, "chatgpt": 0}
    return {"codex": 0, "chatgpt": 0}


def normalize_team_auth_seat_allocations(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    normalized_items: list[dict[str, Any]] = []
    dedupe_index: dict[str, int] = {}
    for item in raw:
        if not isinstance(item, dict):
            continue
        invite_email = str(
            item.get("invite_email")
            or item.get("email")
            or item.get("inviteEmail")
            or ""
        ).strip()
        invite_id = str(item.get("invite_id") or item.get("inviteId") or "").strip()
        member_user_id = str(item.get("member_user_id") or item.get("memberUserId") or item.get("user_id") or "").strip()
        seat_type = normalize_team_auth_seat_type(
            item.get("seat_type")
            or item.get("seatType")
            or item.get("effectiveSeatType")
            or item.get("requestedSeatType")
            or ""
        )
        seat_category = str(item.get("seat_category") or item.get("seatCategory") or "").strip().lower() or team_auth_seat_category_for_type(seat_type)
        reservation_id = str(item.get("reservation_id") or item.get("reservationId") or "").strip()
        reservation_owner = str(item.get("reservation_owner") or item.get("reservationOwner") or "").strip()
        reservation_context = str(item.get("reservation_context") or item.get("reservationContext") or "").strip()
        pending_until = str(item.get("pending_until") or item.get("pendingUntil") or "").strip()
        try:
            pending_until_ts = float(item.get("pending_until_ts") or item.get("pendingUntilTs") or 0.0)
        except Exception:
            pending_until_ts = 0.0
        normalized_item = {
            "seat_category": seat_category,
            "seat_type": seat_type,
            "invite_email": invite_email,
            "invite_id": invite_id,
            "member_user_id": member_user_id,
            "source_role": str(item.get("source_role") or item.get("sourceRole") or "").strip().lower(),
            "source_step": str(item.get("source_step") or item.get("sourceStep") or "").strip().lower(),
            "status": str(item.get("status") or "").strip().lower() or "active",
            "created_at": str(item.get("created_at") or item.get("createdAt") or "").strip(),
            "updated_at": str(item.get("updated_at") or item.get("updatedAt") or "").strip(),
            "reservation_id": reservation_id,
            "reservation_owner": reservation_owner,
            "reservation_context": reservation_context,
            "pending_until": pending_until,
            "pending_until_ts": pending_until_ts,
        }
        if reservation_id:
            dedupe_key = f"reservation:{reservation_id.lower()}"
        elif invite_id:
            dedupe_key = f"invite:{invite_id.lower()}"
        elif member_user_id:
            dedupe_key = f"user:{member_user_id.lower()}"
        elif invite_email:
            dedupe_key = f"email:{invite_email.lower()}"
        else:
            dedupe_key = f"anon:{len(normalized_items)}"
        existing_index = dedupe_index.get(dedupe_key)
        if existing_index is not None:
            existing = normalized_items[existing_index]
            created_at = str(existing.get("created_at") or "").strip() or normalized_item["created_at"]
            normalized_item["created_at"] = created_at
            normalized_items[existing_index] = normalized_item
            continue
        dedupe_index[dedupe_key] = len(normalized_items)
        normalized_items.append(normalized_item)
    return normalized_items


def team_auth_seat_summary_from_allocations(seat_allocations: list[dict[str, Any]]) -> dict[str, Any]:
    total_limit = _team_auth_total_seat_limit()
    chatgpt_limit = _team_auth_chatgpt_seat_limit()
    codex_limit = _team_auth_codex_seat_limit()
    used_chatgpt = sum(
        1
        for item in seat_allocations
        if isinstance(item, dict) and str(item.get("seat_category") or "").strip().lower() == "chatgpt"
    )
    used_codex = sum(
        1
        for item in seat_allocations
        if isinstance(item, dict) and str(item.get("seat_category") or "").strip().lower() == "codex"
    )
    used_total = max(0, used_chatgpt + used_codex)
    available_total = max(0, total_limit - used_total)
    available_chatgpt = max(0, min(chatgpt_limit - used_chatgpt, available_total))
    available_codex = max(0, min(codex_limit - used_codex, available_total))
    return {
        "total_limit": total_limit,
        "chatgpt_limit": chatgpt_limit,
        "codex_limit": codex_limit,
        "used_total": used_total,
        "used_chatgpt": used_chatgpt,
        "used_codex": used_codex,
        "available_total": available_total,
        "available_chatgpt": available_chatgpt,
        "available_codex": available_codex,
    }


def team_auth_seat_summary_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return team_auth_seat_summary_from_allocations(
        normalize_team_auth_seat_allocations(payload.get("seat_allocations"))
    )


def prune_expired_team_auth_seat_allocations(
    seat_allocations: list[dict[str, Any]],
    *,
    now_ts: float | None = None,
) -> tuple[list[dict[str, Any]], bool]:
    normalized_allocations = normalize_team_auth_seat_allocations(seat_allocations)
    current_ts = float(now_ts if now_ts is not None else time.time())
    filtered: list[dict[str, Any]] = []
    changed = False
    for allocation in normalized_allocations:
        status = str(allocation.get("status") or "").strip().lower()
        if status == "pending":
            try:
                pending_until_ts = float(allocation.get("pending_until_ts") or 0.0)
            except Exception:
                pending_until_ts = 0.0
            if pending_until_ts > 0.0 and pending_until_ts <= current_ts:
                changed = True
                continue
        filtered.append(allocation)
    return normalize_team_auth_seat_allocations(filtered), changed


def team_auth_allocation_matches(
    allocation: dict[str, Any],
    *,
    invite_email: str = "",
    invite_id: str = "",
    member_user_id: str = "",
    reservation_id: str = "",
) -> bool:
    normalized_email = str(invite_email or "").strip().lower()
    normalized_invite_id = str(invite_id or "").strip().lower()
    normalized_member_user_id = str(member_user_id or "").strip().lower()
    normalized_reservation_id = str(reservation_id or "").strip().lower()
    allocation_email = str(allocation.get("invite_email") or "").strip().lower()
    allocation_invite_id = str(allocation.get("invite_id") or "").strip().lower()
    allocation_member_user_id = str(allocation.get("member_user_id") or "").strip().lower()
    allocation_reservation_id = str(allocation.get("reservation_id") or "").strip().lower()
    if normalized_reservation_id and allocation_reservation_id and normalized_reservation_id == allocation_reservation_id:
        return True
    if normalized_invite_id and allocation_invite_id and normalized_invite_id == allocation_invite_id:
        return True
    if normalized_member_user_id and allocation_member_user_id and normalized_member_user_id == allocation_member_user_id:
        return True
    if normalized_email and allocation_email and normalized_email == allocation_email:
        return True
    return False


def upsert_team_auth_seat_allocations(
    *,
    seat_allocations: list[dict[str, Any]],
    additions: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], bool]:
    changed = False
    now_iso = datetime.now(timezone.utc).isoformat()
    normalized_allocations = normalize_team_auth_seat_allocations(seat_allocations)
    for item in additions:
        if not isinstance(item, dict):
            continue
        normalized_item = normalize_team_auth_seat_allocations([item])
        if not normalized_item:
            continue
        allocation = normalized_item[0]
        allocation["updated_at"] = now_iso
        if not str(allocation.get("created_at") or "").strip():
            allocation["created_at"] = now_iso
        matched_index = None
        for index, existing in enumerate(normalized_allocations):
            if team_auth_allocation_matches(
                existing,
                invite_email=str(allocation.get("invite_email") or "").strip(),
                invite_id=str(allocation.get("invite_id") or "").strip(),
                member_user_id=str(allocation.get("member_user_id") or "").strip(),
            ):
                matched_index = index
                break
        if matched_index is None:
            normalized_allocations.append(allocation)
            changed = True
            continue
        existing = dict(normalized_allocations[matched_index])
        allocation["created_at"] = str(existing.get("created_at") or "").strip() or allocation["created_at"]
        if existing != allocation:
            normalized_allocations[matched_index] = allocation
            changed = True
    return normalize_team_auth_seat_allocations(normalized_allocations), changed


def remove_team_auth_seat_allocations(
    *,
    seat_allocations: list[dict[str, Any]],
    invite_email: str = "",
    invite_id: str = "",
    member_user_id: str = "",
    reservation_id: str = "",
    seat_category: str = "",
    clear_all: bool = False,
) -> tuple[list[dict[str, Any]], bool]:
    normalized_allocations = normalize_team_auth_seat_allocations(seat_allocations)
    if clear_all:
        return [], bool(normalized_allocations)
    normalized_category = str(seat_category or "").strip().lower()
    filtered: list[dict[str, Any]] = []
    changed = False
    has_filters = any(str(value or "").strip() for value in (invite_email, invite_id, member_user_id, reservation_id))
    for allocation in normalized_allocations:
        matches_identity = team_auth_allocation_matches(
            allocation,
            invite_email=invite_email,
            invite_id=invite_id,
            member_user_id=member_user_id,
            reservation_id=reservation_id,
        )
        matches_category = not normalized_category or str(allocation.get("seat_category") or "").strip().lower() == normalized_category
        should_remove = False
        if has_filters:
            should_remove = matches_identity and matches_category
        elif normalized_category:
            should_remove = matches_category
        if should_remove:
            changed = True
            continue
        filtered.append(allocation)
    return normalize_team_auth_seat_allocations(filtered), changed


def _team_auth_identity_from_path_or_override(
    *,
    team_auth_path: str,
    identity: dict[str, str] | None,
) -> dict[str, str]:
    return {
        **team_mother_identity_from_team_auth_path(team_auth_path),
        **(identity or {}),
    }


def write_team_mother_availability_state(*, state_path: Path, payload: dict[str, Any]) -> None:
    identity_payload = {
        "original_name": str(payload.get("original_name") or "").strip(),
        "email": str(payload.get("email") or "").strip(),
        "account_id": str(payload.get("account_id") or "").strip(),
    }
    cooldown_keys = {
        "reason",
        "cooldown_seconds",
        "cooldown_started_at",
        "cooldown_until",
        "cooldown_until_ts",
    }
    blacklist_keys = {
        "blacklist_reason",
        "blacklist_seconds",
        "blacklist_started_at",
        "blacklist_until",
        "blacklist_until_ts",
    }
    stats_keys = {
        "recent_invite_results",
        "recent_team_expand_results",
        "seat_allocations",
        "seat_summary",
        "last_seat_sync_at",
    }
    seat_allocations = normalize_team_auth_seat_allocations(payload.get("seat_allocations"))
    seat_summary = team_auth_seat_summary_from_allocations(seat_allocations)
    normalized_payload = {
        **identity_payload,
        **{key: payload.get(key) for key in cooldown_keys if key in payload},
        **{key: payload.get(key) for key in blacklist_keys if key in payload},
        **{key: payload.get(key) for key in stats_keys if key in payload},
        "seat_summary": seat_summary,
    }
    if seat_allocations:
        normalized_payload["seat_allocations"] = seat_allocations
    else:
        normalized_payload.pop("seat_allocations", None)
    has_active_window = any(
        str(normalized_payload.get(key) or "").strip()
        for key in ("cooldown_until", "blacklist_until")
    )
    has_recent_history = bool(normalized_payload.get("recent_invite_results")) or bool(
        normalized_payload.get("recent_team_expand_results")
    )
    has_seat_allocations = bool(seat_allocations)
    if not has_active_window and not has_recent_history and not has_seat_allocations:
        state_path.unlink(missing_ok=True)
        return
    write_json_atomic(
        state_path,
        normalized_payload,
        include_pid=True,
        cleanup_temp=True,
    )


def prune_team_mother_availability_state(
    *,
    state_path: Path,
    payload: dict[str, Any],
) -> dict[str, Any]:
    if not payload:
        return {}
    now_ts = time.time()
    normalized = dict(payload)
    changed = False

    try:
        cooldown_until_ts = float(normalized.get("cooldown_until_ts") or 0.0)
    except Exception:
        cooldown_until_ts = 0.0
    if cooldown_until_ts > 0 and cooldown_until_ts <= now_ts:
        for key in (
            "reason",
            "cooldown_seconds",
            "cooldown_started_at",
            "cooldown_until",
            "cooldown_until_ts",
        ):
            if key in normalized:
                normalized.pop(key, None)
                changed = True

    try:
        blacklist_until_ts = float(normalized.get("blacklist_until_ts") or 0.0)
    except Exception:
        blacklist_until_ts = 0.0
    if blacklist_until_ts > 0 and blacklist_until_ts <= now_ts:
        for key in (
            "blacklist_reason",
            "blacklist_seconds",
            "blacklist_started_at",
            "blacklist_until",
            "blacklist_until_ts",
        ):
            if key in normalized:
                normalized.pop(key, None)
                changed = True

    recent_results = normalized.get("recent_invite_results")
    if isinstance(recent_results, list):
        cutoff_ts = now_ts - _team_auth_zero_success_window_seconds()
        filtered_results: list[dict[str, Any]] = []
        for item in recent_results:
            if not isinstance(item, dict):
                continue
            try:
                result_ts = float(item.get("ts") or 0.0)
            except Exception:
                result_ts = 0.0
            if result_ts <= 0.0 or result_ts < cutoff_ts:
                continue
            filtered_results.append(
                {
                    "ts": result_ts,
                    "at": str(item.get("at") or "").strip(),
                    "ok": bool(item.get("ok")),
                    "step": str(item.get("step") or "").strip(),
                }
            )
        filtered_results = filtered_results[-200:]
        if filtered_results != recent_results:
            changed = True
        if filtered_results:
            normalized["recent_invite_results"] = filtered_results
        elif "recent_invite_results" in normalized:
            normalized.pop("recent_invite_results", None)
            changed = True

    recent_team_expand_results = normalized.get("recent_team_expand_results")
    if isinstance(recent_team_expand_results, list):
        cutoff_ts = now_ts - _team_auth_team_expand_window_seconds()
        filtered_expand_results: list[dict[str, Any]] = []
        for item in recent_team_expand_results:
            if not isinstance(item, dict):
                continue
            try:
                result_ts = float(item.get("ts") or 0.0)
            except Exception:
                result_ts = 0.0
            if result_ts <= 0.0 or result_ts < cutoff_ts:
                continue
            filtered_expand_results.append(
                {
                    "ts": result_ts,
                    "at": str(item.get("at") or "").strip(),
                    "all_failed": bool(item.get("all_failed")),
                    "status": str(item.get("status") or "").strip(),
                }
            )
        filtered_expand_results = filtered_expand_results[-200:]
        if filtered_expand_results != recent_team_expand_results:
            changed = True
        if filtered_expand_results:
            normalized["recent_team_expand_results"] = filtered_expand_results
        elif "recent_team_expand_results" in normalized:
            normalized.pop("recent_team_expand_results", None)
            changed = True

    seat_allocations = normalize_team_auth_seat_allocations(normalized.get("seat_allocations"))
    seat_allocations, pruned_expired_reservations = prune_expired_team_auth_seat_allocations(
        seat_allocations,
        now_ts=now_ts,
    )
    if pruned_expired_reservations:
        changed = True
    existing_seat_allocations = normalized.get("seat_allocations")
    if seat_allocations != existing_seat_allocations:
        changed = True
    if seat_allocations:
        normalized["seat_allocations"] = seat_allocations
    elif "seat_allocations" in normalized:
        normalized.pop("seat_allocations", None)
        changed = True
    normalized["seat_summary"] = team_auth_seat_summary_from_allocations(seat_allocations)

    has_active_window = any(
        float(normalized.get(key) or 0.0) > now_ts
        for key in ("cooldown_until_ts", "blacklist_until_ts")
        if str(normalized.get(key) or "").strip()
    )
    has_recent_history = bool(normalized.get("recent_invite_results")) or bool(normalized.get("recent_team_expand_results"))
    has_seat_allocations = bool(seat_allocations)
    if not has_active_window and not has_recent_history and not has_seat_allocations:
        state_path.unlink(missing_ok=True)
        return {}
    if changed:
        write_team_mother_availability_state(state_path=state_path, payload=normalized)
    return normalized


def update_team_auth_seat_state(
    *,
    shared_root: Path,
    team_auth_path: str,
    identity: dict[str, str] | None,
    updater: Any,
) -> dict[str, Any] | None:
    resolved_identity = _team_auth_identity_from_path_or_override(
        team_auth_path=team_auth_path,
        identity=identity,
    )
    original_name = str(resolved_identity.get("original_name") or "").strip()
    email = str(resolved_identity.get("email") or "").strip()
    account_id = str(resolved_identity.get("account_id") or "").strip()
    if not any((original_name, email, account_id)):
        return None
    state_path, payload = load_team_mother_availability_state(
        shared_root=shared_root,
        original_name=original_name,
        email=email,
        account_id=account_id,
    )
    normalized = prune_team_mother_availability_state(state_path=state_path, payload=payload)
    current_allocations = normalize_team_auth_seat_allocations(normalized.get("seat_allocations"))
    updated_allocations, changed = updater(current_allocations)
    if not changed:
        return None
    normalized.update(
        {
            "original_name": original_name,
            "email": email,
            "account_id": account_id,
            "seat_allocations": updated_allocations,
            "last_seat_sync_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    if not updated_allocations:
        normalized.pop("seat_allocations", None)
    write_team_mother_availability_state(state_path=state_path, payload=normalized)
    return team_auth_seat_summary_from_allocations(updated_allocations)


def replace_team_auth_seat_allocations(
    *,
    shared_root: Path,
    team_auth_path: str,
    identity: dict[str, str] | None,
    allocations: list[dict[str, Any]],
) -> dict[str, Any] | None:
    normalized_target = normalize_team_auth_seat_allocations(allocations)
    return update_team_auth_seat_state(
        shared_root=shared_root,
        team_auth_path=team_auth_path,
        identity=identity,
        updater=lambda seat_allocations: (
            normalized_target,
            normalized_target != normalize_team_auth_seat_allocations(seat_allocations),
        ),
    )


def get_team_auth_seat_summary(
    *,
    shared_root: Path,
    team_auth_path: str,
) -> dict[str, Any]:
    identity = team_mother_identity_from_team_auth_path(team_auth_path)
    if not any(str(identity.get(key) or "").strip() for key in ("original_name", "email", "account_id")):
        return team_auth_seat_summary_from_allocations([])
    state_path, payload = load_team_mother_availability_state(
        shared_root=shared_root,
        original_name=str(identity.get("original_name") or "").strip(),
        email=str(identity.get("email") or "").strip(),
        account_id=str(identity.get("account_id") or "").strip(),
    )
    normalized = prune_team_mother_availability_state(state_path=state_path, payload=payload)
    return team_auth_seat_summary_from_payload(normalized)


def team_auth_has_required_seats(
    *,
    shared_root: Path,
    team_auth_path: str,
    required_codex_seats: int,
    required_chatgpt_seats: int,
) -> tuple[bool, dict[str, Any]]:
    summary = get_team_auth_seat_summary(
        shared_root=shared_root,
        team_auth_path=team_auth_path,
    )
    ok = (
        int(summary.get("available_codex") or 0) >= max(0, int(required_codex_seats or 0))
        and int(summary.get("available_chatgpt") or 0) >= max(0, int(required_chatgpt_seats or 0))
        and int(summary.get("available_total") or 0)
        >= max(0, int(required_codex_seats or 0)) + max(0, int(required_chatgpt_seats or 0))
    )
    return ok, summary


def _build_pending_reservation_allocations(
    *,
    required_codex_seats: int,
    required_chatgpt_seats: int,
    reservation_owner: str,
    reservation_context: str,
    source_role: str,
) -> list[dict[str, Any]]:
    additions: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    pending_until_ts = time.time() + _team_auth_reservation_ttl_seconds()
    pending_until_iso = datetime.fromtimestamp(pending_until_ts, timezone.utc).isoformat()

    def _build_item(*, seat_category: str, seat_type: str) -> dict[str, Any]:
        return {
            "seat_category": seat_category,
            "seat_type": seat_type,
            "invite_email": "",
            "invite_id": "",
            "member_user_id": "",
            "source_role": str(source_role or "").strip().lower(),
            "source_step": "seat-reservation",
            "status": "pending",
            "created_at": now_iso,
            "updated_at": now_iso,
            "reservation_id": uuid.uuid4().hex,
            "reservation_owner": str(reservation_owner or "").strip(),
            "reservation_context": str(reservation_context or "").strip(),
            "pending_until": pending_until_iso,
            "pending_until_ts": pending_until_ts,
        }

    for _ in range(max(0, int(required_codex_seats or 0))):
        additions.append(_build_item(seat_category="codex", seat_type="usage_based"))
    for _ in range(max(0, int(required_chatgpt_seats or 0))):
        additions.append(_build_item(seat_category="chatgpt", seat_type="default"))
    return additions


def try_reserve_required_team_auth_seats(
    *,
    shared_root: Path,
    team_auth_path: str,
    required_codex_seats: int,
    required_chatgpt_seats: int,
    reservation_owner: str,
    reservation_context: str,
    source_role: str,
) -> tuple[bool, dict[str, Any] | None, dict[str, Any]]:
    required_codex = max(0, int(required_codex_seats or 0))
    required_chatgpt = max(0, int(required_chatgpt_seats or 0))
    if required_codex <= 0 and required_chatgpt <= 0:
        return True, None, get_team_auth_seat_summary(shared_root=shared_root, team_auth_path=team_auth_path)

    identity = team_mother_identity_from_team_auth_path(team_auth_path)
    if not any(str(identity.get(key) or "").strip() for key in ("original_name", "email", "account_id")):
        return False, None, team_auth_seat_summary_from_allocations([])
    state_path = team_mother_availability_state_path(
        shared_root=shared_root,
        original_name=str(identity.get("original_name") or "").strip(),
        email=str(identity.get("email") or "").strip(),
        account_id=str(identity.get("account_id") or "").strip(),
    )
    lock_path: Path | None = None
    try:
        lock_path = acquire_team_mother_availability_state_lock(state_path=state_path)
        payload = _load_json_dict(state_path) if state_path.is_file() else {}
        normalized = prune_team_mother_availability_state(state_path=state_path, payload=payload)
        summary = team_auth_seat_summary_from_payload(normalized)
        has_capacity = (
            int(summary.get("available_codex") or 0) >= required_codex
            and int(summary.get("available_chatgpt") or 0) >= required_chatgpt
            and int(summary.get("available_total") or 0) >= (required_codex + required_chatgpt)
        )
        if not has_capacity:
            return False, None, summary
        current_allocations = normalize_team_auth_seat_allocations(normalized.get("seat_allocations"))
        additions = _build_pending_reservation_allocations(
            required_codex_seats=required_codex,
            required_chatgpt_seats=required_chatgpt,
            reservation_owner=reservation_owner,
            reservation_context=reservation_context,
            source_role=source_role,
        )
        updated_allocations, _ = upsert_team_auth_seat_allocations(
            seat_allocations=current_allocations,
            additions=additions,
        )
        normalized.update(
            {
                "original_name": str(identity.get("original_name") or "").strip(),
                "email": str(identity.get("email") or "").strip(),
                "account_id": str(identity.get("account_id") or "").strip(),
                "seat_allocations": updated_allocations,
                "last_seat_sync_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        write_team_mother_availability_state(state_path=state_path, payload=normalized)
        reservation = {
            "teamAuthPath": str(team_auth_path or "").strip(),
            "reservationIds": [
                str(item.get("reservation_id") or "").strip()
                for item in additions
                if str(item.get("reservation_id") or "").strip()
            ],
            "owner": str(reservation_owner or "").strip(),
            "context": str(reservation_context or "").strip(),
        }
        return True, reservation, team_auth_seat_summary_from_allocations(updated_allocations)
    finally:
        release_team_mother_availability_state_lock(lock_path=lock_path)


def release_team_auth_seat_reservations(
    *,
    shared_root: Path,
    reservation: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(reservation, dict):
        return None
    team_auth_path = str(reservation.get("teamAuthPath") or "").strip()
    reservation_ids = [
        str(item or "").strip()
        for item in (reservation.get("reservationIds") or [])
        if str(item or "").strip()
    ]
    if not team_auth_path or not reservation_ids:
        return None
    identity = team_mother_identity_from_team_auth_path(team_auth_path)
    if not any(str(identity.get(key) or "").strip() for key in ("original_name", "email", "account_id")):
        return None
    state_path = team_mother_availability_state_path(
        shared_root=shared_root,
        original_name=str(identity.get("original_name") or "").strip(),
        email=str(identity.get("email") or "").strip(),
        account_id=str(identity.get("account_id") or "").strip(),
    )
    lock_path: Path | None = None
    try:
        lock_path = acquire_team_mother_availability_state_lock(state_path=state_path)
        payload = _load_json_dict(state_path) if state_path.is_file() else {}
        normalized = prune_team_mother_availability_state(state_path=state_path, payload=payload)
        current_allocations = normalize_team_auth_seat_allocations(normalized.get("seat_allocations"))
        updated_allocations = current_allocations
        changed = False
        for reservation_id in reservation_ids:
            updated_allocations, removed = remove_team_auth_seat_allocations(
                seat_allocations=updated_allocations,
                reservation_id=reservation_id,
            )
            changed = changed or removed
        if not changed:
            return None
        normalized.update(
            {
                "original_name": str(identity.get("original_name") or "").strip(),
                "email": str(identity.get("email") or "").strip(),
                "account_id": str(identity.get("account_id") or "").strip(),
                "seat_allocations": updated_allocations,
                "last_seat_sync_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        write_team_mother_availability_state(state_path=state_path, payload=normalized)
        return team_auth_seat_summary_from_allocations(updated_allocations)
    finally:
        release_team_mother_availability_state_lock(lock_path=lock_path)


def team_auth_invite_payload_to_seat_allocation(
    *,
    invite_payload: dict[str, Any],
    source_role: str,
    source_step: str,
) -> dict[str, Any] | None:
    if not isinstance(invite_payload, dict) or not bool(invite_payload.get("ok")):
        return None
    response = invite_payload.get("response")
    response_invite = {}
    if isinstance(response, dict):
        account_invites = response.get("account_invites")
        if isinstance(account_invites, list) and account_invites:
            response_invite = account_invites[0] if isinstance(account_invites[0], dict) else {}
    seat_type = (
        str(invite_payload.get("effectiveSeatType") or "").strip()
        or str(invite_payload.get("requestedSeatType") or "").strip()
        or str((response_invite or {}).get("seat_type") or (response_invite or {}).get("seatType") or "").strip()
        or str(invite_payload.get("seat_type") or "").strip()
    )
    invite_email = (
        str(invite_payload.get("invite_email") or "").strip()
        or str((response_invite or {}).get("email_address") or (response_invite or {}).get("email") or "").strip()
    )
    invite_id = (
        str(invite_payload.get("invite_id") or "").strip()
        or str((response_invite or {}).get("id") or "").strip()
    )
    if not invite_email and not invite_id:
        return None
    return {
        "seat_category": team_auth_seat_category_for_type(seat_type),
        "seat_type": normalize_team_auth_seat_type(seat_type),
        "invite_email": invite_email,
        "invite_id": invite_id,
        "member_user_id": str(invite_payload.get("member_user_id") or invite_payload.get("user_id") or "").strip(),
        "source_role": str(source_role or "").strip().lower(),
        "source_step": str(source_step or "").strip().lower(),
        "status": "active",
    }


def extract_team_member_invite_allocations(invite_team_members_output: dict[str, Any]) -> list[dict[str, Any]]:
    results = invite_team_members_output.get("results")
    if not isinstance(results, list):
        return []
    allocations: list[dict[str, Any]] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        invite_payload = item.get("result")
        if not isinstance(invite_payload, dict):
            continue
        allocation = team_auth_invite_payload_to_seat_allocation(
            invite_payload=invite_payload,
            source_role="team",
            source_step="invite-team-members",
        )
        if allocation:
            allocations.append(allocation)
    return allocations


def reconcile_team_auth_seat_state_from_result(
    *,
    shared_root: Path,
    team_auth_path: str,
    result_payload_value: dict[str, Any],
    instance_role: str,
    worker_label: str,
    task_index: int,
) -> None:
    normalized_path = str(team_auth_path or "").strip()
    if not normalized_path or not isinstance(result_payload_value, dict):
        return
    steps = result_payload_value.get("steps")
    outputs = result_payload_value.get("outputs")
    if not isinstance(steps, dict) or not isinstance(outputs, dict):
        return
    identity = (
        team_mother_identity_from_result_payload(result_payload_value)
        if str(instance_role or "").strip().lower() == "team"
        else team_mother_identity_from_team_auth_path(normalized_path)
    )

    def _apply(mutator: Any) -> dict[str, Any] | None:
        return update_team_auth_seat_state(
            shared_root=shared_root,
            team_auth_path=normalized_path,
            identity=identity,
            updater=mutator,
        )

    seat_state_changed = False
    cleanup_output = outputs.get("cleanup-team-all-seats") or {}
    cleanup_response = cleanup_output.get("response") if isinstance(cleanup_output, dict) and isinstance(cleanup_output.get("response"), dict) else {}
    if str(steps.get("cleanup-team-all-seats") or "").strip().lower() == "ok":
        projected_snapshot = cleanup_response.get("seatSnapshotAfterProjected") if isinstance(cleanup_response, dict) else None
        if isinstance(projected_snapshot, dict) and isinstance(projected_snapshot.get("allocations"), list):
            summary = replace_team_auth_seat_allocations(
                shared_root=shared_root,
                team_auth_path=normalized_path,
                identity=identity,
                allocations=projected_snapshot.get("allocations") or [],
            )
        else:
            summary = _apply(
                lambda seat_allocations: remove_team_auth_seat_allocations(
                    seat_allocations=seat_allocations,
                    clear_all=True,
                )
            )
        if summary is not None:
            seat_state_changed = True

    if str(steps.get("invite-team-members") or "").strip().lower() == "ok":
        team_member_allocations = extract_team_member_invite_allocations(outputs.get("invite-team-members") or {})
        if team_member_allocations:
            summary = _apply(
                lambda seat_allocations: upsert_team_auth_seat_allocations(
                    seat_allocations=seat_allocations,
                    additions=team_member_allocations,
                )
            )
            if summary is not None:
                seat_state_changed = True

    if str(steps.get("invite-codex-member") or "").strip().lower() == "ok":
        codex_allocation = team_auth_invite_payload_to_seat_allocation(
            invite_payload=outputs.get("invite-codex-member") or {},
            source_role=str(instance_role or "").strip().lower(),
            source_step="invite-codex-member",
        )
        if codex_allocation:
            summary = _apply(
                lambda seat_allocations: upsert_team_auth_seat_allocations(
                    seat_allocations=seat_allocations,
                    additions=[codex_allocation],
                )
            )
            if summary is not None:
                seat_state_changed = True

    if str(steps.get("revoke-team-members") or "").strip().lower() == "ok":
        revoke_output = outputs.get("revoke-team-members") or {}
        if isinstance(revoke_output, dict):
            revoke_results = revoke_output.get("results")
            if isinstance(revoke_results, list) and revoke_results:
                for item in revoke_results:
                    if not isinstance(item, dict):
                        continue
                    summary = _apply(
                        lambda seat_allocations, item=item: remove_team_auth_seat_allocations(
                            seat_allocations=seat_allocations,
                            invite_email=str(item.get("email") or "").strip(),
                            member_user_id=str(item.get("userId") or "").strip(),
                        )
                    )
                    if summary is not None:
                        seat_state_changed = True

    if str(steps.get("revoke-codex-member") or "").strip().lower() == "ok":
        revoke_output = outputs.get("revoke-codex-member") or {}
        if isinstance(revoke_output, dict) and bool(revoke_output.get("ok")):
            invite_email = str(revoke_output.get("invite_email") or "").strip()
            invite_id = str(revoke_output.get("invite_id") or "").strip()
            member_user_id = str((revoke_output.get("response") or {}).get("user_id") or "").strip()
            if not any((invite_email, invite_id, member_user_id)):
                invite_output = outputs.get("invite-codex-member") or {}
                if isinstance(invite_output, dict):
                    invite_email = invite_email or str(invite_output.get("invite_email") or "").strip()
                    invite_id = invite_id or str(invite_output.get("invite_id") or "").strip()
            summary = _apply(
                lambda seat_allocations: remove_team_auth_seat_allocations(
                    seat_allocations=seat_allocations,
                    invite_email=invite_email,
                    invite_id=invite_id,
                    member_user_id=member_user_id,
                )
            )
            if summary is not None:
                seat_state_changed = True

    if seat_state_changed:
        summary = get_team_auth_seat_summary(
            shared_root=shared_root,
            team_auth_path=normalized_path,
        )
        json_log(
            {
                "event": "register_team_auth_seat_cache_synced",
                "workerId": worker_label,
                "taskIndex": task_index,
                "instanceRole": str(instance_role or "").strip().lower(),
                "teamAuthPath": normalized_path,
                "seatSummary": summary,
            }
        )


def sync_team_auth_codex_seats_from_cleanup_result(
    *,
    shared_root: Path,
    cleanup_result: dict[str, Any],
    worker_label: str,
    task_index: int,
) -> None:
    results = cleanup_result.get("results")
    if not isinstance(results, list) or not results:
        return
    changed_records: list[dict[str, Any]] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        team_auth_path = str(item.get("teamAuthPath") or "").strip()
        if not team_auth_path:
            continue
        response_payload = item.get("response") if isinstance(item.get("response"), dict) else {}
        projected_snapshot = response_payload.get("seatSnapshotAfterProjected") if isinstance(response_payload, dict) else None
        if isinstance(projected_snapshot, dict) and isinstance(projected_snapshot.get("allocations"), list):
            summary = replace_team_auth_seat_allocations(
                shared_root=shared_root,
                team_auth_path=team_auth_path,
                identity=None,
                allocations=projected_snapshot.get("allocations") or [],
            )
            if summary is not None:
                changed_records.append(
                    {
                        "teamAuthPath": team_auth_path,
                        "seatSummary": summary,
                        "mode": "projected_snapshot_replace",
                    }
                )
            continue
        operations = response_payload.get("operations") if isinstance(response_payload, dict) else None
        if bool(item.get("ok")):
            summary = update_team_auth_seat_state(
                shared_root=shared_root,
                team_auth_path=team_auth_path,
                identity=None,
                updater=lambda seat_allocations: remove_team_auth_seat_allocations(
                    seat_allocations=seat_allocations,
                    seat_category="codex",
                ),
            )
            if summary is not None:
                changed_records.append(
                    {
                        "teamAuthPath": team_auth_path,
                        "seatSummary": summary,
                        "mode": "clear_codex_category",
                    }
                )
            continue
        if not isinstance(operations, list):
            continue
        for operation in operations:
            if not isinstance(operation, dict) or not bool(operation.get("ok")):
                continue
            seat_category = team_auth_seat_category_for_type(operation.get("seat_type"))
            if seat_category != "codex":
                continue
            summary = update_team_auth_seat_state(
                shared_root=shared_root,
                team_auth_path=team_auth_path,
                identity=None,
                updater=lambda seat_allocations, operation=operation: remove_team_auth_seat_allocations(
                    seat_allocations=seat_allocations,
                    invite_email=str(operation.get("email") or "").strip(),
                    invite_id=str(operation.get("id") or "").strip() if str(operation.get("kind") or "").strip().lower() == "invite" else "",
                    member_user_id=str(operation.get("id") or "").strip() if str(operation.get("kind") or "").strip().lower() == "user" else "",
                    seat_category="codex",
                ),
            )
            if summary is not None:
                changed_records.append(
                    {
                        "teamAuthPath": team_auth_path,
                        "seatSummary": summary,
                        "mode": "operation_match",
                    }
                )
    if changed_records:
        json_log(
            {
                "event": "register_team_auth_cleanup_seat_cache_synced",
                "workerId": worker_label,
                "taskIndex": task_index,
                "results": changed_records,
            }
        )
