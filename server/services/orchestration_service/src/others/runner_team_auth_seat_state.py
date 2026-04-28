from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from others.common import write_json_atomic
from others.runner_team_auth_pool import (
    _load_json_dict,
    _team_auth_reservation_ttl_seconds,
    acquire_team_mother_availability_state_lock,
    load_team_mother_availability_state,
    release_team_mother_availability_state_lock,
    team_mother_availability_state_path,
    team_mother_identity_from_team_auth_path,
)
from others.runner_team_auth_seat_model import (
    normalize_team_auth_seat_allocations,
    prune_expired_team_auth_seat_allocations,
    remove_team_auth_seat_allocations,
    team_auth_seat_summary_from_allocations,
    team_auth_seat_summary_from_payload,
    upsert_team_auth_seat_allocations,
)


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
        from others.runner_team_auth_pool import _team_auth_zero_success_window_seconds

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
        from others.runner_team_auth_pool import _team_auth_team_expand_window_seconds

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
