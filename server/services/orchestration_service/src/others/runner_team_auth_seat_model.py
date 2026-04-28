from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from others.runner_team_auth_pool import (
    _team_auth_chatgpt_seat_limit,
    _team_auth_codex_seat_limit,
    _team_auth_codex_seat_types,
    _team_auth_total_seat_limit,
)


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
