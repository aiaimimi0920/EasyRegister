from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from others.artifact_pool_claim_recovery import choose_random_files, sort_paths_newest_first
from others.common import extract_auth_claims, team_mother_cooldown_key
from others.storage import load_json_payload


def team_mother_cooldown_path(
    *,
    cooldown_dir: Path,
    original_name: str,
    email: str,
    account_id: str,
) -> Path:
    return cooldown_dir / f"{team_mother_cooldown_key(original_name=original_name, email=email, account_id=account_id)}.json"


def team_mother_cooldown_state(
    *,
    cooldown_dir: Path,
    original_name: str,
    email: str,
    account_id: str,
) -> dict[str, Any]:
    state_path = team_mother_cooldown_path(
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


def team_mother_availability_state_prune(
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


def _team_mother_identity_from_payload(candidate: Path, payload: dict[str, Any]) -> dict[str, str]:
    return {
        "email": str(payload.get("email") or "").strip(),
        "account_id": str(
            payload.get("account_id")
            or ((payload.get("https://api.openai.com/auth") or {}).get("chatgpt_account_id"))
            or ""
        ).strip(),
        "original_name": candidate.name,
    }


def team_mother_is_cooling(
    *,
    cooldown_dir: Path,
    candidate: Path,
    payload: dict[str, Any],
) -> tuple[bool, dict[str, Any]]:
    identity = _team_mother_identity_from_payload(candidate, payload)
    state_path = team_mother_cooldown_path(
        cooldown_dir=cooldown_dir,
        original_name=identity["original_name"],
        email=identity["email"],
        account_id=identity["account_id"],
    )
    state = team_mother_cooldown_state(
        cooldown_dir=cooldown_dir,
        original_name=identity["original_name"],
        email=identity["email"],
        account_id=identity["account_id"],
    )
    if not state:
        return False, {}
    state = team_mother_availability_state_prune(state_path=state_path, state=state)
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
            **identity,
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
            **identity,
            "window": "blacklist",
        }
    state_path.unlink(missing_ok=True)
    return False, {}


def team_mother_has_inflight_primary_usage(
    *,
    cooldown_dir: Path,
    candidate: Path,
    payload: dict[str, Any],
) -> tuple[bool, dict[str, Any]]:
    identity = _team_mother_identity_from_payload(candidate, payload)
    state = team_mother_cooldown_state(
        cooldown_dir=cooldown_dir,
        original_name=identity["original_name"],
        email=identity["email"],
        account_id=identity["account_id"],
    )
    state_path = team_mother_cooldown_path(
        cooldown_dir=cooldown_dir,
        original_name=identity["original_name"],
        email=identity["email"],
        account_id=identity["account_id"],
    )
    if not state:
        return False, {}
    state = team_mother_availability_state_prune(state_path=state_path, state=state)
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
        **identity,
        "inflight": inflight,
        "pending_count": len(inflight),
    }
