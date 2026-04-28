from __future__ import annotations

import random
import time
from datetime import datetime, timedelta, timezone
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
    from others.common import json_log
    from others.runner_team_auth_state import (
        _load_json_dict,
        _team_auth_sall_cc_weight,
        _team_auth_team_expand_failure_weight_step,
        _team_auth_team_expand_floor_weight,
        _team_auth_team_expand_success_credit,
        _team_auth_team_expand_window_seconds,
        _team_auth_zero_success_min_attempts,
        _team_auth_zero_success_window_seconds,
        get_team_auth_seat_summary,
        load_team_mother_availability_state,
        prune_team_mother_availability_state,
        team_auth_has_required_seats,
        team_auth_identity_keys_from_paths,
        team_auth_is_reserved_for_team_expand,
        team_auth_path_is_explicit_mother,
        team_auth_seat_request_for_role,
        team_mother_cooldowns_dir_for_shared_root,
        team_mother_identity_from_team_auth_path,
        team_mother_identity_key,
        team_mother_reserved_identity_keys_for_shared_root,
        try_reserve_required_team_auth_seats,
        write_team_mother_availability_state,
    )
    from others.runner_team_cleanup import team_auth_is_capacity_cooled, team_auth_state_dir, team_cleanup_state_path
else:
    from .common import json_log
    from .runner_team_auth_state import (
        _load_json_dict,
        _team_auth_sall_cc_weight,
        _team_auth_team_expand_failure_weight_step,
        _team_auth_team_expand_floor_weight,
        _team_auth_team_expand_success_credit,
        _team_auth_team_expand_window_seconds,
        _team_auth_zero_success_min_attempts,
        _team_auth_zero_success_window_seconds,
        get_team_auth_seat_summary,
        load_team_mother_availability_state,
        prune_team_mother_availability_state,
        team_auth_has_required_seats,
        team_auth_identity_keys_from_paths,
        team_auth_is_reserved_for_team_expand,
        team_auth_path_is_explicit_mother,
        team_auth_seat_request_for_role,
        team_mother_cooldowns_dir_for_shared_root,
        team_mother_identity_from_team_auth_path,
        team_mother_identity_key,
        team_mother_reserved_identity_keys_for_shared_root,
        try_reserve_required_team_auth_seats,
        write_team_mother_availability_state,
    )
    from .runner_team_cleanup import team_auth_is_capacity_cooled, team_auth_state_dir, team_cleanup_state_path


def team_auth_email_domain(*, team_auth_path: str) -> str:
    identity = team_mother_identity_from_team_auth_path(team_auth_path)
    email = str(identity.get("email") or "").strip().lower()
    if "@" not in email:
        return ""
    return email.rsplit("@", 1)[-1].strip().lower()


def team_auth_is_recent_zero_success(
    *,
    shared_root: Path,
    team_auth_path: str,
) -> tuple[bool, dict[str, Any]]:
    window_seconds = _team_auth_zero_success_window_seconds()
    min_attempts = _team_auth_zero_success_min_attempts()
    if window_seconds <= 0.0 or min_attempts <= 0:
        return False, {}
    identity = team_mother_identity_from_team_auth_path(team_auth_path)
    if not any(str(identity.get(key) or "").strip() for key in ("original_name", "email", "account_id")):
        return False, {}
    state_path, payload = load_team_mother_availability_state(
        shared_root=shared_root,
        original_name=str(identity.get("original_name") or "").strip(),
        email=str(identity.get("email") or "").strip(),
        account_id=str(identity.get("account_id") or "").strip(),
    )
    normalized = prune_team_mother_availability_state(state_path=state_path, payload=payload)
    recent_results = normalized.get("recent_invite_results")
    if not isinstance(recent_results, list) or not recent_results:
        return False, {}
    attempts = len(recent_results)
    success_count = sum(1 for item in recent_results if isinstance(item, dict) and bool(item.get("ok")))
    if attempts < min_attempts or success_count > 0:
        return False, {}
    return True, {
        "state_path": str(state_path),
        "original_name": str(identity.get("original_name") or "").strip(),
        "email": str(identity.get("email") or "").strip(),
        "account_id": str(identity.get("account_id") or "").strip(),
        "window_seconds": window_seconds,
        "attempts": attempts,
        "successes": success_count,
    }


def recent_team_auth_team_expand_weight_info(
    *,
    shared_root: Path,
    team_auth_path: str,
) -> dict[str, Any]:
    identity = team_mother_identity_from_team_auth_path(team_auth_path)
    if not any(str(identity.get(key) or "").strip() for key in ("original_name", "email", "account_id")):
        return {
            "weight": 1.0,
            "failures": 0,
            "successes": 0,
            "penaltyUnits": 0.0,
            "windowSeconds": _team_auth_team_expand_window_seconds(),
        }
    state_path, payload = load_team_mother_availability_state(
        shared_root=shared_root,
        original_name=str(identity.get("original_name") or "").strip(),
        email=str(identity.get("email") or "").strip(),
        account_id=str(identity.get("account_id") or "").strip(),
    )
    normalized = prune_team_mother_availability_state(state_path=state_path, payload=payload)
    recent_results = normalized.get("recent_team_expand_results")
    if not isinstance(recent_results, list) or not recent_results:
        return {
            "weight": 1.0,
            "failures": 0,
            "successes": 0,
            "penaltyUnits": 0.0,
            "windowSeconds": _team_auth_team_expand_window_seconds(),
        }
    failure_count = sum(
        1
        for item in recent_results
        if isinstance(item, dict) and bool(item.get("all_failed"))
    )
    success_count = sum(
        1
        for item in recent_results
        if isinstance(item, dict) and not bool(item.get("all_failed"))
    )
    penalty_units = max(
        0.0,
        float(failure_count) - (float(success_count) * _team_auth_team_expand_success_credit()),
    )
    if penalty_units <= 0.0:
        weight = 1.0
    else:
        weight = max(
            _team_auth_team_expand_floor_weight(),
            1.0 - (penalty_units * _team_auth_team_expand_failure_weight_step()),
        )
    return {
        "weight": float(weight),
        "failures": int(failure_count),
        "successes": int(success_count),
        "penaltyUnits": float(round(penalty_units, 6)),
        "windowSeconds": _team_auth_team_expand_window_seconds(),
        "statePath": str(state_path),
    }


def team_auth_selection_weight(
    *,
    team_auth_path: str,
    shared_root: Path,
    instance_role: str,
) -> float:
    weight = 1.0
    if team_auth_email_domain(team_auth_path=team_auth_path) == "sall.cc":
        weight *= _team_auth_sall_cc_weight()
    if str(instance_role or "").strip().lower() == "team":
        weight *= float(
            recent_team_auth_team_expand_weight_info(
                shared_root=shared_root,
                team_auth_path=team_auth_path,
            ).get("weight")
            or 1.0
        )
    return max(0.0, float(weight))


def choose_weighted_team_auth_candidate(
    *,
    candidates: list[str],
    shared_root: Path,
    instance_role: str,
    required_codex_seats: int = 0,
    required_chatgpt_seats: int = 0,
) -> str:
    if not candidates:
        return ""
    rng = random.SystemRandom()
    weighted_candidates: list[tuple[str, float]] = []
    total_weight = 0.0
    for candidate in candidates:
        base_weight = max(
            0.0,
            float(
                team_auth_selection_weight(
                    team_auth_path=candidate,
                    shared_root=shared_root,
                    instance_role=instance_role,
                )
            ),
        )
        seat_multiplier = 1.0
        if required_chatgpt_seats > 0 or required_codex_seats > 0:
            seat_summary = get_team_auth_seat_summary(
                shared_root=shared_root,
                team_auth_path=candidate,
            )
            if required_chatgpt_seats > 0:
                seat_focus = int(seat_summary.get("available_chatgpt") or 0)
            else:
                seat_focus = int(seat_summary.get("available_codex") or 0)
            seat_multiplier = max(1.0, min(5.0, float(max(0, seat_focus))))
        weight = max(0.0, float(base_weight * seat_multiplier))
        if weight <= 0.0:
            continue
        weighted_candidates.append((candidate, weight))
        total_weight += weight
    if total_weight <= 0.0 or not weighted_candidates:
        return rng.choice(candidates)
    threshold = rng.uniform(0.0, total_weight)
    running_weight = 0.0
    for candidate, weight in weighted_candidates:
        running_weight += weight
        if threshold <= running_weight:
            return candidate
    return weighted_candidates[-1][0]


def record_team_auth_recent_invite_result(
    *,
    shared_root: Path,
    team_auth_path: str,
    result_payload_value: dict[str, Any],
    identity: dict[str, str] | None = None,
) -> None:
    normalized_path = str(team_auth_path or "").strip()
    if not normalized_path:
        return
    steps = result_payload_value.get("steps") if isinstance(result_payload_value, dict) else {}
    if not isinstance(steps, dict):
        return
    step_name = ""
    invite_ok: bool | None = None
    for candidate in ("invite-codex-member", "invite-team-members"):
        status = str(steps.get(candidate) or "").strip().lower()
        if status == "ok":
            step_name = candidate
            invite_ok = True
            break
        if status == "failed":
            step_name = candidate
            invite_ok = False
            break
    if invite_ok is None:
        return
    resolved_identity = {
        **team_mother_identity_from_team_auth_path(normalized_path),
        **(identity or {}),
    }
    if not any(str(resolved_identity.get(key) or "").strip() for key in ("original_name", "email", "account_id")):
        return
    state_path, payload = load_team_mother_availability_state(
        shared_root=shared_root,
        original_name=str(resolved_identity.get("original_name") or "").strip(),
        email=str(resolved_identity.get("email") or "").strip(),
        account_id=str(resolved_identity.get("account_id") or "").strip(),
    )
    normalized = prune_team_mother_availability_state(state_path=state_path, payload=payload)
    recent_results = normalized.get("recent_invite_results")
    if not isinstance(recent_results, list):
        recent_results = []
    now = datetime.now(timezone.utc)
    recent_results.append(
        {
            "ts": now.timestamp(),
            "at": now.isoformat(),
            "ok": bool(invite_ok),
            "step": step_name,
        }
    )
    normalized.update(
        {
            "original_name": str(resolved_identity.get("original_name") or "").strip(),
            "email": str(resolved_identity.get("email") or "").strip(),
            "account_id": str(resolved_identity.get("account_id") or "").strip(),
            "recent_invite_results": recent_results[-200:],
        }
    )
    write_team_mother_availability_state(state_path=state_path, payload=normalized)


def record_team_auth_recent_team_expand_result(
    *,
    shared_root: Path,
    team_auth_path: str,
    result_payload_value: dict[str, Any],
    instance_role: str,
    identity: dict[str, str] | None = None,
) -> None:
    if str(instance_role or "").strip().lower() != "team":
        return
    normalized_path = str(team_auth_path or "").strip()
    if not normalized_path:
        return
    steps = result_payload_value.get("steps") if isinstance(result_payload_value, dict) else {}
    outputs = result_payload_value.get("outputs") if isinstance(result_payload_value, dict) else {}
    if not isinstance(steps, dict) or not isinstance(outputs, dict):
        return
    invite_status = str(steps.get("invite-team-members") or "").strip().lower()
    invite_output = outputs.get("invite-team-members")
    if invite_status != "ok" or not isinstance(invite_output, dict):
        return
    if "allInviteAttemptsFailed" not in invite_output:
        return
    resolved_identity = {
        **team_mother_identity_from_team_auth_path(normalized_path),
        **(identity or {}),
    }
    if not any(str(resolved_identity.get(key) or "").strip() for key in ("original_name", "email", "account_id")):
        return
    state_path, payload = load_team_mother_availability_state(
        shared_root=shared_root,
        original_name=str(resolved_identity.get("original_name") or "").strip(),
        email=str(resolved_identity.get("email") or "").strip(),
        account_id=str(resolved_identity.get("account_id") or "").strip(),
    )
    normalized = prune_team_mother_availability_state(state_path=state_path, payload=payload)
    recent_results = normalized.get("recent_team_expand_results")
    if not isinstance(recent_results, list):
        recent_results = []
    now = datetime.now(timezone.utc)
    recent_results.append(
        {
            "ts": now.timestamp(),
            "at": now.isoformat(),
            "all_failed": bool(invite_output.get("allInviteAttemptsFailed")),
            "status": str(invite_output.get("status") or "").strip(),
        }
    )
    normalized.update(
        {
            "original_name": str(resolved_identity.get("original_name") or "").strip(),
            "email": str(resolved_identity.get("email") or "").strip(),
            "account_id": str(resolved_identity.get("account_id") or "").strip(),
            "recent_team_expand_results": recent_results[-200:],
        }
    )
    write_team_mother_availability_state(state_path=state_path, payload=normalized)


def team_auth_is_temp_blacklisted(
    *,
    shared_root: Path,
    team_auth_path: str,
) -> tuple[bool, dict[str, Any]]:
    identity = team_mother_identity_from_team_auth_path(team_auth_path)
    if not any(str(identity.get(key) or "").strip() for key in ("original_name", "email", "account_id")):
        return False, {}
    state_path, payload = load_team_mother_availability_state(
        shared_root=shared_root,
        original_name=str(identity.get("original_name") or "").strip(),
        email=str(identity.get("email") or "").strip(),
        account_id=str(identity.get("account_id") or "").strip(),
    )
    normalized = prune_team_mother_availability_state(state_path=state_path, payload=payload)
    if not normalized:
        return False, {}
    try:
        until_ts = float(normalized.get("blacklist_until_ts") or 0.0)
    except Exception:
        until_ts = 0.0
    now_ts = time.time()
    if until_ts <= now_ts:
        return False, {}
    return True, {
        "state_path": str(state_path),
        "original_name": str(identity.get("original_name") or "").strip(),
        "email": str(identity.get("email") or "").strip(),
        "account_id": str(identity.get("account_id") or "").strip(),
        "blacklist_until": str(normalized.get("blacklist_until") or "").strip(),
        "blacklist_until_ts": until_ts,
        "remaining_seconds": round(max(0.0, until_ts - now_ts), 3),
        "reason": str(normalized.get("blacklist_reason") or "").strip(),
    }


def mark_team_auth_temporary_blacklist(
    *,
    shared_root: Path,
    team_auth_path: str,
    identity: dict[str, str] | None,
    reason: str,
    blacklist_seconds: float,
    worker_label: str,
    task_index: int,
) -> dict[str, Any] | None:
    resolved_identity = {
        **team_mother_identity_from_team_auth_path(team_auth_path),
        **(identity or {}),
    }
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
    now = datetime.now(timezone.utc)
    blacklist_seconds = max(0.0, float(blacklist_seconds or 0.0))
    blacklist_until = now + timedelta(seconds=blacklist_seconds)
    normalized.update(
        {
            "original_name": original_name,
            "email": email,
            "account_id": account_id,
            "blacklist_reason": str(reason or "").strip(),
            "blacklist_seconds": blacklist_seconds,
            "blacklist_started_at": now.isoformat(),
            "blacklist_until": blacklist_until.isoformat(),
            "blacklist_until_ts": blacklist_until.timestamp(),
        }
    )
    write_team_mother_availability_state(state_path=state_path, payload=normalized)
    result = {
        "state_path": str(state_path),
        "original_name": original_name,
        "email": email,
        "account_id": account_id,
        "blacklist_reason": str(reason or "").strip(),
        "blacklist_seconds": blacklist_seconds,
        "blacklist_started_at": now.isoformat(),
        "blacklist_until": blacklist_until.isoformat(),
        "blacklist_until_ts": blacklist_until.timestamp(),
        "team_auth_path": str(team_auth_path or "").strip(),
    }
    json_log(
        {
            "event": "register_team_auth_temp_blacklist_marked",
            "workerId": worker_label,
            "taskIndex": task_index,
            **result,
        }
    )
    return result


def clear_team_auth_temporary_blacklist(
    *,
    shared_root: Path,
    team_auth_path: str,
    identity: dict[str, str] | None,
    worker_label: str,
    task_index: int,
) -> bool:
    resolved_identity = {
        **team_mother_identity_from_team_auth_path(team_auth_path),
        **(identity or {}),
    }
    original_name = str(resolved_identity.get("original_name") or "").strip()
    email = str(resolved_identity.get("email") or "").strip()
    account_id = str(resolved_identity.get("account_id") or "").strip()
    if not any((original_name, email, account_id)):
        return False
    state_path, payload = load_team_mother_availability_state(
        shared_root=shared_root,
        original_name=original_name,
        email=email,
        account_id=account_id,
    )
    normalized = prune_team_mother_availability_state(state_path=state_path, payload=payload)
    if not normalized or not any(key in normalized for key in ("blacklist_until", "blacklist_until_ts")):
        return False
    for key in (
        "blacklist_reason",
        "blacklist_seconds",
        "blacklist_started_at",
        "blacklist_until",
        "blacklist_until_ts",
    ):
        normalized.pop(key, None)
    write_team_mother_availability_state(state_path=state_path, payload=normalized)
    json_log(
        {
            "event": "register_team_auth_temp_blacklist_cleared",
            "workerId": worker_label,
            "taskIndex": task_index,
            "statePath": str(state_path),
            "original_name": original_name,
            "email": email,
            "account_id": account_id,
            "team_auth_path": str(team_auth_path or "").strip(),
        }
    )
    return True


def prune_stale_team_auth_caches(
    *,
    shared_root: Path,
    active_team_auth_paths: list[str],
) -> dict[str, Any]:
    active_paths = {
        str(Path(candidate).resolve()).strip().lower()
        for candidate in active_team_auth_paths
        if str(candidate or "").strip()
    }
    active_identity_keys = team_auth_identity_keys_from_paths(active_team_auth_paths)
    reserved_identity_keys = team_mother_reserved_identity_keys_for_shared_root(shared_root=shared_root)
    allowed_identity_keys = active_identity_keys | reserved_identity_keys

    removed_state_paths: list[str] = []
    state_dir = team_auth_state_dir(shared_root=shared_root)
    if state_dir.is_dir():
        cleanup_state_name = team_cleanup_state_path(shared_root=shared_root).name.lower()
        for state_path in state_dir.glob("*.json"):
            if not state_path.is_file():
                continue
            if state_path.name.lower() == cleanup_state_name:
                continue
            payload = _load_json_dict(state_path)
            team_auth_path = str(payload.get("teamAuthPath") or "").strip()
            resolved_team_auth_path = str(Path(team_auth_path).resolve()).strip().lower() if team_auth_path else ""
            if not resolved_team_auth_path or resolved_team_auth_path not in active_paths or not Path(team_auth_path).is_file():
                state_path.unlink(missing_ok=True)
                removed_state_paths.append(str(state_path))

    removed_availability_paths: list[str] = []
    cooldown_dir = team_mother_cooldowns_dir_for_shared_root(shared_root=shared_root)
    if cooldown_dir.is_dir():
        for state_path in cooldown_dir.glob("*.json"):
            if not state_path.is_file():
                continue
            payload = _load_json_dict(state_path)
            if not payload:
                state_path.unlink(missing_ok=True)
                removed_availability_paths.append(str(state_path))
                continue
            normalized = prune_team_mother_availability_state(state_path=state_path, payload=payload)
            if not normalized:
                removed_availability_paths.append(str(state_path))
                continue
            identity_key = team_mother_identity_key(
                original_name=str(normalized.get("original_name") or "").strip(),
                email=str(normalized.get("email") or "").strip(),
                account_id=str(normalized.get("account_id") or "").strip(),
            )
            if identity_key and identity_key not in allowed_identity_keys:
                state_path.unlink(missing_ok=True)
                removed_availability_paths.append(str(state_path))

    return {
        "removedTeamAuthStatePaths": removed_state_paths,
        "removedAvailabilityStatePaths": removed_availability_paths,
    }


def select_team_auth_path(
    *,
    team_auth_pool: list[str],
    task_index: int,
    shared_root: Path,
    instance_role: str,
    worker_label: str = "",
) -> tuple[str, dict[str, Any] | None]:
    if not team_auth_pool:
        return "", None
    normalized_role = str(instance_role or "").strip().lower()
    seat_request = team_auth_seat_request_for_role(instance_role=instance_role)
    required_codex_seats = int(seat_request.get("codex") or 0)
    required_chatgpt_seats = int(seat_request.get("chatgpt") or 0)
    reservation_context = f"{str(instance_role or '').strip().lower()}:{task_index}"
    reserved_for_team_expand = (
        team_mother_reserved_identity_keys_for_shared_root(shared_root=shared_root)
        if normalized_role in {"main", "continue"}
        else set()
    )
    explicit_candidates: list[str] = []
    inferred_candidates: list[str] = []
    seen: set[str] = set()
    for candidate in team_auth_pool:
        normalized = str(candidate or "").strip()
        if not normalized:
            continue
        lowered = normalized.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        if team_auth_path_is_explicit_mother(Path(normalized)):
            explicit_candidates.append(normalized)
        else:
            inferred_candidates.append(normalized)
    for candidate_pool in (explicit_candidates, inferred_candidates):
        eligible = [
            candidate
            for candidate in candidate_pool
            if not team_auth_is_capacity_cooled(shared_root=shared_root, team_auth_path=candidate)
            and not team_auth_is_temp_blacklisted(shared_root=shared_root, team_auth_path=candidate)[0]
            and not team_auth_is_reserved_for_team_expand(
                shared_root=shared_root,
                team_auth_path=candidate,
                reserved_keys=reserved_for_team_expand,
            )[0]
        ]
        if eligible:
            seat_eligible = [
                candidate
                for candidate in eligible
                if team_auth_has_required_seats(
                    shared_root=shared_root,
                    team_auth_path=candidate,
                    required_codex_seats=required_codex_seats,
                    required_chatgpt_seats=required_chatgpt_seats,
                )[0]
            ]
            if not seat_eligible:
                continue
            recent_success_filtered = [
                candidate
                for candidate in seat_eligible
                if not team_auth_is_recent_zero_success(shared_root=shared_root, team_auth_path=candidate)[0]
            ]
            selectable_candidates = list(recent_success_filtered or seat_eligible)
            if required_codex_seats <= 0 and required_chatgpt_seats <= 0:
                return (
                    choose_weighted_team_auth_candidate(
                        candidates=selectable_candidates,
                        shared_root=shared_root,
                        instance_role=normalized_role,
                        required_codex_seats=required_codex_seats,
                        required_chatgpt_seats=required_chatgpt_seats,
                    ),
                    None,
                )
            remaining_candidates = list(selectable_candidates)
            while remaining_candidates:
                selected_candidate = choose_weighted_team_auth_candidate(
                    candidates=remaining_candidates,
                    shared_root=shared_root,
                    instance_role=normalized_role,
                    required_codex_seats=required_codex_seats,
                    required_chatgpt_seats=required_chatgpt_seats,
                )
                if not selected_candidate:
                    break
                reserved, reservation, _ = try_reserve_required_team_auth_seats(
                    shared_root=shared_root,
                    team_auth_path=selected_candidate,
                    required_codex_seats=required_codex_seats,
                    required_chatgpt_seats=required_chatgpt_seats,
                    reservation_owner=worker_label,
                    reservation_context=reservation_context,
                    source_role=str(instance_role or "").strip().lower(),
                )
                if reserved:
                    return selected_candidate, reservation
                remaining_candidates = [
                    candidate
                    for candidate in remaining_candidates
                    if str(candidate or "").strip().lower() != str(selected_candidate or "").strip().lower()
                ]
    return "", None
