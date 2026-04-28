from __future__ import annotations

import random
from pathlib import Path

from others.runner_team_auth_state import (
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
    team_mother_identity_from_team_auth_path,
)


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
) -> tuple[bool, dict[str, object]]:
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
) -> dict[str, object]:
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
