from __future__ import annotations

from pathlib import Path

from others.common import json_log
from others.runner_team_auth_history import team_auth_is_temp_blacklisted
from others.runner_team_auth_metrics import (
    choose_weighted_team_auth_candidate,
    team_auth_is_recent_zero_success,
)
from others.runner_team_auth_state import (
    get_team_auth_seat_summary,
    team_auth_has_required_seats,
    team_auth_is_reserved_for_team_expand,
    team_auth_path_is_explicit_mother,
    team_auth_seat_request_for_role,
    team_mother_reserved_identity_keys_for_shared_root,
    try_reserve_required_team_auth_seats,
)
from others.runner_team_cleanup import team_auth_is_capacity_cooled


def select_team_auth_path(
    *,
    team_auth_pool: list[str],
    task_index: int,
    shared_root: Path,
    instance_role: str,
    worker_label: str = "",
) -> tuple[str, dict[str, object] | None]:
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
            break
    else:
        eligible = []
    if not eligible:
        return "", None

    seat_eligible: list[str] = []
    seat_shortages: list[dict[str, object]] = []
    zero_success_filtered: list[dict[str, object]] = []
    for candidate in eligible:
        seat_ok, seat_summary = team_auth_has_required_seats(
            shared_root=shared_root,
            team_auth_path=candidate,
            required_codex_seats=required_codex_seats,
            required_chatgpt_seats=required_chatgpt_seats,
        )
        if not seat_ok:
            seat_shortages.append(
                {
                    "teamAuthPath": candidate,
                    "seatSummary": seat_summary,
                }
            )
            continue
        if normalized_role in {"main", "continue"}:
            zero_success, zero_success_state = team_auth_is_recent_zero_success(
                shared_root=shared_root,
                team_auth_path=candidate,
            )
            if zero_success:
                zero_success_filtered.append(
                    {
                        "teamAuthPath": candidate,
                        "zeroSuccessState": zero_success_state,
                    }
                )
                continue
        seat_eligible.append(candidate)
    if not seat_eligible:
        json_log(
            {
                "event": "register_team_auth_filtered_exhausted",
                "workerId": worker_label,
                "taskIndex": task_index,
                "instanceRole": normalized_role,
                "requiredCodexSeats": required_codex_seats,
                "requiredChatgptSeats": required_chatgpt_seats,
                "seatShortages": seat_shortages,
                "zeroSuccessFiltered": zero_success_filtered,
            }
        )
        return "", None
    selected_candidate = choose_weighted_team_auth_candidate(
        candidates=seat_eligible,
        shared_root=shared_root,
        instance_role=normalized_role,
        required_codex_seats=required_codex_seats,
        required_chatgpt_seats=required_chatgpt_seats,
    )
    if not selected_candidate:
        return "", None
    reservation: dict[str, object] | None = None
    if required_codex_seats > 0 or required_chatgpt_seats > 0:
        reserved, reservation, summary = try_reserve_required_team_auth_seats(
            shared_root=shared_root,
            team_auth_path=selected_candidate,
            required_codex_seats=required_codex_seats,
            required_chatgpt_seats=required_chatgpt_seats,
            reservation_owner=worker_label or normalized_role or "worker",
            reservation_context=reservation_context,
            source_role=normalized_role,
        )
        if not reserved:
            json_log(
                {
                    "event": "register_team_auth_reservation_failed",
                    "workerId": worker_label,
                    "taskIndex": task_index,
                    "instanceRole": normalized_role,
                    "teamAuthPath": selected_candidate,
                    "seatSummary": summary,
                }
            )
            return "", None
        json_log(
            {
                "event": "register_team_auth_seat_reserved",
                "workerId": worker_label,
                "taskIndex": task_index,
                "instanceRole": normalized_role,
                "teamAuthPath": selected_candidate,
                "seatSummary": summary,
                "reservation": reservation,
            }
        )
    else:
        json_log(
            {
                "event": "register_team_auth_selected",
                "workerId": worker_label,
                "taskIndex": task_index,
                "instanceRole": normalized_role,
                "teamAuthPath": selected_candidate,
                "seatSummary": get_team_auth_seat_summary(
                    shared_root=shared_root,
                    team_auth_path=selected_candidate,
                ),
            }
        )
    return selected_candidate, reservation
