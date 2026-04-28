from __future__ import annotations

from pathlib import Path
from typing import Any

from others.common import json_log
from others.result_artifacts import team_mother_identity as team_mother_identity_from_result_payload
from others.runner_team_auth_pool import team_mother_identity_from_team_auth_path
from others.runner_team_auth_seat_model import (
    normalize_team_auth_seat_type,
    remove_team_auth_seat_allocations,
    team_auth_seat_category_for_type,
    upsert_team_auth_seat_allocations,
)
from others.runner_team_auth_seat_state import (
    get_team_auth_seat_summary,
    replace_team_auth_seat_allocations,
    update_team_auth_seat_state,
)


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
