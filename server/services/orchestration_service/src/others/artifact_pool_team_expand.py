from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from others.artifact_pool_claim_recovery import safe_count
from others.artifact_pool_paths import team_auth_runtime_config_for_step_input
from others.common import extract_auth_claims
from others.storage import load_json_payload


def extract_free_oauth_plan_type(payload: Any) -> str:
    auth_claims = extract_auth_claims(payload)
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


def extract_free_oauth_organizations(payload: Any) -> list[dict[str, Any]]:
    auth_claims = extract_auth_claims(payload)
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


def has_free_personal_oauth_claims(payload: Any) -> bool:
    plan_type = extract_free_oauth_plan_type(payload).strip().lower()
    if plan_type != "free":
        return False
    organizations = extract_free_oauth_organizations(payload)
    for organization in organizations:
        title = str(organization.get("title") or "").strip().lower()
        role = str(organization.get("role") or "").strip().lower()
        is_default = organization.get("is_default")
        if title == "personal" and role == "owner":
            return True
        if title == "personal" and is_default is True:
            return True
    return False


def team_expand_target_count(step_input: dict[str, Any] | None = None, default: int = 4) -> int:
    candidate = ""
    if isinstance(step_input, dict):
        candidate = str(step_input.get("member_count") or "").strip()
    if not candidate:
        candidate = str(
            team_auth_runtime_config_for_step_input(step_input).team_member_count or default
        ).strip()
    try:
        return max(1, int(candidate or default))
    except Exception:
        return max(1, int(default))


def team_expand_progress_from_payload(payload: Any, *, fallback_target: int) -> dict[str, Any]:
    progress = {}
    if isinstance(payload, dict):
        team_flow = payload.get("teamFlow")
        if isinstance(team_flow, dict):
            raw_progress = team_flow.get("teamExpandProgress")
            if isinstance(raw_progress, dict):
                progress = dict(raw_progress)
    target_count = max(1, safe_count(progress.get("targetCount") or fallback_target, fallback_target))
    emails: list[str] = []
    raw_emails = progress.get("successfulMemberEmails")
    if isinstance(raw_emails, list):
        for item in raw_emails:
            email = str(item or "").strip().lower()
            if email and email not in emails:
                emails.append(email)
    success_count = max(len(emails), safe_count(progress.get("successCount") or len(emails), len(emails)))
    remaining_count = max(0, target_count - success_count)
    ready = bool(progress.get("readyForMotherCollection")) or success_count >= target_count
    return {
        "targetCount": target_count,
        "successfulMemberEmails": emails,
        "successCount": success_count,
        "remainingCount": remaining_count,
        "readyForMotherCollection": ready,
    }


def team_expand_progress_is_in_progress(progress: Any) -> bool:
    if not isinstance(progress, dict):
        return False
    target_count = max(1, safe_count(progress.get("targetCount") or 4, 4))
    success_count = max(0, safe_count(progress.get("successCount") or 0, 0))
    ready = bool(progress.get("readyForMotherCollection")) or success_count >= target_count
    return success_count > 0 and not ready


def team_expand_progress_is_completed(progress: Any) -> bool:
    if not isinstance(progress, dict):
        return False
    target_count = max(1, safe_count(progress.get("targetCount") or 4, 4))
    success_count = max(0, safe_count(progress.get("successCount") or 0, 0))
    return bool(progress.get("readyForMotherCollection")) or success_count >= target_count


def reset_claimed_team_expand_cycle_payload(
    payload: dict[str, Any],
    *,
    target_count: int,
    reason: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    normalized_target_count = max(1, int(target_count or 4))
    team_flow = dict(payload.get("teamFlow") or {})
    previous_progress = team_expand_progress_from_payload(payload, fallback_target=normalized_target_count)
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


def load_team_expand_progress_from_artifact(
    artifact: Any,
    *,
    fallback_target: int,
) -> dict[str, Any]:
    if not isinstance(artifact, dict):
        return team_expand_progress_from_payload({}, fallback_target=fallback_target)
    source_path_text = str(artifact.get("source_path") or artifact.get("claimed_path") or "").strip()
    if not source_path_text:
        return team_expand_progress_from_payload({}, fallback_target=fallback_target)
    source_path = Path(source_path_text).resolve()
    if not source_path.exists():
        return team_expand_progress_from_payload({}, fallback_target=fallback_target)
    try:
        payload = load_json_payload(source_path)
    except Exception:
        payload = {}
    return team_expand_progress_from_payload(payload, fallback_target=fallback_target)
