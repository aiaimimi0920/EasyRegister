from __future__ import annotations

from pathlib import Path
from typing import Any

from others.artifact_pool_common import (
    load_team_expand_progress_from_artifact,
    path_is_inside_directory,
    resolve_team_mother_pool,
    resolve_team_pool,
    resolve_team_pre_pool,
    restore_to_pool,
    team_expand_target_count,
)
from others.common import (
    canonical_team_artifact_name,
    ensure_directory,
    extract_org_id,
    standardize_export_credential_payload,
)
from others.prepared_artifacts import (
    copy_delete_prepared_artifact_to_dir,
    prepare_named_artifact,
)
from others.storage import load_json_payload


def mother_team_pool_name(source_path: Path, mother_artifact: dict[str, Any] | None = None) -> str:
    payload: dict[str, Any] = {}
    try:
        payload = load_json_payload(source_path)
    except Exception:
        payload = {}
    payload = standardize_export_credential_payload(payload)

    email = str(
        ((mother_artifact or {}).get("email") if isinstance(mother_artifact, dict) else "")
        or payload.get("email")
        or ""
    ).strip()
    org_id = extract_org_id(payload)
    if email and org_id:
        return canonical_team_artifact_name(payload, is_mother=True)

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


def member_team_pool_name(source_path: Path, member_artifact: dict[str, Any] | None = None) -> str:
    payload: dict[str, Any] = {}
    try:
        payload = load_json_payload(source_path)
    except Exception:
        payload = {}
    payload = standardize_export_credential_payload(payload)
    email = str(
        ((member_artifact or {}).get("email") if isinstance(member_artifact, dict) else "")
        or payload.get("email")
        or ""
    ).strip()
    org_id = extract_org_id(payload)
    if email and org_id:
        return canonical_team_artifact_name(payload, is_mother=False)
    return str(source_path.name or "").strip() or "codex-team-unknown-org-unknown-email.json"


def restore_team_members_on_success(invite_result: Any) -> bool:
    if not isinstance(invite_result, dict):
        return False
    if bool(invite_result.get("restoreMembersToTeamPrePool")):
        return True
    return str(invite_result.get("status") or "").strip().lower() == "mother_only_all_invites_failed"


def preserve_mother_after_invite_result(invite_result: Any) -> bool:
    if not isinstance(invite_result, dict):
        return False
    if bool(invite_result.get("allInviteAttemptsFailed")):
        return True
    if not bool(invite_result.get("memberOauthRequired", True)):
        return True
    return str(invite_result.get("status") or "").strip().lower() == "mother_only_all_invites_failed"


def team_member_success_emails(invite_result: Any, oauth_result: Any) -> set[str]:
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


def team_member_discard_emails(invite_result: Any) -> set[str]:
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


def collect_result_has_mother(collect_result: Any) -> bool:
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


def collect_team_pool_artifacts(*, step_input: dict[str, Any]) -> dict[str, Any]:
    team_pool_dir = resolve_team_pool(step_input)
    ensure_directory(team_pool_dir)

    collected: list[dict[str, Any]] = []
    target_count = team_expand_target_count(step_input)
    mother_progress = load_team_expand_progress_from_artifact(
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
                prepared_artifact = prepare_named_artifact(
                    source_path=mother_path,
                    preferred_name=mother_team_pool_name(mother_path, mother_artifact),
                )
                team_pool_path = copy_delete_prepared_artifact_to_dir(
                    prepared_artifact,
                    destination_dir=team_pool_dir,
                    overwrite_existing=True,
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
                if staged_path.exists() and path_is_inside_directory(path=staged_path, directory=team_pool_dir):
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
                if path_is_inside_directory(path=member_path, directory=team_pool_dir):
                    team_pool_path = str(member_path)
                else:
                    prepared_artifact = prepare_named_artifact(
                        source_path=member_path,
                        preferred_name=member_team_pool_name(member_path, artifact),
                    )
                    team_pool_path = copy_delete_prepared_artifact_to_dir(
                        prepared_artifact,
                        destination_dir=team_pool_dir,
                        overwrite_existing=True,
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


def finalize_team_batch(*, step_input: dict[str, Any]) -> dict[str, Any]:
    task_error_code = str(step_input.get("task_error_code") or "").strip()
    invite_result = step_input.get("invite_result")
    oauth_result = step_input.get("oauth_result")
    restore_members_on_success_flag = restore_team_members_on_success(invite_result)
    target_count = team_expand_target_count(step_input)
    mother_progress = load_team_expand_progress_from_artifact(
        step_input.get("mother_artifact"),
        fallback_target=target_count,
    )
    mother_success_count = int(mother_progress.get("successCount") or 0)
    mother_ready_for_collection = bool(mother_progress.get("readyForMotherCollection"))
    preserve_mother_on_failure = collect_result_has_mother(step_input.get("collect_result")) or preserve_mother_after_invite_result(
        invite_result
    )
    restore_mother_for_iteration = (
        not bool(task_error_code)
        and not mother_ready_for_collection
        and mother_success_count > 0
    )
    successful_member_emails = team_member_success_emails(invite_result, oauth_result)
    discarded_member_emails = team_member_discard_emails(invite_result)
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
        ensure_directory(target_pool_dir)
        restored_path = restore_to_pool(
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
        pool_dir=resolve_team_mother_pool(
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
        team_pre_pool_dir = resolve_team_pre_pool(
            {
                "team_pre_pool_dir": str(step_input.get("team_pre_pool_dir") or "").strip(),
                "output_dir": step_input.get("output_dir"),
            }
        )
        for artifact in member_artifacts:
            member_email = str((artifact or {}).get("email") or "").strip().lower() if isinstance(artifact, dict) else ""
            member_restore_on_success = restore_members_on_success_flag
            member_force_delete = member_email in discarded_member_emails
            if not task_error_code and not restore_members_on_success_flag:
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
        "restore_members_on_success": restore_members_on_success_flag,
        "preserve_mother_on_failure": preserve_mother_on_failure,
        "restore_mother_for_iteration": restore_mother_for_iteration,
        "mother_progress": mother_progress,
        "restored": restored,
        "deleted": deleted,
    }
