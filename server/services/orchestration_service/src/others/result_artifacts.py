from __future__ import annotations

from pathlib import Path
from typing import Any


FREE_SMALL_SUCCESS_SOURCE_CANDIDATES: tuple[tuple[str, str], ...] = (
    ("create-openai-account", "storage_path"),
    ("create_openai_account", "storage_path"),
    ("acquire-small-success-artifact", "source_path"),
    ("acquire-small-success-artifact", "claimed_path"),
)

TEAM_MOTHER_PATH_CANDIDATES: tuple[tuple[str, str], ...] = (
    ("obtain-team-mother-oauth", "successPath"),
    ("acquire-team-mother-artifact", "source_path"),
    ("acquire-team-mother-artifact", "claimed_path"),
)


def result_payload(result: Any) -> dict[str, Any]:
    if isinstance(result, dict):
        return dict(result)
    try:
        payload = result.to_dict()
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass
    return {}


def result_outputs(result_or_payload: Any) -> dict[str, Any]:
    payload = result_payload(result_or_payload)
    outputs = payload.get("outputs")
    return outputs if isinstance(outputs, dict) else {}


def output_dict(result_or_payload: Any, step_id: str) -> dict[str, Any]:
    candidate = result_outputs(result_or_payload).get(step_id)
    return candidate if isinstance(candidate, dict) else {}


def output_text(result_or_payload: Any, step_id: str, *field_names: str) -> str:
    payload = output_dict(result_or_payload, step_id)
    for field_name in field_names:
        value = str(payload.get(field_name) or "").strip()
        if value:
            return value
    return ""


def first_output_text(
    result_or_payload: Any,
    candidates: list[tuple[str, str]] | tuple[tuple[str, str], ...],
) -> str:
    for value in all_output_texts(result_or_payload, candidates):
        return value
    return ""


def all_output_texts(
    result_or_payload: Any,
    candidates: list[tuple[str, str]] | tuple[tuple[str, str], ...],
) -> list[str]:
    outputs = result_outputs(result_or_payload)
    collected: list[str] = []
    for step_id, field_name in candidates:
        payload = outputs.get(step_id)
        if not isinstance(payload, dict):
            continue
        value = str(payload.get(field_name) or "").strip()
        if value:
            collected.append(value)
    return collected


def first_existing_output_path(
    result_or_payload: Any,
    candidates: list[tuple[str, str]] | tuple[tuple[str, str], ...],
) -> Path | None:
    candidate = first_output_text(result_or_payload, candidates)
    if not candidate:
        return None
    path = Path(candidate).resolve()
    if path.is_file():
        return path
    return None


def normalized_team_pool_artifacts(result_or_payload: Any) -> list[dict[str, str]]:
    collected = output_dict(result_or_payload, "collect-team-pool-artifacts")
    artifacts = collected.get("artifacts")
    if not isinstance(artifacts, list):
        return []
    normalized: list[dict[str, str]] = []
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            continue
        path_text = str(artifact.get("team_pool_path") or "").strip()
        if not path_text:
            continue
        normalized.append(
            {
                "kind": str(artifact.get("kind") or "").strip(),
                "email": str(artifact.get("email") or "").strip(),
                "preferred_name": str(artifact.get("preferred_name") or "").strip(),
                "path": str(Path(path_text).resolve()),
            }
        )
    return normalized


def team_mother_identity(result_or_payload: Any) -> dict[str, str]:
    mother_artifact = output_dict(result_or_payload, "acquire-team-mother-artifact")
    return {
        "original_name": str(mother_artifact.get("original_name") or "").strip(),
        "email": str(mother_artifact.get("email") or "").strip(),
        "account_id": str(mother_artifact.get("account_id") or "").strip(),
    }


def team_auth_path(result_or_payload: Any, fallback_path: str) -> str:
    candidate = first_output_text(result_or_payload, TEAM_MOTHER_PATH_CANDIDATES)
    if candidate:
        return candidate
    return str(fallback_path or "").strip()


def restored_path_for_source(result_or_payload: Any, source_path: Path) -> Path | None:
    normalized_source = str(source_path).lower()

    finalize_small = output_dict(result_or_payload, "finalize-small-success-artifact")
    claimed_path = str(finalize_small.get("claimed_path") or "").strip()
    restored_path = str(finalize_small.get("restored_path") or "").strip()
    if claimed_path and restored_path and normalized_source == str(Path(claimed_path).resolve()).lower():
        candidate = Path(restored_path).resolve()
        if candidate.exists():
            return candidate

    finalize_team = output_dict(result_or_payload, "finalize-team-batch")
    restored = finalize_team.get("restored")
    if not isinstance(restored, list):
        return None
    for item in restored:
        if not isinstance(item, dict):
            continue
        claimed_path = str(item.get("claimed_path") or "").strip()
        restored_path = str(item.get("restored_path") or "").strip()
        if not claimed_path or not restored_path:
            continue
        if normalized_source != str(Path(claimed_path).resolve()).lower():
            continue
        candidate = Path(restored_path).resolve()
        if candidate.exists():
            return candidate
    return None


def credential_backwrite_actions(result_or_payload: Any) -> list[dict[str, Any]]:
    if not result_outputs(result_or_payload):
        return []

    actions: list[dict[str, Any]] = []

    mother_source = output_text(result_or_payload, "acquire-team-mother-artifact", "source_path")
    mother_success = output_text(result_or_payload, "obtain-team-mother-oauth", "successPath")
    if mother_source and mother_success:
        actions.append(
            {
                "kind": "team_mother",
                "source_path": mother_source,
                "refreshed_path": mother_success,
                "force": True,
            }
        )

    codex_success = output_text(result_or_payload, "obtain-codex-oauth", "successPath")
    if codex_success:
        for source_path in all_output_texts(result_or_payload, FREE_SMALL_SUCCESS_SOURCE_CANDIDATES):
            actions.append(
                {
                    "kind": "generic_oauth_refresh",
                    "source_path": source_path,
                    "refreshed_path": codex_success,
                    "force": False,
                }
            )

    members = output_dict(result_or_payload, "acquire-team-member-candidates").get("members") or []
    oauth_batch = output_dict(result_or_payload, "obtain-team-member-oauth-batch").get("artifacts") or []
    if isinstance(members, list) and isinstance(oauth_batch, list):
        for index, item in enumerate(oauth_batch):
            if not isinstance(item, dict) or index >= len(members) or not isinstance(members[index], dict):
                continue
            refreshed_path = str(item.get("successPath") or "").strip()
            source_path = str(members[index].get("source_path") or members[index].get("claimed_path") or "").strip()
            if not refreshed_path or not source_path:
                continue
            actions.append(
                {
                    "kind": "team_member_oauth_refresh",
                    "source_path": source_path,
                    "refreshed_path": refreshed_path,
                    "force": False,
                }
            )

    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in actions:
        key = (
            str(item.get("source_path") or "").strip().lower(),
            str(item.get("refreshed_path") or "").strip().lower(),
        )
        if not key[0] or not key[1] or key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped
