from __future__ import annotations

from typing import Any

from others.artifact_pool_claims import (
    claim_small_success_artifact,
    claim_team_member_candidates,
    claim_team_mother_artifact,
    fill_team_pre_pool,
    finalize_small_success_artifact,
    sleep_seconds,
    validate_free_personal_oauth,
)
from others.artifact_pool_team_batch import (
    collect_team_pool_artifacts,
    finalize_team_batch,
)


def dispatch_orchestration_step(*, step_type: str, step_input: dict[str, Any]) -> dict[str, Any]:
    normalized_step_type = str(step_type or "").strip()
    if normalized_step_type == "sleep_seconds":
        return sleep_seconds(step_input=step_input)
    if normalized_step_type == "acquire_small_success_artifact":
        return claim_small_success_artifact(step_input=step_input)
    if normalized_step_type == "finalize_small_success_artifact":
        return finalize_small_success_artifact(step_input=step_input)
    if normalized_step_type == "validate_free_personal_oauth":
        return validate_free_personal_oauth(step_input=step_input)
    if normalized_step_type == "fill_team_pre_pool":
        return fill_team_pre_pool(step_input=step_input)
    if normalized_step_type == "acquire_team_mother_artifact":
        return claim_team_mother_artifact(step_input=step_input)
    if normalized_step_type == "acquire_team_member_candidates":
        return claim_team_member_candidates(step_input=step_input)
    if normalized_step_type == "collect_team_pool_artifacts":
        return collect_team_pool_artifacts(step_input=step_input)
    if normalized_step_type == "finalize_team_batch":
        return finalize_team_batch(step_input=step_input)
    raise RuntimeError(f"unsupported_orchestration_step:{normalized_step_type}")
