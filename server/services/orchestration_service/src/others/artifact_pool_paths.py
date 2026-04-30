from __future__ import annotations

from pathlib import Path
from typing import Any

from others.config import ArtifactRoutingConfig, TeamAuthRuntimeConfig
from others.paths import (
    resolve_shared_root,
    resolve_small_success_claims_dir,
    resolve_small_success_pool_dir,
    resolve_team_member_claims_dir,
    resolve_team_mother_claims_dir,
    resolve_team_mother_cooldowns_dir,
    resolve_team_mother_pool_dir,
    resolve_team_pool_dir,
    resolve_team_post_pool_dir,
    resolve_team_pre_pool_dir,
)


def derive_output_root_from_run_dir(output_dir: str | None) -> Path:
    if str(output_dir or "").strip():
        run_dir = Path(str(output_dir)).resolve()
        if run_dir.name.startswith("run-") and run_dir.parent.name.startswith("worker-"):
            return resolve_shared_root(str(run_dir.parents[1]))
        if run_dir.name.startswith("run-"):
            return resolve_shared_root(str(run_dir.parent))
        return resolve_shared_root(str(run_dir))
    return resolve_shared_root(str(Path.cwd()))


def artifact_routing_config_for_step_input(step_input: dict[str, Any]) -> ArtifactRoutingConfig:
    return ArtifactRoutingConfig.from_env(
        output_root=derive_output_root_from_run_dir(step_input.get("output_dir"))
    )


def team_auth_runtime_config_for_step_input(step_input: dict[str, Any] | None = None) -> TeamAuthRuntimeConfig:
    output_root = derive_output_root_from_run_dir((step_input or {}).get("output_dir"))
    shared_root = resolve_shared_root(str(output_root))
    return TeamAuthRuntimeConfig.from_env(output_root=output_root, shared_root=Path(shared_root))


def resolve_small_success_pool(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("pool_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return resolve_small_success_pool_dir(str(derive_output_root_from_run_dir(step_input.get("output_dir"))))


def resolve_small_success_claims(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("claims_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return resolve_small_success_claims_dir(str(derive_output_root_from_run_dir(step_input.get("output_dir"))))


def resolve_small_success_wait_pool(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("wait_pool_dir") or step_input.get("small_success_wait_pool_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return artifact_routing_config_for_step_input(step_input).small_success_wait_pool_dir


def resolve_small_success_continue_pool(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("continue_pool_dir") or step_input.get("small_success_continue_pool_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return artifact_routing_config_for_step_input(step_input).small_success_continue_pool_dir


def resolve_free_manual_oauth_pool(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("free_manual_oauth_pool_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return artifact_routing_config_for_step_input(step_input).free_manual_oauth_pool_dir


def resolve_team_pre_pool(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("team_pre_pool_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return resolve_team_pre_pool_dir(str(derive_output_root_from_run_dir(step_input.get("output_dir"))))


def resolve_team_mother_pool(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("team_mother_pool_dir") or step_input.get("pool_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return resolve_team_mother_pool_dir(str(derive_output_root_from_run_dir(step_input.get("output_dir"))))


def resolve_team_mother_claims(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("team_mother_claims_dir") or step_input.get("claims_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return resolve_team_mother_claims_dir(str(derive_output_root_from_run_dir(step_input.get("output_dir"))))


def resolve_team_mother_cooldowns(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("team_mother_cooldowns_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return resolve_team_mother_cooldowns_dir(str(derive_output_root_from_run_dir(step_input.get("output_dir"))))


def resolve_team_member_claims(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("team_member_claims_dir") or step_input.get("claims_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return resolve_team_member_claims_dir(str(derive_output_root_from_run_dir(step_input.get("output_dir"))))


def resolve_team_post_pool(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("team_post_pool_dir") or step_input.get("pool_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return resolve_team_post_pool_dir(str(derive_output_root_from_run_dir(step_input.get("output_dir"))))


def resolve_team_pool(step_input: dict[str, Any]) -> Path:
    explicit = str(step_input.get("team_pool_dir") or step_input.get("pool_dir") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return resolve_team_pool_dir(str(derive_output_root_from_run_dir(step_input.get("output_dir"))))


def path_is_inside_directory(*, path: Path, directory: Path) -> bool:
    try:
        resolved_path = path.resolve()
        resolved_dir = directory.resolve()
    except Exception:
        return False
    try:
        resolved_path.relative_to(resolved_dir)
        return True
    except Exception:
        return False
