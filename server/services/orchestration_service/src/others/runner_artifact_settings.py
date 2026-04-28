from __future__ import annotations

import random
from pathlib import Path
from typing import Any

from easyprotocol_flow import dispatch_easyprotocol_step
from others.config import ArtifactRoutingConfig, DstTaskEnvConfig


def artifact_routing_config(*, output_root: Path | None = None) -> ArtifactRoutingConfig:
    return ArtifactRoutingConfig.from_env(output_root=output_root)


def should_cleanup_successful_run_output(result: Any) -> bool:
    try:
        if not bool(getattr(result, "ok", False)):
            return False
        steps = getattr(result, "steps", {}) or {}
        outputs = getattr(result, "outputs", {}) or {}
        if str(steps.get("upload-oauth-artifact") or "").strip().lower() != "ok":
            return False
        upload_output = outputs.get("upload-oauth-artifact")
        if isinstance(upload_output, dict) and bool(upload_output.get("ok")):
            return True
        return False
    except Exception:
        return False


def resolve_small_success_pool_dir(*, output_root: Path) -> Path:
    return artifact_routing_config(output_root=output_root).small_success_pool_dir


def resolve_small_success_wait_pool_dir(*, output_root: Path) -> Path:
    return artifact_routing_config(output_root=output_root).small_success_wait_pool_dir


def resolve_small_success_continue_pool_dir(*, output_root: Path) -> Path:
    return artifact_routing_config(output_root=output_root).small_success_continue_pool_dir


def resolve_free_oauth_pool_dir(*, output_root: Path) -> Path:
    return artifact_routing_config(output_root=output_root).free_oauth_pool_dir


def resolve_free_manual_oauth_pool_dir(*, output_root: Path) -> Path:
    return artifact_routing_config(output_root=output_root).free_manual_oauth_pool_dir


def resolve_free_local_dir(*, output_root: Path) -> Path:
    return artifact_routing_config(output_root=output_root).free_local_dir


def resolve_team_local_dir(*, output_root: Path) -> Path:
    return artifact_routing_config(output_root=output_root).team_local_dir


def select_local_split(*, percent: float) -> bool:
    if float(percent or 0.0) <= 0.0:
        return False
    if float(percent) >= 100.0:
        return True
    return random.random() * 100.0 < float(percent)


def small_success_wait_seconds() -> float:
    return artifact_routing_config().small_success_wait_seconds


def small_success_continue_prefill_count() -> int:
    return artifact_routing_config().small_success_continue_prefill_count


def small_success_continue_prefill_target_count() -> int:
    return artifact_routing_config().small_success_continue_prefill_target_count


def small_success_continue_prefill_min_age_seconds() -> float:
    return artifact_routing_config().small_success_continue_prefill_min_age_seconds


def free_stop_after_validate_mode() -> bool:
    return DstTaskEnvConfig.from_env().free_stop_after_validate


def upload_artifact_to_r2(*, source_path: Path, target_folder: str, object_name: str | None = None) -> dict[str, Any]:
    config = artifact_routing_config()
    step_input = {
        "source_path": str(source_path),
        "bucket": config.r2_bucket,
        "target_folder": str(target_folder or "").strip(),
        "object_name": str(object_name or "").strip() or source_path.name,
        "account_id": config.r2_account_id,
        "endpoint_url": config.r2_endpoint_url,
        "access_key_id": config.r2_access_key_id,
        "secret_access_key": config.r2_secret_access_key,
        "region": config.r2_region,
        "public_base_url": config.r2_public_base_url,
        "overwrite": True,
    }
    return dispatch_easyprotocol_step(step_type="upload_file_to_r2", step_input=step_input)
