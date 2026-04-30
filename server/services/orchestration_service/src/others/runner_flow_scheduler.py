from __future__ import annotations

import random
from pathlib import Path
from typing import Any

from others.config import RunnerFlowSpec, TeamAuthRuntimeConfig


def normalize_flow_role(value: str) -> str:
    return str(value or "").strip().lower()


def configured_flow_roles(flow_specs: tuple[RunnerFlowSpec, ...]) -> set[str]:
    roles: set[str] = set()
    for spec in flow_specs:
        normalized_role = normalize_flow_role(spec.instance_role)
        if normalized_role:
            roles.add(normalized_role)
    return roles


def flow_spec_summary(spec: RunnerFlowSpec) -> dict[str, Any]:
    return {
        "name": str(spec.name or "").strip(),
        "flowPath": str(spec.flow_path or "").strip(),
        "instanceRole": normalize_flow_role(spec.instance_role),
        "weight": float(spec.weight or 0.0),
        "teamAuthPath": str(spec.team_auth_path or "").strip(),
        "taskMaxAttempts": int(spec.task_max_attempts or 0),
        "smallSuccessPoolDir": str(spec.small_success_pool_dir),
        "mailboxBusinessKey": str(spec.mailbox_business_key or "").strip().lower(),
    }


def _path_has_json_files(path: Path) -> bool:
    if not path.is_dir():
        return False
    return any(candidate.is_file() for candidate in path.glob("*.json"))


def _team_mother_pool_dir(*, output_root: Path, shared_root: Path) -> Path:
    return TeamAuthRuntimeConfig.from_env(
        output_root=output_root,
        shared_root=shared_root,
    ).mother_pool_dir


def flow_spec_runnable_state(
    spec: RunnerFlowSpec,
    *,
    output_root: Path,
    shared_root: Path,
) -> dict[str, Any]:
    summary = flow_spec_summary(spec)
    normalized_role = normalize_flow_role(spec.instance_role)
    if normalized_role == "continue":
        ready = _path_has_json_files(spec.small_success_pool_dir)
        return {
            **summary,
            "ready": ready,
            "reason": "pool_ready" if ready else "small_success_pool_empty",
        }
    if normalized_role == "team":
        mother_pool_dir = _team_mother_pool_dir(output_root=output_root, shared_root=shared_root)
        ready = _path_has_json_files(mother_pool_dir)
        return {
            **summary,
            "ready": ready,
            "reason": "mother_pool_ready" if ready else "team_mother_pool_empty",
            "teamMotherPoolDir": str(mother_pool_dir),
        }
    return {
        **summary,
        "ready": True,
        "reason": "always_runnable",
    }


def choose_runnable_flow_spec(
    *,
    flow_specs: tuple[RunnerFlowSpec, ...],
    output_root: Path,
    shared_root: Path,
) -> tuple[RunnerFlowSpec | None, dict[str, Any]]:
    ready_specs: list[tuple[RunnerFlowSpec, dict[str, Any]]] = []
    skipped: list[dict[str, Any]] = []
    for spec in flow_specs:
        state = flow_spec_runnable_state(
            spec,
            output_root=output_root,
            shared_root=shared_root,
        )
        if bool(state.get("ready")):
            ready_specs.append((spec, state))
        else:
            skipped.append(state)
    if not ready_specs:
        return None, {
            "selected": None,
            "ready": [],
            "skipped": skipped,
        }

    total_weight = sum(max(0.0, float(spec.weight or 0.0)) for spec, _ in ready_specs)
    if total_weight <= 0.0:
        selected_spec, selected_state = ready_specs[0]
        return selected_spec, {
            "selected": selected_state,
            "ready": [state for _, state in ready_specs],
            "skipped": skipped,
        }

    draw = random.SystemRandom().random() * total_weight
    cumulative = 0.0
    selected_spec = ready_specs[-1][0]
    selected_state = ready_specs[-1][1]
    for candidate_spec, candidate_state in ready_specs:
        cumulative += max(0.0, float(candidate_spec.weight or 0.0))
        if draw <= cumulative:
            selected_spec = candidate_spec
            selected_state = candidate_state
            break
    return selected_spec, {
        "selected": selected_state,
        "ready": [state for _, state in ready_specs],
        "skipped": skipped,
    }
