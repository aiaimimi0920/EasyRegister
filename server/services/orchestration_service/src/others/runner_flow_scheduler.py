from __future__ import annotations

from contextlib import nullcontext
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
        "concurrencyLimit": max(0, int(spec.concurrency_limit or 0)),
        "teamAuthPath": str(spec.team_auth_path or "").strip(),
        "taskMaxAttempts": int(spec.task_max_attempts or 0),
        "openaiOauthPoolDir": str(spec.openai_oauth_pool_dir),
        "smallSuccessPoolDir": str(spec.openai_oauth_pool_dir),
        "mailboxBusinessKey": str(spec.mailbox_business_key or "").strip().lower(),
    }


def flow_slot_key(spec: RunnerFlowSpec) -> str:
    name = str(spec.name or "").strip()
    if name:
        return name
    role = normalize_flow_role(spec.instance_role)
    if role:
        return role
    return str(spec.flow_path or "").strip()


def reserve_flow_slot(
    *,
    spec: RunnerFlowSpec,
    active_flow_counts: Any,
    active_flow_lock: Any,
) -> bool:
    if active_flow_counts is None:
        return True
    key = flow_slot_key(spec)
    if not key:
        return True
    lock = active_flow_lock if active_flow_lock is not None else nullcontext()
    with lock:
        current = int(active_flow_counts.get(key, 0) or 0)
        limit = max(0, int(spec.concurrency_limit or 0))
        if limit > 0 and current >= limit:
            return False
        active_flow_counts[key] = current + 1
        return True


def release_flow_slot(
    *,
    spec: RunnerFlowSpec,
    active_flow_counts: Any,
    active_flow_lock: Any,
) -> None:
    if active_flow_counts is None:
        return
    key = flow_slot_key(spec)
    if not key:
        return
    lock = active_flow_lock if active_flow_lock is not None else nullcontext()
    with lock:
        current = int(active_flow_counts.get(key, 0) or 0)
        next_value = max(0, current - 1)
        if next_value <= 0:
            try:
                del active_flow_counts[key]
            except Exception:
                active_flow_counts[key] = 0
        else:
            active_flow_counts[key] = next_value


def snapshot_active_flow_counts(
    *,
    active_flow_counts: Any,
    active_flow_lock: Any,
) -> dict[str, int]:
    if active_flow_counts is None:
        return {}
    lock = active_flow_lock if active_flow_lock is not None else nullcontext()
    with lock:
        return {str(key): int(value or 0) for key, value in dict(active_flow_counts).items()}


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
    active_flow_counts: dict[str, int] | None = None,
) -> dict[str, Any]:
    summary = flow_spec_summary(spec)
    normalized_role = normalize_flow_role(spec.instance_role)
    slot_key = flow_slot_key(spec)
    active_count = int((active_flow_counts or {}).get(slot_key, 0) or 0)
    concurrency_limit = max(0, int(spec.concurrency_limit or 0))
    summary["slotKey"] = slot_key
    summary["activeCount"] = active_count
    if concurrency_limit > 0 and active_count >= concurrency_limit:
        return {
            **summary,
            "ready": False,
            "reason": "concurrency_limit_reached",
        }
    if normalized_role == "continue":
        ready = _path_has_json_files(spec.openai_oauth_pool_dir)
        return {
            **summary,
            "ready": ready,
            "reason": "pool_ready" if ready else "openai_oauth_pool_empty",
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
    active_flow_counts: dict[str, int] | None = None,
) -> tuple[RunnerFlowSpec | None, dict[str, Any]]:
    ready_specs: list[tuple[RunnerFlowSpec, dict[str, Any]]] = []
    skipped: list[dict[str, Any]] = []
    for spec in flow_specs:
        state = flow_spec_runnable_state(
            spec,
            output_root=output_root,
            shared_root=shared_root,
            active_flow_counts=active_flow_counts,
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

    continue_ready_specs = [
        (spec, state)
        for spec, state in ready_specs
        if normalize_flow_role(spec.instance_role) == "continue"
    ]
    if continue_ready_specs:
        ready_specs = continue_ready_specs

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
