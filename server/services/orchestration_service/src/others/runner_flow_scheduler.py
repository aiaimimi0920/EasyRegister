from __future__ import annotations

from contextlib import nullcontext
import random
from pathlib import Path
from typing import Any

from others.account_availability_audit import production_audit_has_due_targets
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
        "inputSourceDir": str(spec.input_source_dir or "").strip(),
        "inputClaimsDir": str(spec.input_claims_dir or "").strip(),
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
    active_flow_owners: Any | None = None,
    owner_id: str = "",
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
        normalized_owner_id = str(owner_id or "").strip()
        if active_flow_owners is not None and normalized_owner_id:
            active_flow_owners[normalized_owner_id] = key
        return True


def _decrement_flow_slot_by_key(*, key: str, active_flow_counts: Any) -> None:
    current = int(active_flow_counts.get(key, 0) or 0)
    next_value = max(0, current - 1)
    if next_value <= 0:
        try:
            del active_flow_counts[key]
        except Exception:
            active_flow_counts[key] = 0
    else:
        active_flow_counts[key] = next_value


def release_flow_slot(
    *,
    spec: RunnerFlowSpec,
    active_flow_counts: Any,
    active_flow_lock: Any,
    active_flow_owners: Any | None = None,
    owner_id: str = "",
) -> None:
    if active_flow_counts is None:
        return
    key = flow_slot_key(spec)
    if not key:
        return
    lock = active_flow_lock if active_flow_lock is not None else nullcontext()
    with lock:
        normalized_owner_id = str(owner_id or "").strip()
        if active_flow_owners is not None and normalized_owner_id:
            try:
                owned_key = str(active_flow_owners.get(normalized_owner_id, "") or "").strip()
            except Exception:
                owned_key = ""
            if owned_key:
                key = owned_key
            try:
                del active_flow_owners[normalized_owner_id]
            except Exception:
                pass
        _decrement_flow_slot_by_key(key=key, active_flow_counts=active_flow_counts)


def release_flow_slot_for_owner(
    *,
    owner_id: str,
    active_flow_counts: Any,
    active_flow_owners: Any,
    active_flow_lock: Any,
) -> str | None:
    normalized_owner_id = str(owner_id or "").strip()
    if not normalized_owner_id or active_flow_counts is None or active_flow_owners is None:
        return None
    lock = active_flow_lock if active_flow_lock is not None else nullcontext()
    with lock:
        try:
            key = str(active_flow_owners.get(normalized_owner_id, "") or "").strip()
        except Exception:
            key = ""
        if not key:
            return None
        try:
            del active_flow_owners[normalized_owner_id]
        except Exception:
            pass
        _decrement_flow_slot_by_key(key=key, active_flow_counts=active_flow_counts)
        return key


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
    check_account_audit_due_targets: bool = False,
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
    if normalized_role == "account-audit":
        configured_root = str(spec.input_source_dir or "").strip()
        output_root_for_audit = Path(configured_root).expanduser().resolve() if configured_root else output_root
        configured_input_ready = bool(configured_root) and _path_has_json_files(output_root_for_audit)
        if not check_account_audit_due_targets:
            ready = configured_input_ready or bool(configured_root)
            return {
                **summary,
                "ready": ready,
                "reason": (
                    "input_source_dir_ready"
                    if configured_input_ready
                    else "production_pool_maybe_ready"
                    if ready
                    else "production_pool_root_missing"
                ),
                "productionOutputRoot": str(output_root_for_audit),
            }
        production_ready = production_audit_has_due_targets(output_root=output_root_for_audit)
        ready = production_ready or configured_input_ready
        return {
            **summary,
            "ready": ready,
            "reason": (
                "production_pool_ready"
                if production_ready
                else "input_source_dir_ready"
                if configured_input_ready
                else "production_pool_empty_or_not_due"
            ),
            "productionOutputRoot": str(output_root_for_audit),
        }
    configured_input_source_dir = str(spec.input_source_dir or "").strip()
    if configured_input_source_dir:
        input_source_dir = Path(configured_input_source_dir).expanduser().resolve()
        ready = _path_has_json_files(input_source_dir)
        return {
            **summary,
            "ready": ready,
            "reason": "input_source_dir_ready" if ready else "input_source_dir_empty",
            "inputSourceDir": str(input_source_dir),
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
    previous_flow_role: str = "",
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
        main_ready_specs = [
            (spec, state)
            for spec, state in ready_specs
            if normalize_flow_role(spec.instance_role) == "main"
        ]
        ready_specs = (
            main_ready_specs
            if normalize_flow_role(previous_flow_role) == "continue" and main_ready_specs
            else continue_ready_specs
        )

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
