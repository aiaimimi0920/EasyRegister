from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

if __package__ in (None, ""):
    _CURRENT_DIR = Path(__file__).resolve().parent
    _SRC_DIR = _CURRENT_DIR.parent
    for _candidate in (_CURRENT_DIR, _SRC_DIR):
        candidate_text = str(_candidate)
        if candidate_text not in sys.path:
            sys.path.append(candidate_text)
    from errors import ErrorCodes, build_error_details, resolve_retry_codes
    from others.config import DstTaskEnvConfig
    from artifact_pool_flow import dispatch_orchestration_step
    from easyemail_flow import dispatch_easyemail_step
    from easyproxy_flow import dispatch_easyproxy_step
    from easyprotocol_flow import dispatch_easyprotocol_step
else:
    from .errors import ErrorCodes, build_error_details, resolve_retry_codes
    from .others.config import DstTaskEnvConfig
    from .artifact_pool_flow import dispatch_orchestration_step
    from .easyemail_flow import dispatch_easyemail_step
    from .easyproxy_flow import dispatch_easyproxy_step
    from .easyprotocol_flow import dispatch_easyprotocol_step


PLACEHOLDER_RE = re.compile(r"^\{\{\s*([a-zA-Z0-9_]+(?:\.[a-zA-Z0-9_]+)*)\s*\}\}$")
DEFAULT_DST_FLOW_PATH = (
    Path(__file__).resolve().parents[1]
    / "flows"
    / "codex-openai-account-v1.semantic-flow.json"
)


@dataclass(frozen=True)
class DstStatement:
    step_id: str
    step_type: str
    input: dict[str, Any] = field(default_factory=dict)
    save_as: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DstPlan:
    steps: list[DstStatement]
    platform: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class DstExecutionResult:
    ok: bool
    task_attempts: int = 1
    steps: dict[str, str] = field(default_factory=dict)
    outputs: dict[str, Any] = field(default_factory=dict)
    step_attempts: dict[str, int] = field(default_factory=dict)
    step_errors: dict[str, dict[str, Any]] = field(default_factory=dict)
    error: str = ""
    error_step: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "taskAttempts": int(self.task_attempts or 1),
            "steps": dict(self.steps),
            "outputs": dict(self.outputs),
            "stepAttempts": dict(self.step_attempts),
            "stepErrors": dict(self.step_errors),
            "error": self.error,
            "errorStep": self.error_step,
        }


OWNER_DISPATCHERS: dict[str, Callable[..., dict[str, Any]]] = {
    "orchestration": dispatch_orchestration_step,
    "easyemail": dispatch_easyemail_step,
    "easyproxy": dispatch_easyproxy_step,
    "easyprotocol": dispatch_easyprotocol_step,
}


def _step_output_ok(*, step_type: str, step_output: Any) -> tuple[bool, str]:
    normalized_step_type = str(step_type or "").strip()
    if normalized_step_type == "upload_file_to_r2":
        if isinstance(step_output, dict) and bool(step_output.get("ok")):
            return True, ""
        return False, str((step_output or {}).get("detail") or "upload_file_to_r2_failed").strip()
    if normalized_step_type == "obtain_team_mother_oauth":
        if isinstance(step_output, dict) and (
            bool(step_output.get("ok")) or bool(str(step_output.get("successPath") or "").strip())
        ):
            return True, ""
        return False, str(
            (step_output or {}).get("detail")
            or (step_output or {}).get("status")
            or "obtain_team_mother_oauth_failed"
        ).strip()
    if normalized_step_type in {
        "initialize_platform_organization",
        "initialize_chatgpt_login_session",
        "invite_codex_member",
        "revoke_codex_member",
        "invite_team_members",
        "obtain_team_member_oauth_batch",
        "revoke_team_members",
    }:
        if isinstance(step_output, dict) and bool(step_output.get("ok")):
            return True, ""
        return False, str(
            (step_output or {}).get("detail")
            or (step_output or {}).get("status")
            or f"{normalized_step_type}_failed"
        ).strip()
    if normalized_step_type == "release_proxy_chain":
        if isinstance(step_output, dict) and bool(step_output.get("released")):
            return True, ""
        return False, str((step_output or {}).get("detail") or "release_proxy_chain_failed").strip()
    if normalized_step_type == "release_mailbox":
        if isinstance(step_output, dict):
            released = step_output.get("released")
            detail = str(step_output.get("detail") or "").strip().lower()
            if released is True:
                return True, ""
            if detail in {
                "deleted",
                "not_found",
                "already_deleted",
                "provider_does_not_support_release",
                "skipped_non_moemail",
                "skipped_preserved_for_manual_oauth",
            }:
                return True, ""
            return False, str(step_output.get("detail") or "release_mailbox_failed").strip()
        return False, "release_mailbox_invalid_output"
    if normalized_step_type in {
        "fill_team_pre_pool",
        "acquire_team_mother_artifact",
        "acquire_team_member_candidates",
        "collect_team_pool_artifacts",
        "finalize_team_batch",
        "acquire_small_success_artifact",
        "finalize_small_success_artifact",
        "validate_free_personal_oauth",
        "sleep_seconds",
    }:
        if isinstance(step_output, dict) and bool(step_output.get("ok")):
            return True, ""
        return False, str(
            (step_output or {}).get("code")
            or (step_output or {}).get("detail")
            or (step_output or {}).get("status")
            or f"{normalized_step_type}_failed"
        ).strip()
    return True, ""


def _step_always_run(statement: DstStatement) -> bool:
    return bool(statement.metadata.get("alwaysRun"))


def _step_retry_policy(statement: DstStatement) -> dict[str, Any]:
    retry = statement.metadata.get("retry")
    return retry if isinstance(retry, dict) else {}


def _step_error_details(*, step_type: str, exc: BaseException) -> dict[str, Any]:
    return build_error_details(
        step_type=step_type,
        message=str(exc or "").strip(),
        detail=str(getattr(exc, "detail", "") or "").strip(),
        stage=str(getattr(exc, "stage", "") or "").strip(),
        category=str(getattr(exc, "category", "") or "").strip(),
        code=str(getattr(exc, "code", "") or "").strip(),
    )


def _should_retry_step(*, statement: DstStatement, error_details: dict[str, Any], attempt_index: int) -> bool:
    retry = _step_retry_policy(statement)
    try:
        max_attempts = max(1, int(retry.get("maxAttempts") or 1))
    except Exception:
        max_attempts = 1
    if attempt_index >= max_attempts:
        return False
    retry_codes = resolve_retry_codes(retry)
    if retry_codes:
        return str(error_details.get("code") or "").strip().lower() in retry_codes
    return False


def _step_retry_backoff_seconds(statement: DstStatement) -> float:
    retry = _step_retry_policy(statement)
    try:
        return max(0.0, float(retry.get("backoffSeconds") or 0.0))
    except Exception:
        return 0.0


def _maybe_prepare_special_step_retry(
    *,
    statement: DstStatement,
    state: dict[str, Any],
    result: DstExecutionResult,
    error_details: dict[str, Any],
) -> bool:
    if str(statement.step_type or "").strip() != "invite_codex_member":
        return False
    if str(error_details.get("code") or "").strip() != ErrorCodes.TEAM_SEATS_FULL:
        return False
    task_state = state.get("task") if isinstance(state.get("task"), dict) else {}
    if not isinstance(task_state, dict):
        return False
    recovery_attempts = int(task_state.get("__inviteSeatCleanupAttempts") or 0)
    if recovery_attempts >= 1:
        return False
    team_auth_path = str(task_state.get("team_auth_path") or "").strip()
    if not team_auth_path:
        return False
    task_state["__inviteSeatCleanupAttempts"] = recovery_attempts + 1
    try:
        cleanup_result = dispatch_easyprotocol_step(
            step_type="cleanup_codex_capacity",
            step_input={"team_auth_path": team_auth_path},
        )
    except Exception as exc:
        cleanup_result = {
            "ok": False,
            "status": "cleanup_transport_failed",
            "detail": str(exc),
            "response": None,
        }
    result.outputs["invite-codex-member-capacity-cleanup"] = cleanup_result
    if not isinstance(cleanup_result, dict):
        return False
    response_payload = cleanup_result.get("response") if isinstance(cleanup_result.get("response"), dict) else {}
    projected_snapshot = (
        response_payload.get("seatSnapshotAfterProjected")
        if isinstance(response_payload, dict)
        else {}
    )
    summary = projected_snapshot.get("summary") if isinstance(projected_snapshot, dict) else {}
    available_codex = int(summary.get("available_codex") or 0) if isinstance(summary, dict) else 0
    available_total = int(summary.get("available_total") or 0) if isinstance(summary, dict) else 0
    released_count = int(cleanup_result.get("revoked_invites") or 0) + int(cleanup_result.get("removed_users") or 0)
    if bool(cleanup_result.get("ok")):
        return True
    if available_codex > 0 and available_total > 0:
        return True
    if released_count > 0:
        return True
    return False

def _statement_enabled(*, statement: DstStatement, state: dict[str, Any]) -> bool:
    enabled_when = statement.metadata.get("enabledWhen")
    if enabled_when is None:
        return True
    resolved = _resolve_value(enabled_when, state)
    if isinstance(resolved, bool):
        return resolved
    if resolved is None:
        return False
    if isinstance(resolved, str):
        return bool(resolved.strip())
    if isinstance(resolved, (list, dict, tuple, set)):
        return len(resolved) > 0
    return bool(resolved)


def _run_statement_once(
    *,
    statement: DstStatement,
    state: dict[str, Any],
    result: DstExecutionResult,
) -> Any:
    owner = str(statement.metadata.get("owner") or "").strip().lower()
    if not owner:
        raise RuntimeError(f"dst_step_owner_missing:{statement.step_type}")
    dispatcher = OWNER_DISPATCHERS.get(owner)
    if dispatcher is None:
        raise RuntimeError(f"dst_step_owner_unsupported:{owner}")
    resolved_input = _resolve_value(statement.input, state)
    step_output = dispatcher(
        step_type=statement.step_type,
        step_input=resolved_input if isinstance(resolved_input, dict) else {},
    )
    step_ok, step_error = _step_output_ok(step_type=statement.step_type, step_output=step_output)
    if not step_ok:
        raise RuntimeError(step_error or f"{statement.step_type}_failed")
    result.steps[statement.step_id] = "ok"
    result.outputs[statement.step_id] = step_output
    if statement.save_as:
        state[statement.save_as] = step_output
    return step_output


def _refresh_retry_state(
    *,
    statement: DstStatement,
    state: dict[str, Any],
    result: DstExecutionResult,
    save_as_index: dict[str, DstStatement],
) -> None:
    retry = _step_retry_policy(statement)
    refresh_saved_states = retry.get("refreshSavedStates")
    if not isinstance(refresh_saved_states, list):
        return
    for saved_state_name in refresh_saved_states:
        normalized_name = str(saved_state_name or "").strip()
        refresh_statement = save_as_index.get(normalized_name)
        if refresh_statement is None:
            raise RuntimeError(f"dst_refresh_state_missing:{normalized_name}")
        refresh_attempts = int(result.step_attempts.get(refresh_statement.step_id, 0) or 0) + 1
        result.step_attempts[refresh_statement.step_id] = refresh_attempts
        _run_statement_once(statement=refresh_statement, state=state, result=result)


def load_dst_flow(path: str | Path | None = None) -> DstPlan:
    resolved_path = Path(path or DEFAULT_DST_FLOW_PATH).resolve()
    payload = json.loads(resolved_path.read_text(encoding="utf-8"))
    definition = payload.get("definition") if isinstance(payload.get("definition"), dict) else payload
    steps = definition.get("steps")
    if not isinstance(steps, list) or not steps:
        raise RuntimeError(f"dst flow missing steps: {resolved_path}")
    result_steps: list[DstStatement] = []
    for index, raw_step in enumerate(steps, start=1):
        if not isinstance(raw_step, dict):
            raise RuntimeError(f"dst flow step #{index} is not an object")
        step_id = str(raw_step.get("id") or f"step-{index}").strip() or f"step-{index}"
        step_type = str(raw_step.get("type") or "").strip()
        if not step_type:
            raise RuntimeError(f"dst flow step {step_id} missing type")
        metadata = raw_step.get("metadata") if isinstance(raw_step.get("metadata"), dict) else {}
        owner = str(metadata.get("owner") or "").strip().lower()
        if not owner:
            raise RuntimeError(f"dst flow step {step_id} missing metadata.owner")
        if owner not in OWNER_DISPATCHERS:
            raise RuntimeError(f"dst flow step {step_id} unsupported owner: {owner}")
        result_steps.append(
            DstStatement(
                step_id=step_id,
                step_type=step_type,
                input=raw_step.get("input") if isinstance(raw_step.get("input"), dict) else {},
                save_as=str(raw_step.get("saveAs") or raw_step.get("save_as") or "").strip() or None,
                metadata=metadata,
            )
        )
    return DstPlan(
        steps=result_steps,
        platform=str(definition.get("platform") or "").strip(),
        metadata=definition.get("metadata") if isinstance(definition.get("metadata"), dict) else {},
    )


def _task_retry_policy(plan: DstPlan) -> dict[str, Any]:
    retry = plan.metadata.get("taskRetry")
    return retry if isinstance(retry, dict) else {}


def _task_retry_max_attempts(plan: DstPlan, override: int | None = None) -> int:
    if override is not None:
        try:
            return max(1, int(override))
        except Exception:
            return 1
    retry = _task_retry_policy(plan)
    try:
        return max(1, int(retry.get("maxAttempts") or 1))
    except Exception:
        return 1


def _task_retry_backoff_seconds(plan: DstPlan) -> float:
    retry = _task_retry_policy(plan)
    try:
        return max(0.0, float(retry.get("backoffSeconds") or 0.0))
    except Exception:
        return 0.0


def _should_retry_task(
    *,
    plan: DstPlan,
    error_step: str,
    error_details: dict[str, Any],
    attempt_index: int,
    override: int | None = None,
) -> bool:
    if attempt_index >= _task_retry_max_attempts(plan, override):
        return False
    retry = _task_retry_policy(plan)
    retry_steps = retry.get("retryOnSteps")
    if isinstance(retry_steps, list) and retry_steps:
        normalized_steps = {str(item or "").strip() for item in retry_steps}
        if str(error_step or "").strip() not in normalized_steps:
            return False
    retry_codes = resolve_retry_codes(retry)
    if retry_codes:
        return str(error_details.get("code") or "").strip().lower() in retry_codes
    return False


def _resolve_placeholder(path_text: str, state: dict[str, Any]) -> Any:
    current: Any = state
    for part in path_text.split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return ""
    return current


def _resolve_value(value: Any, state: dict[str, Any]) -> Any:
    if isinstance(value, str):
        match = PLACEHOLDER_RE.match(value.strip())
        if match:
            return _resolve_placeholder(match.group(1), state)
        return value
    if isinstance(value, dict):
        return {key: _resolve_value(inner, state) for key, inner in value.items()}
    if isinstance(value, list):
        return [_resolve_value(item, state) for item in value]
    return value


def run_dst_flow_once(
    *,
    output_dir: str | None = None,
    team_auth_path: str | Path | None = None,
    preallocated_email: str | None = None,
    preallocated_session_id: str | None = None,
    preallocated_mailbox_ref: str | None = None,
    r2_target_folder: str | None = None,
    r2_bucket: str | None = None,
    r2_object_name: str | None = None,
    r2_account_id: str | None = None,
    r2_endpoint_url: str | None = None,
    r2_access_key_id: str | None = None,
    r2_secret_access_key: str | None = None,
    r2_region: str | None = None,
    r2_public_base_url: str | None = None,
    r2_upload_enabled: bool | None = None,
    small_success_pool_dir: str | None = None,
    flow_path: str | Path | None = None,
    task_max_attempts: int | None = None,
) -> DstExecutionResult:
    plan = load_dst_flow(flow_path)
    default_r2_target_folder = str(r2_target_folder or "").strip() or str(plan.platform or "").strip()
    effective_r2_upload_enabled = bool(default_r2_target_folder) if r2_upload_enabled is None else bool(r2_upload_enabled)
    save_as_index: dict[str, DstStatement] = {
        str(statement.save_as or "").strip(): statement
        for statement in plan.steps
        if str(statement.save_as or "").strip()
    }

    max_task_attempts = _task_retry_max_attempts(plan, task_max_attempts)
    task_attempt = 0
    last_result = DstExecutionResult(ok=False)
    failed_task_proxy_urls: list[str] = []
    while task_attempt < max_task_attempts:
        task_attempt += 1
        env_config = DstTaskEnvConfig.from_env()
        free_stop_after_validate = env_config.free_stop_after_validate
        state: dict[str, Any] = {
            "task": {
                "output_dir": str(output_dir or "").strip(),
                "team_auth_path": str(team_auth_path or "").strip(),
                "preallocated_email": str(preallocated_email or "").strip(),
                "preallocated_session_id": str(preallocated_session_id or "").strip(),
                "preallocated_mailbox_ref": str(preallocated_mailbox_ref or "").strip(),
                "r2_target_folder": default_r2_target_folder,
                "r2_bucket": str(r2_bucket or "").strip(),
                "r2_object_name": str(r2_object_name or "").strip(),
                "r2_account_id": str(r2_account_id or "").strip(),
                "r2_endpoint_url": str(r2_endpoint_url or "").strip(),
                "r2_access_key_id": str(r2_access_key_id or "").strip(),
                "r2_secret_access_key": str(r2_secret_access_key or "").strip(),
                "r2_region": str(r2_region or "").strip(),
                "r2_public_base_url": str(r2_public_base_url or "").strip(),
                "r2_upload_enabled": (effective_r2_upload_enabled and not free_stop_after_validate),
                "small_success_pool_dir": str(small_success_pool_dir or "").strip(),
                "team_pre_pool_dir": env_config.team_pre_pool_dir,
                "team_mother_pool_dir": env_config.team_mother_pool_dir,
                "team_mother_claims_dir": env_config.team_mother_claims_dir,
                "team_member_claims_dir": env_config.team_member_claims_dir,
                "team_post_pool_dir": env_config.team_post_pool_dir,
                "team_pool_dir": env_config.team_pool_dir,
                "team_pre_fill_count": env_config.team_pre_fill_count,
                "team_member_count": env_config.team_member_count,
                "team_workspace_selector": env_config.team_workspace_selector,
                "free_workspace_selector": env_config.free_workspace_selector,
                "free_oauth_delay_seconds": env_config.free_oauth_delay_seconds,
                "free_stop_after_validate": free_stop_after_validate,
                "free_stop_after_validate_cleanup_enabled": not free_stop_after_validate,
                "platform": str(plan.platform or "").strip(),
                "taskAttempt": task_attempt,
                "errorCode": "",
                "errorStep": "",
                "avoidProxyUrls": list(failed_task_proxy_urls),
            }
        }
        result = DstExecutionResult(ok=False, task_attempts=task_attempt)
        flow_failed = False

        for statement in plan.steps:
            if not _statement_enabled(statement=statement, state=state):
                result.steps.setdefault(statement.step_id, "skipped")
                continue
            if flow_failed and not _step_always_run(statement):
                result.steps.setdefault(statement.step_id, "skipped")
                continue
            attempt_index = 0
            while True:
                attempt_index += 1
                result.step_attempts[statement.step_id] = attempt_index
                try:
                    _run_statement_once(statement=statement, state=state, result=result)
                    break
                except Exception as exc:
                    error_details = _step_error_details(step_type=statement.step_type, exc=exc)
                    result.step_errors[statement.step_id] = error_details
                    if not flow_failed and _maybe_prepare_special_step_retry(
                        statement=statement,
                        state=state,
                        result=result,
                        error_details=error_details,
                    ):
                        continue
                    if not flow_failed and _should_retry_step(
                        statement=statement,
                        error_details=error_details,
                        attempt_index=attempt_index,
                    ):
                        try:
                            backoff_seconds = _step_retry_backoff_seconds(statement)
                            if backoff_seconds > 0:
                                time.sleep(backoff_seconds)
                            _refresh_retry_state(
                                statement=statement,
                                state=state,
                                result=result,
                                save_as_index=save_as_index,
                            )
                            continue
                        except Exception as refresh_exc:
                            refresh_details = _step_error_details(
                                step_type=statement.step_type,
                                exc=refresh_exc,
                            )
                            result.step_errors[statement.step_id] = refresh_details
                            result.steps[statement.step_id] = "failed"
                            if not flow_failed:
                                result.error = str(refresh_exc)
                                result.error_step = statement.step_id
                                state["task"]["errorCode"] = str(refresh_details.get("code") or "").strip()
                                state["task"]["errorStep"] = statement.step_id
                                flow_failed = True
                            break
                    result.steps[statement.step_id] = "failed"
                    if not flow_failed:
                        result.error = str(exc)
                        result.error_step = statement.step_id
                        state["task"]["errorCode"] = str(error_details.get("code") or "").strip()
                        state["task"]["errorStep"] = statement.step_id
                        flow_failed = True
                    break
        result.ok = not flow_failed
        last_result = result
        if result.ok:
            return result
        root_error_details = dict(result.step_errors.get(result.error_step) or {})
        if not _should_retry_task(
            plan=plan,
            error_step=result.error_step,
            error_details=root_error_details,
            attempt_index=task_attempt,
            override=task_max_attempts,
        ):
            return result
        proxy_chain_output = result.outputs.get("acquire-proxy-chain")
        if isinstance(proxy_chain_output, dict):
            proxy_url = str(proxy_chain_output.get("proxy_url") or "").strip().lower()
            if proxy_url and proxy_url not in failed_task_proxy_urls:
                failed_task_proxy_urls.append(proxy_url)
        backoff_seconds = _task_retry_backoff_seconds(plan)
        if backoff_seconds > 0:
            time.sleep(backoff_seconds)
    return last_result


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the unified top-level DST flow.")
    parser.add_argument("--output-dir", default="", help="Optional output directory.")
    parser.add_argument("--team-auth", default="", help="Optional team auth json path.")
    parser.add_argument("--email", default="", help="Optional preallocated mailbox email.")
    parser.add_argument("--session-id", default="", help="Optional preallocated mailbox session id.")
    parser.add_argument("--mailbox-ref", default="", help="Optional preallocated mailbox ref.")
    parser.add_argument("--r2-target-folder", default="", help="Optional R2 target folder to enable artifact upload.")
    parser.add_argument("--r2-bucket", default="", help="Optional R2 bucket override.")
    parser.add_argument("--r2-object-name", default="", help="Optional R2 object name override.")
    parser.add_argument("--r2-account-id", default="", help="Optional R2 account id override.")
    parser.add_argument("--r2-endpoint-url", default="", help="Optional R2 endpoint override.")
    parser.add_argument("--r2-access-key-id", default="", help="Optional R2 access key id override.")
    parser.add_argument("--r2-secret-access-key", default="", help="Optional R2 secret access key override.")
    parser.add_argument("--r2-region", default="", help="Optional R2 region override.")
    parser.add_argument("--r2-public-base-url", default="", help="Optional R2 public base url override.")
    parser.add_argument("--small-success-pool-dir", default="", help="Optional pooled small-success artifact directory.")
    parser.add_argument("--flow-path", default="", help="Optional semantic flow json path.")
    parser.add_argument("--task-max-attempts", default="", help="Optional task-level retry attempts override.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    result = run_dst_flow_once(
        output_dir=str(args.output_dir or "").strip() or None,
        team_auth_path=str(args.team_auth or "").strip() or None,
        preallocated_email=str(args.email or "").strip() or None,
        preallocated_session_id=str(args.session_id or "").strip() or None,
        preallocated_mailbox_ref=str(args.mailbox_ref or "").strip() or None,
        r2_target_folder=str(args.r2_target_folder or "").strip() or None,
        r2_bucket=str(args.r2_bucket or "").strip() or None,
        r2_object_name=str(args.r2_object_name or "").strip() or None,
        r2_account_id=str(args.r2_account_id or "").strip() or None,
        r2_endpoint_url=str(args.r2_endpoint_url or "").strip() or None,
        r2_access_key_id=str(args.r2_access_key_id or "").strip() or None,
        r2_secret_access_key=str(args.r2_secret_access_key or "").strip() or None,
        r2_region=str(args.r2_region or "").strip() or None,
        r2_public_base_url=str(args.r2_public_base_url or "").strip() or None,
        small_success_pool_dir=str(args.small_success_pool_dir or "").strip() or None,
        flow_path=str(args.flow_path or "").strip() or None,
        task_max_attempts=int(str(args.task_max_attempts or "").strip()) if str(args.task_max_attempts or "").strip() else None,
    )
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
