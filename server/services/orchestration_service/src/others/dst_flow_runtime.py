from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from easyprotocol_flow import dispatch_easyprotocol_step
from errors import ErrorCodes, resolve_retry_codes
from others.config import DstTaskEnvConfig
from others.dst_flow_loader import load_dst_flow
from others.dst_flow_models import DstExecutionResult, DstPlan, DstStatement
from others.dst_flow_support import OWNER_DISPATCHERS
from others.dst_flow_support import resolve_value
from others.dst_flow_support import step_always_run
from others.dst_flow_support import step_error_details
from others.dst_flow_support import step_output_ok
from others.dst_flow_support import step_retry_policy


def _cleanup_saved_state_before_refresh(*, refresh_statement: DstStatement, state: dict[str, Any], result: DstExecutionResult) -> None:
    save_as_name = str(refresh_statement.save_as or "").strip()
    if not save_as_name:
        return
    existing_output = state.get(save_as_name)
    if not isinstance(existing_output, dict):
        existing_output = result.outputs.get(refresh_statement.step_id)
    if not isinstance(existing_output, dict) or not existing_output:
        return

    owner = str(refresh_statement.metadata.get("owner") or "").strip().lower()
    dispatcher = OWNER_DISPATCHERS.get(owner)
    if dispatcher is None:
        return

    normalized_step_type = str(refresh_statement.step_type or "").strip().lower()
    if normalized_step_type == "acquire_proxy_chain":
        proxy_url = str(existing_output.get("proxy_url") or "").strip()
        lease_id = str(existing_output.get("lease_id") or "").strip()
        if not proxy_url and not lease_id:
            return
        try:
            dispatcher(
                step_type="release_proxy_chain",
                step_input={
                    "proxy_url": proxy_url,
                    "lease_id": lease_id,
                    "error_code": "refresh_retry_state",
                },
            )
        except Exception:
            pass
        return

    if normalized_step_type == "acquire_mailbox":
        mailbox_ref = str(existing_output.get("mailbox_ref") or "").strip()
        mailbox_session_id = str(existing_output.get("session_id") or "").strip()
        provider = str(existing_output.get("provider") or "").strip().lower()
        if not mailbox_ref and not mailbox_session_id:
            return
        try:
            dispatcher(
                step_type="release_mailbox",
                step_input={
                    "provider": provider,
                    "mailbox_ref": mailbox_ref,
                    "mailbox_session_id": mailbox_session_id,
                    "error_code": "refresh_retry_state",
                },
            )
        except Exception:
            pass


def should_retry_step(*, statement: DstStatement, error_details: dict[str, Any], attempt_index: int) -> bool:
    retry = step_retry_policy(statement)
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


def step_retry_backoff_seconds(statement: DstStatement) -> float:
    retry = step_retry_policy(statement)
    try:
        return max(0.0, float(retry.get("backoffSeconds") or 0.0))
    except Exception:
        return 0.0


def maybe_prepare_special_step_retry(
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


def statement_enabled(*, statement: DstStatement, state: dict[str, Any]) -> bool:
    enabled_when = statement.metadata.get("enabledWhen")
    if enabled_when is None:
        return True
    resolved = resolve_value(enabled_when, state)
    if isinstance(resolved, bool):
        return resolved
    if resolved is None:
        return False
    if isinstance(resolved, str):
        return bool(resolved.strip())
    if isinstance(resolved, (list, dict, tuple, set)):
        return len(resolved) > 0
    return bool(resolved)


def run_statement_once(
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
    resolved_input = resolve_value(statement.input, state)
    step_output = dispatcher(
        step_type=statement.step_type,
        step_input=resolved_input if isinstance(resolved_input, dict) else {},
    )
    step_ok, step_error = step_output_ok(step_type=statement.step_type, step_output=step_output)
    if not step_ok:
        raise RuntimeError(step_error or f"{statement.step_type}_failed")
    result.steps[statement.step_id] = "ok"
    result.outputs[statement.step_id] = step_output
    if statement.save_as:
        state[statement.save_as] = step_output
    return step_output


def refresh_retry_state(
    *,
    statement: DstStatement,
    state: dict[str, Any],
    result: DstExecutionResult,
    save_as_index: dict[str, DstStatement],
) -> None:
    retry = step_retry_policy(statement)
    refresh_saved_states = retry.get("refreshSavedStates")
    if not isinstance(refresh_saved_states, list):
        return
    for saved_state_name in refresh_saved_states:
        normalized_name = str(saved_state_name or "").strip()
        refresh_statement = save_as_index.get(normalized_name)
        if refresh_statement is None:
            raise RuntimeError(f"dst_refresh_state_missing:{normalized_name}")
        _cleanup_saved_state_before_refresh(
            refresh_statement=refresh_statement,
            state=state,
            result=result,
        )
        refresh_attempts = int(result.step_attempts.get(refresh_statement.step_id, 0) or 0) + 1
        result.step_attempts[refresh_statement.step_id] = refresh_attempts
        run_statement_once(statement=refresh_statement, state=state, result=result)


def task_retry_policy(plan: DstPlan) -> dict[str, Any]:
    retry = plan.metadata.get("taskRetry")
    return retry if isinstance(retry, dict) else {}


def task_retry_max_attempts(plan: DstPlan, override: int | None = None) -> int:
    if override is not None:
        try:
            return max(1, int(override))
        except Exception:
            return 1
    retry = task_retry_policy(plan)
    try:
        return max(1, int(retry.get("maxAttempts") or 1))
    except Exception:
        return 1


def task_retry_backoff_seconds(plan: DstPlan) -> float:
    retry = task_retry_policy(plan)
    try:
        return max(0.0, float(retry.get("backoffSeconds") or 0.0))
    except Exception:
        return 0.0


def resolve_task_mailbox_business_key(plan: DstPlan, override: str | None = None) -> str:
    normalized_override = str(override or "").strip().lower()
    if normalized_override:
        return normalized_override
    metadata = plan.metadata if isinstance(plan.metadata, dict) else {}
    mailbox_metadata = metadata.get("mailbox") if isinstance(metadata.get("mailbox"), dict) else {}
    candidate = (
        mailbox_metadata.get("businessKey")
        or mailbox_metadata.get("business_key")
        or metadata.get("mailboxBusinessKey")
        or metadata.get("businessKey")
        or ""
    )
    return str(candidate or "").strip().lower()


def should_retry_task(
    *,
    plan: DstPlan,
    error_step: str,
    error_details: dict[str, Any],
    attempt_index: int,
    override: int | None = None,
) -> bool:
    if attempt_index >= task_retry_max_attempts(plan, override):
        return False
    retry = task_retry_policy(plan)
    retry_steps = retry.get("retryOnSteps")
    if isinstance(retry_steps, list) and retry_steps:
        normalized_steps = {str(item or "").strip() for item in retry_steps}
        if str(error_step or "").strip() not in normalized_steps:
            return False
    retry_codes = resolve_retry_codes(retry)
    if retry_codes:
        return str(error_details.get("code") or "").strip().lower() in retry_codes
    return False


def run_dst_flow_once(
    *,
    output_dir: str | None = None,
    team_auth_path: str | Path | None = None,
    team_invite_enabled: bool | None = None,
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
    openai_oauth_pool_dir: str | None = None,
    flow_path: str | Path | None = None,
    task_max_attempts: int | None = None,
    mailbox_business_key: str | None = None,
    failed_task_proxy_urls: list[str] | None = None,
) -> DstExecutionResult:
    plan = load_dst_flow(flow_path)
    env_config = DstTaskEnvConfig.from_env()
    free_stop_after_validate = bool(env_config.free_stop_after_validate)
    failed_task_proxy_urls = list(failed_task_proxy_urls or [])
    resolved_flow_path = str(Path(flow_path).resolve()) if flow_path else ""
    resolved_mailbox_business_key = resolve_task_mailbox_business_key(
        plan,
        override=mailbox_business_key,
    )
    base_task_context = {
        "flowId": str(plan.flow_id or "").strip(),
        "flowPath": resolved_flow_path,
        "platform": str(plan.platform or "").strip(),
        "mailboxBusinessKey": resolved_mailbox_business_key,
    }
    last_result = DstExecutionResult(
        ok=False,
        task_attempts=1,
        task_context=dict(base_task_context),
    )
    save_as_index = {
        str(statement.save_as or "").strip(): statement
        for statement in plan.steps
        if str(statement.save_as or "").strip()
    }
    for task_attempt in range(1, task_retry_max_attempts(plan, task_max_attempts) + 1):
        resolved_team_auth_path = str(team_auth_path or "").strip()
        resolved_team_invite_enabled = bool(team_invite_enabled) if team_invite_enabled is not None else bool(resolved_team_auth_path)
        state = {
            "task": {
                "output_dir": str(output_dir or "").strip(),
                "team_auth_path": resolved_team_auth_path,
                "team_invite_enabled": resolved_team_invite_enabled,
                "team_invite_cleanup_enabled": resolved_team_invite_enabled and (not free_stop_after_validate),
                "preallocated_email": str(preallocated_email or "").strip(),
                "preallocated_session_id": str(preallocated_session_id or "").strip(),
                "preallocated_mailbox_ref": str(preallocated_mailbox_ref or "").strip(),
                "r2_target_folder": str(r2_target_folder or "").strip(),
                "r2_bucket": str(r2_bucket or "").strip(),
                "r2_object_name": str(r2_object_name or "").strip(),
                "r2_account_id": str(r2_account_id or "").strip(),
                "r2_endpoint_url": str(r2_endpoint_url or "").strip(),
                "r2_access_key_id": str(r2_access_key_id or "").strip(),
                "r2_secret_access_key": str(r2_secret_access_key or "").strip(),
                "r2_region": str(r2_region or "").strip(),
                "r2_public_base_url": str(r2_public_base_url or "").strip(),
                "r2_upload_enabled": bool(r2_upload_enabled) if r2_upload_enabled is not None else False,
                "openai_oauth_pool_dir": str(openai_oauth_pool_dir or "").strip(),
                "mailbox_ttl_seconds": env_config.mailbox_ttl_seconds,
                "mailbox_recreate_preallocated": bool(env_config.mailbox_recreate_preallocated),
                "team_pre_fill_count": env_config.team_pre_fill_count,
                "team_member_count": env_config.team_member_count,
                "team_workspace_selector": env_config.team_workspace_selector,
                "free_workspace_selector": env_config.free_workspace_selector,
                "free_oauth_delay_seconds": env_config.free_oauth_delay_seconds,
                "free_stop_after_validate": free_stop_after_validate,
                "free_stop_after_validate_cleanup_enabled": not free_stop_after_validate,
                "platform": str(plan.platform or "").strip(),
                "flow_id": str(plan.flow_id or "").strip(),
                "flow_path": resolved_flow_path,
                "mailbox_business_key": resolved_mailbox_business_key,
                "taskAttempt": task_attempt,
                "errorCode": "",
                "errorStep": "",
                "avoidProxyUrls": list(failed_task_proxy_urls),
            }
        }
        result = DstExecutionResult(
            ok=False,
            task_attempts=task_attempt,
            task_context={
                **base_task_context,
                "taskAttempt": task_attempt,
            },
        )
        flow_failed = False

        for statement in plan.steps:
            if not statement_enabled(statement=statement, state=state):
                result.steps.setdefault(statement.step_id, "skipped")
                continue
            if flow_failed and not step_always_run(statement):
                result.steps.setdefault(statement.step_id, "skipped")
                continue
            attempt_index = 0
            while True:
                attempt_index += 1
                result.step_attempts[statement.step_id] = attempt_index
                try:
                    run_statement_once(statement=statement, state=state, result=result)
                    break
                except Exception as exc:
                    error_details = step_error_details(step_type=statement.step_type, exc=exc)
                    result.step_errors[statement.step_id] = error_details
                    if not flow_failed and maybe_prepare_special_step_retry(
                        statement=statement,
                        state=state,
                        result=result,
                        error_details=error_details,
                    ):
                        continue
                    if not flow_failed and should_retry_step(
                        statement=statement,
                        error_details=error_details,
                        attempt_index=attempt_index,
                    ):
                        try:
                            backoff_seconds = step_retry_backoff_seconds(statement)
                            if backoff_seconds > 0:
                                time.sleep(backoff_seconds)
                            refresh_retry_state(
                                statement=statement,
                                state=state,
                                result=result,
                                save_as_index=save_as_index,
                            )
                            continue
                        except Exception as refresh_exc:
                            refresh_details = step_error_details(
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
        if not should_retry_task(
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
        backoff_seconds = task_retry_backoff_seconds(plan)
        if backoff_seconds > 0:
            time.sleep(backoff_seconds)
    return last_result
