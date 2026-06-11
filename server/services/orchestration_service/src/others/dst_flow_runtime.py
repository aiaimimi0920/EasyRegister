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
from others.resource_cleanup import ResourceCleanupGuard
from others.structured_logger import StructuredLogger
from others.circuit_breaker import CircuitBreaker


def _normalize_mailbox_provider(provider: Any) -> str:
    value = str(provider or "").strip().lower()
    alias_map = {
        "cloudflare-temp-email": "cloudflare_temp_email",
        "cloudflaretempemail": "cloudflare_temp_email",
        "mail-to-you": "m2u",
        "mailtoyou": "m2u",
        "tempmaillol": "tempmail-lol",
        "tempmail.lol": "tempmail-lol",
    }
    return alias_map.get(value, value)


def _mailbox_provider_from_ref(mailbox_ref: Any) -> str:
    value = str(mailbox_ref or "").strip()
    if not value:
        return ""
    if ":" not in value:
        return "moemail"
    return _normalize_mailbox_provider(value.split(":", 1)[0])


def _normalize_mailbox_email(email: Any) -> str:
    normalized = str(email or "").strip().lower()
    if "@" not in normalized:
        return ""
    local_part, _, domain = normalized.partition("@")
    local_part = local_part.strip()
    domain = domain.strip().lower()
    if not local_part or not domain:
        return ""
    return f"{local_part}@{domain}"


def _mailbox_domain_from_email(email: Any) -> str:
    normalized = _normalize_mailbox_email(email)
    if "@" not in normalized:
        return ""
    return normalized.rsplit("@", 1)[-1].strip().lower()


def _append_unique_text(existing: Any, value: str) -> list[str]:
    values: list[str] = []
    if isinstance(existing, list):
        values = [str(item or "").strip() for item in existing if str(item or "").strip()]
    elif str(existing or "").strip():
        values = [str(existing or "").strip()]
    if value and value not in values:
        values.append(value)
    return values


def _mailbox_retry_failure_class(*, error_code: str, error_message: str) -> tuple[str, str]:
    normalized_code = str(error_code or "").strip().lower()
    lowered = str(error_message or "").strip().lower()
    if (
        normalized_code == ErrorCodes.UNSUPPORTED_EMAIL
        or "unsupported_email" in lowered
        or "the email you provided is not supported" in lowered
        or (
            ("invalid_username" in lowered or "invalid username" in lowered)
            and "mailbox_provider=" in lowered
        )
    ):
        return "unsupported_email", "strong_mailbox_unsupported"
    if (
        normalized_code == ErrorCodes.OTP_TIMEOUT
        or "otp_timeout" in lowered
        or "timeout waiting for 6-digit code" in lowered
        or "chatgpt_login_email_otp_wait_failed" in lowered
    ):
        return "email_otp_timeout", "weak_attributed_email_otp_timeout"
    if "registration_disallowed" in lowered and "mailbox_provider=" in lowered:
        return "registration_disallowed", "strong_mailbox_registration_disallowed"
    if normalized_code == ErrorCodes.USER_REGISTER_400:
        return "create_account_user_register_400", "weak_attributed_generic_register_400"
    if normalized_code == ErrorCodes.INVALID_REQUEST_ERROR:
        return "create_account_invalid_request_error", "weak_attributed_invalid_request_error"
    return "", ""


def _current_mailbox_context(*, state: dict[str, Any], result: DstExecutionResult) -> dict[str, str]:
    mailbox_output = state.get("mailbox")
    if not isinstance(mailbox_output, dict):
        mailbox_output = result.outputs.get("acquire-mailbox")
    mailbox_output = mailbox_output if isinstance(mailbox_output, dict) else {}
    email = _normalize_mailbox_email(
        mailbox_output.get("email")
        or mailbox_output.get("emailAddress")
        or ""
    )
    mailbox_ref = str(
        mailbox_output.get("mailbox_ref")
        or mailbox_output.get("mailboxRef")
        or ""
    ).strip()
    mailbox_session_id = str(
        mailbox_output.get("session_id")
        or mailbox_output.get("sessionId")
        or mailbox_output.get("mailbox_session_id")
        or mailbox_output.get("mailboxSessionId")
        or ""
    ).strip()
    provider = _normalize_mailbox_provider(
        mailbox_output.get("provider")
        or mailbox_output.get("providerTypeKey")
        or ""
    )
    if not provider and mailbox_ref:
        provider = _mailbox_provider_from_ref(mailbox_ref)
    return {
        "provider": provider,
        "domain": _mailbox_domain_from_email(email),
        "email": email,
        "mailbox_ref": mailbox_ref,
        "mailbox_session_id": mailbox_session_id,
        "business_key": str(
            mailbox_output.get("business_key")
            or mailbox_output.get("businessKey")
            or ""
        ).strip().lower(),
    }


def _prepare_create_account_mailbox_retry_context(
    *,
    statement: DstStatement,
    state: dict[str, Any],
    result: DstExecutionResult,
    error_details: dict[str, Any],
    attempt_index: int,
) -> None:
    if str(statement.step_type or "").strip().lower() != "create_openai_account":
        return
    error_code = str(error_details.get("code") or "").strip().lower()
    error_message = str(error_details.get("message") or "").strip()
    failure_reason, failure_class = _mailbox_retry_failure_class(
        error_code=error_code,
        error_message=error_message,
    )
    if not failure_reason:
        return
    context = _current_mailbox_context(state=state, result=result)
    if not context.get("email") and not context.get("provider") and not context.get("mailbox_ref"):
        return
    task_state = state.get("task")
    if not isinstance(task_state, dict):
        return
    email = str(context.get("email") or "").strip().lower()
    domain = str(context.get("domain") or "").strip().lower()
    provider = str(context.get("provider") or "").strip().lower()
    if email:
        task_state["avoidMailboxEmails"] = _append_unique_text(task_state.get("avoidMailboxEmails"), email)
    if domain:
        task_state["avoidMailboxDomains"] = _append_unique_text(task_state.get("avoidMailboxDomains"), domain)
    if provider and failure_reason != "unsupported_email":
        task_state["avoidMailboxProviders"] = _append_unique_text(task_state.get("avoidMailboxProviders"), provider)
    task_state["avoidMailboxReason"] = failure_reason
    outcomes = result.outputs.get("mailbox-attempt-outcomes")
    if not isinstance(outcomes, list):
        outcomes = []
    outcomes.append(
        {
            "outcome": "failure",
            "failureReason": failure_reason,
            "failureClass": failure_class,
            "errorCode": error_code,
            "provider": provider,
            "domain": domain,
            "email": email,
            "mailbox_ref": str(context.get("mailbox_ref") or "").strip(),
            "mailbox_session_id": str(context.get("mailbox_session_id") or "").strip(),
            "business_key": str(context.get("business_key") or "").strip().lower(),
            "stepId": statement.step_id,
            "attempt": max(1, int(attempt_index or 1)),
        }
    )
    result.outputs["mailbox-attempt-outcomes"] = outcomes


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
                    "proxy_chain": dict(existing_output),
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


def step_retry_backoff_seconds(statement: DstStatement, attempt: int = 0) -> float:
    retry = step_retry_policy(statement)
    try:
        base = max(0.0, float(retry.get("backoffSeconds") or 0.0))
        if base == 0.0:
            return 0.0
        # 使用指数退避: base * 2^attempt，最大300秒
        from others.exponential_backoff import exponential_backoff
        return exponential_backoff(attempt, base_seconds=base, max_seconds=300.0)
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


def cleanup_failure_is_nonfatal(*, statement: DstStatement, flow_failed: bool) -> bool:
    if flow_failed:
        return False
    if not step_always_run(statement):
        return False
    stage = str(statement.metadata.get("stage") or "").strip().lower()
    if stage != "cleanup":
        return False
    return str(statement.step_type or "").strip().lower() in {
        "release_mailbox",
        "release_mailbox_sessions_by_email",
        "release_proxy_chain",
        "revoke_codex_member",
        "revoke_team_members",
    }


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
    error_details: dict[str, Any] | None = None,
    attempt_index: int = 1,
) -> None:
    retry = step_retry_policy(statement)
    refresh_saved_states = retry.get("refreshSavedStates")
    if not isinstance(refresh_saved_states, list):
        return
    _prepare_create_account_mailbox_retry_context(
        statement=statement,
        state=state,
        result=result,
        error_details=error_details or {},
        attempt_index=attempt_index,
    )
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


def task_retry_backoff_seconds(plan: DstPlan, attempt: int = 0) -> float:
    retry = task_retry_policy(plan)
    try:
        base = max(0.0, float(retry.get("backoffSeconds") or 0.0))
        if base == 0.0:
            return 0.0
        from others.exponential_backoff import exponential_backoff
        return exponential_backoff(attempt, base_seconds=base, max_seconds=300.0)
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


def _retain_output_across_task_retry(statement: DstStatement) -> bool:
    return str(statement.step_type or "").strip() == "acquire_openai_oauth_artifact" and bool(
        str(statement.save_as or "").strip()
    )


def _defer_cleanup_for_task_retry(*, statement: DstStatement, state: dict[str, Any]) -> bool:
    if str(statement.step_type or "").strip() != "finalize_openai_oauth_artifact":
        return False
    task = state.get("task") if isinstance(state.get("task"), dict) else {}
    return bool(task.get("willRetry"))


def run_dst_flow_once(
    *,
    output_dir: str | None = None,
    team_auth_path: str | Path | None = None,
    team_invite_enabled: bool | None = None,
    input_source_dir: str | None = None,
    input_claims_dir: str | None = None,
    login_entry_url: str | None = None,
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
        "inputSourceDir": str(input_source_dir or env_config.input_source_dir or "").strip(),
        "inputClaimsDir": str(input_claims_dir or env_config.input_claims_dir or "").strip(),
        "loginEntryUrl": str(login_entry_url or env_config.login_entry_url or "").strip(),
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
    task_retry_retained_outputs: dict[str, Any] = {}
    circuit_breaker = CircuitBreaker(failure_threshold=3, timeout_seconds=60.0)

    for task_attempt in range(1, task_retry_max_attempts(plan, task_max_attempts) + 1):
        # 检查熔断器
        if circuit_breaker.is_open():
            logger.warning("circuit_breaker_open", state=circuit_breaker.get_state())
            last_result.error = "Circuit breaker open - too many consecutive failures"
            last_result.task_context["circuitBreakerState"] = circuit_breaker.get_state()
            return last_result

        # 创建资源清理守护
        cleanup_guard = ResourceCleanupGuard()

        # 创建结构化日志
        logger = StructuredLogger({
            "flow_id": str(plan.flow_id or ""),
            "task_attempt": task_attempt
        })
        logger.info("task_started", platform=str(plan.platform or ""))

        resolved_team_auth_path = str(team_auth_path or "").strip()
        resolved_team_invite_enabled = bool(team_invite_enabled) if team_invite_enabled is not None else bool(resolved_team_auth_path)
        state = {
            "task": {
                "output_dir": str(output_dir or "").strip(),
                "team_auth_path": resolved_team_auth_path,
                "team_invite_enabled": resolved_team_invite_enabled,
                "team_invite_cleanup_enabled": resolved_team_invite_enabled and (not free_stop_after_validate),
                "input_source_dir": str(input_source_dir or env_config.input_source_dir or "").strip(),
                "input_claims_dir": str(input_claims_dir or env_config.input_claims_dir or "").strip(),
                "login_entry_url": str(login_entry_url or env_config.login_entry_url or "").strip(),
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

        try:
            for statement in plan.steps:
            if not statement_enabled(statement=statement, state=state):
                result.steps.setdefault(statement.step_id, "skipped")
                continue
            if flow_failed and not step_always_run(statement):
                result.steps.setdefault(statement.step_id, "skipped")
                continue
            if flow_failed and _defer_cleanup_for_task_retry(statement=statement, state=state):
                result.steps.setdefault(statement.step_id, "skipped_task_retry")
                continue
            retained_save_as = str(statement.save_as or "").strip()
            if (
                not flow_failed
                and retained_save_as
                and _retain_output_across_task_retry(statement)
                and retained_save_as in task_retry_retained_outputs
            ):
                retained_output = task_retry_retained_outputs[retained_save_as]
                result.steps[statement.step_id] = "ok"
                result.outputs[statement.step_id] = retained_output
                result.step_attempts[statement.step_id] = 1
                state[retained_save_as] = retained_output
                continue
            attempt_index = 0
            while True:
                attempt_index += 1
                result.step_attempts[statement.step_id] = attempt_index
                step_start = time.time()
                try:
                    logger.info("step_started", step_id=statement.step_id, attempt=attempt_index)
                    step_output = run_statement_once(statement=statement, state=state, result=result)
                    duration_ms = int((time.time() - step_start) * 1000)
                    logger.info("step_completed", step_id=statement.step_id, duration_ms=duration_ms)

                    retained_save_as = str(statement.save_as or "").strip()
                    if retained_save_as and _retain_output_across_task_retry(statement):
                        task_retry_retained_outputs[retained_save_as] = step_output

                    # 标记资源获取
                    step_type = str(statement.step_type or "").strip()
                    if step_type == "acquire_mailbox" and isinstance(step_output, dict):
                        cleanup_guard.mark_acquired("mailbox", step_output)
                    elif step_type == "acquire_proxy_chain" and isinstance(step_output, dict):
                        cleanup_guard.mark_acquired("proxy_chain", step_output)
                    # 标记资源释放
                    elif step_type == "release_mailbox":
                        cleanup_guard.mark_released("mailbox")
                    elif step_type == "release_proxy_chain":
                        cleanup_guard.mark_released("proxy_chain")

                    break
                except Exception as exc:
                    error_details = step_error_details(step_type=statement.step_type, exc=exc)
                    result.step_errors[statement.step_id] = error_details
                    logger.error("step_failed",
                        step_id=statement.step_id,
                        attempt=attempt_index,
                        error_code=error_details.get("code"),
                        error_message=str(exc)[:200]
                    )
                    if cleanup_failure_is_nonfatal(statement=statement, flow_failed=flow_failed):
                        result.steps[statement.step_id] = "cleanup_warning"
                        break
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
                            backoff_seconds = step_retry_backoff_seconds(statement, attempt=attempt_index)
                            if backoff_seconds > 0:
                                time.sleep(backoff_seconds)
                            refresh_retry_state(
                                statement=statement,
                                state=state,
                                result=result,
                                save_as_index=save_as_index,
                                error_details=error_details,
                                attempt_index=attempt_index,
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
                                state["task"]["willRetry"] = should_retry_task(
                                    plan=plan,
                                    error_step=statement.step_id,
                                    error_details=refresh_details,
                                    attempt_index=task_attempt,
                                    override=task_max_attempts,
                                )
                                flow_failed = True
                            break
                    result.steps[statement.step_id] = "failed"
                    if not flow_failed:
                        result.error = str(exc)
                        result.error_step = statement.step_id
                        state["task"]["errorCode"] = str(error_details.get("code") or "").strip()
                        state["task"]["errorStep"] = statement.step_id
                        state["task"]["willRetry"] = should_retry_task(
                            plan=plan,
                            error_step=statement.step_id,
                            error_details=error_details,
                            attempt_index=task_attempt,
                            override=task_max_attempts,
                        )
                        flow_failed = True
                    break
        finally:
            # 确保资源清理
            if cleanup_guard.acquired_resources:
                cleanup_results = cleanup_guard.cleanup_all()
                if cleanup_results:
                    result.task_context["cleanupResults"] = cleanup_results

        result.ok = not flow_failed
        last_result = result
        if result.ok:
            circuit_breaker.record_success()
            logger.info("task_completed", status="success")
            return result
        circuit_breaker.record_failure()
        logger.warning("task_failed", error_step=result.error_step, circuit_breaker_state=circuit_breaker.get_state())
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
        backoff_seconds = task_retry_backoff_seconds(plan, attempt=task_attempt)
        if backoff_seconds > 0:
            time.sleep(backoff_seconds)
    return last_result
