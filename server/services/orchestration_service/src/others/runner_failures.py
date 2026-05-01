from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from errors import ErrorCodes, result_error_matches, result_error_message, result_step_error
from others.common import ensure_directory, json_log, team_mother_cooldown_key, write_json_atomic
from others.config import CleanupRuntimeConfig, TeamAuthRuntimeConfig
from others.paths import resolve_team_mother_cooldowns_dir
from others.result_artifacts import result_payload, team_mother_identity


def _cleanup_runtime_config() -> CleanupRuntimeConfig:
    return CleanupRuntimeConfig.from_env()


def _team_auth_runtime_config(*, output_root: Path | None = None, shared_root: Path | None = None) -> TeamAuthRuntimeConfig:
    return TeamAuthRuntimeConfig.from_env(output_root=output_root, shared_root=shared_root)


def team_auth_blacklist_reason(*, result_payload_value: dict[str, Any]) -> str:
    error_step = str(result_payload_value.get("errorStep") or "").strip().lower()
    if error_step not in {
        "invite-codex-member",
        "invite-team-members",
        "cleanup-team-all-seats",
        "revoke-team-members",
        "refresh-team-auth-on-demand",
        "obtain-team-mother-oauth",
    }:
        return ""

    if error_step in {"invite-codex-member", "invite-team-members"} and result_error_matches(
        result_payload_value,
        ErrorCodes.TEAM_WORKSPACE_DEACTIVATED,
        step_id=error_step,
    ):
        return result_error_message(result_payload_value, error_step)

    if not result_error_matches(result_payload_value, ErrorCodes.TEAM_AUTH_TOKEN_INVALIDATED, step_id=error_step):
        return ""

    step_attempts = result_payload_value.get("stepAttempts") or {}
    refresh_attempts = int(step_attempts.get("refresh-team-auth-on-demand") or 0)
    mother_refresh_attempts = int(step_attempts.get("obtain-team-mother-oauth") or 0)

    if error_step == "invite-codex-member":
        invite_attempts = int(step_attempts.get("invite-codex-member") or 0)
        if refresh_attempts >= 1 and invite_attempts >= 2:
            return result_error_message(result_payload_value, error_step)
        return ""

    if error_step in {
        "invite-team-members",
        "cleanup-team-all-seats",
        "revoke-team-members",
        "refresh-team-auth-on-demand",
        "obtain-team-mother-oauth",
    }:
        if refresh_attempts >= 1 or mother_refresh_attempts >= 1:
            return result_error_message(result_payload_value, error_step)

    return ""


def team_mother_failure_cooldown_seconds(*, result: Any) -> float:
    payload = result_payload(result)
    error_step = str(payload.get("errorStep") or "").strip().lower()
    team_auth_config = _team_auth_runtime_config()

    if error_step == "obtain-team-mother-oauth":
        if result_error_matches(
            payload,
            ErrorCodes.REFRESH_TOKEN_REUSED,
            ErrorCodes.TEAM_MOTHER_TOKEN_VALIDATION_FAILED,
            ErrorCodes.INVALID_REQUEST_ERROR,
            ErrorCodes.AUTHORIZE_CONTINUE_RATE_LIMITED,
            ErrorCodes.OTP_TIMEOUT,
            ErrorCodes.MAILBOX_UNAVAILABLE,
            step_id="obtain-team-mother-oauth",
        ):
            return team_auth_config.oauth_failure_cooldown_seconds

    if error_step == "invite-team-members":
        if result_error_matches(
            payload,
            ErrorCodes.TEAM_SEATS_FULL,
            ErrorCodes.TEAM_INVITE_UPSTREAM_ERROR,
            step_id="invite-team-members",
        ):
            return team_auth_config.invite_failure_cooldown_seconds

    return 0.0


def mark_team_mother_failure_cooldown(
    *,
    shared_root: Path,
    result_payload_value: dict[str, Any],
    cooldown_seconds: float,
    reason: str,
    worker_label: str,
    task_index: int,
) -> dict[str, Any] | None:
    if float(cooldown_seconds or 0.0) <= 0:
        return None
    identity = team_mother_identity(result_payload_value)
    original_name = str(identity.get("original_name") or "").strip()
    email = str(identity.get("email") or "").strip()
    account_id = str(identity.get("account_id") or "").strip()
    if not original_name and not email and not account_id:
        return None

    cooldown_dir = resolve_team_mother_cooldowns_dir(str(shared_root))
    ensure_directory(cooldown_dir)
    state_path = cooldown_dir / f"{team_mother_cooldown_key(original_name=original_name, email=email, account_id=account_id)}.json"
    now = datetime.now(timezone.utc)
    cooldown_until = now + timedelta(seconds=max(0.0, float(cooldown_seconds or 0.0)))
    payload = {
        "original_name": original_name,
        "email": email,
        "account_id": account_id,
        "reason": str(reason or "").strip(),
        "cooldown_seconds": float(max(0.0, float(cooldown_seconds or 0.0))),
        "cooldown_started_at": now.isoformat(),
        "cooldown_until": cooldown_until.isoformat(),
        "cooldown_until_ts": cooldown_until.timestamp(),
    }
    write_json_atomic(state_path, payload, include_pid=True, cleanup_temp=True)
    json_log(
        {
            "event": "register_team_mother_cooldown_marked",
            "workerId": worker_label,
            "taskIndex": task_index,
            "statePath": str(state_path),
            **payload,
        }
    )
    return {"state_path": str(state_path), **payload}


def extra_failure_cooldown_seconds(*, result: Any) -> float:
    payload = result_payload(result)
    error_step = str(payload.get("errorStep") or "").strip().lower()
    cleanup_config = _cleanup_runtime_config()
    team_auth_config = _team_auth_runtime_config()

    if error_step == "create-openai-account":
        combined = result_error_message(payload, "create-openai-account").lower()
        if result_error_matches(
            payload,
            ErrorCodes.AUTHORIZE_CONTINUE_RATE_LIMITED,
            ErrorCodes.TRANSPORT_ERROR,
            step_id="create-openai-account",
        ) or any(
            marker in combined
            for marker in (
                "status=403",
                "platform_login status=403",
                "authorize_continue status=429",
                "unexpected_eof_while_reading",
                "eof occurred in violation of protocol",
            )
        ):
            return cleanup_config.create_account_cooldown_seconds

    if error_step == "acquire-mailbox":
        if bool(result_step_error(payload, "acquire-mailbox")):
            return cleanup_config.mailbox_failure_cooldown_seconds

    if error_step == "invite-codex-member":
        if result_error_matches(payload, ErrorCodes.TEAM_SEATS_FULL, step_id="invite-codex-member"):
            return team_auth_config.capacity_cooldown_seconds

    return 0.0
