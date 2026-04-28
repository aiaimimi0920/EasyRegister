from __future__ import annotations

import os
from pathlib import Path
from typing import Any

if __package__ in (None, "", "others"):
    import sys

    _CURRENT_DIR = Path(__file__).resolve().parent
    _PARENT_DIR = _CURRENT_DIR.parent
    for _candidate in (_CURRENT_DIR, _PARENT_DIR):
        candidate_text = str(_candidate)
        if candidate_text not in sys.path:
            sys.path.append(candidate_text)
    from config import (
        ArtifactRoutingConfig,
        CleanupRuntimeConfig,
        MailboxRuntimeConfig,
        ProxyRuntimeConfig,
        RunnerMainConfig,
        TeamAuthRuntimeConfig,
    )
    from paths import resolve_shared_root
else:
    from .config import (
        ArtifactRoutingConfig,
        CleanupRuntimeConfig,
        MailboxRuntimeConfig,
        ProxyRuntimeConfig,
        RunnerMainConfig,
        TeamAuthRuntimeConfig,
    )
    from .paths import resolve_shared_root


DEFAULT_EASY_PROXY_BASE_URL_HOST = "http://localhost:19888"
DEFAULT_EASY_PROXY_BASE_URL_DOCKER = "http://easy-proxy-service:9888"
DEFAULT_EASY_PROXY_RUNTIME_HOST_DOCKER = "easy-proxy-service"
DEFAULT_EASY_PROXY_TTL_MINUTES = 30
DEFAULT_EASY_PROXY_MODE = "auto"
DEFAULT_MAILBOX_TTL_SECONDS = 90
DEFAULT_REGISTER_MOEMAIL_DOMAIN_POOL = (
    "sall.cc",
    "cnmlgb.de",
    "zhooo.org",
    "cksa.eu.cc",
    "wqwq.eu.cc",
    "zhoo.eu.cc",
    "zhooo.ggff.net",
    "coolkidsa.ggff.net",
)
DEFAULT_REGISTER_MAILBOX_DOMAIN_BLACKLIST_MIN_ATTEMPTS = 20
DEFAULT_REGISTER_MAILBOX_DOMAIN_BLACKLIST_FAILURE_RATE = 90.0


def _running_in_docker() -> bool:
    if str(os.environ.get("RUNNING_IN_DOCKER") or "").strip():
        return True
    return Path("/.dockerenv").exists()


def _default_easy_proxy_management_base_url() -> str:
    return DEFAULT_EASY_PROXY_BASE_URL_DOCKER if _running_in_docker() else DEFAULT_EASY_PROXY_BASE_URL_HOST


def _mailbox_runtime_config() -> MailboxRuntimeConfig:
    output_root_text = str(os.environ.get("REGISTER_OUTPUT_ROOT") or "").strip()
    if output_root_text:
        default_state_path = resolve_shared_root(output_root_text) / "others" / "register-mailbox-domain-state.json"
    else:
        default_state_path = Path.cwd().resolve() / "others" / "register-mailbox-domain-state.json"
    return MailboxRuntimeConfig.from_env(
        default_ttl_seconds=DEFAULT_MAILBOX_TTL_SECONDS,
        default_state_path=default_state_path,
        default_business_domain_pool=DEFAULT_REGISTER_MOEMAIL_DOMAIN_POOL,
        default_blacklist_min_attempts=DEFAULT_REGISTER_MAILBOX_DOMAIN_BLACKLIST_MIN_ATTEMPTS,
        default_blacklist_failure_rate=DEFAULT_REGISTER_MAILBOX_DOMAIN_BLACKLIST_FAILURE_RATE,
    )


def _proxy_runtime_config() -> ProxyRuntimeConfig:
    return ProxyRuntimeConfig.from_env(
        default_management_base_url=_default_easy_proxy_management_base_url(),
        default_ttl_minutes=DEFAULT_EASY_PROXY_TTL_MINUTES,
        default_runtime_host=DEFAULT_EASY_PROXY_RUNTIME_HOST_DOCKER,
        default_mode=DEFAULT_EASY_PROXY_MODE,
        running_in_docker=_running_in_docker(),
    )


def _missing_r2_fields(artifact_config: ArtifactRoutingConfig) -> list[str]:
    field_map = {
        "r2_bucket": artifact_config.r2_bucket,
        "r2_account_id": artifact_config.r2_account_id,
        "r2_endpoint_url": artifact_config.r2_endpoint_url,
        "r2_access_key_id": artifact_config.r2_access_key_id,
        "r2_secret_access_key": artifact_config.r2_secret_access_key,
        "r2_region": artifact_config.r2_region,
    }
    present = [name for name, value in field_map.items() if str(value or "").strip()]
    if not present:
        return []
    return [name for name, value in field_map.items() if not str(value or "").strip()]


def validate_runtime_preflight() -> dict[str, Any]:
    runner_config = RunnerMainConfig.from_env()
    artifact_config = ArtifactRoutingConfig.from_env(output_root=runner_config.output_root)
    team_auth_config = TeamAuthRuntimeConfig.from_env(
        output_root=runner_config.output_root,
        shared_root=runner_config.shared_root,
    )
    cleanup_config = CleanupRuntimeConfig.from_env()
    proxy_config = _proxy_runtime_config()
    mailbox_config = _mailbox_runtime_config()

    errors: list[str] = []
    flow_path = Path(runner_config.flow_path).expanduser().resolve() if str(runner_config.flow_path or "").strip() else None
    if flow_path is not None and not flow_path.is_file():
        errors.append(f"missing_flow_path:{flow_path}")
    team_auth_path = Path(runner_config.team_auth_path).expanduser().resolve() if str(runner_config.team_auth_path or "").strip() else None
    if team_auth_path is not None and not team_auth_path.exists():
        errors.append(f"missing_team_auth_path:{team_auth_path}")
    missing_r2_fields = _missing_r2_fields(artifact_config)
    if missing_r2_fields:
        errors.append(f"incomplete_r2_config:{','.join(missing_r2_fields)}")

    if errors:
        raise RuntimeError(";".join(errors))

    return {
        "outputRoot": str(runner_config.output_root),
        "sharedRoot": str(runner_config.shared_root),
        "flowPath": str(flow_path or ""),
        "instanceId": runner_config.instance_id,
        "instanceRole": runner_config.instance_role,
        "proxy": {
            "enabled": proxy_config.enabled,
            "requiredByDefault": proxy_config.required_by_default,
            "mode": proxy_config.mode,
            "managementBaseUrl": proxy_config.management_base_url,
        },
        "mailbox": {
            "ttlSeconds": mailbox_config.ttl_seconds,
            "providers": list(mailbox_config.providers),
            "domainStatePath": str(mailbox_config.domain_state_path),
        },
        "teamAuth": {
            "motherPoolDir": str(team_auth_config.mother_pool_dir),
            "authPath": runner_config.team_auth_path,
            "reservationTtlSeconds": team_auth_config.reservation_ttl_seconds,
        },
        "artifactRouting": {
            "smallSuccessPoolDir": str(artifact_config.small_success_pool_dir),
            "freeOauthPoolDir": str(artifact_config.free_oauth_pool_dir),
            "r2Configured": not bool(missing_r2_fields),
        },
        "cleanup": {
            "teamCleanupCooldownSeconds": cleanup_config.team_cleanup_cooldown_seconds,
            "mailboxCleanupCooldownSeconds": cleanup_config.mailbox_cleanup_cooldown_seconds,
        },
    }
