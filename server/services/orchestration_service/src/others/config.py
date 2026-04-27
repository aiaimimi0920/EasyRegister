from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

if __package__ in (None, "", "others"):
    import sys

    _CURRENT_DIR = Path(__file__).resolve().parent
    _PARENT_DIR = _CURRENT_DIR.parent
    for _candidate in (_CURRENT_DIR, _PARENT_DIR):
        candidate_text = str(_candidate)
        if candidate_text not in sys.path:
            sys.path.append(candidate_text)
    from paths import (
        resolve_free_oauth_pool_dir,
        resolve_shared_root,
        resolve_small_success_pool_dir,
        resolve_team_mother_pool_dir,
    )
else:
    from .paths import (
        resolve_free_oauth_pool_dir,
        resolve_shared_root,
        resolve_small_success_pool_dir,
        resolve_team_mother_pool_dir,
    )


def env_text(name: str, default: str = "") -> str:
    return str(os.environ.get(name) or default).strip()


def env_first_text(*names: str, default: str = "") -> str:
    for name in names:
        value = env_text(name, "")
        if value:
            return value
    return str(default).strip()


def env_bool(name: str, default: bool = False) -> bool:
    raw = env_text(name, "")
    if not raw:
        return bool(default)
    return raw.lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    raw = env_text(name, str(default))
    try:
        return int(raw)
    except Exception:
        return default


def env_float(name: str, default: float) -> float:
    raw = env_text(name, str(default))
    try:
        return float(raw)
    except Exception:
        return default


def env_ratio(name: str, default: float = 0.0) -> float:
    raw = env_text(name, str(default))
    try:
        value = float(raw)
    except Exception:
        value = float(default or 0.0)
    if value > 1.0:
        value = value / 100.0
    return max(0.0, min(1.0, value))


def env_percent_value(name: str, default: float = 0.0) -> float:
    raw = env_text(name, str(default))
    try:
        value = float(raw)
    except Exception:
        value = float(default or 0.0)
    if 0.0 < value <= 1.0:
        value = value * 100.0
    return max(0.0, min(100.0, value))


def split_csv(raw: str) -> tuple[str, ...]:
    normalized = (
        str(raw or "")
        .replace(";", ",")
        .replace("|", ",")
        .replace("\r", ",")
        .replace("\n", ",")
    )
    values: list[str] = []
    seen: set[str] = set()
    for item in normalized.split(","):
        cleaned = str(item or "").strip()
        if not cleaned:
            continue
        lowered = cleaned.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        values.append(cleaned)
    return tuple(values)


def split_path_list(raw: str) -> tuple[str, ...]:
    normalized = str(raw or "").strip()
    if not normalized:
        return ()
    values: list[str] = []
    seen: set[str] = set()
    for item in normalized.split(os.pathsep):
        cleaned = str(item or "").strip()
        if not cleaned:
            continue
        lowered = cleaned.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        values.append(cleaned)
    return tuple(values)


def env_path(name: str, default: str = "") -> Path:
    return Path(env_text(name, default) or default).expanduser().resolve()


def _resolve_output_root_text(default: str = "/shared/register-output") -> str:
    return env_text("REGISTER_OUTPUT_ROOT", default) or default


def _resolve_output_root(default: str = "/shared/register-output") -> Path:
    return Path(_resolve_output_root_text(default)).expanduser().resolve()


def _resolve_shared_root(default_output_root: str = "/shared/register-output") -> Path:
    return resolve_shared_root(_resolve_output_root_text(default_output_root))


@dataclass(frozen=True)
class DashboardSettings:
    enabled: bool
    listen: str
    allow_remote: bool
    recent_window_seconds: int

    @classmethod
    def from_env(cls) -> "DashboardSettings":
        return cls(
            enabled=env_bool("REGISTER_DASHBOARD_ENABLED", False),
            listen=env_text("REGISTER_DASHBOARD_LISTEN", "127.0.0.1:9790") or "127.0.0.1:9790",
            allow_remote=env_bool("REGISTER_DASHBOARD_ALLOW_REMOTE", False),
            recent_window_seconds=max(1, env_int("REGISTER_DASHBOARD_RECENT_WINDOW_SECONDS", 900)),
        )


@dataclass(frozen=True)
class DstTaskEnvConfig:
    team_pre_pool_dir: str
    team_mother_pool_dir: str
    team_mother_claims_dir: str
    team_member_claims_dir: str
    team_post_pool_dir: str
    team_pool_dir: str
    team_pre_fill_count: str
    team_member_count: str
    team_workspace_selector: str
    free_workspace_selector: str
    free_oauth_delay_seconds: str
    free_stop_after_validate: bool

    @classmethod
    def from_env(cls) -> "DstTaskEnvConfig":
        return cls(
            team_pre_pool_dir=env_text("REGISTER_TEAM_PRE_POOL_DIR"),
            team_mother_pool_dir=env_text("REGISTER_TEAM_MOTHER_POOL_DIR"),
            team_mother_claims_dir=env_text("REGISTER_TEAM_MOTHER_CLAIMS_DIR"),
            team_member_claims_dir=env_text("REGISTER_TEAM_MEMBER_CLAIMS_DIR"),
            team_post_pool_dir=env_text("REGISTER_TEAM_POST_POOL_DIR"),
            team_pool_dir=env_text("REGISTER_TEAM_POOL_DIR"),
            team_pre_fill_count=env_text("REGISTER_TEAM_PRE_FILL_COUNT"),
            team_member_count=env_text("REGISTER_TEAM_MEMBER_COUNT"),
            team_workspace_selector=env_text("REGISTER_TEAM_WORKSPACE_SELECTOR"),
            free_workspace_selector=env_text("REGISTER_FREE_WORKSPACE_SELECTOR", "personal") or "personal",
            free_oauth_delay_seconds=env_text("REGISTER_FREE_OAUTH_DELAY_SECONDS", "180") or "180",
            free_stop_after_validate=env_bool("REGISTER_FREE_STOP_AFTER_VALIDATE", False),
        )


@dataclass(frozen=True)
class RunnerMainConfig:
    output_root: Path
    shared_root: Path
    delay_seconds: float
    worker_count: int
    worker_stagger_seconds: float
    max_runs: int
    task_max_attempts: int
    team_auth_path: str
    flow_path: str
    small_success_pool_dir: Path
    free_oauth_pool_dir: Path
    instance_id: str
    instance_role: str
    easy_protocol_base_url: str
    easy_protocol_control_token: str
    easy_protocol_control_actor: str

    @classmethod
    def from_env(cls) -> "RunnerMainConfig":
        output_root = _resolve_output_root()
        shared_root = _resolve_shared_root()
        instance_id = env_text("REGISTER_INSTANCE_ID", "main") or "main"
        instance_role = env_text("REGISTER_INSTANCE_ROLE", instance_id) or instance_id
        return cls(
            output_root=output_root,
            shared_root=shared_root,
            delay_seconds=max(0.0, env_float("REGISTER_LOOP_DELAY_SECONDS", 5.0)),
            worker_count=max(1, env_int("REGISTER_WORKER_COUNT", 10)),
            worker_stagger_seconds=max(0.0, env_float("REGISTER_WORKER_STAGGER_SECONDS", 2.0)),
            max_runs=max(0, env_int("REGISTER_INFINITE_MAX_RUNS", 0)),
            task_max_attempts=env_int("REGISTER_TASK_MAX_ATTEMPTS", 0),
            team_auth_path=env_text("REGISTER_TEAM_AUTH_PATH"),
            flow_path=env_text("REGISTER_FLOW_PATH"),
            small_success_pool_dir=Path(resolve_small_success_pool_dir(str(output_root))).resolve(),
            free_oauth_pool_dir=Path(resolve_free_oauth_pool_dir(str(output_root))).resolve(),
            instance_id=instance_id,
            instance_role=instance_role,
            easy_protocol_base_url=env_text("EASY_PROTOCOL_BASE_URL", "http://easy-protocol-service:9788"),
            easy_protocol_control_token=env_text("EASY_PROTOCOL_CONTROL_TOKEN", ""),
            easy_protocol_control_actor=env_text("EASY_PROTOCOL_CONTROL_ACTOR", "register-dashboard"),
        )


@dataclass(frozen=True)
class ProxyRuntimeConfig:
    enabled: bool
    required_by_default: bool
    management_base_url: str
    api_key: str
    ttl_minutes: int
    mode: str
    runtime_host: str
    unique_attempts: int
    recent_window_seconds: int
    failure_window_seconds: int

    @classmethod
    def from_env(
        cls,
        *,
        default_management_base_url: str,
        default_ttl_minutes: int,
        default_runtime_host: str,
        default_mode: str,
        running_in_docker: bool,
    ) -> "ProxyRuntimeConfig":
        del running_in_docker
        management_base_url = env_first_text(
            "EASY_PROXY_BASE_URL",
            "EASY_PROXY_MANAGEMENT_URL",
            default=default_management_base_url,
        )
        mode = env_text("REGISTER_PROXY_MODE", default_mode).lower()
        if mode in {"lease", "compat"}:
            mode = "lease"
        elif mode in {"random", "random-node", "random_node"}:
            mode = "random-node"
        else:
            mode = default_mode
        return cls(
            enabled=env_bool("REGISTER_ENABLE_EASY_PROXY", True),
            required_by_default=env_bool("REGISTER_REQUIRE_EASY_PROXY", True),
            management_base_url=management_base_url,
            api_key=env_text("EASY_PROXY_API_KEY"),
            ttl_minutes=max(1, env_int("REGISTER_PROXY_TTL_MINUTES", env_int("EASY_PROXY_TTL_MINUTES", default_ttl_minutes))),
            mode=mode,
            runtime_host=env_text("EASY_PROXY_RUNTIME_HOST", default_runtime_host) or default_runtime_host,
            unique_attempts=max(1, env_int("REGISTER_PROXY_UNIQUE_ATTEMPTS", 3)),
            recent_window_seconds=max(0, env_int("REGISTER_PROXY_RECENT_WINDOW_SECONDS", 180)),
            failure_window_seconds=max(0, env_int("REGISTER_PROXY_FAILURE_WINDOW_SECONDS", 300)),
        )


@dataclass(frozen=True)
class MailboxRuntimeConfig:
    ttl_seconds: int
    providers: tuple[str, ...]
    strategy_mode_id: str
    routing_profile_id: str
    domain_state_path: Path
    business_domain_pool: tuple[str, ...]
    blacklist_min_attempts: int
    blacklist_failure_rate_percent: float

    @classmethod
    def from_env(
        cls,
        *,
        default_ttl_seconds: int,
        default_state_path: Path,
        default_business_domain_pool: tuple[str, ...],
        default_blacklist_min_attempts: int,
        default_blacklist_failure_rate: float,
    ) -> "MailboxRuntimeConfig":
        providers = tuple(item.lower() for item in split_csv(env_first_text("REGISTER_MAILBOX_PROVIDERS", "MAILBOX_PROVIDER_CANDIDATES")))
        explicit_state_path = env_text("REGISTER_MAILBOX_DOMAIN_STATE_PATH")
        domain_state_path = Path(explicit_state_path).expanduser().resolve() if explicit_state_path else default_state_path
        business_domain_pool = tuple(item.lower() for item in split_csv(env_text("REGISTER_MAILBOX_DOMAIN_POOL"))) or default_business_domain_pool
        return cls(
            ttl_seconds=max(1, int(float(env_text("REGISTER_MAILBOX_TTL_SECONDS", str(default_ttl_seconds)) or default_ttl_seconds))),
            providers=providers,
            strategy_mode_id=env_first_text("REGISTER_MAILBOX_STRATEGY_MODE_ID", "MAILBOX_PROVIDER_STRATEGY_MODE_ID"),
            routing_profile_id=env_first_text(
                "REGISTER_MAILBOX_ROUTING_PROFILE_ID",
                "MAILBOX_PROVIDER_ROUTING_PROFILE_ID",
                default="high-availability",
            ),
            domain_state_path=domain_state_path,
            business_domain_pool=business_domain_pool,
            blacklist_min_attempts=max(1, env_int("REGISTER_MAILBOX_DOMAIN_BLACKLIST_MIN_ATTEMPTS", default_blacklist_min_attempts)),
            blacklist_failure_rate_percent=env_percent_value(
                "REGISTER_MAILBOX_DOMAIN_BLACKLIST_FAILURE_RATE",
                default_blacklist_failure_rate,
            ),
        )


@dataclass(frozen=True)
class TeamAuthRuntimeConfig:
    auth_glob: str
    mother_pool_dir: Path
    auth_paths: tuple[str, ...]
    auth_path: str
    auth_dirs: tuple[str, ...]
    auth_local_dir: str
    auth_default_dir: str
    local_dir: str
    sall_cc_weight: float
    zero_success_window_seconds: float
    zero_success_min_attempts: int
    team_expand_window_seconds: float
    team_expand_failure_weight_step: float
    team_expand_floor_weight: float
    team_expand_success_credit: float
    total_seat_limit: int
    chatgpt_seat_limit: int
    codex_seat_limit: int
    reservation_ttl_seconds: float
    state_lock_timeout_seconds: float
    team_member_count: int
    codex_seat_types: tuple[str, ...]
    oauth_failure_cooldown_seconds: float
    invite_failure_cooldown_seconds: float
    capacity_cooldown_seconds: float
    temp_blacklist_seconds: float

    @classmethod
    def from_env(cls, *, output_root: Path | None = None, shared_root: Path | None = None) -> "TeamAuthRuntimeConfig":
        resolved_output_root = output_root.resolve() if output_root is not None else _resolve_output_root()
        resolved_shared_root = shared_root.resolve() if shared_root is not None else resolve_shared_root(str(resolved_output_root))
        total_seat_limit = max(1, env_int("REGISTER_TEAM_TOTAL_SEAT_LIMIT", 9))
        return cls(
            auth_glob=env_text("REGISTER_TEAM_AUTH_GLOB", "*-team.json") or "*-team.json",
            mother_pool_dir=Path(resolve_team_mother_pool_dir(str(resolved_shared_root))).resolve(),
            auth_paths=split_path_list(env_text("REGISTER_TEAM_AUTH_PATHS")),
            auth_path=env_text("REGISTER_TEAM_AUTH_PATH"),
            auth_dirs=split_path_list(env_text("REGISTER_TEAM_AUTH_DIRS")),
            auth_local_dir=env_first_text("REGISTER_TEAM_AUTH_LOCAL_DIR", "REGISTER_TEAM_LOCAL_DIR"),
            auth_default_dir=env_first_text("REGISTER_TEAM_AUTH_DEFAULT_DIR", "REGISTER_TEAM_AUTH_DIR"),
            local_dir=env_text("REGISTER_TEAM_LOCAL_DIR"),
            sall_cc_weight=max(0.0, min(1.0, env_percent_value("REGISTER_TEAM_AUTH_SALL_CC_WEIGHT", 5.0) / 100.0)),
            zero_success_window_seconds=max(0.0, env_float("REGISTER_TEAM_AUTH_ZERO_SUCCESS_WINDOW_SECONDS", 1800.0)),
            zero_success_min_attempts=max(0, env_int("REGISTER_TEAM_AUTH_ZERO_SUCCESS_MIN_ATTEMPTS", 10)),
            team_expand_window_seconds=max(0.0, env_float("REGISTER_TEAM_AUTH_TEAM_EXPAND_WINDOW_SECONDS", 21600.0)),
            team_expand_failure_weight_step=max(0.0, min(1.0, env_float("REGISTER_TEAM_AUTH_TEAM_EXPAND_FAILURE_WEIGHT_STEP", 0.25))),
            team_expand_floor_weight=max(0.05, min(1.0, env_float("REGISTER_TEAM_AUTH_TEAM_EXPAND_FLOOR_WEIGHT", 0.2))),
            team_expand_success_credit=max(0.0, env_float("REGISTER_TEAM_AUTH_TEAM_EXPAND_SUCCESS_CREDIT", 0.5)),
            total_seat_limit=total_seat_limit,
            chatgpt_seat_limit=max(0, min(total_seat_limit, env_int("REGISTER_TEAM_CHATGPT_SEAT_LIMIT", 4))),
            codex_seat_limit=max(0, min(total_seat_limit, env_int("REGISTER_TEAM_CODEX_SEAT_LIMIT", total_seat_limit))),
            reservation_ttl_seconds=max(30.0, env_float("REGISTER_TEAM_AUTH_RESERVATION_TTL_SECONDS", 300.0)),
            state_lock_timeout_seconds=max(1.0, env_float("REGISTER_TEAM_AUTH_STATE_LOCK_TIMEOUT_SECONDS", 5.0)),
            team_member_count=max(1, env_int("REGISTER_TEAM_MEMBER_COUNT", 4)),
            codex_seat_types=tuple(item.lower() for item in split_csv(env_text("REGISTER_TEAM_CODEX_SEAT_TYPES", "usage_based,codex"))),
            oauth_failure_cooldown_seconds=max(0.0, env_float("REGISTER_TEAM_OAUTH_FAILURE_COOLDOWN_SECONDS", 300.0)),
            invite_failure_cooldown_seconds=max(0.0, env_float("REGISTER_TEAM_INVITE_FAILURE_COOLDOWN_SECONDS", 300.0)),
            capacity_cooldown_seconds=max(0.0, env_float("REGISTER_TEAM_CAPACITY_COOLDOWN_SECONDS", 180.0)),
            temp_blacklist_seconds=max(0.0, env_float("REGISTER_TEAM_AUTH_TEMP_BLACKLIST_SECONDS", 3600.0)),
        )


@dataclass(frozen=True)
class ArtifactRoutingConfig:
    small_success_pool_dir: Path
    small_success_wait_pool_dir: Path
    small_success_continue_pool_dir: Path
    free_oauth_pool_dir: Path
    free_manual_oauth_pool_dir: Path
    free_local_dir: Path
    team_local_dir: Path
    free_local_split_percent: float
    team_local_split_percent: float
    small_success_wait_seconds: float
    small_success_continue_prefill_count: int
    small_success_continue_prefill_target_count: int
    small_success_continue_prefill_min_age_seconds: float
    r2_bucket: str
    r2_account_id: str
    r2_endpoint_url: str
    r2_access_key_id: str
    r2_secret_access_key: str
    r2_region: str
    r2_public_base_url: str

    @classmethod
    def from_env(cls, *, output_root: Path | None = None) -> "ArtifactRoutingConfig":
        resolved_output_root = output_root.resolve() if output_root is not None else _resolve_output_root()
        shared_root = resolve_shared_root(str(resolved_output_root))
        return cls(
            small_success_pool_dir=Path(
                env_text("REGISTER_SMALL_SUCCESS_POOL_DIR") or resolve_small_success_pool_dir(str(resolved_output_root))
            ).expanduser().resolve(),
            small_success_wait_pool_dir=Path(
                env_text("REGISTER_SMALL_SUCCESS_WAIT_POOL_DIR") or shared_root / "others" / "small-success-wait-pool"
            ).expanduser().resolve(),
            small_success_continue_pool_dir=Path(
                env_text("REGISTER_SMALL_SUCCESS_CONTINUE_POOL_DIR") or shared_root / "others" / "small-success-continue-pool"
            ).expanduser().resolve(),
            free_oauth_pool_dir=Path(
                env_text("REGISTER_FREE_OAUTH_POOL_DIR") or resolve_free_oauth_pool_dir(str(resolved_output_root))
            ).expanduser().resolve(),
            free_manual_oauth_pool_dir=Path(
                env_text("REGISTER_FREE_MANUAL_OAUTH_POOL_DIR") or shared_root / "others" / "free-manual-oauth-pool"
            ).expanduser().resolve(),
            free_local_dir=Path(
                env_text("REGISTER_FREE_LOCAL_DIR") or shared_root / "others" / "free-local-store"
            ).expanduser().resolve(),
            team_local_dir=Path(
                env_text("REGISTER_TEAM_LOCAL_DIR") or shared_root / "others" / "team-local-store"
            ).expanduser().resolve(),
            free_local_split_percent=env_percent_value("REGISTER_FREE_LOCAL_SPLIT_PERCENT", 100.0),
            team_local_split_percent=env_percent_value("REGISTER_TEAM_LOCAL_SPLIT_PERCENT", 0.0),
            small_success_wait_seconds=max(0.0, env_float("REGISTER_SMALL_SUCCESS_WAIT_SECONDS", 600.0)),
            small_success_continue_prefill_count=max(0, env_int("REGISTER_SMALL_SUCCESS_CONTINUE_PREFILL_COUNT", 1)),
            small_success_continue_prefill_target_count=max(0, env_int("REGISTER_SMALL_SUCCESS_CONTINUE_PREFILL_TARGET_COUNT", 2)),
            small_success_continue_prefill_min_age_seconds=max(
                0.0,
                env_float("REGISTER_SMALL_SUCCESS_CONTINUE_PREFILL_MIN_AGE_SECONDS", 0.0),
            ),
            r2_bucket=env_first_text("REGISTER_R2_BUCKET", "R2_BUCKET"),
            r2_account_id=env_first_text("REGISTER_R2_ACCOUNT_ID", "R2_ACCOUNT_ID"),
            r2_endpoint_url=env_first_text("REGISTER_R2_ENDPOINT_URL", "R2_ENDPOINT_URL"),
            r2_access_key_id=env_first_text("REGISTER_R2_ACCESS_KEY_ID", "R2_ACCESS_KEY_ID"),
            r2_secret_access_key=env_first_text("REGISTER_R2_SECRET_ACCESS_KEY", "R2_SECRET_ACCESS_KEY"),
            r2_region=env_first_text("REGISTER_R2_REGION", "R2_REGION"),
            r2_public_base_url=env_first_text("REGISTER_R2_PUBLIC_BASE_URL", "R2_PUBLIC_BASE_URL"),
        )


@dataclass(frozen=True)
class CleanupRuntimeConfig:
    team_cleanup_lock_stale_seconds: float
    mailbox_cleanup_lock_stale_seconds: float
    mailbox_cleanup_cooldown_seconds: float
    mailbox_cleanup_max_delete_count: int
    mailbox_cleanup_failure_threshold: int
    team_cleanup_cooldown_seconds: float
    create_account_cooldown_seconds: float
    mailbox_failure_cooldown_seconds: float
    crash_cooldown_seconds: float

    @classmethod
    def from_env(cls) -> "CleanupRuntimeConfig":
        return cls(
            team_cleanup_lock_stale_seconds=max(0.0, env_float("REGISTER_TEAM_CLEANUP_LOCK_STALE_SECONDS", 600.0)),
            mailbox_cleanup_lock_stale_seconds=max(0.0, env_float("REGISTER_MAILBOX_CLEANUP_LOCK_STALE_SECONDS", 600.0)),
            mailbox_cleanup_cooldown_seconds=max(0.0, env_float("REGISTER_MAILBOX_CLEANUP_COOLDOWN_SECONDS", 120.0)),
            mailbox_cleanup_max_delete_count=max(1, env_int("REGISTER_MAILBOX_CLEANUP_MAX_DELETE_COUNT", 30)),
            mailbox_cleanup_failure_threshold=max(1, env_int("REGISTER_MAILBOX_CLEANUP_FAILURE_THRESHOLD", 3)),
            team_cleanup_cooldown_seconds=max(0.0, env_float("REGISTER_TEAM_CLEANUP_COOLDOWN_SECONDS", 180.0)),
            create_account_cooldown_seconds=max(0.0, env_float("REGISTER_CREATE_ACCOUNT_COOLDOWN_SECONDS", 60.0)),
            mailbox_failure_cooldown_seconds=max(0.0, env_float("REGISTER_MAILBOX_FAILURE_COOLDOWN_SECONDS", 15.0)),
            crash_cooldown_seconds=max(0.0, env_float("REGISTER_CRASH_COOLDOWN_SECONDS", 20.0)),
        )
