from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from others.config_env import env_bool
from others.config_env import env_first_text
from others.config_env import env_float
from others.config_env import env_int
from others.config_env import env_percent_value
from others.config_env import env_text
from others.config_env import resolve_output_root
from others.config_env import resolve_shared_root_from_env
from others.config_env import split_csv
from others.config_env import split_path_list
from others.paths import (
    resolve_free_oauth_pool_dir,
    resolve_plus_oauth_pool_dir,
    resolve_shared_root,
    resolve_openai_oauth_pool_dir,
    resolve_openai_oauth_success_pool_dir,
    resolve_team_input_dir,
    resolve_team_mother_pool_dir,
    resolve_team_pool_dir,
)

DEFAULT_DST_LOGIN_ENTRY_URL = "https://auth.openai.com/log-in-or-create-account"


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
    mailbox_ttl_seconds: str
    mailbox_recreate_preallocated: bool
    free_stop_after_validate: bool
    input_source_dir: str
    input_claims_dir: str
    login_entry_url: str

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
            mailbox_ttl_seconds=env_text("REGISTER_MAILBOX_TTL_SECONDS"),
            mailbox_recreate_preallocated=env_bool("REGISTER_MAILBOX_RECREATE_PREALLOCATED", False),
            free_stop_after_validate=env_bool("REGISTER_FREE_STOP_AFTER_VALIDATE", False),
            input_source_dir=env_text("REGISTER_INPUT_SOURCE_DIR"),
            input_claims_dir=env_text("REGISTER_INPUT_CLAIMS_DIR"),
            login_entry_url=env_text("REGISTER_DST_LOGIN_ENTRY_URL", DEFAULT_DST_LOGIN_ENTRY_URL) or DEFAULT_DST_LOGIN_ENTRY_URL,
        )


@dataclass(frozen=True)
class RunnerFlowSpec:
    name: str
    flow_path: str
    instance_role: str
    weight: float
    team_auth_path: str
    task_max_attempts: int
    openai_oauth_pool_dir: Path
    mailbox_business_key: str
    input_source_dir: str
    input_claims_dir: str
    concurrency_limit: int = 0


def _normalize_instance_role(value: Any, *, default: str) -> str:
    normalized = str(value or "").strip().lower()
    return normalized or str(default or "main").strip().lower() or "main"


def _default_openai_oauth_pool_dir_for_role(
    *,
    output_root: Path,
    shared_root: Path,
    instance_role: str,
) -> Path:
    normalized_role = _normalize_instance_role(instance_role, default="main")
    if normalized_role == "continue":
        return (shared_root / "openai" / "failed-once").resolve()
    return Path(resolve_openai_oauth_pool_dir(str(output_root))).resolve()


def _default_flow_concurrency_limit_for_role(instance_role: str) -> int:
    normalized_role = _normalize_instance_role(instance_role, default="main")
    if normalized_role == "main":
        return max(0, env_int("REGISTER_MAIN_CONCURRENCY_LIMIT", 5))
    if normalized_role == "continue":
        return max(0, env_int("REGISTER_CONTINUE_CONCURRENCY_LIMIT", 2))
    if normalized_role == "team":
        return max(0, env_int("REGISTER_TEAM_CONCURRENCY_LIMIT", 1))
    return max(0, env_int("REGISTER_DEFAULT_FLOW_CONCURRENCY_LIMIT", 0))


def _split_top_level_parts(text: str, delimiter: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    depth = 0
    quote_char = ""
    escaped = False
    for char in str(text or ""):
        if quote_char:
            current.append(char)
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote_char:
                quote_char = ""
            continue
        if char in {"'", '"'}:
            quote_char = char
            current.append(char)
            continue
        if char in {"{", "["}:
            depth += 1
            current.append(char)
            continue
        if char in {"}", "]"}:
            depth = max(0, depth - 1)
            current.append(char)
            continue
        if char == delimiter and depth == 0:
            parts.append("".join(current).strip())
            current = []
            continue
        current.append(char)
    if current:
        parts.append("".join(current).strip())
    return [part for part in parts if part]


def _find_top_level_colon(text: str) -> int:
    depth = 0
    quote_char = ""
    escaped = False
    for index, char in enumerate(str(text or "")):
        if quote_char:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote_char:
                quote_char = ""
            continue
        if char in {"'", '"'}:
            quote_char = char
            continue
        if char in {"{", "["}:
            depth += 1
            continue
        if char in {"}", "]"}:
            depth = max(0, depth - 1)
            continue
        if char == ":" and depth == 0:
            return index
    return -1


def _strip_optional_quotes(value: str) -> str:
    text = str(value or "").strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        return text[1:-1]
    return text


def _parse_relaxed_scalar(value: str) -> Any:
    text = str(value or "").strip()
    if not text:
        return ""
    if text[0] in {"'", '"'} and text[-1] == text[0]:
        return _strip_optional_quotes(text)
    lowered = text.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered == "null":
        return None
    try:
        if any(marker in text for marker in (".", "e", "E")):
            return float(text)
        return int(text)
    except Exception:
        return text


def _parse_relaxed_flow_spec_items(text: str) -> list[dict[str, Any]]:
    raw = str(text or "").strip()
    if not raw.startswith("[") or not raw.endswith("]"):
        return []
    inner = raw[1:-1].strip()
    if not inner:
        return []
    items: list[dict[str, Any]] = []
    for candidate in _split_top_level_parts(inner, ","):
        body = candidate.strip()
        if body.startswith("{") and body.endswith("}"):
            body = body[1:-1].strip()
        if not body:
            continue
        parsed: dict[str, Any] = {}
        for pair in _split_top_level_parts(body, ","):
            split_index = _find_top_level_colon(pair)
            if split_index <= 0:
                continue
            raw_key = pair[:split_index].strip()
            raw_value = pair[split_index + 1 :].strip()
            key = _strip_optional_quotes(raw_key)
            if not key:
                continue
            parsed[key] = _parse_relaxed_scalar(raw_value)
        if parsed:
            items.append(parsed)
    return items


def _parse_runner_flow_specs(
    raw: str,
    *,
    output_root: Path,
    shared_root: Path,
    default_instance_role: str,
    default_team_auth_path: str,
    default_input_source_dir: str,
    default_input_claims_dir: str,
    default_task_max_attempts: int,
) -> tuple[RunnerFlowSpec, ...]:
    text = str(raw or "").strip()
    if not text:
        return ()
    try:
        payload = json.loads(text)
    except Exception:
        payload = _parse_relaxed_flow_spec_items(text)
    items = payload.get("flows") if isinstance(payload, dict) and isinstance(payload.get("flows"), list) else payload
    if not isinstance(items, list):
        return ()

    specs: list[RunnerFlowSpec] = []
    for index, item in enumerate(items, start=1):
        if isinstance(item, str):
            item = {"path": item}
        if not isinstance(item, dict):
            continue
        flow_path = str(
            item.get("flowPath")
            or item.get("flow_path")
            or item.get("path")
            or ""
        ).strip()
        instance_role = _normalize_instance_role(
            item.get("instanceRole") or item.get("instance_role") or item.get("role"),
            default=default_instance_role,
        )
        try:
            weight = max(0.0, float(item.get("weight") or 1.0))
        except Exception:
            weight = 1.0
        try:
            task_max_attempts = max(0, int(item.get("taskMaxAttempts") or item.get("task_max_attempts") or default_task_max_attempts))
        except Exception:
            task_max_attempts = max(0, int(default_task_max_attempts or 0))
        try:
            concurrency_limit = max(
                0,
                int(
                    item.get("concurrencyLimit")
                    or item.get("concurrency_limit")
                    or item.get("maxConcurrentTasks")
                    or _default_flow_concurrency_limit_for_role(instance_role)
                ),
            )
        except Exception:
            concurrency_limit = _default_flow_concurrency_limit_for_role(instance_role)
        openai_oauth_pool_dir_text = str(
            item.get("openaiOauthPoolDir")
            or item.get("openai_oauth_pool_dir")
            or item.get("smallSuccessPoolDir")
            or item.get("small_success_pool_dir")
            or ""
        ).strip()
        openai_oauth_pool_dir = (
            Path(openai_oauth_pool_dir_text).expanduser().resolve()
            if openai_oauth_pool_dir_text
            else _default_openai_oauth_pool_dir_for_role(
                output_root=output_root,
                shared_root=shared_root,
                instance_role=instance_role,
            )
        )
        name = str(item.get("name") or item.get("id") or "").strip()
        if not name:
            if flow_path:
                name = Path(flow_path).stem
            else:
                name = f"flow-{index}"
        specs.append(
            RunnerFlowSpec(
                name=name,
                flow_path=flow_path,
                instance_role=instance_role,
                weight=weight,
                team_auth_path=str(
                    item.get("teamAuthPath")
                    or item.get("team_auth_path")
                    or default_team_auth_path
                    or ""
                ).strip(),
                task_max_attempts=task_max_attempts,
                openai_oauth_pool_dir=openai_oauth_pool_dir,
                mailbox_business_key=str(
                    item.get("mailboxBusinessKey")
                    or item.get("mailbox_business_key")
                    or ""
                ).strip().lower(),
                input_source_dir=str(
                    item.get("inputSourceDir")
                    or item.get("input_source_dir")
                    or default_input_source_dir
                    or ""
                ).strip(),
                input_claims_dir=str(
                    item.get("inputClaimsDir")
                    or item.get("input_claims_dir")
                    or default_input_claims_dir
                    or ""
                ).strip(),
                concurrency_limit=concurrency_limit,
            )
        )
    return tuple(spec for spec in specs if spec.weight > 0.0)


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
    flow_specs: tuple[RunnerFlowSpec, ...]
    openai_oauth_pool_dir: Path
    free_oauth_pool_dir: Path
    instance_id: str
    instance_role: str
    easy_protocol_base_url: str
    easy_protocol_control_token: str
    easy_protocol_control_actor: str

    @classmethod
    def from_env(cls) -> "RunnerMainConfig":
        output_root = resolve_output_root()
        shared_root = resolve_shared_root_from_env()
        instance_id = env_text("REGISTER_INSTANCE_ID", "main") or "main"
        instance_role = env_text("REGISTER_INSTANCE_ROLE", instance_id) or instance_id
        team_auth_path = env_text("REGISTER_TEAM_AUTH_PATH")
        input_source_dir = env_text("REGISTER_INPUT_SOURCE_DIR")
        input_claims_dir = env_text("REGISTER_INPUT_CLAIMS_DIR")
        flow_path = env_text("REGISTER_FLOW_PATH")
        task_max_attempts = env_int("REGISTER_TASK_MAX_ATTEMPTS", 0)
        flow_specs = _parse_runner_flow_specs(
            env_text("REGISTER_FLOW_SPECS_JSON"),
            output_root=output_root,
            shared_root=shared_root,
            default_instance_role=instance_role,
            default_team_auth_path=team_auth_path,
            default_input_source_dir=input_source_dir,
            default_input_claims_dir=input_claims_dir,
            default_task_max_attempts=task_max_attempts,
        )
        if not flow_specs:
            flow_specs = (
                RunnerFlowSpec(
                    name=str(instance_role or instance_id or "main").strip().lower() or "main",
                    flow_path=str(flow_path or "").strip(),
                    instance_role=_normalize_instance_role(instance_role, default=instance_id),
                    weight=1.0,
                    team_auth_path=str(team_auth_path or "").strip(),
                    task_max_attempts=max(0, int(task_max_attempts or 0)),
                    openai_oauth_pool_dir=_default_openai_oauth_pool_dir_for_role(
                        output_root=output_root,
                        shared_root=shared_root,
                        instance_role=instance_role,
                    ),
                    mailbox_business_key="",
                    input_source_dir=str(input_source_dir or "").strip(),
                    input_claims_dir=str(input_claims_dir or "").strip(),
                    concurrency_limit=_default_flow_concurrency_limit_for_role(instance_role),
                ),
            )
        effective_flow_path = str(flow_path or "").strip() or str(flow_specs[0].flow_path or "").strip()
        effective_team_auth_path = str(team_auth_path or "").strip() or str(flow_specs[0].team_auth_path or "").strip()
        return cls(
            output_root=output_root,
            shared_root=shared_root,
            delay_seconds=max(0.0, env_float("REGISTER_LOOP_DELAY_SECONDS", 5.0)),
            worker_count=max(1, env_int("REGISTER_WORKER_COUNT", 10)),
            worker_stagger_seconds=max(0.0, env_float("REGISTER_WORKER_STAGGER_SECONDS", 2.0)),
            max_runs=max(0, env_int("REGISTER_INFINITE_MAX_RUNS", 0)),
            task_max_attempts=task_max_attempts,
            team_auth_path=effective_team_auth_path,
            flow_path=effective_flow_path,
            flow_specs=flow_specs,
            openai_oauth_pool_dir=Path(resolve_openai_oauth_pool_dir(str(output_root))).resolve(),
            free_oauth_pool_dir=Path(resolve_free_oauth_pool_dir(str(output_root))).resolve(),
            instance_id=instance_id,
            instance_role=instance_role,
            easy_protocol_base_url=env_text("EASY_PROTOCOL_BASE_URL", "http://easy-protocol:9788"),
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
class MailboxBusinessPolicy:
    business_key: str
    domain_pool: tuple[str, ...]
    explicit_blacklist_domains: tuple[str, ...]
    explicit_blacklist_providers: tuple[str, ...]


_MAILBOX_DEFAULT_POLICY_KEYS = ("default", "*", "__default__")


def _normalize_mailbox_business_key(value: Any) -> str:
    return str(value or "").strip().lower()


def _split_mailbox_domains(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        text = str(value or "").strip()
        if text.startswith("[") and text.endswith("]"):
            inner = text[1:-1].strip()
            if not inner:
                return ()
            return tuple(
                _strip_optional_quotes(item).strip().lower()
                for item in _split_top_level_parts(inner, ",")
                if _strip_optional_quotes(item).strip()
            )
        return tuple(item.lower() for item in split_csv(text))
    if isinstance(value, (list, tuple, set)):
        normalized: list[str] = []
        for item in value:
            text = str(item or "").strip().lower()
            if text:
                normalized.append(text)
        return tuple(normalized)
    return ()


def _split_mailbox_providers(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        text = str(value or "").strip()
        if text.startswith("[") and text.endswith("]"):
            inner = text[1:-1].strip()
            if not inner:
                return ()
            return tuple(
                _normalize_mailbox_provider_key(_strip_optional_quotes(item))
                for item in _split_top_level_parts(inner, ",")
                if _normalize_mailbox_provider_key(_strip_optional_quotes(item))
            )
        return tuple(
            normalized
            for normalized in (_normalize_mailbox_provider_key(item) for item in split_csv(text))
            if normalized
        )
    if isinstance(value, (list, tuple, set)):
        normalized: list[str] = []
        for item in value:
            provider_key = _normalize_mailbox_provider_key(item)
            if provider_key:
                normalized.append(provider_key)
        return tuple(normalized)
    return ()


def _normalize_mailbox_provider_key(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    alias_map = {
        "mail-to-you": "m2u",
        "mailtoyou": "m2u",
        "cloudflare-temp-email": "cloudflare_temp_email",
        "cloudflaretempemail": "cloudflare_temp_email",
        "tempmail.lol": "tempmail-lol",
        "tempmaillol": "tempmail-lol",
    }
    return alias_map.get(normalized, normalized)


def _parse_relaxed_mailbox_business_policy_map(text: str) -> dict[str, dict[str, Any]]:
    raw = str(text or "").strip()
    if not raw.startswith("{") or not raw.endswith("}"):
        return {}
    inner = raw[1:-1].strip()
    if not inner:
        return {}

    payload: dict[str, dict[str, Any]] = {}
    for pair in _split_top_level_parts(inner, ","):
        split_index = _find_top_level_colon(pair)
        if split_index <= 0:
            continue
        business_key = _strip_optional_quotes(pair[:split_index]).strip()
        raw_policy = pair[split_index + 1 :].strip()
        if not business_key or not raw_policy.startswith("{") or not raw_policy.endswith("}"):
            continue
        policy_body = raw_policy[1:-1].strip()
        parsed_policy: dict[str, Any] = {}
        for policy_pair in _split_top_level_parts(policy_body, ","):
            policy_split_index = _find_top_level_colon(policy_pair)
            if policy_split_index <= 0:
                continue
            key = _strip_optional_quotes(policy_pair[:policy_split_index]).strip()
            value_text = policy_pair[policy_split_index + 1 :].strip()
            if not key:
                continue
            if value_text.startswith("[") and value_text.endswith("]"):
                parsed_policy[key] = _split_mailbox_domains(value_text)
            else:
                parsed_policy[key] = _parse_relaxed_scalar(value_text)
        if parsed_policy:
            payload[business_key] = parsed_policy
    return payload


def _parse_mailbox_business_policies(raw: str) -> tuple[MailboxBusinessPolicy, ...]:
    text = str(raw or "").strip()
    if not text:
        return ()
    try:
        payload = json.loads(text)
    except Exception:
        payload = _parse_relaxed_mailbox_business_policy_map(text)
    if not isinstance(payload, dict):
        return ()
    policies: list[MailboxBusinessPolicy] = []
    for raw_business_key, raw_policy in payload.items():
        business_key = _normalize_mailbox_business_key(raw_business_key)
        if not business_key or not isinstance(raw_policy, dict):
            continue
        domain_pool = _split_mailbox_domains(
            raw_policy.get("domainPool")
            or raw_policy.get("domain_pool")
            or raw_policy.get("domains")
        )
        explicit_blacklist_domains = _split_mailbox_domains(
            raw_policy.get("explicitBlacklistDomains")
            or raw_policy.get("explicit_blacklist_domains")
            or raw_policy.get("domainBlacklist")
            or raw_policy.get("domain_blacklist")
            or raw_policy.get("blacklist")
        )
        explicit_blacklist_providers = _split_mailbox_providers(
            raw_policy.get("providerBlacklist")
            or raw_policy.get("provider_blacklist")
            or raw_policy.get("explicitBlacklistProviders")
            or raw_policy.get("explicit_blacklist_providers")
            or raw_policy.get("blacklistProviders")
            or raw_policy.get("blacklist_providers")
        )
        policies.append(
            MailboxBusinessPolicy(
                business_key=business_key,
                domain_pool=domain_pool,
                explicit_blacklist_domains=explicit_blacklist_domains,
                explicit_blacklist_providers=explicit_blacklist_providers,
            )
        )
    return tuple(policies)


@dataclass(frozen=True)
class MailboxRuntimeConfig:
    ttl_seconds: int
    providers: tuple[str, ...]
    strategy_mode_id: str
    routing_profile_id: str
    business_key: str
    domain_state_path: Path
    business_domain_pool: tuple[str, ...]
    explicit_blacklist_domains: tuple[str, ...]
    explicit_blacklist_providers: tuple[str, ...]
    business_policies: tuple[MailboxBusinessPolicy, ...]
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
        explicit_blacklist_domains = tuple(item.lower() for item in split_csv(env_text("REGISTER_MAILBOX_DOMAIN_BLACKLIST")))
        explicit_blacklist_providers = _split_mailbox_providers(env_text("REGISTER_MAILBOX_PROVIDER_BLACKLIST"))
        business_policies = _parse_mailbox_business_policies(env_text("REGISTER_MAILBOX_BUSINESS_POLICIES_JSON"))
        return cls(
            ttl_seconds=max(1, int(float(env_text("REGISTER_MAILBOX_TTL_SECONDS", str(default_ttl_seconds)) or default_ttl_seconds))),
            providers=providers,
            strategy_mode_id=env_first_text("REGISTER_MAILBOX_STRATEGY_MODE_ID", "MAILBOX_PROVIDER_STRATEGY_MODE_ID"),
            routing_profile_id=env_first_text(
                "REGISTER_MAILBOX_ROUTING_PROFILE_ID",
                "MAILBOX_PROVIDER_ROUTING_PROFILE_ID",
                default="high-availability",
            ),
            business_key=_normalize_mailbox_business_key(env_text("REGISTER_MAILBOX_BUSINESS_KEY", "openai")) or "openai",
            domain_state_path=domain_state_path,
            business_domain_pool=business_domain_pool,
            explicit_blacklist_domains=explicit_blacklist_domains,
            explicit_blacklist_providers=explicit_blacklist_providers,
            business_policies=business_policies,
            blacklist_min_attempts=max(1, env_int("REGISTER_MAILBOX_DOMAIN_BLACKLIST_MIN_ATTEMPTS", default_blacklist_min_attempts)),
            blacklist_failure_rate_percent=env_percent_value(
                "REGISTER_MAILBOX_DOMAIN_BLACKLIST_FAILURE_RATE",
                default_blacklist_failure_rate,
            ),
        )

    def resolve_business_key(self, business_key: str | None = None) -> str:
        normalized = _normalize_mailbox_business_key(business_key)
        if normalized:
            return normalized
        fallback = _normalize_mailbox_business_key(self.business_key)
        return fallback or "default"

    def resolve_business_policy(self, business_key: str | None = None) -> MailboxBusinessPolicy:
        resolved_business_key = self.resolve_business_key(business_key)
        for policy in self.business_policies:
            if policy.business_key == resolved_business_key:
                return policy
        for policy in self.business_policies:
            if policy.business_key in _MAILBOX_DEFAULT_POLICY_KEYS:
                return MailboxBusinessPolicy(
                    business_key=resolved_business_key,
                    domain_pool=policy.domain_pool,
                    explicit_blacklist_domains=policy.explicit_blacklist_domains,
                    explicit_blacklist_providers=policy.explicit_blacklist_providers,
                )
        return MailboxBusinessPolicy(
            business_key=resolved_business_key,
            domain_pool=self.business_domain_pool,
            explicit_blacklist_domains=self.explicit_blacklist_domains,
            explicit_blacklist_providers=self.explicit_blacklist_providers,
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
    stale_claim_seconds: int
    codex_seat_types: tuple[str, ...]
    oauth_failure_cooldown_seconds: float
    invite_failure_cooldown_seconds: float
    capacity_cooldown_seconds: float
    temp_blacklist_seconds: float

    @classmethod
    def from_env(cls, *, output_root: Path | None = None, shared_root: Path | None = None) -> "TeamAuthRuntimeConfig":
        resolved_output_root = output_root.resolve() if output_root is not None else resolve_output_root()
        resolved_shared_root = shared_root.resolve() if shared_root is not None else resolve_shared_root(str(resolved_output_root))
        total_seat_limit = max(1, env_int("REGISTER_TEAM_TOTAL_SEAT_LIMIT", 9))
        return cls(
            auth_glob=env_text("REGISTER_TEAM_AUTH_GLOB", "*-team.json") or "*-team.json",
            mother_pool_dir=Path(resolve_team_mother_pool_dir(str(resolved_shared_root))).resolve(),
            auth_paths=split_path_list(env_text("REGISTER_TEAM_AUTH_PATHS")),
            auth_path=env_text("REGISTER_TEAM_AUTH_PATH"),
            auth_dirs=split_path_list(env_text("REGISTER_TEAM_AUTH_DIRS")),
            auth_local_dir=env_text("REGISTER_TEAM_AUTH_LOCAL_DIR") or str(resolve_team_input_dir(str(resolved_shared_root))),
            auth_default_dir=env_text("REGISTER_TEAM_AUTH_DEFAULT_DIR") or env_text("REGISTER_TEAM_AUTH_DIR") or str(resolve_team_input_dir(str(resolved_shared_root))),
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
            stale_claim_seconds=max(0, env_int("REGISTER_TEAM_STALE_CLAIM_SECONDS", 60)),
            codex_seat_types=tuple(item.lower() for item in split_csv(env_text("REGISTER_TEAM_CODEX_SEAT_TYPES", "usage_based,codex"))),
            oauth_failure_cooldown_seconds=max(0.0, env_float("REGISTER_TEAM_OAUTH_FAILURE_COOLDOWN_SECONDS", 300.0)),
            invite_failure_cooldown_seconds=max(0.0, env_float("REGISTER_TEAM_INVITE_FAILURE_COOLDOWN_SECONDS", 300.0)),
            capacity_cooldown_seconds=max(0.0, env_float("REGISTER_TEAM_CAPACITY_COOLDOWN_SECONDS", 180.0)),
            temp_blacklist_seconds=max(0.0, env_float("REGISTER_TEAM_AUTH_TEMP_BLACKLIST_SECONDS", 3600.0)),
        )


@dataclass(frozen=True)
class ArtifactRoutingConfig:
    openai_oauth_pool_dir: Path
    openai_oauth_success_pool_dir: Path
    openai_oauth_wait_pool_dir: Path
    openai_oauth_continue_pool_dir: Path
    openai_oauth_need_phone_pool_dir: Path
    free_oauth_pool_dir: Path
    free_manual_oauth_pool_dir: Path
    free_local_dir: Path
    team_local_dir: Path
    plus_local_dir: Path
    free_local_split_percent: float
    team_local_split_percent: float
    openai_upload_percent: float
    codex_free_upload_percent: float
    codex_team_upload_percent: float
    codex_plus_upload_percent: float
    openai_oauth_wait_seconds: float
    openai_oauth_continue_prefill_count: int
    openai_oauth_continue_prefill_target_count: int
    openai_oauth_continue_prefill_min_age_seconds: float
    r2_bucket: str
    r2_account_id: str
    r2_endpoint_url: str
    r2_access_key_id: str
    r2_secret_access_key: str
    r2_region: str
    r2_public_base_url: str

    @classmethod
    def from_env(cls, *, output_root: Path | None = None) -> "ArtifactRoutingConfig":
        resolved_output_root = output_root.resolve() if output_root is not None else resolve_output_root()
        shared_root = resolve_shared_root(str(resolved_output_root))
        return cls(
            openai_oauth_pool_dir=Path(
                env_first_text("REGISTER_OPENAI_OAUTH_POOL_DIR", "REGISTER_SMALL_SUCCESS_POOL_DIR")
                or resolve_openai_oauth_pool_dir(str(resolved_output_root))
            ).expanduser().resolve(),
            openai_oauth_success_pool_dir=Path(
                env_text("REGISTER_OPENAI_OAUTH_SUCCESS_POOL_DIR")
                or resolve_openai_oauth_success_pool_dir(str(resolved_output_root))
            ).expanduser().resolve(),
            openai_oauth_wait_pool_dir=Path(
                env_first_text("REGISTER_OPENAI_OAUTH_WAIT_POOL_DIR", "REGISTER_SMALL_SUCCESS_WAIT_POOL_DIR")
                or shared_root / "others" / "openai-oauth-wait-pool"
            ).expanduser().resolve(),
            openai_oauth_continue_pool_dir=Path(
                env_first_text("REGISTER_OPENAI_OAUTH_CONTINUE_POOL_DIR", "REGISTER_SMALL_SUCCESS_CONTINUE_POOL_DIR")
                or shared_root / "openai" / "failed-once"
            ).expanduser().resolve(),
            openai_oauth_need_phone_pool_dir=Path(
                env_text("REGISTER_OPENAI_OAUTH_NEED_PHONE_POOL_DIR") or shared_root / "openai" / "failed-twice"
            ).expanduser().resolve(),
            free_oauth_pool_dir=Path(
                env_text("REGISTER_FREE_OAUTH_POOL_DIR") or resolve_free_oauth_pool_dir(str(resolved_output_root))
            ).expanduser().resolve(),
            free_manual_oauth_pool_dir=Path(
                env_text("REGISTER_FREE_MANUAL_OAUTH_POOL_DIR") or shared_root / "others" / "free-manual-oauth-pool"
            ).expanduser().resolve(),
            free_local_dir=Path(
                env_text("REGISTER_FREE_LOCAL_DIR") or resolve_free_oauth_pool_dir(str(resolved_output_root))
            ).expanduser().resolve(),
            team_local_dir=Path(
                env_text("REGISTER_TEAM_LOCAL_DIR") or resolve_team_pool_dir(str(resolved_output_root))
            ).expanduser().resolve(),
            plus_local_dir=Path(
                env_text("REGISTER_PLUS_LOCAL_DIR") or resolve_plus_oauth_pool_dir(str(resolved_output_root))
            ).expanduser().resolve(),
            free_local_split_percent=env_percent_value("REGISTER_FREE_LOCAL_SPLIT_PERCENT", 100.0),
            team_local_split_percent=env_percent_value("REGISTER_TEAM_LOCAL_SPLIT_PERCENT", 0.0),
            openai_upload_percent=env_percent_value("REGISTER_OPENAI_UPLOAD_PERCENT", 0.0),
            codex_free_upload_percent=env_percent_value(
                "REGISTER_CODEX_FREE_UPLOAD_PERCENT",
                max(0.0, 100.0 - env_percent_value("REGISTER_FREE_LOCAL_SPLIT_PERCENT", 100.0)),
            ),
            codex_team_upload_percent=env_percent_value(
                "REGISTER_CODEX_TEAM_UPLOAD_PERCENT",
                max(0.0, 100.0 - env_percent_value("REGISTER_TEAM_LOCAL_SPLIT_PERCENT", 0.0)),
            ),
            codex_plus_upload_percent=env_percent_value("REGISTER_CODEX_PLUS_UPLOAD_PERCENT", 0.0),
            openai_oauth_wait_seconds=max(
                0.0,
                env_float(
                    "REGISTER_OPENAI_OAUTH_WAIT_SECONDS",
                    env_float("REGISTER_SMALL_SUCCESS_WAIT_SECONDS", 600.0),
                ),
            ),
            openai_oauth_continue_prefill_count=max(
                0,
                env_int(
                    "REGISTER_OPENAI_OAUTH_CONTINUE_PREFILL_COUNT",
                    env_int("REGISTER_SMALL_SUCCESS_CONTINUE_PREFILL_COUNT", 1),
                ),
            ),
            openai_oauth_continue_prefill_target_count=max(
                0,
                env_int(
                    "REGISTER_OPENAI_OAUTH_CONTINUE_PREFILL_TARGET_COUNT",
                    env_int("REGISTER_SMALL_SUCCESS_CONTINUE_PREFILL_TARGET_COUNT", 2),
                ),
            ),
            openai_oauth_continue_prefill_min_age_seconds=max(
                0.0,
                env_float(
                    "REGISTER_OPENAI_OAUTH_CONTINUE_PREFILL_MIN_AGE_SECONDS",
                    env_float("REGISTER_SMALL_SUCCESS_CONTINUE_PREFILL_MIN_AGE_SECONDS", 0.0),
                ),
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
