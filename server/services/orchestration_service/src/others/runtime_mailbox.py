from __future__ import annotations

import json
import os
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from errors import ensure_protocol_runtime_error
from others.bootstrap import ensure_local_bundle_imports
from others.common import json_log
from others.config import MailboxRuntimeConfig, env_bool, env_int, env_text
from others.local_config import read_easyemail_server_api_key
from others.paths import resolve_shared_root as _shared_root_from_output_root

ensure_local_bundle_imports()

from shared_mailbox.easy_email_client import Mailbox, create_mailbox, plan_mailbox, release_mailbox


DEFAULT_ORCHESTRATION_HOST_ID = "python-register-orchestration"
DEFAULT_EASY_EMAIL_BASE_URL = "http://localhost:18080"
DEFAULT_MAILBOX_TTL_SECONDS = 1800
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
DEFAULT_REGISTER_MAILBOX_DOMAIN_CONSECUTIVE_FAILURE_BLACKLIST_THRESHOLD = 500
DEFAULT_MAILBOX_BUSINESS_RETRY_ATTEMPTS = 12
DEFAULT_MAILBOX_DYNAMIC_BLACKLIST_TTL_SECONDS = 6 * 60 * 60
MAILBOX_DOMAIN_STATS_SCHEMA_VERSION = 3
EMAIL_OTP_FAILURE_REASONS = {"email_otp_timeout", "email_otp_wrong_code"}
_MAILBOX_DEFAULT_POLICY_KEYS = {"default", "*", "__default__"}


def _mailbox_runtime_config() -> MailboxRuntimeConfig:
    output_root_text = env_text("REGISTER_OUTPUT_ROOT")
    if output_root_text:
        default_state_path = _shared_root_from_output_root(Path(output_root_text).expanduser()) / "others" / "register-mailbox-domain-state.json"
    else:
        default_state_path = Path.cwd().resolve() / "others" / "register-mailbox-domain-state.json"
    return MailboxRuntimeConfig.from_env(
        default_ttl_seconds=DEFAULT_MAILBOX_TTL_SECONDS,
        default_state_path=default_state_path,
        default_business_domain_pool=DEFAULT_REGISTER_MOEMAIL_DOMAIN_POOL,
        default_blacklist_min_attempts=DEFAULT_REGISTER_MAILBOX_DOMAIN_BLACKLIST_MIN_ATTEMPTS,
        default_blacklist_failure_rate=DEFAULT_REGISTER_MAILBOX_DOMAIN_BLACKLIST_FAILURE_RATE,
        default_consecutive_failure_blacklist_threshold=DEFAULT_REGISTER_MAILBOX_DOMAIN_CONSECUTIVE_FAILURE_BLACKLIST_THRESHOLD,
    )


def ensure_easy_email_env_defaults() -> None:
    base_url = str(os.environ.get("MAILBOX_SERVICE_BASE_URL") or "").strip()
    if not base_url:
        os.environ["MAILBOX_SERVICE_BASE_URL"] = DEFAULT_EASY_EMAIL_BASE_URL
    api_key = str(os.environ.get("MAILBOX_SERVICE_API_KEY") or "").strip()
    if not api_key:
        discovered_api_key = read_easyemail_server_api_key()
        if discovered_api_key:
            os.environ["MAILBOX_SERVICE_API_KEY"] = discovered_api_key


def resolve_mailbox_provider_selections() -> tuple[str, ...]:
    providers = _mailbox_runtime_config().providers
    if not providers:
        return ()
    return tuple(
        normalized
        for normalized in (_normalize_mailbox_provider(item) for item in providers)
        if normalized
    )


def resolve_mailbox_strategy_mode_id() -> str:
    return _mailbox_runtime_config().strategy_mode_id


def resolve_mailbox_routing_profile_id() -> str:
    return _mailbox_runtime_config().routing_profile_id


def _resolve_mailbox_ttl_seconds() -> int:
    return _mailbox_runtime_config().ttl_seconds


def _normalize_mailbox_provider(provider: str) -> str:
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


def _provider_from_mailbox_ref(mailbox_ref: str) -> str:
    value = str(mailbox_ref or "").strip()
    if not value:
        return ""
    if ":" not in value:
        return "moemail"
    provider = value.split(":", 1)[0]
    return _normalize_mailbox_provider(provider)


def _normalize_requested_email_address(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    if not normalized or "@" not in normalized:
        return ""
    local_part, _, domain = normalized.partition("@")
    local_part = local_part.strip()
    domain = domain.strip().lower()
    if not local_part or not domain:
        return ""
    return f"{local_part}@{domain}"


def _resolve_mailbox_domain_state_path() -> Path:
    return _mailbox_runtime_config().domain_state_path


def _load_mailbox_domain_state() -> dict[str, Any]:
    state_path = _resolve_mailbox_domain_state_path()
    if not state_path.is_file():
        return {}
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    try:
        schema_version = int(payload.get("schemaVersion") or 0)
    except Exception:
        schema_version = 0
    if schema_version != MAILBOX_DOMAIN_STATS_SCHEMA_VERSION:
        json_log(
            {
                "event": "register_mailbox_domain_state_ignored",
                "reason": "legacy_schema_version",
                "statePath": str(state_path),
                "schemaVersion": schema_version,
                "expectedSchemaVersion": MAILBOX_DOMAIN_STATS_SCHEMA_VERSION,
            }
        )
        return {}
    return payload


def resolve_mailbox_business_key(*, business_key: str | None = None) -> str:
    return _mailbox_runtime_config().resolve_business_key(business_key)


def _resolve_mailbox_explicit_blacklist_domains(*, business_key: str | None = None) -> tuple[str, ...]:
    return _mailbox_runtime_config().resolve_business_policy(business_key).explicit_blacklist_domains


def _resolve_mailbox_explicit_blacklist_providers(*, business_key: str | None = None) -> tuple[str, ...]:
    return _mailbox_runtime_config().resolve_business_policy(business_key).explicit_blacklist_providers


def _resolve_business_mailbox_domain_pool(*, business_key: str | None = None) -> tuple[str, ...]:
    config = _mailbox_runtime_config()
    resolved_business_key = config.resolve_business_key(business_key)
    for policy in config.business_policies:
        if policy.business_key == resolved_business_key and policy.domain_pool:
            return policy.domain_pool
    for policy in config.business_policies:
        if policy.business_key in _MAILBOX_DEFAULT_POLICY_KEYS and policy.domain_pool:
            return policy.domain_pool
    if env_text("REGISTER_MAILBOX_DOMAIN_POOL"):
        return config.resolve_business_policy(resolved_business_key).domain_pool
    return ()


def _resolve_mailbox_domain_blacklist_min_attempts() -> int:
    return _mailbox_runtime_config().blacklist_min_attempts


def _resolve_mailbox_domain_blacklist_failure_rate() -> float:
    return _mailbox_runtime_config().blacklist_failure_rate_percent


def _resolve_mailbox_domain_consecutive_failure_blacklist_threshold() -> int:
    return _mailbox_runtime_config().consecutive_failure_blacklist_threshold


def _resolve_mailbox_email_otp_failure_blacklist_threshold() -> int:
    return max(0, env_int("REGISTER_MAILBOX_EMAIL_OTP_FAILURE_BLACKLIST_THRESHOLD", 3))


def _resolve_mailbox_email_otp_provider_failure_blacklist_threshold() -> int:
    return max(0, env_int("REGISTER_MAILBOX_EMAIL_OTP_PROVIDER_FAILURE_BLACKLIST_THRESHOLD", 3))


def _resolve_mailbox_dynamic_blacklist_ttl_seconds() -> int:
    return max(0, env_int("REGISTER_MAILBOX_DYNAMIC_BLACKLIST_TTL_SECONDS", DEFAULT_MAILBOX_DYNAMIC_BLACKLIST_TTL_SECONDS))


def _parse_mailbox_state_timestamp(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _mailbox_dynamic_blacklist_expired(stats: dict[str, Any]) -> bool:
    ttl_seconds = _resolve_mailbox_dynamic_blacklist_ttl_seconds()
    if ttl_seconds <= 0:
        return False
    last_failure = (
        _parse_mailbox_state_timestamp(stats.get("lastFailureAt"))
        or _parse_mailbox_state_timestamp(stats.get("lastOutcomeAt"))
    )
    if last_failure is None:
        return False
    return (datetime.now(timezone.utc) - last_failure).total_seconds() >= ttl_seconds


def _mailbox_failure_reason_total(failure_reasons: Any, reasons: set[str]) -> int:
    if not isinstance(failure_reasons, dict):
        return 0
    total = 0
    for reason in reasons:
        try:
            total += max(0, int(failure_reasons.get(reason) or 0))
        except Exception:
            continue
    return total


def _mailbox_domain_stats(domain: str, state_payload: dict[str, Any], *, business_key: str | None = None) -> dict[str, Any]:
    resolved_business_key = resolve_mailbox_business_key(business_key=business_key)
    businesses = state_payload.get("businesses")
    if isinstance(businesses, dict):
        business_payload = businesses.get(resolved_business_key)
        if isinstance(business_payload, dict):
            domains = business_payload.get("domains")
            if isinstance(domains, dict):
                stats = domains.get(domain)
                if isinstance(stats, dict):
                    return stats
    domains = state_payload.get("domains")
    if not isinstance(domains, dict):
        return {}
    stats = domains.get(domain)
    return stats if isinstance(stats, dict) else {}


def _mailbox_provider_stats(provider: str, state_payload: dict[str, Any], *, business_key: str | None = None) -> dict[str, Any]:
    normalized_provider = _normalize_mailbox_provider(provider)
    if not normalized_provider:
        return {}
    resolved_business_key = resolve_mailbox_business_key(business_key=business_key)
    businesses = state_payload.get("businesses")
    if isinstance(businesses, dict):
        business_payload = businesses.get(resolved_business_key)
        if isinstance(business_payload, dict):
            providers = business_payload.get("providers")
            if isinstance(providers, dict):
                stats = providers.get(normalized_provider)
                if isinstance(stats, dict):
                    return stats
    providers = state_payload.get("providers")
    if not isinstance(providers, dict):
        return {}
    stats = providers.get(normalized_provider)
    return stats if isinstance(stats, dict) else {}


def _mailbox_domain_is_business_blacklisted(domain: str, state_payload: dict[str, Any], *, business_key: str | None = None) -> bool:
    if domain in set(_resolve_mailbox_explicit_blacklist_domains(business_key=business_key)):
        return True
    stats = _mailbox_domain_stats(domain, state_payload, business_key=business_key)
    if stats and _mailbox_dynamic_blacklist_expired(stats):
        return False
    if bool(stats.get("blacklisted")):
        return True
    threshold = _resolve_mailbox_email_otp_failure_blacklist_threshold()
    return (
        threshold > 0
        and _mailbox_failure_reason_total(stats.get("failureReasons"), EMAIL_OTP_FAILURE_REASONS) >= threshold
    )


def _mailbox_provider_is_business_blacklisted(provider: str, state_payload: dict[str, Any], *, business_key: str | None = None) -> bool:
    normalized_provider = _normalize_mailbox_provider(provider)
    if not normalized_provider:
        return False
    if normalized_provider in set(_resolve_mailbox_explicit_blacklist_providers(business_key=business_key)):
        return True
    stats = _mailbox_provider_stats(normalized_provider, state_payload, business_key=business_key)
    if stats and _mailbox_dynamic_blacklist_expired(stats):
        return False
    if bool(stats.get("blacklisted")):
        return True
    threshold = _resolve_mailbox_email_otp_provider_failure_blacklist_threshold()
    return (
        threshold > 0
        and _mailbox_failure_reason_total(stats.get("failureReasons"), EMAIL_OTP_FAILURE_REASONS) >= threshold
    )


def _select_business_mailbox_domain(*, business_key: str | None = None) -> tuple[str, str]:
    domain_pool = _resolve_business_mailbox_domain_pool(business_key=business_key)
    if not domain_pool:
        return "", "not_configured"
    explicit_blacklist = set(_resolve_mailbox_explicit_blacklist_domains(business_key=business_key))
    candidates = tuple(domain for domain in domain_pool if domain and domain not in explicit_blacklist)
    if not candidates:
        return "", "all_explicitly_blacklisted"
    state_payload = _load_mailbox_domain_state()
    eligible = tuple(
        domain
        for domain in candidates
        if not _mailbox_domain_is_business_blacklisted(
            domain,
            state_payload,
            business_key=business_key,
        )
    )
    if eligible:
        return random.choice(eligible), "eligible"
    return random.choice(candidates), "dynamic_blacklist_exhausted"


def _resolve_mailbox_business_retry_attempts() -> int:
    return max(1, env_int("REGISTER_MAILBOX_BUSINESS_RETRY_ATTEMPTS", DEFAULT_MAILBOX_BUSINESS_RETRY_ATTEMPTS))


def _dynamic_blacklist_exhausted_fallback_enabled() -> bool:
    return env_bool("REGISTER_MAILBOX_DYNAMIC_BLACKLIST_EXHAUSTED_FALLBACK", False)


def _mailbox_domain_from_email(email: str) -> str:
    normalized = str(email or "").strip().lower()
    if "@" not in normalized:
        return ""
    return normalized.rsplit("@", 1)[-1].strip().lower()


def _normalize_mailbox_avoid_values(value: Any, *, kind: str) -> tuple[str, ...]:
    raw_items: list[Any]
    if isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        text = str(value or "").strip()
        raw_items = text.split(",") if text else []
    normalized: list[str] = []
    for item in raw_items:
        text = str(item or "").strip().lower()
        if not text:
            continue
        if kind == "provider":
            text = _normalize_mailbox_provider(text)
        elif kind == "email":
            text = _normalize_requested_email_address(text)
        elif kind == "domain":
            text = text.strip().lower()
        if text and text not in normalized:
            normalized.append(text)
    return tuple(normalized)


def _mailbox_attempt_local_avoidance_violation(
    mailbox: Mailbox,
    *,
    business_key: str | None = None,
    avoid_emails: Any = None,
    avoid_domains: Any = None,
    avoid_providers: Any = None,
    avoid_reason: str = "",
) -> dict[str, Any] | None:
    resolved_business_key = resolve_mailbox_business_key(business_key=business_key)
    provider = _normalize_mailbox_provider(str(getattr(mailbox, "provider", "") or ""))
    email = _normalize_requested_email_address(str(getattr(mailbox, "email", "") or ""))
    domain = _mailbox_domain_from_email(email)
    normalized_avoid_emails = set(_normalize_mailbox_avoid_values(avoid_emails, kind="email"))
    normalized_avoid_domains = set(_normalize_mailbox_avoid_values(avoid_domains, kind="domain"))
    normalized_avoid_providers = set(_normalize_mailbox_avoid_values(avoid_providers, kind="provider"))
    reason = ""
    if email and email in normalized_avoid_emails:
        reason = "attempt_local_mailbox_email"
    elif domain and domain in normalized_avoid_domains:
        reason = "attempt_local_mailbox_domain"
    elif provider and provider in normalized_avoid_providers:
        reason = "attempt_local_mailbox_provider"
    if not reason:
        return None
    return {
        "reason": reason,
        "business_key": resolved_business_key,
        "provider": provider,
        "domain": domain,
        "email": email,
        "avoidReason": str(avoid_reason or "").strip(),
    }


def _mailbox_domain_policy_violation(mailbox: Mailbox, *, business_key: str | None = None) -> dict[str, Any] | None:
    resolved_business_key = resolve_mailbox_business_key(business_key=business_key)
    provider = _normalize_mailbox_provider(str(getattr(mailbox, "provider", "") or ""))
    email = str(getattr(mailbox, "email", "") or "").strip().lower()
    domain = _mailbox_domain_from_email(email)
    explicit_provider_blacklist = set(_resolve_mailbox_explicit_blacklist_providers(business_key=resolved_business_key))
    if provider and provider in explicit_provider_blacklist:
        return {
            "reason": "explicit_business_provider_blacklist",
            "business_key": resolved_business_key,
            "provider": provider,
            "domain": domain,
            "email": email,
        }
    if not domain:
        return None

    state_payload = _load_mailbox_domain_state()
    explicit_blacklist = set(_resolve_mailbox_explicit_blacklist_domains(business_key=resolved_business_key))
    if domain in explicit_blacklist:
        return {
            "reason": "explicit_business_blacklist",
            "business_key": resolved_business_key,
            "provider": provider,
            "domain": domain,
            "email": email,
        }

    business_domain_pool = set(_resolve_business_mailbox_domain_pool(business_key=resolved_business_key))
    if business_domain_pool and provider == "moemail" and domain not in business_domain_pool:
        return {
            "reason": "outside_business_domain_pool",
            "business_key": resolved_business_key,
            "provider": provider,
            "domain": domain,
            "email": email,
        }

    pool_domain_is_authoritative_moemail = bool(
        business_domain_pool and provider == "moemail" and domain in business_domain_pool
    )
    if provider and not pool_domain_is_authoritative_moemail and _mailbox_provider_is_business_blacklisted(
        provider,
        state_payload,
        business_key=resolved_business_key,
    ):
        return {
            "reason": "dynamic_business_provider_blacklist",
            "business_key": resolved_business_key,
            "provider": provider,
            "domain": domain,
            "email": email,
        }

    if _mailbox_domain_is_business_blacklisted(
        domain,
        state_payload,
        business_key=resolved_business_key,
    ):
        return {
            "reason": "dynamic_business_blacklist",
            "business_key": resolved_business_key,
            "provider": provider,
            "domain": domain,
            "email": email,
        }
    return None


def _mailbox_policy_violation_is_dynamic(violation: dict[str, Any] | None) -> bool:
    if not isinstance(violation, dict):
        return False
    return str(violation.get("reason") or "").strip() in {
        "dynamic_business_blacklist",
        "dynamic_business_provider_blacklist",
    }


def _release_mailbox_quiet(mailbox: Mailbox, *, reason: str) -> None:
    try:
        release_mailbox(
            mailbox_ref=str(getattr(mailbox, "ref", "") or "").strip() or None,
            session_id=str(getattr(mailbox, "session_id", "") or "").strip() or None,
            reason=reason,
        )
    except Exception:
        pass


def _create_mailbox_with_business_policy(
    *,
    create_fn: Any,
    business_key: str | None = None,
    avoid_emails: Any = None,
    avoid_domains: Any = None,
    avoid_providers: Any = None,
    avoid_reason: str = "",
) -> Mailbox:
    max_attempts = _resolve_mailbox_business_retry_attempts()
    last_violation: dict[str, Any] | None = None
    for attempt_index in range(1, max_attempts + 1):
        mailbox = create_fn()
        violation = _mailbox_attempt_local_avoidance_violation(
            mailbox,
            business_key=business_key,
            avoid_emails=avoid_emails,
            avoid_domains=avoid_domains,
            avoid_providers=avoid_providers,
            avoid_reason=avoid_reason,
        ) or _mailbox_domain_policy_violation(mailbox, business_key=business_key)
        if violation is None:
            return mailbox
        last_violation = violation
        if (
            attempt_index >= max_attempts
            and _mailbox_policy_violation_is_dynamic(violation)
            and _dynamic_blacklist_exhausted_fallback_enabled()
        ):
            json_log(
                {
                    "event": "register_mailbox_business_dynamic_blacklist_exhausted_fallback",
                    "attempt": attempt_index,
                    "maxAttempts": max_attempts,
                    "reason": str(violation.get("reason") or ""),
                    "businessKey": str(violation.get("business_key") or ""),
                    "provider": str(violation.get("provider") or ""),
                    "domain": str(violation.get("domain") or ""),
                    "email": str(violation.get("email") or ""),
                }
            )
            return mailbox
        json_log(
            {
                "event": (
                    "register_mailbox_attempt_local_avoidance_applied"
                    if str(violation.get("reason") or "").startswith("attempt_local_mailbox_")
                    else "register_mailbox_business_domain_rejected"
                ),
                "attempt": attempt_index,
                "maxAttempts": max_attempts,
                "reason": str(violation.get("reason") or ""),
                "avoidReason": str(violation.get("avoidReason") or ""),
                "businessKey": str(violation.get("business_key") or ""),
                "provider": str(violation.get("provider") or ""),
                "domain": str(violation.get("domain") or ""),
                "email": str(violation.get("email") or ""),
            }
        )
        _release_mailbox_quiet(mailbox, reason="business_domain_rejected")
    raise RuntimeError(
        "mailbox_business_policy_retries_exhausted:"
        f"{json.dumps(last_violation or {}, ensure_ascii=False)}"
    )


def _resolve_mailbox_strategy_kwargs() -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    routing_profile_id = resolve_mailbox_routing_profile_id()
    if routing_profile_id:
        kwargs["provider_routing_profile_id"] = routing_profile_id
    strategy_mode_id = resolve_mailbox_strategy_mode_id()
    if strategy_mode_id:
        kwargs["provider_strategy_mode_id"] = strategy_mode_id
    provider_selections = resolve_mailbox_provider_selections()
    if provider_selections:
        kwargs["provider_group_selections"] = provider_selections
    return kwargs


def _resolve_planned_mailbox_provider(*, ttl_seconds: int, strategy_kwargs: dict[str, Any]) -> str:
    try:
        plan = plan_mailbox(
            provider="auto",
            default_host_id=DEFAULT_ORCHESTRATION_HOST_ID,
            ttl_seconds=ttl_seconds,
            **strategy_kwargs,
        )
    except Exception as exc:
        json_log(
            {
                "event": "register_mailbox_plan_skipped",
                "error": str(exc),
            }
        )
        return ""
    if not isinstance(plan, dict):
        return ""
    instance = plan.get("instance")
    provider_type = plan.get("providerType")
    return _normalize_mailbox_provider(
        str(
            (instance.get("providerTypeKey") if isinstance(instance, dict) else "")
            or (provider_type.get("key") if isinstance(provider_type, dict) else "")
            or ""
        ).strip()
    )


def resolve_mailbox(
    *,
    preallocated_email: str | None,
    preallocated_session_id: str | None,
    preallocated_mailbox_ref: str | None,
    recreate_preallocated_email: bool = False,
    business_key: str | None = None,
    avoid_emails: Any = None,
    avoid_domains: Any = None,
    avoid_providers: Any = None,
    avoid_reason: str = "",
) -> Mailbox:
    ensure_easy_email_env_defaults()
    mailbox_config = _mailbox_runtime_config()
    resolved_business_key = resolve_mailbox_business_key(business_key=business_key)
    normalized_preallocated_email = _normalize_requested_email_address(preallocated_email)
    if normalized_preallocated_email and recreate_preallocated_email:
        ttl_seconds = mailbox_config.ttl_seconds
        requested_local_part, _, requested_domain = normalized_preallocated_email.partition("@")
        preferred_provider = _provider_from_mailbox_ref(preallocated_mailbox_ref or "")
        try:
            return _create_mailbox_with_business_policy(
                create_fn=lambda: create_mailbox(
                    provider=preferred_provider or "auto",
                    default_host_id=DEFAULT_ORCHESTRATION_HOST_ID,
                    prefer_raw_self_hosted_ref=True,
                    ttl_seconds=ttl_seconds,
                    requested_email_address=normalized_preallocated_email,
                    requested_local_part=requested_local_part,
                    mailcreate_domain=requested_domain,
                    **_resolve_mailbox_strategy_kwargs(),
                ),
                business_key=resolved_business_key,
                avoid_emails=avoid_emails,
                avoid_domains=avoid_domains,
                avoid_providers=avoid_providers,
                avoid_reason=avoid_reason,
            )
        except Exception as exc:
            raise ensure_protocol_runtime_error(
                exc,
                stage="stage_other",
                detail="recreate_mailbox",
                category="flow_error",
            ) from exc
    if preallocated_email and preallocated_mailbox_ref:
        ref = str(preallocated_mailbox_ref).strip()
        session_id = str(preallocated_session_id or "").strip()
        if not session_id:
            if ":" in ref:
                session_id = ref.split(":", 1)[1].strip()
            else:
                session_id = ref
        return Mailbox(
            provider=_provider_from_mailbox_ref(ref),
            email=str(preallocated_email).strip(),
            ref=ref,
            session_id=session_id,
        )
    if preallocated_email and preallocated_session_id:
        session_id = str(preallocated_session_id).strip()
        return Mailbox(
            provider="moemail",
            email=str(preallocated_email).strip(),
            ref=f"moemail:{session_id}",
            session_id=session_id,
        )
    ttl_seconds = mailbox_config.ttl_seconds
    strategy_kwargs = _resolve_mailbox_strategy_kwargs()
    planned_provider = _resolve_planned_mailbox_provider(
        ttl_seconds=ttl_seconds,
        strategy_kwargs=strategy_kwargs,
    )
    try:
        if planned_provider:
            json_log(
                {
                    "event": "register_mailbox_provider_planned",
                    "provider": planned_provider,
                    "businessKey": resolved_business_key,
                }
            )
        planned_provider_blocked = False
        if planned_provider and planned_provider != "moemail":
            planned_provider_blocked = _mailbox_provider_is_business_blacklisted(
                planned_provider,
                _load_mailbox_domain_state(),
                business_key=resolved_business_key,
            )
        if planned_provider == "moemail" or planned_provider_blocked:
            selected_domain, domain_selection_reason = _select_business_mailbox_domain(
                business_key=resolved_business_key,
            )
            if planned_provider_blocked:
                domain_selection_reason = f"planned_provider_blacklisted:{domain_selection_reason}"
        else:
            selected_domain = ""
            domain_selection_reason = "planned_provider_not_moemail" if planned_provider else "no_planned_provider"
        if selected_domain:
            json_log(
                {
                    "event": "register_mailbox_business_domain_selected",
                    "businessKey": resolved_business_key,
                    "provider": "moemail",
                    "domain": selected_domain,
                    "reason": domain_selection_reason,
                }
            )
            return _create_mailbox_with_business_policy(
                create_fn=lambda: create_mailbox(
                    provider="moemail",
                    default_host_id=DEFAULT_ORCHESTRATION_HOST_ID,
                    prefer_raw_self_hosted_ref=True,
                    ttl_seconds=ttl_seconds,
                    mailcreate_domain=selected_domain,
                    **strategy_kwargs,
                ),
                business_key=resolved_business_key,
                avoid_emails=avoid_emails,
                avoid_domains=avoid_domains,
                avoid_providers=avoid_providers,
                avoid_reason=avoid_reason,
            )
        if _resolve_business_mailbox_domain_pool(business_key=resolved_business_key):
            json_log(
                {
                    "event": "register_mailbox_business_domain_deferred",
                    "businessKey": resolved_business_key,
                    "provider": planned_provider,
                    "reason": domain_selection_reason,
                }
            )
        return _create_mailbox_with_business_policy(
            create_fn=lambda: create_mailbox(
                provider="auto",
                default_host_id=DEFAULT_ORCHESTRATION_HOST_ID,
                prefer_raw_self_hosted_ref=True,
                ttl_seconds=ttl_seconds,
                **strategy_kwargs,
            ),
            business_key=resolved_business_key,
            avoid_emails=avoid_emails,
            avoid_domains=avoid_domains,
            avoid_providers=avoid_providers,
            avoid_reason=avoid_reason,
        )
    except Exception as exc:
        raise ensure_protocol_runtime_error(
            exc,
            stage="stage_other",
            detail="create_mailbox",
            category="flow_error",
        ) from exc
