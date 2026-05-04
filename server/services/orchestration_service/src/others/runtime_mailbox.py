from __future__ import annotations

import json
import os
import random
from pathlib import Path
from typing import Any

from errors import ensure_protocol_runtime_error
from others.bootstrap import ensure_local_bundle_imports
from others.common import json_log
from others.config import MailboxRuntimeConfig, env_int, env_text
from others.local_config import read_easyemail_server_api_key
from others.paths import resolve_shared_root as _shared_root_from_output_root

ensure_local_bundle_imports()

from shared_mailbox.easy_email_client import Mailbox, create_mailbox, plan_mailbox, release_mailbox


DEFAULT_ORCHESTRATION_HOST_ID = "python-register-orchestration"
DEFAULT_EASY_EMAIL_BASE_URL = "http://localhost:18080"
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
DEFAULT_MAILBOX_BUSINESS_RETRY_ATTEMPTS = 4


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
        return ("m2u", "moemail")
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
    return payload if isinstance(payload, dict) else {}


def _resolve_business_mailbox_domain_pool(*, business_key: str | None = None) -> tuple[str, ...]:
    return _mailbox_runtime_config().resolve_business_policy(business_key).domain_pool


def resolve_mailbox_business_key(*, business_key: str | None = None) -> str:
    return _mailbox_runtime_config().resolve_business_key(business_key)


def _resolve_mailbox_explicit_blacklist_domains(*, business_key: str | None = None) -> tuple[str, ...]:
    return _mailbox_runtime_config().resolve_business_policy(business_key).explicit_blacklist_domains


def _resolve_mailbox_explicit_blacklist_providers(*, business_key: str | None = None) -> tuple[str, ...]:
    return _mailbox_runtime_config().resolve_business_policy(business_key).explicit_blacklist_providers


def _resolve_mailbox_domain_blacklist_min_attempts() -> int:
    return _mailbox_runtime_config().blacklist_min_attempts


def _resolve_mailbox_domain_blacklist_failure_rate() -> float:
    return _mailbox_runtime_config().blacklist_failure_rate_percent


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


def _mailbox_domain_is_business_blacklisted(domain: str, state_payload: dict[str, Any], *, business_key: str | None = None) -> bool:
    if domain in set(_resolve_mailbox_explicit_blacklist_domains(business_key=business_key)):
        return True
    stats = _mailbox_domain_stats(domain, state_payload, business_key=business_key)
    return bool(stats.get("blacklisted"))


def _select_business_mailbox_domain(*, business_key: str | None = None) -> tuple[str, dict[str, Any]]:
    resolved_business_key = resolve_mailbox_business_key(business_key=business_key)
    domain_pool = list(_resolve_business_mailbox_domain_pool(business_key=resolved_business_key))
    if not domain_pool:
        return "", {"reason": "empty_pool", "pool_size": 0, "eligible_count": 0, "dynamic_blacklisted": [], "explicit_blacklisted": []}

    state_payload = _load_mailbox_domain_state()
    explicit_blacklisted = [
        domain
        for domain in domain_pool
        if domain in set(_resolve_mailbox_explicit_blacklist_domains(business_key=resolved_business_key))
    ]
    candidate_pool = [domain for domain in domain_pool if domain not in explicit_blacklisted]
    if not candidate_pool:
        return "", {
            "reason": "empty_pool_after_explicit_blacklist",
            "pool_size": len(domain_pool),
            "eligible_count": 0,
            "dynamic_blacklisted": [],
            "explicit_blacklisted": explicit_blacklisted,
            "business_key": resolved_business_key,
        }
    dynamic_blacklisted = [
        domain
        for domain in candidate_pool
        if _mailbox_domain_is_business_blacklisted(
            domain,
            state_payload,
            business_key=resolved_business_key,
        )
    ]
    eligible = [domain for domain in candidate_pool if domain not in dynamic_blacklisted]
    effective_pool = eligible or candidate_pool
    selected_domain = random.SystemRandom().choice(effective_pool)
    return selected_domain, {
        "reason": "eligible_pool" if eligible else "all_dynamic_blacklisted_fallback",
        "pool_size": len(domain_pool),
        "eligible_count": len(eligible),
        "dynamic_blacklisted": dynamic_blacklisted,
        "explicit_blacklisted": explicit_blacklisted,
        "business_key": resolved_business_key,
    }


def _resolve_mailbox_business_retry_attempts() -> int:
    return max(1, env_int("REGISTER_MAILBOX_BUSINESS_RETRY_ATTEMPTS", DEFAULT_MAILBOX_BUSINESS_RETRY_ATTEMPTS))


def _mailbox_domain_from_email(email: str) -> str:
    normalized = str(email or "").strip().lower()
    if "@" not in normalized:
        return ""
    return normalized.rsplit("@", 1)[-1].strip().lower()


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

    explicit_blacklist = set(_resolve_mailbox_explicit_blacklist_domains(business_key=resolved_business_key))
    if domain in explicit_blacklist:
        return {
            "reason": "explicit_business_blacklist",
            "business_key": resolved_business_key,
            "provider": provider,
            "domain": domain,
            "email": email,
        }

    state_payload = _load_mailbox_domain_state()
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


def _release_mailbox_quiet(mailbox: Mailbox, *, reason: str) -> None:
    try:
        release_mailbox(
            mailbox_ref=str(getattr(mailbox, "ref", "") or "").strip() or None,
            session_id=str(getattr(mailbox, "session_id", "") or "").strip() or None,
            reason=reason,
        )
    except Exception:
        pass


def _create_mailbox_with_business_policy(*, create_fn: Any, business_key: str | None = None) -> Mailbox:
    max_attempts = _resolve_mailbox_business_retry_attempts()
    last_violation: dict[str, Any] | None = None
    for attempt_index in range(1, max_attempts + 1):
        mailbox = create_fn()
        violation = _mailbox_domain_policy_violation(mailbox, business_key=business_key)
        if violation is None:
            return mailbox
        last_violation = violation
        json_log(
            {
                "event": "register_mailbox_business_domain_rejected",
                "attempt": attempt_index,
                "maxAttempts": max_attempts,
                "reason": str(violation.get("reason") or ""),
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
        return _create_mailbox_with_business_policy(
            create_fn=lambda: create_mailbox(
                provider="auto",
                default_host_id=DEFAULT_ORCHESTRATION_HOST_ID,
                prefer_raw_self_hosted_ref=True,
                ttl_seconds=ttl_seconds,
                **strategy_kwargs,
            ),
            business_key=resolved_business_key,
        )
    except Exception as exc:
        raise ensure_protocol_runtime_error(
            exc,
            stage="stage_other",
            detail="create_mailbox",
            category="flow_error",
        ) from exc
