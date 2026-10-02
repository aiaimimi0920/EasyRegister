from __future__ import annotations

from others.runtime_mailbox import (
    resolve_mailbox,
    resolve_mailbox_provider_selections,
    resolve_mailbox_routing_profile_id,
    resolve_mailbox_strategy_mode_id,
)
from others.runtime_proxy import (
    FlowProxyLease,
    acquire_flow_proxy_lease,
    ensure_easy_proxy_env_defaults,
    flow_network_env,
    lease_flow_proxy,
    release_flow_proxy_lease,
    resolve_easy_proxy_runtime_host,
    runtime_reachable_proxy_url,
    seed_device_cookie,
    without_proxy_env,
)


def ensure_easy_email_env_defaults() -> None:
    from others.runtime_mailbox import ensure_easy_email_env_defaults as _ensure

    _ensure()
