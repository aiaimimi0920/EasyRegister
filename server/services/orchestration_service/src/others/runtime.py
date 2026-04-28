from __future__ import annotations

import os

if __package__ in (None, "", "others"):
    import sys
    from pathlib import Path

    _CURRENT_DIR = Path(__file__).resolve().parent
    _PARENT_DIR = _CURRENT_DIR.parent
    for _candidate in (_CURRENT_DIR, _PARENT_DIR):
        candidate_text = str(_candidate)
        if candidate_text not in sys.path:
            sys.path.append(candidate_text)
    from local_config import read_easyemail_server_api_key
    from runtime_mailbox import (
        DEFAULT_EASY_EMAIL_BASE_URL,
        resolve_mailbox,
        resolve_mailbox_provider_selections,
        resolve_mailbox_routing_profile_id,
        resolve_mailbox_strategy_mode_id,
    )
    from runtime_proxy import (
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
else:
    from .local_config import read_easyemail_server_api_key
    from .runtime_mailbox import (
        DEFAULT_EASY_EMAIL_BASE_URL,
        resolve_mailbox,
        resolve_mailbox_provider_selections,
        resolve_mailbox_routing_profile_id,
        resolve_mailbox_strategy_mode_id,
    )
    from .runtime_proxy import (
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
    base_url = str(os.environ.get("MAILBOX_SERVICE_BASE_URL") or "").strip()
    if not base_url:
        os.environ["MAILBOX_SERVICE_BASE_URL"] = DEFAULT_EASY_EMAIL_BASE_URL
    api_key = str(os.environ.get("MAILBOX_SERVICE_API_KEY") or "").strip()
    if not api_key:
        discovered_api_key = read_easyemail_server_api_key()
        if discovered_api_key:
            os.environ["MAILBOX_SERVICE_API_KEY"] = discovered_api_key
