from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


SRC_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from others import runtime_proxy_acquire  # noqa: E402


class RuntimeProxyAcquireTests(unittest.TestCase):
    def test_duplicate_active_route_releases_lease_without_reporting_failure_and_falls_back(self) -> None:
        released_lease_ids: list[str] = []
        reported_lease_ids: list[str] = []

        config = SimpleNamespace(
            enabled=True,
            required_by_default=True,
            management_base_url="http://easy-proxy:29888",
            api_key="",
            ttl_minutes=30,
        )

        with runtime_proxy_acquire._ACTIVE_FLOW_PROXY_LOCK:
            original_active = set(runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS)
            original_recent = dict(runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS)
            original_failed = dict(runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS)
            runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS.clear()
            runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS.clear()
            runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS.clear()
            runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS.add("http://easy-proxy:25023")

        try:
            with mock.patch.object(runtime_proxy_acquire, "_proxy_runtime_config", return_value=config), \
                mock.patch.object(runtime_proxy_acquire, "ensure_easy_proxy_env_defaults"), \
                mock.patch.object(runtime_proxy_acquire, "_resolve_easy_proxy_mode", return_value="auto"), \
                mock.patch.object(runtime_proxy_acquire, "_resolve_easy_proxy_unique_attempts", return_value=3), \
                mock.patch.object(runtime_proxy_acquire, "_default_easy_proxy_service_key", return_value="service-key"), \
                mock.patch.object(runtime_proxy_acquire, "_default_easy_proxy_stage", return_value="registration"), \
                mock.patch.object(runtime_proxy_acquire, "_build_easy_proxy_host_id", return_value="host-1"), \
                mock.patch.object(runtime_proxy_acquire, "runtime_reachable_proxy_url", side_effect=lambda value: value), \
                mock.patch.object(runtime_proxy_acquire, "_probe_flow_proxy"), \
                mock.patch.object(
                    runtime_proxy_acquire,
                    "checkout_proxy",
                    return_value={
                        "id": "lease-1",
                        "proxyUrl": "http://easy-proxy:25023",
                    },
                ) as checkout_proxy_mock, \
                mock.patch.object(
                    runtime_proxy_acquire,
                    "checkout_random_node_proxy",
                    return_value={
                        "proxyUrl": "http://easy-proxy:25039",
                        "metadata": {
                            "selectedNodeTag": "tag-25039",
                            "selectedNodePort": "25039",
                        },
                    },
                ) as checkout_random_mock, \
                mock.patch.object(
                    runtime_proxy_acquire,
                    "release_lease",
                    side_effect=lambda lease_id, **_: released_lease_ids.append(str(lease_id)),
                ), \
                mock.patch.object(
                    runtime_proxy_acquire,
                    "report_usage",
                    side_effect=lambda lease_id, **_: reported_lease_ids.append(str(lease_id)),
                ):
                lease = runtime_proxy_acquire.acquire_flow_proxy_lease(
                    flow_name="codex_openai_account_task",
                    probe_url="https://platform.openai.com/login",
                )

            self.assertEqual("http://easy-proxy:25039", lease.proxy_url)
            self.assertEqual("random-node", lease.acquisition_mode)
            self.assertEqual(1, checkout_proxy_mock.call_count)
            self.assertEqual(1, checkout_random_mock.call_count)
            self.assertEqual(["lease-1"], released_lease_ids)
            self.assertEqual([], reported_lease_ids)
        finally:
            with runtime_proxy_acquire._ACTIVE_FLOW_PROXY_LOCK:
                runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS.update(original_active)
                runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS.update(original_recent)
                runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS.update(original_failed)


if __name__ == "__main__":
    unittest.main()
