from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


SRC_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from others import runtime_proxy_acquire  # noqa: E402


class RuntimeProxyAcquireTests(unittest.TestCase):
    def setUp(self) -> None:
        self._output_root = tempfile.TemporaryDirectory()
        self.addCleanup(self._output_root.cleanup)
        self._env_patch = mock.patch.dict(
            "os.environ",
            {"REGISTER_OUTPUT_ROOT": self._output_root.name},
            clear=False,
        )
        self._env_patch.start()
        self.addCleanup(self._env_patch.stop)
        runtime_proxy_acquire._COMPAT_CHECKOUT_COOLDOWN_UNTIL.clear()

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
                    probe_url="https://chatgpt.com/auth/login",
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

    def test_initial_probe_pending_aborts_compat_retries_and_falls_back_once(self) -> None:
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
                    side_effect=RuntimeError("EasyProxy API POST /proxy/leases/checkout returned 503: {\"error\":\"INITIAL_PROXY_PROBE_PENDING\"}"),
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
                mock.patch.object(runtime_proxy_acquire, "release_lease") as release_lease_mock, \
                mock.patch.object(runtime_proxy_acquire, "report_usage") as report_usage_mock:
                lease = runtime_proxy_acquire.acquire_flow_proxy_lease(
                    flow_name="codex_openai_account_task",
                    probe_url="https://chatgpt.com/auth/login",
                )

            self.assertEqual("http://easy-proxy:25039", lease.proxy_url)
            self.assertEqual("random-node", lease.acquisition_mode)
            self.assertEqual(1, checkout_proxy_mock.call_count)
            self.assertEqual(1, checkout_random_mock.call_count)
            release_lease_mock.assert_not_called()
            report_usage_mock.assert_not_called()
        finally:
            with runtime_proxy_acquire._ACTIVE_FLOW_PROXY_LOCK:
                runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS.update(original_active)
                runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS.update(original_recent)
                runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS.update(original_failed)

    def test_compat_checkout_timeout_enters_short_cooldown_before_random_fallback(self) -> None:
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

        try:
            with tempfile.TemporaryDirectory() as tmp_dir, \
                mock.patch.dict(
                    "os.environ",
                    {
                        "REGISTER_OUTPUT_ROOT": tmp_dir,
                        "REGISTER_PROXY_LEASE_FAILURE_COOLDOWN_SECONDS": "60",
                    },
                    clear=False,
                ), \
                mock.patch.object(runtime_proxy_acquire, "_proxy_runtime_config", return_value=config), \
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
                    side_effect=RuntimeError("timed out"),
                ) as checkout_proxy_mock, \
                mock.patch.object(
                    runtime_proxy_acquire,
                    "checkout_random_node_proxy",
                    side_effect=[
                        {
                            "proxyUrl": "http://easy-proxy:25039",
                            "metadata": {
                                "selectedNodeTag": "tag-25039",
                                "selectedNodePort": "25039",
                            },
                        },
                        {
                            "proxyUrl": "http://easy-proxy:25041",
                            "metadata": {
                                "selectedNodeTag": "tag-25041",
                                "selectedNodePort": "25041",
                            },
                        },
                    ],
                ) as checkout_random_mock, \
                mock.patch.object(runtime_proxy_acquire, "release_lease") as release_lease_mock, \
                mock.patch.object(runtime_proxy_acquire, "report_usage") as report_usage_mock:
                first = runtime_proxy_acquire.acquire_flow_proxy_lease(
                    flow_name="codex_openai_account_task",
                    probe_url="https://chatgpt.com/auth/login",
                )
                runtime_proxy_acquire._COMPAT_CHECKOUT_COOLDOWN_UNTIL.clear()
                second = runtime_proxy_acquire.acquire_flow_proxy_lease(
                    flow_name="codex_openai_account_task",
                    probe_url="https://chatgpt.com/auth/login",
                )

            self.assertEqual("random-node", first.acquisition_mode)
            self.assertEqual("random-node", second.acquisition_mode)
            self.assertEqual(1, checkout_proxy_mock.call_count)
            self.assertEqual(2, checkout_random_mock.call_count)
            release_lease_mock.assert_not_called()
            report_usage_mock.assert_not_called()
        finally:
            with runtime_proxy_acquire._ACTIVE_FLOW_PROXY_LOCK:
                runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS.update(original_active)
                runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS.update(original_recent)
                runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS.update(original_failed)

    def test_random_node_route_failure_is_shared_between_worker_processes(self) -> None:
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

        random_calls: list[set[str]] = []
        random_candidates = [
            {
                "proxyUrl": "http://easy-proxy:25001",
                "metadata": {"selectedNodeTag": "bad", "selectedNodePort": "25001"},
            },
            {
                "proxyUrl": "http://easy-proxy:25002",
                "metadata": {"selectedNodeTag": "good-1", "selectedNodePort": "25002"},
            },
            {
                "proxyUrl": "http://easy-proxy:25003",
                "metadata": {"selectedNodeTag": "good-2", "selectedNodePort": "25003"},
            },
        ]

        def _random_candidate(**kwargs):
            random_calls.append(set(kwargs.get("excluded_proxy_urls") or set()))
            return random_candidates[len(random_calls) - 1]

        try:
            with mock.patch.object(runtime_proxy_acquire, "_proxy_runtime_config", return_value=config), \
                mock.patch.object(runtime_proxy_acquire, "ensure_easy_proxy_env_defaults"), \
                mock.patch.object(runtime_proxy_acquire, "_resolve_easy_proxy_mode", return_value="random-node"), \
                mock.patch.object(runtime_proxy_acquire, "_resolve_easy_proxy_unique_attempts", return_value=3), \
                mock.patch.object(runtime_proxy_acquire, "_default_easy_proxy_service_key", return_value="service-key"), \
                mock.patch.object(runtime_proxy_acquire, "_default_easy_proxy_stage", return_value="registration"), \
                mock.patch.object(runtime_proxy_acquire, "runtime_reachable_proxy_url", side_effect=lambda value: value), \
                mock.patch.object(
                    runtime_proxy_acquire,
                    "_probe_flow_proxy",
                    side_effect=[
                        RuntimeError("easy_proxy_probe_failed status=403 target=https://chatgpt.com/auth/login"),
                        None,
                        None,
                    ],
                ), \
                mock.patch.object(
                    runtime_proxy_acquire,
                    "checkout_random_node_proxy",
                    side_effect=_random_candidate,
                ):
                first = runtime_proxy_acquire.acquire_flow_proxy_lease(
                    flow_name="codex_openai_account_task",
                    probe_url="https://chatgpt.com/auth/login",
                )
                with runtime_proxy_acquire._ACTIVE_FLOW_PROXY_LOCK:
                    runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS.clear()
                    runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS.clear()
                    runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS.clear()
                second = runtime_proxy_acquire.acquire_flow_proxy_lease(
                    flow_name="codex_openai_account_task",
                    probe_url="https://chatgpt.com/auth/login",
                )

            self.assertEqual("http://easy-proxy:25002", first.proxy_url)
            self.assertEqual("http://easy-proxy:25003", second.proxy_url)
            self.assertIn("http://easy-proxy:25001", random_calls[1])
            self.assertIn("http://easy-proxy:25001", random_calls[2])
        finally:
            with runtime_proxy_acquire._ACTIVE_FLOW_PROXY_LOCK:
                runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS.update(original_active)
                runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS.update(original_recent)
                runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS.update(original_failed)

    def test_route_failure_aborts_compat_retries_and_falls_back_once(self) -> None:
        reported_lease_ids: list[str] = []
        released_lease_ids: list[str] = []
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

        try:
            with mock.patch.object(runtime_proxy_acquire, "_proxy_runtime_config", return_value=config), \
                mock.patch.object(runtime_proxy_acquire, "ensure_easy_proxy_env_defaults"), \
                mock.patch.object(runtime_proxy_acquire, "_resolve_easy_proxy_mode", return_value="auto"), \
                mock.patch.object(runtime_proxy_acquire, "_resolve_easy_proxy_unique_attempts", return_value=3), \
                mock.patch.object(runtime_proxy_acquire, "_default_easy_proxy_service_key", return_value="service-key"), \
                mock.patch.object(runtime_proxy_acquire, "_default_easy_proxy_stage", return_value="registration"), \
                mock.patch.object(runtime_proxy_acquire, "_build_easy_proxy_host_id", return_value="host-1"), \
                mock.patch.object(runtime_proxy_acquire, "runtime_reachable_proxy_url", side_effect=lambda value: value), \
                mock.patch.object(
                    runtime_proxy_acquire,
                    "checkout_proxy",
                    return_value={
                        "id": "lease-route-fail",
                        "proxyUrl": "http://easy-proxy:25023",
                    },
                ) as checkout_proxy_mock, \
                mock.patch.object(
                    runtime_proxy_acquire,
                    "_probe_flow_proxy",
                    side_effect=[
                        RuntimeError("easy_proxy_probe_failed status=403 target=https://chatgpt.com/auth/login"),
                        None,
                    ],
                ), \
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
                    probe_url="https://chatgpt.com/auth/login",
                )

            self.assertEqual("http://easy-proxy:25039", lease.proxy_url)
            self.assertEqual("random-node", lease.acquisition_mode)
            self.assertEqual(1, checkout_proxy_mock.call_count)
            self.assertEqual(1, checkout_random_mock.call_count)
            self.assertEqual(["lease-route-fail"], reported_lease_ids)
            self.assertEqual(["lease-route-fail"], released_lease_ids)
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
