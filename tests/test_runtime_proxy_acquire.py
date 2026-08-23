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
from shared_proxy.easy_proxy_client import EasyProxyAPIError  # noqa: E402


class RuntimeProxyAcquireTests(unittest.TestCase):
    OPENAI_PROXY_PROBE_URLS = [
        "https://chatgpt.com/auth/login",
        "https://platform.openai.com/login",
        "https://auth.openai.com/log-in-or-create-account",
    ]

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

    def test_random_node_host_id_is_easy_proxy_device_compatible(self) -> None:
        raw = "python-register-orchestration-codex_openai_oauth_continue_task-12345678"

        normalized = runtime_proxy_acquire._normalize_random_node_host_id(raw)

        self.assertLessEqual(len(normalized), 64)
        self.assertRegex(normalized, r"^[a-z0-9._-]+$")
        self.assertEqual(normalized, runtime_proxy_acquire._normalize_random_node_host_id(raw))
        self.assertNotEqual(raw, normalized)

    def test_registration_defaults_to_reusing_recent_success(self) -> None:
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertEqual(
                "false",
                runtime_proxy_acquire._default_avoid_recent_success_reuse("registration"),
            )
            self.assertEqual(
                "true",
                runtime_proxy_acquire._default_avoid_recent_success_reuse("oauth"),
            )

    def test_recent_success_reuse_default_accepts_operator_override(self) -> None:
        with mock.patch.dict(
            "os.environ",
            {"REGISTER_PROXY_AVOID_RECENT_SUCCESS_REUSE": "true"},
            clear=True,
        ):
            self.assertEqual(
                "true",
                runtime_proxy_acquire._default_avoid_recent_success_reuse("registration"),
            )
        with mock.patch.dict(
            "os.environ",
            {"REGISTER_PROXY_AVOID_RECENT_SUCCESS_REUSE": "false"},
            clear=True,
        ):
            self.assertEqual(
                "false",
                runtime_proxy_acquire._default_avoid_recent_success_reuse("oauth"),
            )

    def test_static_mode_probes_and_selects_configured_proxy_without_easy_proxy_checkout(self) -> None:
        config = SimpleNamespace(
            enabled=True,
            required_by_default=True,
            management_base_url="http://easy-proxy:29888",
            api_key="",
            ttl_minutes=30,
        )
        raw_proxy = "http://192.168.15.20:42344"

        with mock.patch.dict(
            "os.environ",
            {"REGISTER_STATIC_PROXY_URL": raw_proxy},
            clear=False,
        ), mock.patch.object(runtime_proxy_acquire, "_proxy_runtime_config", return_value=config), \
            mock.patch.object(runtime_proxy_acquire, "ensure_easy_proxy_env_defaults"), \
            mock.patch.object(runtime_proxy_acquire, "_resolve_easy_proxy_mode", return_value="static"), \
            mock.patch.object(runtime_proxy_acquire, "_resolve_easy_proxy_unique_attempts", return_value=1), \
            mock.patch.object(runtime_proxy_acquire, "_default_easy_proxy_service_key", return_value="service-key"), \
            mock.patch.object(runtime_proxy_acquire, "_default_easy_proxy_stage", return_value="registration"), \
            mock.patch.object(runtime_proxy_acquire, "runtime_reachable_proxy_url", side_effect=lambda value: value), \
            mock.patch.object(runtime_proxy_acquire, "_probe_flow_proxy") as probe_mock, \
            mock.patch.object(runtime_proxy_acquire, "checkout_proxy") as checkout_proxy_mock, \
            mock.patch.object(runtime_proxy_acquire, "checkout_random_node_proxy") as checkout_random_mock:
            lease = runtime_proxy_acquire.acquire_flow_proxy_lease(
                flow_name="codex_openai_account_task",
                probe_urls=self.OPENAI_PROXY_PROBE_URLS,
                probe_expected_statuses={200},
            )

        self.assertEqual(raw_proxy, lease.proxy_url)
        self.assertEqual(raw_proxy, lease.raw_proxy_url)
        self.assertEqual("static", lease.acquisition_mode)
        self.assertFalse(lease.checked_out)
        self.assertEqual(3, probe_mock.call_count)
        checkout_proxy_mock.assert_not_called()
        checkout_random_mock.assert_not_called()

    def test_static_mode_retries_transient_probe_failure(self) -> None:
        config = SimpleNamespace(
            enabled=True,
            required_by_default=True,
            management_base_url="http://easy-proxy:29888",
            api_key="",
            ttl_minutes=30,
        )
        raw_proxy = "http://192.168.15.20:42344"
        first_round_failure = RuntimeError("easy_proxy_probe_failed status=403")

        with mock.patch.dict(
            "os.environ",
            {"REGISTER_STATIC_PROXY_URL": raw_proxy},
            clear=False,
        ), mock.patch.object(runtime_proxy_acquire, "_proxy_runtime_config", return_value=config), \
            mock.patch.object(runtime_proxy_acquire, "ensure_easy_proxy_env_defaults"), \
            mock.patch.object(runtime_proxy_acquire, "_resolve_easy_proxy_mode", return_value="static"), \
            mock.patch.object(runtime_proxy_acquire, "_resolve_easy_proxy_unique_attempts", return_value=3), \
            mock.patch.object(runtime_proxy_acquire, "_default_easy_proxy_service_key", return_value="service-key"), \
            mock.patch.object(runtime_proxy_acquire, "_default_easy_proxy_stage", return_value="registration"), \
            mock.patch.object(runtime_proxy_acquire, "runtime_reachable_proxy_url", side_effect=lambda value: value), \
            mock.patch.object(
                runtime_proxy_acquire,
                "_probe_flow_proxy",
                side_effect=[first_round_failure, first_round_failure, first_round_failure, None, None, None],
            ) as probe_mock, \
            mock.patch.object(runtime_proxy_acquire.time, "sleep") as sleep_mock, \
            mock.patch.object(runtime_proxy_acquire, "checkout_proxy") as checkout_proxy_mock, \
            mock.patch.object(runtime_proxy_acquire, "checkout_random_node_proxy") as checkout_random_mock:
            lease = runtime_proxy_acquire.acquire_flow_proxy_lease(
                flow_name="codex_openai_account_task",
                probe_urls=self.OPENAI_PROXY_PROBE_URLS,
                probe_expected_statuses={200},
            )

        self.assertEqual("static", lease.acquisition_mode)
        self.assertEqual(6, probe_mock.call_count)
        sleep_mock.assert_called_once_with(0.1)
        checkout_proxy_mock.assert_not_called()
        checkout_random_mock.assert_not_called()

    def test_static_mode_rejects_proxy_after_probe_retries_are_exhausted(self) -> None:
        config = SimpleNamespace(
            enabled=True,
            required_by_default=True,
            management_base_url="http://easy-proxy:29888",
            api_key="",
            ttl_minutes=30,
        )
        raw_proxy = "http://192.168.15.20:42344"

        with mock.patch.dict(
            "os.environ",
            {"REGISTER_STATIC_PROXY_URL": raw_proxy},
            clear=False,
        ), mock.patch.object(runtime_proxy_acquire, "_proxy_runtime_config", return_value=config), \
            mock.patch.object(runtime_proxy_acquire, "ensure_easy_proxy_env_defaults"), \
            mock.patch.object(runtime_proxy_acquire, "_resolve_easy_proxy_mode", return_value="static"), \
            mock.patch.object(runtime_proxy_acquire, "_resolve_easy_proxy_unique_attempts", return_value=2), \
            mock.patch.object(runtime_proxy_acquire, "_default_easy_proxy_service_key", return_value="service-key"), \
            mock.patch.object(runtime_proxy_acquire, "_default_easy_proxy_stage", return_value="registration"), \
            mock.patch.object(runtime_proxy_acquire, "runtime_reachable_proxy_url", side_effect=lambda value: value), \
            mock.patch.object(
                runtime_proxy_acquire,
                "_probe_flow_proxy",
                side_effect=RuntimeError("easy_proxy_probe_failed status=403"),
            ) as probe_mock, \
            mock.patch.object(runtime_proxy_acquire.time, "sleep") as sleep_mock, \
            mock.patch.object(runtime_proxy_acquire, "checkout_proxy") as checkout_proxy_mock, \
            mock.patch.object(runtime_proxy_acquire, "checkout_random_node_proxy") as checkout_random_mock:
            with self.assertRaisesRegex(RuntimeError, "easy_proxy_checkout_failed"):
                runtime_proxy_acquire.acquire_flow_proxy_lease(
                    flow_name="codex_openai_account_task",
                    probe_urls=self.OPENAI_PROXY_PROBE_URLS,
                    probe_expected_statuses={200},
                )

        self.assertEqual(6, probe_mock.call_count)
        sleep_mock.assert_called_once_with(0.1)
        checkout_proxy_mock.assert_not_called()
        checkout_random_mock.assert_not_called()

    def test_static_mode_can_skip_repeated_probe_after_operator_validation(self) -> None:
        config = SimpleNamespace(
            enabled=True,
            required_by_default=True,
            management_base_url="http://easy-proxy:29888",
            api_key="",
            ttl_minutes=30,
        )
        raw_proxy = "http://192.168.15.20:42344"

        with mock.patch.dict(
            "os.environ",
            {
                "REGISTER_STATIC_PROXY_URL": raw_proxy,
                "REGISTER_STATIC_PROXY_SKIP_PROBE": "true",
            },
            clear=False,
        ), mock.patch.object(runtime_proxy_acquire, "_proxy_runtime_config", return_value=config), \
            mock.patch.object(runtime_proxy_acquire, "ensure_easy_proxy_env_defaults"), \
            mock.patch.object(runtime_proxy_acquire, "_resolve_easy_proxy_mode", return_value="static"), \
            mock.patch.object(runtime_proxy_acquire, "_resolve_easy_proxy_unique_attempts", return_value=1), \
            mock.patch.object(runtime_proxy_acquire, "_default_easy_proxy_service_key", return_value="service-key"), \
            mock.patch.object(runtime_proxy_acquire, "_default_easy_proxy_stage", return_value="registration"), \
            mock.patch.object(runtime_proxy_acquire, "runtime_reachable_proxy_url", side_effect=lambda value: value), \
            mock.patch.object(runtime_proxy_acquire, "_probe_flow_proxy") as probe_mock, \
            mock.patch.object(runtime_proxy_acquire, "checkout_proxy") as checkout_proxy_mock, \
            mock.patch.object(runtime_proxy_acquire, "checkout_random_node_proxy") as checkout_random_mock:
            lease = runtime_proxy_acquire.acquire_flow_proxy_lease(
                flow_name="codex_openai_account_task",
                probe_urls=self.OPENAI_PROXY_PROBE_URLS,
                probe_expected_statuses={200},
            )

        self.assertEqual("static", lease.acquisition_mode)
        probe_mock.assert_not_called()
        checkout_proxy_mock.assert_not_called()
        checkout_random_mock.assert_not_called()

    def test_compat_checkout_probes_runtime_reachable_mixed_proxy_url(self) -> None:
        config = SimpleNamespace(
            enabled=True,
            required_by_default=True,
            management_base_url="http://192.168.15.201:29888",
            api_key="",
            ttl_minutes=30,
        )
        raw_proxy = "mixed://user:secret@192.168.15.201:22323"
        reachable_proxy = "http://user:secret@192.168.15.201:22323"
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
                mock.patch.object(runtime_proxy_acquire, "_resolve_easy_proxy_mode", return_value="lease"), \
                mock.patch.object(runtime_proxy_acquire, "_resolve_easy_proxy_unique_attempts", return_value=1), \
                mock.patch.object(runtime_proxy_acquire, "_default_easy_proxy_service_key", return_value="service-key"), \
                mock.patch.object(runtime_proxy_acquire, "_default_easy_proxy_stage", return_value="request"), \
                mock.patch.object(runtime_proxy_acquire, "_build_easy_proxy_host_id", return_value="host-1"), \
                mock.patch.object(runtime_proxy_acquire, "_read_shared_compat_checkout_cooldowns", return_value={}), \
                mock.patch.object(runtime_proxy_acquire, "runtime_reachable_proxy_url", return_value=reachable_proxy), \
                mock.patch.object(
                    runtime_proxy_acquire,
                    "checkout_proxy",
                    return_value={"id": "lease-1", "proxyUrl": raw_proxy},
                ), \
                mock.patch.object(runtime_proxy_acquire, "_probe_flow_proxy") as probe_mock:
                lease = runtime_proxy_acquire.acquire_flow_proxy_lease(
                    flow_name="easyproxy_live_smoke",
                    probe_url="https://example.com/generate_204",
                )

            self.assertEqual(reachable_proxy, lease.proxy_url)
            self.assertEqual(raw_proxy, lease.raw_proxy_url)
            probe_mock.assert_called_once_with(
                proxy_url=reachable_proxy,
                probe_url="https://example.com/generate_204",
                expected_statuses=None,
            )
        finally:
            with runtime_proxy_acquire._ACTIVE_FLOW_PROXY_LOCK:
                runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS.update(original_active)
                runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS.update(original_recent)
                runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS.update(original_failed)

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
                    side_effect=EasyProxyAPIError(
                        "EasyProxy checkout pending: INITIAL_PROXY_PROBE_PENDING",
                        status_code=503,
                        error_code="INITIAL_PROXY_PROBE_PENDING",
                    ),
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
        random_host_ids: list[str] = []
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
            random_host_ids.append(str(kwargs.get("host_id") or ""))
            return random_candidates[len(random_calls) - 1]

        try:
            with mock.patch.object(runtime_proxy_acquire, "_proxy_runtime_config", return_value=config), \
                mock.patch.object(runtime_proxy_acquire, "ensure_easy_proxy_env_defaults"), \
                mock.patch.object(runtime_proxy_acquire, "_resolve_easy_proxy_mode", return_value="random-node"), \
                mock.patch.object(runtime_proxy_acquire, "_resolve_easy_proxy_unique_attempts", return_value=3), \
                mock.patch.object(runtime_proxy_acquire, "_default_easy_proxy_service_key", return_value="service-key"), \
                mock.patch.object(runtime_proxy_acquire, "_default_easy_proxy_stage", return_value="registration"), \
                mock.patch.object(runtime_proxy_acquire, "_build_easy_proxy_host_id", return_value="random-host-1"), \
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
            self.assertEqual(["random-host-1"] * 3, random_host_ids)
            self.assertEqual("random-host-1", first.host_id)
            self.assertEqual("random-host-1", second.host_id)
        finally:
            with runtime_proxy_acquire._ACTIVE_FLOW_PROXY_LOCK:
                runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS.update(original_active)
                runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS.update(original_recent)
                runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS.update(original_failed)

    def test_random_node_rejects_candidate_when_auth_probe_fails(self) -> None:
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
        probe_calls: list[tuple[str, str]] = []
        random_candidates = [
            {
                "proxyUrl": "http://easy-proxy:25001",
                "metadata": {"selectedNodeTag": "partial-openai", "selectedNodePort": "25001"},
            },
            {
                "proxyUrl": "http://easy-proxy:25002",
                "metadata": {"selectedNodeTag": "openai-ready", "selectedNodePort": "25002"},
            },
        ]

        def _random_candidate(**kwargs):
            random_calls.append(set(kwargs.get("excluded_proxy_urls") or set()))
            return random_candidates[len(random_calls) - 1]

        def _probe_candidate(*, proxy_url: str, probe_url: str, expected_statuses: set[int] | None) -> None:
            probe_calls.append((proxy_url, probe_url))
            self.assertEqual({200}, expected_statuses)
            if proxy_url == "http://easy-proxy:25001" and probe_url == "https://auth.openai.com/log-in-or-create-account":
                raise RuntimeError("easy_proxy_probe_failed status=403 url=https://auth.openai.com/log-in-or-create-account")

        try:
            with mock.patch.object(runtime_proxy_acquire, "_proxy_runtime_config", return_value=config), \
                mock.patch.object(runtime_proxy_acquire, "ensure_easy_proxy_env_defaults"), \
                mock.patch.object(runtime_proxy_acquire, "_resolve_easy_proxy_mode", return_value="random-node"), \
                mock.patch.object(runtime_proxy_acquire, "_resolve_easy_proxy_unique_attempts", return_value=2), \
                mock.patch.object(runtime_proxy_acquire, "_default_easy_proxy_service_key", return_value="service-key"), \
                mock.patch.object(runtime_proxy_acquire, "_default_easy_proxy_stage", return_value="registration"), \
                mock.patch.object(runtime_proxy_acquire, "runtime_reachable_proxy_url", side_effect=lambda value: value), \
                mock.patch.object(
                    runtime_proxy_acquire,
                    "_probe_flow_proxy",
                    side_effect=_probe_candidate,
                ), \
                mock.patch.object(
                    runtime_proxy_acquire,
                    "checkout_random_node_proxy",
                    side_effect=_random_candidate,
                ):
                lease = runtime_proxy_acquire.acquire_flow_proxy_lease(
                    flow_name="codex_openai_account_task",
                    probe_urls=self.OPENAI_PROXY_PROBE_URLS,
                    probe_expected_statuses={200},
                )

            self.assertEqual("http://easy-proxy:25002", lease.proxy_url)
            self.assertEqual("random-node", lease.acquisition_mode)
            self.assertEqual(
                [
                    ("http://easy-proxy:25001", "https://chatgpt.com/auth/login"),
                    ("http://easy-proxy:25001", "https://platform.openai.com/login"),
                    ("http://easy-proxy:25001", "https://auth.openai.com/log-in-or-create-account"),
                    ("http://easy-proxy:25002", "https://chatgpt.com/auth/login"),
                    ("http://easy-proxy:25002", "https://platform.openai.com/login"),
                    ("http://easy-proxy:25002", "https://auth.openai.com/log-in-or-create-account"),
                ],
                probe_calls,
            )
            self.assertIn("http://easy-proxy:25001", random_calls[1])
        finally:
            with runtime_proxy_acquire._ACTIVE_FLOW_PROXY_LOCK:
                runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS.update(original_active)
                runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS.update(original_recent)
                runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS.update(original_failed)

    def test_random_node_accepts_candidate_when_auth_and_chatgpt_probes_succeed(self) -> None:
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
        probe_calls: list[tuple[str, str]] = []

        def _random_candidate(**kwargs):
            random_calls.append(set(kwargs.get("excluded_proxy_urls") or set()))
            return {
                "proxyUrl": "http://easy-proxy:25048",
                "metadata": {"selectedNodeTag": "chatgpt-auth-usable", "selectedNodePort": "25048"},
            }

        def _probe_candidate(*, proxy_url: str, probe_url: str, expected_statuses: set[int] | None) -> None:
            probe_calls.append((proxy_url, probe_url))
            self.assertEqual("http://easy-proxy:25048", proxy_url)
            self.assertEqual({200}, expected_statuses)
            if probe_url == "https://platform.openai.com/login":
                raise RuntimeError("easy_proxy_probe_failed status=403 url=https://platform.openai.com/login")

        try:
            with mock.patch.object(runtime_proxy_acquire, "_proxy_runtime_config", return_value=config), \
                mock.patch.object(runtime_proxy_acquire, "ensure_easy_proxy_env_defaults"), \
                mock.patch.object(runtime_proxy_acquire, "_resolve_easy_proxy_mode", return_value="random-node"), \
                mock.patch.object(runtime_proxy_acquire, "_resolve_easy_proxy_unique_attempts", return_value=2), \
                mock.patch.object(runtime_proxy_acquire, "_default_easy_proxy_service_key", return_value="service-key"), \
                mock.patch.object(runtime_proxy_acquire, "_default_easy_proxy_stage", return_value="registration"), \
                mock.patch.object(runtime_proxy_acquire, "runtime_reachable_proxy_url", side_effect=lambda value: value), \
                mock.patch.object(
                    runtime_proxy_acquire,
                    "_probe_flow_proxy",
                    side_effect=_probe_candidate,
                ), \
                mock.patch.object(
                    runtime_proxy_acquire,
                    "checkout_random_node_proxy",
                    side_effect=_random_candidate,
                ):
                lease = runtime_proxy_acquire.acquire_flow_proxy_lease(
                    flow_name="codex_openai_account_task",
                    probe_urls=self.OPENAI_PROXY_PROBE_URLS,
                    probe_expected_statuses={200},
                )

            self.assertEqual("http://easy-proxy:25048", lease.proxy_url)
            self.assertEqual("random-node", lease.acquisition_mode)
            self.assertEqual(
                [
                    ("http://easy-proxy:25048", "https://chatgpt.com/auth/login"),
                    ("http://easy-proxy:25048", "https://platform.openai.com/login"),
                    ("http://easy-proxy:25048", "https://auth.openai.com/log-in-or-create-account"),
                ],
                probe_calls,
            )
            self.assertEqual(1, len(random_calls))
        finally:
            with runtime_proxy_acquire._ACTIVE_FLOW_PROXY_LOCK:
                runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS.update(original_active)
                runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS.update(original_recent)
                runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS.update(original_failed)

    def test_random_node_reuses_recent_success_after_fresh_candidates_are_exhausted(self) -> None:
        config = SimpleNamespace(
            enabled=True,
            required_by_default=True,
            management_base_url="http://easy-proxy:29888",
            api_key="",
            ttl_minutes=30,
        )

        recent_proxy = "http://easy-proxy:25001"
        with runtime_proxy_acquire._ACTIVE_FLOW_PROXY_LOCK:
            original_active = set(runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS)
            original_recent = dict(runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS)
            original_failed = dict(runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS)
            runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS.clear()
            runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS.clear()
            runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS.clear()
            runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS[recent_proxy] = 999999999.0

        random_calls: list[set[str]] = []

        def _random_candidate(**kwargs):
            excluded = set(kwargs.get("excluded_proxy_urls") or set())
            random_calls.append(excluded)
            if recent_proxy in excluded:
                raise RuntimeError("EasyProxy random node checkout exhausted available nodes")
            return {
                "proxyUrl": recent_proxy,
                "metadata": {"selectedNodeTag": "recent-good", "selectedNodePort": "25001"},
            }

        try:
            with mock.patch.object(runtime_proxy_acquire, "_proxy_runtime_config", return_value=config), \
                mock.patch.object(runtime_proxy_acquire, "ensure_easy_proxy_env_defaults"), \
                mock.patch.object(runtime_proxy_acquire, "_resolve_easy_proxy_mode", return_value="random-node"), \
                mock.patch.object(runtime_proxy_acquire, "_resolve_easy_proxy_unique_attempts", return_value=2), \
                mock.patch.object(runtime_proxy_acquire, "_default_easy_proxy_service_key", return_value="service-key"), \
                mock.patch.object(runtime_proxy_acquire, "_default_easy_proxy_stage", return_value="registration"), \
                mock.patch.object(runtime_proxy_acquire, "runtime_reachable_proxy_url", side_effect=lambda value: value), \
                mock.patch.object(runtime_proxy_acquire, "_probe_flow_proxy"), \
                mock.patch.object(
                    runtime_proxy_acquire,
                    "checkout_random_node_proxy",
                    side_effect=_random_candidate,
                ):
                lease = runtime_proxy_acquire.acquire_flow_proxy_lease(
                    flow_name="codex_openai_account_task",
                    probe_url="https://chatgpt.com/auth/login",
                )

            self.assertEqual(recent_proxy, lease.proxy_url)
            self.assertEqual("random-node", lease.acquisition_mode)
            self.assertEqual(3, len(random_calls))
            self.assertIn(recent_proxy, random_calls[0])
            self.assertIn(recent_proxy, random_calls[1])
            self.assertNotIn(recent_proxy, random_calls[2])
        finally:
            with runtime_proxy_acquire._ACTIVE_FLOW_PROXY_LOCK:
                runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS.update(original_active)
                runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS.update(original_recent)
                runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS.update(original_failed)

    def test_compat_route_failure_is_shared_before_random_fallback(self) -> None:
        config = SimpleNamespace(
            enabled=True,
            required_by_default=True,
            management_base_url="http://easy-proxy:29888",
            api_key="",
            ttl_minutes=30,
        )

        bad_proxy = "http://easy-proxy:25007"
        with runtime_proxy_acquire._ACTIVE_FLOW_PROXY_LOCK:
            original_active = set(runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS)
            original_recent = dict(runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS)
            original_failed = dict(runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS)
            runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS.clear()
            runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS.clear()
            runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS.clear()

        random_calls: list[set[str]] = []

        def _random_candidate(**kwargs):
            random_calls.append(set(kwargs.get("excluded_proxy_urls") or set()))
            return {
                "proxyUrl": "http://easy-proxy:25039",
                "metadata": {"selectedNodeTag": "good", "selectedNodePort": "25039"},
            }

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
                    return_value={"id": "lease-bad", "proxyUrl": bad_proxy},
                ), \
                mock.patch.object(
                    runtime_proxy_acquire,
                    "_probe_flow_proxy",
                    side_effect=[
                        RuntimeError("easy_proxy_probe_failed status=403 target=https://chatgpt.com/auth/login"),
                        RuntimeError("easy_proxy_probe_failed status=403 target=https://chatgpt.com/auth/login"),
                        RuntimeError("easy_proxy_probe_failed status=403 target=https://chatgpt.com/auth/login"),
                        RuntimeError("easy_proxy_probe_failed status=403 target=https://chatgpt.com/auth/login"),
                        None,
                    ],
                ), \
                mock.patch.object(
                    runtime_proxy_acquire,
                    "checkout_random_node_proxy",
                    side_effect=_random_candidate,
                ), \
                mock.patch.object(runtime_proxy_acquire, "release_lease"), \
                mock.patch.object(runtime_proxy_acquire, "report_usage"):
                lease = runtime_proxy_acquire.acquire_flow_proxy_lease(
                    flow_name="codex_openai_account_task",
                    probe_url="https://chatgpt.com/auth/login",
                )

            self.assertEqual("http://easy-proxy:25039", lease.proxy_url)
            self.assertEqual("random-node", lease.acquisition_mode)
            self.assertEqual(1, len(random_calls))
            self.assertIn(bad_proxy, random_calls[0])
            with runtime_proxy_acquire._ACTIVE_FLOW_PROXY_LOCK:
                self.assertIn(bad_proxy, runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS)
        finally:
            with runtime_proxy_acquire._ACTIVE_FLOW_PROXY_LOCK:
                runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS.update(original_active)
                runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS.update(original_recent)
                runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS.update(original_failed)

    def test_compat_checkout_reuses_recent_success_after_fresh_lease_attempts_are_exhausted(self) -> None:
        released_lease_ids: list[str] = []
        reported_lease_ids: list[str] = []
        config = SimpleNamespace(
            enabled=True,
            required_by_default=True,
            management_base_url="http://easy-proxy:29888",
            api_key="",
            ttl_minutes=30,
        )

        recent_proxy = "http://easy-proxy:25027"
        with runtime_proxy_acquire._ACTIVE_FLOW_PROXY_LOCK:
            original_active = set(runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS)
            original_recent = dict(runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS)
            original_failed = dict(runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS)
            runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS.clear()
            runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS.clear()
            runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS.clear()
            runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS[recent_proxy] = 999999999.0

        try:
            with mock.patch.object(runtime_proxy_acquire, "_proxy_runtime_config", return_value=config), \
                mock.patch.object(runtime_proxy_acquire, "ensure_easy_proxy_env_defaults"), \
                mock.patch.object(runtime_proxy_acquire, "_resolve_easy_proxy_mode", return_value="auto"), \
                mock.patch.object(runtime_proxy_acquire, "_resolve_easy_proxy_unique_attempts", return_value=2), \
                mock.patch.object(runtime_proxy_acquire, "_default_easy_proxy_service_key", return_value="service-key"), \
                mock.patch.object(runtime_proxy_acquire, "_default_easy_proxy_stage", return_value="registration"), \
                mock.patch.object(runtime_proxy_acquire, "_build_easy_proxy_host_id", return_value="host-1"), \
                mock.patch.object(runtime_proxy_acquire, "runtime_reachable_proxy_url", side_effect=lambda value: value), \
                mock.patch.object(runtime_proxy_acquire, "_probe_flow_proxy"), \
                mock.patch.object(
                    runtime_proxy_acquire,
                    "checkout_proxy",
                    side_effect=[
                        {"id": "lease-1", "proxyUrl": recent_proxy},
                        {"id": "lease-2", "proxyUrl": recent_proxy},
                        {"id": "lease-3", "proxyUrl": recent_proxy},
                    ],
                ) as checkout_proxy_mock, \
                mock.patch.object(
                    runtime_proxy_acquire,
                    "checkout_random_node_proxy",
                    side_effect=RuntimeError("EasyProxy random node checkout exhausted available nodes"),
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

            self.assertEqual(recent_proxy, lease.proxy_url)
            self.assertEqual("lease", lease.acquisition_mode)
            self.assertEqual("lease-3", lease.lease_id)
            self.assertEqual(3, checkout_proxy_mock.call_count)
            checkout_random_mock.assert_not_called()
            self.assertEqual(["lease-1", "lease-2"], released_lease_ids)
            self.assertEqual([], reported_lease_ids)
        finally:
            with runtime_proxy_acquire._ACTIVE_FLOW_PROXY_LOCK:
                runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._ACTIVE_FLOW_PROXY_URLS.update(original_active)
                runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._RECENT_FLOW_PROXY_URLS.update(original_recent)
                runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS.clear()
                runtime_proxy_acquire._FAILED_FLOW_PROXY_URLS.update(original_failed)

    def test_route_failure_reports_and_retries_compat_checkout(self) -> None:
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
                    side_effect=[
                        {
                            "id": "lease-route-fail",
                            "proxyUrl": "http://easy-proxy:25023",
                        },
                        {
                            "id": "lease-route-ready",
                            "proxyUrl": "http://easy-proxy:25039",
                        },
                    ],
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
            self.assertEqual("lease", lease.acquisition_mode)
            self.assertEqual(2, checkout_proxy_mock.call_count)
            checkout_random_mock.assert_not_called()
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
