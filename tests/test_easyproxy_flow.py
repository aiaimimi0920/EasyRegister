from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock


SRC_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "src"
PYTHON_SHARED_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "python_shared" / "src"
for candidate in (SRC_ROOT, PYTHON_SHARED_ROOT):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

import easyproxy_flow  # noqa: E402
from easyproxy_flow import _proxy_failure_class_from_code  # noqa: E402
from errors import ErrorCodes  # noqa: E402


class EasyProxyFlowTests(unittest.TestCase):
    def test_authorize_continue_blocked_reports_high_confidence_route_failure(self) -> None:
        failure_class, route_confidence = _proxy_failure_class_from_code(ErrorCodes.AUTHORIZE_CONTINUE_BLOCKED)

        self.assertEqual("route_failure", failure_class)
        self.assertEqual("high", route_confidence)

    def test_release_proxy_chain_for_authorize_blocked_reports_route_failure(self) -> None:
        with mock.patch.object(easyproxy_flow, "release_flow_proxy_lease") as release_lease:
            result = easyproxy_flow.dispatch_easyproxy_step(
                step_type="release_proxy_chain",
                step_input={
                    "proxy_chain": {
                        "flow_name": "codex_openai_account_task",
                        "proxy_url": "http://easy-proxy:25001",
                        "raw_proxy_url": "http://easy-proxy:25001",
                        "lease_id": "lease-1",
                        "host_id": "host-1",
                        "management_base_url": "http://easy-proxy:29888",
                        "unique_key": "http://easy-proxy:25001",
                        "started_monotonic": 1.0,
                        "service_key": "openai",
                        "stage": "registration",
                        "acquisition_mode": "checkout",
                        "checked_out": True,
                    },
                    "error_code": ErrorCodes.AUTHORIZE_CONTINUE_BLOCKED,
                },
            )

        self.assertTrue(result["released"])
        release_lease.assert_called_once()
        self.assertEqual(ErrorCodes.AUTHORIZE_CONTINUE_BLOCKED, release_lease.call_args.kwargs["error_code"])
        self.assertEqual("route_failure", release_lease.call_args.kwargs["failure_class"])
        self.assertEqual("high", release_lease.call_args.kwargs["route_confidence"])

    def test_acquire_proxy_chain_passes_all_probe_urls_to_runtime_acquire(self) -> None:
        probe_urls = [
            "https://chatgpt.com/auth/login",
            "https://platform.openai.com/login",
            "https://auth.openai.com/log-in-or-create-account",
        ]
        with mock.patch.object(easyproxy_flow, "acquire_flow_proxy_lease", return_value=easyproxy_flow.FlowProxyLease.direct(flow_name="test")) as acquire_lease:
            result = easyproxy_flow.dispatch_easyproxy_step(
                step_type="acquire_proxy_chain",
                step_input={
                    "flow_name": "codex_openai_account_task",
                    "probe_url": "https://chatgpt.com/auth/login",
                    "probe_urls": probe_urls,
                    "probe_expected_statuses": [200],
                    "max_acquire_attempts": 1,
                    "required": True,
                },
            )

        self.assertEqual("direct", result["acquisition_mode"])
        acquire_lease.assert_called_once()
        self.assertEqual("https://chatgpt.com/auth/login", acquire_lease.call_args.kwargs["probe_url"])
        self.assertEqual(probe_urls, acquire_lease.call_args.kwargs["probe_urls"])
        self.assertEqual({200}, acquire_lease.call_args.kwargs["probe_expected_statuses"])

    def test_acquire_proxy_chain_reuses_avoided_proxy_after_exhaustion(self) -> None:
        lease = easyproxy_flow.FlowProxyLease(
            flow_name="codex_openai_account_task",
            proxy_url="http://easy-proxy:25044",
            raw_proxy_url="http://easy-proxy:25044",
            lease_id="lease-1",
            host_id="host-1",
            management_base_url="http://easy-proxy:29888",
            unique_key="http://easy-proxy:25044",
            started_monotonic=1.0,
            service_key="openai",
            stage="registration",
            acquisition_mode="checkout",
            checked_out=True,
        )
        with mock.patch.object(
            easyproxy_flow,
            "acquire_flow_proxy_lease",
            side_effect=[lease, lease],
        ) as acquire_lease, mock.patch.object(
            easyproxy_flow,
            "release_flow_proxy_lease",
        ) as release_lease:
            result = easyproxy_flow.dispatch_easyproxy_step(
                step_type="acquire_proxy_chain",
                step_input={
                    "flow_name": "codex_openai_account_task",
                    "avoid_proxy_urls": ["http://easy-proxy:25044"],
                    "max_acquire_attempts": 2,
                    "required": True,
                },
            )

        self.assertEqual("http://easy-proxy:25044", result["proxy_url"])
        self.assertTrue(result["reused_avoided_proxy"])
        self.assertEqual(2, acquire_lease.call_count)
        release_lease.assert_called_once()


if __name__ == "__main__":
    unittest.main()
