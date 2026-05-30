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


if __name__ == "__main__":
    unittest.main()
