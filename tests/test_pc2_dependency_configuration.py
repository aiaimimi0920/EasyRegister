from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/prepare-pc2-dependencies.py"
SPEC = importlib.util.spec_from_file_location("pc2_dependency_configuration", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
PREPARE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PREPARE)
VERIFY_SPEC = importlib.util.spec_from_file_location("pc2_runtime_verification", SCRIPT.with_name("verify-pc2-runtime.py"))
assert VERIFY_SPEC is not None and VERIFY_SPEC.loader is not None
VERIFY = importlib.util.module_from_spec(VERIFY_SPEC)
VERIFY_SPEC.loader.exec_module(VERIFY)


class Pc2DependencyConfigurationTests(unittest.TestCase):
    def test_external_provider_disables_both_managed_and_bundled_pools(self) -> None:
        gateway = {
            "services": [{"name": "PythonProtocol-001", "endpoint": "http://old:9100", "supported_operations": ["codex.semantic.step"]}],
            "provider_pool": {"providers": {"python": {"warm_replicas": 5, "max_replicas": 20}}},
            "managed_provider_runtime": {"enabled": True},
            "control_plane": {"enabled": True},
        }
        PREPARE.configure_external_python_provider(gateway)
        self.assertIsNone(gateway["provider_pool"]["providers"])
        self.assertIn('"providers": null', json.dumps(gateway))
        self.assertFalse(gateway["managed_provider_runtime"]["enabled"])
        self.assertEqual("http://easy-protocol-python-001:9100", gateway["services"][0]["endpoint"])
        self.assertEqual(["codex.semantic.step"], gateway["services"][0]["supported_operations"])
        self.assertEqual({"enabled": True}, gateway["control_plane"])

    def test_unexpected_registry_is_rejected_without_partial_mutation(self) -> None:
        gateway = {"services": [{"name": "other"}], "provider_pool": {"providers": {"python": {}}}}
        before = copy.deepcopy(gateway)
        with self.assertRaisesRegex(RuntimeError, "Unexpected source provider registry"):
            PREPARE.configure_external_python_provider(gateway)
        self.assertEqual(before, gateway)


class Pc2ExecutorIdentityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.direct = {
            "status": "succeeded",
            "result": {"listen": "0.0.0.0:9100", "service": "PythonProtocol", "pool": {
                "mode": "dynamic_process_pool", "maxWorkers": 1, "maxTasksPerWorker": 50,
            }},
        }
        self.routed = copy.deepcopy(self.direct)
        self.routed["selected_service"] = "PythonProtocol-001"

    def test_matching_external_executor_passes(self) -> None:
        self.assertTrue(VERIFY.external_executor_identity_matches(self.direct, self.routed))

    def test_embedded_executor_is_rejected_even_when_both_are_healthy(self) -> None:
        self.routed["result"]["listen"] = "127.0.0.1:41509"
        self.assertFalse(VERIFY.external_executor_identity_matches(self.direct, self.routed))

    def test_echo_response_cannot_prove_executor_identity(self) -> None:
        echo = {"status": "succeeded", "selected_service": "PythonProtocol-001", "result": {"echo": {}}}
        self.assertFalse(VERIFY.external_executor_identity_matches(self.direct, echo))

    def test_matching_malformed_pool_cannot_prove_identity(self) -> None:
        self.direct["result"]["pool"] = {}
        self.routed["result"]["pool"] = {}
        self.assertFalse(VERIFY.external_executor_identity_matches(self.direct, self.routed))

    def test_wrong_pool_or_service_is_rejected(self) -> None:
        self.routed["result"]["pool"]["maxTasksPerWorker"] = 1000
        self.assertFalse(VERIFY.external_executor_identity_matches(self.direct, self.routed))
        self.routed = copy.deepcopy(self.direct)
        self.routed["selected_service"] = "other"
        self.assertFalse(VERIFY.external_executor_identity_matches(self.direct, self.routed))


if __name__ == "__main__":
    unittest.main()
