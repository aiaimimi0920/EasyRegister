from __future__ import annotations

import shutil
import subprocess
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
MAIN_COMPOSE_PATH = REPO_ROOT / "compose" / "docker-compose.yaml"
TEST_COMPOSE_PATH = REPO_ROOT / "compose" / "docker-compose.test.yaml"


class ComposeSmokeTests(unittest.TestCase):
    def test_main_compose_uses_easyaimi_external_network(self) -> None:
        payload = MAIN_COMPOSE_PATH.read_text(encoding="utf-8")
        self.assertIn("EasyAiMi", payload)

    def test_main_compose_exposes_protocol_bridge_env_vars(self) -> None:
        payload = MAIN_COMPOSE_PATH.read_text(encoding="utf-8")
        for expected in (
            "REGISTER_PROTOCOL_BRIDGE_DIR",
            "REGISTER_PROTOCOL_BRIDGE_TARGET_DIR",
            "REGISTER_PROTOCOL_OUTPUT_MIRROR_DIR",
            "REGISTER_PROTOCOL_OUTPUT_TARGET_DIR",
        ):
            self.assertIn(expected, payload)

    def test_compose_keeps_email_otp_provider_threshold_aligned_with_domain_threshold(self) -> None:
        main_payload = MAIN_COMPOSE_PATH.read_text(encoding="utf-8")
        test_payload = TEST_COMPOSE_PATH.read_text(encoding="utf-8")
        self.assertIn(
            "REGISTER_MAILBOX_EMAIL_OTP_FAILURE_BLACKLIST_THRESHOLD:-6",
            main_payload,
        )
        self.assertIn(
            "REGISTER_MAILBOX_EMAIL_OTP_PROVIDER_FAILURE_BLACKLIST_THRESHOLD:-6",
            main_payload,
        )
        self.assertIn(
            "REGISTER_MAILBOX_EMAIL_OTP_PROVIDER_FAILURE_BLACKLIST_THRESHOLD:-6",
            test_payload,
        )

    def test_compose_disables_dynamic_mailbox_blacklist_exhausted_fallback_by_default(self) -> None:
        main_payload = MAIN_COMPOSE_PATH.read_text(encoding="utf-8")
        test_payload = TEST_COMPOSE_PATH.read_text(encoding="utf-8")
        self.assertIn(
            "REGISTER_MAILBOX_DYNAMIC_BLACKLIST_EXHAUSTED_FALLBACK:-false",
            main_payload,
        )
        self.assertIn(
            "REGISTER_MAILBOX_DYNAMIC_BLACKLIST_EXHAUSTED_FALLBACK:-false",
            test_payload,
        )

    def test_deploy_host_generates_protocol_bridge_mount_contract(self) -> None:
        payload = (REPO_ROOT / "deploy-host.ps1").read_text(encoding="utf-8")
        for expected in (
            "ProtocolRegisterOutputDirHost",
            "REGISTER_PROTOCOL_BRIDGE_DIR",
            "REGISTER_PROTOCOL_BRIDGE_TARGET_DIR",
            "REGISTER_PROTOCOL_OUTPUT_MIRROR_DIR",
            "REGISTER_PROTOCOL_OUTPUT_TARGET_DIR",
            "/shared/protocol-register-output",
            "easyregister-bridge",
        ):
            self.assertIn(expected, payload)

    def test_test_compose_keeps_isolated_contract_strings(self) -> None:
        payload = TEST_COMPOSE_PATH.read_text(encoding="utf-8")
        for expected in (
            "easy-register-test",
            "REGISTER_FLOW_SPECS_JSON",
            "\"role\":\"main\"",
            "\"role\":\"continue\"",
            "\"role\":\"team\"",
            "29790",
            "tmp/easyregister-test-output",
            "EasyAiMi",
        ):
            self.assertIn(expected, payload)

    def test_test_compose_config_parses_when_docker_is_available(self) -> None:
        docker_path = shutil.which("docker")
        if not docker_path:
            self.skipTest("docker not available")
        result = subprocess.run(
            [docker_path, "compose", "-f", str(TEST_COMPOSE_PATH), "config"],
            cwd=REPO_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(0, result.returncode, msg=result.stderr or result.stdout)


if __name__ == "__main__":
    unittest.main()
