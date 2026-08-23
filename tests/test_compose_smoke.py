from __future__ import annotations

import os
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
            "REGISTER_MAILBOX_DYNAMIC_BLACKLIST_TTL_SECONDS:-21600",
            main_payload,
        )
        self.assertIn(
            "REGISTER_MAILBOX_DYNAMIC_BLACKLIST_TTL_SECONDS:-21600",
            test_payload,
        )
        self.assertIn(
            "REGISTER_MAILBOX_DYNAMIC_BLACKLIST_EXHAUSTED_FALLBACK:-false",
            main_payload,
        )
        self.assertIn(
            "REGISTER_MAILBOX_DYNAMIC_BLACKLIST_EXHAUSTED_FALLBACK:-false",
            test_payload,
        )

    def test_compose_account_audit_defaults_satisfy_preflight(self) -> None:
        """The audit batch size, protocol timeout and worker hard timeout are three
        separate defaults that preflight cross-checks. Parse them out of the shipped
        compose files and prove the combination actually starts."""
        import os
        import re
        import sys
        import tempfile
        from unittest import mock

        src_root = REPO_ROOT / "server" / "services" / "orchestration_service" / "src"
        if str(src_root) not in sys.path:
            sys.path.insert(0, str(src_root))
        shared_root = REPO_ROOT / "server" / "services" / "python_shared" / "src"
        if str(shared_root) not in sys.path:
            sys.path.insert(0, str(shared_root))
        from others.preflight import validate_runtime_preflight

        for compose_path in (MAIN_COMPOSE_PATH, TEST_COMPOSE_PATH):
            payload = compose_path.read_text(encoding="utf-8")
            defaults = {}
            for name in (
                "EASY_PROTOCOL_ACCOUNT_AUDIT_TIMEOUT_SECONDS",
                "REGISTER_ACCOUNT_AUDIT_WORKER_HARD_TIMEOUT_SECONDS",
            ):
                match = re.search(r"%s:\s*\$\{[A-Z_]+:-([0-9]+)\}" % name, payload)
                self.assertIsNotNone(match, f"{name} missing a default in {compose_path.name}")
                defaults[name] = match.group(1)

            with tempfile.TemporaryDirectory() as tmp_dir:
                env = dict(defaults)
                env["REGISTER_OUTPUT_ROOT"] = str(Path(tmp_dir) / "register-output")
                with mock.patch.dict(os.environ, env, clear=True):
                    preflight = validate_runtime_preflight()

            audit = preflight["accountAudit"]
            self.assertEqual(
                int(defaults["EASY_PROTOCOL_ACCOUNT_AUDIT_TIMEOUT_SECONDS"]),
                audit["protocolTimeoutSeconds"],
            )
            self.assertEqual(
                int(defaults["REGISTER_ACCOUNT_AUDIT_WORKER_HARD_TIMEOUT_SECONDS"]),
                audit["workerHardTimeoutSeconds"],
            )

    def test_compose_bounds_continue_worker_runtime(self) -> None:
        self.assertIn(
            "REGISTER_CONTINUE_WORKER_HARD_TIMEOUT_SECONDS:-900",
            MAIN_COMPOSE_PATH.read_text(encoding="utf-8"),
        )
        self.assertIn(
            "EASYREGISTER_TEST_CONTINUE_WORKER_HARD_TIMEOUT_SECONDS:-900",
            TEST_COMPOSE_PATH.read_text(encoding="utf-8"),
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

    def test_deploy_host_autodetects_easyprotocol_python_provider_output_mount(self) -> None:
        payload = (REPO_ROOT / "deploy-host.ps1").read_text(encoding="utf-8")
        self.assertIn("Get-DockerBindSourceForProtocolTarget", payload)
        self.assertIn("$ContainerName-python-*", payload)

    def test_test_compose_keeps_isolated_contract_strings(self) -> None:
        payload = TEST_COMPOSE_PATH.read_text(encoding="utf-8")
        for expected in (
            "easy-register-test",
            "REGISTER_FLOW_SPECS_JSON",
            "EASYREGISTER_TEST_FLOW_SPECS_JSON",
            "EASYREGISTER_TEST_INSTANCE_ROLE",
            "29790",
            "tmp/easyregister-test-output",
            "EasyAiMi",
        ):
            self.assertIn(expected, payload)

    def test_compose_exposes_current_easyproxy_management_contract(self) -> None:
        for compose_path in (MAIN_COMPOSE_PATH, TEST_COMPOSE_PATH):
            payload = compose_path.read_text(encoding="utf-8")
            for expected in (
                "EASY_PROXY_BASE_URL",
                "EASY_PROXY_RUNTIME_HOST",
                "EASY_PROXY_MANAGEMENT_USERNAME",
                "EASY_PROXY_MANAGEMENT_PASSWORD",
                "EASY_PROXY_INITIAL_PROBE_MAX_ATTEMPTS",
                "EASY_PROXY_INITIAL_PROBE_BACKOFF_SECONDS",
                "REGISTER_PROXY_MODE",
            ):
                self.assertIn(expected, payload)
            self.assertIn("REGISTER_PROXY_MODE:", payload)
            self.assertIn("PROXY_MODE:-lease", payload)

    def test_compose_allows_runtime_flow_spec_overrides(self) -> None:
        main_payload = MAIN_COMPOSE_PATH.read_text(encoding="utf-8")
        test_payload = TEST_COMPOSE_PATH.read_text(encoding="utf-8")

        self.assertIn("REGISTER_INSTANCE_ROLE: ${REGISTER_INSTANCE_ROLE:-mixed}", main_payload)
        self.assertIn("REGISTER_FLOW_PATH: ${REGISTER_FLOW_PATH:-}", main_payload)
        self.assertIn("REGISTER_FLOW_SPECS_JSON: ${REGISTER_FLOW_SPECS_JSON:-}", main_payload)

        self.assertIn("REGISTER_INSTANCE_ROLE: ${EASYREGISTER_TEST_INSTANCE_ROLE:-mixed}", test_payload)
        self.assertIn("REGISTER_FLOW_PATH: ${EASYREGISTER_TEST_FLOW_PATH:-}", test_payload)
        self.assertIn("REGISTER_FLOW_SPECS_JSON: ${EASYREGISTER_TEST_FLOW_SPECS_JSON:-}", test_payload)

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

    def test_main_compose_config_parses_with_default_output_mount_when_docker_is_available(self) -> None:
        docker_path = shutil.which("docker")
        if not docker_path:
            self.skipTest("docker not available")
        result = subprocess.run(
            [docker_path, "compose", "-f", str(MAIN_COMPOSE_PATH), "config"],
            cwd=REPO_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(0, result.returncode, msg=result.stderr or result.stdout)

    def test_main_compose_honors_flow_spec_env_override_when_docker_is_available(self) -> None:
        docker_path = shutil.which("docker")
        if not docker_path:
            self.skipTest("docker not available")
        override = (
            '[{"name":"override-main","path":"override-main.json","role":"main","weight":9},'
            '{"name":"override-continue","path":"override-continue.json","role":"continue","weight":4}]'
        )
        env = {
            **os.environ,
            "REGISTER_OUTPUT_DIR_HOST": str(REPO_ROOT / "tmp" / "compose-smoke-output"),
            "REGISTER_TEAM_AUTH_DIR_HOST": str(REPO_ROOT / "tmp" / "compose-smoke-team-auth"),
            "REGISTER_FLOW_SPECS_JSON": override,
            "REGISTER_INSTANCE_ROLE": "mixed",
        }
        result = subprocess.run(
            [docker_path, "compose", "-f", str(MAIN_COMPOSE_PATH), "config"],
            cwd=REPO_ROOT,
            check=False,
            capture_output=True,
            text=True,
            env=env,
        )
        self.assertEqual(0, result.returncode, msg=result.stderr or result.stdout)
        self.assertIn("override-main", result.stdout)
        self.assertIn("override-continue", result.stdout)
        self.assertNotIn("openai-account-availability-audit", result.stdout)
        self.assertNotIn("codex-team-expand", result.stdout)


if __name__ == "__main__":
    unittest.main()
