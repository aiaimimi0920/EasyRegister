from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEPLOY_HOST = REPO_ROOT / "deploy-host.ps1"


def _read_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="ascii").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key] = value
    return values


class DeployHostEnvTests(unittest.TestCase):
    def test_materialize_only_keeps_service_api_keys_empty_without_explicit_input(self) -> None:
        powershell = shutil.which("powershell") or shutil.which("pwsh")
        if not powershell:
            self.skipTest("PowerShell not available")

        with tempfile.TemporaryDirectory(prefix="easyregister-deploy-host-") as temp:
            launcher_root = Path(temp)
            script_path = launcher_root / "deploy-host.ps1"
            shutil.copyfile(DEPLOY_HOST, script_path)

            command = [powershell, "-NoProfile"]
            if Path(powershell).name.lower().startswith("powershell"):
                command.extend(["-ExecutionPolicy", "Bypass"])
            command.extend(
                [
                    "-File",
                    str(script_path),
                    "-RepoCacheRoot",
                    str(REPO_ROOT),
                    "-OutputDirHost",
                    str(launcher_root / "runtime" / "register-output"),
                    "-CodexFreeDirHost",
                    str(launcher_root / "codex" / "free"),
                    "-CodexTeamDirHost",
                    str(launcher_root / "codex" / "team"),
                    "-CodexTeamInputDirHost",
                    str(launcher_root / "codex" / "team-input"),
                    "-CodexTeamMotherInputDirHost",
                    str(launcher_root / "codex" / "team-mother-input"),
                    "-Image",
                    "ghcr.io/example/easyregister:test",
                    "-MaterializeOnly",
                    "-NoBuild",
                ]
            )

            result = subprocess.run(
                command,
                cwd=launcher_root,
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(
                0,
                result.returncode,
                msg=(result.stderr or result.stdout).strip(),
            )

            env_values = _read_dotenv(launcher_root / ".deploy-compose.env")
            self.assertEqual("", env_values.get("MAILBOX_SERVICE_API_KEY"))
            self.assertEqual("", env_values.get("EASY_PROXY_API_KEY"))

    def test_materialize_only_exports_sms_policy_and_dashboard_default(self) -> None:
        powershell = shutil.which("powershell") or shutil.which("pwsh")
        if not powershell:
            self.skipTest("PowerShell not available")

        with tempfile.TemporaryDirectory(prefix="easyregister-deploy-host-") as temp:
            launcher_root = Path(temp)
            script_path = launcher_root / "deploy-host.ps1"
            shutil.copyfile(DEPLOY_HOST, script_path)

            command = [powershell, "-NoProfile"]
            if Path(powershell).name.lower().startswith("powershell"):
                command.extend(["-ExecutionPolicy", "Bypass"])
            command.extend(
                [
                    "-File",
                    str(script_path),
                    "-RepoCacheRoot",
                    str(REPO_ROOT),
                    "-OutputDirHost",
                    str(launcher_root / "runtime" / "register-output"),
                    "-CodexFreeDirHost",
                    str(launcher_root / "codex" / "free"),
                    "-CodexTeamDirHost",
                    str(launcher_root / "codex" / "team"),
                    "-CodexTeamInputDirHost",
                    str(launcher_root / "codex" / "team-input"),
                    "-CodexTeamMotherInputDirHost",
                    str(launcher_root / "codex" / "team-mother-input"),
                    "-MailboxServiceApiKey",
                    "mailbox-test-key",
                    "-EasyProxyApiKey",
                    "proxy-test-key",
                    "-SmsServiceApiKey",
                    "sms-test-key",
                    "-SmsSelectionPlanTimeoutSeconds",
                    "75",
                    "-SmsSelectionPlanAttempts",
                    "2",
                    "-PhoneTerminalRetryAttempts",
                    "4",
                    "-PhoneSmsCodeWaitRetryAttempts",
                    "3",
                    "-Image",
                    "ghcr.io/example/easyregister:test",
                    "-MaterializeOnly",
                    "-NoBuild",
                ]
            )

            result = subprocess.run(
                command,
                cwd=launcher_root,
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(
                0,
                result.returncode,
                msg=(result.stderr or result.stdout).strip(),
            )

            env_values = _read_dotenv(launcher_root / ".deploy-compose.env")
            self.assertEqual("true", env_values.get("REGISTER_DASHBOARD_ENABLED"))
            self.assertEqual("6", env_values.get("REGISTER_MAILBOX_EMAIL_OTP_FAILURE_BLACKLIST_THRESHOLD"))
            self.assertEqual("6", env_values.get("REGISTER_MAILBOX_EMAIL_OTP_PROVIDER_FAILURE_BLACKLIST_THRESHOLD"))
            self.assertEqual("21600", env_values.get("REGISTER_MAILBOX_DYNAMIC_BLACKLIST_TTL_SECONDS"))
            self.assertEqual("http://easy-sms:8080", env_values.get("SMS_SERVICE_BASE_URL"))
            self.assertEqual("sms-test-key", env_values.get("SMS_SERVICE_API_KEY"))
            self.assertEqual("75", env_values.get("SMS_SERVICE_SELECTION_PLAN_TIMEOUT_SECONDS"))
            self.assertEqual("2", env_values.get("SMS_SERVICE_SELECTION_PLAN_ATTEMPTS"))
            self.assertEqual("4", env_values.get("REGISTER_PHONE_VERIFICATION_TERMINAL_RETRY_ATTEMPTS"))
            self.assertEqual("3", env_values.get("REGISTER_PHONE_VERIFICATION_SMS_CODE_WAIT_RETRY_ATTEMPTS"))
            self.assertEqual("openai", env_values.get("REGISTER_SMS_BUSINESS_KEY"))
            self.assertEqual("hero_sms", env_values.get("REGISTER_SMS_PROVIDER_BLACKLIST"))
            self.assertEqual("false", env_values.get("REGISTER_SMS_ALLOW_PAID"))
            self.assertEqual("false", env_values.get("REGISTER_SMS_ALLOW_REUSE"))
            self.assertEqual("1", env_values.get("REGISTER_SMS_MAX_BINDINGS_PER_PHONE"))
            self.assertEqual("balanced", env_values.get("REGISTER_SMS_SELECTION_MODE"))
            self.assertEqual("21600", env_values.get("REGISTER_SMS_TERMINAL_INVALID_PHONE_BLACKLIST_SECONDS"))

            policies = env_values.get("REGISTER_SMS_BUSINESS_POLICIES_JSON", "")
            self.assertIn('"openai":{"enabled":true', policies)
            self.assertIn('"providerBlacklist":["hero_sms"]', policies)
            self.assertIn('"allowPaid":false', policies)

    def test_materialize_only_generates_result_pool_mounts_for_credential_root(self) -> None:
        powershell = shutil.which("powershell") or shutil.which("pwsh")
        if not powershell:
            self.skipTest("PowerShell not available")

        with tempfile.TemporaryDirectory(prefix="easyregister-deploy-host-") as temp:
            launcher_root = Path(temp)
            script_path = launcher_root / "deploy-host.ps1"
            shutil.copyfile(DEPLOY_HOST, script_path)
            credential_root = launcher_root / "nas-oauth"

            command = [powershell, "-NoProfile"]
            if Path(powershell).name.lower().startswith("powershell"):
                command.extend(["-ExecutionPolicy", "Bypass"])
            command.extend(
                [
                    "-File",
                    str(script_path),
                    "-RepoCacheRoot",
                    str(REPO_ROOT),
                    "-OutputDirHost",
                    str(launcher_root / "runtime" / "register-output"),
                    "-CredentialRootHost",
                    str(credential_root),
                    "-MailboxServiceApiKey",
                    "mailbox-test-key",
                    "-EasyProxyApiKey",
                    "proxy-test-key",
                    "-SmsServiceApiKey",
                    "sms-test-key",
                    "-Image",
                    "ghcr.io/example/easyregister:test",
                    "-MaterializeOnly",
                    "-NoBuild",
                ]
            )

            result = subprocess.run(
                command,
                cwd=launcher_root,
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(
                0,
                result.returncode,
                msg=(result.stderr or result.stdout).strip(),
            )

            env_values = _read_dotenv(launcher_root / ".deploy-compose.env")
            self.assertEqual(str(credential_root), env_values.get("REGISTER_CREDENTIAL_ROOT_HOST"))
            self.assertEqual(str(credential_root / "codex"), env_values.get("REGISTER_CODEX_ROOT_DIR_HOST"))
            self.assertEqual(str(credential_root / "openai"), env_values.get("REGISTER_OPENAI_ROOT_DIR_HOST"))

            override_payload = (launcher_root / ".deploy-compose.result-pools.generated.yaml").read_text(
                encoding="utf-8"
            )
            self.assertIn(str(credential_root / "codex").replace("\\", "/"), override_payload)
            self.assertIn('target: "/shared/register-output/codex"', override_payload)
            self.assertIn(str(credential_root / "openai").replace("\\", "/"), override_payload)
            self.assertIn('target: "/shared/register-output/openai"', override_payload)
            self.assertTrue((credential_root / "codex" / "free").is_dir())
            self.assertTrue((credential_root / "openai" / "converted").is_dir())
            self.assertTrue((launcher_root / "runtime" / "register-output" / "others").is_dir())

    def test_materialize_only_can_mount_result_pools_from_external_docker_volumes(self) -> None:
        powershell = shutil.which("powershell") or shutil.which("pwsh")
        if not powershell:
            self.skipTest("PowerShell not available")

        with tempfile.TemporaryDirectory(prefix="easyregister-deploy-host-") as temp:
            launcher_root = Path(temp)
            script_path = launcher_root / "deploy-host.ps1"
            shutil.copyfile(DEPLOY_HOST, script_path)
            credential_root = launcher_root / "nas-oauth"

            command = [powershell, "-NoProfile"]
            if Path(powershell).name.lower().startswith("powershell"):
                command.extend(["-ExecutionPolicy", "Bypass"])
            command.extend(
                [
                    "-File",
                    str(script_path),
                    "-RepoCacheRoot",
                    str(REPO_ROOT),
                    "-OutputDirHost",
                    str(launcher_root / "runtime" / "register-output"),
                    "-CredentialRootHost",
                    str(credential_root),
                    "-CodexRootDockerVolume",
                    "easyregister_codex_pool",
                    "-OpenaiRootDockerVolume",
                    "easyregister_openai_pool",
                    "-MailboxServiceApiKey",
                    "mailbox-test-key",
                    "-EasyProxyApiKey",
                    "proxy-test-key",
                    "-SmsServiceApiKey",
                    "sms-test-key",
                    "-Image",
                    "ghcr.io/example/easyregister:test",
                    "-MaterializeOnly",
                    "-NoBuild",
                ]
            )

            result = subprocess.run(
                command,
                cwd=launcher_root,
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(
                0,
                result.returncode,
                msg=(result.stderr or result.stdout).strip(),
            )

            env_values = _read_dotenv(launcher_root / ".deploy-compose.env")
            self.assertEqual("easyregister_codex_pool", env_values.get("REGISTER_CODEX_ROOT_DOCKER_VOLUME"))
            self.assertEqual("easyregister_openai_pool", env_values.get("REGISTER_OPENAI_ROOT_DOCKER_VOLUME"))

            override_payload = (launcher_root / ".deploy-compose.result-pools.generated.yaml").read_text(
                encoding="utf-8"
            )
            self.assertIn('type: volume', override_payload)
            self.assertIn('source: "easyregister_codex_pool"', override_payload)
            self.assertIn('target: "/shared/register-output/codex"', override_payload)
            self.assertIn('source: "easyregister_openai_pool"', override_payload)
            self.assertIn('target: "/shared/register-output/openai"', override_payload)
            self.assertIn("volumes:", override_payload)
            self.assertIn("easyregister_codex_pool:", override_payload)
            self.assertIn("external: true", override_payload)
            self.assertIn("easyregister_openai_pool:", override_payload)

    def test_materialize_only_can_mount_protocol_bridge_from_external_docker_volume(self) -> None:
        powershell = shutil.which("powershell") or shutil.which("pwsh")
        if not powershell:
            self.skipTest("PowerShell not available")

        with tempfile.TemporaryDirectory(prefix="easyregister-deploy-host-") as temp:
            launcher_root = Path(temp)
            script_path = launcher_root / "deploy-host.ps1"
            shutil.copyfile(DEPLOY_HOST, script_path)
            protocol_output = launcher_root / "protocol" / "register-output"

            command = [powershell, "-NoProfile"]
            if Path(powershell).name.lower().startswith("powershell"):
                command.extend(["-ExecutionPolicy", "Bypass"])
            command.extend(
                [
                    "-File",
                    str(script_path),
                    "-RepoCacheRoot",
                    str(REPO_ROOT),
                    "-OutputDirHost",
                    str(launcher_root / "runtime" / "register-output"),
                    "-ProtocolRegisterOutputDirHost",
                    str(protocol_output),
                    "-ProtocolBridgeDockerVolume",
                    "easyregister_protocol_bridge",
                    "-MailboxServiceApiKey",
                    "mailbox-test-key",
                    "-EasyProxyApiKey",
                    "proxy-test-key",
                    "-SmsServiceApiKey",
                    "sms-test-key",
                    "-Image",
                    "ghcr.io/example/easyregister:test",
                    "-MaterializeOnly",
                    "-NoBuild",
                ]
            )

            result = subprocess.run(
                command,
                cwd=launcher_root,
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(
                0,
                result.returncode,
                msg=(result.stderr or result.stdout).strip(),
            )

            env_values = _read_dotenv(launcher_root / ".deploy-compose.env")
            self.assertEqual(
                "easyregister_protocol_bridge",
                env_values.get("REGISTER_PROTOCOL_BRIDGE_DOCKER_VOLUME"),
            )

            override_payload = (launcher_root / ".deploy-compose.protocol-bridge.generated.yaml").read_text(
                encoding="utf-8"
            )
            self.assertIn(str(protocol_output).replace("\\", "/"), override_payload)
            self.assertIn('type: volume', override_payload)
            self.assertIn('source: "easyregister_protocol_bridge"', override_payload)
            self.assertIn(
                'target: "/shared/protocol-register-output/easyregister-bridge"',
                override_payload,
            )
            self.assertIn("easyregister_protocol_bridge:", override_payload)
            self.assertIn("external: true", override_payload)

    def test_materialize_only_autodetects_majority_easyprotocol_provider_output_mount(self) -> None:
        powershell = shutil.which("powershell") or shutil.which("pwsh")
        if not powershell:
            self.skipTest("PowerShell not available")

        with tempfile.TemporaryDirectory(prefix="easyregister-deploy-host-") as temp:
            launcher_root = Path(temp)
            script_path = launcher_root / "deploy-host.ps1"
            shutil.copyfile(DEPLOY_HOST, script_path)

            fake_bin = launcher_root / "fake-bin"
            fake_bin.mkdir()
            fake_docker_py = fake_bin / "fake_docker.py"
            fake_docker_py.write_text(
                """from __future__ import annotations

import json
import os
import sys


args = sys.argv[1:]
wrong_source = os.environ["FAKE_PROTOCOL_WRONG_SOURCE"]
right_source = os.environ["FAKE_PROTOCOL_RIGHT_SOURCE"]

if args[:1] == ["ps"]:
    print("easy-protocol-python-021")
    print("easy-protocol-python-022")
    print("easy-protocol-python-023")
    raise SystemExit(0)

if args[:1] == ["inspect"]:
    container_name = args[-1]
    source_by_container = {
        "easy-protocol": "",
        "easy-protocol-python-021": wrong_source,
        "easy-protocol-python-022": right_source,
        "easy-protocol-python-023": right_source,
    }
    source = source_by_container.get(container_name, "")
    mounts = []
    if source:
        mounts.append({"Source": source, "Destination": "/shared/register-output"})
    print(json.dumps(mounts))
    raise SystemExit(0)

raise SystemExit(1)
""",
                encoding="utf-8",
            )
            (fake_bin / "docker.cmd").write_text(
                "@echo off\r\npython \"%~dp0fake_docker.py\" %*\r\n",
                encoding="ascii",
            )
            fake_docker_sh = fake_bin / "docker"
            fake_docker_sh.write_text(
                "#!/usr/bin/env python3\n"
                "import runpy\n"
                f"runpy.run_path({str(fake_docker_py)!r}, run_name='__main__')\n",
                encoding="utf-8",
            )
            fake_docker_sh.chmod(0o755)

            wrong_source = launcher_root / "protocol-wrong" / "register-output"
            right_source = launcher_root / "protocol-right" / "register-output"
            wrong_docker_source = self._docker_desktop_host_mount_source(wrong_source)
            right_docker_source = self._docker_desktop_host_mount_source(right_source)
            command = [powershell, "-NoProfile"]
            if Path(powershell).name.lower().startswith("powershell"):
                command.extend(["-ExecutionPolicy", "Bypass"])
            command.extend(
                [
                    "-File",
                    str(script_path),
                    "-RepoCacheRoot",
                    str(REPO_ROOT),
                    "-OutputDirHost",
                    str(launcher_root / "runtime" / "register-output"),
                    "-CodexFreeDirHost",
                    str(launcher_root / "codex" / "free"),
                    "-CodexTeamDirHost",
                    str(launcher_root / "codex" / "team"),
                    "-CodexTeamInputDirHost",
                    str(launcher_root / "codex" / "team-input"),
                    "-CodexTeamMotherInputDirHost",
                    str(launcher_root / "codex" / "team-mother-input"),
                    "-MailboxServiceApiKey",
                    "mailbox-test-key",
                    "-EasyProxyApiKey",
                    "proxy-test-key",
                    "-SmsServiceApiKey",
                    "sms-test-key",
                    "-Image",
                    "ghcr.io/example/easyregister:test",
                    "-MaterializeOnly",
                    "-NoBuild",
                ]
            )

            env = os.environ.copy()
            env["PATH"] = str(fake_bin) + os.pathsep + env.get("PATH", "")
            env["FAKE_PROTOCOL_WRONG_SOURCE"] = wrong_docker_source
            env["FAKE_PROTOCOL_RIGHT_SOURCE"] = right_docker_source

            result = subprocess.run(
                command,
                cwd=launcher_root,
                env=env,
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(
                0,
                result.returncode,
                msg=(result.stderr or result.stdout).strip(),
            )

            override_payload = (launcher_root / ".deploy-compose.protocol-bridge.generated.yaml").read_text(
                encoding="utf-8"
            )
            self.assertIn(str(right_source).replace("\\", "/"), override_payload)
            self.assertNotIn(str(wrong_source).replace("\\", "/"), override_payload)

    @staticmethod
    def _docker_desktop_host_mount_source(path: Path) -> str:
        normalized = str(path.resolve()).replace("\\", "/")
        if len(normalized) >= 3 and normalized[1:3] == ":/":
            drive = normalized[0].lower()
            rest = normalized[3:]
            return f"/run/desktop/mnt/host/{drive}/{rest}"
        return normalized


if __name__ == "__main__":
    unittest.main()
