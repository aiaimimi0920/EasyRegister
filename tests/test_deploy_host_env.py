from __future__ import annotations

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
            self.assertEqual("openai", env_values.get("REGISTER_SMS_BUSINESS_KEY"))
            self.assertEqual("hero_sms", env_values.get("REGISTER_SMS_PROVIDER_BLACKLIST"))
            self.assertEqual("false", env_values.get("REGISTER_SMS_ALLOW_PAID"))
            self.assertEqual("false", env_values.get("REGISTER_SMS_ALLOW_REUSE"))
            self.assertEqual("1", env_values.get("REGISTER_SMS_MAX_BINDINGS_PER_PHONE"))
            self.assertEqual("balanced", env_values.get("REGISTER_SMS_SELECTION_MODE"))

            policies = env_values.get("REGISTER_SMS_BUSINESS_POLICIES_JSON", "")
            self.assertIn('"openai":{"enabled":true', policies)
            self.assertIn('"providerBlacklist":["hero_sms"]', policies)
            self.assertIn('"allowPaid":false', policies)


if __name__ == "__main__":
    unittest.main()
