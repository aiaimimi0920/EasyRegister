"""Exercise redacted remote audit output without SSH or external requests."""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import subprocess
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("pc2_restart_audit", ROOT / "scripts/audit-pc2-restart-results.py")
assert SPEC is not None and SPEC.loader is not None
AUDIT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(AUDIT)


class Response(io.BytesIO):
    status = 200


class RestartAuditTests(unittest.TestCase):
    def observe(self, message: str) -> tuple[dict[str, object], str]:
        token = "private-control-credential"
        names = ("easy-register", "easy-register-protocol-python", "easy-register-protocol", "easy-register-sms")
        containers = [{
            "Name": "/" + name, "Id": name + "-id", "Image": name + "-image",
            "State": {"Running": True, "StartedAt": "2026-09-12T18:38:58Z"},
            "RestartCount": 0, "Config": {"Env": ["EASY_PROTOCOL_CONTROL_TOKEN=" + token]},
        } for name in names]
        record = {
            "event": "register_run_finished", "taskIndex": 1, "ok": False,
            "startedAt": "2026-09-12T18:39:00Z", "finishedAt": "2026-09-12T18:39:08Z",
            "result": {
                "errorStep": "create-openai-account", "steps": {},
                "stepErrors": {"create-openai-account": {
                    "code": "browser_verification_required", "stage": "stage_platform_login",
                    "detail": "platform_login", "category": "blocked", "message": message,
                }},
            },
        }

        def run(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
            if args[:2] == ["docker", "inspect"]:
                output = json.dumps(containers)
            elif args[:2] == ["docker", "logs"]:
                output = json.dumps(record) if args[-1] == "easy-register" else ""
            elif args[:2] == ["docker", "exec"]:
                output = "{}"
            else:
                raise AssertionError("unexpected_command")
            return subprocess.CompletedProcess(args, 0, output, "")

        output = io.StringIO()
        with (
            mock.patch("subprocess.run", side_effect=run),
            mock.patch("urllib.request.urlopen", return_value=Response(b"{}")),
            contextlib.redirect_stdout(output),
        ):
            exec(compile(AUDIT.OBSERVE, "remote-audit", "exec"), {
                "SINCE": "2026-09-12T18:38:58Z", "ARTIFACT_SOURCE": AUDIT.ARTIFACTS,
            })
        raw = output.getvalue()
        result = json.loads(raw)
        self.assertIs(result["ok"], True)
        self.assertNotIn(token, raw)
        return result["completedTasks"][0], raw

    def test_retains_challenge_evidence_without_private_message(self) -> None:
        task, raw = self.observe(
            "browser_verification_required status=403 cf_mitigated=challenge "
            "email=private-account@example.com cookie=private-cookie-value "
            "proxy=http://private-user:private-password@localhost:1234",
        )
        self.assertIs(task.get("terminalCloudflareChallenge"), True)
        self.assertEqual(task["terminalHttpStatuses"], [403])
        for private in ("private-account", "private-cookie", "private-user", "private-password"):
            self.assertNotIn(private, raw)

    def test_plain_403_is_not_evidence_of_cloudflare_challenge(self) -> None:
        task, _ = self.observe("status=403 access_denied")
        self.assertIs(task.get("terminalCloudflareChallenge"), False)
        self.assertEqual(task["terminalHttpStatuses"], [403])

    def test_accepts_explicit_header_spellings(self) -> None:
        for marker in ("cf-mitigated: challenge", "CF_MITIGATED = CHALLENGE"):
            with self.subTest(marker=marker):
                task, _ = self.observe("http=403 " + marker)
                self.assertIs(task.get("terminalCloudflareChallenge"), True)


if __name__ == "__main__":
    unittest.main()
