"""Exercise the actual promotion program with unchanged orchestrator images."""

from __future__ import annotations

import copy
import importlib.util
import io
import json
import subprocess
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("pc2_promotion", ROOT / "scripts/promote-pc2-auth-boundary.py")
assert SPEC is not None and SPEC.loader is not None
PROMOTION = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PROMOTION)


class PromotionResumeTests(unittest.TestCase):
    def run_promotion(self, *, fail_after_stop: bool = False, restart_provider: bool = False) -> tuple[dict, list[list[str]], bool]:
        names = ("easy-register", "easy-register-protocol-python", "easy-register-protocol", "easy-register-sms")
        containers = {
            name: {
                "Id": name + "-id", "Image": name + "-image", "Name": "/" + name,
                "State": {"Running": True, "StartedAt": "2026-09-11T11:35:50Z"},
                "Config": {"Image": name + "-tag", "Labels": {
                    "com.docker.compose.project.working_dir": str(Path("/home/mjc/easyregister")),
                    "com.docker.compose.project": "fixture",
                    "com.docker.compose.service": name,
                    "com.docker.compose.project.config_files": str(Path("/home/mjc/easyregister/base.yaml")),
                }},
            } for name in names
        }
        images = {
            key: {"tag": name + "-tag", "id": name + "-image", "files": {}}
            for key, name in (("register", names[0]), ("provider", names[1]))
        }
        payload = {"ok": True, "productionPromoted": False, "restartProvider": restart_provider, "images": images, "containersAfter": {
            name: {"id": c["Id"], "imageId": c["Image"], "startedAt": c["State"]["StartedAt"], "running": True}
            for name, c in containers.items()
        }}
        commands: list[list[str]] = []
        health_calls = 0

        def run(args, **kwargs):
            nonlocal health_calls
            commands.append(list(args))
            result: object = ""
            if args[1:3] == ["container", "inspect"]:
                result = [copy.deepcopy(containers[args[3]])]
            elif args[1:3] == ["image", "inspect"]:
                image = next(image for image in images.values() if image["tag"] == args[3])
                result = [{"Id": image["id"]}]
            elif args[1] == "stop":
                containers[names[0]]["State"]["Running"] = False
            elif args[1] == "start":
                self.assertIn(args[2], (names[0], containers[names[0]]["Id"]))
                containers[names[0]]["State"]["Running"] = True
            elif args[1] == "restart":
                self.assertFalse(containers[names[0]]["State"]["Running"])
                self.assertEqual(containers[names[1]]["Id"], args[-1])
                containers[names[1]]["State"]["StartedAt"] = "2026-09-13T00:00:00Z"
            elif args[1] == "logs":
                pass
            elif args[1] in ("run", "exec"):
                if "/health" in args[-1]:
                    health_calls += 1
                    result = {"status": "ok", "pool": {"busyWorkers": int(fail_after_stop and health_calls == 2)}}
                else:
                    result = {}
            else:
                self.fail(f"Unexpected command: {args}")
            output = result if isinstance(result, str) else json.dumps(result)
            return subprocess.CompletedProcess(args, 0, stdout=output, stderr="")

        def urlopen(*args, **kwargs):
            if not containers[names[0]]["State"]["Running"]:
                raise ConnectionError("orchestrator is stopped")
            response = io.BytesIO(b"{}")
            response.status = 200
            return response

        output = io.StringIO()
        with mock.patch("sys.stdin", io.StringIO(json.dumps(payload))), mock.patch("sys.stdout", output), mock.patch(
            "subprocess.run", side_effect=run
        ), mock.patch("urllib.request.urlopen", side_effect=urlopen), mock.patch(
            "time.sleep"
        ), mock.patch("time.monotonic", side_effect=iter(range(1000))):
            with self.assertRaises(SystemExit):
                exec(compile(PROMOTION.REMOTE, "promotion-fixture", "exec"), {})
        summary = json.loads(output.getvalue().splitlines()[-1])
        return summary, commands, bool(containers[names[0]]["State"]["Running"])

    def test_unchanged_orchestrator_is_running_after_promotion(self) -> None:
        summary, commands, running = self.run_promotion()
        self.assertTrue(summary["ok"], summary)
        self.assertTrue(running)
        self.assertEqual(1, sum(args[1] == "start" for args in commands))

    def test_rollback_resumes_unchanged_orchestrator_after_stop_failure(self) -> None:
        summary, commands, running = self.run_promotion(fail_after_stop=True)
        self.assertFalse(summary["ok"])
        self.assertEqual("provider_became_busy", summary["errorCode"])
        self.assertTrue(summary["rollbackCompleted"], summary)
        self.assertTrue(running)
        self.assertEqual(1, sum(args[1] == "start" for args in commands))

    def test_opt_in_provider_restart_occurs_while_generation_is_stopped(self) -> None:
        summary, commands, running = self.run_promotion(restart_provider=True)
        self.assertTrue(summary["ok"], summary)
        self.assertTrue(summary["providerRestarted"], summary)
        self.assertTrue(running)
        operations = [args[1] for args in commands]
        self.assertEqual(1, operations.count("restart"))
        self.assertLess(operations.index("stop"), operations.index("restart"))
        self.assertLess(operations.index("restart"), operations.index("start"))


if __name__ == "__main__":
    unittest.main()
