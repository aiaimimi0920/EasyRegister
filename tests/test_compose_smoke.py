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

    def test_test_compose_keeps_isolated_contract_strings(self) -> None:
        payload = TEST_COMPOSE_PATH.read_text(encoding="utf-8")
        for expected in (
            "easy-register-main",
            "easy-register-continue",
            "easy-register-team",
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
