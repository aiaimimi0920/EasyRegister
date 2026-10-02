from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "pc2_start_paid_when_seed_ready.py"
spec = importlib.util.spec_from_file_location("first_paid_start", SCRIPT)
controller = importlib.util.module_from_spec(spec)
spec.loader.exec_module(controller)


class FirstPaidStartTests(unittest.TestCase):
    def test_ready_seed_starts_once_and_durable_marker_rejects_second_invocation(self) -> None:
        seed = {"ready": True, "backupMatches": True, "hashMatches": True, "started": False}
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            controller, "BASE", Path(directory),
        ), mock.patch.object(controller, "verify_runtime"), mock.patch.object(
            controller, "in_container", side_effect=[seed, {"quotedCostUsd": "0.045"}],
        ), mock.patch.object(controller.subprocess, "run") as start:
            self.assertEqual(0, controller.main())
            with self.assertRaises(FileExistsError):
                controller.main()
        start.assert_called_once_with(["docker", "start", controller.PAID], capture_output=True, check=True)

    def test_no_ready_seed_does_not_query_price_or_start_paid_container(self) -> None:
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            controller, "BASE", Path(directory),
        ), mock.patch.object(controller, "verify_runtime"), mock.patch.object(
            controller, "in_container", return_value={"ready": False},
        ) as query, mock.patch.object(controller.time, "monotonic", side_effect=[0, 1, 1801]), mock.patch.object(
            controller.time, "sleep",
        ), mock.patch.object(controller.subprocess, "run") as start:
            self.assertEqual(1, controller.main())
        query.assert_called_once_with("easy-register", controller.SEED_STATUS)
        start.assert_not_called()

    def test_quote_failure_stops_without_starting_the_first_paid_container(self) -> None:
        seed = {"ready": True, "backupMatches": True, "hashMatches": True, "started": False}
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            controller, "BASE", Path(directory),
        ), mock.patch.object(controller, "verify_runtime"), mock.patch.object(
            controller, "in_container", side_effect=[seed, RuntimeError("quote unavailable")],
        ), mock.patch.object(controller.subprocess, "run") as start:
            with self.assertRaisesRegex(RuntimeError, "quote unavailable"):
                controller.main()
        start.assert_not_called()


if __name__ == "__main__":
    unittest.main()
