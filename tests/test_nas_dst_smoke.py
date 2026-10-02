from __future__ import annotations

import contextlib
import io
import json
import os
import unittest
from unittest import mock

import nas_dst_smoke
from others.dst_flow_loader import load_dst_flow


class NasDstSmokeTests(unittest.TestCase):
    def test_diagnostic_flow_uses_real_dependency_owners_and_always_cleans_up(self) -> None:
        plan = load_dst_flow(str(nas_dst_smoke.FLOW_PATH))
        self.assertEqual("nas-dependencies-smoke-v1", plan.flow_id)
        self.assertEqual(6, len(plan.steps))
        payload = json.loads(nas_dst_smoke.FLOW_PATH.read_text(encoding="utf-8"))["definition"]
        self.assertEqual(1, payload["metadata"]["taskRetry"]["maxAttempts"])
        self.assertEqual({"easyemail", "easyproxy"}, {step["metadata"]["owner"] for step in payload["steps"]})
        for step in payload["steps"]:
            if step["id"].startswith("release-"):
                self.assertTrue(step["metadata"]["alwaysRun"])
        recover = payload["steps"][2]
        self.assertTrue(recover["input"]["recover_preallocated_email"])
        self.assertIn("preallocated_recovery_data_credential", recover["input"])
        # Do not let the legacy reference fallback hide a failed real recovery.
        self.assertNotIn("preallocated_mailbox_ref", recover["input"])

    def test_missing_credentials_fail_before_any_dst_or_network_work(self) -> None:
        output = io.StringIO()
        with mock.patch.dict("os.environ", {}, clear=True), \
            mock.patch.object(nas_dst_smoke, "run_dst_flow_once") as run_flow, \
            contextlib.redirect_stdout(output):
            self.assertEqual(1, nas_dst_smoke.main())
        run_flow.assert_not_called()
        result = json.loads(output.getvalue())
        self.assertFalse(result["ok"])
        self.assertEqual(["EASY_EMAIL_API_KEY", "EASY_PROXY_MANAGEMENT_PASSWORD"], result["missingEnvironment"])

    def test_normalizes_deployed_mailbox_credentials_before_preflight(self) -> None:
        cases = (
            ({"MAILBOX_SERVICE_API_KEY": "canonical-fixture-key"}, "canonical-fixture-key"),
            ({"EASY_EMAIL_API_KEY": "alias-fixture-key"}, "alias-fixture-key"),
            ({"MAILBOX_SERVICE_API_KEY": "  ", "EASY_EMAIL_API_KEY": "alias-fixture-key"}, "alias-fixture-key"),
            ({"MAILBOX_SERVICE_API_KEY": "canonical-fixture-key", "EASY_EMAIL_API_KEY": "alias-fixture-key"}, "canonical-fixture-key"),
        )
        for credentials, expected in cases:
            with self.subTest(credentials=tuple(credentials)):
                output = io.StringIO()
                environment = {
                    **credentials,
                    "MAILBOX_SERVICE_BASE_URL": "http://mail-fixture.invalid:8080",
                    "EASY_PROXY_MANAGEMENT_PASSWORD": "proxy-fixture-key",
                }

                def stop_before_network(**_kwargs: object) -> None:
                    self.assertEqual(expected, os.environ["MAILBOX_SERVICE_API_KEY"])
                    self.assertEqual(expected, os.environ["EASY_EMAIL_API_KEY"])
                    self.assertEqual(environment["MAILBOX_SERVICE_BASE_URL"], os.environ["EASY_EMAIL_BASE_URL"])
                    raise RuntimeError("fixture-stop-before-network")

                with mock.patch.dict(os.environ, environment, clear=True), \
                    mock.patch.object(nas_dst_smoke, "run_dst_flow_once", side_effect=stop_before_network) as run_flow, \
                    contextlib.redirect_stdout(output):
                    self.assertEqual(1, nas_dst_smoke.main())
                run_flow.assert_called_once()
                summary = json.loads(output.getvalue())
                self.assertNotIn("missingEnvironment", summary)
                self.assertEqual("RuntimeError", summary["errorType"])
                self.assertNotIn("fixture-key", output.getvalue())


if __name__ == "__main__":
    unittest.main()
