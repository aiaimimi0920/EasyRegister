from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "server" / "services" / "orchestration_service" / "src"
PYTHON_SHARED_ROOT = REPO_ROOT / "server" / "services" / "python_shared" / "src"
for candidate in (SRC_ROOT, PYTHON_SHARED_ROOT):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

import dst_flow  # noqa: E402
from others.dst_flow_loader import load_dst_flow  # noqa: E402
from others.error_catalog import ErrorCodes, resolve_retry_codes  # noqa: E402


FLOW_PATH = (
    REPO_ROOT
    / "server"
    / "services"
    / "orchestration_service"
    / "flows"
    / "codex-openai-account-prestage-v1.semantic-flow.json"
)


class PrestageFlowTests(unittest.TestCase):
    def test_canonical_flow_has_a_narrow_stop_boundary_and_unconditional_cleanup(self) -> None:
        plan = load_dst_flow(FLOW_PATH)

        self.assertEqual(
            [
                "acquire-proxy-chain",
                "acquire-mailbox",
                "create-openai-account",
                "release-proxy-chain",
                "release-mailbox",
            ],
            [statement.step_id for statement in plan.steps],
        )
        self.assertFalse(any(statement.step_type == "obtain_codex_oauth" for statement in plan.steps))
        cleanup_steps = [statement for statement in plan.steps if statement.metadata.get("stage") == "cleanup"]
        self.assertEqual(2, len(cleanup_steps))
        self.assertTrue(all(statement.metadata.get("alwaysRun") is True for statement in cleanup_steps))
        self.assertTrue(all("enabledWhen" not in statement.metadata for statement in cleanup_steps))

    def test_task_retry_profile_covers_observed_transient_failures(self) -> None:
        plan = load_dst_flow(FLOW_PATH)
        retry_policy = plan.metadata["taskRetry"]

        self.assertEqual(6, retry_policy["maxAttempts"])
        self.assertEqual("task-openai-default", retry_policy["retryProfile"])
        self.assertEqual(
            {"acquire-mailbox", "acquire-proxy-chain", "create-openai-account"},
            set(retry_policy["retryOnSteps"]),
        )
        retry_codes = resolve_retry_codes(retry_policy)
        self.assertIn(ErrorCodes.AUTHORIZE_CONTINUE_BLOCKED, retry_codes)
        self.assertIn(ErrorCodes.TRANSPORT_ERROR, retry_codes)

    def test_success_collects_continue_seed_and_releases_resources(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            run_output_dir = output_root / "others" / "main-runs" / "worker-01" / "run-task000001"
            small_success_dir = run_output_dir / "small_success"
            pool_dir = output_root / "openai" / "pending"
            artifact_path = small_success_dir / "prestage@example.com.json"
            calls: list[str] = []

            def easyproxy_dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                calls.append(step_type)
                if step_type == "acquire_proxy_chain":
                    return {"ok": True, "proxy_url": "http://proxy.local:25000", "lease_id": "lease-1"}
                if step_type == "release_proxy_chain":
                    return {"released": True}
                raise AssertionError(step_type)

            def easyemail_dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                calls.append(step_type)
                if step_type == "acquire_mailbox":
                    return {
                        "ok": True,
                        "provider": "cloudflare_temp_email",
                        "email": "prestage@example.com",
                        "mailbox_ref": "cloudflare_temp_email:prestage@example.com",
                        "session_id": "mailbox-session-1",
                        "recovery_data_credential": {
                            "emailAddress": "prestage@example.com",
                            "providerTypeKey": "cloudflare_temp_email",
                        },
                    }
                if step_type == "release_mailbox":
                    return {"released": True, "detail": "deleted"}
                raise AssertionError(step_type)

            def easyprotocol_dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
                calls.append(step_type)
                self.assertEqual("create_openai_account", step_type)
                small_success_dir.mkdir(parents=True, exist_ok=True)
                artifact_path.write_text(
                    json.dumps(
                        {
                            "outcome": "small_success",
                            "source": "protocol_small_success",
                            "email": "prestage@example.com",
                            "mailboxRef": "cloudflare_temp_email:prestage@example.com",
                            "mailboxSessionId": "mailbox-session-1",
                            "createdAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                            "platformAuth": {
                                "clientId": "client-id",
                                "redirectUri": "https://platform.openai.com/auth/callback",
                                "codeVerifier": "code-verifier",
                                "state": "state-value",
                                "nonce": "nonce-value",
                            },
                        }
                    ),
                    encoding="utf-8",
                )
                return {
                    "ok": True,
                    "status": "completed",
                    "email": "prestage@example.com",
                    "storage_path": str(artifact_path),
                }

            with mock.patch.dict(
                dst_flow.OWNER_DISPATCHERS,
                {
                    "easyproxy": easyproxy_dispatcher,
                    "easyemail": easyemail_dispatcher,
                    "easyprotocol": easyprotocol_dispatcher,
                },
                clear=True,
            ), mock.patch.dict(
                os.environ,
                {"REGISTER_OPENAI_OAUTH_SEED_MAX_AGE_SECONDS": "0"},
                clear=False,
            ):
                result = dst_flow.run_dst_flow_once(
                    output_dir=str(run_output_dir),
                    flow_path=FLOW_PATH,
                )

            copied_paths = sorted(pool_dir.glob("*.json"))

        self.assertTrue(result.ok)
        self.assertEqual(
            [
                "acquire_proxy_chain",
                "acquire_mailbox",
                "create_openai_account",
                "release_proxy_chain",
                "release_mailbox",
            ],
            calls,
        )
        self.assertEqual(["prestage@example.com.json"], [path.name for path in copied_paths])
        self.assertEqual("ok", result.steps["release-proxy-chain"])
        self.assertEqual("ok", result.steps["release-mailbox"])


if __name__ == "__main__":
    unittest.main()
