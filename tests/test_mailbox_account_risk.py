from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

REPO = Path(__file__).resolve().parents[1]
for path in (REPO / "server/services/orchestration_service/src", REPO / "server/services/python_shared/src"):
    sys.path.insert(0, str(path))
from others import mailbox_account_risk as risk, runner_mailbox, runtime_mailbox, dst_flow_runtime
from others.dst_flow_models import DstPlan, DstStatement


def registered_result(index=0, *, business="openai", domain="risky.test", message=None):
    email = f"account-{index}@{domain}"
    return {
        "ok": False, "errorStep": "initialize-chatgpt-login-session",
        "taskContext": {"mailboxBusinessKey": business},
        "steps": {"create-openai-account": "ok", "initialize-chatgpt-login-session": "failed"},
        "outputs": {
            "acquire-mailbox": {"email": email, "provider": "cloudflare_temp_email", "business_key": business, "mailboxRef": "fixture-ref", "mailboxSessionId": "fixture-session"},
            "create-openai-account": {"email": email, "outcome": "small_success", "page_type": "platform_callback"},
        },
        "stepErrors": {"initialize-chatgpt-login-session": {
            "code": "authorize_continue_blocked", "stage": "stage_otp_validate",
            "message": message or "chatgpt_login_otp_validate_failed status=403 body=You do not have an account because it has been deleted or deactivated.",
        }},
    }


class AccountDomainRiskTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.shared = Path(self.temporary.name)
        self.state = self.shared / "others/register-mailbox-domain-state.json"
        self.env = mock.patch.dict(os.environ, {
            "REGISTER_OUTPUT_ROOT": str(self.shared),
            "REGISTER_MAILBOX_DOMAIN_STATE_PATH": str(self.state),
            "REGISTER_MAILBOX_BUSINESS_KEY": "openai",
            "REGISTER_MAILBOX_PROVIDERS": "cloudflare_temp_email",
            "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": '{"openai":{"domainPool":["risky.test","healthy.test"]},"other":{"domainPool":["risky.test"]}}',
        }, clear=True)
        self.env.start()

    def tearDown(self):
        self.env.stop()
        self.temporary.cleanup()

    def record(self, payload):
        return runner_mailbox.record_post_registration_account_domain_outcome(
            shared_root=self.shared, result_payload_value=payload,
        )

    def block(self):
        for index in range(5):
            outcome = self.record(registered_result(index))
        return outcome

    def test_fifth_distinct_account_blocks_only_its_business_and_domain(self):
        for index in range(5):
            outcome = self.record(registered_result(index))
            self.assertEqual(index + 1, outcome["consecutiveAccountBans"])
            self.assertEqual(index == 4, outcome["blacklisted"])
        self.assertEqual(("risky.test",), runtime_mailbox._post_registration_banned_domains(business_key="openai"))
        self.assertEqual((), runtime_mailbox._post_registration_banned_domains(business_key="other"))
        self.assertFalse(runtime_mailbox._mailbox_domain_is_business_blacklisted("healthy.test", {}, business_key="openai"))
        self.assertFalse(self.state.exists())

    def test_same_account_case_and_retries_count_once(self):
        first = self.record(registered_result(0))
        payload = registered_result(0, domain="RISKY.TEST")
        for _ in range(6):
            outcome = self.record(payload)
        self.assertEqual(1, first["consecutiveAccountBans"])
        self.assertEqual(1, outcome["consecutiveAccountBans"])
        self.assertTrue(outcome["duplicate"])

    def test_generic_failures_and_workspace_errors_do_not_count(self):
        messages = ["status=403 cf-mitigated=challenge", "status=429 rate limited", "curl timeout", "wrong_otp_code", "deactivated_workspace", "TEAM_WORKSPACE_DEACTIVATED"]
        for message in messages:
            with self.subTest(message=message):
                self.assertIsNone(self.record(registered_result(message=message)))
        self.assertFalse(risk.account_risk_state_path(self.state).exists())

    def test_provider_or_signup_rejection_is_not_post_registration_ban(self):
        payload = registered_result()
        error = payload["stepErrors"].pop("initialize-chatgpt-login-session")
        for step in ("acquire-mailbox", "release-mailbox", "create-openai-account"):
            payload["errorStep"] = step
            payload["stepErrors"] = {step: error}
            self.assertIsNone(self.record(payload))

    def test_no_registration_or_mismatched_identity_is_not_counted(self):
        payload = registered_result()
        payload["steps"]["create-openai-account"] = "failed"
        self.assertIsNone(self.record(payload))
        payload["steps"]["create-openai-account"] = "ok"
        payload["outputs"]["create-openai-account"]["email"] = "different@risky.test"
        self.assertIsNone(self.record(payload))

    def test_registration_and_transient_failure_do_not_reset_streak(self):
        self.record(registered_result(0))
        payload = registered_result(1)
        payload.update(ok=True, errorStep="", stepErrors={})
        self.assertIsNone(self.record(payload))
        self.assertIsNone(self.record(registered_result(2, message="network timeout")))
        self.assertEqual(2, self.record(registered_result(3))["consecutiveAccountBans"])

    def test_fresh_healthy_login_resets_before_block_but_cached_login_does_not(self):
        self.record(registered_result(0))
        payload = registered_result(1, message="sms_no_stock")
        payload["outputs"]["initialize-chatgpt-login-session"] = {"ok": True, "status": "already_initialized", "personalWorkspaceId": "fixture", "mailboxRef": "fixture-ref", "mailboxSessionId": "fixture-session"}
        payload["steps"]["initialize-chatgpt-login-session"] = "ok"
        payload["errorStep"] = "obtain-codex-oauth"
        payload["stepErrors"] = {"obtain-codex-oauth": {"message": "sms_no_stock"}}
        self.assertIsNone(self.record(payload))
        payload["outputs"]["initialize-chatgpt-login-session"]["status"] = "completed"
        self.assertEqual(0, self.record(payload)["consecutiveAccountBans"])
        self.assertEqual(1, self.record(registered_result(2))["consecutiveAccountBans"])
        self.assertTrue(self.record(payload)["duplicate"])
        self.assertEqual(2, self.record(registered_result(3))["consecutiveAccountBans"])

    def test_block_is_sticky_and_hashed_state_contains_no_addresses(self):
        self.block()
        risk.record_account_verdict(domain_state_path=self.state, business_key="openai", email="healthy@risky.test", verdict="healthy", threshold=5)
        self.assertEqual(("risky.test",), runtime_mailbox._post_registration_banned_domains(business_key="openai"))
        text = risk.account_risk_state_path(self.state).read_text(encoding="utf-8")
        self.assertNotIn("account-0@risky.test", text)
        self.assertNotIn("healthy@risky.test", text)

    def test_new_healthy_observation_of_same_account_resets_streak(self):
        payload = registered_result(10)
        payload.update(ok=True, errorStep="", stepErrors={})
        payload["steps"]["initialize-chatgpt-login-session"] = "ok"
        payload["outputs"]["initialize-chatgpt-login-session"] = {"ok": True, "status": "completed", "personalWorkspaceId": "fixture", "email": "account-10@risky.test"}
        payload["taskContext"]["accountRiskObservationId"] = "login-1"
        self.assertEqual(0, self.record(payload)["consecutiveAccountBans"])
        self.record(registered_result(1))
        self.record(registered_result(2))
        self.assertTrue(self.record(payload)["duplicate"])
        payload["taskContext"]["accountRiskObservationId"] = "login-2"
        self.assertEqual(0, self.record(payload)["consecutiveAccountBans"])
        self.record(registered_result(3))
        self.record(registered_result(4))
        outcome = self.record(registered_result(5))
        self.assertEqual(3, outcome["consecutiveAccountBans"])
        self.assertFalse(outcome["blacklisted"])

    def test_restored_account_health_does_not_forget_previously_counted_ban(self):
        payload = registered_result(1)
        self.record(payload)
        healthy = json.loads(json.dumps(payload))
        healthy.update(ok=True, errorStep="", stepErrors={})
        healthy["steps"]["initialize-chatgpt-login-session"] = "ok"
        healthy["outputs"]["initialize-chatgpt-login-session"] = {"ok": True, "status": "completed", "personalWorkspaceId": "fixture", "email": "account-1@risky.test"}
        self.assertEqual(0, self.record(healthy)["consecutiveAccountBans"])
        outcome = self.record(payload)
        self.assertTrue(outcome["duplicate"])
        self.assertEqual(0, outcome["consecutiveAccountBans"])

    def test_healthy_login_must_match_account_identity(self):
        self.record(registered_result(0))
        payload = registered_result(1)
        payload.update(ok=True, errorStep="", stepErrors={})
        payload["steps"]["initialize-chatgpt-login-session"] = "ok"
        login = {"ok": True, "status": "completed", "personalWorkspaceId": "fixture"}
        payload["outputs"]["initialize-chatgpt-login-session"] = login
        for wrong in ({}, {"email": "other@risky.test"}, {"mailboxRef": "wrong", "mailboxSessionId": "fixture-session"}):
            login.update(wrong)
            self.assertIsNone(self.record(payload))
        self.assertEqual(2, self.record(registered_result(2))["consecutiveAccountBans"])

    def test_quoted_negated_provider_and_workspace_mentions_are_not_bans(self):
        sentence = "You do not have an account because it has been deleted or deactivated."
        for message in (f'Not returned: "{sentence}"', f'Example: {sentence}', f'mailbox provider failed: {sentence}', f'workspace error: {sentence}', f'User says "{sentence}"'):
            self.assertIsNone(self.record(registered_result(message=message)))
        payload = registered_result(message=sentence)
        payload["stepErrors"]["initialize-chatgpt-login-session"]["code"] = "TEAM_WORKSPACE_DEACTIVATED"
        self.assertIsNone(self.record(payload))
        payload["stepErrors"]["initialize-chatgpt-login-session"] = {"message": 'status=403 body=' + json.dumps({"error": {"code": "account_deactivated", "message": sentence}})}
        self.assertEqual(1, self.record(payload)["consecutiveAccountBans"])

    def test_real_upstream_truncated_json_preview_counts_but_other_wrappers_do_not(self):
        upstream = {"error": {"message": "You do not have an account because it has been deleted or deactivated. If you believe this was an error, please contact us through our help center at help.openai.com.", "type": "invalid_request_error", "param": None, "code": "account_deactivated"}}
        message = "chatgpt_login_otp_validate_failed status=403 body=" + json.dumps(upstream)[:220]
        self.assertEqual(1, self.record(registered_result(message=message))["consecutiveAccountBans"])
        self.assertIsNone(self.record(registered_result(1, message=message.replace("chatgpt_login_otp_validate_failed", "mailbox_provider_failed"))))
        self.assertEqual(2, self.record(registered_result(1, message=message.replace("chatgpt_login_otp_validate_failed", "otp_validate")))["consecutiveAccountBans"])

    def test_later_explicit_rejection_overrides_generic_healthy_output(self):
        payload = registered_result()
        payload["outputs"]["account-availability"] = {"email": "account-0@risky.test", "status": "login_succeeded"}
        self.assertEqual("banned", self.record(payload)["verdict"])

    def test_exclusions_survive_exception_dynamic_relaxation_and_other_provider(self):
        self.block()
        self.assertIn("risky.test", runtime_mailbox._resolve_mailbox_excluded_domains(business_key="openai", include_dynamic=False, except_domains=["risky.test"]))
        self.assertIn("risky.test", runtime_mailbox._resolve_mailbox_excluded_domains_for_provider(provider="im215", business_key="openai", except_domains=["risky.test"]))
        mailbox = runtime_mailbox.Mailbox(provider="im215", email="new@risky.test", ref="fixture", session_id="fixture")
        violation = runtime_mailbox._mailbox_domain_policy_violation(mailbox, business_key="openai")
        self.assertEqual(risk.ACCOUNT_BAN_BLACKLIST_REASON, violation["reason"])
        self.assertFalse(runtime_mailbox._mailbox_policy_violation_is_dynamic(violation))

    def test_actual_mailbox_selection_chooses_other_domain(self):
        self.block()
        mailbox = runtime_mailbox.Mailbox(provider="cloudflare_temp_email", email="new@healthy.test", ref="fixture", session_id="fixture")
        with mock.patch.object(runtime_mailbox, "_resolve_planned_mailbox_provider", return_value="cloudflare_temp_email"), mock.patch.object(runtime_mailbox, "create_mailbox", return_value=mailbox) as create, mock.patch.object(runtime_mailbox, "json_log"):
            resolved = runtime_mailbox.resolve_mailbox(preallocated_email=None, preallocated_session_id=None, preallocated_mailbox_ref=None, business_key="openai")
        self.assertIs(mailbox, resolved)
        # Older production selects non-moemail domains in EasyEmail rather than
        # forcing a local hint; both paths must forward the hard exclusion.
        self.assertIn(create.call_args.kwargs.get("mailcreate_domain"), (None, "healthy.test"))
        self.assertIn("risky.test", create.call_args.kwargs["excluded_domains"])

    def test_provider_returned_blocked_domain_is_rejected_even_with_fallback_enabled(self):
        self.block()
        mailbox = runtime_mailbox.Mailbox(provider="cloudflare_temp_email", email="new@risky.test", ref="fixture", session_id="fixture")
        with mock.patch.dict(os.environ, {"REGISTER_MAILBOX_DYNAMIC_BLACKLIST_EXHAUSTED_FALLBACK": "true"}), mock.patch.object(runtime_mailbox, "_resolve_mailbox_business_retry_attempts", return_value=2), mock.patch.object(runtime_mailbox, "_release_mailbox_quiet") as release, mock.patch.object(runtime_mailbox, "json_log"), self.assertRaisesRegex(RuntimeError, risk.ACCOUNT_BAN_BLACKLIST_REASON):
            runtime_mailbox._create_mailbox_with_business_policy(create_fn=lambda: mailbox, business_key="openai", accept_dynamic_violation_fallback_immediately=True)
        self.assertEqual(2, release.call_count)

    def test_legacy_mailbox_quality_success_cannot_clear_account_risk_block(self):
        self.block()
        payload = registered_result(10)
        payload.update(ok=True, errorStep="", stepErrors={})
        outcome = runner_mailbox.record_business_mailbox_domain_outcome(shared_root=self.shared, result_payload_value=payload, instance_role="main")
        self.assertFalse(outcome["blacklisted"])
        self.assertEqual(("risky.test",), runtime_mailbox._post_registration_banned_domains(business_key="openai"))

    def test_new_preallocated_address_is_rejected_before_provider_call(self):
        self.block()
        with mock.patch.object(runtime_mailbox, "create_mailbox") as create, self.assertRaisesRegex(RuntimeError, risk.ACCOUNT_BAN_BLACKLIST_REASON):
            runtime_mailbox.resolve_mailbox(preallocated_email="new@risky.test", preallocated_session_id=None, preallocated_mailbox_ref=None, recreate_preallocated_email=True, business_key="openai")
        create.assert_not_called()

    def test_continuation_old_registration_seed_does_not_mask_new_ban(self):
        payload = registered_result()
        email = payload["outputs"]["acquire-mailbox"]["email"]
        seed = {"email": email, "platformOrganization": {"status": "completed"}, "refreshToken": "synthetic", "mailboxRef": "fixture-ref", "mailboxSessionId": "fixture-session", "createdAt": datetime.now(timezone.utc).isoformat()}
        path = self.shared / "seed.json"
        path.write_text(json.dumps(seed), encoding="utf-8")
        payload["outputs"].pop("create-openai-account")
        payload["steps"].pop("create-openai-account")
        payload["outputs"]["acquire-openai-oauth-artifact"] = {"email": email, "source_path": str(path), "ok": True}
        self.assertEqual(1, self.record(payload)["consecutiveAccountBans"])
        outcome = runner_mailbox.record_business_mailbox_domain_outcome(shared_root=self.shared, result_payload_value=payload, instance_role="continue")
        self.assertEqual("openai_oauth_artifact", outcome["qualitySuccessReason"])
        self.assertTrue(outcome["postRegistrationAccountRisk"]["duplicate"])
        self.assertEqual(1, outcome["postRegistrationAccountRisk"]["consecutiveAccountBans"])

    def test_continuation_protocol_callback_seed_is_proof_but_phone_wall_is_not(self):
        payload = registered_result()
        payload["outputs"].pop("create-openai-account")
        seed = {"email": "account-0@risky.test", "outcome": "small_success", "source": "protocol_small_success", "page_type": "phone_wall", "mailboxRef": "fixture", "mailboxSessionId": "fixture", "createdAt": datetime.now(timezone.utc).isoformat(), "platformAuth": {key: "synthetic" for key in ("clientId", "redirectUri", "codeVerifier", "state", "nonce")}}
        path = self.shared / "protocol-seed.json"
        payload["outputs"]["acquire-openai-oauth-artifact"] = {"email": seed["email"], "source_path": str(path)}
        path.write_text(json.dumps(seed), encoding="utf-8")
        self.assertIsNone(self.record(payload))
        seed["page_type"] = "platform_callback"
        path.write_text(json.dumps(seed), encoding="utf-8")
        self.assertEqual(1, self.record(payload)["consecutiveAccountBans"])

    def test_concurrent_duplicate_observations_are_serialized(self):
        with ThreadPoolExecutor(max_workers=6) as pool:
            list(pool.map(lambda i: self.record(registered_result(i % 5)), range(30)))
        data = json.loads(risk.account_risk_state_path(self.state).read_text(encoding="utf-8"))
        stats = data["businesses"]["openai"]["domains"]["risky.test"]
        self.assertEqual(5, stats["consecutiveAccountBans"])
        self.assertEqual(5, len(stats["accountOutcomes"]))
        self.assertTrue(stats["blacklisted"])

    def test_corrupt_risk_state_fails_closed_without_overwriting(self):
        path = risk.account_risk_state_path(self.state)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("invalid-json", encoding="utf-8")
        with self.assertRaisesRegex(RuntimeError, "state_unreadable"):
            runtime_mailbox._post_registration_banned_domains(business_key="openai")
        with self.assertRaisesRegex(RuntimeError, "state_unreadable"):
            self.record(registered_result())
        self.assertEqual("invalid-json", path.read_text(encoding="utf-8"))

    def test_malformed_nested_risk_state_fails_closed(self):
        path = risk.account_risk_state_path(self.state)
        path.parent.mkdir(parents=True, exist_ok=True)
        for stats in (None, {"blacklisted": "false"}, {"blacklisted": False, "consecutiveAccountBans": 0, "accountOutcomes": []}):
            path.write_text(json.dumps({"schemaVersion": 1, "businesses": {"openai": {"domains": {"risky.test": stats}}}}), encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "domain_stats_invalid"):
                runtime_mailbox._post_registration_banned_domains(business_key="openai")

    def test_existing_account_recovery_is_preserved_after_block(self):
        self.block()
        recovered = {"session": {"emailAddress": "existing@risky.test", "id": "fixture-session", "mailboxRef": "cloudflare_temp_email:fixture-session", "providerTypeKey": "cloudflare_temp_email"}}
        with mock.patch.object(runtime_mailbox, "recover_mailbox_by_email", return_value=recovered) as recover, mock.patch.object(runtime_mailbox, "create_mailbox") as create:
            mailbox = runtime_mailbox.resolve_mailbox(preallocated_email="existing@risky.test", preallocated_session_id=None, preallocated_mailbox_ref=None, recover_preallocated_email=True, business_key="openai")
        self.assertEqual("existing@risky.test", mailbox.email)
        recover.assert_called_once()
        create.assert_not_called()

    def test_configurable_threshold_and_generic_business_output_contract(self):
        with mock.patch.dict(os.environ, {"REGISTER_MAILBOX_DOMAIN_POST_REGISTRATION_BAN_THRESHOLD": "2"}):
            for i in range(2):
                email = f"generic-{i}@risky.test"
                payload = {"ok": False, "taskContext": {"mailboxBusinessKey": "platform-a"}, "steps": {"account-registration": "ok"}, "outputs": {"account-registration": {"email": email, "status": "registered"}, "account-availability": {"email": email, "status": "account_disabled"}}}
                outcome = self.record(payload)
            self.assertTrue(outcome["blacklisted"])
            self.assertEqual("platform-a", outcome["businessKey"])
            self.assertEqual((), runtime_mailbox._post_registration_banned_domains(business_key="openai"))

    def test_standalone_dst_result_records_ban_and_second_report_is_duplicate(self):
        payload = registered_result()
        plan = DstPlan(steps=[DstStatement("acquire-mailbox", "fixture"), DstStatement("create-openai-account", "fixture"), DstStatement("initialize-chatgpt-login-session", "fixture")], platform="openai")

        def run_statement(*, statement, state, result):
            if statement.step_id == "initialize-chatgpt-login-session":
                raise RuntimeError(payload["stepErrors"][statement.step_id]["message"])
            value = payload["outputs"][statement.step_id]
            result.outputs[statement.step_id] = value
            result.steps[statement.step_id] = "ok"
            return value

        with mock.patch.object(dst_flow_runtime, "load_dst_flow", return_value=plan), mock.patch.object(dst_flow_runtime, "run_statement_once", side_effect=run_statement), mock.patch.object(dst_flow_runtime, "step_error_details", return_value=payload["stepErrors"]["initialize-chatgpt-login-session"]), mock.patch.object(dst_flow_runtime, "_collect_openai_oauth_artifact_after_step_nonfatal"), mock.patch.object(dst_flow_runtime, "maybe_prepare_special_step_retry", return_value=False), mock.patch.object(dst_flow_runtime, "should_retry_step", return_value=False), mock.patch.object(dst_flow_runtime, "should_retry_task", return_value=False), mock.patch.object(dst_flow_runtime, "_prepare_task_retry_mailbox_context"):
            result = dst_flow_runtime.run_dst_flow_once(task_max_attempts=1)
        self.assertFalse(result.ok)
        self.assertEqual(1, result.outputs["post-registration-account-domain-outcome"]["consecutiveAccountBans"])
        self.assertTrue(self.record(result.to_dict())["duplicate"])

    def test_standalone_explicit_output_without_env_shares_risk_read_and_write_path(self):
        payload = registered_result()
        plan = DstPlan(steps=[DstStatement("create-openai-account", "fixture"), DstStatement("authenticate-account", "fixture")], platform="openai")
        output = self.shared / "others/canary-runs/worker-1/run-1"
        output.mkdir(parents=True)
        def run_statement(*, statement, state, result):
            self.assertEqual(self.shared / "others/register-mailbox-domain-state.json", risk.resolve_account_risk_domain_state_path(Path("ignored")))
            if statement.step_id == "authenticate-account":
                self.assertEqual(("risky.test",), runtime_mailbox._post_registration_banned_domains(business_key="openai"))
                raise RuntimeError("account unavailable")
            value = {"email": "account-9@risky.test", "status": "registered"}
            result.outputs[statement.step_id] = value
            result.steps[statement.step_id] = "ok"
            return value
        with mock.patch.dict(os.environ, {}, clear=True):
            for i in range(5):
                risk.record_account_verdict(domain_state_path=self.state, business_key="openai", email=f"prior-{i}@risky.test", verdict="banned", threshold=5)
            with mock.patch.object(dst_flow_runtime, "load_dst_flow", return_value=plan), mock.patch.object(dst_flow_runtime, "run_statement_once", side_effect=run_statement), mock.patch.object(dst_flow_runtime, "step_error_details", return_value={"code": "account_disabled"}), mock.patch.object(dst_flow_runtime, "_collect_openai_oauth_artifact_after_step_nonfatal"), mock.patch.object(dst_flow_runtime, "maybe_prepare_special_step_retry", return_value=False), mock.patch.object(dst_flow_runtime, "should_retry_step", return_value=False), mock.patch.object(dst_flow_runtime, "should_retry_task", return_value=False), mock.patch.object(dst_flow_runtime, "_prepare_task_retry_mailbox_context"):
                result = dst_flow_runtime.run_dst_flow_once(task_max_attempts=1, output_dir=str(output))
            self.assertEqual(str(risk.account_risk_state_path(self.state)), result.outputs["post-registration-account-domain-outcome"]["statePath"])
            self.assertTrue(self.record(result.to_dict())["duplicate"])
            self.assertEqual(Path("unscoped"), risk.resolve_account_risk_domain_state_path(Path("unscoped")))


if __name__ == "__main__":
    unittest.main()
