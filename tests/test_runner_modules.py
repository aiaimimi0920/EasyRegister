from __future__ import annotations

from contextlib import nullcontext
import os
import sys
import tempfile
import unittest
import json
from types import SimpleNamespace
from pathlib import Path
from unittest import mock


SRC_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from errors import ErrorCodes  # noqa: E402
from others.config import RunnerFlowSpec  # noqa: E402
from others import runner_artifacts, runner_credential_sync, runner_failures, runner_flow_scheduler, runner_mailbox, runner_process_supervisor, runner_team_artifacts, runner_team_auth, runner_team_cleanup, runner_worker_loop, runner_worker_maintenance, runner_worker_results  # noqa: E402


class RunnerArtifactsTests(unittest.TestCase):
    def test_select_local_split_obeys_percentage(self) -> None:
        with mock.patch("others.runner_artifacts.random.random", return_value=0.20):
            self.assertTrue(runner_artifacts.select_local_split(percent=50.0))
        with mock.patch("others.runner_artifacts.random.random", return_value=0.80):
            self.assertFalse(runner_artifacts.select_local_split(percent=50.0))

    def test_small_success_failure_target_pool_dir_routes_wait_pool(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            with mock.patch.dict(os.environ, {"REGISTER_OUTPUT_ROOT": str(output_root)}, clear=True):
                target = runner_artifacts.small_success_failure_target_pool_dir(
                    output_root=output_root,
                    result_payload_value={"errorCode": "free_personal_workspace_missing"},
                )
        self.assertEqual((output_root / "others" / "small-success-wait-pool").resolve(), target)

    def test_small_success_failure_target_pool_dir_routes_manual_oauth_pool(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "REGISTER_FREE_MANUAL_OAUTH_PRESERVE_ENABLED": "true",
                    "REGISTER_FREE_MANUAL_OAUTH_PRESERVE_ERROR_CODES": "token_invalidated",
                },
                clear=True,
            ):
                target = runner_artifacts.small_success_failure_target_pool_dir(
                    output_root=output_root,
                    result_payload_value={"errorCode": "token_invalidated"},
                )
        self.assertEqual((output_root / "others" / "free-manual-oauth-pool").resolve(), target)


class RunnerTeamArtifactsTests(unittest.TestCase):
    def test_team_has_collectable_artifacts_accepts_result_object(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            team_path = Path(tmp_dir) / "member.json"
            team_path.write_text("{}", encoding="utf-8")
            result = SimpleNamespace(
                to_dict=lambda: {
                    "outputs": {
                        "collect-team-pool-artifacts": {
                            "artifacts": [
                                {
                                    "kind": "member",
                                    "email": "member@example.com",
                                    "preferred_name": "member.json",
                                    "team_pool_path": str(team_path),
                                }
                            ]
                        }
                    }
                }
            )
            self.assertTrue(runner_team_artifacts.team_has_collectable_artifacts(result=result))


class RunnerFlowSchedulerTests(unittest.TestCase):
    def test_choose_runnable_flow_spec_skips_empty_continue_pool(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            shared_root = output_root / "shared"
            continue_pool_dir = shared_root / "others" / "small-success-continue-pool"
            spec = RunnerFlowSpec(
                name="continue-openai",
                flow_path="continue-flow.json",
                instance_role="continue",
                weight=1.0,
                team_auth_path="",
                task_max_attempts=0,
                small_success_pool_dir=continue_pool_dir,
                mailbox_business_key="openai",
            )
            selected, selection = runner_flow_scheduler.choose_runnable_flow_spec(
                flow_specs=(spec,),
                output_root=output_root,
                shared_root=shared_root,
            )
        self.assertIsNone(selected)
        self.assertEqual("small_success_pool_empty", selection["skipped"][0]["reason"])

    def test_choose_runnable_flow_spec_selects_ready_continue_pool(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            shared_root = output_root / "shared"
            continue_pool_dir = shared_root / "others" / "small-success-continue-pool"
            continue_pool_dir.mkdir(parents=True, exist_ok=True)
            (continue_pool_dir / "seed.json").write_text("{}", encoding="utf-8")
            spec = RunnerFlowSpec(
                name="continue-openai",
                flow_path="continue-flow.json",
                instance_role="continue",
                weight=1.0,
                team_auth_path="",
                task_max_attempts=0,
                small_success_pool_dir=continue_pool_dir,
                mailbox_business_key="openai",
            )
            selected, selection = runner_flow_scheduler.choose_runnable_flow_spec(
                flow_specs=(spec,),
                output_root=output_root,
                shared_root=shared_root,
            )
        self.assertIsNotNone(selected)
        self.assertEqual("continue-openai", selected.name)
        self.assertEqual("pool_ready", selection["selected"]["reason"])


class RunnerProcessSupervisorTests(unittest.TestCase):
    def test_task_slots_exhausted_reads_counter_without_lock(self) -> None:
        class _Counter:
            @property
            def value(self) -> int:
                raise AssertionError("synchronized value getter should not be used")

            def get_obj(self) -> Any:
                return SimpleNamespace(value=1)

        counter = _Counter()
        self.assertTrue(runner_process_supervisor.task_slots_exhausted(task_counter=counter, max_runs=1))

    def test_should_stop_supervisor_after_worker_stop_only_when_last_worker_and_exhausted(self) -> None:
        counter = SimpleNamespace(get_obj=lambda: SimpleNamespace(value=1))
        self.assertTrue(
            runner_process_supervisor.should_stop_supervisor_after_worker_stop(
                processes={},
                task_counter=counter,
                max_runs=1,
            )
        )
        self.assertFalse(
            runner_process_supervisor.should_stop_supervisor_after_worker_stop(
                processes={1: object()},
                task_counter=counter,
                max_runs=1,
            )
        )

    def test_cleanup_process_handle_joins_closes_and_optionally_terminates(self) -> None:
        process = mock.Mock()
        process.is_alive.return_value = True
        runner_process_supervisor.cleanup_process_handle(
            process=process,
            join_timeout=0.25,
            terminate_if_alive=True,
        )
        process.join.assert_any_call(timeout=0.25)
        process.terminate.assert_called_once_with()
        process.join.assert_any_call(timeout=1.0)
        process.close.assert_called_once_with()

    def test_main_exits_cleanly_after_last_worker_when_max_runs_reached(self) -> None:
        fake_process = mock.Mock()
        fake_process.pid = 321
        fake_process.exitcode = 0
        fake_process.is_alive.return_value = False

        stop_event = mock.Mock()
        stop_event.is_set.return_value = False
        task_counter = SimpleNamespace(get_obj=lambda: SimpleNamespace(value=1))
        ctx = SimpleNamespace(
            Event=mock.Mock(return_value=stop_event),
            Value=mock.Mock(return_value=task_counter),
        )
        config = SimpleNamespace(
            output_root=Path("C:/tmp/register-output"),
            shared_root=Path("C:/tmp/register-output"),
            small_success_pool_dir=Path("C:/tmp/register-output/small-success-pool"),
            free_oauth_pool_dir=Path("C:/tmp/register-output/free-oauth-pool"),
            flow_path="team-flow.json",
            instance_id="mixed-test",
            instance_role="mixed",
            worker_count=1,
            delay_seconds=0.0,
            worker_stagger_seconds=0.0,
            max_runs=1,
            task_max_attempts=1,
            flow_specs=(),
            easy_protocol_base_url="http://easy-protocol-service:9788",
            easy_protocol_control_token="secure-token",
            easy_protocol_control_actor="register-dashboard",
        )
        service_state = mock.Mock()
        with mock.patch.object(runner_process_supervisor, "_validate_runtime_preflight", return_value={}):
            with mock.patch.object(runner_process_supervisor, "RunnerMainConfig") as config_cls:
                config_cls.from_env.return_value = config
                with mock.patch.object(runner_process_supervisor, "_ensure_directory"):
                    with mock.patch.object(runner_process_supervisor, "cleanup_dashboard_worker_state_files"):
                        with mock.patch.object(runner_process_supervisor, "ServiceRuntimeState", return_value=service_state):
                            with mock.patch.object(runner_process_supervisor, "install_signal_handlers"):
                                with mock.patch.object(runner_process_supervisor, "start_dashboard_server_if_enabled", return_value=None):
                                    with mock.patch.object(runner_process_supervisor.mp, "get_context", return_value=ctx):
                                        with mock.patch.object(runner_process_supervisor, "start_worker", return_value=fake_process):
                                            with mock.patch.object(runner_process_supervisor, "_json_log") as json_log:
                                                exit_code = runner_process_supervisor.main()
        self.assertEqual(0, exit_code)
        service_state.started.assert_called_once_with(pid=mock.ANY, max_runs=1)
        service_state.stopped.assert_called_once_with(pid=mock.ANY, task_count=1)
        stop_event.set.assert_not_called()
        fake_process.join.assert_any_call(timeout=0.0)
        fake_process.close.assert_called_once_with()
        events = [call.args[0]["event"] for call in json_log.call_args_list if call.args and isinstance(call.args[0], dict) and "event" in call.args[0]]
        self.assertIn("register_supervisor_finally_entered", events)
        self.assertIn("register_supervisor_stopped", events)


class RunnerFailuresTests(unittest.TestCase):
    def test_team_auth_blacklist_reason_requires_retry_evidence(self) -> None:
        payload = {
            "errorStep": "invite-codex-member",
            "stepAttempts": {
                "invite-codex-member": 2,
                "refresh-team-auth-on-demand": 1,
            },
            "stepErrors": {
                "invite-codex-member": {
                    "code": ErrorCodes.TEAM_AUTH_TOKEN_INVALIDATED,
                    "message": "token expired",
                }
            },
        }
        reason = runner_failures.team_auth_blacklist_reason(result_payload_value=payload)
        self.assertIn("token expired", reason)
        self.assertIn(ErrorCodes.TEAM_AUTH_TOKEN_INVALIDATED, reason)

    def test_extra_failure_cooldown_seconds_uses_typed_cleanup_config(self) -> None:
        payload = {
            "errorStep": "create-openai-account",
            "stepErrors": {
                "create-openai-account": {
                    "code": ErrorCodes.TRANSPORT_ERROR,
                    "message": "transport failure",
                }
            },
        }
        with mock.patch.dict(
            os.environ,
            {"REGISTER_CREATE_ACCOUNT_COOLDOWN_SECONDS": "45"},
            clear=True,
        ):
            cooldown = runner_failures.extra_failure_cooldown_seconds(result=payload)
        self.assertEqual(45.0, cooldown)

    def test_team_mother_failure_cooldown_seconds_uses_structured_codes(self) -> None:
        payload = {
            "errorStep": "invite-team-members",
            "stepErrors": {
                "invite-team-members": {
                    "code": ErrorCodes.TEAM_SEATS_FULL,
                    "message": "workspace full",
                }
            },
        }
        with mock.patch.dict(
            os.environ,
            {"REGISTER_TEAM_INVITE_FAILURE_COOLDOWN_SECONDS": "123"},
            clear=True,
        ):
            cooldown = runner_failures.team_mother_failure_cooldown_seconds(result=payload)
        self.assertEqual(123.0, cooldown)


class RunnerMailboxTests(unittest.TestCase):
    def test_mailbox_capacity_failure_detail_uses_structured_code(self) -> None:
        payload = {
            "errorStep": "acquire-mailbox",
            "stepErrors": {
                "acquire-mailbox": {
                    "code": ErrorCodes.MAILBOX_UNAVAILABLE,
                    "message": "mailbox capacity unavailable",
                }
            },
        }
        detail = runner_mailbox.mailbox_capacity_failure_detail(result_payload_value=payload)
        self.assertIn("mailbox capacity unavailable", detail)

    def test_record_business_mailbox_domain_outcome_writes_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            payload = {
                "ok": False,
                "steps": {"acquire-mailbox": "ok"},
                "outputs": {
                    "acquire-mailbox": {
                        "email": "user@sall.cc",
                        "provider": "moemail",
                        "business_key": "openai",
                    }
                },
            }
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_MAILBOX_BUSINESS_KEY": "generic",
                    "REGISTER_MAILBOX_DOMAIN_BLACKLIST": "fallback.test",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": (
                        '{"openai":{"explicitBlacklistDomains":["coolkid.icu"]}}'
                    ),
                },
                clear=True,
            ):
                outcome = runner_mailbox.record_business_mailbox_domain_outcome(
                    shared_root=shared_root,
                    result_payload_value=payload,
                    instance_role="main",
                )
            self.assertIsNotNone(outcome)
            self.assertEqual("openai", outcome["businessKey"])
            self.assertEqual("sall.cc", outcome["domain"])
            state_path = Path(outcome["statePath"])
            self.assertTrue(state_path.is_file())
            state_payload = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertIn("businesses", state_payload)
            self.assertIn("openai", state_payload["businesses"])
            self.assertEqual(
                ["coolkid.icu"],
                state_payload["businesses"]["openai"]["explicitBlacklistDomains"],
            )

    def test_mailbox_domain_blacklist_reason_requires_unsupported_email(self) -> None:
        unsupported_payload = {
            "stepErrors": {
                "create-openai-account": {
                    "message": "create_account status=400 body={\"error\":{\"code\":\"unsupported_email\"}}",
                }
            }
        }
        generic_payload = {
            "stepErrors": {
                "create-openai-account": {
                    "message": "Failed to create account. Please try again.",
                }
            }
        }
        self.assertEqual(
            "unsupported_email",
            runner_mailbox.mailbox_domain_blacklist_reason(result_payload_value=unsupported_payload),
        )
        self.assertEqual(
            "",
            runner_mailbox.mailbox_domain_blacklist_reason(result_payload_value=generic_payload),
        )

    def test_mark_mailbox_capacity_failure_respects_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            with mock.patch.dict(
                os.environ,
                {"REGISTER_MAILBOX_CLEANUP_FAILURE_THRESHOLD": "3"},
                clear=True,
            ):
                result = runner_mailbox.mark_mailbox_capacity_failure(
                    shared_root=shared_root,
                    detail="mailbox capacity unavailable",
                )
            self.assertEqual("recovery_threshold_not_reached", result["status"])
            self.assertEqual(1, result["consecutiveFailures"])


class RunnerTeamCleanupTests(unittest.TestCase):
    def test_team_capacity_failure_detail_uses_structured_code(self) -> None:
        payload = {
            "errorStep": "invite-codex-member",
            "stepErrors": {
                "invite-codex-member": {
                    "code": ErrorCodes.TEAM_SEATS_FULL,
                    "message": "workspace full",
                }
            },
        }
        detail = runner_team_cleanup.team_capacity_failure_detail(result_payload_value=payload)
        self.assertIn("workspace full", detail)

    def test_capacity_cooldown_state_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            team_auth_path = str(shared_root / "mother.json")
            runner_team_cleanup.mark_team_auth_capacity_cooldown(
                shared_root=shared_root,
                team_auth_path=team_auth_path,
                cooldown_seconds=60.0,
                detail="capacity full",
            )
            self.assertTrue(
                runner_team_cleanup.team_auth_is_capacity_cooled(
                    shared_root=shared_root,
                    team_auth_path=team_auth_path,
                )
            )
            runner_team_cleanup.clear_team_auth_capacity_cooldown(
                shared_root=shared_root,
                team_auth_path=team_auth_path,
            )
            self.assertFalse(
                runner_team_cleanup.team_auth_is_capacity_cooled(
                    shared_root=shared_root,
                    team_auth_path=team_auth_path,
                )
            )


class RunnerTeamAuthTests(unittest.TestCase):
    def test_temp_blacklist_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            source_path = shared_root / "mother.json"
            source_path.parent.mkdir(parents=True, exist_ok=True)
            source_path.write_text(
                '{"email":"mother@example.com","account_id":"acct_123"}',
                encoding="utf-8",
            )
            team_auth_path = str(source_path)
            identity = {
                "original_name": "mother.json",
                "email": "mother@example.com",
                "account_id": "acct_123",
            }
            record = runner_team_auth.mark_team_auth_temporary_blacklist(
                shared_root=shared_root,
                team_auth_path=team_auth_path,
                identity=identity,
                reason="token invalidated",
                blacklist_seconds=120.0,
                worker_label="worker-01",
                task_index=1,
            )
            self.assertIsNotNone(record)
            blacklisted, _ = runner_team_auth.team_auth_is_temp_blacklisted(
                shared_root=shared_root,
                team_auth_path=team_auth_path,
            )
            self.assertTrue(blacklisted)
            self.assertTrue(
                runner_team_auth.clear_team_auth_temporary_blacklist(
                    shared_root=shared_root,
                    team_auth_path=team_auth_path,
                    identity=identity,
                    worker_label="worker-01",
                    task_index=1,
                )
            )
            blacklisted, _ = runner_team_auth.team_auth_is_temp_blacklisted(
                shared_root=shared_root,
                team_auth_path=team_auth_path,
            )
            self.assertFalse(blacklisted)

    def test_release_reservation_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            source_path = shared_root / "mother.json"
            source_path.parent.mkdir(parents=True, exist_ok=True)
            source_path.write_text(
                '{"email":"mother@example.com","account_id":"acct_123"}',
                encoding="utf-8",
            )
            team_auth_path = str(source_path)
            reserved, reservation, summary = runner_team_auth.try_reserve_required_team_auth_seats(
                shared_root=shared_root,
                team_auth_path=team_auth_path,
                required_codex_seats=1,
                required_chatgpt_seats=0,
                reservation_owner="worker-01",
                reservation_context="main:1",
                source_role="main",
            )
            self.assertTrue(reserved)
            self.assertIsNotNone(reservation)
            self.assertIsInstance(summary, dict)
            released = runner_team_auth.release_team_auth_seat_reservations(
                shared_root=shared_root,
                reservation=reservation,
            )
            self.assertIsNotNone(released)


class RunnerWorkerMaintenanceTests(unittest.TestCase):
    def test_resolve_worker_team_auth_falls_back_when_pinned_path_is_reserved(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            pinned_path = tmp_path / "pinned.json"
            pinned_path.write_text("{}", encoding="utf-8")
            with mock.patch.object(
                runner_worker_maintenance,
                "_resolve_team_auth_pool",
                return_value=[str(pinned_path), "fallback.json"],
            ), mock.patch.object(
                runner_worker_maintenance,
                "_prune_stale_team_auth_caches",
                return_value={},
            ), mock.patch.object(
                runner_worker_maintenance,
                "_team_auth_is_reserved_for_team_expand",
                return_value=(True, {"reason": "team-expand"}),
            ), mock.patch.object(
                runner_worker_maintenance,
                "_select_team_auth_path",
                return_value=("fallback.json", {"reservationIds": ["r1"]}),
            ) as select_team_auth_path:
                selection = runner_worker_maintenance.resolve_worker_team_auth(
                    normalized_role="main",
                    shared_root=tmp_path / "shared",
                    output_root=tmp_path / "output",
                    worker_label="worker-01",
                    task_index=1,
                    pinned_team_auth_path=str(pinned_path),
                )
        self.assertEqual([str(pinned_path), "fallback.json"], selection.team_auth_pool)
        self.assertEqual("fallback.json", selection.selected_team_auth_path)
        self.assertEqual({"reservationIds": ["r1"]}, selection.seat_reservation)
        self.assertEqual(
            [str(pinned_path), "fallback.json"],
            select_team_auth_path.call_args.kwargs["team_auth_pool"],
        )


class RunnerWorkerLoopTests(unittest.TestCase):
    def test_worker_loop_exits_before_flow_selection_when_max_runs_already_reached(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            free_oauth_pool_dir = output_root / "free-oauth-pool"
            spec = RunnerFlowSpec(
                name="continue-openai",
                flow_path="continue-flow.json",
                instance_role="continue",
                weight=1.0,
                team_auth_path="",
                task_max_attempts=3,
                small_success_pool_dir=output_root / "others" / "small-success-continue-pool",
                mailbox_business_key="openai",
            )
            task_counter = SimpleNamespace(value=1, get_lock=lambda: nullcontext())
            worker_state = mock.Mock()
            with mock.patch.object(runner_worker_loop, "WorkerRuntimeState", return_value=worker_state):
                with mock.patch.object(runner_worker_loop, "_process_worker_maintenance") as maintenance:
                    with mock.patch.object(runner_worker_loop, "_choose_runnable_flow_spec") as choose_flow:
                        runner_worker_loop.worker_loop(
                            worker_id=1,
                            instance_id="mixed",
                            instance_role="mixed",
                            output_root_text=str(output_root),
                            delay_seconds=0.0,
                            max_runs=1,
                            task_max_attempts=0,
                            flow_specs=(spec,),
                            stop_event=SimpleNamespace(is_set=lambda: False),
                            task_counter=task_counter,
                            free_oauth_pool_dir_text=str(free_oauth_pool_dir),
                        )
        maintenance.assert_not_called()
        choose_flow.assert_not_called()
        worker_state.exited.assert_called_once_with(local_runs=0)

    def test_worker_loop_runs_selected_flow_spec(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            free_oauth_pool_dir = output_root / "free-oauth-pool"
            flow_pool_dir = output_root / "others" / "small-success-continue-pool"
            spec = RunnerFlowSpec(
                name="continue-openai",
                flow_path="continue-flow.json",
                instance_role="continue",
                weight=1.0,
                team_auth_path="",
                task_max_attempts=3,
                small_success_pool_dir=flow_pool_dir,
                mailbox_business_key="openai",
            )
            dummy_result = SimpleNamespace(
                ok=True,
                to_dict=lambda: {"ok": True, "steps": {}, "outputs": {}},
            )
            worker_state = mock.Mock()
            with mock.patch.object(runner_worker_loop, "WorkerRuntimeState", return_value=worker_state):
                with mock.patch.object(runner_worker_loop, "_process_worker_maintenance"):
                    with mock.patch.object(
                        runner_worker_loop,
                        "_choose_runnable_flow_spec",
                        return_value=(spec, {"selected": {"name": "continue-openai"}}),
                    ):
                        with mock.patch.object(runner_worker_loop, "claim_task_index", side_effect=[1, None]):
                            with mock.patch.object(
                                runner_worker_loop,
                                "_resolve_worker_team_auth",
                                return_value=SimpleNamespace(
                                    team_auth_pool=[],
                                    selected_team_auth_path="",
                                    seat_reservation=None,
                                ),
                            ):
                                with mock.patch.object(runner_worker_loop, "run_dst_flow_once", return_value=dummy_result) as run_once:
                                    with mock.patch.object(runner_worker_loop, "_process_worker_run_result", return_value=0.0):
                                        with mock.patch("others.runner_worker_loop.time.sleep"):
                                            runner_worker_loop.worker_loop(
                                                worker_id=1,
                                                instance_id="mixed",
                                                instance_role="mixed",
                                                output_root_text=str(output_root),
                                                delay_seconds=0.0,
                                                max_runs=1,
                                                task_max_attempts=0,
                                                flow_specs=(spec,),
                                                stop_event=SimpleNamespace(is_set=lambda: False),
                                                task_counter=SimpleNamespace(value=0),
                                                free_oauth_pool_dir_text=str(free_oauth_pool_dir),
                                            )
        run_once.assert_called_once()
        self.assertEqual("continue-flow.json", run_once.call_args.kwargs["flow_path"])
        self.assertEqual(str(flow_pool_dir.resolve()), run_once.call_args.kwargs["small_success_pool_dir"])
        self.assertEqual(3, run_once.call_args.kwargs["task_max_attempts"])
        self.assertEqual("openai", run_once.call_args.kwargs["mailbox_business_key"])


class RunnerWorkerResultsTests(unittest.TestCase):
    def test_process_worker_run_result_passes_result_payload_value_to_team_auth_history(self) -> None:
        result = SimpleNamespace(
            ok=True,
            to_dict=lambda: {"ok": True, "steps": {}, "outputs": {}},
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            shared_root = output_root
            run_output_dir = output_root / "worker-01" / "run-1"
            small_success_pool_dir = output_root / "small-success-pool"
            worker_state = mock.Mock()
            with mock.patch.object(runner_worker_results, "_json_log"), mock.patch.object(
                runner_worker_results,
                "_team_auth_path_from_result_payload",
                return_value="",
            ), mock.patch.object(
                runner_worker_results,
                "_output_dict",
                return_value={},
            ), mock.patch.object(
                runner_worker_results,
                "_record_business_mailbox_domain_outcome",
                return_value=None,
            ), mock.patch.object(
                runner_worker_results,
                "_record_team_auth_recent_invite_result",
            ) as record_invite, mock.patch.object(
                runner_worker_results,
                "_record_team_auth_recent_team_expand_result",
            ) as record_expand, mock.patch.object(
                runner_worker_results,
                "_team_auth_reconcile_seat_state_from_result",
            ), mock.patch.object(
                runner_worker_results,
                "_sync_refreshed_credentials_back_to_sources",
                return_value=[],
            ) as sync_credentials, mock.patch.object(
                runner_worker_results,
                "_free_stop_after_validate_mode",
                return_value=False,
            ), mock.patch.object(
                runner_worker_results,
                "_mailbox_capacity_failure_detail",
                return_value="",
            ), mock.patch.object(
                runner_worker_results,
                "_team_capacity_failure_detail",
                return_value="",
            ), mock.patch.object(
                runner_worker_results,
                "_team_auth_blacklist_reason",
                return_value="",
            ), mock.patch.object(
                runner_worker_results,
                "_postprocess_free_success_artifact",
                return_value={"ok": True, "cleanup_run_output": False},
            ), mock.patch.object(
                runner_worker_results,
                "_extra_failure_cooldown_seconds",
                return_value=0.0,
            ):
                cooldown = runner_worker_results.process_worker_run_result(
                    result=result,
                    started_at="2026-01-01T00:00:00+00:00",
                    run_output_dir=run_output_dir,
                    output_root=output_root,
                    shared_root=shared_root,
                    small_success_pool_dir=small_success_pool_dir,
                    normalized_role="main",
                    worker_label="worker-01",
                    task_index=1,
                    local_run_index=1,
                    worker_state=worker_state,
                    selected_team_auth_path="",
                    free_local_selected=True,
                    team_auth_pool=[],
                )
        self.assertEqual(0.0, cooldown)
        self.assertIn("result_payload_value", record_invite.call_args.kwargs)
        self.assertIn("result_payload_value", record_expand.call_args.kwargs)
        self.assertIn("result_payload_value", sync_credentials.call_args.kwargs)


class RunnerCredentialSyncTests(unittest.TestCase):
    def test_sync_refreshed_credentials_back_to_sources_forwards_payload_to_helpers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            refreshed_path = tmp_path / "refreshed.json"
            restored_source_path = tmp_path / "restored-source.json"
            refreshed_path.write_text("{}", encoding="utf-8")
            restored_source_path.write_text("{}", encoding="utf-8")
            payload = {"outputs": {"obtain-codex-oauth": {"successPath": str(refreshed_path)}}}
            actions = [
                {
                    "kind": "generic_oauth_refresh",
                    "source_path": str(tmp_path / "missing-source.json"),
                    "refreshed_path": str(refreshed_path),
                    "force": True,
                }
            ]
            with mock.patch.object(
                runner_credential_sync,
                "credential_backwrite_actions",
                return_value=actions,
            ) as build_actions, mock.patch.object(
                runner_credential_sync,
                "restored_path_for_source",
                return_value=restored_source_path,
            ) as restored_path, mock.patch.object(
                runner_credential_sync,
                "_load_json_dict",
                side_effect=[{"email": "before@example.com"}, {"email": "after@example.com"}],
            ), mock.patch.object(
                runner_credential_sync,
                "_merge_refreshed_credential",
                return_value={"email": "after@example.com"},
            ), mock.patch.object(
                runner_credential_sync,
                "write_json_atomic",
            ), mock.patch.object(
                runner_credential_sync,
                "json_log",
            ):
                synced = runner_credential_sync.sync_refreshed_credentials_back_to_sources(
                    result_payload_value=payload,
                    worker_label="worker-01",
                    task_index=1,
                )
        self.assertEqual(1, len(synced))
        self.assertIs(build_actions.call_args.args[0], payload)
        self.assertIs(restored_path.call_args.args[0], payload)


if __name__ == "__main__":
    unittest.main()
