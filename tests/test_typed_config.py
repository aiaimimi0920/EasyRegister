from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SRC_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from others.config import (  # noqa: E402
    ArtifactRoutingConfig,
    CleanupRuntimeConfig,
    DashboardSettings,
    DstTaskEnvConfig,
    MailboxRuntimeConfig,
    ProxyRuntimeConfig,
    RunnerMainConfig,
    TeamAuthRuntimeConfig,
    env_percent_value,
    env_ratio,
)
from others.paths import resolve_shared_root  # noqa: E402


class TypedConfigTests(unittest.TestCase):
    def test_dashboard_settings_reads_typed_values(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "REGISTER_DASHBOARD_ENABLED": "true",
                "REGISTER_DASHBOARD_LISTEN": "0.0.0.0:9999",
                "REGISTER_DASHBOARD_ALLOW_REMOTE": "yes",
                "REGISTER_DASHBOARD_RECENT_WINDOW_SECONDS": "1200",
            },
            clear=True,
        ):
            settings = DashboardSettings.from_env()
        self.assertTrue(settings.enabled)
        self.assertEqual("0.0.0.0:9999", settings.listen)
        self.assertTrue(settings.allow_remote)
        self.assertEqual(1200, settings.recent_window_seconds)

    def test_dst_task_env_config_preserves_string_inputs(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "REGISTER_TEAM_PRE_FILL_COUNT": "7",
                "REGISTER_TEAM_MEMBER_COUNT": "3",
                "REGISTER_TEAM_WORKSPACE_SELECTOR": "codex",
                "REGISTER_FREE_WORKSPACE_SELECTOR": "personal",
                "REGISTER_FREE_OAUTH_DELAY_SECONDS": "90",
                "REGISTER_FREE_STOP_AFTER_VALIDATE": "true",
            },
            clear=True,
        ):
            config = DstTaskEnvConfig.from_env()
        self.assertEqual("7", config.team_pre_fill_count)
        self.assertEqual("3", config.team_member_count)
        self.assertEqual("codex", config.team_workspace_selector)
        self.assertEqual("90", config.free_oauth_delay_seconds)
        self.assertTrue(config.free_stop_after_validate)

    def test_runner_main_config_resolves_paths_and_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "REGISTER_LOOP_DELAY_SECONDS": "1.5",
                    "REGISTER_WORKER_COUNT": "4",
                    "REGISTER_INSTANCE_ID": "continue",
                    "EASY_PROTOCOL_BASE_URL": "http://control:9788",
                },
                clear=True,
            ):
                config = RunnerMainConfig.from_env()
            self.assertEqual(output_root.resolve(), config.output_root)
            self.assertEqual(resolve_shared_root(str(output_root)), config.shared_root)
            self.assertEqual(1.5, config.delay_seconds)
            self.assertEqual(4, config.worker_count)
            self.assertEqual("continue", config.instance_id)
            self.assertEqual("continue", config.instance_role)
            self.assertEqual("http://control:9788", config.easy_protocol_base_url)
            self.assertEqual(config.shared_root / "small-success-pool", config.small_success_pool_dir)
            self.assertEqual(config.shared_root / "free-oauth-pool", config.free_oauth_pool_dir)

    def test_proxy_runtime_config_normalizes_mode_and_fallbacks(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "REGISTER_ENABLE_EASY_PROXY": "false",
                "REGISTER_REQUIRE_EASY_PROXY": "false",
                "REGISTER_PROXY_MODE": "random_node",
                "REGISTER_PROXY_UNIQUE_ATTEMPTS": "5",
                "EASY_PROXY_MANAGEMENT_URL": "http://manager:9888",
            },
            clear=True,
        ):
            config = ProxyRuntimeConfig.from_env(
                default_management_base_url="http://default:9888",
                default_ttl_minutes=30,
                default_runtime_host="easy-proxy-service",
                default_mode="auto",
                running_in_docker=True,
            )
        self.assertFalse(config.enabled)
        self.assertFalse(config.required_by_default)
        self.assertEqual("random-node", config.mode)
        self.assertEqual("http://manager:9888", config.management_base_url)
        self.assertEqual(5, config.unique_attempts)
        self.assertEqual("easy-proxy-service", config.runtime_host)

    def test_mailbox_runtime_config_supports_fallback_env_names_and_percent_semantics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "domain-state.json"
            with mock.patch.dict(
                os.environ,
                {
                    "MAILBOX_PROVIDER_CANDIDATES": "m2u, moemail, m2u",
                    "MAILBOX_PROVIDER_STRATEGY_MODE_ID": "fast-lane",
                    "MAILBOX_PROVIDER_ROUTING_PROFILE_ID": "stable",
                    "REGISTER_MAILBOX_DOMAIN_BLACKLIST_FAILURE_RATE": "0.9",
                },
                clear=True,
            ):
                config = MailboxRuntimeConfig.from_env(
                    default_ttl_seconds=90,
                    default_state_path=state_path,
                    default_business_domain_pool=("a.test", "b.test"),
                    default_blacklist_min_attempts=20,
                    default_blacklist_failure_rate=90.0,
                )
        self.assertEqual(("m2u", "moemail"), config.providers)
        self.assertEqual("fast-lane", config.strategy_mode_id)
        self.assertEqual("stable", config.routing_profile_id)
        self.assertEqual(state_path.resolve(), config.domain_state_path)
        self.assertEqual(90.0, config.blacklist_failure_rate_percent)

    def test_team_auth_runtime_config_normalizes_seat_limits_and_weights(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "REGISTER_TEAM_AUTH_PATHS": os.pathsep.join(["A.json", "B.json", "A.json"]),
                    "REGISTER_TEAM_AUTH_DIRS": os.pathsep.join(["X", "Y"]),
                    "REGISTER_TEAM_AUTH_SALL_CC_WEIGHT": "25",
                    "REGISTER_TEAM_TOTAL_SEAT_LIMIT": "8",
                    "REGISTER_TEAM_CHATGPT_SEAT_LIMIT": "9",
                    "REGISTER_TEAM_CODEX_SEAT_LIMIT": "6",
                    "REGISTER_TEAM_CODEX_SEAT_TYPES": "codex,usage_based",
                    "REGISTER_TEAM_AUTH_TEMP_BLACKLIST_SECONDS": "7200",
                },
                clear=True,
            ):
                config = TeamAuthRuntimeConfig.from_env(output_root=output_root)
        self.assertEqual(("A.json", "B.json"), config.auth_paths)
        self.assertEqual(("X", "Y"), config.auth_dirs)
        self.assertAlmostEqual(0.25, config.sall_cc_weight)
        self.assertEqual(8, config.total_seat_limit)
        self.assertEqual(8, config.chatgpt_seat_limit)
        self.assertEqual(6, config.codex_seat_limit)
        self.assertEqual(("codex", "usage_based"), config.codex_seat_types)
        self.assertEqual(7200.0, config.temp_blacklist_seconds)
        self.assertEqual(resolve_shared_root(str(output_root)) / "team-mother-pool", config.mother_pool_dir)

    def test_artifact_routing_config_resolves_paths_and_upload_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "REGISTER_FREE_LOCAL_SPLIT_PERCENT": "0.8",
                    "REGISTER_TEAM_LOCAL_SPLIT_PERCENT": "25",
                    "REGISTER_R2_BUCKET": "artifacts",
                    "R2_REGION": "auto",
                },
                clear=True,
            ):
                config = ArtifactRoutingConfig.from_env(output_root=output_root)
        shared_root = resolve_shared_root(str(output_root))
        self.assertEqual(shared_root / "others" / "free-local-store", config.free_local_dir)
        self.assertEqual(shared_root / "others" / "team-local-store", config.team_local_dir)
        self.assertEqual(80.0, config.free_local_split_percent)
        self.assertEqual(25.0, config.team_local_split_percent)
        self.assertEqual("artifacts", config.r2_bucket)
        self.assertEqual("auto", config.r2_region)

    def test_cleanup_runtime_config_clamps_numeric_fields(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "REGISTER_MAILBOX_CLEANUP_MAX_DELETE_COUNT": "0",
                "REGISTER_MAILBOX_CLEANUP_FAILURE_THRESHOLD": "-4",
                "REGISTER_CRASH_COOLDOWN_SECONDS": "12",
            },
            clear=True,
        ):
            config = CleanupRuntimeConfig.from_env()
        self.assertEqual(1, config.mailbox_cleanup_max_delete_count)
        self.assertEqual(1, config.mailbox_cleanup_failure_threshold)
        self.assertEqual(12.0, config.crash_cooldown_seconds)

    def test_ratio_and_percent_helpers_keep_expected_ranges(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "RATIO_VALUE": "75",
                "PERCENT_VALUE": "0.75",
            },
            clear=True,
        ):
            self.assertAlmostEqual(0.75, env_ratio("RATIO_VALUE"))
            self.assertAlmostEqual(75.0, env_percent_value("PERCENT_VALUE"))


if __name__ == "__main__":
    unittest.main()
