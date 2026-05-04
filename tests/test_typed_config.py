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
from others.preflight import validate_runtime_preflight  # noqa: E402
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
                "REGISTER_MAILBOX_TTL_SECONDS": "120",
                "REGISTER_MAILBOX_RECREATE_PREALLOCATED": "true",
                "REGISTER_FREE_STOP_AFTER_VALIDATE": "true",
                "REGISTER_DST_LOGIN_ENTRY_URL": "https://auth.openai.com/log-in-or-create-account",
            },
            clear=True,
        ):
            config = DstTaskEnvConfig.from_env()
        self.assertEqual("7", config.team_pre_fill_count)
        self.assertEqual("3", config.team_member_count)
        self.assertEqual("codex", config.team_workspace_selector)
        self.assertEqual("90", config.free_oauth_delay_seconds)
        self.assertEqual("120", config.mailbox_ttl_seconds)
        self.assertTrue(config.mailbox_recreate_preallocated)
        self.assertTrue(config.free_stop_after_validate)
        self.assertEqual("https://auth.openai.com/log-in-or-create-account", config.login_entry_url)

    def test_runner_main_config_resolves_paths_and_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "REGISTER_LOOP_DELAY_SECONDS": "1.5",
                    "REGISTER_WORKER_COUNT": "4",
                    "REGISTER_MAIN_CONCURRENCY_LIMIT": "5",
                    "REGISTER_CONTINUE_CONCURRENCY_LIMIT": "2",
                    "REGISTER_TEAM_CONCURRENCY_LIMIT": "1",
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
            self.assertEqual(config.shared_root / "openai" / "pending", config.openai_oauth_pool_dir)
            self.assertEqual(config.shared_root / "codex" / "free", config.free_oauth_pool_dir)
            self.assertEqual(2, config.flow_specs[0].concurrency_limit)

    def test_runner_main_config_parses_mixed_flow_specs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            flow_main = Path(tmp_dir) / "main-flow.json"
            flow_continue = Path(tmp_dir) / "continue-flow.json"
            flow_main.write_text("{}", encoding="utf-8")
            flow_continue.write_text("{}", encoding="utf-8")
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "REGISTER_INSTANCE_ID": "mixed",
                    "REGISTER_MAIN_CONCURRENCY_LIMIT": "5",
                    "REGISTER_CONTINUE_CONCURRENCY_LIMIT": "2",
                    "REGISTER_FLOW_SPECS_JSON": (
                        "["
                        "{\"name\":\"main-openai\",\"path\":\"" + str(flow_main).replace("\\", "\\\\") + "\",\"role\":\"main\",\"weight\":3},"
                        "{\"name\":\"continue-openai\",\"path\":\"" + str(flow_continue).replace("\\", "\\\\") + "\",\"role\":\"continue\",\"weight\":1}"
                        "]"
                    ),
                },
                clear=True,
            ):
                config = RunnerMainConfig.from_env()
        self.assertEqual(2, len(config.flow_specs))
        self.assertEqual("main-openai", config.flow_specs[0].name)
        self.assertEqual("main", config.flow_specs[0].instance_role)
        self.assertEqual(5, config.flow_specs[0].concurrency_limit)
        self.assertEqual("continue", config.flow_specs[1].instance_role)
        self.assertEqual(2, config.flow_specs[1].concurrency_limit)
        self.assertEqual(
            config.shared_root / "openai" / "failed-once",
            config.flow_specs[1].openai_oauth_pool_dir,
        )
        self.assertEqual(str(flow_main.resolve()), config.flow_path)

    def test_runner_main_config_parses_relaxed_flow_specs_from_docker_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            flow_main = Path(tmp_dir) / "main-flow.json"
            flow_main.write_text("{}", encoding="utf-8")
            relaxed_specs = (
                "["
                "{name:openai-main,path:" + str(flow_main.resolve()).replace("\\", "/") + ",role:main,weight:100,mailboxBusinessKey:openai}"
                "]"
            )
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "REGISTER_INSTANCE_ID": "mixed",
                    "REGISTER_INSTANCE_ROLE": "mixed",
                    "REGISTER_MAIN_CONCURRENCY_LIMIT": "5",
                    "REGISTER_FLOW_SPECS_JSON": relaxed_specs,
                },
                clear=True,
            ):
                config = RunnerMainConfig.from_env()
        self.assertEqual(1, len(config.flow_specs))
        self.assertEqual("openai-main", config.flow_specs[0].name)
        self.assertEqual("main", config.flow_specs[0].instance_role)
        self.assertEqual(5, config.flow_specs[0].concurrency_limit)
        self.assertEqual(flow_main.resolve(), Path(config.flow_specs[0].flow_path).resolve())
        self.assertEqual(flow_main.resolve(), Path(config.flow_path).resolve())

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
                default_runtime_host="easy-proxy",
                default_mode="auto",
                running_in_docker=True,
            )
        self.assertFalse(config.enabled)
        self.assertFalse(config.required_by_default)
        self.assertEqual("random-node", config.mode)
        self.assertEqual("http://manager:9888", config.management_base_url)
        self.assertEqual(5, config.unique_attempts)
        self.assertEqual("easy-proxy", config.runtime_host)

    def test_mailbox_runtime_config_supports_fallback_env_names_and_percent_semantics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "domain-state.json"
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_MAILBOX_BUSINESS_KEY": "openai-signup",
                    "REGISTER_MAILBOX_DOMAIN_BLACKLIST": "coolkid.icu, cksa.eu.cc",
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
                    default_consecutive_failure_blacklist_threshold=500,
                )
        self.assertEqual(("m2u", "moemail"), config.providers)
        self.assertEqual("fast-lane", config.strategy_mode_id)
        self.assertEqual("stable", config.routing_profile_id)
        self.assertEqual("openai-signup", config.business_key)
        self.assertEqual(state_path.resolve(), config.domain_state_path)
        self.assertEqual(("coolkid.icu", "cksa.eu.cc"), config.explicit_blacklist_domains)
        self.assertEqual(90.0, config.blacklist_failure_rate_percent)

    def test_mailbox_runtime_config_parses_business_policy_map(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "domain-state.json"
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_MAILBOX_BUSINESS_KEY": "generic",
                    "REGISTER_MAILBOX_DOMAIN_POOL": "fallback.test",
                    "REGISTER_MAILBOX_DOMAIN_BLACKLIST": "fallback-black.test",
                    "REGISTER_MAILBOX_PROVIDER_BLACKLIST": "m2u, tempmail.lol",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": (
                        '{"openai":{"domainPool":["zhooo.org","cnmlgb.de"],'
                        '"explicitBlacklistDomains":["coolkid.icu"],'
                        '"providerBlacklist":["moemail"]}}'
                    ),
                },
                clear=True,
            ):
                config = MailboxRuntimeConfig.from_env(
                    default_ttl_seconds=90,
                    default_state_path=state_path,
                    default_business_domain_pool=("a.test", "b.test"),
                    default_blacklist_min_attempts=20,
                    default_blacklist_failure_rate=90.0,
                    default_consecutive_failure_blacklist_threshold=500,
                )
        fallback_policy = config.resolve_business_policy()
        openai_policy = config.resolve_business_policy("openai")
        self.assertEqual(("fallback.test",), fallback_policy.domain_pool)
        self.assertEqual(("fallback-black.test",), fallback_policy.explicit_blacklist_domains)
        self.assertEqual(("m2u", "tempmail-lol"), fallback_policy.explicit_blacklist_providers)
        self.assertEqual(("zhooo.org", "cnmlgb.de"), openai_policy.domain_pool)
        self.assertEqual(("coolkid.icu",), openai_policy.explicit_blacklist_domains)
        self.assertEqual(("moemail",), openai_policy.explicit_blacklist_providers)

    def test_mailbox_runtime_config_parses_relaxed_business_policy_map(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "domain-state.json"
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_MAILBOX_BUSINESS_KEY": "generic",
                    "REGISTER_MAILBOX_DOMAIN_POOL": "fallback.test",
                    "REGISTER_MAILBOX_DOMAIN_BLACKLIST": "fallback-black.test",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": (
                        "{openai:{domainPool:[cnmlgb.de,zhooo.org,shaole.me,cpu.edu.kg,tmail.bio,do4.tech],"
                        "explicitBlacklistDomains:[coolkid.icu,shaole.me,cpu.edu.kg,tmail.bio,do4.tech],"
                        "providerBlacklist:[m2u,tempmail.lol]}}"
                    ),
                },
                clear=True,
            ):
                config = MailboxRuntimeConfig.from_env(
                    default_ttl_seconds=90,
                    default_state_path=state_path,
                    default_business_domain_pool=("a.test", "b.test"),
                    default_blacklist_min_attempts=20,
                    default_blacklist_failure_rate=90.0,
                    default_consecutive_failure_blacklist_threshold=500,
                )
        openai_policy = config.resolve_business_policy("openai")
        self.assertEqual(
            ("cnmlgb.de", "zhooo.org", "shaole.me", "cpu.edu.kg", "tmail.bio", "do4.tech"),
            openai_policy.domain_pool,
        )
        self.assertEqual(
            ("coolkid.icu", "shaole.me", "cpu.edu.kg", "tmail.bio", "do4.tech"),
            openai_policy.explicit_blacklist_domains,
        )
        self.assertEqual(("m2u", "tempmail-lol"), openai_policy.explicit_blacklist_providers)

    def test_mailbox_runtime_config_uses_default_policy_for_unmapped_business(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "domain-state.json"
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_MAILBOX_BUSINESS_KEY": "generic",
                    "REGISTER_MAILBOX_DOMAIN_POOL": "fallback.test",
                    "REGISTER_MAILBOX_DOMAIN_BLACKLIST": "fallback-black.test",
                    "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": (
                        '{"default":{"domainPool":["cnmlgb.de","zhooo.org"],'
                        '"explicitBlacklistDomains":["coolkid.icu","shaole.me"],'
                        '"providerBlacklist":["m2u"]},'
                        '"openai":{"domainPool":["cnmlgb.de","zhooo.org"],'
                        '"explicitBlacklistDomains":["coolkid.icu","shaole.me"]}}'
                    ),
                },
                clear=True,
            ):
                config = MailboxRuntimeConfig.from_env(
                    default_ttl_seconds=90,
                    default_state_path=state_path,
                    default_business_domain_pool=("a.test", "b.test"),
                    default_blacklist_min_attempts=20,
                    default_blacklist_failure_rate=90.0,
                    default_consecutive_failure_blacklist_threshold=500,
                )
        other_policy = config.resolve_business_policy("codex-team")
        self.assertEqual(("cnmlgb.de", "zhooo.org"), other_policy.domain_pool)
        self.assertEqual(("coolkid.icu", "shaole.me"), other_policy.explicit_blacklist_domains)
        self.assertEqual(("m2u",), other_policy.explicit_blacklist_providers)
        self.assertEqual("codex-team", other_policy.business_key)

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
                    "REGISTER_TEAM_STALE_CLAIM_SECONDS": "77",
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
        self.assertEqual(77, config.stale_claim_seconds)
        self.assertEqual(7200.0, config.temp_blacklist_seconds)
        self.assertEqual(resolve_shared_root(str(output_root)) / "codex" / "team-mother-input", config.mother_pool_dir)
        self.assertEqual(str(resolve_shared_root(str(output_root)) / "codex" / "team-input"), config.auth_local_dir)
        self.assertEqual(str(resolve_shared_root(str(output_root)) / "codex" / "team-input"), config.auth_default_dir)

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
        self.assertEqual(shared_root / "codex" / "free", config.free_local_dir)
        self.assertEqual(shared_root / "codex" / "team", config.team_local_dir)
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

    def test_runtime_preflight_rejects_partial_r2_configuration(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "REGISTER_R2_BUCKET": "artifacts",
                },
                clear=True,
            ):
                with self.assertRaisesRegex(RuntimeError, "incomplete_r2_config"):
                    validate_runtime_preflight()

    def test_runtime_preflight_accepts_minimal_local_configuration(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            flow_path = Path(tmp_dir) / "flow.json"
            flow_path.write_text(
                '{"definition":{"steps":[{"id":"acquire-mailbox","type":"acquire_mailbox","metadata":{"owner":"easyemail"}}]}}',
                encoding="utf-8",
            )
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "REGISTER_FLOW_PATH": str(flow_path),
                },
                clear=True,
            ):
                summary = validate_runtime_preflight()
        self.assertEqual(str(output_root.resolve()), summary["outputRoot"])
        self.assertEqual(str(flow_path.resolve()), summary["flowPath"])

    def test_runtime_preflight_accepts_mixed_flow_specs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            flow_main = Path(tmp_dir) / "main-flow.json"
            flow_continue = Path(tmp_dir) / "continue-flow.json"
            for flow_path in (flow_main, flow_continue):
                flow_path.write_text(
                    '{"definition":{"steps":[{"id":"acquire-mailbox","type":"acquire_mailbox","metadata":{"owner":"easyemail"}}]}}',
                    encoding="utf-8",
                )
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "REGISTER_FLOW_SPECS_JSON": (
                        "["
                        "{\"name\":\"main-openai\",\"path\":\"" + str(flow_main).replace("\\", "\\\\") + "\",\"role\":\"main\"},"
                        "{\"name\":\"continue-openai\",\"path\":\"" + str(flow_continue).replace("\\", "\\\\") + "\",\"role\":\"continue\"}"
                        "]"
                    ),
                },
                clear=True,
            ):
                summary = validate_runtime_preflight()
        self.assertEqual(2, len(summary["flowSpecs"]))
        self.assertEqual(str(flow_main.resolve()), summary["flowSpecs"][0]["flowPath"])


if __name__ == "__main__":
    unittest.main()
