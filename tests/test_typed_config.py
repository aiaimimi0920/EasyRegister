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
    SmsRuntimeConfig,
    env_percent_value,
    env_ratio,
)
from others import preflight, runtime_proxy_env  # noqa: E402
from others.preflight import validate_runtime_preflight  # noqa: E402
from others.paths import resolve_shared_root  # noqa: E402


class TypedConfigTests(unittest.TestCase):
    def test_preflight_default_mailbox_ttl_covers_full_openai_main_flow(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(Path(tmp_dir) / "others" / "mixed-runs"),
                    "REGISTER_FLOW_PATH": "",
                    "REGISTER_FLOW_SPECS_JSON": "[]",
                    "REGISTER_TEAM_AUTH_PATH": "",
                    "REGISTER_OPENAI_UPLOAD_PERCENT": "0",
                    "REGISTER_CODEX_FREE_UPLOAD_PERCENT": "0",
                    "REGISTER_CODEX_TEAM_UPLOAD_PERCENT": "0",
                    "REGISTER_CODEX_PLUS_UPLOAD_PERCENT": "0",
                },
                clear=True,
            ):
                preflight = validate_runtime_preflight()

        self.assertGreaterEqual(preflight["mailbox"]["ttlSeconds"], 1800)

    def test_cleanup_runtime_config_parses_sms_no_selection_cooldown(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"REGISTER_SMS_NO_SELECTION_COOLDOWN_SECONDS": "123"},
            clear=True,
        ):
            config = CleanupRuntimeConfig.from_env()
        self.assertEqual(123.0, config.sms_no_selection_cooldown_seconds)

    def test_cleanup_runtime_config_parses_oauth_specific_cooldowns(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "REGISTER_OAUTH_RATE_LIMIT_COOLDOWN_SECONDS": "301",
                "REGISTER_OAUTH_BLOCKED_COOLDOWN_SECONDS": "302",
                "REGISTER_OAUTH_MISSING_SESSION_COOLDOWN_SECONDS": "303",
            },
            clear=True,
        ):
            config = CleanupRuntimeConfig.from_env()

        self.assertEqual(301.0, config.oauth_rate_limit_cooldown_seconds)
        self.assertEqual(302.0, config.oauth_blocked_cooldown_seconds)
        self.assertEqual(303.0, config.oauth_missing_session_cooldown_seconds)

    def test_sms_runtime_config_parses_default_and_openai_business_policies(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "REGISTER_SMS_BUSINESS_KEY": "openai",
                "REGISTER_SMS_PROVIDER_BLACKLIST": "hero_sms",
                "REGISTER_SMS_ALLOW_PAID": "false",
                "REGISTER_SMS_BUSINESS_POLICIES_JSON": (
                    '{"default":{"enabled":false,"providerBlacklist":["hero_sms"],"allowPaid":false},'
                    '"openai":{"enabled":true,"providerBlacklist":["hero_sms","paid_backup"],'
                    '"allowPaid":false,"allowReuse":false,"maxBindingsPerPhone":1,'
                    '"countryCodes":[],"countryId":2,"selectionMode":"balanced","maxPrice":0.605}}'
                ),
            },
            clear=False,
        ):
            config = SmsRuntimeConfig.from_env(default_state_path=Path("C:/tmp/register-sms-state.json"))

        policy = config.resolve_business_policy("openai")
        self.assertTrue(policy.enabled)
        self.assertEqual(("hero_sms", "paid_backup"), policy.explicit_blacklist_providers)
        self.assertFalse(policy.allow_paid)
        self.assertFalse(policy.allow_reuse)
        self.assertEqual(1, policy.max_bindings_per_phone)
        self.assertEqual((), policy.country_codes)
        self.assertEqual(2, policy.country_id)
        self.assertEqual("balanced", policy.selection_mode)
        self.assertEqual(0.605, policy.max_price)

    def test_sms_runtime_config_falls_back_to_default_policy(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "REGISTER_SMS_BUSINESS_KEY": "generic",
                "REGISTER_SMS_PROVIDER_BLACKLIST": "hero_sms",
                "REGISTER_SMS_ALLOW_PAID": "false",
                "REGISTER_SMS_BUSINESS_POLICIES_JSON": (
                    '{"default":{"enabled":true,"providerBlacklist":["hero_sms"],'
                    '"allowPaid":false,"allowReuse":true,"maxBindingsPerPhone":2,'
                    '"countryCodes":["CA"],"selectionMode":"balanced","maxPrice":0.7}}'
                ),
            },
            clear=False,
        ):
            config = SmsRuntimeConfig.from_env(default_state_path=Path("C:/tmp/register-sms-state.json"))

        policy = config.resolve_business_policy("openai")
        self.assertTrue(policy.enabled)
        self.assertEqual("openai", policy.business_key)
        self.assertEqual(("hero_sms",), policy.explicit_blacklist_providers)
        self.assertFalse(policy.allow_paid)
        self.assertTrue(policy.allow_reuse)
        self.assertEqual(2, policy.max_bindings_per_phone)
        self.assertEqual(("ca",), policy.country_codes)
        self.assertEqual("balanced", policy.selection_mode)
        self.assertEqual(0.7, policy.max_price)

    def test_sms_runtime_config_empty_business_country_codes_inherit_global_countries(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "REGISTER_SMS_BUSINESS_KEY": "openai",
                "REGISTER_SMS_COUNTRY_CODES": "+44,+358,+1",
                "REGISTER_SMS_COUNTRY_ID": "16",
                "REGISTER_SMS_MAX_PRICE": "0.605",
                "REGISTER_SMS_BUSINESS_POLICIES_JSON": (
                    '{"default":{"enabled":false,"providerBlacklist":["hero_sms"],"allowPaid":false},'
                    '"openai":{"enabled":true,"providerBlacklist":["hero_sms"],'
                    '"allowPaid":false,"allowReuse":false,"maxBindingsPerPhone":1,'
                    '"countryCodes":[],"selectionMode":"balanced"}}'
                ),
            },
            clear=True,
        ):
            config = SmsRuntimeConfig.from_env(default_state_path=Path("C:/tmp/register-sms-state.json"))

        policy = config.resolve_business_policy("openai")
        self.assertEqual(("+44", "+358", "+1"), policy.country_codes)
        self.assertEqual(16, policy.country_id)
        self.assertEqual(0.605, policy.max_price)

    def test_sms_runtime_config_ignores_invalid_selection_mode(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "REGISTER_SMS_SELECTION_MODE": "available-first",
                "REGISTER_SMS_BUSINESS_POLICIES_JSON": (
                    '{"openai":{"enabled":true,"providerBlacklist":["hero_sms"],"selectionMode":"available-first"}}'
                ),
            },
            clear=False,
        ):
            config = SmsRuntimeConfig.from_env(default_state_path=Path("C:/tmp/register-sms-state.json"))

        self.assertEqual("", config.selection_mode)
        self.assertEqual("", config.resolve_business_policy("openai").selection_mode)

    def test_sms_runtime_config_defaults_allow_paid_when_not_overridden(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "REGISTER_SMS_BUSINESS_KEY": "openai",
            },
            clear=True,
        ):
            config = SmsRuntimeConfig.from_env(default_state_path=Path("C:/tmp/register-sms-state.json"))

        policy = config.resolve_business_policy("openai")
        self.assertTrue(config.allow_paid)
        self.assertTrue(policy.allow_paid)

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

    def test_runner_main_config_uses_standard_mixed_flows_when_flow_specs_are_truncated(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "REGISTER_INSTANCE_ID": "easy-register",
                    "REGISTER_INSTANCE_ROLE": "mixed",
                    "REGISTER_MAIN_CONCURRENCY_LIMIT": "5",
                    "REGISTER_CONTINUE_CONCURRENCY_LIMIT": "2",
                    "REGISTER_TEAM_CONCURRENCY_LIMIT": "1",
                    "REGISTER_FLOW_PATH": "",
                    "REGISTER_FLOW_SPECS_JSON": "[",
                },
                clear=True,
            ):
                config = RunnerMainConfig.from_env()

        self.assertEqual(
            ["openai-main", "openai-continue", "openai-account-availability-audit", "codex-team-expand"],
            [spec.name for spec in config.flow_specs],
        )
        self.assertEqual(
            ["main", "continue", "account-audit", "team"],
            [spec.instance_role for spec in config.flow_specs],
        )
        self.assertEqual(
            output_root / "openai" / "failed-once",
            config.flow_specs[1].openai_oauth_pool_dir,
        )
        self.assertEqual(5, config.flow_specs[0].concurrency_limit)
        self.assertEqual(2, config.flow_specs[1].concurrency_limit)
        self.assertEqual(1, config.flow_specs[2].concurrency_limit)
        self.assertEqual(1, config.flow_specs[3].concurrency_limit)
        self.assertEqual(
            "openai-account-availability-audit-v1.semantic-flow.json",
            Path(config.flow_specs[2].flow_path).name,
        )
        self.assertEqual(str(output_root.resolve()), config.flow_specs[2].input_source_dir)
        self.assertEqual(
            "codex-openai-account-v1.semantic-flow.json",
            Path(config.flow_specs[0].flow_path).name,
        )

    def test_runner_main_config_mixed_account_audit_scans_shared_root_when_output_root_is_runs_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "register-output"
            output_root = shared_root / "others" / "mixed-runs"
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "REGISTER_INSTANCE_ID": "easy-register",
                    "REGISTER_INSTANCE_ROLE": "mixed",
                    "REGISTER_FLOW_PATH": "",
                    "REGISTER_FLOW_SPECS_JSON": "[",
                },
                clear=True,
            ):
                config = RunnerMainConfig.from_env()

        self.assertEqual("account-audit", config.flow_specs[2].instance_role)
        self.assertEqual(str(shared_root.resolve()), config.flow_specs[2].input_source_dir)

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

    def test_easy_proxy_defaults_use_current_management_port_and_lease_api(self) -> None:
        self.assertEqual("http://localhost:29888", runtime_proxy_env.DEFAULT_EASY_PROXY_BASE_URL_HOST)
        self.assertEqual("http://localhost:29888", preflight.DEFAULT_EASY_PROXY_BASE_URL_HOST)
        self.assertEqual("127.0.0.1", runtime_proxy_env.DEFAULT_EASY_PROXY_RUNTIME_HOST_HOST)
        self.assertEqual("127.0.0.1", preflight.DEFAULT_EASY_PROXY_RUNTIME_HOST_HOST)
        self.assertEqual("lease", runtime_proxy_env.DEFAULT_EASY_PROXY_MODE)
        self.assertEqual("lease", preflight.DEFAULT_EASY_PROXY_MODE)

    def test_easy_proxy_host_mode_uses_loopback_runtime_host(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True), \
            mock.patch.object(runtime_proxy_env, "_running_in_docker", return_value=False), \
            mock.patch.object(preflight, "_running_in_docker", return_value=False):
            runtime_config = runtime_proxy_env.proxy_runtime_config()
            preflight_config = preflight._proxy_runtime_config()

        self.assertEqual("127.0.0.1", runtime_config.runtime_host)
        self.assertEqual("127.0.0.1", preflight_config.runtime_host)

    def test_proxy_runtime_config_preserves_auto_mode_for_lease_fallback(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"REGISTER_PROXY_MODE": "auto"},
            clear=True,
        ):
            config = ProxyRuntimeConfig.from_env(
                default_management_base_url="http://localhost:29888",
                default_ttl_minutes=30,
                default_runtime_host="easy-proxy",
                default_mode="lease",
                running_in_docker=True,
            )

        self.assertEqual("auto", config.mode)

    def test_proxy_runtime_config_preserves_static_mode(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"REGISTER_PROXY_MODE": "static"},
            clear=True,
        ):
            config = ProxyRuntimeConfig.from_env(
                default_management_base_url="http://localhost:29888",
                default_ttl_minutes=30,
                default_runtime_host="easy-proxy",
                default_mode="lease",
                running_in_docker=True,
            )

        self.assertEqual("static", config.mode)

    def test_proxy_runtime_config_prefers_management_password_over_legacy_api_key(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "EASY_PROXY_MANAGEMENT_PASSWORD": "current-password",
                "EASY_PROXY_API_KEY": "legacy-password",
            },
            clear=True,
        ):
            config = ProxyRuntimeConfig.from_env(
                default_management_base_url="http://localhost:29888",
                default_ttl_minutes=30,
                default_runtime_host="easy-proxy",
                default_mode="lease",
                running_in_docker=False,
            )

        self.assertEqual("current-password", config.api_key)
        self.assertEqual("lease", config.mode)

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
        self.assertEqual("stable", config.routing_profile_id)

    def test_mailbox_runtime_config_defaults_to_empty_routing_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "domain-state.json"
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_MAILBOX_BUSINESS_KEY": "openai-signup",
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
        self.assertEqual("", config.routing_profile_id)

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

    def test_runtime_preflight_requires_headroom_for_proxy_acquire_and_finalize(self) -> None:
        """The protocol call is not the whole run: proxy acquisition and finalize also
        happen inside the worker, and a kill skips the proxy release step."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "EASY_PROTOCOL_ACCOUNT_AUDIT_TIMEOUT_SECONDS": "400",
                    "REGISTER_ACCOUNT_AUDIT_WORKER_HARD_TIMEOUT_SECONDS": "420",
                },
                clear=True,
            ):
                with self.assertRaisesRegex(RuntimeError, "account_audit_timeout_leaves_no_headroom"):
                    validate_runtime_preflight()

    def test_runtime_preflight_rejects_audit_http_timeout_above_worker_hard_timeout(self) -> None:
        """A batch the worker gets killed mid-way through marks every claimed target inconclusive."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "EASY_PROTOCOL_ACCOUNT_AUDIT_TIMEOUT_SECONDS": "600",
                    "REGISTER_ACCOUNT_AUDIT_WORKER_HARD_TIMEOUT_SECONDS": "420",
                },
                clear=True,
            ):
                with self.assertRaisesRegex(RuntimeError, "account_audit_timeout_exceeds_worker_hard_timeout"):
                    validate_runtime_preflight()

    def test_runtime_preflight_accepts_audit_http_timeout_below_worker_hard_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "register-output"
            with mock.patch.dict(
                os.environ,
                {
                    "REGISTER_OUTPUT_ROOT": str(output_root),
                    "EASY_PROTOCOL_ACCOUNT_AUDIT_TIMEOUT_SECONDS": "300",
                    "REGISTER_ACCOUNT_AUDIT_WORKER_HARD_TIMEOUT_SECONDS": "420",
                },
                clear=True,
            ):
                preflight = validate_runtime_preflight()
        self.assertEqual(300, preflight["accountAudit"]["protocolTimeoutSeconds"])
        self.assertEqual(420, preflight["accountAudit"]["workerHardTimeoutSeconds"])

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
