from __future__ import annotations

import base64
import io
import json
import sys
import unittest
import urllib.error
from pathlib import Path
from unittest import mock


ORCHESTRATION_SRC_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "src"
PYTHON_SHARED_SRC_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "python_shared" / "src"
for candidate in (ORCHESTRATION_SRC_ROOT, PYTHON_SHARED_SRC_ROOT):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from shared_proxy import easy_proxy_client  # noqa: E402


class EasyProxyClientTests(unittest.TestCase):
    def test_api_request_discovers_password_auth_and_uses_raw_management_password(self) -> None:
        with mock.patch.object(easy_proxy_client, "_build_management_opener", return_value=object()), \
            mock.patch.object(
                easy_proxy_client,
                "_read_json_response",
                side_effect=[
                    {"auth_mode": "password", "no_password": False},
                    {"ok": True},
                ],
            ) as read_json_response:
            result = easy_proxy_client._api_request(
                "GET",
                "/proxy/catalog",
                base_url="http://easy-proxy:29888",
                api_key="management-secret",
                wait_for_ready=False,
            )

        self.assertEqual({"ok": True}, result)
        self.assertEqual(2, read_json_response.call_count)
        discovery_request = read_json_response.call_args_list[0].args[1]
        api_request = read_json_response.call_args_list[1].args[1]
        self.assertEqual("http://easy-proxy:29888/api/auth", discovery_request.full_url)
        self.assertIsNone(discovery_request.get_header("Authorization"))
        self.assertEqual("management-secret", api_request.get_header("Authorization"))
        self.assertIsNone(api_request.get_header("Proxy-authorization"))

    def test_api_request_falls_back_to_legacy_api_key_after_management_password_401(self) -> None:
        unauthorized = urllib.error.HTTPError(
            "http://easy-proxy:29888/api/nodes",
            401,
            "Unauthorized",
            hdrs=None,
            fp=io.BytesIO(b"{\"error\":\"UNAUTHORIZED\"}"),
        )
        with mock.patch.dict(
            "os.environ",
            {
                "EASY_PROXY_MANAGEMENT_PASSWORD": "stale-management-secret",
                "EASY_PROXY_API_KEY": "active-api-key",
            },
            clear=True,
        ), mock.patch.object(easy_proxy_client, "_build_management_opener", return_value=object()), \
            mock.patch.object(
                easy_proxy_client,
                "_read_json_response",
                side_effect=[
                    {"auth_mode": "password", "no_password": False},
                    unauthorized,
                    {"ok": True},
                ],
            ) as read_json_response:
            result = easy_proxy_client._api_request(
                "GET",
                "/api/nodes",
                base_url="http://easy-proxy:29888",
                api_key="stale-management-secret",
                wait_for_ready=False,
            )

        self.assertEqual({"ok": True}, result)
        self.assertEqual(3, read_json_response.call_count)
        first_request = read_json_response.call_args_list[1].args[1]
        fallback_request = read_json_response.call_args_list[2].args[1]
        self.assertEqual("stale-management-secret", first_request.get_header("Authorization"))
        self.assertEqual("active-api-key", fallback_request.get_header("Authorization"))

    def test_api_request_uses_basic_auth_for_discovered_canonical_pair(self) -> None:
        with mock.patch.dict(
            "os.environ",
            {"EASY_PROXY_MANAGEMENT_USERNAME": "easyproxy"},
            clear=False,
        ), mock.patch.object(
            easy_proxy_client,
            "_build_management_opener",
            return_value=object(),
        ), mock.patch.object(
            easy_proxy_client,
            "_read_json_response",
            side_effect=[
                {"auth_mode": "canonical_pair", "username_required": True, "no_password": False},
                {"ok": True},
            ],
        ) as read_json_response:
            easy_proxy_client._api_request(
                "GET",
                "/proxy/snapshot",
                base_url="http://easy-proxy:29888",
                api_key="management-secret",
                wait_for_ready=False,
            )

        api_request = read_json_response.call_args_list[1].args[1]
        expected = base64.b64encode(b"easyproxy:management-secret").decode("ascii")
        self.assertEqual(f"Basic {expected}", api_request.get_header("Authorization"))

    def test_api_request_defaults_canonical_pair_username(self) -> None:
        with mock.patch.dict(
            "os.environ",
            {"EASY_PROXY_MANAGEMENT_USERNAME": ""},
            clear=False,
        ), mock.patch.object(
            easy_proxy_client,
            "_build_management_opener",
            return_value=object(),
        ), mock.patch.object(
            easy_proxy_client,
            "_read_json_response",
            side_effect=[
                {"auth_mode": "canonical_pair", "username_required": True, "no_password": False},
                {"ok": True},
            ],
        ) as read_json_response:
            easy_proxy_client._api_request(
                "GET",
                "/proxy/snapshot",
                base_url="http://easy-proxy:29888",
                api_key="management-secret",
                wait_for_ready=False,
            )

        api_request = read_json_response.call_args_list[1].args[1]
        expected = base64.b64encode(b"easyproxy:management-secret").decode("ascii")
        self.assertEqual(f"Basic {expected}", api_request.get_header("Authorization"))

    def test_api_request_preserves_bearer_auth_for_legacy_server_without_discovery(self) -> None:
        not_found = urllib.error.HTTPError(
            "http://legacy-proxy:9888/api/auth",
            404,
            "Not Found",
            hdrs=None,
            fp=io.BytesIO(b""),
        )
        with mock.patch.object(easy_proxy_client, "_build_management_opener", return_value=object()), \
            mock.patch.object(
                easy_proxy_client,
                "_read_json_response",
                side_effect=[not_found, {"ok": True}],
            ) as read_json_response:
            easy_proxy_client._api_request(
                "GET",
                "/proxy/catalog",
                base_url="http://legacy-proxy:9888",
                api_key="legacy-secret",
                wait_for_ready=False,
            )

        api_request = read_json_response.call_args_list[1].args[1]
        self.assertEqual("Bearer legacy-secret", api_request.get_header("Authorization"))

    def test_api_request_does_not_send_configured_secret_when_auth_is_disabled(self) -> None:
        with mock.patch.object(easy_proxy_client, "_build_management_opener", return_value=object()), \
            mock.patch.object(
                easy_proxy_client,
                "_read_json_response",
                side_effect=[
                    {"no_password": True},
                    {"ok": True},
                ],
            ) as read_json_response:
            easy_proxy_client._api_request(
                "GET",
                "/api/source-sync/status",
                base_url="http://easy-proxy:29888",
                api_key="must-not-leak",
                wait_for_ready=False,
            )

        api_request = read_json_response.call_args_list[1].args[1]
        self.assertIsNone(api_request.get_header("Authorization"))

    def test_api_error_redacts_sensitive_response_fields(self) -> None:
        payload = {
            "error": "CHECKOUT_FAILED",
            "password": "response-secret",
            "proxyUrl": "http://user:proxy-secret@easy-proxy:22323",
            "details": {"api_key": "nested-secret"},
        }
        http_error = urllib.error.HTTPError(
            "http://easy-proxy:29888/proxy/leases/checkout",
            503,
            "Service Unavailable",
            hdrs=None,
            fp=io.BytesIO(json.dumps(payload).encode("utf-8")),
        )
        with mock.patch.object(easy_proxy_client, "_build_management_opener", return_value=object()), \
            mock.patch.object(
                easy_proxy_client,
                "_read_json_response",
                side_effect=[
                    {"auth_mode": "password", "no_password": False},
                    http_error,
                ],
            ):
            with self.assertRaises(easy_proxy_client.EasyProxyAPIError) as raised:
                easy_proxy_client._api_request(
                    "POST",
                    "/proxy/leases/checkout",
                    {"hostId": "worker-1"},
                    base_url="http://easy-proxy:29888",
                    api_key="request-secret",
                    wait_for_ready=False,
                )

        error_text = str(raised.exception)
        self.assertEqual(503, raised.exception.status_code)
        self.assertEqual("CHECKOUT_FAILED", raised.exception.error_code)
        self.assertNotIn("response-secret", error_text)
        self.assertNotIn("proxy-secret", error_text)
        self.assertNotIn("nested-secret", error_text)
        self.assertNotIn("request-secret", error_text)
        self.assertIn("[REDACTED]", error_text)

    def test_error_redaction_covers_mixed_listener_credentials(self) -> None:
        redacted = easy_proxy_client.redact_easy_proxy_error(
            "curl rejected mixed://device-user:proxy-secret@192.168.15.201:22323"
        )

        self.assertNotIn("device-user", redacted)
        self.assertNotIn("proxy-secret", redacted)
        self.assertIn("mixed://[REDACTED]:[REDACTED]@192.168.15.201:22323", redacted)

    def test_checkout_retries_initial_probe_pending_with_exact_wire_payload(self) -> None:
        pending = easy_proxy_client.EasyProxyAPIError(
            "EasyProxy checkout pending",
            status_code=503,
            error_code="INITIAL_PROXY_PROBE_PENDING",
        )
        response = {
            "result": {
                "lease": {
                    "id": "lease-1",
                    "proxyUrl": "http://easy-proxy:25001",
                    "port": 25001,
                    "metadata": {
                        "selectedNodeMode": "dedicated-node",
                        "selectedNodePort": "25001",
                    },
                }
            }
        }
        with mock.patch.object(
            easy_proxy_client,
            "_api_request",
            side_effect=[pending, pending, response],
        ) as api_request, mock.patch.object(
            easy_proxy_client,
            "_resolve_initial_probe_max_attempts",
            return_value=4,
        ), mock.patch.object(
            easy_proxy_client,
            "_resolve_initial_probe_backoff_seconds",
            return_value=0.25,
        ), mock.patch(
            "shared_proxy.easy_proxy_client.time.sleep"
        ) as sleep:
            lease = easy_proxy_client.checkout_proxy(
                host_id="worker-1",
                ttl_minutes=45,
                base_url="http://easy-proxy:29888",
                api_key="management-secret",
                metadata={"source": "easyregister"},
                require_dedicated_node=True,
            )

        self.assertEqual("lease-1", lease["id"])
        self.assertEqual(3, api_request.call_count)
        self.assertEqual(
            [True, False, False],
            [call.kwargs["wait_for_ready"] for call in api_request.call_args_list],
        )
        request_body = api_request.call_args_list[0].args[2]
        self.assertEqual(
            {
                "hostId": "worker-1",
                "providerTypeKey": "easy-proxies",
                "provisionMode": "reuse-only",
                "bindingMode": "shared-instance",
                "protocol": "http",
                "ttlMinutes": 45,
                "metadata": {"source": "easyregister"},
            },
            request_body,
        )
        self.assertEqual([mock.call(0.25), mock.call(0.5)], sleep.call_args_list)

    def test_checkout_does_not_retry_unrelated_service_unavailable(self) -> None:
        unavailable = easy_proxy_client.EasyProxyAPIError(
            "No route",
            status_code=503,
            error_code="NO_PROXY_PROVIDER_ROUTE",
        )
        with mock.patch.object(
            easy_proxy_client,
            "_api_request",
            side_effect=unavailable,
        ) as api_request, mock.patch("shared_proxy.easy_proxy_client.time.sleep") as sleep:
            with self.assertRaises(easy_proxy_client.EasyProxyAPIError):
                easy_proxy_client.checkout_proxy(base_url="http://easy-proxy:29888")

        api_request.assert_called_once()
        sleep.assert_not_called()

    def test_checkout_releases_created_lease_when_dedicated_validation_fails(self) -> None:
        response = {
            "result": {
                "lease": {
                    "id": "lease-invalid",
                    "proxyUrl": "http://easy-proxy:2323",
                    "port": 2323,
                    "metadata": {
                        "selectedNodeMode": "shared-listener",
                        "selectedNodePort": "2323",
                    },
                }
            }
        }
        with mock.patch.object(
            easy_proxy_client,
            "_api_request",
            return_value=response,
        ), mock.patch.object(easy_proxy_client, "release_lease") as release_lease:
            with self.assertRaisesRegex(RuntimeError, "non-dedicated route"):
                easy_proxy_client.checkout_proxy(
                    base_url="http://easy-proxy:29888",
                    api_key="management-secret",
                    require_dedicated_node=True,
                )

        release_lease.assert_called_once_with(
            "lease-invalid",
            base_url="http://easy-proxy:29888",
            api_key="management-secret",
        )

    def test_list_available_nodes_falls_back_after_valid_empty_filtered_response(self) -> None:
        with mock.patch.object(easy_proxy_client, "_build_management_opener", return_value=object()):
            with mock.patch.object(
                easy_proxy_client,
                "_read_json_response",
                side_effect=[
                    {"no_password": True},
                    {"available_nodes": 0, "nodes": []},
                    {"no_password": True},
                    {
                        "available_nodes": 1,
                        "nodes": [
                            {"tag": "node-up", "available": True, "availability_score": 10},
                        ],
                    },
                ],
            ) as read_json_response:
                with mock.patch.object(
                    easy_proxy_client,
                    "_wait_easy_proxy_ready",
                    side_effect=AssertionError("list_available_nodes must not run readiness polling"),
                ) as wait_ready:
                    nodes = easy_proxy_client.list_available_nodes(
                        base_url="http://easy-proxy-monorepo-service:29888",
                        only_available=True,
                        prefer_available=True,
                    )

        self.assertEqual(["node-up"], [node["tag"] for node in nodes])
        wait_ready.assert_not_called()
        self.assertEqual(4, read_json_response.call_count)
        first_request = read_json_response.call_args_list[1].args[1]
        second_request = read_json_response.call_args_list[3].args[1]
        self.assertEqual(
            "http://easy-proxy-monorepo-service:29888/api/nodes?only_available=1&prefer_available=1",
            first_request.full_url,
        )
        self.assertEqual(
            "http://easy-proxy-monorepo-service:29888/api/nodes",
            second_request.full_url,
        )

    def test_list_available_nodes_uses_no_readiness_wait_for_filtered_and_fallback_requests(self) -> None:
        with mock.patch.object(
            easy_proxy_client,
            "_api_request",
            side_effect=[
                {"available_nodes": 0, "nodes": []},
                {"available_nodes": 1, "nodes": [{"tag": "node-up", "available": True}]},
            ],
        ) as api_request:
            nodes = easy_proxy_client.list_available_nodes(
                base_url="http://easy-proxy-monorepo-service:29888",
                only_available=True,
                prefer_available=True,
            )

        self.assertEqual(["node-up"], [node["tag"] for node in nodes])
        self.assertEqual(2, api_request.call_count)
        self.assertEqual(
            mock.call(
                "GET",
                "/api/nodes?only_available=1&prefer_available=1",
                base_url="http://easy-proxy-monorepo-service:29888",
                api_key="",
                wait_for_ready=False,
            ),
            api_request.call_args_list[0],
        )
        self.assertEqual(
            mock.call(
                "GET",
                "/api/nodes",
                base_url="http://easy-proxy-monorepo-service:29888",
                api_key="",
                wait_for_ready=False,
            ),
            api_request.call_args_list[1],
        )

    def test_list_available_nodes_falls_back_to_unfiltered_payload(self) -> None:
        fallback_payload = {
            "available_nodes": 2,
            "nodes": [
                {"tag": "node-down", "available": False, "availability_score": 90},
                {"tag": "node-up-low", "available": True, "availability_score": 10},
                {"tag": "node-up-high", "effective_available": True, "availability_score": 80},
            ],
        }

        with mock.patch.object(
            easy_proxy_client,
            "_api_request",
            side_effect=[
                easy_proxy_client.EasyProxyAPIError(
                    "Initial proxy probe pending",
                    status_code=503,
                    error_code="INITIAL_PROXY_PROBE_PENDING",
                ),
                fallback_payload,
            ],
        ) as api_request:
            nodes = easy_proxy_client.list_available_nodes(
                base_url="http://easy-proxy-monorepo-service:29888",
                only_available=True,
                prefer_available=True,
            )

        self.assertEqual(["node-up-high", "node-up-low"], [node["tag"] for node in nodes])
        self.assertEqual(2, api_request.call_count)
        self.assertEqual(
            mock.call(
                "GET",
                "/api/nodes?only_available=1&prefer_available=1",
                base_url="http://easy-proxy-monorepo-service:29888",
                api_key="",
                wait_for_ready=False,
            ),
            api_request.call_args_list[0],
        )
        self.assertEqual(
            mock.call(
                "GET",
                "/api/nodes",
                base_url="http://easy-proxy-monorepo-service:29888",
                api_key="",
                wait_for_ready=False,
            ),
            api_request.call_args_list[1],
        )

    def test_list_available_nodes_does_not_mask_management_auth_failure(self) -> None:
        unauthorized = easy_proxy_client.EasyProxyAPIError(
            "Management authentication failed",
            status_code=401,
            error_code="UNAUTHORIZED",
        )
        with mock.patch.object(
            easy_proxy_client,
            "_api_request",
            side_effect=unauthorized,
        ) as api_request:
            with self.assertRaises(easy_proxy_client.EasyProxyAPIError):
                easy_proxy_client.list_available_nodes(
                    base_url="http://easy-proxy-monorepo-service:29888",
                    only_available=True,
                    prefer_available=True,
                )

        api_request.assert_called_once()

    def test_list_available_nodes_falls_back_when_filtered_nodes_shape_is_invalid(self) -> None:
        fallback_payload = {
            "available_nodes": 1,
            "nodes": [
                {"tag": "node-up", "effective_available": True, "availability_score": 80},
            ],
        }

        with mock.patch.object(
            easy_proxy_client,
            "_api_request",
            side_effect=[
                {"available_nodes": 1, "nodes": 1},
                fallback_payload,
            ],
        ) as api_request:
            try:
                nodes = easy_proxy_client.list_available_nodes(
                    base_url="http://easy-proxy-monorepo-service:29888",
                    only_available=True,
                    prefer_available=True,
                )
            except Exception as exc:
                self.fail(f"filtered response parse errors must use the unfiltered fallback: {exc}")

        self.assertEqual(["node-up"], [node["tag"] for node in nodes])
        self.assertEqual(2, api_request.call_count)
        self.assertEqual("/api/nodes", api_request.call_args_list[1].args[1])

    def test_checkout_random_node_proxy_rejects_empty_available_list_without_unfiltered_fallback(self) -> None:
        with mock.patch.object(
            easy_proxy_client,
            "get_settings",
            return_value={
                "multi_port_protocol": "http",
                "multi_port_username": "",
                "multi_port_password": "",
            },
        ):
            with mock.patch.object(
                easy_proxy_client,
                "list_available_nodes",
                return_value=[],
            ) as list_nodes_mock:
                with self.assertRaisesRegex(RuntimeError, "found no available nodes"):
                    easy_proxy_client.checkout_random_node_proxy(
                        base_url="http://easy-proxy-monorepo-service:29888",
                        runtime_host="easy-proxy",
                    )

        list_nodes_mock.assert_called_once_with(
            base_url="http://easy-proxy-monorepo-service:29888",
            api_key="",
            only_available=True,
            prefer_available=True,
        )

    def test_checkout_random_node_proxy_excludes_mixed_listener_by_runtime_http_url(self) -> None:
        with mock.patch.object(
            easy_proxy_client,
            "get_settings",
            return_value={
                "multi_port_protocol": "mixed",
                "multi_port_username": "proxy-user",
                "multi_port_password": "proxy-password",
            },
        ), mock.patch.object(
            easy_proxy_client,
            "list_available_nodes",
            return_value=[{"port": 22323, "available": True}],
        ):
            with self.assertRaisesRegex(RuntimeError, "exhausted available nodes"):
                easy_proxy_client.checkout_random_node_proxy(
                    base_url="http://192.168.15.201:29888",
                    runtime_host="192.168.15.201",
                    excluded_proxy_urls={
                        "http://proxy-user:proxy-password@192.168.15.201:22323"
                    },
                )

    def test_checkout_random_node_proxy_prefers_traffic_proven_nodes(self) -> None:
        with mock.patch.object(
            easy_proxy_client,
            "get_settings",
            return_value={
                "multi_port_protocol": "http",
                "multi_port_username": "proxy-user",
                "multi_port_password": "proxy-password",
            },
        ), mock.patch.object(
            easy_proxy_client,
            "list_available_nodes",
            return_value=[
                {"tag": "probe-only", "port": 22323, "effective_available": True},
                {
                    "tag": "traffic-proven",
                    "port": 22324,
                    "effective_available": True,
                    "traffic_proven_usable": True,
                },
            ],
        ):
            candidate = easy_proxy_client.checkout_random_node_proxy(
                base_url="http://192.168.15.201:29888",
                runtime_host="192.168.15.201",
            )

        self.assertEqual("traffic-proven", candidate["metadata"]["selectedNodeTag"])
        self.assertEqual(22324, candidate["port"])

    def test_checkout_random_node_proxy_uses_host_specific_listener_username(self) -> None:
        with mock.patch.object(
            easy_proxy_client,
            "get_settings",
            return_value={
                "multi_port_protocol": "http",
                "multi_port_username": "proxy-user",
                "multi_port_password": "proxy-password",
            },
        ), mock.patch.object(
            easy_proxy_client,
            "list_available_nodes",
            return_value=[{"tag": "node-1", "port": 22324, "available": True}],
        ):
            candidate = easy_proxy_client.checkout_random_node_proxy(
                base_url="http://192.168.15.201:29888",
                runtime_host="192.168.15.201",
                host_id="Worker.OpenAI_1",
            )

        self.assertEqual("proxy-user+dev=worker.openai_1", candidate["username"])
        self.assertEqual(
            "http://proxy-user%2Bdev%3Dworker.openai_1:proxy-password@192.168.15.201:22324",
            candidate["proxyUrl"],
        )

    def test_checkout_random_node_proxy_uses_management_credential_when_settings_redact_password(self) -> None:
        with mock.patch.object(
            easy_proxy_client,
            "get_settings",
            return_value={
                "listener_protocol": "http",
                "listener_username": "proxy-user",
                "listener_password": "",
                "multi_port_password": "",
            },
        ), mock.patch.object(
            easy_proxy_client,
            "list_available_nodes",
            return_value=[{"tag": "node-1", "port": 22324, "available": True}],
        ):
            candidate = easy_proxy_client.checkout_random_node_proxy(
                base_url="http://192.168.15.201:29888",
                api_key="management-password",
                runtime_host="192.168.15.201",
                host_id="worker-openai-1",
            )

        self.assertEqual("management-password", candidate["password"])
        self.assertEqual(
            "http://proxy-user%2Bdev%3Dworker-openai-1:management-password@192.168.15.201:22324",
            candidate["proxyUrl"],
        )

    def test_checkout_random_node_proxy_keeps_base_username_for_invalid_host_id(self) -> None:
        with mock.patch.object(
            easy_proxy_client,
            "get_settings",
            return_value={
                "multi_port_protocol": "http",
                "multi_port_username": "proxy-user",
                "multi_port_password": "proxy-password",
            },
        ), mock.patch.object(
            easy_proxy_client,
            "list_available_nodes",
            return_value=[{"tag": "node-1", "port": 22324, "available": True}],
        ):
            candidate = easy_proxy_client.checkout_random_node_proxy(
                base_url="http://192.168.15.201:29888",
                runtime_host="192.168.15.201",
                host_id="invalid host id",
            )

        self.assertEqual("proxy-user", candidate["username"])

    def test_wait_ready_accepts_unfiltered_available_nodes(self) -> None:
        with mock.patch.object(easy_proxy_client, "_build_management_opener", return_value=object()):
            with mock.patch.object(
                easy_proxy_client,
                "_read_json_response",
                side_effect=[
                    {"no_password": True},
                    RuntimeError("INITIAL_PROXY_PROBE_PENDING"),
                    {
                        "available_nodes": 0,
                        "nodes": [{"tag": "node-up", "available": True}],
                    },
                ],
            ):
                with mock.patch("shared_proxy.easy_proxy_client.time.sleep", return_value=None):
                    easy_proxy_client._wait_easy_proxy_ready("http://easy-proxy-monorepo-service:29888")

    def test_wait_ready_fetches_unfiltered_nodes_after_valid_zero_response(self) -> None:
        with mock.patch.object(easy_proxy_client, "_build_management_opener", return_value=object()):
            with mock.patch.object(
                easy_proxy_client,
                "_read_json_response",
                side_effect=[
                    {"no_password": True},
                    {"available_nodes": 0, "nodes": []},
                    {"available_nodes": 0, "nodes": [{"tag": "node-up", "available": True}]},
                ],
            ) as read_json_response:
                with mock.patch.object(
                    easy_proxy_client,
                    "_resolve_easy_proxy_ready_timeout_seconds",
                    return_value=1,
                ):
                    with mock.patch(
                        "shared_proxy.easy_proxy_client.time.time",
                        side_effect=[0, 0, 2],
                    ):
                        with mock.patch("shared_proxy.easy_proxy_client.time.sleep", return_value=None):
                            easy_proxy_client._wait_easy_proxy_ready(
                                "http://easy-proxy-monorepo-service:29888"
                            )

        self.assertEqual(3, read_json_response.call_count)
        first_request = read_json_response.call_args_list[1].args[1]
        second_request = read_json_response.call_args_list[2].args[1]
        self.assertEqual(
            "http://easy-proxy-monorepo-service:29888/api/nodes?only_available=1&prefer_available=1",
            first_request.full_url,
        )
        self.assertEqual(
            "http://easy-proxy-monorepo-service:29888/api/nodes",
            second_request.full_url,
        )


if __name__ == "__main__":
    unittest.main()
