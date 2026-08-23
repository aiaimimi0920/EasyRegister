from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


SRC_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from others import runtime_proxy_probe  # noqa: E402


class _FakeProbeSession:
    def __init__(self, response: SimpleNamespace) -> None:
        self.headers: dict[str, str] = {}
        self.response = response
        self.closed = False
        self.get_kwargs: dict[str, object] = {}

    def get(self, *_args: object, **_kwargs: object) -> SimpleNamespace:
        self.get_kwargs = dict(_kwargs)
        return self.response

    def close(self) -> None:
        self.closed = True


class RuntimeProxyProbeTests(unittest.TestCase):
    def _probe_with_response(self, *, probe_url: str, status_code: int, text: str = "") -> _FakeProbeSession:
        session = _FakeProbeSession(SimpleNamespace(status_code=status_code, text=text, headers={}))
        with mock.patch.object(runtime_proxy_probe.requests, "Session", return_value=session), \
            mock.patch.object(runtime_proxy_probe, "build_request_proxies", return_value={}):
            runtime_proxy_probe.probe_flow_proxy(
                proxy_url="http://easy-proxy:25001",
                probe_url=probe_url,
                expected_statuses={200},
            )
        return session

    def test_openai_auth_cloudflare_challenge_403_fails_probe_by_default(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "easy_proxy_probe_failed status=403"):
            self._probe_with_response(
                probe_url="https://auth.openai.com/log-in-or-create-account",
                status_code=403,
                text="<!DOCTYPE html><html><head><title>Just a moment...</title></head></html>",
            )

    def test_chatgpt_auth_empty_403_fails_probe(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "easy_proxy_probe_failed status=403"):
            self._probe_with_response(
                probe_url="https://chatgpt.com/auth/login",
                status_code=403,
                text="",
            )

    def test_chatgpt_auth_cloudflare_challenge_403_fails_probe_by_default(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "easy_proxy_probe_failed status=403"):
            self._probe_with_response(
                probe_url="https://chatgpt.com/auth/login",
                status_code=403,
                text="cf-mitigated challenge page",
            )

    def test_openai_auth_cloudflare_challenge_403_cannot_be_opted_back_in(self) -> None:
        session = _FakeProbeSession(
            SimpleNamespace(
                status_code=403,
                text="<!DOCTYPE html><html><head><title>Just a moment...</title></head></html>",
                headers={},
            )
        )
        with mock.patch.dict("os.environ", {"REGISTER_PROXY_ACCEPT_OPENAI_AUTH_CHALLENGE_AS_SUCCESS": "true"}), \
            mock.patch.object(runtime_proxy_probe.requests, "Session", return_value=session), \
            mock.patch.object(runtime_proxy_probe, "build_request_proxies", return_value={}):
            with self.assertRaisesRegex(RuntimeError, "easy_proxy_probe_failed status=403"):
                runtime_proxy_probe.probe_flow_proxy(
                    proxy_url="http://easy-proxy:25001",
                    probe_url="https://auth.openai.com/log-in-or-create-account",
                    expected_statuses={200},
                )

        self.assertTrue(session.closed)

    def test_platform_openai_auth_cloudflare_header_challenge_cannot_be_opted_back_in(self) -> None:
        session = _FakeProbeSession(
            SimpleNamespace(
                status_code=403,
                text="<html><head><style global>body{font-family:Arial,Helvetica,sans-serif}</style></head></html>",
                headers={"cf-mitigated": "challenge", "server": "cloudflare"},
            )
        )
        with mock.patch.dict("os.environ", {"REGISTER_PROXY_ACCEPT_OPENAI_AUTH_CHALLENGE_AS_SUCCESS": "true"}), \
            mock.patch.object(runtime_proxy_probe.requests, "Session", return_value=session), \
            mock.patch.object(runtime_proxy_probe, "build_request_proxies", return_value={}):
            with self.assertRaisesRegex(RuntimeError, "easy_proxy_probe_failed status=403"):
                runtime_proxy_probe.probe_flow_proxy(
                    proxy_url="http://easy-proxy:25001",
                    probe_url="https://platform.openai.com/login",
                    expected_statuses={200},
                )

        self.assertTrue(session.closed)

    def test_openai_auth_probe_follows_redirects(self) -> None:
        session = _FakeProbeSession(
            SimpleNamespace(
                status_code=200,
                text="login",
                headers={},
                history=[],
            )
        )
        with mock.patch.object(runtime_proxy_probe.requests, "Session", return_value=session), \
            mock.patch.object(runtime_proxy_probe, "build_request_proxies", return_value={}):
            runtime_proxy_probe.probe_flow_proxy(
                proxy_url="http://easy-proxy:25001",
                probe_url="https://auth.openai.com/log-in-or-create-account",
                expected_statuses={200},
            )

        self.assertTrue(session.get_kwargs["allow_redirects"])
        self.assertTrue(session.closed)

    def test_probe_keeps_impersonated_transport_user_agent(self) -> None:
        session = self._probe_with_response(
            probe_url="https://auth.openai.com/log-in-or-create-account",
            status_code=200,
            text="login",
        )

        self.assertNotIn("user-agent", session.headers)
        self.assertEqual("en-US,en;q=0.9", session.headers["accept-language"])

    def test_non_openai_403_still_fails_probe(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "easy_proxy_probe_failed status=403"):
            self._probe_with_response(
                probe_url="https://example.com/login",
                status_code=403,
                text="Forbidden",
            )

    def test_probe_failure_does_not_include_target_url_or_response_body(self) -> None:
        target = "https://example.com/login?token=should-not-leak"
        secret_body = "proxyUrl=http://user:password@example.com:25001"
        with self.assertRaises(RuntimeError) as raised:
            self._probe_with_response(
                probe_url=target,
                status_code=502,
                text=secret_body,
            )

        message = str(raised.exception)
        self.assertEqual("easy_proxy_probe_failed status=502", message)
        self.assertNotIn(target, message)
        self.assertNotIn(secret_body, message)

    def test_openai_proxy_auth_407_still_fails_probe(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "easy_proxy_probe_failed status=407"):
            self._probe_with_response(
                probe_url="https://auth.openai.com/log-in-or-create-account",
                status_code=407,
                text="Proxy Authentication Required",
            )

    def test_curl_transport_failures_are_high_confidence_route_failures(self) -> None:
        samples = [
            "Failed to perform, curl: (35) Recv failure: Connection reset by peer.",
            "curl: (7) Failed to connect to easy-proxy port 25155 after 0 ms: Could not connect to server",
            "Failed to perform, curl: (7) CONNECT tunnel failed, response 502.",
            "Failed to perform, curl: (56) Connection closed abruptly.",
            "curl: (35) TLS connect error: error:00000000:invalid library (0):OPENSSL_internal:invalid library (0).",
        ]

        for sample in samples:
            with self.subTest(sample=sample):
                code, failure_class, confidence = runtime_proxy_probe.classify_easy_proxy_error(RuntimeError(sample))

                self.assertEqual(sample, code)
                self.assertEqual("route_failure", failure_class)
                self.assertEqual("high", confidence)


if __name__ == "__main__":
    unittest.main()
