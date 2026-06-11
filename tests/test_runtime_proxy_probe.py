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

    def get(self, *_args: object, **_kwargs: object) -> SimpleNamespace:
        return self.response

    def close(self) -> None:
        self.closed = True


class RuntimeProxyProbeTests(unittest.TestCase):
    def _probe_with_response(self, *, probe_url: str, status_code: int, text: str = "") -> _FakeProbeSession:
        session = _FakeProbeSession(SimpleNamespace(status_code=status_code, text=text))
        with mock.patch.object(runtime_proxy_probe.requests, "Session", return_value=session), \
            mock.patch.object(runtime_proxy_probe, "build_request_proxies", return_value={}):
            runtime_proxy_probe.probe_flow_proxy(
                proxy_url="http://easy-proxy:25001",
                probe_url=probe_url,
                expected_statuses={200},
            )
        return session

    def test_openai_auth_cloudflare_challenge_403_counts_as_reachable_probe(self) -> None:
        session = self._probe_with_response(
            probe_url="https://auth.openai.com/log-in-or-create-account",
            status_code=403,
            text="<!DOCTYPE html><html><head><title>Just a moment...</title></head></html>",
        )

        self.assertTrue(session.closed)

    def test_chatgpt_auth_empty_403_counts_as_reachable_probe(self) -> None:
        session = self._probe_with_response(
            probe_url="https://chatgpt.com/auth/login",
            status_code=403,
            text="",
        )

        self.assertTrue(session.closed)

    def test_non_openai_403_still_fails_probe(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "easy_proxy_probe_failed status=403"):
            self._probe_with_response(
                probe_url="https://example.com/login",
                status_code=403,
                text="Forbidden",
            )

    def test_openai_proxy_auth_407_still_fails_probe(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "easy_proxy_probe_failed status=407"):
            self._probe_with_response(
                probe_url="https://auth.openai.com/log-in-or-create-account",
                status_code=407,
                text="Proxy Authentication Required",
            )


if __name__ == "__main__":
    unittest.main()
