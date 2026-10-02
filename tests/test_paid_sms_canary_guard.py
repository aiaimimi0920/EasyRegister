"""Financial boundary checks. All provider responses are local test fixtures."""

from __future__ import annotations

import concurrent.futures
import contextlib
import importlib.util
import io
import json
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("paid_sms_canary_guard", ROOT / "scripts/paid_sms_canary_guard.py")
assert SPEC is not None and SPEC.loader is not None
GUARD = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = GUARD
SPEC.loader.exec_module(GUARD)


class PaidSmsCanaryGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.ledger = Path(self.directory.name) / "attempt.json"
        self.gate = self.new_gate()
        self.transport = mock.Mock(return_value=GUARD.Response(200, json.dumps({
            "activationId": 1234, "activationCost": "0.40", "phoneNumber": "fixture-private-phone",
        }).encode("utf-8")))

    def new_gate(self):
        return GUARD.SinglePaidOrderGate(
            api_key="fixture-private-key", ledger=self.ledger,
            max_price="0.50", country=16, service="dr",
        )

    def query(self, **overrides) -> str:
        values = {
            "api_key": "fixture-private-key", "action": "getNumberV2",
            "service": "dr", "country": "16", "maxPrice": "0.50",
        }
        values.update(overrides)
        return urllib.parse.urlencode(values)

    def test_success_does_not_allow_a_second_order_after_restart(self) -> None:
        self.assertEqual(200, self.gate.handle(self.query(), self.transport).status)
        self.assertEqual(409, self.new_gate().handle(self.query(), self.transport).status)
        self.transport.assert_called_once()

    def test_reservation_is_written_before_upstream_io(self) -> None:
        def send(query):
            self.assertTrue(self.ledger.is_file())
            self.assertEqual(1, json.loads(self.ledger.read_text(encoding="utf-8"))["acquisitionAttemptLimit"])
            return self.transport(query)

        self.assertEqual(200, self.gate.handle(self.query(), send).status)
        self.transport.assert_called_once()

    def test_transport_timeout_is_not_replayed(self) -> None:
        self.transport.side_effect = TimeoutError("result may already have been charged")
        self.assertEqual(502, self.gate.handle(self.query(), self.transport).status)
        self.assertEqual(409, self.gate.handle(self.query(), self.transport).status)
        self.transport.assert_called_once()

    def test_upstream_rejection_still_consumes_the_attempt(self) -> None:
        for code in (401, 409, 429, 500, 502, 503):
            with self.subTest(code=code):
                ledger = Path(self.directory.name) / f"attempt-{code}.json"
                gate = GUARD.SinglePaidOrderGate(api_key="fixture-private-key", ledger=ledger, max_price="0.50", country=16)
                transport = mock.Mock(return_value=GUARD.Response(code, b'{"title":"NO_NUMBERS"}'))
                self.assertEqual(code, gate.handle(self.query(), transport).status)
                self.assertEqual(409, gate.handle(self.query(), transport).status)
                transport.assert_called_once()

    def test_concurrent_requests_make_only_one_upstream_call(self) -> None:
        barrier = threading.Barrier(6)

        def attempt():
            barrier.wait(timeout=5)
            return self.new_gate().handle(self.query(), self.transport).status

        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            statuses = list(pool.map(lambda _: attempt(), range(6)))
        self.assertEqual([200, 409, 409, 409, 409, 409], sorted(statuses))
        self.transport.assert_called_once()

    def test_bad_or_missing_price_never_reaches_provider(self) -> None:
        for value in ("", "0", "-1", "NaN", "Infinity", "0.51", "not-a-price"):
            with self.subTest(value=value):
                self.assertEqual(400, self.gate.handle(self.query(maxPrice=value), self.transport).status)
        self.transport.assert_not_called()
        self.assertFalse(self.ledger.exists())

    def test_scope_and_credentials_cannot_change(self) -> None:
        for overrides in ({"country": "36"}, {"service": "other"}, {"api_key": "wrong"}, {"api_key": "\u00e9"}):
            with self.subTest(overrides=overrides):
                self.assertIn(self.gate.handle(self.query(**overrides), self.transport).status, (400, 401))
        self.transport.assert_not_called()

    def test_duplicate_parameters_and_other_purchase_actions_are_rejected(self) -> None:
        self.assertEqual(400, self.gate.handle(self.query() + "&action=getBalance", self.transport).status)
        for action in ("getNumber", "getMultiServiceNumber", "getExtraActivation", "setBalance"):
            self.assertEqual(403, self.gate.handle(self.query(action=action), self.transport).status)
        self.transport.assert_not_called()

    def test_read_only_queries_do_not_consume_budget(self) -> None:
        self.assertEqual(200, self.gate.handle(self.query(action="getBalance"), self.transport).status)
        self.assertFalse(self.ledger.exists())
        self.assertEqual(200, self.gate.handle(self.query(), self.transport).status)
        self.assertEqual(2, self.transport.call_count)

    def test_refund_and_completion_do_not_rearm_budget(self) -> None:
        self.gate.handle(self.query(), self.transport)
        for status in ("1", "6", "8"):
            self.assertEqual(200, self.gate.handle(self.query(action="setStatus", id="1234", status=status), self.transport).status)
        self.assertEqual(409, self.gate.handle(self.query(), self.transport).status)
        self.assertEqual(4, self.transport.call_count)

    def test_resend_and_other_account_activations_are_blocked(self) -> None:
        self.gate.handle(self.query(), self.transport)
        for values in ({"action": "setStatus", "id": "1234", "status": "3"}, {"action": "getStatusV2", "id": "9999"}):
            self.assertEqual(403, self.gate.handle(self.query(**values), self.transport).status)
        self.transport.assert_called_once()

    def test_ledger_failure_prevents_network_io(self) -> None:
        with mock.patch.object(GUARD, "write_exclusive", side_effect=OSError("disk unavailable")):
            self.assertEqual(503, self.gate.handle(self.query(), self.transport).status)
        self.transport.assert_not_called()

    def test_receipt_failure_cannot_reopen_budget(self) -> None:
        original = GUARD.write_exclusive

        def fail_receipt(path, payload):
            if path == self.gate.receipt:
                raise OSError("disk unavailable after upstream call")
            return original(path, payload)

        with mock.patch.object(GUARD, "write_exclusive", side_effect=fail_receipt):
            self.assertEqual(503, self.gate.handle(self.query(), self.transport).status)
        self.assertEqual(409, self.new_gate().handle(self.query(), self.transport).status)
        self.transport.assert_called_once()

    def test_provider_price_violation_keeps_receipt_but_blocks_submission(self) -> None:
        self.transport.return_value = GUARD.Response(200, b'{"activationId":1234,"activationCost":"0.60"}')
        self.assertEqual(502, self.gate.handle(self.query(), self.transport).status)
        self.assertTrue(self.gate._owns_activation("1234"))
        self.assertEqual(409, self.gate.handle(self.query(), self.transport).status)
        self.transport.assert_called_once()

    def test_private_provider_material_is_not_written_to_ledger(self) -> None:
        self.gate.handle(self.query(), self.transport)
        evidence = self.ledger.read_text(encoding="utf-8") + self.gate.receipt.read_text(encoding="utf-8")
        self.assertNotIn("fixture-private-key", evidence)
        self.assertNotIn("fixture-private-phone", evidence)
        self.assertEqual(1, json.loads(self.ledger.read_text(encoding="utf-8"))["acquisitionAttemptLimit"])

    def test_unexpected_provider_currency_keeps_receipt_but_stops_flow(self) -> None:
        self.transport.return_value = GUARD.Response(200, b'{"activationId":1234,"activationCost":"0.40","currency":978}')
        self.assertEqual(502, self.gate.handle(self.query(), self.transport).status)
        receipt = json.loads(self.gate.receipt.read_text(encoding="utf-8"))
        self.assertEqual("978", receipt["currency"])
        self.assertTrue(self.gate._owns_activation("1234"))
        self.assertEqual(409, self.gate.handle(self.query(), self.transport).status)
        self.transport.assert_called_once()

    def start_server(self, handler):
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
        thread.start()

        def stop():
            server.shutdown()
            thread.join(timeout=5)
            server.server_close()

        self.addCleanup(stop)
        return f"http://127.0.0.1:{server.server_port}"

    def provider_handler(self, calls: list[str], *, redirect: bool = False):
        class Provider(BaseHTTPRequestHandler):
            def do_GET(self):
                calls.append(self.path)
                # HeroSMS rejects Python's default user agent with error 1010.
                if not self.headers.get("User-Agent", "").startswith("Mozilla/5.0"):
                    self.send_response(403)
                    self.end_headers()
                    return
                self.send_response(302 if redirect else 200)
                if redirect:
                    self.send_header("Location", "/would-purchase-again")
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"activationId":1234,"activationCost":"0.40"}')

            def log_message(self, format, *args):
                pass

        return Provider

    def http_status(self, url: str) -> int:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), GUARD.NoRedirects())
        try:
            with opener.open(url, timeout=5) as response:
                response.read()
                return response.status
        except urllib.error.HTTPError as exc:
            exc.read()
            return exc.code

    def test_real_http_proxy_accepts_one_order_without_logging_credentials(self) -> None:
        calls: list[str] = []
        provider = self.start_server(self.provider_handler(calls))
        proxy = self.start_server(GUARD.make_handler(self.gate))
        logs = io.StringIO()
        with mock.patch.object(GUARD, "UPSTREAM_URL", provider + GUARD.API_PATH), contextlib.redirect_stderr(logs):
            url = proxy + GUARD.API_PATH + "?" + self.query()
            self.assertEqual(200, self.http_status(url))
            self.assertEqual(409, self.http_status(url))
        self.assertEqual(1, len(calls))
        self.assertEqual("", logs.getvalue())

    def test_upstream_http_redirect_does_not_repeat_the_order(self) -> None:
        calls: list[str] = []
        provider = self.start_server(self.provider_handler(calls, redirect=True))
        proxy = self.start_server(GUARD.make_handler(self.gate))
        with mock.patch.object(GUARD, "UPSTREAM_URL", provider + GUARD.API_PATH):
            url = proxy + GUARD.API_PATH + "?" + self.query()
            self.assertEqual(302, self.http_status(url))
            self.assertEqual(409, self.http_status(url))
        self.assertEqual(1, len(calls))

    def test_proxy_rejects_other_paths_before_provider_call(self) -> None:
        calls: list[str] = []
        provider = self.start_server(self.provider_handler(calls))
        proxy = self.start_server(GUARD.make_handler(self.gate))
        with mock.patch.object(GUARD, "UPSTREAM_URL", provider + GUARD.API_PATH):
            self.assertEqual(404, self.http_status(proxy + "/other?" + self.query()))
        self.assertEqual([], calls)
        self.assertFalse(self.ledger.exists())


if __name__ == "__main__":
    unittest.main()
