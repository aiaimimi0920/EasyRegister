"""Opt-in HeroSMS proxy: one durable acquisition attempt, including failures.

Run only for an isolated SMS canary. Its HeroSMS baseUrl must point here.
The normal SMS service and continuous workers must keep paid SMS disabled.
Starting this server never buys a number. Never delete a consumed ledger to retry.
"""

from __future__ import annotations

import argparse
import hmac
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Callable


UPSTREAM_URL = "https://hero-sms.com/stubs/handler_api.php"
UPSTREAM_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
)
API_PATH = "/stubs/handler_api.php"
READ_ACTIONS = {
    "getBalance", "getServicesList", "getCountries", "getPrices", "getOperators",
    "getTopCountriesByService", "getTopCountriesByServiceRank", "getActiveActivations",
}
QUERY_KEYS = {
    "api_key", "action", "service", "country", "operator", "maxPrice", "fixedPrice",
    "ref", "phoneException", "lang", "id", "status",
}


@dataclass(frozen=True)
class Response:
    status: int
    body: bytes
    content_type: str = "application/json"


def error_response(status: int, code: str) -> Response:
    return Response(status, json.dumps({"title": code}).encode("utf-8"))


def positive_price(value: str) -> Decimal:
    try:
        price = Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("A positive finite provider-native price is required") from exc
    if not price.is_finite() or price <= 0:
        raise ValueError("A positive finite provider-native price is required")
    return price


def write_exclusive(path: Path, payload: dict[str, object]) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as output:
        json.dump(payload, output, ensure_ascii=True, sort_keys=True)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())
    if hasattr(os, "O_DIRECTORY"):
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)


class NoRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def send_upstream(query: str) -> Response:
    request = urllib.request.Request(
        UPSTREAM_URL + "?" + query,
        headers={
            "Accept": "application/json,text/plain;q=0.9,*/*;q=0.8",
            "User-Agent": UPSTREAM_USER_AGENT,
        },
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirects())
    try:
        with opener.open(request, timeout=30) as response:
            return Response(response.status, response.read(), response.headers.get_content_type())
    except urllib.error.HTTPError as exc:
        return Response(exc.code, exc.read(), exc.headers.get_content_type())


class SinglePaidOrderGate:
    def __init__(self, *, api_key: str, ledger: Path, max_price: str, country: int, service: str = "dr"):
        if not api_key or not ledger.is_absolute() or not ledger.parent.is_dir():
            raise ValueError("Key and an absolute ledger path in an existing directory are required")
        if country < 0 or not re.fullmatch(r"[a-z0-9]{1,12}", service):
            raise ValueError("An explicit country and service are required")
        self.api_key = api_key
        self.ledger = ledger
        self.receipt = ledger.with_name(ledger.name + ".receipt.json")
        self.max_price = positive_price(max_price)
        self.country = country
        self.service = service

    def _owns_activation(self, activation_id: str) -> bool:
        if not activation_id.isdigit() or not self.ledger.is_file():
            return False
        try:
            receipt = json.loads(self.receipt.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return False
        return str(receipt.get("activationId", "")) == activation_id

    def handle(self, query: str, transport: Callable[[str], Response] = send_upstream) -> Response:
        try:
            values = urllib.parse.parse_qs(query, keep_blank_values=True, strict_parsing=True)
        except ValueError:
            return error_response(400, "PAID_SMS_INVALID_QUERY")
        if set(values) - QUERY_KEYS or any(len(value) != 1 for value in values.values()):
            return error_response(400, "PAID_SMS_INVALID_QUERY")
        params = {key: value[0] for key, value in values.items()}
        if not hmac.compare_digest(params.get("api_key", "").encode("utf-8"), self.api_key.encode("utf-8")):
            return error_response(401, "BAD_KEY")
        action = params.get("action", "")
        acquisition = action == "getNumberV2"
        if acquisition:
            try:
                price = positive_price(params.get("maxPrice", ""))
            except ValueError:
                return error_response(400, "PAID_SMS_EXPLICIT_PRICE_REQUIRED")
            if (
                price > self.max_price
                or params.get("service") != self.service
                or params.get("country") != str(self.country)
            ):
                return error_response(400, "PAID_SMS_APPROVED_SCOPE_MISMATCH")
            try:
                if self.receipt.exists():
                    return error_response(409, "PAID_SMS_ORDER_BUDGET_EXHAUSTED")
                # Commit before network I/O. A timeout, rejection or crash consumes the attempt.
                write_exclusive(self.ledger, {
                    "reservedAt": datetime.now(timezone.utc).isoformat(),
                    "acquisitionAttemptLimit": 1,
                    "service": self.service,
                    "country": self.country,
                    "maxPriceProviderNative": str(price),
                })
            except FileExistsError:
                return error_response(409, "PAID_SMS_ORDER_BUDGET_EXHAUSTED")
            except OSError:
                return error_response(503, "PAID_SMS_LEDGER_UNAVAILABLE")
        elif action in {"getStatusV2", "getStatus", "setStatus"}:
            if not self._owns_activation(params.get("id", "")):
                return error_response(403, "PAID_SMS_ACTIVATION_NOT_OWNED")
            if action == "setStatus" and params.get("status") not in {"1", "6", "8"}:
                return error_response(403, "PAID_SMS_RESEND_NOT_ALLOWED")
        elif action not in READ_ACTIONS:
            return error_response(403, "PAID_SMS_ACTION_NOT_ALLOWED")
        try:
            response = transport(urllib.parse.urlencode(params))
        except Exception:
            response = error_response(502, "PAID_SMS_UPSTREAM_RESULT_UNKNOWN")
        if acquisition:
            receipt: dict[str, object] = {"upstreamStatus": response.status}
            try:
                payload = json.loads(response.body)
            except (ValueError, UnicodeError):
                payload = None
            if isinstance(payload, dict):
                if "currency" in payload:
                    receipt["currency"] = str(payload["currency"])
                activation_id = str(payload.get("activationId", ""))
                if activation_id.isdigit() and int(activation_id) > 0:
                    receipt["activationId"] = activation_id
                raw_cost = payload.get("activationCost")
                if raw_cost is not None:
                    try:
                        receipt["activationCostProviderNative"] = str(positive_price(str(raw_cost)))
                    except ValueError:
                        receipt["activationCostInvalid"] = True
            try:
                write_exclusive(self.receipt, receipt)
            except OSError:
                return error_response(503, "PAID_SMS_RECEIPT_UNAVAILABLE")
            cost = receipt.get("activationCostProviderNative")
            if "currency" in receipt and receipt["currency"] != "840":
                return error_response(502, "PAID_SMS_UPSTREAM_CURRENCY_MISMATCH")
            if receipt.get("activationCostInvalid") or (cost and Decimal(str(cost)) > price):
                return error_response(502, "PAID_SMS_UPSTREAM_PRICE_INVALID")
        return response


def make_handler(gate: SinglePaidOrderGate):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            target = urllib.parse.urlsplit(self.path)
            if target.path != API_PATH or target.scheme or target.netloc:
                response = error_response(404, "PAID_SMS_UNKNOWN_PATH")
            else:
                response = gate.handle(target.query)
            self.send_response(response.status)
            self.send_header("Content-Type", response.content_type)
            self.send_header("Content-Length", str(len(response.body)))
            self.end_headers()
            self.wfile.write(response.body)

        def log_message(self, format, *args) -> None:
            # Request URLs carry the provider API key; never use the default access log.
            pass

    return Handler


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--max-price", required=True, help="Approved cap in the verified provider currency")
    parser.add_argument("--country", type=int, required=True)
    parser.add_argument("--service", default="dr")
    parser.add_argument("--listen", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=19881)
    args = parser.parse_args()
    gate = SinglePaidOrderGate(
        api_key=os.environ.get("HEROSMS_API_KEY", ""), ledger=args.ledger,
        max_price=args.max_price, country=args.country, service=args.service,
    )
    server = ThreadingHTTPServer((args.listen, args.port), make_handler(gate))
    print(json.dumps({"event": "paid_sms_guard_ready", "acquisitionAttemptLimit": 1}), flush=True)
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
