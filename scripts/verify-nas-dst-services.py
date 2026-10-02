"""Exercise the real DST service clients without starting registration workers.

Requires injected EASY_EMAIL_BASE_URL/EASY_EMAIL_API_KEY and
EASY_PROXY_BASE_URL/EASY_PROXY_MANAGEMENT_PASSWORD. Creates one short-lived
mailbox and proxy lease, then releases both. Never prints credentials or mail.
"""
from __future__ import annotations

import base64
import http.client
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "server/services/python_shared/src"))

from shared_mailbox import easy_email_client as mail
from shared_proxy import easy_proxy_client as proxy


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def failure_summary(stage: str, exc: Exception) -> dict[str, str]:
    result = {"stage": stage, "errorType": type(exc).__name__}
    cause = exc.__cause__
    if isinstance(cause, urllib.error.HTTPError):
        result["httpStatus"] = str(cause.code)
    code = re.search(r"\[code=([A-Z][A-Z0-9_]{0,100})\]", str(exc))
    if code:
        result["errorCode"] = code.group(1)
    return result


def check_proxy_traffic(lease: dict[str, object]) -> None:
    endpoint = urllib.parse.urlsplit(str(lease.get("proxyUrl") or ""))
    expected_host = urllib.parse.urlsplit(os.environ["EASY_PROXY_BASE_URL"]).hostname
    require(endpoint.hostname == expected_host, "lease host differs from the configured gateway")
    require(endpoint.port is not None, "lease has no port")
    username = urllib.parse.unquote(endpoint.username or "")
    password = urllib.parse.unquote(endpoint.password or "")
    credential = base64.b64encode(f"{username}:{password}".encode()).decode()
    # An explicit CONNECT prevents system proxy/NO_PROXY settings from masking
    # a broken lease with a successful direct request. TLS verification stays on.
    connection = http.client.HTTPSConnection(endpoint.hostname, endpoint.port, timeout=20)
    try:
        connection.set_tunnel("example.com", 443, {"Proxy-Authorization": f"Basic {credential}"})
        connection.request("GET", "/", headers={"Host": "example.com"})
        response = connection.getresponse()
        body = response.read(64 * 1024)
        require(response.status == 200 and b"Example Domain" in body, "proxy response failed semantic validation")
    finally:
        connection.close()


def main() -> int:
    results: dict[str, object] = {
        "checkedAtUtc": datetime.now(timezone.utc).isoformat(),
        "scope": "real service clients; not a deployed registration workflow",
    }
    mailbox = None
    lease: dict[str, object] | None = None
    stage = "configuration"
    failures: list[dict[str, str]] = []
    try:
        for name in ("EASY_EMAIL_BASE_URL", "EASY_EMAIL_API_KEY", "EASY_PROXY_BASE_URL", "EASY_PROXY_MANAGEMENT_PASSWORD"):
            require(bool(os.environ.get(name, "").strip()), f"missing {name}")
        # Do not silently test a stale legacy endpoint/token instead of the new SDK.
        for legacy, current in (("MAILBOX_SERVICE_BASE_URL", "EASY_EMAIL_BASE_URL"), ("MAILBOX_SERVICE_API_KEY", "EASY_EMAIL_API_KEY")):
            require(not os.environ.get(legacy) or os.environ[legacy] == os.environ[current], "conflicting mailbox environment aliases")
        os.environ["MAILBOX_SERVICE_REQUEST_ATTEMPTS"] = "1"
        os.environ["MAILBOX_SERVICE_READY_TIMEOUT_SECONDS"] = "10"
        os.environ["EASY_PROXY_READY_TIMEOUT_SECONDS"] = "10"

        stage = "mailbox_authentication"
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            with opener.open(os.environ["EASY_EMAIL_BASE_URL"].rstrip("/") + "/mail/catalog", timeout=10):
                raise RuntimeError("unauthenticated catalog unexpectedly succeeded")
        except urllib.error.HTTPError as exc:
            require(exc.code == 401, "unexpected unauthenticated catalog status")
        mail._wait_mail_service_ready()
        results[stage] = "unauthenticated 401; authenticated catalog object"

        stage = "mailbox_plan"
        host_id = "easyregister/nas-dst-migration-smoke"
        plan = mail.plan_mailbox(provider="cloudflare_temp_email", default_host_id=host_id, ttl_minutes=5)
        require(isinstance(plan.get("providerType"), dict), "mailbox plan has no provider type")
        results[stage] = "passed"

        stage = "mailbox_open"
        mailbox = mail.create_mailbox(provider="cloudflare_temp_email", default_host_id=host_id, ttl_minutes=5)
        require(bool(mailbox.session_id and mailbox.email and mailbox.recovery_data_credential), "invalid mailbox or missing recovery data")
        results[stage] = "session, address and opaque recovery data present"

        stage = "mailbox_refresh"
        session_path = urllib.parse.quote(mailbox.session_id, safe="")
        mail._post_json(f"/mail/mailboxes/{session_path}/refresh", {})
        results[stage] = "passed"
        stage = "mailbox_message_query"
        messages = mail._get_json(f"/mail/query/observed-messages?sessionId={session_path}&limit=10")
        require(isinstance(messages.get("messages"), list), "message query contract mismatch")
        results[stage] = "passed"
        stage = "mailbox_code"
        code = mail._get_json(f"/mail/mailboxes/{session_path}/code")
        # VerificationCodeResponse has no required fields: no message yields {}.
        require(isinstance(code, dict) and ("code" not in code or isinstance(code["code"], dict)), "code response contract mismatch")
        results[stage] = "passed; empty inbox/code is not proof of email delivery"

        stage = "proxy_nodes"
        require(bool(proxy.list_available_nodes()), "no available proxy nodes")
        results[stage] = "authenticated nodes available"
        stage = "proxy_checkout"
        lease = proxy.checkout_proxy(host_id=host_id, ttl_minutes=2, metadata={"source": host_id})
        require(bool(lease.get("id")), "checkout has no lease ID")
        results[stage] = "passed"
        stage = "proxy_https_connect"
        check_proxy_traffic(lease)
        results[stage] = "verified TLS tunnel and Example Domain response"
    except Exception as exc:
        # Raw upstream exceptions can include tokens, addresses or recovery data.
        failures.append(failure_summary(stage, exc))
    finally:
        if lease and lease.get("id"):
            try:
                result = proxy._api_request("POST", f"/proxy/leases/{lease['id']}/release", {}, wait_for_ready=False)
                require(result.get("ok") is True, "lease release not acknowledged")
                results["proxy_release"] = "acknowledged"
            except Exception as exc:
                failures.append(failure_summary("proxy_release", exc))
        if mailbox:
            try:
                mail.release_mailbox(session_id=mailbox.session_id, reason="nas-dst-migration-smoke-complete")
                query = urllib.parse.urlencode({"hostId": "easyregister/nas-dst-migration-smoke", "status": "expired", "limit": 100, "newestFirst": "true"})
                sessions = mail._get_json("/mail/query/mailbox-sessions?" + query).get("sessions", [])
                require(any(item.get("id") == mailbox.session_id and item.get("status") == "expired" for item in sessions), "mailbox expiration not verified")
                results["mailbox_release"] = "session expiration verified"
            except Exception as exc:
                failures.append(failure_summary("mailbox_release", exc))
    results["failures"] = failures
    results["passed"] = not failures
    print(json.dumps(results, ensure_ascii=True, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
