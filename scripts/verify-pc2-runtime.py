"""Non-purchasing, redacted checks of PC2's complete dependency surface."""

from __future__ import annotations

import base64
import json
import os
import urllib.error
import urllib.request


def external_executor_identity_matches(direct: object, routed: object) -> bool:
    def identity(payload: object) -> tuple | None:
        if not isinstance(payload, dict) or payload.get("status") != "succeeded":
            return None
        result = payload.get("result")
        if not isinstance(result, dict) or result.get("listen") != "0.0.0.0:9100":
            return None
        pool = result.get("pool")
        if not isinstance(pool, dict) or result.get("service") != "PythonProtocol":
            return None
        if pool.get("mode") != "dynamic_process_pool" or pool.get("maxWorkers") != 1:
            return None
        if not isinstance(pool.get("maxTasksPerWorker"), int) or pool["maxTasksPerWorker"] <= 0:
            return None
        return result["listen"], pool.get("mode"), pool.get("maxWorkers"), pool.get("maxTasksPerWorker")

    expected = identity(direct)
    return (
        expected is not None and isinstance(routed, dict)
        and routed.get("selected_service") == "PythonProtocol-001"
        and identity(routed) == expected
    )


def main() -> int:
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    results: dict[str, object] = {}

    def request(url: str, authorization: str = "", payload: dict[str, object] | None = None) -> tuple[int, object]:
        headers = {"Content-Type": "application/json"}
        if authorization:
            headers["Authorization"] = authorization
        data = json.dumps(payload).encode() if payload is not None else None
        try:
            with opener.open(urllib.request.Request(url, data=data, headers=headers), timeout=20) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as error:
            return error.code, {}

    email = os.environ["MAILBOX_SERVICE_BASE_URL"].rstrip("/")
    proxy = os.environ["EASY_PROXY_BASE_URL"].rstrip("/")
    sms = os.environ["SMS_SERVICE_BASE_URL"].rstrip("/")
    protocol = os.environ["EASY_PROTOCOL_BASE_URL"].rstrip("/")
    checks = (
        ("emailCatalog", email + "/mail/catalog", "Bearer " + os.environ["MAILBOX_SERVICE_API_KEY"]),
        ("smsHealth", sms + "/healthz", ""),
        ("smsCatalog", sms + "/sms/catalog", "Bearer " + os.environ["SMS_SERVICE_API_KEY"]),
        ("protocolHealth", protocol + "/api/health", ""),
        ("protocolControl", protocol + "/api/internal/control-plane", "Bearer " + os.environ["EASY_PROTOCOL_CONTROL_TOKEN"]),
        ("pythonHealth", "http://easy-protocol-python-001:9100/health", ""),
        ("proxyNodes", proxy + "/api/nodes", "Basic " + base64.b64encode(
            ("easyproxy:" + os.environ["EASY_PROXY_MANAGEMENT_PASSWORD"]).encode()
        ).decode()),
    )
    for name, url, authorization in checks:
        try:
            status, body = request(url, authorization)
            results[name] = {"status": status, "ok": status == 200 and isinstance(body, dict) and bool(body)}
        except Exception as error:
            results[name] = {"ok": False, "errorType": type(error).__name__}
    for name, url in (("smsAuthEnforced", sms + "/sms/catalog"), ("protocolAuthEnforced", protocol + "/api/internal/control-plane")):
        try:
            status, _ = request(url)
            results[name] = {"status": status, "ok": status in (401, 403)}
        except Exception as error:
            results[name] = {"ok": False, "errorType": type(error).__name__}
    try:
        payload = {"request_id": "pc2-safe-echo", "mode": "specified", "operation": "protocol.echo", "payload": {"marker": "pc2-safe-echo"}}
        status, body = request("http://easy-protocol-python-001:9100/invoke", payload=payload)
        matched = isinstance(body, dict) and body.get("result", {}).get("echo", {}).get("marker") == "pc2-safe-echo"
        results["pythonSemanticEcho"] = {"status": status, "ok": status == 200 and matched}
    except Exception as error:
        results["pythonSemanticEcho"] = {"ok": False, "errorType": type(error).__name__}
    try:
        payload = {
            "request_id": "pc2-executor-identity", "mode": "specified",
            "requested_service": "PythonProtocol-001", "operation": "health.inspect", "payload": {},
        }
        direct_status, direct = request("http://easy-protocol-python-001:9100/invoke", payload=payload)
        routed_status, routed = request(protocol + "/api/public/request", payload=payload)
        results["gatewayExternalExecutorIdentity"] = {
            "ok": direct_status == 200 and routed_status == 200 and external_executor_identity_matches(direct, routed),
            "directStatus": direct_status, "routedStatus": routed_status,
        }
    except Exception as error:
        results["gatewayExternalExecutorIdentity"] = {"ok": False, "errorType": type(error).__name__}
    passed = all(isinstance(result, dict) and result.get("ok") is True for result in results.values())
    print(json.dumps({"scope": "dependency health, catalogs, safe echo and external executor identity; no SMS purchase or registration", "checks": results, "ok": passed}, indent=2))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
