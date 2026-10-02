"""Verify the deployed Dashboard through SSH without persisting credentials."""

from __future__ import annotations

import argparse
import base64
import json
import select
import socketserver
import threading
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import paramiko
from playwright.sync_api import sync_playwright


EXPECTED_IMAGE = "sha256:72c243dc42297e22b7d2107cb0e87e2c1ce7e09338d37db17186073de3af0b4e"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--screenshot", type=Path, required=True)
    args = parser.parse_args()
    if args.evidence.exists() or args.screenshot.exists():
        raise RuntimeError("verification_artifact_already_exists")
    args.evidence.parent.mkdir(parents=True, exist_ok=True)
    args.screenshot.parent.mkdir(parents=True, exist_ok=True)
    proof: dict[str, object] = {"startedAt": datetime.now(timezone.utc).isoformat()}
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    server = None
    thread = None
    stage = "ssh"
    try:
        client.connect(
            "192.168.15.104", username="mjc",
            key_filename="C:/Users/vmjcv/.ssh/id_ed25519",
            look_for_keys=False, allow_agent=False, timeout=15,
        )
        stage = "deployed-container"
        _, stdout, stderr = client.exec_command("docker inspect easy-register", timeout=20)
        raw = stdout.read()
        stderr.read()
        if stdout.channel.recv_exit_status() != 0:
            raise RuntimeError("container_inspection_failed")
        container = json.loads(raw)[0]
        if not container["State"]["Running"] or container["Image"] != EXPECTED_IMAGE:
            raise RuntimeError("deployed_image_mismatch")
        environment = dict(value.split("=", 1) for value in container["Config"]["Env"])
        token = environment["EASY_PROTOCOL_CONTROL_TOKEN"]
        if not token or environment.get("REGISTER_SMS_ALLOW_PAID") != "false":
            raise RuntimeError("runtime_boundary_mismatch")
        proof.update({"containerId": container["Id"], "image": container["Image"],
                      "productionPaidSmsEnabled": False})
        transport = client.get_transport()
        if transport is None:
            raise RuntimeError("ssh_transport_missing")

        class Forward(socketserver.BaseRequestHandler):
            def handle(self) -> None:
                channel = transport.open_channel(
                    "direct-tcpip", ("127.0.0.1", 19790), self.client_address,
                )
                try:
                    while True:
                        readable, _, _ = select.select([self.request, channel], [], [], 10)
                        for source in readable:
                            data = source.recv(65536)
                            if not data:
                                return
                            destination = channel if source is self.request else self.request
                            destination.sendall(data)
                finally:
                    channel.close()

        server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), Forward)
        server.daemon_threads = True
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base = "http://127.0.0.1:" + str(server.server_address[1])
        stage = "http-authentication"
        checks = []
        headers = {
            "anonymous": None,
            "bearer": "Bearer " + token,
            "basic": "Basic " + base64.b64encode(("dashboard:" + token).encode()).decode(),
        }
        for mode, authorization in headers.items():
            for path in ("/", "/index.html", "/api/status"):
                request = urllib.request.Request(
                    base + path,
                    headers={"Authorization": authorization} if authorization else {},
                )
                try:
                    response = urllib.request.urlopen(request, timeout=15)
                except urllib.error.HTTPError as error:
                    response = error
                with response:
                    body = response.read()
                    check = {
                        "mode": mode, "path": path, "status": response.status,
                        "noStore": response.headers.get("Cache-Control") == "no-store",
                        "basicChallenge": response.headers.get("WWW-Authenticate", "").startswith("Basic "),
                        "credentialInBody": token.encode() in body,
                    }
                checks.append(check)
                if check["status"] != (401 if mode == "anonymous" else 200):
                    raise RuntimeError("http_authentication_status_mismatch")
                if not check["noStore"] or check["credentialInBody"]:
                    raise RuntimeError("http_response_boundary_mismatch")
                if mode == "anonymous" and not check["basicChallenge"]:
                    raise RuntimeError("http_basic_challenge_missing")
        proof["httpChecks"] = checks
        stage = "browser-rendering"
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                proof["browserVersion"] = browser.version
                context = browser.new_context(
                    http_credentials={"username": "dashboard", "password": token},
                    viewport={"width": 1280, "height": 960},
                )
                try:
                    page = context.new_page()
                    page_errors = []
                    page.on("pageerror", lambda error: page_errors.append(type(error).__name__))
                    with page.expect_response(lambda response: response.url == base + "/api/status") as first:
                        navigation = page.goto(base + "/", wait_until="domcontentloaded")
                    status_response = first.value
                    data = status_response.json()
                    page.locator("#summary .card").nth(3).wait_for()
                    pipelines = data.get("pipelines") or {}
                    expected_metrics = [
                        f"{pipelines.get(role, {}).get('activeWorkers', 0)} / "
                        f"{pipelines.get(role, {}).get('configuredWorkers', 0)}"
                        for role in ("main", "continue")
                    ] + [
                        str(data.get("openaiOauthPool", data.get("smallSuccessPool", {})).get("size", 0)),
                        str(data.get("recentUploads", {}).get("count", 0)),
                    ]
                    metrics = page.locator("#summary .metric-value").all_text_contents()
                    if navigation is None or navigation.status != 200 or status_response.status != 200:
                        raise RuntimeError("browser_authentication_failed")
                    if metrics != expected_metrics or token in page.content() or page_errors:
                        raise RuntimeError("browser_rendering_mismatch")
                    with page.expect_response(
                        lambda response: response.url == base + "/api/status", timeout=10000,
                    ) as refresh:
                        page.wait_for_timeout(5500)
                    if refresh.value.status != 200 or page_errors:
                        raise RuntimeError("browser_refresh_failed")
                    page.locator("#summary").screenshot(path=str(args.screenshot))
                    proof["browser"] = {
                        "navigationStatus": navigation.status, "statusFetch": status_response.status,
                        "refreshStatus": refresh.value.status, "metricCardCount": len(metrics),
                        "metricsMatchApi": True, "metrics": metrics, "pageErrorCount": len(page_errors),
                        "executorRows": page.locator("#executors-body tr").count(),
                        "credentialInHtml": False, "screenshot": str(args.screenshot),
                    }
                finally:
                    context.close()
            finally:
                browser.close()
        proof["ok"] = True
    except Exception as error:
        proof.update({"ok": False, "failedStage": stage, "errorType": type(error).__name__})
    finally:
        if server is not None:
            server.shutdown()
            server.server_close()
        if thread is not None:
            thread.join(timeout=5)
        client.close()
    proof["finishedAt"] = datetime.now(timezone.utc).isoformat()
    proof["verificationResourcesClosed"] = True
    with args.evidence.open("x", encoding="utf-8", newline="\n") as output:
        json.dump(proof, output, indent=2)
        output.write("\n")
    print(json.dumps({"ok": proof["ok"], "evidence": str(args.evidence),
                      "failedStage": proof.get("failedStage")}))
    return 0 if proof["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
