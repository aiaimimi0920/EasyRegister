"""Verify one anonymous ChatGPT bootstrap with the candidate browser Session."""

from __future__ import annotations

import contextlib
import json
import os
import time
import traceback
import urllib.parse
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from selenium import webdriver

from new_protocol_register import protocol_chatgpt_login as flow
from new_protocol_register.protocol_browser_session import BrowserLoginSession, _navigate_html


class ObservedSession(BrowserLoginSession):
    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)
        self.observations: list[dict[str, object]] = []
        self.fetch_diagnostic: dict[str, object] | None = None

    def _ensure_browser(self, proxy):
        if self._driver is not None or os.environ.get("PROBE_NETWORK_DIAGNOSTIC") != "1":
            return super()._ensure_browser(proxy)
        create = webdriver.Chrome

        def instrumented_chrome(*args, **kwargs):
            kwargs["options"].set_capability("goog:loggingPrefs", {"performance": "ALL"})
            return create(*args, **kwargs)

        with patch.object(webdriver, "Chrome", side_effect=instrumented_chrome):
            return super()._ensure_browser(proxy)

    def request(self, method: str, url: str, **kwargs: object) -> flow.requests.Response:
        response = super().request(method, url, **kwargs)
        parsed = urllib.parse.urlsplit(url)
        self.observations.append({
            "method": method.upper(), "origin": parsed.hostname, "path": parsed.path,
            "status": response.status_code, "challenge": flow._response_has_cloudflare_challenge(response),
            "browserUsed": self._driver is not None,
        })
        return response

    def _native_request(self, method, url, kwargs):
        try:
            return super()._native_request(method, url, kwargs)
        except Exception:
            traceback.print_exc()
            if (
                os.environ.get("PROBE_FETCH_DIAGNOSTIC") == "1" and self._driver is not None
                and method.upper() == "GET" and url == flow.CHATGPT_NEXTAUTH_CSRF_URL
            ):
                driver = self._driver
                driver.set_script_timeout(25)
                self.fetch_diagnostic = driver.execute_async_script("""
                    const done = arguments[arguments.length - 1];
                    const result = {readyState: document.readyState,
                        nativeFetch: /\\[native code\\]/.test(Function.prototype.toString.call(fetch)),
                        requests: []};
                    (async () => {
                        for (const redirect of ['follow', 'error']) {
                            const started = performance.now();
                            const controller = new AbortController();
                            const timer = setTimeout(() => controller.abort(), 8000);
                            try {
                                const response = await fetch('/api/auth/csrf', {credentials: 'include',
                                    headers: {Accept: 'application/json'}, redirect, signal: controller.signal});
                                let body = {}; try { body = await response.json(); } catch (_) {}
                                result.requests.push({redirect, status: response.status,
                                    redirected: response.redirected, csrfPresent: Boolean(body.csrfToken),
                                    elapsedMs: Math.round(performance.now() - started)});
                            } catch (error) {
                                result.requests.push({redirect, status: 0, errorType: error.name,
                                    errorMessage: String(error.message),
                                    elapsedMs: Math.round(performance.now() - started)});
                            } finally { clearTimeout(timer); }
                        }
                        result.readyStateAfter = document.readyState;
                        done(result);
                    })().catch(error => done({errorType: error.name}));
                """)
                if os.environ.get("PROBE_NETWORK_DIAGNOSTIC") == "1":
                    with contextlib.suppress(Exception):
                        _navigate_html(driver, flow.CHATGPT_NEXTAUTH_CSRF_URL, 20)
                        status = driver.execute_script(
                            "return performance.getEntriesByType('navigation')[0]?.responseStatus || 0",
                        )
                        body = json.loads(driver.find_element("tag name", "body").text)
                        self.fetch_diagnostic["navigationCsrf"] = {
                            "status": status, "csrfPresent": bool(body.get("csrfToken")),
                        }
            raise


def main() -> int:
    os.umask(0o077)
    root = Path("/probe")
    summary: dict[str, object] = {"startedAt": datetime.now(timezone.utc).isoformat()}
    with (root / "run.started.json").open("x", encoding="utf-8") as stream:
        json.dump(summary, stream)
    assert os.environ.get("REGISTER_SMS_ALLOW_PAID") == "false"
    proxy = json.loads((root / "private-lease.json").read_text(encoding="utf-8"))["proxy_url"]
    session = ObservedSession(impersonate="chrome", timeout=25, verify=False)
    session.headers.update({"user-agent": flow.DEFAULT_PROTOCOL_USER_AGENT})
    device_id = str(uuid.uuid4())
    flow.seed_device_cookie(session, device_id)
    with (root / "private-runtime.log").open("x", encoding="utf-8") as log:
        with contextlib.redirect_stdout(log), contextlib.redirect_stderr(log):
            try:
                summary["nativeTransportRequired"] = os.environ.get("PROBE_BROWSER_TRANSPORT") == "1"
                if summary["nativeTransportRequired"]:
                    session._ensure_browser(proxy)
                auth_url = flow._bootstrap_chatgpt_login_with_redirect(
                    session=session, device_id=device_id, explicit_proxy=proxy,
                )
                summary["nextAuthStatePresent"] = bool(flow._get_session_cookie(
                    session, "__Secure-next-auth.state", preferred_domains=("chatgpt.com", ".chatgpt.com"),
                ))
                authorize = flow._chatgpt_login_request(
                    session, "GET", auth_url, explicit_proxy=proxy,
                    request_label="anonymous-browser-login-authorize", deadline=None, timeout=25,
                )
                summary["loginSessionPresent"] = bool(flow._get_session_cookie(
                    session, "login_session", preferred_domains=("auth.openai.com", ".openai.com"),
                ))
                if session._driver is not None and not summary["loginSessionPresent"]:
                    driver = session._driver
                    current = urllib.parse.urlsplit(driver.current_url)
                    summary["initialAuthPage"] = {"origin": current.hostname, "path": current.path}
                    (root / "private-page.html").write_text(driver.page_source, encoding="utf-8")
                    snapshot = driver.execute_cdp_cmd("Network.getAllCookies", {})
                    summary["initialCookieNames"] = sorted({item["name"] for item in snapshot.get("cookies", [])})
                    started = time.monotonic()
                    found = False
                    while time.monotonic() - started < 8:
                        time.sleep(0.25)
                        session._sync_application_cookies(driver)
                        found = bool(flow._get_session_cookie(session, "login_session"))
                        if found:
                            break
                    current = urllib.parse.urlsplit(driver.current_url)
                    summary["delayedAuthObservation"] = {
                        "loginSessionPresent": found, "waitSeconds": round(time.monotonic() - started, 2),
                        "origin": current.hostname, "path": current.path,
                        "status": driver.execute_script("return performance.getEntriesByType('navigation')[0]?.responseStatus || 0"),
                    }
                summary["ok"] = (
                    authorize.status_code == 200 and summary["nextAuthStatePresent"]
                    and summary["loginSessionPresent"] and all(item["status"] == 200 for item in session.observations)
                )
            except Exception as exc:
                summary.update({"ok": False, "exceptionType": type(exc).__name__})
                traceback.print_exc()
                if session._driver is not None:
                    (root / "private-page.html").write_text(session._driver.page_source, encoding="utf-8")
            finally:
                summary["requests"] = session.observations
                if session.fetch_diagnostic is not None:
                    (root / "private-fetch-diagnostic.json").write_text(
                        json.dumps(session.fetch_diagnostic), encoding="utf-8",
                    )
                if os.environ.get("PROBE_NETWORK_DIAGNOSTIC") == "1" and session._driver is not None:
                    with contextlib.suppress(Exception):
                        (root / "private-network.json").write_text(
                            json.dumps(session._driver.get_log("performance")), encoding="utf-8",
                        )
                session.close()
                summary["sessionClosed"] = session._driver is None and session._profile is None
    summary["finishedAt"] = datetime.now(timezone.utc).isoformat()
    with (root / "summary.json").open("x", encoding="utf-8") as stream:
        json.dump(summary, stream, indent=2)
    print(json.dumps(summary), flush=True)
    return 0 if summary.get("ok") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
