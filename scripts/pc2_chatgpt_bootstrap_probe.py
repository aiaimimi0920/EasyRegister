"""Observe ChatGPT bootstrap endpoints on one lease without submitting an identity."""

from __future__ import annotations

import contextlib
import json
import os
import urllib.parse
import uuid
from datetime import datetime, timezone
from pathlib import Path

from new_protocol_register import protocol_chatgpt_login as flow
from protocol_runtime import protocol_register as browser_runtime


def main() -> int:
    os.umask(0o077)
    root = Path("/probe")
    result = {"startedAt": datetime.now(timezone.utc).isoformat()}
    proxy = json.loads((root / "private-lease.json").read_text())["proxy_url"]
    with (root / "private-runtime.log").open("x", encoding="utf-8") as log:
        with contextlib.redirect_stdout(log), contextlib.redirect_stderr(log):
            session = flow.requests.Session(impersonate="chrome", timeout=20, verify=False)
            session.headers.update({"user-agent": flow.DEFAULT_PROTOCOL_USER_AGENT})
            device_id = str(uuid.uuid4())
            flow.seed_device_cookie(session, device_id)
            driver = None

            def request(method: str, url: str, **kwargs):
                return flow._chatgpt_login_request(
                    session, method, url, explicit_proxy=proxy,
                    request_label="read-only-chatgpt-bootstrap", deadline=None, timeout=20, **kwargs,
                )

            def describe(response):
                parsed = urllib.parse.urlsplit(str(response.url))
                return {
                    "status": response.status_code,
                    "challenge": flow._response_has_cloudflare_challenge(response),
                    "origin": parsed.netloc, "path": parsed.path,
                }

            try:
                landing = request("GET", flow.CHATGPT_LOGIN_URL)
                result["landing"] = describe(landing)
                csrf = request("GET", flow.CHATGPT_NEXTAUTH_CSRF_URL, headers={
                    "accept": "application/json", "referer": flow.CHATGPT_LOGIN_URL,
                })
                result["csrf"] = describe(csrf)
                token = csrf.json().get("csrfToken", "") if csrf.status_code == 200 else ""
                result["csrfTokenPresent"] = bool(token)
                if token:
                    query = urllib.parse.urlencode({
                        "prompt": "login", "screen_hint": "login", "device_id": device_id,
                        "ext-oai-did": device_id, "auth_session_logging_id": str(uuid.uuid4()),
                    })
                    signin = request("POST", flow.CHATGPT_NEXTAUTH_SIGNIN_OPENAI_URL + "?" + query, headers={
                        "accept": "application/json", "content-type": "application/x-www-form-urlencoded",
                        "origin": flow.CHATGPT_BASE, "referer": flow.CHATGPT_LOGIN_URL,
                    }, data=urllib.parse.urlencode({
                        "csrfToken": token, "callbackUrl": "https://chatgpt.com/auth/login_with", "json": "true",
                    }))
                    result["signin"] = describe(signin)
                    auth_url = signin.json().get("url", "") if signin.status_code == 200 else ""
                    result["authUrlPresent"] = bool(auth_url)
                    if auth_url:
                        authorize = request("GET", auth_url)
                        result["authorize"] = describe(authorize)
                        result["loginSessionPresent"] = bool(flow._get_session_cookie(
                            session, "login_session", preferred_domains=("auth.openai.com", ".openai.com"),
                        ))
                driver, _ = browser_runtime._load_protocol_browser_new_driver()(proxy, browser_backend="custom")
                driver.set_page_load_timeout(25)
                driver.set_script_timeout(25)
                driver.get(flow.CHATGPT_LOGIN_URL)
                current = urllib.parse.urlsplit(driver.current_url)
                result["nativeLanding"] = {
                    "origin": current.netloc, "path": current.path,
                    "status": driver.execute_script("return performance.getEntriesByType('navigation')[0]?.responseStatus || 0"),
                }
                driver.save_screenshot(str(root / "private-browser.png"))
                (root / "private-page.html").write_text(driver.page_source, encoding="utf-8")
                native_csrf = driver.execute_async_script("""
                    const done = arguments[arguments.length - 1];
                    fetch('/api/auth/csrf', {credentials: 'include', headers: {Accept: 'application/json'}})
                    .then(async r => {
                        let data = {}; try { data = await r.json(); } catch (_) {}
                        done({status: r.status, token: data.csrfToken || ''});
                    }).catch(() => done({status: 0, token: ''}));
                """)
                result["nativeCsrf"] = {"status": native_csrf.get("status"), "tokenPresent": bool(native_csrf.get("token"))}
                if native_csrf.get("status") == 200 and native_csrf.get("token"):
                    native_signin = driver.execute_async_script("""
                        const args = arguments[0], done = arguments[arguments.length - 1];
                        const params = new URLSearchParams({prompt: 'login', screen_hint: 'login',
                            device_id: args.deviceId, 'ext-oai-did': args.deviceId,
                            auth_session_logging_id: args.loggingId});
                        fetch('/api/auth/signin/openai?' + params, {method: 'POST', credentials: 'include',
                            headers: {'Content-Type': 'application/x-www-form-urlencoded', Accept: 'application/json'},
                            body: new URLSearchParams({csrfToken: args.token, callbackUrl: 'https://chatgpt.com/auth/login_with', json: 'true'})
                        }).then(async r => {
                            let data = {}; try { data = await r.json(); } catch (_) {}
                            done({status: r.status, url: data.url || ''});
                        }).catch(() => done({status: 0, url: ''}));
                    """, {"deviceId": device_id, "loggingId": str(uuid.uuid4()), "token": native_csrf["token"]})
                    result["nativeSignin"] = {"status": native_signin.get("status"), "authUrlPresent": bool(native_signin.get("url"))}
                    if native_signin.get("status") == 200 and native_signin.get("url"):
                        driver.get(native_signin["url"])
                        current = urllib.parse.urlsplit(driver.current_url)
                        result["nativeAuthorize"] = {
                            "origin": current.netloc, "path": current.path,
                            "status": driver.execute_script("return performance.getEntriesByType('navigation')[0]?.responseStatus || 0"),
                            "loginSessionPresent": any(c.get("name") == "login_session" for c in driver.get_cookies()),
                        }
                        snapshot = driver.execute_cdp_cmd("Network.getAllCookies", {})
                        for cookie in snapshot.get("cookies", []):
                            session.cookies.set(
                                cookie["name"], cookie["value"], domain=cookie["domain"],
                                path=cookie.get("path", "/"), secure=cookie.get("secure", False),
                            )
                        session.headers.update({"user-agent": driver.execute_script("return navigator.userAgent")})
                        hydrated_csrf = request("GET", flow.CHATGPT_NEXTAUTH_CSRF_URL, headers={
                            "accept": "application/json", "referer": flow.CHATGPT_LOGIN_URL,
                        })
                        result["protocolCsrfAfterBrowser"] = describe(hydrated_csrf)
            except Exception as exc:
                result["exceptionType"] = type(exc).__name__
            finally:
                if driver is not None:
                    with contextlib.suppress(Exception):
                        driver.quit()
                session.close()
    result["finishedAt"] = datetime.now(timezone.utc).isoformat()
    (root / "summary.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
