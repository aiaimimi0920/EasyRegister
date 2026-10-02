"""Compare protocol and native-browser login on one proxy; never register or buy SMS."""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from new_protocol_register import protocol_small_success as protocol
from protocol_runtime import protocol_register as browser_runtime


def describe_trace(body: str) -> dict[str, str | None]:
    fields = dict(line.split("=", 1) for line in body.splitlines() if "=" in line)
    ip = fields.get("ip", "")
    return {
        "country": fields.get("loc"), "colo": fields.get("colo"),
        "ipHash": hashlib.sha256(ip.encode()).hexdigest()[:16] if ip else None,
    }


def protocol_login(session, proxy: str, auth_url: str = "") -> dict[str, object]:
    try:
        if auth_url:
            response = protocol._session_request(
                session, "GET", auth_url, explicit_proxy=proxy, request_label="read-only-auth-browser-probe",
                timeout=20, headers={
                    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "accept-language": protocol._PLATFORM_AUTH0_ACCEPT_LANGUAGE,
                    "upgrade-insecure-requests": "1",
                    "user-agent": str(session.headers.get("user-agent") or protocol.DEFAULT_PROTOCOL_USER_AGENT),
                    "sec-ch-ua": protocol._PLATFORM_AUTH0_SEC_CH_UA,
                    "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": '"Windows"',
                    "sec-fetch-dest": "document", "sec-fetch-mode": "navigate", "sec-fetch-site": "same-site",
                },
            )
            protocol._validate_platform_authorize_response(session=session, response=response)
        else:
            response = protocol._open_platform_login(session=session, explicit_proxy=proxy)
        return {
            "ok": True, "status": response.status_code,
            "loginSessionPresent": bool(protocol._login_session_cookie(session)),
        }
    except Exception as exc:
        return {
            "ok": False,
            "exceptionType": type(exc).__name__,
            "stage": getattr(exc, "stage", None),
            "browserChallenge": "browser_verification_required" in str(exc),
        }


def main() -> int:
    os.umask(0o077)
    root = Path("/probe")
    auth_url_path = os.environ.get("PROBE_AUTH_URL_FILE", "")
    auth_url = Path(auth_url_path).read_text(encoding="utf-8").strip() if auth_url_path else ""
    result: dict[str, object] = {"startedAt": datetime.now(timezone.utc).isoformat()}
    with (root / "private-runtime.log").open("x", encoding="utf-8") as log:
        with contextlib.redirect_stdout(log), contextlib.redirect_stderr(log):
            driver = None
            session = None
            try:
                with (root / "private-lease.json").open(encoding="utf-8") as lease_input:
                    proxy = json.load(lease_input)["proxy_url"]
                    print("[probe] protocol login", flush=True)
                    session = protocol.requests.Session(
                        impersonate=os.environ.get("PROTOCOL_HTTP_IMPERSONATE", "chrome"),
                        timeout=20, verify=False,
                    )
                    session.headers.update({"user-agent": protocol.DEFAULT_PROTOCOL_USER_AGENT})
                    if auth_url:
                        device_id = parse_qs(urlsplit(auth_url).query)["device_id"][0]
                        protocol.seed_device_cookie(session, device_id)
                    result["beforeBrowser"] = protocol_login(session, proxy, auth_url)
                    print("[probe] native browser startup", flush=True)
                    factory = browser_runtime._load_protocol_browser_new_driver()
                    driver, _ = factory(proxy, browser_backend="custom")
                    driver.set_page_load_timeout(25)
                    print("[probe] native browser navigation", flush=True)
                    try:
                        driver.get(auth_url or protocol.PLATFORM_LOGIN_URL)
                    except Exception as exc:
                        result["navigationExceptionType"] = type(exc).__name__
                    deadline = time.monotonic() + 30
                    while True:
                        body = driver.page_source.lower()
                        blocked = any(marker in body for marker in (
                            "sorry, you have been blocked", "attention required! | cloudflare",
                            "you are unable to access",
                        ))
                        challenge = any(marker in body for marker in (
                            "cf-chl-", "just a moment...", "verify you are human",
                        ))
                        if blocked or not challenge or time.monotonic() >= deadline:
                            break
                        time.sleep(2)
                    email_input_present = bool(driver.find_elements("css selector", "input[type=email]"))
                    navigation_status = driver.execute_script(
                        "return performance.getEntriesByType('navigation')[0]?.responseStatus || 0"
                    )
                    login_session_present = any(
                        cookie.get("name") == "login_session" for cookie in driver.get_cookies()
                    )
                    current = urlsplit(driver.current_url)
                    result["nativeBrowser"] = {
                        "origin": current.netloc,
                        "path": current.path,
                        "navigationStatus": navigation_status,
                        "challengePresent": challenge,
                        "blockedPagePresent": blocked,
                        "emailInputPresent": email_input_present,
                        "loginSessionPresent": login_session_present,
                    }
                    driver.save_screenshot(str(root / "private-browser.png"))
                    (root / "private-page.html").write_text(driver.page_source, encoding="utf-8")
                    usable_auth_page = email_input_present or current.path in {
                        "/email-verification", "/create-account/password", "/log-in/password",
                    }
                    if (
                        not challenge and not blocked and usable_auth_page
                        and current.netloc == "auth.openai.com"
                        and navigation_status == 200 and login_session_present
                    ):
                        expected = parse_qs(urlsplit(auth_url).query).get("login_hint", [""])[0]
                        native_auth_sessions = []
                        for cookie in driver.get_cookies():
                            if cookie.get("name") != "oai-client-auth-session":
                                continue
                            value = str(cookie.get("value", ""))
                            decoded = protocol._decode_cookie_payload(value)
                            uri_decoded = protocol._decode_cookie_payload(protocol.urllib.parse.unquote(value))
                            user = decoded.get("username", {})
                            uri_user = uri_decoded.get("username", {})
                            hint = decoded.get("original_screen_hint", "")
                            (root / "private-auth-snapshot.json").write_text(json.dumps({
                                "cookie": value, "expectedEmail": expected,
                            }), encoding="utf-8")
                            segments = []
                            for index, part in enumerate(value.split(".")):
                                decoded_part = protocol._decode_cookie_payload(part)
                                part_user = decoded_part.get("username", {})
                                segments.append({
                                    "index": index,
                                    "keys": [key for key in decoded_part if key in {
                                        "alg", "typ", "username", "original_screen_hint", "client_id",
                                        "email", "state", "session_id", "iss", "sub", "data",
                                    }],
                                    "identityMatches": isinstance(part_user, dict) and str(part_user.get("value") or "").lower() == expected.lower(),
                                    "screenHintMatches": decoded_part.get("original_screen_hint") == "login_or_signup",
                                })
                            native_auth_sessions.append({
                                "domain": cookie.get("domain"),
                                "identityMatches": isinstance(user, dict) and str(user.get("value") or "").lower() == expected.lower(),
                                "screenHint": hint if hint in {"login_or_signup", "login", "signup", ""} else "other",
                                "rawDecoded": bool(decoded),
                                "urlDecodedIdentityMatches": isinstance(uri_user, dict) and str(uri_user.get("value") or "").lower() == expected.lower(),
                                "percentEncoded": "%" in value,
                                "periodCount": value.count("."),
                                "segments": segments,
                            })
                        result["nativeAuthSessions"] = native_auth_sessions
                        imported = browser_runtime._import_browser_driver_cookies_into_session(session, driver=driver)
                        payload = protocol._decode_current_auth_session_payload(session)
                        username = payload.get("username", {})
                        result["authIdentityMatches"] = (
                            bool(expected) and isinstance(username, dict)
                            and str(username.get("value", "")).lower() == expected.lower()
                        )
                        user_agent = driver.execute_script("return navigator.userAgent")
                        session.headers.update({"user-agent": user_agent})
                        result["importedCookieCount"] = imported
                        result["afterBrowser"] = protocol_login(session, proxy, auth_url)
                    if os.environ.get("PROBE_EGRESS") == "1":
                        egress: dict[str, object] = {}
                        result["egress"] = egress
                        for host in ("auth.openai.com", "www.cloudflare.com"):
                            url = "https://" + host + "/cdn-cgi/trace"
                            observation: dict[str, object] = {}
                            egress[host] = observation
                            try:
                                response = protocol._session_request(
                                    session, "GET", url, explicit_proxy=proxy,
                                    request_label="read-only-egress", timeout=15,
                                )
                                observation["protocol"] = describe_trace(response.text)
                            except Exception as exc:
                                observation["protocolException"] = type(exc).__name__
                            try:
                                driver.get(url)
                                observation["browser"] = describe_trace(driver.find_element("tag name", "body").text)
                            except Exception as exc:
                                observation["browserException"] = type(exc).__name__
                    result["probeCompleted"] = True
            except Exception as exc:
                traceback.print_exc()
                result.update({"probeCompleted": False, "exceptionType": type(exc).__name__})
            finally:
                if driver is not None:
                    with contextlib.suppress(Exception):
                        driver.quit()
                if session is not None:
                    session.close()
    result["finishedAt"] = datetime.now(timezone.utc).isoformat()
    with (root / "summary.json").open("x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2)
    print(json.dumps(result), flush=True)
    return 0 if result.get("probeCompleted") else 1


if __name__ == "__main__":
    raise SystemExit(main())
