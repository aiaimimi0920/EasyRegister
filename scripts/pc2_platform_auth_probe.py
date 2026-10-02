"""Compare the public landing page and actual OAuth entry without registering."""

from __future__ import annotations

import contextlib
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

from new_protocol_register import protocol_small_success as protocol


def describe_response(response, session) -> dict[str, object]:
    body = str(response.text or "").lower()
    return {
        "status": response.status_code,
        "origin": urlsplit(response.url).netloc,
        "path": urlsplit(response.url).path,
        "challenge": str(response.headers.get("cf-mitigated", "")).lower() == "challenge",
        "blockedPage": "sorry, you have been blocked" in body,
        "loginSessionPresent": bool(protocol._login_session_cookie(session)),
        "authSessionPresent": bool(protocol._decode_current_auth_session_payload(session)),
        "emailInputPresent": 'type="email"' in body or "type='email'" in body,
    }


def main() -> int:
    os.umask(0o077)
    root = Path("/probe")
    result: dict[str, object] = {"startedAt": datetime.now(timezone.utc).isoformat()}
    with (root / "private-runtime.log").open("x", encoding="utf-8") as log:
        with contextlib.redirect_stdout(log), contextlib.redirect_stderr(log):
            proxy = json.loads((root / "private-lease.json").read_text(encoding="utf-8"))["proxy_url"]
            for mode in ("explicitUserAgent", "profileUserAgent"):
                observations: dict[str, object] = {}
                result[mode] = observations
                with protocol.requests.Session(
                    impersonate=os.environ.get("PROTOCOL_HTTP_IMPERSONATE", "chrome"),
                    timeout=20, verify=False,
                ) as session:
                    headers = {
                        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        "accept-language": "en-US,en;q=0.9",
                    }
                    if mode == "explicitUserAgent":
                        headers["user-agent"] = protocol.DEFAULT_PROTOCOL_USER_AGENT
                        session.headers.update({"user-agent": protocol.DEFAULT_PROTOCOL_USER_AGENT})
                    device_id = str(uuid.uuid4())
                    protocol.seed_device_cookie(session, device_id)
                    context = protocol._build_platform_auth0_authorize_context(email="", device_id=device_id)
                    targets = (("landingPage", protocol.PLATFORM_LOGIN_URL), ("oauthEntry", context["url"]))
                    for label, url in targets:
                        try:
                            response = protocol._session_request(
                                session, "GET", url, explicit_proxy=proxy, request_label="read-only-" + label,
                                timeout=20, headers=headers,
                            )
                            observations[label] = describe_response(response, session)
                        except Exception as exc:
                            observations[label] = {"exceptionType": type(exc).__name__}
    result["finishedAt"] = datetime.now(timezone.utc).isoformat()
    (root / "summary.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
