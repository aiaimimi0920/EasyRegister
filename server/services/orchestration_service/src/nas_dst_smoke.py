"""Opt-in, redacted acceptance of NAS services through the real DST engine."""
from __future__ import annotations

import contextlib
import io
import json
import os
import re
import tempfile
import urllib.parse
from pathlib import Path

from others.bootstrap import ensure_local_bundle_imports

ensure_local_bundle_imports()

from dst_flow import run_dst_flow_once
from others.runtime_mailbox import ensure_easy_email_env_defaults
from shared_mailbox import easy_email_client as mail
from shared_proxy import easy_proxy_client as proxy


FLOW_PATH = Path(__file__).resolve().parents[1] / "flows" / "nas-dependencies-smoke-v1.semantic-flow.json"


def main() -> int:
    summary: dict[str, object] = {"scope": "real DST dependency flow; no account registration"}
    ensure_easy_email_env_defaults()
    required = ("EASY_EMAIL_API_KEY", "EASY_PROXY_MANAGEMENT_PASSWORD")
    missing = [name for name in required if not str(os.environ.get(name) or "").strip()]
    if missing:
        print(json.dumps({**summary, "ok": False, "missingEnvironment": missing}))
        return 1

    result = None
    cleanup_errors: list[str] = []
    try:
        with tempfile.TemporaryDirectory(prefix="easyregister-nas-dst-") as tmp:
            # This module is a one-shot diagnostic process, not a scheduler.
            os.environ.update({
                "REGISTER_OUTPUT_ROOT": str(Path(tmp) / "output"),
                "REGISTER_MAILBOX_DOMAIN_STATE_PATH": str(Path(tmp) / "mailbox-state.json"),
                "REGISTER_MAILBOX_BUSINESS_RETRY_ATTEMPTS": "1",
                "REGISTER_MAILBOX_PROVIDERS": "cloudflare_temp_email",
                "REGISTER_MAILBOX_TTL_SECONDS": "300",
                "REGISTER_MAILBOX_PROVIDER_BLACKLIST": "",
                "REGISTER_MAILBOX_DOMAIN_BLACKLIST": "",
                "REGISTER_MAILBOX_DOMAIN_POOL": "",
                "REGISTER_MAILBOX_BUSINESS_POLICIES_JSON": "{}",
                "MAILBOX_SERVICE_REQUEST_ATTEMPTS": "1",
                "MAILBOX_SERVICE_READY_TIMEOUT_SECONDS": "10",
                "REGISTER_ENABLE_EASY_PROXY": "true",
                "REGISTER_REQUIRE_EASY_PROXY": "true",
                "REGISTER_PROXY_MODE": "lease",
                "REGISTER_PROXY_TTL_MINUTES": "2",
                "REGISTER_PROXY_UNIQUE_ATTEMPTS": "1",
            })
            # The ordinary DST event log contains resource identities. Only the
            # allowlisted summary below leaves this diagnostic process.
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                result = run_dst_flow_once(
                    flow_path=str(FLOW_PATH), output_dir=str(Path(tmp) / "output"),
                    task_max_attempts=1, team_invite_enabled=False, r2_upload_enabled=False,
                )
            summary["steps"] = result.steps
            summary["errorMarkers"] = {
                step: re.findall(r"HTTP \d{3}|\[code=[A-Z0-9_]+\]|mailbox_recover_by_email_failed", str(error))
                for step, error in result.step_errors.items()
            }
            summary["taskAttempts"] = result.task_attempts
            mailbox = result.outputs.get("acquire-mailbox") or {}
            recovered = result.outputs.get("recover-mailbox") or {}
            summary["recoveryIdentityPreserved"] = bool(
                mailbox.get("email") and mailbox.get("email") == recovered.get("email")
                and recovered.get("session_id") and recovered.get("recovery_data_credential")
            )
            lease = result.outputs.get("acquire-proxy-chain") or {}
            summary["leasedProxyUsed"] = bool(lease.get("lease_id") and lease.get("proxy_url"))
            summary["ok"] = bool(
                result.ok and len(result.steps) == 6
                and all(status == "ok" for status in result.steps.values())
                and summary["recoveryIdentityPreserved"] and summary["leasedProxyUsed"]
            )
    except Exception as exc:
        summary["ok"] = False
        summary["errorType"] = type(exc).__name__
    finally:
        # Recheck cleanup instead of treating a quiet-release wrapper as proof.
        if result is not None:
            session_ids = {
                str((result.outputs.get(step) or {}).get("session_id") or "")
                for step in ("acquire-mailbox", "recover-mailbox")
            } - {""}
            for session_id in session_ids:
                try:
                    mail.release_mailbox(session_id=session_id, reason="nas-dst-smoke-finalize")
                    query = urllib.parse.urlencode({"status": "expired", "limit": 100, "newestFirst": "true"})
                    sessions = mail._get_json("/mail/query/mailbox-sessions?" + query).get("sessions", [])
                    if not any(item.get("id") == session_id and item.get("status") == "expired" for item in sessions):
                        cleanup_errors.append("mailbox_expiration_unverified")
                except Exception as exc:
                    cleanup_errors.append("mailbox_" + type(exc).__name__)
            lease_id = str((result.outputs.get("acquire-proxy-chain") or {}).get("lease_id") or "")
            if lease_id:
                try:
                    response = proxy._api_request("POST", f"/proxy/leases/{lease_id}/release", {}, wait_for_ready=False)
                    if response.get("ok") is not True:
                        cleanup_errors.append("proxy_release_unacknowledged")
                except Exception as exc:
                    cleanup_errors.append("proxy_" + type(exc).__name__)
    summary["cleanupErrors"] = cleanup_errors
    summary["ok"] = bool(summary.get("ok") and not cleanup_errors)
    print(json.dumps(summary, ensure_ascii=True, indent=2))
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
