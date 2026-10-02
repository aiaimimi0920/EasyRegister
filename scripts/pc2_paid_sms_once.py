"""Run exactly one existing continue flow in a separately configured container."""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import re
import traceback
from datetime import datetime, timezone
from pathlib import Path

from dst_flow import run_dst_flow_once
from others.common_runtime import validate_openai_oauth_seed_payload
from others.config_runtime_sections import SmsRuntimeConfig
from paid_sms_canary_guard import write_exclusive


def safe_label(value: object) -> str | None:
    return value if isinstance(value, str) and re.fullmatch(r"[a-zA-Z0-9_-]{1,100}", value) else None


def main() -> int:
    os.umask(0o077)
    manifest = json.loads(Path("/canary/manifest.json").read_text(encoding="utf-8"))
    root = Path(manifest["outputRoot"])
    if not root.is_absolute() or root.parent != Path("/shared/register-output"):
        raise RuntimeError("canary_output_root_invalid")
    if not root.name.startswith("paid-sms-canary-"):
        raise RuntimeError("canary_output_scope_invalid")
    if os.environ.get("SMS_SERVICE_BASE_URL") != manifest["smsBaseUrl"]:
        raise RuntimeError("canary_sms_route_mismatch")
    policy = SmsRuntimeConfig.from_env(default_state_path=root / "sms-state.json").resolve_business_policy("openai")
    if not policy.enabled or not policy.allow_paid or policy.allow_reuse:
        raise RuntimeError("canary_sms_policy_mismatch")
    if policy.max_price != manifest["maxPrice"] or policy.country_id != manifest["countryId"]:
        raise RuntimeError("canary_sms_price_or_country_mismatch")
    for variable in (
        "REGISTER_SMS_SESSION_LOCAL_RETRY_ATTEMPTS",
        "REGISTER_PHONE_VERIFICATION_TERMINAL_RETRY_ATTEMPTS",
        "REGISTER_PHONE_VERIFICATION_SMS_CODE_WAIT_RETRY_ATTEMPTS",
        "SMS_SERVICE_REQUEST_ATTEMPTS",
    ):
        if os.environ.get(variable) != "1":
            raise RuntimeError("canary_retry_limit_mismatch")
    if os.environ.get("REGISTER_PHONE_VERIFICATION_SAME_SESSION_CODE_RETRY_ATTEMPTS") != "0":
        raise RuntimeError("canary_sms_resend_must_be_disabled")
    seeds = list((root / "seed-pool").glob("*.json"))
    if len(seeds) != 1:
        raise RuntimeError("canary_requires_exactly_one_seed")
    raw_seed = seeds[0].read_bytes()
    valid, reason = validate_openai_oauth_seed_payload(json.loads(raw_seed))
    if not valid:
        raise RuntimeError("canary_seed_invalid:" + reason)
    flow = json.loads(Path("/canary/flow.json").read_text(encoding="utf-8"))["definition"]
    if flow.get("metadata", {}).get("taskRetry", {}).get("maxAttempts") != 1:
        raise RuntimeError("canary_flow_task_retry_not_bounded")
    if any(step.get("metadata", {}).get("retry", {}).get("maxAttempts", 1) != 1 for step in flow["steps"]):
        raise RuntimeError("canary_flow_step_retry_not_bounded")
    summary: dict[str, object] = {
        "startedAt": datetime.now(timezone.utc).isoformat(),
        "seedSha256": hashlib.sha256(raw_seed).hexdigest(),
        "seedAgeValid": True,
        "countryId": policy.country_id,
        "maxPriceUsd": policy.max_price,
    }
    write_exclusive(root / "run.started.json", summary)
    print(json.dumps({"event": "paid_sms_canary_started", **summary}), flush=True)
    with (root / "private-runtime.log").open("x", encoding="utf-8") as log:
        with contextlib.redirect_stdout(log), contextlib.redirect_stderr(log):
            try:
                result = run_dst_flow_once(
                    output_dir=str(root / "run"), flow_path="/canary/flow.json",
                    openai_oauth_pool_dir=str(root / "seed-pool"),
                    team_invite_enabled=False, r2_upload_enabled=False,
                    task_max_attempts=1, mailbox_business_key="openai",
                )
                payload = result.to_dict()
                write_exclusive(root / "private-result.json", payload)
                summary.update({
                    "ok": result.ok, "taskAttempts": result.task_attempts,
                    "errorStep": safe_label(result.error_step),
                    "steps": payload.get("steps", {}),
                    "stepAttempts": result.step_attempts,
                    "stepErrors": {
                        key: {field: safe_label(value.get(field)) for field in ("code", "stage", "category")}
                        for key, value in result.step_errors.items()
                    },
                })
            except Exception as exc:
                traceback.print_exc()
                summary.update({"ok": False, "exceptionType": type(exc).__name__})
    summary["finishedAt"] = datetime.now(timezone.utc).isoformat()
    write_exclusive(root / "summary.json", summary)
    print(json.dumps({"event": "paid_sms_canary_finished", **summary}), flush=True)
    return 0 if summary.get("ok") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
