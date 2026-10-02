"""Run one SMS-disabled seed flow using the isolated canary environment."""

from __future__ import annotations

import contextlib
import json
import os
import sys
import traceback
from pathlib import Path

sys.path.insert(0, "/canary")

from dst_flow import run_dst_flow_once
from paid_sms_canary_guard import write_exclusive


def main() -> int:
    os.umask(0o077)
    root = Path(os.environ["REGISTER_OUTPUT_ROOT"]).parent
    if not root.is_relative_to(Path("/shared/register-output/paid-sms-canary-20260912-001")):
        raise RuntimeError("seed_output_outside_canary")
    if os.environ["REGISTER_SMS_ALLOW_PAID"] != "false" or os.environ.get("REGISTER_SMS_BUSINESS_POLICIES_JSON"):
        raise RuntimeError("seed_flow_must_disable_paid_sms")
    flow_path = os.environ["REGISTER_FLOW_PATH"]
    definition = json.loads(Path(flow_path).read_text(encoding="utf-8"))["definition"]
    if definition.get("metadata", {}).get("taskRetry", {}).get("maxAttempts") != 1:
        raise RuntimeError("seed_task_attempts_not_bounded")
    if any(step.get("metadata", {}).get("retry", {}).get("maxAttempts", 1) != 1 for step in definition["steps"]):
        raise RuntimeError("seed_step_attempts_not_bounded")
    write_exclusive(root / "run.started.json", {"paidSms": False, "taskAttemptsLimit": 1})
    with (root / "private-runtime.log").open("x", encoding="utf-8") as log:
        with contextlib.redirect_stdout(log), contextlib.redirect_stderr(log):
            try:
                result = run_dst_flow_once(
                    output_dir=str(root / "run"), flow_path=flow_path,
                    team_invite_enabled=False, r2_upload_enabled=False,
                    task_max_attempts=1, mailbox_business_key="openai",
                )
                write_exclusive(root / "private-result.json", result.to_dict())
                summary = {
                    "ok": result.ok, "taskAttempts": result.task_attempts,
                    "steps": result.steps, "errorStep": result.error_step,
                    "stepErrors": {
                        key: {field: value.get(field) for field in ("code", "stage", "category")}
                        for key, value in result.step_errors.items()
                    },
                }
            except Exception as exc:
                traceback.print_exc()
                summary = {"ok": False, "exceptionType": type(exc).__name__}
    write_exclusive(root / "summary.json", summary)
    print(json.dumps(summary), flush=True)
    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
