"""Observe strict flow preflight decisions without submitting an account or SMS."""

from __future__ import annotations

import contextlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

import easyproxy_flow


def main() -> int:
    os.umask(0o077)
    root = Path("/probe")
    summary: dict[str, object] = {"startedAt": datetime.now(timezone.utc).isoformat()}
    with (root / "run.started.json").open("x", encoding="utf-8") as stream:
        json.dump(summary, stream)
    if os.environ.get("REGISTER_SMS_ALLOW_PAID") != "false":
        raise RuntimeError("read_only_probe_requires_paid_sms_disabled")
    flow_path = Path(os.environ.get("PROBE_FLOW_PATH", "/app/server/services/orchestration_service/flows/codex-openai-account-v1.semantic-flow.json"))
    definition = json.loads(flow_path.read_text(encoding="utf-8"))["definition"]
    step = next(s for s in definition["steps"] if s["type"] == "acquire_proxy_chain")
    step_input = step["input"]
    step_input["max_acquire_attempts"] = 1
    step_input["metadata"] = {"diagnostic": "strict_chatgpt_preflight"}
    assert step_input["allow_openai_auth_challenge"] is False
    assert step_input["probe_expected_statuses"] == [200]
    acquire = easyproxy_flow.acquire_flow_proxy_lease
    original_probe = acquire.__globals__["_probe_flow_proxy"]
    probes = []

    def record_probe(**kwargs):
        parsed = urlsplit(kwargs["probe_url"])
        observation = {
            "origin": parsed.netloc, "path": parsed.path,
            "acceptChallenge": bool(kwargs.get("allow_openai_auth_challenge", False)),
        }
        probes.append(observation)
        try:
            original_probe(**kwargs)
            observation["accepted"] = True
        except Exception as exc:
            observation["accepted"] = False
            status = re.search(r"status=(\d{3})", str(exc))
            if status:
                observation["status"] = int(status.group(1))
            observation["exceptionType"] = type(exc).__name__
            raise

    def record_acquire(**kwargs):
        summary["explicitFalseForwarded"] = kwargs.get("allow_openai_auth_challenge") is False
        return acquire(**kwargs)

    acquire.__globals__["_probe_flow_proxy"] = record_probe
    easyproxy_flow.acquire_flow_proxy_lease = record_acquire
    lease = None
    with (root / "private-runtime.log").open("x", encoding="utf-8") as log:
        with contextlib.redirect_stdout(log), contextlib.redirect_stderr(log):
            try:
                payload = easyproxy_flow.dispatch_easyproxy_step(step_type="acquire_proxy_chain", step_input=step_input)
                lease = easyproxy_flow.FlowProxyLease.from_payload(payload)
                summary["ok"] = bool(lease.proxy_url) and summary.get("explicitFalseForwarded") is True
            except Exception as exc:
                summary.update({"ok": False, "exceptionType": type(exc).__name__})
            finally:
                if lease is not None:
                    easyproxy_flow.release_flow_proxy_lease(lease, success=summary.get("ok") is True)
                    summary["leaseReleased"] = True
                acquire.__globals__["_probe_flow_proxy"] = original_probe
                easyproxy_flow.acquire_flow_proxy_lease = acquire
    summary["probes"] = probes
    summary["finishedAt"] = datetime.now(timezone.utc).isoformat()
    with (root / "summary.json").open("x", encoding="utf-8") as stream:
        json.dump(summary, stream, indent=2)
    print(json.dumps(summary), flush=True)
    return 0 if summary.get("ok") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
