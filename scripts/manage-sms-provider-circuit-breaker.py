#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ORCHESTRATION_SRC = ROOT / "server" / "services" / "orchestration_service" / "src"
PYTHON_SHARED_SRC = ROOT / "server" / "services" / "python_shared" / "src"
for source_root in (ORCHESTRATION_SRC, PYTHON_SHARED_SRC):
    source_text = str(source_root)
    if source_text not in sys.path:
        sys.path.insert(0, source_text)

from others import runtime_sms  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect or manually clear a paid SMS provider circuit breaker.",
    )
    parser.add_argument("action", choices=("status", "clear"))
    parser.add_argument("--provider", default="hero_sms")
    parser.add_argument("--state-path")
    parser.add_argument(
        "--confirm-fixed",
        action="store_true",
        help="Required for clear; confirms downstream code and isolated verification are complete.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    provider_key = str(args.provider or "").strip().lower()
    if not provider_key:
        raise SystemExit("--provider must not be empty")
    if args.state_path:
        os.environ["REGISTER_SMS_STATE_PATH"] = str(Path(args.state_path).expanduser().resolve())

    if args.action == "clear":
        if not args.confirm_fixed:
            raise SystemExit("clear requires --confirm-fixed")
        result = runtime_sms.clear_paid_provider_circuit_breaker(provider_key=provider_key)
        print(json.dumps(result, ensure_ascii=True, sort_keys=True))
        return 0

    config = runtime_sms._sms_runtime_config()
    state = runtime_sms._prune_sms_state(
        payload=runtime_sms._load_sms_state(config=config),
    )
    breakers = state.get(runtime_sms.PROVIDER_CIRCUIT_BREAKERS_KEY, {})
    raw_record = breakers.get(provider_key) if isinstance(breakers, dict) else None
    record = raw_record if isinstance(raw_record, dict) else {}
    print(
        json.dumps(
            {
                "providerKey": provider_key,
                "tripped": bool(record),
                "state": str(record.get("state") or ""),
                "reason": str(record.get("reason") or ""),
                "manualResetRequired": bool(record.get("manualResetRequired")),
                "businessKey": str(record.get("businessKey") or ""),
                "sessionId": str(record.get("sessionId") or ""),
                "failureStage": str(record.get("failureStage") or ""),
                "errorType": str(record.get("errorType") or ""),
                "trippedAt": str(record.get("trippedAt") or ""),
            },
            ensure_ascii=True,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
