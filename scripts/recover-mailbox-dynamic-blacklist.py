#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


TRANSIENT_BLACKLIST_REASONS = {
    "email_otp_failure_threshold",
    "provider_email_otp_failure_threshold",
    "failure_rate_threshold",
    "provider_failure_rate_threshold",
    "provider_consecutive_failures_threshold",
    "consecutive_failures_threshold",
}
STRONG_BLACKLIST_REASONS = {
    "unsupported_email",
    "registration_disallowed",
}
THRESHOLD_REEVALUATED_FAILURE_REASONS = {
    "email_otp_timeout",
    "email_otp_wrong_code",
}
PROVIDER_CONSECUTIVE_BLACKLIST_FAILURE_REASONS = (
    THRESHOLD_REEVALUATED_FAILURE_REASONS
    | STRONG_BLACKLIST_REASONS
    | {"create_account_user_register_400"}
)
PROVIDER_RISK_MIN_ATTEMPTS = 20
PROVIDER_RISK_FAILURE_RATE = 90.0
PROVIDER_BLACKLIST_RECOVERY_MIN_SUCCESSES = 10
PROVIDER_BLACKLIST_RECOVERY_MIN_SUCCESS_RATE = 20.0
DEFAULT_PROVIDER_BLACKLIST_RECOVERY_MAX_CONSECUTIVE_OTP_FAILURES = 6


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _business_payloads(payload: dict[str, Any], business_keys: tuple[str, ...]) -> list[tuple[str, dict[str, Any]]]:
    businesses = _as_dict(payload.get("businesses"))
    if not business_keys:
        business_keys = tuple(str(key or "").strip() for key in businesses if str(key or "").strip())
    result: list[tuple[str, dict[str, Any]]] = []
    for business_key in business_keys:
        business_payload = businesses.get(business_key)
        if isinstance(business_payload, dict):
            result.append((business_key, business_payload))
    return result


def _merge_counts(target: dict[str, Any], source: dict[str, Any]) -> None:
    for key, value in source.items():
        try:
            count = max(0, int(value or 0))
        except Exception:
            continue
        if count <= 0:
            continue
        try:
            existing = max(0, int(target.get(key) or 0))
        except Exception:
            existing = 0
        target[key] = existing + count


def _entry_int(entry: dict[str, Any], key: str) -> int:
    try:
        return max(0, int(entry.get(key) or 0))
    except Exception:
        return 0


def _count_total(values: dict[str, Any]) -> int:
    total = 0
    for value in values.values():
        try:
            total += max(0, int(value or 0))
        except Exception:
            continue
    return total


def _count_reasons(values: dict[str, Any], reasons: set[str]) -> int:
    total = 0
    for reason in reasons:
        try:
            total += max(0, int(values.get(reason) or 0))
        except Exception:
            continue
    return total


def _provider_blacklist_recovery_max_consecutive_otp_failures() -> int:
    raw = str(
        os.environ.get("REGISTER_MAILBOX_EMAIL_OTP_PROVIDER_FAILURE_BLACKLIST_THRESHOLD")
        or DEFAULT_PROVIDER_BLACKLIST_RECOVERY_MAX_CONSECUTIVE_OTP_FAILURES
    ).strip()
    try:
        return max(0, int(raw))
    except Exception:
        return DEFAULT_PROVIDER_BLACKLIST_RECOVERY_MAX_CONSECUTIVE_OTP_FAILURES


def _preserve_provider_risk(entry: dict[str, Any]) -> bool:
    attempts = _entry_int(entry, "attempts")
    successes = _entry_int(entry, "successes")
    failures = _entry_int(entry, "failures")
    consecutive_failures = _entry_int(entry, "consecutiveFailures")
    failure_reasons = _as_dict(entry.get("failureReasons"))
    max_consecutive_otp_failures = _provider_blacklist_recovery_max_consecutive_otp_failures()
    if (
        max_consecutive_otp_failures > 0
        and consecutive_failures >= max_consecutive_otp_failures
        and _count_reasons(failure_reasons, PROVIDER_CONSECUTIVE_BLACKLIST_FAILURE_REASONS)
        >= max_consecutive_otp_failures
    ):
        return True
    if attempts < PROVIDER_RISK_MIN_ATTEMPTS or successes > 0:
        return False
    failure_rate = (float(failures) / float(attempts)) * 100.0 if attempts else 0.0
    return failure_rate >= PROVIDER_RISK_FAILURE_RATE


def _provider_recovery_qualified(entry: dict[str, Any]) -> bool:
    if _preserve_provider_risk(entry):
        return False
    attempts = _entry_int(entry, "attempts")
    successes = _entry_int(entry, "successes")
    failures = _entry_int(entry, "failures")
    if attempts <= 0 or successes < PROVIDER_BLACKLIST_RECOVERY_MIN_SUCCESSES:
        return False
    success_rate = (float(successes) / float(attempts)) * 100.0
    if success_rate < PROVIDER_BLACKLIST_RECOVERY_MIN_SUCCESS_RATE:
        return False
    failure_rate = (float(failures) / float(attempts)) * 100.0
    return failure_rate < PROVIDER_RISK_FAILURE_RATE


def _recover_entry(entry: dict[str, Any], *, section_name: str) -> tuple[bool, bool, bool, int]:
    blacklist_reason = str(entry.get("blacklistReason") or "").strip().lower()
    failure_reasons = _as_dict(entry.get("failureReasons"))
    if blacklist_reason in STRONG_BLACKLIST_REASONS:
        return False, True, False, 0
    if any(reason in failure_reasons for reason in STRONG_BLACKLIST_REASONS):
        return False, True, False, 0
    if section_name == "providers" and (
        _preserve_provider_risk(entry) or not _provider_recovery_qualified(entry)
    ):
        return False, False, True, 0
    if not bool(entry.get("blacklisted")) and blacklist_reason not in TRANSIENT_BLACKLIST_REASONS:
        return False, False, False, 0
    if blacklist_reason and blacklist_reason not in TRANSIENT_BLACKLIST_REASONS:
        return False, False, False, 0

    suppressed = _as_dict(entry.get("suppressedFailureReasons"))
    remaining_reasons: dict[str, Any] = {}
    moved_reasons: dict[str, Any] = {}
    for reason, count in failure_reasons.items():
        normalized_reason = str(reason or "").strip().lower()
        if normalized_reason in THRESHOLD_REEVALUATED_FAILURE_REASONS:
            moved_reasons[normalized_reason] = count
        else:
            remaining_reasons[normalized_reason] = count

    _merge_counts(suppressed, moved_reasons)
    entry["blacklisted"] = False
    entry["blacklistReason"] = ""
    entry["failureReasons"] = remaining_reasons
    if suppressed:
        entry["suppressedFailureReasons"] = suppressed
    return True, False, False, len(moved_reasons)


def recover_payload(payload: dict[str, Any], *, business_keys: tuple[str, ...] = ("openai",)) -> dict[str, Any]:
    summary = {
        "businessKeys": list(business_keys),
        "recoveredEntries": 0,
        "preservedStrongEntries": 0,
        "preservedProviderRiskEntries": 0,
        "suppressedFailureReasonEntries": 0,
        "sections": {
            "domains": 0,
            "providers": 0,
        },
    }
    for _business_key, business_payload in _business_payloads(payload, business_keys):
        for section_name in ("domains", "providers"):
            section = _as_dict(business_payload.get(section_name))
            for entry in section.values():
                if not isinstance(entry, dict):
                    continue
                recovered, preserved_strong, preserved_provider_risk, suppressed_count = _recover_entry(
                    entry,
                    section_name=section_name,
                )
                if recovered:
                    summary["recoveredEntries"] += 1
                    summary["suppressedFailureReasonEntries"] += suppressed_count
                    summary["sections"][section_name] += 1
                elif preserved_strong:
                    summary["preservedStrongEntries"] += 1
                elif preserved_provider_risk:
                    summary["preservedProviderRiskEntries"] += 1
    payload["updatedAt"] = datetime.now(timezone.utc).isoformat()
    return summary


def apply_recovery(
    state_path: Path,
    *,
    business_keys: tuple[str, ...] = ("openai",),
    timestamp_slug: str | None = None,
) -> dict[str, Any]:
    path = Path(state_path).resolve()
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"state payload is not an object: {path}")
    slug = timestamp_slug or datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    backup_path = path.with_name(f"{path.name}.bak-{slug}")
    shutil.copy2(path, backup_path)
    summary = recover_payload(payload, business_keys=business_keys)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    summary["statePath"] = str(path)
    summary["backupPath"] = str(backup_path)
    summary["applied"] = True
    return summary


def inspect_recovery(state_path: Path, *, business_keys: tuple[str, ...] = ("openai",)) -> dict[str, Any]:
    path = Path(state_path).resolve()
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"state payload is not an object: {path}")
    summary = recover_payload(payload, business_keys=business_keys)
    summary["statePath"] = str(path)
    summary["backupPath"] = ""
    summary["applied"] = False
    return summary


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Recover EasyRegister mailbox dynamic blacklist exhaustion by clearing transient "
            "blacklist flags while preserving strong unsupported/registration-disallowed entries."
        )
    )
    parser.add_argument("state_path", type=Path, help="Path to register-mailbox-domain-state.json")
    parser.add_argument(
        "--business-key",
        action="append",
        dest="business_keys",
        default=[],
        help="Business key to recover. Can be repeated. Defaults to openai.",
    )
    parser.add_argument("--apply", action="store_true", help="Write the recovered state. Default is dry-run.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    business_keys = tuple(str(item or "").strip().lower() for item in args.business_keys if str(item or "").strip()) or ("openai",)
    summary = (
        apply_recovery(args.state_path, business_keys=business_keys)
        if args.apply
        else inspect_recovery(args.state_path, business_keys=business_keys)
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
