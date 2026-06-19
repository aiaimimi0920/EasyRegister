from __future__ import annotations

import json
import shutil
import time
import uuid
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


LOGIN_SUCCEEDED_STATUSES = {
    "login_succeeded",
    "login_success",
    "logged_in",
    "available",
    "usable",
}

DELETED_CONFIRMED_STATUSES = {
    "deleted_confirmed",
    "account_deleted",
    "account_disabled",
    "account_deactivated",
    "deleted_or_disabled",
}

SENSITIVE_KEY_MARKERS = {
    "access",
    "authorization",
    "cookie",
    "credential",
    "oauth",
    "password",
    "refresh",
    "secret",
    "token",
}

AUDIT_STATE_KEY = "accountAvailabilityAudit"
AUDIT_FLOW_VERSION = "openai-account-availability-audit-v1"
PRODUCTION_LOGIN_RECHECK_SECONDS = 24 * 60 * 60
PRODUCTION_INCONCLUSIVE_RECHECK_SECONDS = 12 * 60 * 60


def _as_text(value: Any) -> str:
    return str(value or "").strip()


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _as_text(value).lower() in {"1", "true", "yes", "on"}


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(_as_text(value)))
    except Exception:
        return default


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _format_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_utc(value: Any) -> datetime | None:
    text = _as_text(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"account_audit_payload_not_object:{path}")
    return payload


def _extract_email(payload: dict[str, Any]) -> str:
    for key in ("email", "mailboxEmail", "mailbox_email", "accountEmail", "account_email"):
        value = _as_text(payload.get(key))
        if value:
            return value
    profile_claims = payload.get("https://api.openai.com/profile")
    if isinstance(profile_claims, dict):
        return _as_text(profile_claims.get("email"))
    return ""


def _extract_recovery_data_credential(payload: dict[str, Any]) -> dict[str, Any]:
    value = (
        payload.get("recoveryDataCredential")
        or payload.get("recovery_data_credential")
        or payload.get("mailboxRecoveryDataCredential")
        or payload.get("mailbox_recovery_data_credential")
    )
    if not isinstance(value, dict):
        return {}
    return {str(key): item for key, item in value.items() if str(key).strip()}


def _recovery_data_from_result(result: dict[str, Any]) -> dict[str, Any]:
    return _extract_recovery_data_credential(result)


def _is_production_mode(step_input: dict[str, Any]) -> bool:
    return _as_bool(
        step_input.get("production_mode")
        or step_input.get("productionMode")
        or step_input.get("production_audit")
        or step_input.get("productionAudit")
    )


def _production_output_root(step_input: dict[str, Any]) -> Path:
    value = (
        step_input.get("output_root")
        or step_input.get("outputRoot")
        or step_input.get("input_source_dir")
        or step_input.get("inputSourceDir")
        or step_input.get("account_dir")
        or step_input.get("accountDir")
    )
    text = _as_text(value)
    if not text:
        raise RuntimeError("account_audit_output_root_missing")
    return Path(text).expanduser().resolve()


def _production_pool_roots(output_root: Path) -> list[Path]:
    return [
        output_root / "openai" / "converted",
        output_root / "openai" / "failed-twice",
        output_root / "codex",
    ]


def _is_excluded_production_path(path: Path) -> bool:
    excluded_names = {
        "_claims",
        "claims",
        "deleted-confirmed",
        "可登录账号",
        "account-availability-audit",
    }
    return any(part in excluded_names or part.startswith(".") for part in path.parts)


def _production_candidate_paths(output_root: Path) -> Iterable[Path]:
    seen: set[str] = set()
    for root in _production_pool_roots(output_root):
        if not root.is_dir():
            continue
        iterator = root.rglob("*.json") if root.name == "codex" else root.glob("*.json")
        for path in iterator:
            if path.is_file() and not _is_excluded_production_path(path):
                resolved = path.resolve()
                key = str(resolved).lower()
                if key not in seen:
                    seen.add(key)
                    yield resolved


def _next_check_at(payload: dict[str, Any]) -> datetime | None:
    state = payload.get(AUDIT_STATE_KEY)
    if not isinstance(state, dict):
        return None
    return _parse_utc(state.get("nextCheckAt") or state.get("next_check_at"))


def _target_from_payload(*, source_path: Path, original_path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    email = _extract_email(payload)
    if not email:
        raise RuntimeError(f"account_audit_email_missing:{source_path}")
    mailbox_ref = _as_text(payload.get("mailboxRef") or payload.get("mailbox_ref"))
    mailbox_session_id = _as_text(
        payload.get("mailboxSessionId")
        or payload.get("mailbox_session_id")
        or payload.get("session_id")
        or payload.get("sessionId")
    )
    recovery_data_credential = _extract_recovery_data_credential(payload)
    return {
        "target_id": uuid.uuid5(uuid.NAMESPACE_URL, str(original_path.resolve())).hex,
        "source_path": str(source_path.resolve()),
        "original_path": str(original_path.resolve()),
        "original_name": original_path.name,
        "email": email,
        "mailbox_ref": mailbox_ref,
        "mailbox_session_id": mailbox_session_id,
        "recovery_data_credential": recovery_data_credential,
        "recoveryDataCredential": recovery_data_credential,
    }


def _production_target_from_file(*, path: Path, now: datetime) -> tuple[dict[str, Any] | None, dict[str, str] | None]:
    try:
        payload = _read_json_object(path)
        email = _extract_email(payload)
        if not email:
            return None, {"source_path": str(path), "email": "", "reason": "email_missing"}
        next_check_at = _next_check_at(payload)
        if next_check_at is not None and next_check_at > now:
            return None, {
                "source_path": str(path),
                "email": email,
                "reason": "next_check_in_future",
                "nextCheckAt": _format_utc(next_check_at),
            }
        return _target_from_payload(source_path=path, original_path=path, payload=payload), None
    except Exception as exc:
        return None, {"source_path": str(path), "email": "", "reason": str(exc)}


def production_audit_has_due_targets(*, output_root: Path) -> bool:
    now = _utc_now()
    for path in _production_candidate_paths(output_root.resolve()):
        target, _skipped = _production_target_from_file(path=path, now=now)
        if target is not None:
            return True
    return False


def _target_from_account_file(*, source_path: Path, original_path: Path) -> dict[str, Any]:
    payload = _read_json_object(source_path)
    return _target_from_payload(source_path=source_path, original_path=original_path, payload=payload)


def _production_claim_dir(output_root: Path) -> Path:
    return output_root / "others" / "account-availability-audit" / "claims"


def _claim_production_target(*, output_root: Path, target: dict[str, Any]) -> dict[str, Any]:
    original_path = Path(_as_text(target.get("source_path") or target.get("sourcePath"))).expanduser().resolve()
    if not original_path.is_file():
        raise FileNotFoundError(str(original_path))
    claim_path = _unique_destination_path(
        _production_claim_dir(output_root),
        f"{uuid.uuid4().hex[:8]}-{original_path.name}",
    )
    shutil.copy2(original_path, claim_path)
    claimed = dict(target)
    claimed.setdefault("original_path", str(original_path))
    claimed.setdefault("originalPath", str(original_path))
    claimed["source_path"] = str(claim_path.resolve())
    claimed["sourcePath"] = str(claim_path.resolve())
    claimed["production_claim_path"] = str(claim_path.resolve())
    claimed["productionClaimPath"] = str(claim_path.resolve())
    return claimed


def _unique_destination_path(directory: Path, filename: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    candidate = directory / filename
    if not candidate.exists():
        return candidate
    stem = candidate.stem
    suffix = candidate.suffix
    for index in range(1, 10000):
        next_candidate = directory / f"{stem}-{index}{suffix}"
        if not next_candidate.exists():
            return next_candidate
    raise RuntimeError(f"account_audit_destination_collision:{candidate}")


def _resolve_candidate_paths(*, source_dir: Path, max_targets: int) -> list[Path]:
    candidates = sorted(path for path in source_dir.glob("*.json") if path.is_file())
    if max_targets > 0:
        return candidates[:max_targets]
    return candidates


def select_account_audit_targets(*, step_input: dict[str, Any]) -> dict[str, Any]:
    account_file_text = _as_text(step_input.get("account_file") or step_input.get("accountFile"))
    account_dir_text = _as_text(
        step_input.get("account_dir")
        or step_input.get("accountDir")
        or step_input.get("input_source_dir")
        or step_input.get("inputSourceDir")
    )
    account_claims_dir_text = _as_text(
        step_input.get("account_claims_dir")
        or step_input.get("accountClaimsDir")
        or step_input.get("input_claims_dir")
        or step_input.get("inputClaimsDir")
    )
    claim_mode = _as_bool(step_input.get("claim_mode") or step_input.get("claimMode"))
    max_targets = max(0, _as_int(step_input.get("max_targets") or step_input.get("maxTargets"), 0))

    targets: list[dict[str, Any]] = []
    skipped: list[dict[str, str]] = []
    mode = "single-file"
    source_dir = ""
    claims_dir = ""

    if _is_production_mode(step_input):
        output_root = _production_output_root(step_input)
        mode = "production-pools"
        source_dir = str(output_root)
        production_max_targets = max_targets if max_targets > 0 else 1
        now = _utc_now()
        for candidate in _production_candidate_paths(output_root):
            if len(targets) >= production_max_targets:
                break
            target, skipped_item = _production_target_from_file(path=candidate, now=now)
            if skipped_item is not None:
                skipped.append(skipped_item)
                continue
            if target is not None:
                try:
                    targets.append(_claim_production_target(output_root=output_root, target=target))
                except FileNotFoundError:
                    skipped.append(
                        {
                            "source_path": _as_text(target.get("source_path") or target.get("sourcePath")),
                            "email": _as_text(target.get("email")),
                            "reason": "source_disappeared_before_claim",
                        }
                    )
                    continue
    elif account_file_text:
        account_file = Path(account_file_text).expanduser().resolve()
        if not account_file.is_file():
            raise RuntimeError(f"account_audit_file_missing:{account_file}")
        targets.append(_target_from_account_file(source_path=account_file, original_path=account_file))
    else:
        if not account_dir_text:
            raise RuntimeError("account_audit_target_missing")
        source_path = Path(account_dir_text).expanduser().resolve()
        if not source_path.is_dir():
            raise RuntimeError(f"account_audit_dir_missing:{source_path}")
        mode = "directory-claim" if claim_mode else "directory-direct"
        source_dir = str(source_path)
        claims_path = (
            Path(account_claims_dir_text).expanduser().resolve()
            if account_claims_dir_text
            else (source_path / "_claims").resolve()
        )
        if claim_mode:
            claims_path.mkdir(parents=True, exist_ok=True)
            claims_dir = str(claims_path)
        for candidate in _resolve_candidate_paths(source_dir=source_path, max_targets=max_targets):
            active_path = candidate
            if claim_mode:
                active_path = _unique_destination_path(claims_path, f"{uuid.uuid4().hex[:8]}-{candidate.name}")
                try:
                    candidate.replace(active_path)
                except FileNotFoundError:
                    continue
            try:
                targets.append(_target_from_account_file(source_path=active_path, original_path=candidate))
            except Exception as exc:
                skipped_source_path = str(active_path)
                if claim_mode and active_path.is_file():
                    restored_path = _restore_claimed_account_file(
                        source_path=active_path,
                        original_path=candidate,
                        original_name=candidate.name,
                    )
                    skipped_source_path = str(restored_path)
                skipped.append({"source_path": skipped_source_path, "reason": str(exc)})

    if not targets:
        return {
            "ok": False,
            "status": "empty",
            "code": "account_audit_targets_empty",
            "mode": mode,
            "source_dir": source_dir,
            "claims_dir": claims_dir,
            "targets": [],
            "skipped": skipped,
        }
    return {
        "ok": True,
        "status": "selected",
        "mode": mode,
        "source_dir": source_dir,
        "claims_dir": claims_dir,
        "target_count": len(targets),
        "skipped_count": len(skipped),
        "targets": targets,
        "skipped": skipped,
    }


def _normalize_path_key(value: Any) -> str:
    text = _as_text(value)
    if not text:
        return ""
    try:
        return str(Path(text).expanduser().resolve()).lower()
    except Exception:
        return text.lower()


def _audit_results_from_input(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, dict):
        results = value.get("results")
        if isinstance(results, list):
            return [item for item in results if isinstance(item, dict)]
        return [value]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def _index_results(results: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for result in results:
        for key in (
            _as_text(result.get("target_id") or result.get("targetId")),
            _normalize_path_key(result.get("source_path") or result.get("sourcePath")),
            _normalize_path_key(result.get("original_path") or result.get("originalPath")),
            _as_text(result.get("email")).lower(),
        ):
            if key and key not in indexed:
                indexed[key] = result
    return indexed


def _result_for_target(*, target: dict[str, Any], indexed_results: dict[str, dict[str, Any]]) -> dict[str, Any]:
    for key in (
        _as_text(target.get("target_id") or target.get("targetId")),
        _normalize_path_key(target.get("source_path") or target.get("sourcePath")),
        _normalize_path_key(target.get("original_path") or target.get("originalPath")),
        _as_text(target.get("email")).lower(),
    ):
        if key and key in indexed_results:
            return indexed_results[key]
    return {
        "ok": True,
        "status": "inconclusive",
        "detail": "missing_account_audit_result",
    }


def _classify_status(result: dict[str, Any]) -> str:
    raw_status = _as_text(
        result.get("status")
        or result.get("outcome")
        or result.get("classification")
        or result.get("result")
    ).lower()
    normalized_status = (
        raw_status.replace("-", "_")
        .replace(" ", "_")
        .replace("/", "_")
        .replace(":", "_")
    )
    if raw_status in LOGIN_SUCCEEDED_STATUSES:
        return "login_succeeded"
    if raw_status in DELETED_CONFIRMED_STATUSES:
        return "deleted_confirmed"
    if any(
        marker in normalized_status
        for marker in (
            "account_deleted",
            "account_disabled",
            "account_deactivated",
            "user_deleted",
            "user_disabled",
            "user_deactivated",
        )
    ):
        return "deleted_confirmed"
    return "inconclusive"


def _redact_for_audit(value: Any) -> Any:
    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            lowered = key_text.lower()
            if any(marker in lowered for marker in SENSITIVE_KEY_MARKERS):
                redacted[key_text] = "<redacted>"
            else:
                redacted[key_text] = _redact_for_audit(item)
        return redacted
    if isinstance(value, list):
        return [_redact_for_audit(item) for item in value]
    return value


def _default_base_dir(*, step_input: dict[str, Any], targets: list[dict[str, Any]]) -> Path:
    if _is_production_mode(step_input):
        return _production_output_root(step_input)
    account_dir_text = _as_text(step_input.get("account_dir") or step_input.get("accountDir"))
    if account_dir_text:
        return Path(account_dir_text).expanduser().resolve()
    for target in targets:
        source_path_text = _as_text(target.get("original_path") or target.get("source_path"))
        if source_path_text:
            return Path(source_path_text).expanduser().resolve().parent
    return Path.cwd().resolve()


def _move_account_file(*, source_path: Path, destination_dir: Path, original_name: str) -> Path:
    destination = _unique_destination_path(destination_dir, original_name or source_path.name)
    shutil.move(str(source_path), str(destination))
    return destination.resolve()


def _restore_claimed_account_file(*, source_path: Path, original_path: Path, original_name: str) -> Path:
    destination_dir = original_path.parent
    destination = destination_dir / (original_name or original_path.name or source_path.name)
    if destination.resolve() == source_path.resolve():
        return source_path.resolve()
    destination = _unique_destination_path(destination_dir, destination.name)
    shutil.move(str(source_path), str(destination))
    return destination.resolve()


def _related_production_files(*, output_root: Path, email: str, source_path: Path) -> list[Path]:
    normalized_email = _as_text(email).lower()
    related: list[Path] = []
    for path in _production_candidate_paths(output_root):
        try:
            payload = _read_json_object(path)
        except Exception:
            continue
        if _extract_email(payload).lower() == normalized_email:
            related.append(path)
    if source_path.is_file():
        try:
            source_resolved = source_path.resolve()
        except Exception:
            source_resolved = source_path
        if source_resolved not in related:
            related.append(source_resolved)
    return sorted(set(related), key=lambda item: str(item).lower())


def _write_json_payload(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _update_production_account_file(
    *,
    path: Path,
    status: str,
    result: dict[str, Any],
    now: datetime,
) -> bool:
    if not path.is_file():
        return False
    payload = _read_json_object(path)
    next_seconds = (
        PRODUCTION_LOGIN_RECHECK_SECONDS
        if status == "login_succeeded"
        else PRODUCTION_INCONCLUSIVE_RECHECK_SECONDS
    )
    payload[AUDIT_STATE_KEY] = {
        "flowVersion": AUDIT_FLOW_VERSION,
        "lastCheckedAt": _format_utc(now),
        "nextCheckAt": _format_utc(now + timedelta(seconds=next_seconds)),
        "status": status,
        "reason": _as_text(result.get("detail") or result.get("reason") or result.get("error") or result.get("status")),
    }
    recovery_data = _recovery_data_from_result(result)
    if status == "login_succeeded" and recovery_data:
        payload["recoveryDataCredential"] = recovery_data
        if "recovery_data_credential" in payload:
            payload["recovery_data_credential"] = recovery_data
    _write_json_payload(path, payload)
    return True


def _production_claim_path_for_target(*, output_root: Path, target: dict[str, Any]) -> Path | None:
    raw = _as_text(target.get("production_claim_path") or target.get("productionClaimPath"))
    if not raw:
        return None
    try:
        path = Path(raw).expanduser().resolve()
        claim_root = _production_claim_dir(output_root).resolve()
        path.relative_to(claim_root)
        return path
    except Exception:
        return None


def _cleanup_production_claim_file(*, output_root: Path, target: dict[str, Any]) -> bool:
    path = _production_claim_path_for_target(output_root=output_root, target=target)
    if path is None or not path.is_file():
        return False
    try:
        path.unlink()
        return True
    except FileNotFoundError:
        return False


def _finalize_production_account_audit_result(
    *,
    step_input: dict[str, Any],
    targets: list[dict[str, Any]],
    indexed_results: dict[str, dict[str, Any]],
    audit_path: Path,
) -> dict[str, Any]:
    output_root = _production_output_root(step_input)
    records: list[dict[str, Any]] = []
    counts = {
        "login_succeeded": 0,
        "deleted_confirmed": 0,
        "inconclusive": 0,
        "source_missing": 0,
        "deleted_files_removed": 0,
        "files_updated": 0,
        "claim_files_removed": 0,
    }
    now = _utc_now()
    timestamp = _format_utc(now)

    for target in targets:
        result = _result_for_target(target=target, indexed_results=indexed_results)
        status = _classify_status(result)
        counts[status] = int(counts.get(status, 0)) + 1
        email = _as_text(target.get("email") or result.get("email")).lower()
        source_path = Path(_as_text(target.get("source_path") or target.get("sourcePath"))).expanduser()
        original_path_text = _as_text(target.get("original_path") or target.get("originalPath"))
        original_path = Path(original_path_text).expanduser() if original_path_text else source_path
        related_paths = _related_production_files(output_root=output_root, email=email, source_path=original_path)
        action = "left_in_place"
        final_path = str(original_path)
        touched_paths: list[str] = []

        if status == "deleted_confirmed":
            action = "deleted_same_email_files"
            final_path = ""
            for path in related_paths:
                if not path.is_file():
                    continue
                try:
                    path.unlink()
                    counts["deleted_files_removed"] += 1
                    touched_paths.append(str(path))
                except FileNotFoundError:
                    continue
            if not touched_paths:
                counts["source_missing"] += 1
                action = "source_missing"
            final_path = ""
        else:
            action = "updated_audit_state"
            for path in related_paths:
                if _update_production_account_file(
                    path=path,
                    status=status,
                    result=result,
                    now=now,
                ):
                    counts["files_updated"] += 1
                    touched_paths.append(str(path))
            if not touched_paths:
                counts["source_missing"] += 1
                action = "source_missing"
                final_path = ""
            else:
                final_path = touched_paths[0]

        if _cleanup_production_claim_file(output_root=output_root, target=target):
            counts["claim_files_removed"] += 1

        records.append(
            {
                "timestamp": timestamp,
                "email": email,
                "status": status,
                "rawStatus": _as_text(result.get("status") or result.get("outcome")),
                "action": action,
                "source_path": str(original_path),
                "original_path": original_path_text,
                "final_path": final_path,
                "touched_paths": touched_paths,
                "result": _redact_for_audit(result),
            }
        )

    with audit_path.open("a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")

    return {
        "ok": True,
        "status": "finalized",
        "production_mode": True,
        "audit_path": str(audit_path),
        "records_written": len(records),
        "counts": counts,
        "records": records,
    }


def finalize_account_audit_result(*, step_input: dict[str, Any]) -> dict[str, Any]:
    raw_targets = step_input.get("targets")
    targets = [item for item in raw_targets if isinstance(item, dict)] if isinstance(raw_targets, list) else []
    if not targets:
        raise RuntimeError("account_audit_finalize_targets_missing")

    audit_results = _audit_results_from_input(
        step_input.get("audit_result")
        or step_input.get("auditResult")
        or step_input.get("results")
    )
    indexed_results = _index_results(audit_results)
    base_dir = _default_base_dir(step_input=step_input, targets=targets)
    loginable_dir = Path(
        _as_text(step_input.get("loginable_dir") or step_input.get("loginableDir"))
        or str(base_dir / "可登录账号")
    ).expanduser().resolve()
    deleted_dir = Path(
        _as_text(step_input.get("deleted_dir") or step_input.get("deletedDir"))
        or str(base_dir / "deleted-confirmed")
    ).expanduser().resolve()
    audit_path = Path(
        _as_text(step_input.get("audit_path") or step_input.get("auditPath"))
        or (
            str(base_dir / "others" / "account-availability-audit.jsonl")
            if _is_production_mode(step_input)
            else str(base_dir / "account-availability-audit.jsonl")
        )
    ).expanduser().resolve()
    audit_path.parent.mkdir(parents=True, exist_ok=True)

    if _is_production_mode(step_input):
        return _finalize_production_account_audit_result(
            step_input=step_input,
            targets=targets,
            indexed_results=indexed_results,
            audit_path=audit_path,
        )

    records: list[dict[str, Any]] = []
    counts = {
        "login_succeeded": 0,
        "deleted_confirmed": 0,
        "inconclusive": 0,
        "source_missing": 0,
    }
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    for target in targets:
        result = _result_for_target(target=target, indexed_results=indexed_results)
        status = _classify_status(result)
        counts[status] = int(counts.get(status, 0)) + 1
        source_path = Path(_as_text(target.get("source_path") or target.get("sourcePath"))).expanduser()
        original_path_text = _as_text(target.get("original_path") or target.get("originalPath"))
        original_path = Path(original_path_text).expanduser() if original_path_text else source_path
        original_name = _as_text(target.get("original_name") or target.get("originalName")) or source_path.name
        action = "left_in_place"
        final_path = str(source_path)

        if status in {"login_succeeded", "deleted_confirmed"}:
            if source_path.is_file():
                if status == "login_succeeded":
                    destination = _move_account_file(
                        source_path=source_path,
                        destination_dir=loginable_dir,
                        original_name=original_name,
                    )
                    action = "moved_to_loginable"
                else:
                    destination = _move_account_file(
                        source_path=source_path,
                        destination_dir=deleted_dir,
                        original_name=original_name,
                    )
                    action = "moved_to_deleted"
                final_path = str(destination)
            else:
                counts["source_missing"] += 1
                action = "source_missing"
                final_path = ""
        elif source_path.is_file() and original_path_text and source_path.resolve() != original_path.resolve():
            destination = _restore_claimed_account_file(
                source_path=source_path,
                original_path=original_path,
                original_name=original_name,
            )
            action = "restored_to_source"
            final_path = str(destination)

        record = {
            "timestamp": timestamp,
            "email": _as_text(target.get("email")),
            "status": status,
            "rawStatus": _as_text(result.get("status") or result.get("outcome")),
            "action": action,
            "source_path": str(source_path),
            "original_path": original_path_text,
            "final_path": final_path,
            "result": _redact_for_audit(result),
        }
        records.append(record)

    with audit_path.open("a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")

    return {
        "ok": True,
        "status": "finalized",
        "audit_path": str(audit_path),
        "loginable_dir": str(loginable_dir),
        "deleted_dir": str(deleted_dir),
        "records_written": len(records),
        "counts": counts,
        "records": records,
    }
