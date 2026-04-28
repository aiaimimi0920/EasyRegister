from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any

from pathlib import Path

from others.bootstrap import ensure_local_bundle_imports
from others.local_config import read_easyemail_server_api_key

ensure_local_bundle_imports()

from others.runtime import resolve_mailbox
from others.storage import load_json_payload

from shared_mailbox.easy_email_client import recover_mailbox_capacity, release_mailbox


def _ensure_easyemail_runtime_defaults() -> None:
    if not str(os.environ.get("MAILBOX_SERVICE_BASE_URL") or "").strip():
        os.environ["MAILBOX_SERVICE_BASE_URL"] = "http://localhost:18080"
    if not str(os.environ.get("MAILBOX_SERVICE_API_KEY") or "").strip():
        api_key = read_easyemail_server_api_key()
        if api_key:
            os.environ["MAILBOX_SERVICE_API_KEY"] = api_key


def _write_team_flow_update(*, source_path: Path, updater: Any) -> dict[str, Any]:
    payload = load_json_payload(source_path)
    if not isinstance(payload, dict):
        raise RuntimeError("small_success_payload_invalid")
    updated = updater(dict(payload))
    source_path.write_text(json.dumps(updated, indent=2, ensure_ascii=False), encoding="utf-8")
    return updated


def _normalize_release_error(exc: BaseException, *, provider: str) -> dict[str, Any]:
    message = str(exc or "").strip()
    lowered = message.lower()
    if (
        "邮箱不存在或无权限删除" in message
        or "deleteMailboxWeb failed with status 403" in message
        or "delete mailbox web failed with status 403" in lowered
        or "not found" in lowered
        or "already deleted" in lowered
    ):
        return {
            "released": False,
            "provider": provider,
            "detail": "not_found",
        }
    return {
        "released": False,
        "provider": provider,
        "detail": message,
    }


def _should_retry_release_error(exc: BaseException) -> bool:
    message = str(exc or "").strip().lower()
    if not message:
        return False
    retry_markers = (
        'mail service post /mail/mailboxes/release failed: http 500',
        '"error":"fetch failed"',
        'connection refused',
        'timed out',
        'timeout',
        'http 502',
        'http 503',
        'http 504',
    )
    return any(marker in message for marker in retry_markers)


def _is_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _normalize_preserve_codes(value: Any) -> set[str]:
    if isinstance(value, list):
        return {str(item or "").strip() for item in value if str(item or "").strip()}
    if isinstance(value, tuple):
        return {str(item or "").strip() for item in value if str(item or "").strip()}
    raw = str(value or "").strip()
    if not raw:
        return set()
    return {item.strip() for item in raw.split(",") if item.strip()}


def dispatch_easyemail_step(*, step_type: str, step_input: dict[str, Any]) -> dict[str, Any]:
    normalized_step_type = str(step_type or "").strip()
    if normalized_step_type == "acquire_mailbox":
        _ensure_easyemail_runtime_defaults()
        mailbox = resolve_mailbox(
            preallocated_email=str(step_input.get("preallocated_email") or "").strip() or None,
            preallocated_session_id=str(step_input.get("preallocated_session_id") or "").strip() or None,
            preallocated_mailbox_ref=str(step_input.get("preallocated_mailbox_ref") or "").strip() or None,
        )
        return {
            "provider": str(getattr(mailbox, "provider", "") or "").strip(),
            "email": str(getattr(mailbox, "email", "") or "").strip(),
            "mailbox_ref": str(getattr(mailbox, "ref", "") or "").strip(),
            "session_id": str(getattr(mailbox, "session_id", "") or "").strip(),
        }

    if normalized_step_type == "release_mailbox":
        _ensure_easyemail_runtime_defaults()
        provider = str(step_input.get("provider") or step_input.get("providerTypeKey") or "").strip().lower()
        mailbox_ref = str(step_input.get("mailbox_ref") or "").strip()
        mailbox_session_id = str(step_input.get("mailbox_session_id") or "").strip()
        source_path_text = str(step_input.get("source_path") or "").strip()
        error_code = str(step_input.get("error_code") or "").strip()
        preserve_enabled = _is_truthy(step_input.get("preserve_enabled"))
        preserve_codes = _normalize_preserve_codes(step_input.get("preserve_on_error_codes"))
        if preserve_enabled and error_code and error_code in preserve_codes:
            result = {
                "released": False,
                "detail": "skipped_preserved_for_manual_oauth",
                "provider": provider,
            }
            if source_path_text:
                source_path = Path(source_path_text).resolve()
                _write_team_flow_update(
                    source_path=source_path,
                    updater=lambda payload: {
                        **payload,
                        "teamFlow": {
                            **dict(payload.get("teamFlow") or {}),
                            "mailboxRelease": result,
                        },
                    },
                )
            return result
        last_exc: BaseException | None = None
        result = None
        for attempt_index in range(1, 4):
            try:
                result = dict(
                    release_mailbox(
                        mailbox_ref=mailbox_ref or None,
                        session_id=mailbox_session_id or None,
                        reason="dst_flow_cleanup",
                    )
                    or {}
                )
                break
            except Exception as exc:
                last_exc = exc
                if attempt_index >= 3 or not _should_retry_release_error(exc):
                    break
                time.sleep(1.5 * attempt_index)
        if result is None:
            result = _normalize_release_error(last_exc or RuntimeError("release_mailbox_failed"), provider=provider)
        release_provider = str(
            result.get("provider")
            or result.get("providerTypeKey")
            or provider
            or ""
        ).strip().lower()
        if release_provider:
            result.setdefault("provider", release_provider)
        if str(result.get("detail") or "").strip().lower() == "skipped_non_moemail":
            result["detail"] = "provider_does_not_support_release"

        if source_path_text:
            source_path = Path(source_path_text).resolve()
            _write_team_flow_update(
                source_path=source_path,
                updater=lambda payload: {
                    **payload,
                    "teamFlow": {
                        **dict(payload.get("teamFlow") or {}),
                        "mailboxRelease": result,
                    },
                },
            )
        return result

    if normalized_step_type == "recover_mailbox_capacity":
        _ensure_easyemail_runtime_defaults()
        stale_after_seconds = int(float(str(step_input.get("stale_after_seconds") or 0).strip() or "0"))
        max_delete_count = int(float(str(step_input.get("max_delete_count") or 30).strip() or "30"))
        force = str(step_input.get("force") if "force" in step_input else "true").strip().lower()
        provider_type_key = str(
            step_input.get("provider_type_key")
            or step_input.get("providerTypeKey")
            or ""
        ).strip()
        provider_instance_id = str(
            step_input.get("provider_instance_id")
            or step_input.get("providerInstanceId")
            or ""
        ).strip()
        failure_code = str(
            step_input.get("failure_code")
            or step_input.get("failureCode")
            or ""
        ).strip()
        detail = str(step_input.get("detail") or "").strip()
        return dict(
            recover_mailbox_capacity(
                failure_code=failure_code,
                detail=detail,
                provider_type_key=provider_type_key,
                provider_instance_id=provider_instance_id,
                stale_after_seconds=stale_after_seconds,
                max_delete_count=max_delete_count,
                force=force not in {"0", "false", "no", "off"},
            )
            or {}
        )

    raise RuntimeError(f"unsupported_easyemail_step:{normalized_step_type}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dispatch a medium EasyEmail business step.")
    parser.add_argument("--step-type", required=True, help="Generic DST step type.")
    parser.add_argument("--input-json", default="{}", help="JSON object passed as step input.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    payload = json.loads(str(args.input_json or "{}"))
    if not isinstance(payload, dict):
        raise RuntimeError("input_json_must_be_object")
    result = dispatch_easyemail_step(
        step_type=str(args.step_type or "").strip(),
        step_input=payload,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
