from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from others.bootstrap import ensure_local_bundle_imports
from others.runtime import resolve_mailbox
from others.runtime_mailbox import ensure_easy_email_env_defaults as ensure_easyemail_runtime_defaults
from others.runtime_mailbox import resolve_mailbox_business_key
from others.storage import load_json_payload

ensure_local_bundle_imports()

from shared_mailbox.easy_email_client import recover_mailbox_capacity, release_mailbox


def write_team_flow_update(*, source_path: Path, updater: Any) -> dict[str, Any]:
    payload = load_json_payload(source_path)
    if not isinstance(payload, dict):
        raise RuntimeError("small_success_payload_invalid")
    updated = updater(dict(payload))
    source_path.write_text(json.dumps(updated, indent=2, ensure_ascii=False), encoding="utf-8")
    return updated


def maybe_write_team_flow_update(*, source_path_text: str, updater: Any) -> dict[str, Any] | None:
    resolved_text = str(source_path_text or "").strip()
    if not resolved_text:
        return None
    source_path = Path(resolved_text).resolve()
    if not source_path.is_file():
        return None
    return write_team_flow_update(source_path=source_path, updater=updater)


def normalize_release_error(exc: BaseException, *, provider: str) -> dict[str, Any]:
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


def should_retry_release_error(exc: BaseException) -> bool:
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


def is_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def normalize_preserve_codes(value: Any) -> set[str]:
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
        ensure_easyemail_runtime_defaults()
        requested_business_key = (
            str(step_input.get("business_key") or step_input.get("businessKey") or "").strip() or None
        )
        mailbox = resolve_mailbox(
            preallocated_email=str(step_input.get("preallocated_email") or "").strip() or None,
            preallocated_session_id=str(step_input.get("preallocated_session_id") or "").strip() or None,
            preallocated_mailbox_ref=str(step_input.get("preallocated_mailbox_ref") or "").strip() or None,
            business_key=requested_business_key,
        )
        return {
            "provider": str(getattr(mailbox, "provider", "") or "").strip(),
            "email": str(getattr(mailbox, "email", "") or "").strip(),
            "mailbox_ref": str(getattr(mailbox, "ref", "") or "").strip(),
            "session_id": str(getattr(mailbox, "session_id", "") or "").strip(),
            "business_key": resolve_mailbox_business_key(business_key=requested_business_key),
        }

    if normalized_step_type == "release_mailbox":
        ensure_easyemail_runtime_defaults()
        provider = str(step_input.get("provider") or step_input.get("providerTypeKey") or "").strip().lower()
        mailbox_ref = str(step_input.get("mailbox_ref") or "").strip()
        mailbox_session_id = str(step_input.get("mailbox_session_id") or "").strip()
        source_path_text = str(step_input.get("source_path") or "").strip()
        error_code = str(step_input.get("error_code") or "").strip()
        preserve_enabled = is_truthy(step_input.get("preserve_enabled"))
        preserve_codes = normalize_preserve_codes(step_input.get("preserve_on_error_codes"))
        if preserve_enabled and error_code and error_code in preserve_codes:
            result = {
                "released": False,
                "detail": "skipped_preserved_for_manual_oauth",
                "provider": provider,
            }
            maybe_write_team_flow_update(
                source_path_text=source_path_text,
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
                if attempt_index >= 3 or not should_retry_release_error(exc):
                    break
                time.sleep(1.5 * attempt_index)
        if result is None:
            result = normalize_release_error(last_exc or RuntimeError("release_mailbox_failed"), provider=provider)
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

        maybe_write_team_flow_update(
            source_path_text=source_path_text,
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
        ensure_easyemail_runtime_defaults()
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
