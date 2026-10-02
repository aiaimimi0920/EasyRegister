from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import time
from contextvars import ContextVar
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Any

from others.file_lock import release_lock, try_acquire_lock


ACCOUNT_RISK_SCHEMA_VERSION = 1
ACCOUNT_BAN_BLACKLIST_REASON = "post_registration_account_ban_threshold"
ACCOUNT_DISABLED_STATUSES = {
    "account_banned", "account_disabled", "account_deactivated", "account_suspended",
    "deleted_or_disabled",
}
ACCOUNT_DISABLED_MESSAGES = (
    "you do not have an account because it has been deleted or deactivated",
    "your account has been deactivated",
    "your account has been disabled",
    "your account has been suspended",
    "your account has been banned",
)
_SCOPED_DOMAIN_STATE_PATH: ContextVar[Path | None] = ContextVar("mailbox_account_risk_domain_state_path", default=None)


def resolve_account_risk_domain_state_path(default_path: Path, *, preserved_path: str = "") -> Path:
    explicit = str(os.environ.get("REGISTER_MAILBOX_DOMAIN_STATE_PATH") or "").strip()
    if explicit:
        return Path(explicit).expanduser().resolve()
    return _SCOPED_DOMAIN_STATE_PATH.get() or (Path(preserved_path) if preserved_path else default_path)


def account_risk_output_scope(function):
    """Bind standalone DST selection and observation without mutating process env."""
    @wraps(function)
    def scoped(*args, **kwargs):
        from others.artifact_pool_paths import derive_output_root_from_run_dir
        from others.paths import resolve_shared_root

        output_root = str(os.environ.get("REGISTER_OUTPUT_ROOT") or "").strip()
        root = resolve_shared_root(output_root) if output_root else derive_output_root_from_run_dir(kwargs.get("output_dir"))
        token = _SCOPED_DOMAIN_STATE_PATH.set(root / "others" / "register-mailbox-domain-state.json")
        try:
            return function(*args, **kwargs)
        finally:
            _SCOPED_DOMAIN_STATE_PATH.reset(token)
    return scoped


def normalize_domain(domain: str) -> str:
    value = str(domain or "").strip().lower().rstrip(".")
    try:
        return value.encode("idna").decode("ascii")
    except UnicodeError:
        return ""


def account_risk_state_path(domain_state_path: Path) -> Path:
    """Keep account availability independent of legacy mailbox-quality resets/TTL."""
    return domain_state_path.with_name(domain_state_path.stem + ".account-risk.json")


def _load_state(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"schemaVersion": ACCOUNT_RISK_SCHEMA_VERSION, "businesses": {}}
    except (OSError, ValueError) as exc:
        raise RuntimeError("mailbox_account_risk_state_unreadable") from exc
    if (
        not isinstance(payload, dict)
        or payload.get("schemaVersion") != ACCOUNT_RISK_SCHEMA_VERSION
        or not isinstance(payload.get("businesses"), dict)
    ):
        raise RuntimeError("mailbox_account_risk_state_invalid")
    for business in payload["businesses"].values():
        if not isinstance(business, dict) or not isinstance(business.get("domains"), dict):
            raise RuntimeError("mailbox_account_risk_domains_invalid")
        for stats in business["domains"].values():
            if (
                not isinstance(stats, dict) or type(stats.get("blacklisted")) is not bool
                or type(stats.get("consecutiveAccountBans")) is not int
                or stats["consecutiveAccountBans"] < 0
                or not isinstance(stats.get("accountOutcomes"), dict)
                or any(not isinstance(value, str) or value not in {"banned", "healthy"} for value in stats["accountOutcomes"].values())
                or not isinstance(stats.get("healthyObservations", {}), dict)
            ):
                raise RuntimeError("mailbox_account_risk_domain_stats_invalid")
    return payload


def business_blocked_domains(*, domain_state_path: Path, business_key: str) -> tuple[str, ...]:
    state = _load_state(account_risk_state_path(domain_state_path))
    business = state["businesses"].get(str(business_key or "").strip().lower(), {})
    domains = business.get("domains", {}) if isinstance(business, dict) else {}
    if not isinstance(domains, dict):
        raise RuntimeError("mailbox_account_risk_domains_invalid")
    return tuple(
        normalized
        for domain, stats in domains.items()
        if isinstance(stats, dict) and stats.get("blacklisted")
        and (normalized := normalize_domain(domain))
    )


def _explicit_account_rejection(error: dict[str, Any], message: str) -> bool:
    code = str(error.get("code") or error.get("error_code") or "").strip().lower()
    if code in ACCOUNT_DISABLED_STATUSES:
        return True
    if any(marker in code for marker in ("workspace", "mailbox", "provider")):
        return False
    # Only a direct upstream sentence or saved HTTP body is evidence. Quoted,
    # negated and provider-wrapped mentions of the same sentence are not.
    body = message.strip()
    match = re.fullmatch(
        r"(?:(?:chatgpt_login_otp_validate_failed|chatgpt_login_email_otp_validate failed|otp_validate) )?status=403 body=(.*)",
        body, flags=re.DOTALL | re.IGNORECASE,
    )
    if match:
        body = match.group(1).strip()
        if body.startswith("{"):
            try:
                payload = json.loads(body)
            except ValueError:
                # EasyProtocol deliberately previews only 220 characters. A
                # truncated upstream error object can still contain the exact
                # leading account-rejection sentence; arbitrary quotes cannot.
                preview = re.match(r'\{\s*"error"\s*:\s*\{\s*"message"\s*:\s*"((?:\\.|[^"\\])*)', body)
                if not preview:
                    return False
                try:
                    sentence = json.loads('"' + preview.group(1) + '"')
                except ValueError:
                    return False
                return _explicit_account_rejection({}, sentence)
            rejection = payload.get("error") if isinstance(payload, dict) else None
            if not isinstance(rejection, dict):
                return False
            return _explicit_account_rejection(rejection, str(rejection.get("message") or ""))
    body = " ".join(body.lower().split())
    return any(
        body == marker or body == marker + "."
        or body.startswith(marker + ". if you believe this was an error, please contact us")
        for marker in ACCOUNT_DISABLED_MESSAGES
    )


def post_registration_account_verdict(
    result: dict[str, Any], *, email: str, mailbox_ref: str = "", mailbox_session_id: str = "",
) -> str:
    """Only explicit account rejection or freshly completed login is terminal.

    Generic 403, challenge, network, workspace, SMS and OTP errors are not bans.
    Initial registration success and cached initialization are not healthy-login
    evidence. The platform-specific wording is a risk signal, not causal proof.
    """
    outputs = result.get("outputs")
    outputs = outputs if isinstance(outputs, dict) else {}
    status_output = outputs.get("account-availability")
    status_verdict = ""
    if isinstance(status_output, dict):
        observed_email = str(status_output.get("email") or "").strip().lower()
        if observed_email == email.strip().lower():
            status = str(status_output.get("status") or "").strip().lower()
            if status in ACCOUNT_DISABLED_STATUSES:
                return "banned"
            if status == "login_succeeded":
                status_verdict = "healthy"
    error_step = str(result.get("errorStep") or "").strip()
    errors = result.get("stepErrors")
    error = errors.get(error_step, {}) if isinstance(errors, dict) else {}
    if not bool(result.get("ok")) and isinstance(error, dict):
        # Registration-stage rejections are not post-registration account bans.
        if error_step in {
            "initialize-chatgpt-login-session", "initialize-platform-organization",
            "obtain-codex-oauth", "validate-free-personal-oauth", "authenticate-account",
            "login-account", "check-account-availability",
        }:
            message = str(error.get("message") or result.get("error") or "")
            if _explicit_account_rejection(error, message):
                return "banned"
    if status_verdict:
        return status_verdict
    login = outputs.get("initialize-chatgpt-login-session")
    steps = result.get("steps")
    login_identity_matches = False
    if isinstance(login, dict):
        observed_email = str(login.get("email") or "").strip().lower()
        if observed_email:
            login_identity_matches = observed_email == email.strip().lower()
        else:
            login_identity_matches = bool(mailbox_ref and mailbox_session_id) and (
                str(login.get("mailboxRef") or "").strip() == mailbox_ref
                and str(login.get("mailboxSessionId") or "").strip() == mailbox_session_id
            )
    if (
        login_identity_matches and isinstance(login, dict) and login.get("ok") is True
        and str(login.get("status") or "").strip().lower() == "completed"
        and bool(login.get("personalWorkspaceId"))
        and isinstance(steps, dict) and steps.get("initialize-chatgpt-login-session") == "ok"
    ):
        return "healthy"
    return ""


def _write_state(path: Path, payload: dict[str, Any]) -> None:
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def record_account_verdict(
    *, domain_state_path: Path, business_key: str, email: str, verdict: str, threshold: int,
    observation_id: str = "",
) -> dict[str, Any]:
    business_key = str(business_key or "").strip().lower()
    email = str(email or "").strip().lower()
    if not business_key or "@" not in email or verdict not in {"banned", "healthy"}:
        raise ValueError("mailbox_account_risk_invalid_observation")
    local_part, domain = email.rsplit("@", 1)
    domain = normalize_domain(domain)
    if not local_part or not domain:
        raise ValueError("mailbox_account_risk_invalid_email")
    threshold = max(1, int(threshold))
    # One normalized address is one upstream account; retry/claim/session IDs
    # must not turn repeated observations into multiple accounts.
    account_hash = hashlib.sha256((local_part + "@" + domain).encode("utf-8")).hexdigest()
    path = account_risk_state_path(domain_state_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    lock = path.with_name(path.name + ".lock")
    deadline = time.monotonic() + 5.0
    while True:
        try:
            acquired = try_acquire_lock(lock, stale_after_seconds=60.0)
        except PermissionError:
            # Windows can report a transient access-denied while another
            # observer closes/removes the lock. Permanent denial stays bounded.
            acquired = False
        if acquired:
            break
        if time.monotonic() >= deadline:
            raise RuntimeError("mailbox_account_risk_state_busy")
        time.sleep(0.01)
    try:
        state = _load_state(path)
        business = state["businesses"].setdefault(business_key, {"domains": {}})
        domains = business.setdefault("domains", {})
        stats = domains.setdefault(domain, {"consecutiveAccountBans": 0, "accountOutcomes": {}, "blacklisted": False})
        observed = stats.setdefault("accountOutcomes", {})
        prior = observed.get(account_hash)
        healthy_observations = stats.setdefault("healthyObservations", {})
        observation_hash = hashlib.sha256((account_hash + ":" + str(observation_id or account_hash)).encode("utf-8")).hexdigest()
        # Bans are deduplicated by account for its lifetime. Healthy logins are
        # deduplicated by observation: a new login of the same account can reset
        # a still-unblocked streak, but supervisor replay cannot do so again.
        duplicate = prior == "banned" if verdict == "banned" else observation_hash in healthy_observations
        if not duplicate:
            if verdict == "healthy":
                healthy_observations[observation_hash] = True
            if prior != "banned":
                observed[account_hash] = verdict
            now = datetime.now(timezone.utc).isoformat()
            if verdict == "banned":
                stats["consecutiveAccountBans"] = int(stats.get("consecutiveAccountBans") or 0) + 1
            elif not stats.get("blacklisted"):
                stats["consecutiveAccountBans"] = 0
            if verdict == "banned" and stats["consecutiveAccountBans"] >= threshold:
                stats["blacklisted"] = True
                stats.setdefault("blacklistedAt", now)
                stats["blacklistReason"] = ACCOUNT_BAN_BLACKLIST_REASON
            stats.update({"lastOutcome": verdict, "lastOutcomeAt": now, "threshold": threshold})
            state["updatedAt"] = now
            _write_state(path, state)
        return {
            "businessKey": business_key, "domain": domain,
            "verdict": verdict, "duplicate": duplicate,
            "consecutiveAccountBans": int(stats.get("consecutiveAccountBans") or 0),
            "threshold": threshold, "blacklisted": bool(stats.get("blacklisted")),
            "blacklistReason": str(stats.get("blacklistReason") or ""),
            "statePath": str(path),
        }
    finally:
        release_lock(lock)
