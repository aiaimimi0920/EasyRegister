"""官方 CLI 交互登录的隔离准备与本地格式检查，不执行 OAuth 协议或生产入池。"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from others.artifact_pool_team_expand import has_free_personal_oauth_claims
from others.common_credentials import decode_jwt_payload, standardize_export_credential_payload


class InteractiveLoginError(ValueError):
    """错误只携带稳定代码，不能包含凭据、响应或账号信息。"""


def _require(condition: bool, code: str) -> None:
    if not condition:
        raise InteractiveLoginError(code)


def _write_new_json(path: Path, payload: dict[str, Any]) -> None:
    # 只创建新文件，绝不覆盖旧会话或历史授权产物。
    data = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(data)


def _private_directory(path: Path) -> None:
    path.mkdir(mode=0o700)
    if os.name != "nt":
        return
    # Windows 的 mode=0700 不设置 DACL；写任何凭据前必须落实 ACL。
    command = r"""
$ErrorActionPreference = 'Stop'
$sid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
$system = [System.Security.Principal.SecurityIdentifier]::new('S-1-5-18')
$acl = Get-Acl -LiteralPath $env:EASYREGISTER_PRIVATE_DIRECTORY
$acl.SetAccessRuleProtection($true, $false)
foreach ($rule in @($acl.GetAccessRules($true, $false, [System.Security.Principal.SecurityIdentifier]))) {
    $acl.RemoveAccessRuleSpecific($rule)
}
foreach ($identity in @($sid, $system)) {
    $rule = [System.Security.AccessControl.FileSystemAccessRule]::new(
        $identity, 'FullControl', 'ContainerInherit,ObjectInherit', 'None', 'Allow')
    $acl.AddAccessRule($rule)
}
[System.IO.Directory]::SetAccessControl($env:EASYREGISTER_PRIVATE_DIRECTORY, $acl)
$verified = Get-Acl -LiteralPath $env:EASYREGISTER_PRIVATE_DIRECTORY
if (-not $verified.AreAccessRulesProtected) { throw 'private_directory_acl_not_protected' }
$allowed = @($sid.Value, $system.Value)
foreach ($rule in $verified.GetAccessRules($true, $true, [System.Security.Principal.SecurityIdentifier])) {
    if ($rule.IdentityReference.Value -notin $allowed) { throw 'private_directory_acl_not_private' }
}
"""
    env = dict(os.environ, EASYREGISTER_PRIVATE_DIRECTORY=str(path))
    result = subprocess.run(
        ["rtk", "proxy", "powershell.exe", "-NoProfile", "-NonInteractive", "-Command", command],
        env=env, capture_output=True, timeout=30, check=False,
    )
    _require(result.returncode == 0, "private_directory_acl_failed")


def prepare_session(root: Path, expected_email: str, *, now: float | None = None) -> dict[str, Any]:
    expected_email = expected_email.strip().lower()
    _require(bool(expected_email) and "@" in expected_email and not any(c.isspace() for c in expected_email),
             "expected_email_required")
    root = root.resolve(strict=True)
    _require(root.is_dir() and not str(root).startswith("\\\\"), "local_session_root_required")
    created = time.time() if now is None else now
    session = root / ("interactive-codex-" + uuid.uuid4().hex)
    _private_directory(session)
    home = session / "codex-home"
    home.mkdir(mode=0o700)
    (home / "config.toml").write_text(
        'cli_auth_credentials_store = "file"\nforced_login_method = "chatgpt"\n', encoding="utf-8",
    )
    _write_new_json(session / "request.private.json", {
        "schemaVersion": 1, "kind": "interactive-codex-login", "createdAt": created,
        "expiresAt": created + 900, "expectedEmail": expected_email,
    })
    return {"status": "prepared", "sessionDirectory": str(session), "codexHome": str(home)}


def _load_object(path: Path) -> dict[str, Any]:
    _require(not path.is_symlink() and path.is_file(), "input_file_missing_or_linked")
    _require(path.stat().st_size <= 1024 * 1024, "input_file_too_large")

    def unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in pairs:
            _require(key not in result, "duplicate_json_key")
            result[key] = item
        return result

    try:
        value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=unique_keys)
    except InteractiveLoginError:
        raise
    except (ValueError, UnicodeError):
        raise InteractiveLoginError("invalid_json") from None
    _require(isinstance(value, dict), "json_object_required")
    return value


def normalize_cli_auth(
    payload: dict[str, Any], *, expected_email: str, created_at: float, now: float,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """仅做结构/一致性检查；JWT 解码不等于验签、在线账号验证或业务成功。"""
    _require(payload.get("auth_mode") == "chatgpt" and not payload.get("OPENAI_API_KEY"),
             "chatgpt_login_required")
    tokens = payload.get("tokens")
    _require(isinstance(tokens, dict), "cli_tokens_missing")
    for key in ("id_token", "access_token", "refresh_token", "account_id"):
        _require(isinstance(tokens.get(key), str) and bool(tokens[key].strip()), "cli_" + key + "_missing")
    claims = {}
    for key in ("id_token", "access_token"):
        _require(len(tokens[key].split(".")) == 3, "invalid_" + key)
        claims[key] = decode_jwt_payload(tokens[key])
        current = claims[key]
        _require(current.get("iss") == "https://auth.openai.com", "unexpected_token_issuer")
        for field in ("iat", "exp"):
            _require(type(current.get(field)) in (int, float), "token_time_missing")
        _require(created_at - 60 <= current["iat"] <= now + 60, "token_not_from_fresh_login")
        _require(current["exp"] > now + 60, "token_expired_or_expiring")
        auth = current.get("https://api.openai.com/auth")
        _require(isinstance(auth, dict) and auth.get("chatgpt_account_id") == tokens["account_id"],
                 "account_identity_mismatch")
    id_claims = claims["id_token"]
    access_claims = claims["access_token"]
    _require(bool(id_claims.get("sub")) and id_claims.get("sub") == access_claims.get("sub"),
             "token_subject_mismatch")
    audience = id_claims.get("aud")
    audiences = [audience] if isinstance(audience, str) else audience
    _require(isinstance(audiences, list) and bool(audiences)
             and all(isinstance(value, str) and value for value in audiences), "id_token_audience_missing")
    if access_claims.get("client_id"):
        _require(access_claims["client_id"] in audiences, "token_client_mismatch")
    email = str(id_claims.get("email") or "").strip().lower()
    _require(email == expected_email.strip().lower() and bool(email), "expected_email_mismatch")
    profile = access_claims.get("https://api.openai.com/profile")
    if isinstance(profile, dict) and profile.get("email"):
        _require(str(profile["email"]).strip().lower() == email, "token_email_mismatch")
    id_auth = id_claims["https://api.openai.com/auth"]
    access_auth = access_claims["https://api.openai.com/auth"]
    _require(id_auth.get("chatgpt_plan_type") == access_auth.get("chatgpt_plan_type"),
             "token_plan_mismatch")
    last_refresh = payload.get("last_refresh")
    _require(isinstance(last_refresh, str), "cli_last_refresh_missing")
    try:
        refreshed = datetime.fromisoformat(last_refresh.replace("Z", "+00:00"))
    except (ValueError, OverflowError):
        raise InteractiveLoginError("cli_last_refresh_invalid") from None
    _require(refreshed.tzinfo is not None, "cli_last_refresh_invalid")
    _require(created_at - 60 <= refreshed.timestamp() <= now + 60, "cli_last_refresh_not_fresh")
    normalized = standardize_export_credential_payload({
        **tokens, "type": "codex", "email": email,
        "expired": datetime.fromtimestamp(access_claims["exp"], timezone.utc).isoformat().replace("+00:00", "Z"),
        "last_refresh": last_refresh,
        "https://api.openai.com/auth": id_auth,
    })
    personal = has_free_personal_oauth_claims(normalized)
    receipt = {
        "kind": "interactive-codex-login-staging", "schemaVersion": 1,
        "status": "awaiting_upstream_validation" if personal else "free_personal_claims_missing",
        "accountFingerprint": hashlib.sha256(tokens["account_id"].encode()).hexdigest()[:16],
        "claimChecksOnly": True, "signatureVerified": False, "upstreamVerified": False,
        "freePersonalClaimsPresent": personal, "productionPoolWritten": False,
        "registrationResumed": False, "smsPurchases": 0,
        "credentialFile": "credential.private.json",
    }
    return normalized, receipt


def stage_session(session: Path, *, now: float | None = None) -> dict[str, Any]:
    session = session.resolve(strict=True)
    _require(not str(session).startswith("\\\\"), "local_session_root_required")
    request = _load_object(session / "request.private.json")
    timestamp = time.time() if now is None else now
    _require(request.get("kind") == "interactive-codex-login" and request.get("schemaVersion") == 1,
             "session_request_invalid")
    created, expires = request.get("createdAt"), request.get("expiresAt")
    _require(type(created) in (int, float) and type(expires) in (int, float)
             and created <= timestamp <= expires and expires - created == 900, "session_expired_or_invalid")
    home = session / "codex-home"
    _require(not home.is_symlink() and home.resolve().parent == session, "codex_home_outside_session")
    normalized, receipt = normalize_cli_auth(
        _load_object(home / "auth.json"), expected_email=request.get("expectedEmail", ""),
        created_at=created, now=timestamp,
    )
    destination = session / "staged"
    _require(not destination.exists(), "session_already_staged")
    _private_directory(destination)
    _write_new_json(destination / "credential.private.json", normalized)
    _write_new_json(destination / "receipt.public.json", receipt)
    return {**receipt, "stagingDirectory": str(destination)}
