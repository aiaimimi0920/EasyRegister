from __future__ import annotations

import os
from pathlib import Path

from others.paths import resolve_shared_root


def env_text(name: str, default: str = "") -> str:
    return str(os.environ.get(name) or default).strip()


def env_first_text(*names: str, default: str = "") -> str:
    for name in names:
        value = env_text(name, "")
        if value:
            return value
    return str(default).strip()


def env_bool(name: str, default: bool = False) -> bool:
    raw = env_text(name, "")
    if not raw:
        return bool(default)
    return raw.lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    raw = env_text(name, str(default))
    try:
        return int(raw)
    except Exception:
        return default


def env_float(name: str, default: float) -> float:
    raw = env_text(name, str(default))
    try:
        return float(raw)
    except Exception:
        return default


def env_ratio(name: str, default: float = 0.0) -> float:
    raw = env_text(name, str(default))
    try:
        value = float(raw)
    except Exception:
        value = float(default or 0.0)
    if value > 1.0:
        value = value / 100.0
    return max(0.0, min(1.0, value))


def env_percent_value(name: str, default: float = 0.0) -> float:
    raw = env_text(name, str(default))
    try:
        value = float(raw)
    except Exception:
        value = float(default or 0.0)
    if 0.0 < value <= 1.0:
        value = value * 100.0
    return max(0.0, min(100.0, value))


def split_csv(raw: str) -> tuple[str, ...]:
    normalized = (
        str(raw or "")
        .replace(";", ",")
        .replace("|", ",")
        .replace("\r", ",")
        .replace("\n", ",")
    )
    values: list[str] = []
    seen: set[str] = set()
    for item in normalized.split(","):
        cleaned = str(item or "").strip()
        if not cleaned:
            continue
        lowered = cleaned.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        values.append(cleaned)
    return tuple(values)


def split_path_list(raw: str) -> tuple[str, ...]:
    normalized = str(raw or "").strip()
    if not normalized:
        return ()
    values: list[str] = []
    seen: set[str] = set()
    for item in normalized.split(os.pathsep):
        cleaned = str(item or "").strip()
        if not cleaned:
            continue
        lowered = cleaned.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        values.append(cleaned)
    return tuple(values)


def env_path(name: str, default: str = "") -> Path:
    return Path(env_text(name, default) or default).expanduser().resolve()


DEFAULT_EASY_PROTOCOL_TIMEOUT_SECONDS = 900
DEFAULT_EASY_PROTOCOL_ACCOUNT_AUDIT_TIMEOUT_SECONDS = 300
DEFAULT_ACCOUNT_AUDIT_WORKER_HARD_TIMEOUT_SECONDS = 420.0


def easyprotocol_timeout_seconds() -> int:
    raw = env_text("EASY_PROTOCOL_TIMEOUT_SECONDS", "")
    if raw:
        try:
            return max(1, int(float(raw)))
        except Exception:
            return DEFAULT_EASY_PROTOCOL_TIMEOUT_SECONDS
    return DEFAULT_EASY_PROTOCOL_TIMEOUT_SECONDS


def account_audit_protocol_timeout_seconds() -> int:
    """HTTP budget EasyRegister allows one account-audit protocol call."""
    raw = env_text("EASY_PROTOCOL_ACCOUNT_AUDIT_TIMEOUT_SECONDS", "")
    if raw:
        try:
            return max(1, int(float(raw)))
        except Exception:
            return DEFAULT_EASY_PROTOCOL_ACCOUNT_AUDIT_TIMEOUT_SECONDS
    return min(easyprotocol_timeout_seconds(), DEFAULT_EASY_PROTOCOL_ACCOUNT_AUDIT_TIMEOUT_SECONDS)


def account_audit_worker_hard_timeout_seconds() -> float:
    """Wall clock after which the supervisor kills a stuck account-audit worker."""
    raw = env_text("REGISTER_ACCOUNT_AUDIT_WORKER_HARD_TIMEOUT_SECONDS", "")
    if raw:
        try:
            return float(raw)
        except Exception:
            return DEFAULT_ACCOUNT_AUDIT_WORKER_HARD_TIMEOUT_SECONDS
    return DEFAULT_ACCOUNT_AUDIT_WORKER_HARD_TIMEOUT_SECONDS


def resolve_output_root_text(default: str = "/shared/register-output") -> str:
    return env_text("REGISTER_OUTPUT_ROOT", default) or default


def resolve_output_root(default: str = "/shared/register-output") -> Path:
    return Path(resolve_output_root_text(default)).expanduser().resolve()


def resolve_shared_root_from_env(default_output_root: str = "/shared/register-output") -> Path:
    return resolve_shared_root(resolve_output_root_text(default_output_root))
