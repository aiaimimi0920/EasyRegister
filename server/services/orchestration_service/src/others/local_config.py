from __future__ import annotations

import re
from pathlib import Path


def find_easyemail_config(start_path: Path | None = None) -> Path | None:
    current = Path(start_path or __file__).resolve()
    for parent in current.parents:
        direct = parent / "EmailService" / "deploy" / "EasyEmail" / "config.yaml"
        if direct.exists():
            return direct
        nested = parent / "server" / "EmailService" / "deploy" / "EasyEmail" / "config.yaml"
        if nested.exists():
            return nested
    return None


def read_easyemail_server_api_key(start_path: Path | None = None) -> str:
    config_path = find_easyemail_config(start_path=start_path)
    if config_path is None:
        return ""
    try:
        text = config_path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ""
    match = re.search(r'(?m)^\s*apiKey:\s*"([^"]+)"\s*$', text)
    if match:
        return str(match.group(1) or "").strip()
    match = re.search(r"(?m)^\s*apiKey:\s*([^\s#]+)\s*$", text)
    if match:
        return str(match.group(1) or "").strip().strip('"').strip("'")
    return ""
