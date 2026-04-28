from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from typing import Any

from others.common_runtime import ensure_directory


def write_json_atomic(
    path: Path,
    payload: dict[str, Any],
    *,
    json_default: Any | None = None,
    sort_keys: bool = False,
    include_pid: bool = False,
    cleanup_temp: bool = False,
) -> None:
    ensure_directory(path.parent)
    temp_name = f"{path.name}."
    if include_pid:
        temp_name += f"{os.getpid()}."
    temp_name += f"{uuid.uuid4().hex}.tmp"
    tmp_path = path.parent / temp_name
    try:
        tmp_path.write_text(
            json.dumps(
                payload,
                ensure_ascii=False,
                indent=2,
                default=json_default,
                sort_keys=sort_keys,
            ),
            encoding="utf-8",
        )
        os.replace(tmp_path, path)
    finally:
        if cleanup_temp and tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
