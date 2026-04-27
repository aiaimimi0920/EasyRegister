from __future__ import annotations

import os
import time
from pathlib import Path


def lock_file_is_stale(lock_path: str | Path, *, stale_after_seconds: float, now: float | None = None) -> bool:
    if float(stale_after_seconds or 0.0) <= 0:
        return False
    path = Path(lock_path)
    try:
        modified_at = float(path.stat().st_mtime)
    except FileNotFoundError:
        return False
    current_time = time.time() if now is None else float(now)
    return (current_time - modified_at) >= float(stale_after_seconds)


def try_acquire_lock(lock_path: str | Path, *, stale_after_seconds: float = 0.0) -> bool:
    path = Path(lock_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    for _ in range(3):
        try:
            fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            if not lock_file_is_stale(path, stale_after_seconds=stale_after_seconds):
                return False
            try:
                path.unlink()
            except FileNotFoundError:
                continue
            except OSError:
                return False
            continue
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(f"{os.getpid()}\n{int(time.time())}\n")
        return True
    return False


def release_lock(lock_path: str | Path) -> None:
    Path(lock_path).unlink(missing_ok=True)
