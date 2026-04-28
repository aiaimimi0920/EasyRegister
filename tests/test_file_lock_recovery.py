from __future__ import annotations

import os
import sys
import tempfile
import time
import unittest
from pathlib import Path


SRC_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from others.file_lock import lock_file_is_stale, read_lock_metadata, release_lock, try_acquire_lock  # noqa: E402


class FileLockRecoveryTests(unittest.TestCase):
    def test_try_acquire_lock_when_absent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            lock_path = Path(tmp_dir) / "cleanup.lock"
            self.assertTrue(try_acquire_lock(lock_path))
            self.assertTrue(lock_path.is_file())
            metadata = read_lock_metadata(lock_path)
            self.assertIn("pid", metadata)
            self.assertIn("acquired_at_epoch", metadata)
            self.assertIn("acquired_at_iso", metadata)

    def test_try_acquire_lock_rejects_fresh_lock(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            lock_path = Path(tmp_dir) / "cleanup.lock"
            lock_path.write_text("existing\n", encoding="utf-8")
            self.assertFalse(try_acquire_lock(lock_path, stale_after_seconds=600.0))

    def test_try_acquire_lock_replaces_stale_lock(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            lock_path = Path(tmp_dir) / "cleanup.lock"
            lock_path.write_text("stale\n", encoding="utf-8")
            stale_timestamp = time.time() - 900.0
            os.utime(lock_path, (stale_timestamp, stale_timestamp))

            self.assertTrue(lock_file_is_stale(lock_path, stale_after_seconds=600.0))
            self.assertTrue(try_acquire_lock(lock_path, stale_after_seconds=600.0))
            self.assertNotEqual("stale\n", lock_path.read_text(encoding="utf-8"))

    def test_release_lock_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            lock_path = Path(tmp_dir) / "cleanup.lock"
            lock_path.write_text("payload\n", encoding="utf-8")
            release_lock(lock_path)
            release_lock(lock_path)
            self.assertFalse(lock_path.exists())


if __name__ == "__main__":
    unittest.main()
