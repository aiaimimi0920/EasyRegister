from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "promote-bridge-seeds-to-openai-converted.ps1"


@unittest.skipUnless(shutil.which("powershell"), "powershell is required")
class BridgePromotionScriptTests(unittest.TestCase):
    def test_recent_empty_lock_is_removed_and_eligible_seed_is_copied(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            bridge_dir = root / "bridge"
            target_dir = root / "target"
            pending_dir = root / "pending"
            log_dir = root / "logs"
            runtime_dir = root / "runtime"
            lock_path = runtime_dir / "bridge-promotion.lock"

            bridge_dir.mkdir(parents=True)
            target_dir.mkdir(parents=True)
            pending_dir.mkdir(parents=True)
            log_dir.mkdir(parents=True)
            runtime_dir.mkdir(parents=True)

            payload = {
                "platformOrganization": {"status": "completed"},
                "outcome": "small_success",
                "source": "protocol_small_success",
                "accessToken": "access-token",
                "refreshToken": "refresh-token",
                "mailboxRef": "mailbox-ref",
                "mailboxSessionId": "mailbox-session-id",
                "createdAt": "2026-07-05T07:10:32Z",
            }
            source_file = bridge_dir / "small-20260705-071032-velvethollow431eec-pswt@908209381.shop-245ad8.json"
            source_file.write_text(json.dumps(payload), encoding="utf-8")

            lock_path.write_text("", encoding="utf-8")
            os.utime(lock_path, None)

            result = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    str(SCRIPT_PATH),
                    "-BridgeDir",
                    str(bridge_dir),
                    "-TargetDir",
                    str(target_dir),
                    "-PendingDir",
                    str(pending_dir),
                    "-LogDir",
                    str(log_dir),
                    "-LockPath",
                    str(lock_path),
                    "-LockStaleSeconds",
                    "1800",
                    "-MinSourceAgeSeconds",
                    "0",
                    "-PendingSeedMaxAgeSeconds",
                    "0",
                ],
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(
                result.returncode,
                0,
                msg=f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}",
            )
            copied_file = target_dir / source_file.name
            self.assertTrue(
                copied_file.exists(),
                msg=f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}",
            )

            log_files = sorted(log_dir.glob("bridge-promotion-*.log"))
            self.assertTrue(log_files, "expected a promotion log file to be written")
            log_text = log_files[-1].read_text(encoding="utf-8")
            self.assertIn("metadata_missing_lock_removed", log_text)
            self.assertIn("summary new_copied=1", log_text)

    def test_recent_dead_pid_lock_is_removed_and_eligible_seed_is_copied(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            bridge_dir = root / "bridge"
            target_dir = root / "target"
            pending_dir = root / "pending"
            log_dir = root / "logs"
            runtime_dir = root / "runtime"
            lock_path = runtime_dir / "bridge-promotion.lock"

            bridge_dir.mkdir(parents=True)
            target_dir.mkdir(parents=True)
            pending_dir.mkdir(parents=True)
            log_dir.mkdir(parents=True)
            runtime_dir.mkdir(parents=True)

            payload = {
                "platformOrganization": {"status": "completed"},
                "outcome": "small_success",
                "source": "protocol_small_success",
                "accessToken": "access-token",
                "refreshToken": "refresh-token",
                "mailboxRef": "mailbox-ref",
                "mailboxSessionId": "mailbox-session-id",
                "createdAt": "2026-07-05T07:10:32Z",
            }
            source_file = bridge_dir / "small-20260705-071032-velvethollow431eec-pswt@908209381.shop-245ad8.json"
            source_file.write_text(json.dumps(payload), encoding="utf-8")

            lock_path.write_text("pid=999999\nstarted=2026-07-05T15:00:00Z\n", encoding="utf-8")
            os.utime(lock_path, None)

            result = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    str(SCRIPT_PATH),
                    "-BridgeDir",
                    str(bridge_dir),
                    "-TargetDir",
                    str(target_dir),
                    "-PendingDir",
                    str(pending_dir),
                    "-LogDir",
                    str(log_dir),
                    "-LockPath",
                    str(lock_path),
                    "-LockStaleSeconds",
                    "1800",
                    "-MinSourceAgeSeconds",
                    "0",
                    "-PendingSeedMaxAgeSeconds",
                    "0",
                ],
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(
                result.returncode,
                0,
                msg=f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}",
            )
            copied_file = target_dir / source_file.name
            self.assertTrue(
                copied_file.exists(),
                msg=f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}",
            )

            log_files = sorted(log_dir.glob("bridge-promotion-*.log"))
            self.assertTrue(log_files, "expected a promotion log file to be written")
            log_text = log_files[-1].read_text(encoding="utf-8")
            self.assertIn("dead_pid_lock_removed", log_text)
            self.assertIn("summary new_copied=1", log_text)

    def test_protocol_small_success_seed_with_platform_auth_is_promoted_to_pending(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            bridge_dir = root / "bridge"
            converted_dir = root / "converted"
            pending_dir = root / "pending"
            log_dir = root / "logs"
            lock_path = root / "bridge-promotion.lock"

            bridge_dir.mkdir(parents=True)
            converted_dir.mkdir(parents=True)
            pending_dir.mkdir(parents=True)
            log_dir.mkdir(parents=True)

            payload = {
                "outcome": "small_success",
                "source": "protocol_small_success",
                "email": "venf@007.hzeg.eu.org",
                "mailboxRef": "im215:venf@007.hzeg.eu.org",
                "mailboxSessionId": "mailbox-session-id",
                "createdAt": "2026-07-06T14:42:02Z",
                "platformAuth": {
                    "clientId": "app_client",
                    "redirectUri": "https://platform.openai.com/auth/callback",
                    "audience": "https://api.openai.com/v1",
                    "scope": "openid profile email offline_access",
                    "deviceId": "device-id",
                    "codeVerifier": "code-verifier",
                    "state": "state-value",
                    "nonce": "nonce-value",
                    "auth0Client": "auth0-client",
                },
            }
            source_file = bridge_dir / "small-20260706-144202-goldenbadgerd86241-venf@007.hzeg.eu.org-6c6479.json"
            source_file.write_text(json.dumps(payload), encoding="utf-8")

            result = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    str(SCRIPT_PATH),
                    "-BridgeDir",
                    str(bridge_dir),
                    "-TargetDir",
                    str(converted_dir),
                    "-PendingDir",
                    str(pending_dir),
                    "-LogDir",
                    str(log_dir),
                    "-LockPath",
                    str(lock_path),
                    "-MinSourceAgeSeconds",
                    "0",
                    "-PendingSeedMaxAgeSeconds",
                    "0",
                ],
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(
                0,
                result.returncode,
                msg=f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}",
            )
            self.assertFalse((converted_dir / source_file.name).exists())
            self.assertTrue((pending_dir / source_file.name).exists())

            log_files = sorted(log_dir.glob("bridge-promotion-*.log"))
            self.assertTrue(log_files, "expected a promotion log file to be written")
            log_text = log_files[-1].read_text(encoding="utf-8")
            self.assertIn("summary", log_text)

    def test_protocol_small_success_seed_is_not_repromoted_when_failed_twice_copy_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            bridge_dir = root / "bridge"
            converted_dir = root / "converted"
            pending_dir = root / "pending"
            failed_twice_dir = root / "failed-twice"
            log_dir = root / "logs"
            lock_path = root / "bridge-promotion.lock"

            bridge_dir.mkdir(parents=True)
            converted_dir.mkdir(parents=True)
            pending_dir.mkdir(parents=True)
            failed_twice_dir.mkdir(parents=True)
            log_dir.mkdir(parents=True)

            payload = {
                "outcome": "small_success",
                "source": "protocol_small_success",
                "email": "venf@007.hzeg.eu.org",
                "mailboxRef": "im215:venf@007.hzeg.eu.org",
                "mailboxSessionId": "mailbox-session-id",
                "createdAt": "2026-07-06T14:42:02Z",
                "platformAuth": {
                    "clientId": "app_client",
                    "redirectUri": "https://platform.openai.com/auth/callback",
                    "audience": "https://api.openai.com/v1",
                    "scope": "openid profile email offline_access",
                    "deviceId": "device-id",
                    "codeVerifier": "code-verifier",
                    "state": "state-value",
                    "nonce": "nonce-value",
                    "auth0Client": "auth0-client",
                },
            }
            source_file = bridge_dir / "small-20260706-144202-goldenbadgerd86241-venf@007.hzeg.eu.org-6c6479.json"
            source_file.write_text(json.dumps(payload), encoding="utf-8")
            (failed_twice_dir / source_file.name).write_text(json.dumps(payload), encoding="utf-8")

            result = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    str(SCRIPT_PATH),
                    "-BridgeDir",
                    str(bridge_dir),
                    "-TargetDir",
                    str(converted_dir),
                    "-PendingDir",
                    str(pending_dir),
                    "-FailedTwiceDir",
                    str(failed_twice_dir),
                    "-LogDir",
                    str(log_dir),
                    "-LockPath",
                    str(lock_path),
                    "-MinSourceAgeSeconds",
                    "0",
                    "-PendingSeedMaxAgeSeconds",
                    "0",
                ],
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(
                0,
                result.returncode,
                msg=f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}",
            )
            self.assertFalse((pending_dir / source_file.name).exists())

            log_files = sorted(log_dir.glob("bridge-promotion-*.log"))
            self.assertTrue(log_files, "expected a promotion log file to be written")
            log_text = log_files[-1].read_text(encoding="utf-8")
            self.assertIn("summary", log_text)
            self.assertIn("skipped_existing=1", log_text)

    def test_stale_protocol_small_success_seed_is_not_promoted_to_pending(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            bridge_dir = root / "bridge"
            converted_dir = root / "converted"
            pending_dir = root / "pending"
            log_dir = root / "logs"
            lock_path = root / "bridge-promotion.lock"

            bridge_dir.mkdir(parents=True)
            converted_dir.mkdir(parents=True)
            pending_dir.mkdir(parents=True)
            log_dir.mkdir(parents=True)

            payload = {
                "outcome": "small_success",
                "source": "protocol_small_success",
                "email": "stale@example.com",
                "mailboxRef": "im215:stale@example.com",
                "mailboxSessionId": "mailbox-session-id",
                "createdAt": "2026-01-01T00:00:00Z",
                "platformAuth": {
                    "clientId": "app_client",
                    "redirectUri": "https://platform.openai.com/auth/callback",
                    "audience": "https://api.openai.com/v1",
                    "scope": "openid profile email offline_access",
                    "deviceId": "device-id",
                    "codeVerifier": "code-verifier",
                    "state": "state-value",
                    "nonce": "nonce-value",
                    "auth0Client": "auth0-client",
                },
            }
            source_file = bridge_dir / "small-20260101-stale-example.json"
            source_file.write_text(json.dumps(payload), encoding="utf-8")

            result = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    str(SCRIPT_PATH),
                    "-BridgeDir",
                    str(bridge_dir),
                    "-TargetDir",
                    str(converted_dir),
                    "-PendingDir",
                    str(pending_dir),
                    "-LogDir",
                    str(log_dir),
                    "-LockPath",
                    str(lock_path),
                    "-MinSourceAgeSeconds",
                    "0",
                    "-PendingSeedMaxAgeSeconds",
                    "900",
                ],
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(
                0,
                result.returncode,
                msg=f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}",
            )
            self.assertFalse((converted_dir / source_file.name).exists())
            self.assertFalse((pending_dir / source_file.name).exists())

            log_files = sorted(log_dir.glob("bridge-promotion-*.log"))
            self.assertTrue(log_files, "expected a promotion log file to be written")
            log_text = log_files[-1].read_text(encoding="utf-8")
            self.assertIn("skip_pending_seed_too_old", log_text)

    def test_stale_full_protocol_small_success_seed_still_promotes_to_converted(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            bridge_dir = root / "bridge"
            converted_dir = root / "converted"
            pending_dir = root / "pending"
            log_dir = root / "logs"
            lock_path = root / "bridge-promotion.lock"

            bridge_dir.mkdir(parents=True)
            converted_dir.mkdir(parents=True)
            pending_dir.mkdir(parents=True)
            log_dir.mkdir(parents=True)

            payload = {
                "platformOrganization": {"status": "completed"},
                "chatgptLogin": {"status": "completed", "workspaceId": "ws_123"},
                "outcome": "small_success",
                "source": "protocol_small_success",
                "accessToken": "access-token",
                "refreshToken": "refresh-token",
                "mailboxRef": "mailbox-ref",
                "mailboxSessionId": "mailbox-session-id",
                "createdAt": "2026-01-01T00:00:00Z",
                "platformAuth": {
                    "clientId": "app_client",
                    "redirectUri": "https://platform.openai.com/auth/callback",
                    "codeVerifier": "code-verifier",
                    "state": "state-value",
                    "nonce": "nonce-value",
                },
            }
            source_file = bridge_dir / "small-20260101-full-example.json"
            source_file.write_text(json.dumps(payload), encoding="utf-8")

            result = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    str(SCRIPT_PATH),
                    "-BridgeDir",
                    str(bridge_dir),
                    "-TargetDir",
                    str(converted_dir),
                    "-PendingDir",
                    str(pending_dir),
                    "-LogDir",
                    str(log_dir),
                    "-LockPath",
                    str(lock_path),
                    "-MinSourceAgeSeconds",
                    "0",
                    "-PendingSeedMaxAgeSeconds",
                    "900",
                ],
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(
                0,
                result.returncode,
                msg=f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}",
            )
            self.assertTrue((converted_dir / source_file.name).exists())
            self.assertFalse((pending_dir / source_file.name).exists())

    def test_old_duplicate_bridge_file_is_archived_when_lifecycle_copy_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            bridge_dir = root / "bridge"
            converted_dir = root / "converted"
            pending_dir = root / "pending"
            archive_dir = root / "bridge-archive"
            log_dir = root / "logs"
            lock_path = root / "bridge-promotion.lock"

            bridge_dir.mkdir(parents=True)
            converted_dir.mkdir(parents=True)
            pending_dir.mkdir(parents=True)
            archive_dir.mkdir(parents=True)
            log_dir.mkdir(parents=True)

            payload = {
                "platformOrganization": {"status": "completed"},
                "chatgptLogin": {"status": "completed", "workspaceId": "ws_123"},
                "outcome": "small_success",
                "source": "protocol_small_success",
                "accessToken": "access-token",
                "refreshToken": "refresh-token",
                "mailboxRef": "mailbox-ref",
                "mailboxSessionId": "mailbox-session-id",
                "createdAt": "2026-07-05T07:10:32Z",
            }
            source_file = bridge_dir / "small-20260705-071032-velvethollow431eec-pswt@908209381.shop-245ad8.json"
            source_file.write_text(json.dumps(payload), encoding="utf-8")
            (converted_dir / source_file.name).write_text(json.dumps(payload), encoding="utf-8")
            old_timestamp = 1_700_000_000
            os.utime(source_file, (old_timestamp, old_timestamp))

            result = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    str(SCRIPT_PATH),
                    "-BridgeDir",
                    str(bridge_dir),
                    "-TargetDir",
                    str(converted_dir),
                    "-PendingDir",
                    str(pending_dir),
                    "-ArchiveDuplicateBridgeDir",
                    str(archive_dir),
                    "-ArchiveDuplicateOlderThanSeconds",
                    "1",
                    "-LogDir",
                    str(log_dir),
                    "-LockPath",
                    str(lock_path),
                    "-MinSourceAgeSeconds",
                    "0",
                    "-PendingSeedMaxAgeSeconds",
                    "0",
                ],
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(
                0,
                result.returncode,
                msg=f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}",
            )
            self.assertFalse(source_file.exists())
            self.assertTrue((archive_dir / source_file.name).exists())

            log_files = sorted(log_dir.glob("bridge-promotion-*.log"))
            self.assertTrue(log_files, "expected a promotion log file to be written")
            log_text = log_files[-1].read_text(encoding="utf-8")
            self.assertIn("archived_existing_lifecycle", log_text)


if __name__ == "__main__":
    unittest.main()
