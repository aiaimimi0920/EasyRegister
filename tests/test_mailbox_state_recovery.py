from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "recover-mailbox-dynamic-blacklist.py"


def _load_recovery_module():
    spec = importlib.util.spec_from_file_location("recover_mailbox_dynamic_blacklist", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load script: {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class MailboxStateRecoveryTests(unittest.TestCase):
    def test_recover_payload_suppresses_transient_email_otp_blacklists_without_losing_counts(self) -> None:
        recovery = _load_recovery_module()
        payload = {
            "schemaVersion": 3,
            "businesses": {
                "openai": {
                    "domains": {
                        "zhooo.org": {
                            "blacklisted": True,
                            "blacklistReason": "email_otp_failure_threshold",
                            "failureReasons": {
                                "email_otp_timeout": 7,
                                "create_account_failure": 2,
                            },
                        }
                    },
                    "providers": {
                        "moemail": {
                            "blacklisted": True,
                            "blacklistReason": "provider_email_otp_failure_threshold",
                            "failureReasons": {
                                "email_otp_timeout": 12,
                                "obtain_codex_oauth": 1,
                            },
                        }
                    },
                }
            },
        }

        summary = recovery.recover_payload(payload, business_keys=("openai",))

        domain = payload["businesses"]["openai"]["domains"]["zhooo.org"]
        provider = payload["businesses"]["openai"]["providers"]["moemail"]
        self.assertFalse(domain["blacklisted"])
        self.assertEqual("", domain["blacklistReason"])
        self.assertEqual({"create_account_failure": 2}, domain["failureReasons"])
        self.assertEqual({"email_otp_timeout": 7}, domain["suppressedFailureReasons"])
        self.assertFalse(provider["blacklisted"])
        self.assertEqual("", provider["blacklistReason"])
        self.assertEqual({"obtain_codex_oauth": 1}, provider["failureReasons"])
        self.assertEqual({"email_otp_timeout": 12}, provider["suppressedFailureReasons"])
        self.assertEqual(2, summary["recoveredEntries"])
        self.assertEqual(2, summary["suppressedFailureReasonEntries"])

    def test_recover_payload_preserves_strong_mailbox_blacklists(self) -> None:
        recovery = _load_recovery_module()
        payload = {
            "schemaVersion": 3,
            "businesses": {
                "openai": {
                    "domains": {
                        "bad.test": {
                            "blacklisted": True,
                            "blacklistReason": "unsupported_email",
                            "failureReasons": {
                                "unsupported_email": 3,
                                "email_otp_timeout": 9,
                            },
                        },
                        "blocked.test": {
                            "blacklisted": True,
                            "blacklistReason": "registration_disallowed",
                            "failureReasons": {"registration_disallowed": 1},
                        },
                    },
                    "providers": {},
                }
            },
        }

        summary = recovery.recover_payload(payload, business_keys=("openai",))

        bad = payload["businesses"]["openai"]["domains"]["bad.test"]
        blocked = payload["businesses"]["openai"]["domains"]["blocked.test"]
        self.assertTrue(bad["blacklisted"])
        self.assertEqual("unsupported_email", bad["blacklistReason"])
        self.assertEqual({"unsupported_email": 3, "email_otp_timeout": 9}, bad["failureReasons"])
        self.assertNotIn("suppressedFailureReasons", bad)
        self.assertTrue(blocked["blacklisted"])
        self.assertEqual("registration_disallowed", blocked["blacklistReason"])
        self.assertEqual(0, summary["recoveredEntries"])
        self.assertEqual(2, summary["preservedStrongEntries"])

    def test_apply_recovery_writes_backup_and_updates_state(self) -> None:
        recovery = _load_recovery_module()
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "register-mailbox-domain-state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "schemaVersion": 3,
                        "businesses": {
                            "openai": {
                                "domains": {
                                    "zhooo.org": {
                                        "blacklisted": True,
                                        "blacklistReason": "email_otp_failure_threshold",
                                        "failureReasons": {"email_otp_timeout": 7},
                                    }
                                },
                                "providers": {},
                            }
                        },
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            summary = recovery.apply_recovery(
                state_path,
                business_keys=("openai",),
                timestamp_slug="20260602-123456",
            )

            backup_path = Path(summary["backupPath"])
            self.assertTrue(backup_path.is_file())
            self.assertEqual(
                True,
                json.loads(backup_path.read_text(encoding="utf-8"))["businesses"]["openai"]["domains"]["zhooo.org"]["blacklisted"],
            )
            updated = json.loads(state_path.read_text(encoding="utf-8"))
            domain = updated["businesses"]["openai"]["domains"]["zhooo.org"]
            self.assertFalse(domain["blacklisted"])
            self.assertEqual({"email_otp_timeout": 7}, domain["suppressedFailureReasons"])


if __name__ == "__main__":
    unittest.main()
