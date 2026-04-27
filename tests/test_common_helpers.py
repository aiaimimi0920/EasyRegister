from __future__ import annotations

import base64
import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock


SRC_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from others.common import (  # noqa: E402
    canonical_free_artifact_name,
    canonical_team_artifact_name,
    env_flag,
    extract_account_id,
    free_manual_oauth_preserve_codes,
    free_manual_oauth_preserve_enabled,
    standardize_export_credential_payload,
    validate_small_success_seed_payload,
    write_json_atomic,
)


def _jwt_token(payload: dict[str, object]) -> str:
    encoded = base64.urlsafe_b64encode(json.dumps(payload).encode("utf-8")).decode("utf-8").rstrip("=")
    return f"header.{encoded}.signature"


def _valid_small_success_payload(*, created_at: datetime | None = None) -> dict[str, object]:
    created = created_at or datetime.now(timezone.utc)
    return {
        "platformOrganization": {"status": "completed"},
        "chatgptLogin": {
            "status": "completed",
            "personalWorkspaceId": "ws_personal",
        },
        "chatgptLoginDetails": {
            "clientBootstrap": {
                "authStatus": "logged_in",
                "structure": "personal",
            }
        },
        "mailboxRef": "mailbox-ref",
        "mailboxSessionId": "mailbox-session",
        "createdAt": created.isoformat().replace("+00:00", "Z"),
    }


class CommonHelpersTests(unittest.TestCase):
    def test_env_flag_uses_default_for_missing_value(self) -> None:
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertTrue(env_flag("MISSING_FLAG", True))
            self.assertFalse(env_flag("MISSING_FLAG", False))

    def test_preserve_enabled_uses_step_input_before_env(self) -> None:
        with mock.patch.dict(
            "os.environ",
            {"REGISTER_FREE_MANUAL_OAUTH_PRESERVE_ENABLED": "true"},
            clear=True,
        ):
            self.assertFalse(
                free_manual_oauth_preserve_enabled(
                    {"free_manual_oauth_preserve_enabled": "false"}
                )
            )
            self.assertTrue(
                free_manual_oauth_preserve_enabled(
                    {"free_manual_oauth_preserve_enabled": ""}
                )
            )
            self.assertTrue(free_manual_oauth_preserve_enabled())

    def test_preserve_codes_support_step_input_env_and_default(self) -> None:
        with mock.patch.dict(
            "os.environ",
            {"REGISTER_FREE_MANUAL_OAUTH_PRESERVE_ERROR_CODES": "env_code"},
            clear=True,
        ):
            self.assertEqual(
                {"step_code", "next_code"},
                free_manual_oauth_preserve_codes(
                    {"free_manual_oauth_preserve_error_codes": "step_code,next_code"}
                ),
            )
            self.assertEqual({"env_code"}, free_manual_oauth_preserve_codes({}))
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertEqual(
                {"free_personal_workspace_missing", "obtain_codex_oauth_failed"},
                free_manual_oauth_preserve_codes(),
            )

    def test_validate_small_success_seed_payload_accepts_valid_payload(self) -> None:
        with mock.patch.dict("os.environ", {}, clear=True):
            ok, reason = validate_small_success_seed_payload(_valid_small_success_payload())
        self.assertTrue(ok)
        self.assertEqual("", reason)

    def test_validate_small_success_seed_payload_rejects_expired_seed(self) -> None:
        payload = _valid_small_success_payload(
            created_at=datetime.now(timezone.utc) - timedelta(seconds=20)
        )
        with mock.patch.dict(
            "os.environ",
            {"REGISTER_SMALL_SUCCESS_SEED_MAX_AGE_SECONDS": "5"},
            clear=True,
        ):
            ok, reason = validate_small_success_seed_payload(payload)
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("small_success_seed_too_old:"))

    def test_extract_account_id_supports_top_level_auth_claims(self) -> None:
        payload = {
            "https://api.openai.com/auth": {
                "chatgpt_account_id": "acct-top-level",
            }
        }
        self.assertEqual("acct-top-level", extract_account_id(payload))

    def test_canonical_artifact_names_are_shared(self) -> None:
        payload = {
            "email": "user@example.com",
            "https://api.openai.com/auth": {
                "chatgpt_account_id": "org-abcdef12-rest",
            },
        }
        self.assertEqual(
            "codex-free-org-user@example.com.json",
            canonical_free_artifact_name(payload),
        )
        self.assertEqual(
            "codex-team-org-user@example.com.json",
            canonical_team_artifact_name(payload, is_mother=False),
        )
        self.assertEqual(
            "codex-team-mother-org-user@example.com.json",
            canonical_team_artifact_name(payload, is_mother=True),
        )

    def test_standardize_export_credential_payload_uses_nested_claims_and_tokens(self) -> None:
        payload = {
            "auth": {
                "account_id": "acct-auth",
                "type": "codex",
                "disabled": "false",
                "https://api.openai.com/auth": {
                    "chatgpt_account_id": "acct-auth",
                    "organizations": [{"id": "org-auth"}],
                },
            },
            "refresh_token": "refresh-token",
            "id_token": _jwt_token(
                {
                    "https://api.openai.com/profile": {
                        "email": "token@example.com",
                    }
                }
            ),
        }

        standardized = standardize_export_credential_payload(payload)

        self.assertEqual("acct-auth", standardized["account_id"])
        self.assertEqual("token@example.com", standardized["email"])
        self.assertEqual("refresh-token", standardized["refresh_token"])
        self.assertIn("https://api.openai.com/auth", standardized)
        self.assertIn("https://api.openai.com/profile", standardized)

    def test_write_json_atomic_cleans_temp_file_on_replace_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            target = Path(tmp_dir) / "state.json"
            def _raise_replace(_source: str, _target: str) -> None:
                raise RuntimeError("replace_failed")

            with mock.patch("others.common.os.replace", _raise_replace):
                with self.assertRaises(RuntimeError):
                    write_json_atomic(
                        target,
                        {"ok": True},
                        include_pid=True,
                        cleanup_temp=True,
                    )

            self.assertFalse(target.exists())
            self.assertEqual([], list(Path(tmp_dir).glob("*.tmp")))


if __name__ == "__main__":
    unittest.main()
