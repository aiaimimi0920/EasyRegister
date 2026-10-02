from __future__ import annotations

import base64
import json
import sys
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "server/services/orchestration_service/src"))
from others.interactive_codex_login import (  # noqa: E402
    InteractiveLoginError, _load_object, normalize_cli_auth, prepare_session, stage_session,
)


def jwt(claims):
    body = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    return "eyJhbGciOiJSUzI1NiJ9." + body + ".synthetic-signature-not-valid"


def fixture(now=None):
    now = time.time() if now is None else now
    auth = {"chatgpt_account_id": "synthetic-account-id", "chatgpt_plan_type": "free",
            "organizations": [{"title": "Personal", "role": "owner", "is_default": True}]}
    common = {"iss": "https://auth.openai.com", "iat": now, "exp": now + 3600,
              "sub": "synthetic-user", "https://api.openai.com/auth": auth}
    identity = {**common, "email": "operator@example.test", "aud": "synthetic-client"}
    access = {**common, "client_id": "synthetic-client",
              "https://api.openai.com/profile": {"email": "operator@example.test"}}
    payload = {"auth_mode": "chatgpt", "OPENAI_API_KEY": None,
               "last_refresh": datetime.fromtimestamp(now, timezone.utc).isoformat(),
               "tokens": {"account_id": "synthetic-account-id", "id_token": jwt(identity),
                          "access_token": jwt(access), "refresh_token": "synthetic-refresh-secret"}}
    return payload, identity, access


class NormalizeTests(unittest.TestCase):
    def test_preserves_standard_contract_without_claiming_verification(self):
        data, _, _ = fixture(1000)
        credential, receipt = normalize_cli_auth(data, expected_email="OPERATOR@example.test", created_at=990, now=1010)
        self.assertEqual(credential["type"], "codex")
        self.assertEqual(credential["account_id"], "synthetic-account-id")
        self.assertEqual(credential["last_refresh"], data["last_refresh"])
        self.assertTrue(receipt["freePersonalClaimsPresent"])
        self.assertEqual(receipt["status"], "awaiting_upstream_validation")
        for flag in ("signatureVerified", "upstreamVerified", "productionPoolWritten", "registrationResumed"):
            self.assertFalse(receipt[flag])
        text = json.dumps(receipt)
        for secret in ("operator@example.test", "synthetic-refresh-secret", data["tokens"]["access_token"]):
            self.assertNotIn(secret, text)

    def test_rejects_invalid_or_mismatched_credentials(self):
        cases = [
            ("api_login", "payload", {"auth_mode": "api"}),
            ("api_key", "payload", {"OPENAI_API_KEY": "synthetic-key"}),
            ("no_tokens", "payload", {"tokens": {}}),
            ("wrong_issuer", "id_token", {"iss": "https://untrusted.invalid"}),
            ("expired", "id_token", {"exp": 1001}),
            ("future", "access_token", {"iat": 2000}),
            ("old_cache", "id_token", {"iat": 100}),
            ("missing_iat", "id_token", {"iat": None}),
            ("bool_time", "id_token", {"exp": True}),
            ("wrong_email", "id_token", {"email": "wrong@example.test"}),
            ("wrong_subject", "access_token", {"sub": "other-user"}),
            ("wrong_client", "access_token", {"client_id": "other-client"}),
            ("missing_audience", "id_token", {"aud": []}),
            ("wrong_account", "access_token", {"https://api.openai.com/auth": {"chatgpt_account_id": "other"}}),
            ("wrong_profile", "access_token", {"https://api.openai.com/profile": {"email": "other@example.test"}}),
            ("stale_refresh", "payload", {"last_refresh": "1970-01-01T00:00:00Z"}),
            ("unqualified_refresh", "payload", {"last_refresh": "1970-01-01T00:16:40"}),
        ]
        for name, target, changes in cases:
            with self.subTest(name=name):
                data, identity, access = fixture(1000)
                if target == "payload":
                    data.update(changes)
                else:
                    claims = identity if target == "id_token" else access
                    claims.update(changes)
                    data["tokens"][target] = jwt(claims)
                with self.assertRaises(InteractiveLoginError) as result:
                    normalize_cli_auth(data, expected_email="operator@example.test", created_at=990, now=1010)
                self.assertNotIn("operator@example.test", str(result.exception))
                self.assertNotIn("synthetic-refresh-secret", str(result.exception))

    def test_nonpersonal_claims_never_become_free_success(self):
        for plan, organizations in (("plus", []), ("free", []), ("free", [{"title": "Team", "role": "owner"}])):
            with self.subTest(plan=plan, organizations=organizations):
                data, identity, access = fixture(1000)
                for key, claims in (("id_token", identity), ("access_token", access)):
                    claims["https://api.openai.com/auth"].update(chatgpt_plan_type=plan, organizations=organizations)
                    data["tokens"][key] = jwt(claims)
                _, receipt = normalize_cli_auth(data, expected_email="operator@example.test", created_at=990, now=1010)
                self.assertFalse(receipt["freePersonalClaimsPresent"])
                self.assertEqual(receipt["status"], "free_personal_claims_missing")


class SessionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="unit-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_prepare_and_stage_isolated_once(self):
        prepared = prepare_session(self.root, "operator@example.test", now=1000)
        session = Path(prepared["sessionDirectory"])
        home = Path(prepared["codexHome"])
        self.assertEqual(home.parent, session)
        self.assertEqual(home.joinpath("config.toml").read_text(),
                         'cli_auth_credentials_store = "file"\nforced_login_method = "chatgpt"\n')
        payload, _, _ = fixture(1001)
        home.joinpath("auth.json").write_text(json.dumps(payload), encoding="utf-8")
        source = home.joinpath("auth.json").read_bytes()
        receipt = stage_session(session, now=1010)
        dest = session / "staged/credential.private.json"
        original = dest.read_bytes()
        self.assertEqual(json.loads(original)["access_token"], payload["tokens"]["access_token"])
        self.assertFalse(original.startswith(b"\xef\xbb\xbf"))
        self.assertEqual(home.joinpath("auth.json").read_bytes(), source)
        self.assertFalse(receipt["productionPoolWritten"])
        with self.assertRaisesRegex(InteractiveLoginError, "session_already_staged"):
            stage_session(session, now=1010)
        self.assertEqual(dest.read_bytes(), original)

    def test_expired_session_does_not_create_staging(self):
        prepared = prepare_session(self.root, "operator@example.test", now=1000)
        session = Path(prepared["sessionDirectory"])
        with self.assertRaisesRegex(InteractiveLoginError, "session_expired_or_invalid"):
            stage_session(session, now=2000)
        self.assertFalse((session / "staged").exists())

    def test_invalid_email_does_not_create_directory(self):
        with self.assertRaisesRegex(InteractiveLoginError, "expected_email_required"):
            prepare_session(self.root, " ")
        self.assertEqual(list(self.root.iterdir()), [])

    def test_duplicate_keys_are_rejected(self):
        path = self.root / "invalid.json"
        path.write_text('{"tokens":{},"tokens":{"access_token":"secret"}}', encoding="utf-8")
        with self.assertRaisesRegex(InteractiveLoginError, "duplicate_json_key"):
            _load_object(path)


if __name__ == "__main__":
    unittest.main(verbosity=2)
