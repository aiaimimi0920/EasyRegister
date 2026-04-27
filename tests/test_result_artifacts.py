from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path


SRC_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from others.result_artifacts import (  # noqa: E402
    FREE_SMALL_SUCCESS_SOURCE_CANDIDATES,
    credential_backwrite_actions,
    first_existing_output_path,
    normalized_team_pool_artifacts,
    restored_path_for_source,
    result_payload,
    team_auth_path,
    team_mother_identity,
)


class _ResultStub:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def to_dict(self) -> dict[str, object]:
        return dict(self._payload)


class ResultArtifactsTests(unittest.TestCase):
    def test_result_payload_reads_to_dict_result(self) -> None:
        payload = {"ok": True, "outputs": {"step": {"value": "x"}}}
        self.assertEqual(payload, result_payload(_ResultStub(payload)))

    def test_first_existing_output_path_uses_first_existing_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            path = root / "seed.json"
            path.write_text("{}", encoding="utf-8")
            payload = {
                "outputs": {
                    "create-openai-account": {"storage_path": str(path)},
                    "acquire-small-success-artifact": {"claimed_path": str(root / "missing.json")},
                }
            }
            resolved = first_existing_output_path(payload, FREE_SMALL_SUCCESS_SOURCE_CANDIDATES)
            self.assertEqual(path.resolve(), resolved)

    def test_normalized_team_pool_artifacts_resolves_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            team_path = Path(tmp_dir) / "member.json"
            team_path.write_text("{}", encoding="utf-8")
            payload = {
                "outputs": {
                    "collect-team-pool-artifacts": {
                        "artifacts": [
                            {
                                "kind": "member",
                                "email": "a@example.com",
                                "preferred_name": "member.json",
                                "team_pool_path": str(team_path),
                            }
                        ]
                    }
                }
            }
            self.assertEqual(
                [
                    {
                        "kind": "member",
                        "email": "a@example.com",
                        "preferred_name": "member.json",
                        "path": str(team_path.resolve()),
                    }
                ],
                normalized_team_pool_artifacts(payload),
            )

    def test_team_mother_identity_and_auth_path_use_team_outputs(self) -> None:
        payload = {
            "outputs": {
                "acquire-team-mother-artifact": {
                    "original_name": "mother.json",
                    "email": "mother@example.com",
                    "account_id": "acct-1",
                    "source_path": "source.json",
                    "claimed_path": "claimed.json",
                },
                "obtain-team-mother-oauth": {
                    "successPath": "refreshed.json",
                },
            }
        }
        self.assertEqual(
            {
                "original_name": "mother.json",
                "email": "mother@example.com",
                "account_id": "acct-1",
            },
            team_mother_identity(payload),
        )
        self.assertEqual("refreshed.json", team_auth_path(payload, "fallback.json"))

    def test_restored_path_for_source_checks_finalize_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            claimed = root / "claimed.json"
            restored = root / "restored.json"
            claimed.write_text("{}", encoding="utf-8")
            restored.write_text("{}", encoding="utf-8")
            payload = {
                "outputs": {
                    "finalize-small-success-artifact": {
                        "claimed_path": str(claimed),
                        "restored_path": str(restored),
                    }
                }
            }
            self.assertEqual(restored.resolve(), restored_path_for_source(payload, claimed.resolve()))

    def test_credential_backwrite_actions_preserve_multiple_source_candidates(self) -> None:
        payload = {
            "outputs": {
                "obtain-codex-oauth": {"successPath": "oauth.json"},
                "create-openai-account": {"storage_path": "create.json"},
                "acquire-small-success-artifact": {
                    "source_path": "claimed.json",
                    "claimed_path": "claimed.json",
                },
                "acquire-team-member-candidates": {
                    "members": [
                        {"source_path": "member-source.json"},
                    ]
                },
                "obtain-team-member-oauth-batch": {
                    "artifacts": [
                        {"successPath": "member-oauth.json"},
                    ]
                },
            }
        }
        actions = credential_backwrite_actions(payload)
        self.assertEqual(
            [
                {
                    "kind": "generic_oauth_refresh",
                    "source_path": "create.json",
                    "refreshed_path": "oauth.json",
                    "force": False,
                },
                {
                    "kind": "generic_oauth_refresh",
                    "source_path": "claimed.json",
                    "refreshed_path": "oauth.json",
                    "force": False,
                },
                {
                    "kind": "team_member_oauth_refresh",
                    "source_path": "member-source.json",
                    "refreshed_path": "member-oauth.json",
                    "force": False,
                },
            ],
            actions,
        )


if __name__ == "__main__":
    unittest.main()
