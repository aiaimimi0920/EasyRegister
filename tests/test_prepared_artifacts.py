from __future__ import annotations

import base64
import json
import sys
import tempfile
import unittest
from pathlib import Path


SRC_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from others.prepared_artifacts import (  # noqa: E402
    copy_delete_prepared_artifact_to_dir,
    copy_prepared_artifact_to_dir,
    move_prepared_artifact_to_dir,
    prepare_artifact_for_folder,
    prepare_free_artifact,
    prepare_named_artifact,
    prepare_team_artifact,
    write_prepared_artifact,
)


def _jwt_token(payload: dict[str, object]) -> str:
    encoded = base64.urlsafe_b64encode(json.dumps(payload).encode("utf-8")).decode("utf-8").rstrip("=")
    return f"header.{encoded}.signature"


class PreparedArtifactsTests(unittest.TestCase):
    def test_prepare_free_artifact_standardizes_payload_and_name(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "free.json"
            source.write_text(
                json.dumps(
                    {
                        "auth": {
                            "account_id": "org-abcdef12-rest",
                            "https://api.openai.com/auth": {
                                "chatgpt_account_id": "org-abcdef12-rest",
                            },
                        },
                        "id_token": _jwt_token(
                            {
                                "https://api.openai.com/profile": {
                                    "email": "free@example.com",
                                }
                            }
                        ),
                    }
                ),
                encoding="utf-8",
            )

            prepared = prepare_free_artifact(source_path=source)

            self.assertEqual(source.resolve(), prepared.source_path)
            self.assertEqual("codex-free-org-free@example.com.json", prepared.preferred_name)
            self.assertEqual("free@example.com", prepared.payload["email"])

    def test_prepare_team_artifact_uses_team_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "team.json"
            source.write_text(
                json.dumps(
                    {
                        "email": "team@example.com",
                        "https://api.openai.com/auth": {
                            "chatgpt_account_id": "org-abcdef12-rest",
                        },
                    }
                ),
                encoding="utf-8",
            )

            mother = prepare_team_artifact(source_path=source, is_mother=True)
            member = prepare_team_artifact(source_path=source, is_mother=False)

            self.assertEqual("codex-team-mother-org-team@example.com.json", mother.preferred_name)
            self.assertEqual("codex-team-org-team@example.com.json", member.preferred_name)

    def test_prepare_named_artifact_preserves_custom_name(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "source.json"
            source.write_text("{}", encoding="utf-8")

            prepared = prepare_named_artifact(
                source_path=source,
                preferred_name="custom-name.json",
            )

            self.assertEqual("custom-name.json", prepared.preferred_name)

    def test_prepare_artifact_for_folder_switches_by_target_folder(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "source.json"
            source.write_text(
                json.dumps(
                    {
                        "email": "folder@example.com",
                        "https://api.openai.com/auth": {
                            "chatgpt_account_id": "org-abcdef12-rest",
                        },
                    }
                ),
                encoding="utf-8",
            )

            free = prepare_artifact_for_folder(
                source_path=source,
                target_folder="codex",
            )
            team = prepare_artifact_for_folder(
                source_path=source,
                target_folder="codex-team",
                is_mother=False,
            )

            self.assertTrue(free.preferred_name.startswith("codex-free-"))
            self.assertTrue(team.preferred_name.startswith("codex-team-"))

    def test_write_prepared_artifact_rewrites_source_with_standardized_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "source.json"
            source.write_text(
                json.dumps(
                    {
                        "auth": {
                            "account_id": "org-abcdef12-rest",
                        },
                        "id_token": _jwt_token(
                            {
                                "https://api.openai.com/profile": {
                                    "email": "rewrite@example.com",
                                }
                            }
                        ),
                    }
                ),
                encoding="utf-8",
            )

            prepared = prepare_free_artifact(source_path=source)
            write_prepared_artifact(prepared)

            rewritten = json.loads(source.read_text(encoding="utf-8"))
            self.assertEqual("rewrite@example.com", rewritten["email"])
            self.assertEqual("org-abcdef12-rest", rewritten["account_id"])

    def test_copy_prepared_artifact_to_dir_writes_standardized_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "source.json"
            source.write_text(
                json.dumps(
                    {
                        "auth": {
                            "account_id": "org-abcdef12-rest",
                        },
                        "id_token": _jwt_token(
                            {
                                "https://api.openai.com/profile": {
                                    "email": "copy@example.com",
                                }
                            }
                        ),
                    }
                ),
                encoding="utf-8",
            )

            prepared = prepare_free_artifact(source_path=source)
            stored_path = copy_prepared_artifact_to_dir(
                prepared,
                destination_dir=Path(tmp_dir) / "dest",
                overwrite_existing=True,
            )

            self.assertTrue(source.exists())
            stored_payload = json.loads(Path(stored_path).read_text(encoding="utf-8"))
            self.assertEqual("copy@example.com", stored_payload["email"])

    def test_move_prepared_artifact_to_dir_removes_source_and_writes_standardized_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "source.json"
            source.write_text(
                json.dumps(
                    {
                        "auth": {
                            "account_id": "org-abcdef12-rest",
                        },
                        "id_token": _jwt_token(
                            {
                                "https://api.openai.com/profile": {
                                    "email": "move@example.com",
                                }
                            }
                        ),
                    }
                ),
                encoding="utf-8",
            )

            prepared = prepare_free_artifact(source_path=source)
            stored_path = move_prepared_artifact_to_dir(
                prepared,
                destination_dir=Path(tmp_dir) / "dest",
                overwrite_existing=True,
            )

            self.assertFalse(source.exists())
            stored_payload = json.loads(Path(stored_path).read_text(encoding="utf-8"))
            self.assertEqual("move@example.com", stored_payload["email"])

    def test_copy_delete_prepared_artifact_to_dir_uses_payload_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "source.json"
            source.write_text(
                json.dumps(
                    {
                        "auth": {
                            "account_id": "org-abcdef12-rest",
                        },
                        "id_token": _jwt_token(
                            {
                                "https://api.openai.com/profile": {
                                    "email": "delete@example.com",
                                }
                            }
                        ),
                    }
                ),
                encoding="utf-8",
            )

            prepared = prepare_named_artifact(
                source_path=source,
                preferred_name="custom.json",
            )
            stored_path = copy_delete_prepared_artifact_to_dir(
                prepared,
                destination_dir=Path(tmp_dir) / "dest",
                overwrite_existing=True,
            )

            self.assertFalse(source.exists())
            stored_payload = json.loads(Path(stored_path).read_text(encoding="utf-8"))
            self.assertEqual(prepared.payload, stored_payload)


if __name__ == "__main__":
    unittest.main()
