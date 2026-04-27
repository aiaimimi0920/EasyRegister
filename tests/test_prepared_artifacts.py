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
    delete_artifact_quiet,
    merge_route_result,
    move_prepared_artifact_to_dir,
    prepare_artifact_for_folder,
    prepare_free_artifact,
    prepare_named_artifact,
    prepare_team_artifact,
    route_prepared_artifact,
    stage_prepared_artifact_for_upload,
    store_local_prepared_artifact,
    summarize_route_collections,
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

    def test_store_local_prepared_artifact_supports_copy_and_move_modes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            copy_source = root / "copy.json"
            move_source = root / "move.json"
            copy_source.write_text(json.dumps({"email": "copy@example.com"}), encoding="utf-8")
            move_source.write_text(json.dumps({"email": "move@example.com"}), encoding="utf-8")

            copied = store_local_prepared_artifact(
                prepare_named_artifact(source_path=copy_source, preferred_name="copy-stored.json"),
                destination_dir=root / "dest",
                overwrite_existing=True,
                move=False,
            )
            moved = store_local_prepared_artifact(
                prepare_named_artifact(source_path=move_source, preferred_name="move-stored.json"),
                destination_dir=root / "dest",
                overwrite_existing=True,
                move=True,
            )

            self.assertTrue(copy_source.exists())
            self.assertFalse(move_source.exists())
            self.assertEqual("copy-stored.json", Path(copied).name)
            self.assertEqual("move-stored.json", Path(moved).name)

    def test_stage_prepared_artifact_for_upload_supports_copy_and_in_place_modes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            staged_source = root / "stage.json"
            direct_source = root / "direct.json"
            staged_source.write_text(json.dumps({"email": "stage@example.com"}), encoding="utf-8")
            direct_source.write_text(json.dumps({"email": "direct@example.com"}), encoding="utf-8")

            staged = stage_prepared_artifact_for_upload(
                prepare_named_artifact(source_path=staged_source, preferred_name="staged.json"),
                staging_dir=root / "pool",
                overwrite_existing=True,
            )
            direct = stage_prepared_artifact_for_upload(
                prepare_named_artifact(source_path=direct_source, preferred_name="direct.json"),
            )

            self.assertEqual("staged.json", staged.name)
            self.assertTrue(staged.is_file())
            self.assertEqual(direct_source.resolve(), direct)
            self.assertTrue(direct.is_file())

    def test_delete_artifact_quiet_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            target = Path(tmp_dir) / "artifact.json"
            target.write_text("payload", encoding="utf-8")

            delete_artifact_quiet(target)
            delete_artifact_quiet(target)

            self.assertFalse(target.exists())

    def test_route_prepared_artifact_local_mode_returns_stored_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            source = root / "local.json"
            source.write_text(json.dumps({"email": "local@example.com"}), encoding="utf-8")

            result = route_prepared_artifact(
                prepare_named_artifact(source_path=source, preferred_name="stored.json"),
                local_dir=root / "dest",
                move_local=False,
                overwrite_existing=True,
                target_folder="codex",
                upload_fn=lambda **_: {"ok": True},
            )

            self.assertTrue(result["ok"])
            self.assertEqual("local", result["route"])
            self.assertEqual("stored.json", Path(result["stored_path"]).name)
            self.assertTrue(source.exists())

    def test_route_prepared_artifact_upload_success_deletes_staged_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            source = root / "upload.json"
            source.write_text(json.dumps({"email": "upload@example.com"}), encoding="utf-8")

            captured: dict[str, object] = {}

            def _upload_fn(*, source_path: Path, target_folder: str, object_name: str) -> dict[str, object]:
                captured["source_path"] = source_path
                captured["target_folder"] = target_folder
                captured["object_name"] = object_name
                self.assertTrue(source_path.exists())
                return {"ok": True, "object_key": f"{target_folder}/{object_name}"}

            result = route_prepared_artifact(
                prepare_named_artifact(source_path=source, preferred_name="uploaded.json"),
                local_dir=None,
                move_local=False,
                overwrite_existing=True,
                target_folder="codex",
                upload_fn=_upload_fn,
                staging_dir=root / "pool",
            )

            self.assertTrue(result["ok"])
            self.assertEqual("uploaded", result["route"])
            self.assertEqual("codex/uploaded.json", result["object_key"])
            staged_path = Path(str(result["staged_path"]))
            self.assertFalse(staged_path.exists())
            self.assertEqual("codex", captured["target_folder"])
            self.assertEqual("uploaded.json", captured["object_name"])

    def test_route_prepared_artifact_upload_failure_keeps_staged_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            source = root / "upload-fail.json"
            source.write_text(json.dumps({"email": "fail@example.com"}), encoding="utf-8")

            result = route_prepared_artifact(
                prepare_named_artifact(source_path=source, preferred_name="failed.json"),
                local_dir=None,
                move_local=False,
                overwrite_existing=True,
                target_folder="codex",
                upload_fn=lambda **_: {"ok": False, "detail": "upload_failed"},
                staging_dir=root / "pool",
            )

            self.assertFalse(result["ok"])
            self.assertEqual("upload_failed", result["route"])
            self.assertEqual("upload_failed", result["detail"])
            self.assertTrue(Path(str(result["staged_path"])).exists())

    def test_merge_route_result_can_skip_route_field(self) -> None:
        merged = merge_route_result(
            {"path": "artifact.json"},
            {
                "route": "local",
                "stored_path": "stored.json",
                "detail": "ignored",
            },
            include_route=False,
            include_object_key=False,
            include_detail=False,
        )
        self.assertEqual(
            {
                "path": "artifact.json",
                "stored_path": "stored.json",
            },
            merged,
        )

    def test_summarize_route_collections_handles_processed_partial_failed_and_idle(self) -> None:
        self.assertEqual(
            {
                "ok": True,
                "status": "processed",
                "artifacts": [{"id": 1}],
                "failures": [],
            },
            summarize_route_collections(
                failures=[],
                artifacts=[{"id": 1}],
            ),
        )
        self.assertEqual(
            {
                "ok": False,
                "status": "partial_failure",
                "artifacts": [{"id": 1}],
                "failures": [{"id": 2}],
            },
            summarize_route_collections(
                failures=[{"id": 2}],
                artifacts=[{"id": 1}],
            ),
        )
        self.assertEqual(
            {
                "ok": False,
                "status": "failed",
                "artifacts": [],
                "failures": [{"id": 2}],
            },
            summarize_route_collections(
                failures=[{"id": 2}],
                artifacts=[],
            ),
        )
        self.assertEqual(
            {
                "ok": True,
                "status": "idle",
                "artifacts": [],
                "failures": [],
            },
            summarize_route_collections(
                failures=[],
                artifacts=[],
            ),
        )


if __name__ == "__main__":
    unittest.main()
