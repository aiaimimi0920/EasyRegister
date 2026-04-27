from __future__ import annotations

import errno
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SRC_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from others.artifact_transfer import (  # noqa: E402
    build_unique_destination,
    copy_artifact_to_dir,
    copy_delete_artifact_to_dir,
    move_artifact_to_dir,
)


class ArtifactTransferTests(unittest.TestCase):
    def test_build_unique_destination_appends_suffix_when_needed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            existing = root / "artifact.json"
            existing.write_text("{}", encoding="utf-8")

            destination = build_unique_destination(
                destination_dir=root,
                preferred_name="artifact.json",
            )

            self.assertNotEqual(existing, destination)
            self.assertEqual(root, destination.parent)
            self.assertTrue(destination.name.startswith("artifact-"))
            self.assertEqual(".json", destination.suffix)

    def test_copy_artifact_to_dir_preserves_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            source = root / "source.json"
            source.write_text('{"value": 1}', encoding="utf-8")
            destination_dir = root / "dest"

            copied_path = copy_artifact_to_dir(
                source_path=source,
                destination_dir=destination_dir,
                preferred_name="artifact.json",
            )

            self.assertTrue(source.exists())
            self.assertEqual('{"value": 1}', source.read_text(encoding="utf-8"))
            self.assertEqual('{"value": 1}', Path(copied_path).read_text(encoding="utf-8"))

    def test_copy_artifact_to_dir_overwrites_existing_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            source = root / "source.json"
            source.write_text("fresh", encoding="utf-8")
            destination_dir = root / "dest"
            destination_dir.mkdir()
            existing = destination_dir / "artifact.json"
            existing.write_text("stale", encoding="utf-8")

            copied_path = copy_artifact_to_dir(
                source_path=source,
                destination_dir=destination_dir,
                preferred_name="artifact.json",
                overwrite_existing=True,
            )

            self.assertEqual(existing.resolve(), Path(copied_path).resolve())
            self.assertEqual("fresh", existing.read_text(encoding="utf-8"))

    def test_move_artifact_to_dir_removes_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            source = root / "source.json"
            source.write_text("payload", encoding="utf-8")

            moved_path = move_artifact_to_dir(
                source_path=source,
                destination_dir=root / "dest",
                preferred_name="artifact.json",
            )

            self.assertFalse(source.exists())
            self.assertEqual("payload", Path(moved_path).read_text(encoding="utf-8"))

    def test_move_artifact_to_dir_falls_back_on_cross_device_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            source = root / "source.json"
            source.write_text("payload", encoding="utf-8")

            original_replace = Path.replace

            def _replace(self: Path, target: Path) -> Path:
                if self == source:
                    raise OSError(errno.EXDEV, "Invalid cross-device link")
                return original_replace(self, target)

            with mock.patch.object(Path, "replace", _replace):
                moved_path = move_artifact_to_dir(
                    source_path=source,
                    destination_dir=root / "dest",
                    preferred_name="artifact.json",
                )

            self.assertFalse(source.exists())
            self.assertEqual("payload", Path(moved_path).read_text(encoding="utf-8"))

    def test_copy_delete_artifact_to_dir_removes_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            source = root / "source.json"
            source.write_text("payload", encoding="utf-8")

            moved_path = copy_delete_artifact_to_dir(
                source_path=source,
                destination_dir=root / "dest",
                preferred_name="artifact.json",
            )

            self.assertFalse(source.exists())
            self.assertEqual("payload", Path(moved_path).read_text(encoding="utf-8"))

    def test_copy_delete_artifact_to_dir_supports_payload_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            source = root / "source.json"
            source.write_text("old", encoding="utf-8")

            moved_path = copy_delete_artifact_to_dir(
                source_path=source,
                destination_dir=root / "dest",
                preferred_name="artifact.json",
                payload_override={"ok": True},
            )

            self.assertFalse(source.exists())
            self.assertEqual(
                {"ok": True},
                json.loads(Path(moved_path).read_text(encoding="utf-8")),
            )


if __name__ == "__main__":
    unittest.main()
