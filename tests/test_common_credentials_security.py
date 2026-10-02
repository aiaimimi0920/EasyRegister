from __future__ import annotations

import sys
import unittest
from pathlib import Path, PurePosixPath, PureWindowsPath


SRC_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from others.common_credentials import sanitize_filename_component  # noqa: E402


class CredentialFilenameSecurityTests(unittest.TestCase):
    def test_valid_distinct_filenames_do_not_collapse_to_one_fallback(self) -> None:
        names = [
            sanitize_filename_component(value, fallback="artifact")
            for value in ("artifact..one", "artifact..two")
        ]
        self.assertEqual(2, len(set(names)))
        self.assertNotIn("artifact", names)

    def test_untrusted_components_remain_single_filenames_on_both_platforms(self) -> None:
        for value in ("../outside", "../../outside", "/absolute/path", r"C:\outside\file", "..", "."):
            with self.subTest(value=value):
                name = sanitize_filename_component(value, fallback="artifact")
                self.assertEqual(name, PurePosixPath(name).name)
                self.assertEqual(name, PureWindowsPath(name).name)
                self.assertNotIn(name, ("", ".", ".."))


if __name__ == "__main__":
    unittest.main()
