from __future__ import annotations

import sys
from pathlib import Path


def ensure_local_bundle_imports() -> Path:
    current_file = Path(__file__).resolve()
    python_protocol_src = current_file.parents[1]
    services_root = current_file.parents[3]
    python_shared_src = services_root / "python_shared" / "src"

    for candidate in (python_shared_src, python_protocol_src):
        candidate_text = str(candidate)
        if candidate.exists() and candidate_text not in sys.path:
            sys.path.append(candidate_text)

    # RegisterService keeps a single runtime source of truth under
    # `orchestration_service/src` and `python_shared/src`. The historical
    # `others/bundle` copies are intentionally not added to `sys.path` here.
    return python_protocol_src


__all__ = ["ensure_local_bundle_imports"]
