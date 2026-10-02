"""Copy one newly valid seed into the protected canary; never call SMS APIs."""

from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from others.common_runtime import validate_openai_oauth_seed_payload


ROOT = Path("/shared/register-output/paid-sms-canary-20260912-001")


def write_exclusive(path: Path, content: bytes) -> None:
    with path.open("xb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())


def main() -> int:
    os.umask(0o077)
    if list((ROOT / "seed-pool").glob("*.json")) or (ROOT / "run.started.json").exists():
        raise RuntimeError("canary_seed_or_run_already_exists")
    deadline = time.monotonic() + 1800
    while time.monotonic() < deadline:
        sources = [("production_pool", Path("/shared/register-output/openai/pending"))]
        for candidate in sorted(ROOT.glob("seed-refresh-*"), reverse=True):
            if (candidate / "summary.json").exists():
                sources.insert(0, (candidate.name.replace("-", "_"), candidate))
        for source_kind, source_root in sources:
            for path in sorted(source_root.rglob("*.json"), key=lambda item: item.stat().st_mtime, reverse=True):
                try:
                    raw = path.read_bytes()
                    payload = json.loads(raw)
                    valid, _ = validate_openai_oauth_seed_payload(payload)
                    if not valid:
                        continue
                    created_at = datetime.fromisoformat(str(payload.get("createdAt", "")).replace("Z", "+00:00"))
                    age_seconds = (datetime.now(timezone.utc) - created_at).total_seconds()
                except (OSError, ValueError, TypeError):
                    continue
                if age_seconds > 300:
                    continue
                write_exclusive(ROOT / "private-seed-backup" / "seed-original.json", raw)
                write_exclusive(ROOT / "seed-pool" / "seed.json", raw)
                proof = {
                    "ready": True, "sourceKind": source_kind,
                    "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw),
                    "createdAt": payload["createdAt"], "normalAgeValid": True,
                    "captureAgeSeconds": age_seconds,
                    "copiedAt": datetime.now(timezone.utc).isoformat(),
                }
                write_exclusive(ROOT / "seed-ready.json", json.dumps(proof, indent=2).encode("utf-8"))
                print(json.dumps(proof), flush=True)
                return 0
        time.sleep(10)
    print(json.dumps({"ready": False, "reason": "no_valid_seed_within_watch_window"}), flush=True)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
