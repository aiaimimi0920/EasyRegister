#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import boto3


def hash_hex_bytes(data: bytes, algorithm: str) -> str:
    hasher = hashlib.new(algorithm)
    hasher.update(data)
    return hasher.hexdigest()


def write_atomic(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_bytes(data)
    temp_path.replace(path)


def load_bootstrap(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"Bootstrap file not found: {path}")
    return json.loads(path.read_text(encoding="utf-8-sig"))


def build_s3_client(bootstrap: dict[str, Any]) -> Any:
    endpoint = str(bootstrap.get("endpoint") or "").strip()
    account_id = str(bootstrap.get("accountId") or "").strip()
    if not endpoint:
        if not account_id:
            raise SystemExit("Bootstrap file must provide either endpoint or accountId.")
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"

    access_key_id = str(bootstrap.get("accessKeyId") or "").strip()
    secret_access_key = str(bootstrap.get("secretAccessKey") or "").strip()
    if not access_key_id or not secret_access_key:
        raise SystemExit("Bootstrap file must provide accessKeyId and secretAccessKey.")

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="auto",
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
    )


def download_object(client: Any, *, bucket: str, object_key: str) -> bytes:
    buffer = client.get_object(Bucket=bucket, Key=object_key)["Body"].read()
    if not isinstance(buffer, (bytes, bytearray)):
        raise SystemExit(f"Unexpected payload type for {bucket}/{object_key}")
    return bytes(buffer)


def resolve_distribution(client: Any, bootstrap: dict[str, Any]) -> dict[str, Any]:
    bucket = str(bootstrap.get("bucket") or "").strip()
    if not bucket:
        raise SystemExit("Bootstrap file must provide bucket.")

    manifest_object_key = str(bootstrap.get("manifestObjectKey") or "").strip()
    if manifest_object_key:
        manifest_bytes = download_object(client, bucket=bucket, object_key=manifest_object_key)
        manifest = json.loads(manifest_bytes.decode("utf-8"))
        runtime_env_entry = ((manifest.get("runtime") or {}).get("env") or {})
        if not runtime_env_entry.get("objectKey"):
            raise SystemExit(f"Manifest {manifest_object_key} does not contain runtime.env.objectKey.")
        return {
            "bucket": bucket,
            "runtimeEnvObjectKey": str(runtime_env_entry.get("objectKey") or "").strip(),
            "expectedRuntimeEnvSha256": str(runtime_env_entry.get("sha256") or bootstrap.get("expectedRuntimeEnvSha256") or "").strip(),
            "fingerprint": str((manifest.get("runtime") or {}).get("fingerprint") or runtime_env_entry.get("sha256") or "").strip(),
            "manifestObjectKey": manifest_object_key,
            "manifestSha256": hash_hex_bytes(manifest_bytes, "sha256"),
        }

    runtime_env_object_key = str(bootstrap.get("runtimeEnvObjectKey") or "").strip()
    if not runtime_env_object_key:
        raise SystemExit("Bootstrap file must provide manifestObjectKey or runtimeEnvObjectKey.")

    return {
        "bucket": bucket,
        "runtimeEnvObjectKey": runtime_env_object_key,
        "expectedRuntimeEnvSha256": str(bootstrap.get("expectedRuntimeEnvSha256") or "").strip(),
        "fingerprint": runtime_env_object_key,
        "manifestObjectKey": "",
        "manifestSha256": "",
    }


def save_state(path: Path, *, bootstrap: dict[str, Any], distribution: dict[str, Any]) -> None:
    state = {
        "schemaVersion": 1,
        "kind": "easyregister-runtime-distribution-state",
        "distribution": {
            "accountId": str(bootstrap.get("accountId") or "").strip(),
            "endpoint": str(bootstrap.get("endpoint") or "").strip(),
            "bucket": distribution["bucket"],
            "manifestObjectKey": distribution["manifestObjectKey"],
            "manifestSha256": distribution["manifestSha256"],
            "fingerprint": distribution["fingerprint"],
        },
        "artifacts": {
            "runtimeEnvObjectKey": distribution["runtimeEnvObjectKey"],
            "expectedRuntimeEnvSha256": distribution["expectedRuntimeEnvSha256"],
        },
        "sync": {
            "enabled": bool(bootstrap.get("syncEnabled", True)),
            "intervalSeconds": int(bootstrap.get("syncIntervalSeconds") or 7200),
        },
    }
    write_atomic(path, (json.dumps(state, ensure_ascii=False, indent=2) + "\n").encode("utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch EasyRegister runtime env from Cloudflare R2 before deployment.")
    parser.add_argument("--bootstrap-path", required=True)
    parser.add_argument("--runtime-env-path", required=True)
    parser.add_argument("--state-path", default="")
    parser.add_argument("--mode", choices=["initial", "sync"], default="initial")
    args = parser.parse_args()

    bootstrap_path = Path(args.bootstrap_path).resolve()
    runtime_env_path = Path(args.runtime_env_path).resolve()
    state_path = Path(args.state_path).resolve() if args.state_path else runtime_env_path.parent / ".import-state.json"

    bootstrap = load_bootstrap(bootstrap_path)
    client = build_s3_client(bootstrap)
    distribution = resolve_distribution(client, bootstrap)

    runtime_env_bytes = download_object(
        client,
        bucket=distribution["bucket"],
        object_key=distribution["runtimeEnvObjectKey"],
    )
    expected_sha = distribution["expectedRuntimeEnvSha256"]
    if expected_sha and hash_hex_bytes(runtime_env_bytes, "sha256") != expected_sha:
        raise SystemExit(
            f"SHA256 mismatch for {distribution['runtimeEnvObjectKey']}: expected {expected_sha}, got {hash_hex_bytes(runtime_env_bytes, 'sha256')}"
        )

    write_atomic(runtime_env_path, runtime_env_bytes)
    save_state(state_path, bootstrap=bootstrap, distribution=distribution)
    print(f"runtime_env_path={runtime_env_path}")
    print(f"state_path={state_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
