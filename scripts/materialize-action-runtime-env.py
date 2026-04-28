from __future__ import annotations

import argparse
import base64
from pathlib import Path


PREFIX = "EASYREGISTER_ENV_"
RUNTIME_ENV_B64 = "EASYREGISTER_RUNTIME_ENV_B64"


def _parse_env_file(path: Path) -> tuple[list[str], dict[str, str]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    values: dict[str, str] = {}
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value
    return lines, values


def _load_secret_env(os_env: dict[str, str]) -> dict[str, str]:
    overrides: dict[str, str] = {}
    for key, value in os_env.items():
        if not key.startswith(PREFIX):
            continue
        runtime_key = key[len(PREFIX) :].strip()
        if not runtime_key:
            continue
        overrides[runtime_key] = str(value or "")
    return overrides


def _render_lines(base_lines: list[str], merged: dict[str, str]) -> str:
    rendered: list[str] = []
    seen: set[str] = set()
    for line in base_lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            rendered.append(line)
            continue
        key, _value = line.split("=", 1)
        normalized_key = key.strip()
        if normalized_key in merged:
            rendered.append(f"{normalized_key}={merged[normalized_key]}")
            seen.add(normalized_key)
        else:
            rendered.append(line)
            seen.add(normalized_key)
    for key in sorted(merged):
        if key in seen:
            continue
        rendered.append(f"{key}={merged[key]}")
    return "\n".join(rendered).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Materialize an EasyRegister runtime env file for GitHub Actions.")
    parser.add_argument("--base-env", required=True, help="Path to the committed .env example file.")
    parser.add_argument("--output", required=True, help="Path to write the materialized env file.")
    args = parser.parse_args()

    base_env_path = Path(args.base_env).resolve()
    output_path = Path(args.output).resolve()

    runtime_env_b64 = __import__("os").environ.get(RUNTIME_ENV_B64, "").strip()
    if runtime_env_b64:
        decoded = base64.b64decode(runtime_env_b64).decode("utf-8")
        output_path.write_text(decoded, encoding="utf-8")
        print(f"materialized_from={RUNTIME_ENV_B64}")
        print(f"output_path={output_path}")
        return 0

    base_lines, base_values = _parse_env_file(base_env_path)
    overrides = _load_secret_env(__import__("os").environ)
    merged = {**base_values, **overrides}
    output_path.write_text(_render_lines(base_lines, merged), encoding="utf-8")
    print(f"materialized_from={base_env_path}")
    print(f"override_count={len(overrides)}")
    print(f"output_path={output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
