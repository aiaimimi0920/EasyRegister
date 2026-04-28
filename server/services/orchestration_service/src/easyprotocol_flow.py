from __future__ import annotations

import argparse
import json

from others.easyprotocol_runtime import dispatch_easyprotocol_step


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dispatch a medium EasyProtocol business step via EasyProtocol service.")
    parser.add_argument("--step-type", required=True, help="Generic DST step type.")
    parser.add_argument("--input-json", default="{}", help="JSON object passed as step input.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    payload = json.loads(str(args.input_json or "{}"))
    if not isinstance(payload, dict):
        raise RuntimeError("input_json_must_be_object")
    result = dispatch_easyprotocol_step(
        step_type=str(args.step_type or "").strip(),
        step_input=payload,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
