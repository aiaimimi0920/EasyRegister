from __future__ import annotations

import json
from pathlib import Path

from others.dst_flow_models import DstPlan, DstStatement
from others.dst_flow_support import OWNER_DISPATCHERS


DEFAULT_DST_FLOW_PATH = (
    Path(__file__).resolve().parents[2]
    / "flows"
    / "codex-openai-account-v1.semantic-flow.json"
)


def load_dst_flow(path: str | Path | None = None) -> DstPlan:
    resolved_path = Path(path or DEFAULT_DST_FLOW_PATH).resolve()
    payload = json.loads(resolved_path.read_text(encoding="utf-8"))
    definition = payload.get("definition") if isinstance(payload.get("definition"), dict) else payload
    steps = definition.get("steps")
    if not isinstance(steps, list) or not steps:
        raise RuntimeError(f"dst flow missing steps: {resolved_path}")
    result_steps: list[DstStatement] = []
    for index, raw_step in enumerate(steps, start=1):
        if not isinstance(raw_step, dict):
            raise RuntimeError(f"dst flow step #{index} is not an object")
        step_id = str(raw_step.get("id") or f"step-{index}").strip() or f"step-{index}"
        step_type = str(raw_step.get("type") or "").strip()
        if not step_type:
            raise RuntimeError(f"dst flow step {step_id} missing type")
        metadata = raw_step.get("metadata") if isinstance(raw_step.get("metadata"), dict) else {}
        owner = str(metadata.get("owner") or "").strip().lower()
        if not owner:
            raise RuntimeError(f"dst flow step {step_id} missing metadata.owner")
        if owner not in OWNER_DISPATCHERS:
            raise RuntimeError(f"dst flow step {step_id} unsupported owner: {owner}")
        result_steps.append(
            DstStatement(
                step_id=step_id,
                step_type=step_type,
                input=raw_step.get("input") if isinstance(raw_step.get("input"), dict) else {},
                save_as=str(raw_step.get("saveAs") or raw_step.get("save_as") or "").strip() or None,
                metadata=metadata,
            )
        )
    return DstPlan(
        steps=result_steps,
        platform=str(definition.get("platform") or "").strip(),
        metadata=definition.get("metadata") if isinstance(definition.get("metadata"), dict) else {},
    )
