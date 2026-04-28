from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class DstStatement:
    step_id: str
    step_type: str
    input: dict[str, Any] = field(default_factory=dict)
    save_as: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DstPlan:
    steps: list[DstStatement]
    platform: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class DstExecutionResult:
    ok: bool
    task_attempts: int = 1
    steps: dict[str, str] = field(default_factory=dict)
    outputs: dict[str, Any] = field(default_factory=dict)
    step_attempts: dict[str, int] = field(default_factory=dict)
    step_errors: dict[str, dict[str, Any]] = field(default_factory=dict)
    error: str = ""
    error_step: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "taskAttempts": int(self.task_attempts or 1),
            "steps": dict(self.steps),
            "outputs": dict(self.outputs),
            "stepAttempts": dict(self.step_attempts),
            "stepErrors": dict(self.step_errors),
            "error": self.error,
            "errorStep": self.error_step,
        }
