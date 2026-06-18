# Production Account Availability Audit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run exactly one production account-availability audit DST alongside 5 main, 2 continue, and 1 team worker slots, and make it safely inspect existing credential pools.

**Architecture:** Extend the existing account availability audit flow rather than adding a second flow. Selection will support production pools and per-file `nextCheckAt`; finalization will delete only confirmed-deleted accounts across same-email artifacts, update recovery data across same-email artifacts for usable accounts, and keep inconclusive accounts for retry.

**Tech Stack:** Python unittest, EasyRegister semantic DST runtime, JSON credential artifact files, Docker env flow specs.

---

### Task 1: Fixed mixed flow spec

**Files:**
- Modify: `server/services/orchestration_service/src/others/config.py`
- Test: `tests/test_typed_config.py`, `tests/test_runner_modules.py`

- [ ] Write failing tests that standard mixed flow specs include `openai-account-availability-audit` with `instanceRole=account-audit` and `concurrencyLimit=1`, while main remains 5, continue 2, team 1.
- [ ] Implement the standard flow spec addition and default source path wiring.
- [ ] Run the relevant config tests.

### Task 2: Production audit source selection

**Files:**
- Modify: `server/services/orchestration_service/src/others/account_availability_audit.py`
- Test: `tests/test_dst_flow_integration.py` or `tests/test_artifact_pool_modules.py`

- [ ] Write failing tests for scanning `openai/converted`, `openai/failed-twice`, and recursive `codex/**` json files.
- [ ] Write failing tests for skipping files whose `accountAvailabilityAudit.nextCheckAt` is in the future.
- [ ] Implement production source discovery with `maxTargets=1` default for production mode.

### Task 3: Safe finalization

**Files:**
- Modify: `server/services/orchestration_service/src/others/account_availability_audit.py`
- Test: `tests/test_dst_flow_integration.py`

- [ ] Write failing test that `deleted_confirmed` removes same-email files across openai converted, openai failed-twice, and codex recursively.
- [ ] Write failing test that `login_succeeded` updates recoveryDataCredential across same-email files and schedules next check in one day.
- [ ] Write failing test that inconclusive keeps files and schedules next check in 12 hours.
- [ ] Implement JSON updates, deletion, and JSONL audit records with redaction.

### Task 4: Flow JSON and validation

**Files:**
- Modify: `server/services/orchestration_service/flows/openai-account-availability-audit-v1.semantic-flow.json`
- Modify: docs if needed.

- [ ] Pass production-source fields into select/finalize steps.
- [ ] Run targeted tests.
- [ ] Run `python -m unittest discover -s tests -p "test_*.py" -v`.
