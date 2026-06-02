# Mailbox User Register 400 Governance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `user_register_400` retries avoid the mailbox/provider/domain risk surface that just failed, while keeping global blacklist behavior conservative.

**Architecture:** DST retry builds task-local mailbox avoidance from the failed `create-openai-account` context and passes it into refreshed `acquire-mailbox`. Runtime mailbox selection rejects only the current attempt's avoided email/domain/provider and releases rejected mailboxes best-effort. Runner mailbox governance records retry-time mailbox failures from `mailbox-attempt-outcomes` before the final outcome.

**Tech Stack:** Python 3, `unittest`, existing EasyRegister DST flow/runtime modules.

---

### Task 1: DST retry attempt-local hints

**Files:**
- Modify: `tests/test_dst_flow_integration.py`
- Modify: `server/services/orchestration_service/src/others/dst_flow_runtime.py`
- Modify: `server/services/orchestration_service/flows/codex-openai-account-v1.semantic-flow.json`

- [x] Write/extend a failing integration test proving refreshed `acquire-mailbox` receives `avoid_emails`, `avoid_domains`, `avoid_providers`, and `avoid_reason` after `create-openai-account` returns `user_register_400` with mailbox context.
- [x] Run the targeted test and confirm it fails because the hints are absent.
- [x] Implement minimal DST retry context extraction and `mailbox-attempt-outcomes` output.
- [x] Add avoid placeholders to the main account flow's `acquire-mailbox` input.
- [x] Run the targeted test and confirm it passes.

### Task 2: Runtime mailbox attempt-local avoidance

**Files:**
- Modify: `tests/test_adapter_runtimes.py`
- Modify: `server/services/orchestration_service/src/others/easyemail_runtime.py`
- Modify: `server/services/orchestration_service/src/others/runtime_mailbox.py`

- [x] Write failing runtime tests proving `resolve_mailbox()` rejects attempt-local avoided email/domain/provider, releases rejected mailboxes, and does not write global state.
- [x] Run the targeted tests and confirm they fail because runtime does not accept/enforce avoid hints.
- [x] Implement optional avoid hint parameters in `easyemail_runtime.dispatch_easyemail_step()` and `runtime_mailbox.resolve_mailbox()`.
- [x] Run targeted runtime tests and confirm they pass.

### Task 3: Runner records retry-attempt mailbox outcomes

**Files:**
- Modify: `tests/test_runner_modules.py`
- Modify: `server/services/orchestration_service/src/others/runner_mailbox.py`

- [x] Write failing tests proving `mailbox-attempt-outcomes` are recorded before final success, with `create_account_user_register_400` not immediately blacklisting and `unsupported_email` still immediately blacklisting.
- [x] Run the targeted tests and confirm they fail because per-attempt outcomes are ignored.
- [x] Implement per-attempt outcome recording by reusing the existing business mailbox outcome writer.
- [x] Run targeted runner tests and confirm they pass.

### Task 4: Full regression

**Files:**
- No additional files expected.

- [x] Run `python -m unittest discover -s tests -p "test_*.py" -v`.
- [x] If failures are related to this work, fix them with another red/green loop.
- [x] Report exact verification output and remaining risks.

## Completion evidence

- Targeted governance suites: `python -m unittest tests.test_dst_flow_integration tests.test_adapter_runtimes tests.test_runner_modules -v` -> `Ran 183 tests ... OK`.
- Recovery helper suite: `python -m unittest tests.test_mailbox_state_recovery -v` -> `Ran 3 tests ... OK`.
- Full EasyRegister regression after compose fix: `python -m unittest discover -s tests -p "test_*.py" -v` -> `Ran 335 tests ... OK`.
- Compose rendering: `docker compose -f compose/docker-compose.yaml config > $null` and `docker compose -f compose/docker-compose.test.yaml config > $null` both exited 0.
- Cross-repo EasyProtocol phone-wall regression: `python -m unittest tests.test_easyprotocol_flow -v` in `C:\Users\Public\nas_home\AI\GameEditor\EasyProtocol` -> `Ran 35 tests ... OK`.
