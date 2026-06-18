# OpenAI Community Login Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an isolated DST that claims a small-success account and verifies Community login through EasyBrowser-backed OpenAI web login.

**Architecture:** EasyRegister gets a new flow file and integration tests. EasyProtocol gets a new `login_openai_community` dispatcher branch plus a focused helper/client that calls EasyBrowser `flow_type=login` / `step_type=openai_web_login`. Existing main, continue, team, ChatGPT login, and Codex OAuth flows remain unchanged.

**Tech Stack:** Python 3, `unittest`, EasyRegister DST runtime, EasyProtocol Python provider, EasyBrowser Flow API.

---

### Task 1: EasyRegister DST contract

**Files:**
- Create: `server/services/orchestration_service/flows/openai-community-login-v1.semantic-flow.json`
- Modify: `tests/test_dst_flow_integration.py`
- Modify: `server/services/orchestration_service/src/others/dst_flow_support.py`

- [ ] **Step 1: Write failing EasyRegister integration tests**

Add tests that load the new flow, execute it with mocked dispatchers, and assert the new `login_openai_community` step receives `source_path`, `proxy_url`, `startup_url`, `mailbox_ref`, and `mailbox_session_id`.

- [ ] **Step 2: Run targeted failing tests**

Run: `python -m unittest tests.test_dst_flow_integration.DstFlowIntegrationTests.test_openai_community_login_flow_uses_claimed_small_success_artifact -v`

Expected before implementation: failure because the flow file or new step success handling does not exist.

- [ ] **Step 3: Add new flow and success handling**

Create the new flow file with steps: `claim-small-success-artifact`, `acquire-proxy-chain`, `login-openai-community`, `release-proxy-chain`. Add `login_openai_community` to the generic successful `ok` step family in `dst_flow_support.py`.

- [ ] **Step 4: Run targeted tests**

Run: `python -m unittest tests.test_dst_flow_integration.DstFlowIntegrationTests.test_openai_community_login_flow_uses_claimed_small_success_artifact -v`

Expected: PASS.

### Task 2: EasyProtocol Community login step

**Files:**
- Create: `C:/Users/Public/nas_home/AI/GameEditor/EasyProtocol/providers/python/src/new_protocol_register/protocol_community_login.py`
- Modify: `C:/Users/Public/nas_home/AI/GameEditor/EasyProtocol/providers/python/src/new_protocol_register/easyprotocol_flow.py`
- Modify: `C:/Users/Public/nas_home/AI/GameEditor/EasyProtocol/tests/test_easyprotocol_flow.py`

- [ ] **Step 1: Write failing EasyProtocol tests**

Add tests that patch the helper client, call `dispatch_easyprotocol_step(step_type="login_openai_community", ...)`, and assert it loads a small-success artifact and delegates with `startup_url=https://community.openai.com/`.

- [ ] **Step 2: Run targeted failing tests**

Run: `python -m unittest tests.test_easyprotocol_flow.EasyProtocolFlowTests.test_login_openai_community_dispatch_uses_easybrowser_login_flow -v`

Expected before implementation: failure because the step is unsupported.

- [ ] **Step 3: Implement new helper and dispatcher branch**

Add a focused helper that loads the artifact, normalizes email/password/mailbox fields, posts an EasyBrowser login flow request, waits for completion, and validates a `community.openai.com` target URL. Wire it through a new `login_openai_community` branch.

- [ ] **Step 4: Run targeted tests**

Run: `python -m unittest tests.test_easyprotocol_flow.EasyProtocolFlowTests.test_login_openai_community_dispatch_uses_easybrowser_login_flow tests.test_easyprotocol_flow.EasyProtocolFlowTests.test_login_openai_community_rejects_non_community_target_url -v`

Expected: PASS.

### Task 3: Regression verification

**Files:**
- No new files beyond Tasks 1 and 2.

- [ ] **Step 1: Run EasyRegister targeted suite**

Run: `python -m unittest tests.test_dst_flow_integration -v`

Expected: PASS.

- [ ] **Step 2: Run EasyProtocol targeted suite**

Run: `python -m unittest tests.test_easyprotocol_flow -v`

Expected: PASS.

- [ ] **Step 3: Compile changed Python files**

Run: `python -m py_compile server/services/orchestration_service/src/others/dst_flow_support.py`

Run in EasyProtocol: `python -m py_compile providers/python/src/new_protocol_register/easyprotocol_flow.py providers/python/src/new_protocol_register/protocol_community_login.py`

Expected: no output and exit 0.
