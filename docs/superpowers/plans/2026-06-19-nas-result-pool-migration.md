# NAS Result Pool Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add deployment support for storing only `codex` and `openai` credential result pools on NAS while leaving `others` local.

**Architecture:** Keep `/shared/register-output` as the container contract. Generate a nested compose override that binds the NAS-backed `codex` and `openai` roots over `/shared/register-output/codex` and `/shared/register-output/openai`; leave `/shared/register-output/others` on the local output root. Make account-audit enumerate codex pool directories explicitly so linked or separately mounted result pools are not skipped.

**Tech Stack:** PowerShell deployment scripts, Docker Compose YAML overrides, Python unittest, Python pathlib.

---

### Task 1: Add failing deployment and account-audit tests

**Files:**
- Modify: `tests/test_deploy_host_env.py`
- Modify: `tests/test_dst_flow_integration.py`

- [x] **Step 1: Add deploy-host materialize-only test**

Add `test_materialize_only_generates_result_pool_mounts_for_credential_root` to run `deploy-host.ps1 -MaterializeOnly -CredentialRootHost <temp>\nas-oauth` and assert:

```python
self.assertEqual(str(credential_root), env_values.get("REGISTER_CREDENTIAL_ROOT_HOST"))
self.assertEqual(str(credential_root / "codex"), env_values.get("REGISTER_CODEX_ROOT_DIR_HOST"))
self.assertEqual(str(credential_root / "openai"), env_values.get("REGISTER_OPENAI_ROOT_DIR_HOST"))
self.assertIn('target: "/shared/register-output/codex"', override_payload)
self.assertIn('target: "/shared/register-output/openai"', override_payload)
```

- [x] **Step 2: Add account-audit no-root-rglob test**

Add `test_account_availability_audit_production_selection_does_not_rely_on_codex_root_rglob` and patch `Path.rglob` to raise for the codex root. The test should still select a JSON file under `codex/free`.

- [x] **Step 3: Verify tests fail**

Run:

```powershell
python -m unittest tests.test_deploy_host_env.DeployHostEnvTests.test_materialize_only_generates_result_pool_mounts_for_credential_root tests.test_dst_flow_integration.DstFlowIntegrationTests.test_account_availability_audit_production_selection_does_not_rely_on_codex_root_rglob -v
```

Expected: both tests fail before implementation.

### Task 2: Implement result-pool mount support in deploy-host

**Files:**
- Modify: `deploy-host.ps1`

- [x] **Step 1: Add parameters**

Add these parameters near existing output/team-auth parameters:

```powershell
[string]$CredentialRootHost = "",
[string]$CodexRootDirHost = "",
[string]$OpenaiRootDirHost = "",
[string]$CodexRootDockerSource = "",
[string]$OpenaiRootDockerSource = "",
```

- [x] **Step 2: Add result pool directory and compose override helpers**

Add a helper that creates standard pool directories under the resolved codex/openai roots and writes `.deploy-compose.result-pools.generated.yaml` with nested bind mounts:

```yaml
services:
  easy-register:
    volumes:
      - type: bind
        source: "<codex source>"
        target: "/shared/register-output/codex"
      - type: bind
        source: "<openai source>"
        target: "/shared/register-output/openai"
```

- [x] **Step 3: Resolve root values**

Resolve `CredentialRootHost`, `CodexRootDirHost`, and `OpenaiRootDirHost` after import-code/runtime-env resolution:

```powershell
$resolvedCredentialRootHost = Resolve-EnvValue -ParameterName 'CredentialRootHost' -RuntimeKey 'REGISTER_CREDENTIAL_ROOT_HOST' -Fallback ''
$resolvedCodexRootDirHost = Resolve-EnvValue -ParameterName 'CodexRootDirHost' -RuntimeKey 'REGISTER_CODEX_ROOT_DIR_HOST' -Fallback ''
$resolvedOpenaiRootDirHost = Resolve-EnvValue -ParameterName 'OpenaiRootDirHost' -RuntimeKey 'REGISTER_OPENAI_ROOT_DIR_HOST' -Fallback ''
```

If `CredentialRootHost` is set and either child root is blank, derive:

```powershell
codex -> <CredentialRootHost>\codex
openai -> <CredentialRootHost>\openai
```

- [x] **Step 4: Export env values and include override**

Write the three root env values plus optional docker source env values into `.deploy-compose.env`, generate the result-pool override, and include it in `AdditionalComposeFiles` when present.

### Task 3: Make account-audit codex enumeration explicit

**Files:**
- Modify: `server/services/orchestration_service/src/others/account_availability_audit.py`

- [x] **Step 1: Add codex pool iterator**

Add an iterator that enumerates JSON files under known immediate codex pool directories:

```python
def _production_codex_candidate_paths(root: Path) -> Iterable[Path]:
    pool_names = (
        "free",
        "team",
        "plus",
        "team-input",
        "team-mother-input",
    )
    for name in pool_names:
        pool = root / name
        if not pool.is_dir():
            continue
        yield from pool.glob("*.json")
```

- [x] **Step 2: Use explicit iterator for codex root**

In `_production_candidate_paths`, replace `root.rglob("*.json")` for codex with `_production_codex_candidate_paths(root)`.

### Task 4: Update operator documentation

**Files:**
- Modify: `README.md`
- Modify: `deploy/easyregister.runtime.env.example`

- [x] **Step 1: Document split storage**

Update README deployment notes to state:

```text
codex/openai result pools can be mounted from a NAS credential root, while others remains local.
```

Add example:

```powershell
powershell -ExecutionPolicy Bypass -File ".\deploy-host.ps1" `
  -OutputDirHost "C:\Users\Public\nas_home\AI\GameEditor\SelfDocker\EasyRegister\runtime\register-output" `
  -CredentialRootHost "Z:\oauth"
```

- [x] **Step 2: Add env examples**

Add comments for:

```text
REGISTER_CREDENTIAL_ROOT_HOST
REGISTER_CODEX_ROOT_DIR_HOST
REGISTER_OPENAI_ROOT_DIR_HOST
REGISTER_CODEX_ROOT_DOCKER_SOURCE
REGISTER_OPENAI_ROOT_DOCKER_SOURCE
```

### Task 5: Verify and commit

**Files:**
- Modified files from Tasks 1-4

- [x] **Step 1: Run targeted tests**

```powershell
python -m unittest tests.test_deploy_host_env.DeployHostEnvTests.test_materialize_only_generates_result_pool_mounts_for_credential_root tests.test_dst_flow_integration.DstFlowIntegrationTests.test_account_availability_audit_production_selection_does_not_rely_on_codex_root_rglob -v
```

Expected: PASS.

- [x] **Step 2: Run existing related tests**

```powershell
python -m unittest tests.test_deploy_host_env tests.test_compose_smoke tests.test_dst_flow_integration.DstFlowIntegrationTests.test_account_availability_audit_production_selection_scans_pools_and_skips_future_checks tests.test_dst_flow_integration.DstFlowIntegrationTests.test_account_availability_audit_production_deleted_removes_same_email_files -v
```

Expected: PASS, except Docker-dependent compose config tests may be skipped or fail only if Docker Desktop is unhealthy.

- [x] **Step 3: Review diff**

```powershell
git diff -- deploy-host.ps1 README.md deploy/easyregister.runtime.env.example server/services/orchestration_service/src/others/account_availability_audit.py tests/test_deploy_host_env.py tests/test_dst_flow_integration.py docs/superpowers/specs/2026-06-19-nas-result-pool-design.md docs/superpowers/plans/2026-06-19-nas-result-pool-migration.md
```

- [ ] **Step 4: Commit**

```powershell
git add deploy-host.ps1 README.md deploy/easyregister.runtime.env.example server/services/orchestration_service/src/others/account_availability_audit.py tests/test_deploy_host_env.py tests/test_dst_flow_integration.py docs/superpowers/specs/2026-06-19-nas-result-pool-design.md docs/superpowers/plans/2026-06-19-nas-result-pool-migration.md
git commit -m "feat: split credential result pools onto NAS"
```
