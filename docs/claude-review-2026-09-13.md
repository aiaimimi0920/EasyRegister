# EasyRegister review of recent Claude changes

> Deployment update, 2026-09-13: the reviewed candidate was deployed and the
> generation services restarted at 2026-09-12 18:39:04 UTC. Current deployment,
> browser, and paid-continuation evidence is in the
> [deployment follow-up](pc2-review-deployment-2026-09-13.md).

Review date: 2026-09-13, Asia/Shanghai.
Original conversation: `01a09004-2f9b-7540-8cff-ea1648b24da9`.
Status: local corrections verified; no deployment or paid execution performed.
Full CI and a real large-success artifact remain unverified.

## Findings and corrections

The reviewed code commit is `99211ed`, committed at
2026-09-12 23:15:50 +08:00. Its parent is `5864a2a`. The accompanying
documentation commits are `2bdfe87` and `7c07ce5`; the worktree HEAD remains
`7c07ce5`. The initial worktree already contained 30 modified tracked files and
approximately 4,815 untracked entries. Corrections were made to specific
sections without reverting commits or removing unrelated work.

### 1. High: incomplete phone verification passed final OAuth validation

Location: [artifact_pool_claims.py](../server/services/orchestration_service/src/others/artifact_pool_claims.py),
`validate_free_personal_oauth`, lines 621-700.

Claude changed three branches from `ok: False` to `ok: True`:

- `phone_verification_terminal_small_success`
- `phone_verification_submitted_small_success`
- `phone_verification_attempted_small_success`

These branches run after the free/personal claims check has failed. Returning
true lets incomplete or rejected phone verification satisfy the final
validator, with a risk of incorrect success routing and upload. The three
results now return false again, retaining their status and failure metadata.

The assertion that false validation prevents seed pooling was disproved by the
existing integration test: the seed is collected when small success is created,
and remains pooled while the final flow result is false. The three existing
validator regression tests failed before the correction and passed afterward.
The pooling integration test also passed with the corrected false results.

Evidence:
[validator tests](../tests/test_artifact_pool_modules.py), lines 160-221;
[pooling integration test](../tests/test_dst_flow_integration.py),
`test_run_dst_flow_once_collects_openai_pool_as_soon_as_small_success_is_created`,
line 3092 onward. The corrected validator's semantic AST matches the
pre-Claude version.

The sampled production source at 2026-09-12 16:38:57 UTC already returned false
for all three states. The erroneous validator change was absent from that
production source, so it does not explain the observed production failures.

### 2. High: Dashboard authentication broke browser and monitoring clients

Location: [dashboard_http.py](../server/services/orchestration_service/src/others/dashboard_http.py),
authorization helper and handler, lines 31-44 and 119-149; browser refresh,
line 469 onward.

Claude required Bearer authentication for `/api/status`, but the existing
browser fetch and monitoring scripts sent no credentials. The page parsed the
401 JSON as status data and could display empty metrics.

The correction protects `/`, `/index.html`, and `/api/status`. API callers can
use Bearer authentication. Browsers can use HTTP Basic with username
`dashboard` and the existing `EASY_PROTOCOL_CONTROL_TOKEN` as password. Token
comparison uses `hmac.compare_digest`; an empty configured token cannot
authorize an empty Bearer header. Unauthorized responses include a Basic
challenge, and protected responses use `Cache-Control: no-store`. No credential
is embedded in HTML. The page checks `response.ok` and displays fetch errors.

The existing integration clients and the embedded remote programs in
[audit-pc2-restart-results.py](../scripts/audit-pc2-restart-results.py) and
[promote-pc2-auth-boundary.py](../scripts/promote-pc2-auth-boundary.py) now obtain
the running container's shared control token in memory and send the header
without printing the credential. Changing the promotion script did not execute
a promotion.

HTTP Basic and Bearer do not encrypt transport. Remote access should retain
the deployment's trusted transport, TLS termination, or SSH tunnel. The shared
control token must remain aligned with EasyProtocol and its callers.

### 3. Medium: private-IP rejection blocked normal internal statistics

Location: `dashboard_http.py`, `_fetch_easy_protocol_stats`, lines 275-301.

The EasyProtocol base URL is operator-configured, and valid deployments use
private or loopback addresses. Claude's filter blocked these deployments while
still allowing DNS names, including names that can resolve to private IPs.
No request-controlled destination was found in this Dashboard path.

The correction permits configured internal HTTP/HTTPS services, validates URL
structure, rejects userinfo/query/fragment components, and uses
`_NoRedirectHandler` so the control credential is never forwarded through a
redirect. Only a JSON object is accepted as statistics. Real local HTTP tests
verify internal statistics retrieval and that a redirect target is not called.

### 4. Medium: the added JWT verification API did not verify JWTs

Location: [common_credentials.py](../server/services/orchestration_service/src/others/common_credentials.py),
`decode_jwt_payload`, line 32 onward.

The new `verify=True` branch simply returned an empty dictionary; it did not
validate a signature. No caller used the added parameter. The unused parameter
and branch were removed, and the docstring now explicitly describes unverified
metadata extraction. Existing behavior was preserved. This review did not add
JWKS/signature/issuer/audience/expiry authentication. Artifact claims checks
remain semantic checks and must not be presented as cryptographic verification.

### 5. Medium: filename sanitization collapsed distinct valid names

Location: `common_credentials.py`, `sanitize_filename_component`, lines 10-19.

Claude rejected every name containing `..`. Distinct names such as
`artifact..one` and `artifact..two` therefore collapsed to the same fallback,
creating a collision risk. Path separators were already flattened, and leading
or trailing dots were already stripped.

The original normalization was restored. A regression reproduced the collision
before correction; tests now verify that valid names remain distinct and that
the result remains a single filename component on Windows and POSIX.

The minimum 16-character control-token rule and process loopback fallback were
retained. Existing NAS mailbox/proxy work was preserved. The recent source
scan found no EasyProtocol or EasySMS source modifications in the inspected
window, and this review made no edits to those sibling projects.

## What the live small-success cohort actually shows

The read-only PC2 audit command was:

```text
rtk proxy python scripts/audit-pc2-restart-results.py --since 2026-09-12T14:30:00Z
```

The snapshot was captured at 2026-09-12 16:30:06 UTC
(2026-09-13 00:30:06 +08:00). These are timestamped observations, not a claim
about later scheduler activity.

| Observation | Count |
| --- | ---: |
| Completed production tasks in the window | 34 |
| Genuine small successes | 8 |
| Large successes | 0 |
| Other tasks with browser-verification failures | 20 |
| Other tasks with proxy failures | 6 |
| All pool files | 218 |
| Historical artifacts valid by milestone/structure | 109 |
| Artifacts valid under the current-age check at the snapshot | 0 |
| New files in the window | 16 |
| New valid small-success seeds | 8 |
| New incomplete records excluded from success count | 8 |

All eight genuine small successes completed account creation, Platform
organization initialization, and ChatGPT login initialization. Their later
failures split as follows:

| Tasks | Observed failure | Interpretation |
| --- | --- | --- |
| 393, 397, 399, 401, 404 | `sms_not_enabled_for_business` | Reached the local SMS acquisition gate; effective business policy disabled SMS. |
| 394, 396, 406 | OAuth-step timeout | The inspected messages were 59 characters long and stage/detail were empty; the internal timeout boundary is unresolved. |

Five of eight recent genuine small successes, or 62.5%, therefore reached a
disabled SMS gate. That is evidence supporting the user's expectation that
SMS is the next practical boundary for many valid small successes. These five
tasks did not test paid-provider conversion, so this cohort provides no
measured paid SMS success rate.

[runtime_sms.py](../server/services/orchestration_service/src/others/runtime_sms.py),
lines 936-941, raises this exact error when `policy.enabled` is false.
[config_runtime_sections.py](../server/services/orchestration_service/src/others/config_runtime_sections.py),
lines 999-1012, defaults to `enabled=False` if neither a matching business policy
nor a default policy is found. Production also had
`REGISTER_SMS_ALLOW_PAID=false`. These are separate controls: enabling global
paid routing alone does not enable a disabled business policy.

The inspected environment JSON did not contain the direct `openai` policy
fields. Absence of those raw fields was not treated as an independently
evaluated effective policy; the five runtime errors establish the disabled
effective policy for those tasks.

## Why small success still needs an end-to-end continuation check

The current continuation contract includes:

1. A structurally valid seed within the normal 900-second lifetime.
2. Platform and ChatGPT session initialization, plus configured conditional
   refresh/invite steps in the continuation flow.
3. Codex OAuth reaching an actual phone-verification requirement.
4. A valid SMS business policy, routing, price/country settings, and an available
   EasySMS session under the original bounded purchase guard.
5. Number/code submission and completion of OAuth.
6. `ok: True`, `status=completed`, a nonempty success artifact path, final
   free/personal claims validation, identity-matching artifact evidence, and
   reconciliation of any SMS order and charge.

Source references:
[seed validation](../server/services/orchestration_service/src/others/common_runtime.py),
lines 87-167;
[continuation flow](../server/services/orchestration_service/flows/codex-openai-oauth-continue-v1.semantic-flow.json);
[phone OAuth completion](../server/services/orchestration_service/src/others/easyprotocol_runtime.py),
lines 884-920 and 1048-1072.

The prior isolated paid-enabled continuations described in
[the repair trace](pc2-paid-sms-repair-2026-09-12.md) failed before buying a
number, including ChatGPT login initialization failures. Those historical
attempts and the recent production cohort have different execution conditions.
Their failures should not be combined into a supposed paid-SMS conversion
failure rate. The three current OAuth timeouts also cannot be assigned the
older ChatGPT-bootstrap cause from the available messages.

No current-age-valid pool entry remained at the 16:30 snapshot. A subsequent
bounded continuation needs a new valid seed; editing a timestamp or reusing an
expired seed would invalidate the acceptance evidence.

## Production and purchase guard observations

At 2026-09-12 16:38:57 UTC, the production validator's three partial phone
states all returned false. Its source SHA-256 was:

```text
8d9c8b3261b9fa7e140346f853045d2e190a7a4371325cb627c9fcb2d5ab940c
```

The sampled production images were:

```text
orchestrator: sha256:a2da348213fe60d102c157ad290455527b94deb3928025e29606ffe8a793f20c
python provider: sha256:4297f6a2fe88c80ce3f5f4ef984bad0550a255c087d53b65699529f44e84b0ed
```

The audited Dashboard request returned HTTP 200. Production container restart
counts remained zero in the audit. The read-only guard check found:

- Root: `/home/mjc/easyregister/paid-sms-canary/20260912-001`.
- Paid continuations `001-verified`, `002`, `003`, and `004`: exited.
- `easyregister-paid-once-20260912-005`: created, never started.
- `guard-state/attempt.json`: absent.
- `guard-state/attempt.json.receipt.json`: absent.

The guard still permits at most one purchase attempt, service `dr`, country
`16`, with a 0.05 USD cap. It must not be reset, replaced, or deleted. Exited
one-shot containers must not be restarted. The created `005` container is not
proof that its seed and configuration remain suitable for a future run.

This review made no paid API calls, purchases, account-registration submissions,
deployment changes, container restarts, Git commits, or pushes. Provider
balance and quote values from earlier reports were not refreshed and are not
presented as current evidence.

## Verification results

The distinct module runs produced the following results:

| Module or pattern | Passed | Failed | Errors |
| --- | ---: | ---: | ---: |
| `test_artifact_pool_modules.py` | 44 | 0 | 0 |
| `test_dst_flow_integration.py` | 52 | 12 | 2 |
| `test_dashboard*.py` | 9 | 0 | 0 |
| `test_common_credentials_security.py` | 2 | 0 | 0 |
| `test_security_defaults.py` | 8 | 0 | 0 |
| `test_pc2_promotion_resume.py` | 2 | 0 | 0 |
| Total | 117 | 12 | 2 |

All 14 failures/errors are in the account-availability audit group, which was
already recorded as failing in
[the pre-review repair trace](pc2-paid-sms-repair-2026-09-12.md), lines 53-58.
All 27 account-availability test methods have unchanged ASTs relative to
`99211ed^`, and no account-availability-named file differed against that
baseline. The corrected validator and credential helpers also match the
pre-Claude semantic baseline. No clean-baseline full-suite rerun was performed;
these observations do not establish that every historical failure was
identical. Unrelated expectations were not weakened to obtain a green result.

The full CI command, `python -m unittest discover -s tests -v`, was attempted
and timed out after 240 seconds, producing approximately 57 MB of output. There
was no completed overall result, and the last test identity was not retained
by the output-processing script. A subsequent process check found no matching
unittest process. The full suite was not blindly rerun.

Real-browser verification used headless Chromium 131.0.6778.33 against an
isolated local server: Basic-authenticated navigation returned HTTP 200,
anonymous navigation returned HTTP 401, and the actual page fetch rendered
four metric cards with fixture pool size 7. There were no page errors and no
token in the HTML. The browser, contexts, and local server were closed. This
was a local runtime check; it did not deploy the updated Dashboard to PC2.

Eight modified/new Python files compiled, as did the embedded `ARTIFACTS`,
`OBSERVE`, and `REMOTE` programs. Strict UTF-8 without BOM and the scoped source
diff check passed. Git emitted LF-to-CRLF advisory warnings only. No official
formatter was configured in `pyproject.toml` or CI. Two new test files contain
six Dashboard authentication/transport tests and two credential-helper tests.
The source hashes in the accompanying evidence file pin these results to the
reviewed contents; subsequent documentation edits did not change those files.

## Documentation corrections and remaining work

The following documents were corrected in place:

- [FIX-SUMMARY-2026-09-12.md](FIX-SUMMARY-2026-09-12.md)
- [DEPLOYMENT-2026-09-12.md](DEPLOYMENT-2026-09-12.md)
- [security-audit-2026-09-12-preliminary.md](security-audit-2026-09-12-preliminary.md)

They now distinguish historical selected-test claims from current results,
remove the fake JWT-verification claim and incorrect pooling explanation, use
the real shared `EASY_PROTOCOL_CONTROL_TOKEN`, and name the actual Compose
service `easy-register`. The deployment guide accounts for container listen
address `0.0.0.0:9790`, explicit remote opt-in, and default host port `19790`.
Blanket stack shutdown, unilateral token rotation, whole-commit rollback, and
an unsupported blanket authorization claim were removed.

To finish the original large-success objective, prepare a reviewed isolated
candidate, acquire a fresh valid seed, and verify the effective SMS policy
within the original one-purchase guard. Keep global production paid routing
disabled. Then record the actual phone submission, code receipt/submission,
completed OAuth, final claims and matching artifact, and order/cost outcome.
Diagnose a failed execution before another run, without renewing its spending
allowance. The three observed OAuth timeouts need more precise boundary
evidence if they recur. A complete repository CI result also remains pending.

Sanitized machine-readable observations and source hashes:
[claude-review-20260913.json](../deploy-evidence/claude-review-20260913.json).
