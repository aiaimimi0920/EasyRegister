# PC2 reviewed deployment and bounded SMS verification

> Follow-up at 2026-09-12 20:12:58 UTC: the login request and explicit browser
> challenge are identified in the [investigation report](pc2-login-challenge-investigation-2026-09-13.md).
> Registration remains blocked; that follow-up did not deploy another image.

Date: 2026-09-13, Asia/Shanghai.
Deployment completed at 2026-09-12 18:39:04 UTC (02:39:04 +08:00).
Production restart and the deployed Dashboard are verified. The bounded
observation produced no valid small-success seed, so no paid continuation was
started. Task SMS spend is 0 USD; a real large success remains unverified.

This report supersedes the deployment status in the earlier
[Claude review](claude-review-2026-09-13.md) and
[deployment guide](DEPLOYMENT-2026-09-12.md). Their earlier observations remain
historical evidence.

## Deployed changes and verification

The candidate contains exactly four reviewed orchestrator files:

- `others/artifact_pool_claims.py`
- `others/dashboard_http.py`
- `others/common_credentials.py`
- `dashboard_server.py`

The three incomplete phone-verification results remain `ok: False`; seed
pooling still happens before final validation. Dashboard browser/API
authentication, configured internal statistics retrieval, filename handling,
and the misleading JWT metadata API are corrected. The production validator
already rejected incomplete phone verification before this deployment, so
that local Claude regression did not cause the previously observed live
signup failures. The [review](claude-review-2026-09-13.md) explains each finding.

The candidate was built on the live image, copying only those four files.
Eighty-three Linux candidate tests passed with no failures, errors, or skips.
The first test-container run had one read-only working-directory error; the
same image passed after using writable `/tmp` as the test working directory.
The failed harness evidence is retained. Three promotion regression tests
passed, including provider restart ordering and resuming after rollback.

Deployed image:

```text
tag: easy-register/easy-register:pc2-auth-boundary-20260913-001
image: sha256:72c243dc42297e22b7d2107cb0e87e2c1ce7e09338d37db17186073de3af0b4e
base: sha256:a2da348213fe60d102c157ad290455527b94deb3928025e29606ffe8a793f20c
```

The deployment drained to zero active tasks and zero busy provider workers.
The orchestrator was stopped, the existing production Python provider was
restarted and health-checked, and the new orchestrator was then started.
All four deployed file hashes matched the candidate. Environment and mounts
were preserved. The production protocol gateway and SMS service were unchanged.
No rollback was needed.

```text
easy-register:
  container: 998f545f6a62dac6b5cb62cb87edee2939cdd20f05bdc905ef65644333f3d633
  started: 2026-09-12T18:38:58.580421244Z

easy-register-protocol-python:
  container: a02766f671b0840cc6e5b040041ee91e57d3578d559082a136459ec6ab120eba
  image: sha256:4297f6a2fe88c80ce3f5f4ef984bad0550a255c087d53b65699529f44e84b0ed
  started: 2026-09-12T18:38:50.873565595Z
```

Retained PC2 Compose overrides:

```text
/home/mjc/easyregister/auth-boundary-register-20260913-001.yaml
/home/mjc/easyregister/auth-boundary-register-rollback-20260913-001.yaml
```

Evidence:
[candidate gate](../deploy-evidence/pc2-review-candidates-20260913-001-verified.json),
[deployment](../deploy-evidence/pc2-review-deployment-20260913-001.json), and
[first harness run](../deploy-evidence/pc2-review-candidates-20260913-001.json).

## Actual deployed Dashboard

The live check used an ephemeral SSH tunnel to PC2 port 19790. The running
image was checked before navigation; credentials remained in process memory.
For `/`, `/index.html`, and `/api/status`, anonymous requests returned 401
with a Basic challenge, and both Basic and Bearer requests returned 200.
All nine responses used `Cache-Control: no-store` and contained no control
credential in their bodies.

Real headless Chromium navigated to the deployed page, fetched the authenticated
status endpoint, and rendered four metric cards matching the actual response.
The next periodic refresh also returned 200. One executor row was displayed;
there were no page errors or embedded credentials. The screenshot contains only
the summary cards. The browser, contexts, tunnel, and SSH client were closed.

Evidence:
[live HTTP and browser proof](../deploy-evidence/pc2-dashboard-live-20260913-001.json),
[summary screenshot](../output/playwright/pc2-dashboard-live-20260913-001.png), and
[verification script](../scripts/verify-pc2-dashboard-live.py).

The displayed pool size is a file count. It does not establish that those files
contain valid, unexpired seeds or completed OAuth artifacts.

## Paid continuation boundary

The user's authorization is applied serially: one account and at most one
purchase attempt through the original guard, service `dr`, country 16, maximum
0.05 USD, no number reuse or same-session resend. Production global paid SMS
remains disabled. The private SMS service remains isolated and routes HeroSMS
requests through the original guard.

The prepared continuation is `easyregister-paid-once-20260912-005`, using the
retained strict-preflight image. Its initial check in this deployment sequence
found it `created`, never started, with an empty seed directory and no run
marker. The original acquisition ledger and receipt were absent. Both earlier
executed continuations and their private evidence remain retained.

At 19:26:39 UTC, the exact prepared image and environment were also exercised
in a temporary, read-only container with networking disabled. The real
`SmsRuntimeConfig.resolve_business_policy("openai")` returned `enabled=True`,
`allowPaid=True`, `allowReuse=False`, one binding per phone, country 16, and a
0.05 USD cap. The runner/flow hashes matched the reviewed contents, and all
flow retry limits remained one. This resolves the effective-policy question
for 005 without executing its account flow or making any SMS request.
See the [effective policy evidence](../deploy-evidence/pc2-paid-005-policy-20260913-001.json).

A valid small success is a prerequisite, followed by fresh quote/balance/order
checks. It must be copied without modification and verified under the normal
900-second age limit. A final large success requires completed OAuth,
free/personal validation, an identity-matching artifact, and reconciled order
and charge evidence. A paid-enabled configuration or a successful HTTP request
does not establish paid conversion.

## Post-restart generation and financial result

The closing PC2 snapshot was captured at 2026-09-12 19:35:42 UTC
(03:35:42 +08:00), covering approximately 57 minutes after the production
restart. The preceding 15-minute read-only watcher made 29 observations and
then exited; it could not start a paid task. Its last snapshot at 19:25:57 UTC
contained 15 completed tasks; the later closing snapshot contains 18.

- Completed post-restart tasks: 18.
- Genuine small successes: 0; large successes: 0.
- All 18 stopped at `create-openai-account`, with
  `browser_verification_required`, `stage_platform_login`, `platform_login`,
  and actual HTTP 403.
- Mailbox and proxy cleanup succeeded for all 18.
- The pool still contained 232 files, including 116 historically valid seeds,
  but zero current-age-valid seeds and zero newly created files.
- All four observed production containers were running, restart counts were
  zero, and authenticated Dashboard status returned 200.

These executions failed before SMS. They provide no measurement of paid SMS
conversion. The earlier eight-small-success cohort remains separate: five
reached the disabled SMS gate and three timed out during OAuth. The user's
expectation that SMS can advance a valid small success still requires one
real continuation with a fresh seed. No seed timestamp or age check was changed.

See the [bounded observation](../deploy-evidence/pc2-post-deploy-observation-20260913-001.json)
and [closing snapshot](../deploy-evidence/pc2-restart-final-snapshot-20260913-001.json).

The final read-only reconciliation at 2026-09-12 19:29:39 UTC used only
`getBalance`, `getPrices`, and `getActiveActivations` through the original guard:

- Balance: 4.5953 USD.
- Quote for service `dr`, country 16: 0.045 USD; available stock: 148,148.
- Active provider orders: 0.
- Task purchase requests: 0; task SMS spend: 0 USD.
- Original purchase ledger and receipt: absent; original guard hash unchanged.
- Continuation 005: `created`, never started; no seed, backup, or run marker.
- Active paid runners and known paid controllers: 0.
- Production paid SMS remained disabled; the private provider route and both
  credentials matched their intended peers in memory.

All four live source hashes and the production image still matched the deployed
candidate at this reconciliation. The quote is a timestamped observation and
must be refreshed before any later purchase. The original one-purchase allowance
has not been reset, replaced, or consumed.

See the [financial and source reconciliation](../deploy-evidence/pc2-paid-guard-reconciliation-20260913-001.json).

## Scope and remaining verification

No Git commit or push was performed, and the existing dirty worktree and sibling
projects were preserved. The focused candidate and runtime gates above passed.
The earlier complete CI attempt timed out; the known account-availability test
failures remain outside this deployment gate. This report does not claim a
complete green repository test suite.

The remaining acceptance gap is a fresh seed followed by one bounded real
phone-verification/OAuth completion and artifact/cost reconciliation. No paid
controller remains armed after this observation. Production generation is
running with its existing unpaid policy.
