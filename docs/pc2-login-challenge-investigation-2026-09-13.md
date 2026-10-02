# PC2 login challenge investigation

Date: 2026-09-13, Asia/Shanghai.
Latest task snapshot: 2026-09-12 20:12:58 UTC (04:12:58 +08:00).
Status: the failure boundary is identified; registration remains blocked.
The audit's missing challenge evidence was corrected. No new production image
was deployed in this follow-up, and no paid continuation was started.

## Observed failure boundary

The updated read-only audit found 30 completed tasks since the orchestrator
restart at 18:38:58 UTC. All 30 contained an explicit Cloudflare challenge
marker and failed at:

```text
step: create-openai-account
stage: stage_platform_login
detail: platform_login
code: browser_verification_required
HTTP status: 403
response evidence: cf-mitigated: challenge
```

Inspection of the actual running provider resolves this stage to the first
`GET https://platform.openai.com/login`. Its response checker recognizes the
challenge header before accepting an HTTP status. This flow has not yet reached
account submission, Codex OAuth, or SMS acquisition when it stops here.

There were zero genuine small successes, zero current-age-valid seeds, and
zero active tasks at the latest snapshot. All four observed production services
were running, and authenticated Dashboard status returned HTTP 200.

The response establishes a server-side browser-verification requirement. It
does not reveal the server's reason for choosing that challenge. The available
evidence does not establish a specific proxy, request-header, account-input,
or cookie defect whose correction would make the server accept the request.

Evidence: [latest task snapshot](../deploy-evidence/pc2-login-challenge-20260913-002.json).

## Why the prior restart did not clear it

The prior release changed four orchestrator files. It restarted the production
Python provider using its existing image, as recorded in the
[deployment report](pc2-review-deployment-2026-09-13.md). The provider's request
implementation therefore did not change with that restart.

The current production provider is:

```text
image: sha256:4297f6a2fe88c80ce3f5f4ef984bad0550a255c087d53b65699529f44e84b0ed
module: /app/src/new_protocol_register/protocol_small_success.py
module SHA-256: 0af3bc76897232bb70e277f1912d50a8f414cad3b3cbc7fd017d13376ad02974
```

The retained isolated provider has a different image and module hash. It is
not the production provider, and its past partial checks do not prove that
replacing production with it would complete registration. Neither provider
was replaced during this investigation.

The live orchestrator's configured cooldown for this failure is 180 seconds.
The first 28 completed tasks showed 180.108-180.149 seconds from each failure
to the next task's start. These are new iterations of the continuous worker
after cooldown. The step/task retry code also contains explicit browser-
verification guards. The terminal log does not serialize a retryability flag,
so the audit does not invent one from missing metadata.

Evidence: [runtime source and cooldown inspection](../deploy-evidence/pc2-login-boundary-runtime-20260913-001.json).

## Correction made in this follow-up

The previous audit omitted the explicit `cf-mitigated` evidence from its
redacted task records. That made a generic HTTP 403 and a confirmed browser
challenge harder to distinguish in the exported evidence.

[audit-pc2-restart-results.py](../scripts/audit-pc2-restart-results.py) now emits
`terminalCloudflareChallenge`, derived only from an explicit challenge marker
in the recorded message. It does not infer a challenge from HTTP 403 alone and
does not export the private message, email, proxy credential, cookie, or token.
A false value means that this marker was not found in the recorded message.

The regression first reproduced the missing output. The final
[three-test gate](../tests/test_pc2_restart_audit.py) passes and executes the
actual embedded remote audit with mocked Docker and HTTP I/O. It checks:

- Explicit challenge evidence is retained while private values stay redacted.
- A plain HTTP 403 is not labelled as a confirmed Cloudflare challenge.
- Both supported header spellings and case variations are recognized.

The corrected audit was then run against PC2, producing the latest 30-task
snapshot above. This is a diagnostic correction; it has not resolved the
registration failure. No success condition or upstream verification was
weakened, and no additional registration request was submitted by these probes.

## Spending boundary and next step

At 20:10:54 UTC, continuation `easyregister-paid-once-20260912-005` was still
`created`, never started. The original guard's identity and source hash were
unchanged; its purchase ledger and receipt were absent. The original single-
purchase allowance remains unused, production paid SMS remains disabled, and
task SMS spend remains 0 USD.

The next useful input is a self-owned account that can complete the normal
interactive browser verification and log in. A subsequent single-account OAuth
diagnostic can then determine whether the SMS and final artifact stages work.
The current evidence does not support claiming a paid conversion rate or a
large success. Automated attempts to bypass the platform's browser verification
are outside this repair.

All this follow-up's provider inspection was read-only. Source changes are
limited to the audit and its regression tests; existing deployments, sibling
source trees, private run histories, seed timestamps, and spending limits remain
preserved.
