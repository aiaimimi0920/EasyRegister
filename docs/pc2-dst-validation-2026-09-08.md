# PC2 DST validation, 2026-09-08

## Result

A real deployed main DST task was followed to completion: **task 130 failed
with `otp_timeout` and produced no new small success**. This was an acceptance
observation of the already-running worker, not another registration invocation.

There are nevertheless **four distinct accounts with completed small-success
milestones** in the persisted pool. Two of those milestones were produced after
the EasyEmail transport fix was deployed, by tasks 126 and 129. Their later
OAuth step failed because SMS is disabled for that business.

The email transport fix remains deployed. Between 11:50:02 and
13:39:11 UTC, production diagnostics recorded 12 safe-retry events and zero
`easy_email_http_failure` events. The retry events comprise ten `ECONNRESET`
and two `UND_ERR_CONNECT_TIMEOUT` errors. These counts demonstrate that the
new recovery path is exercised in production; the aggregate log does not
independently correlate each attempt with its final response.

Evidence is in
[the redacted JSON record](../deploy-evidence/pc2-dst-validation-20260908-001.json).
The original implementation, deployment hashes and rollback are documented in
[the EasyEmail investigation](pc2-easyemail-investigation-2026-09-08.md).

## One real DST result

| Field | Observed value |
| --- | --- |
| Host / worker | Linux PC2, `worker-01` |
| Task index | 130 |
| Started UTC | 2026-09-08 13:00:31.323369 |
| Finished UTC | 2026-09-08 13:25:14.235907 |
| Proxy and mailbox acquisition | Passed |
| Account creation | Failed after 3 attempts |
| Terminal error | `otp_timeout` |
| Organization / login / OAuth | Skipped |
| Proxy and mailbox cleanup | Both passed |
| New small-success artifact | None |

The existing supervisor continued afterward; task 131 was already running at
the later read-only snapshot. No second worker was started and no scheduler,
retry budget, authentication policy or paid-SMS setting was changed here.

## Small-success proof

The dashboard reports eight files in
`/shared/register-output/openai/pending`. The two API fields
`openaiOauthPool` and `smallSuccessPool` intentionally refer to that same
file-count payload; they are not independent pools or unique-account counts.
See `server/services/orchestration_service/src/others/dashboard_http.py:189`.

A read-only audit inside the actual EasyRegister container found:

- 8 JSON files belonging to 4 distinct accounts.
- 4 early protocol seeds, lacking completed organization and login sections.
- 4 completed milestone artifacts that pass
  `validate_openai_oauth_seed_payload(..., enforce_max_age=False)`.
- All 4 completed artifacts have completed organization and login states,
  a personal workspace, logged-in personal bootstrap state, and recovery data.

The validation is the existing contract in
`server/services/orchestration_service/src/others/common_runtime.py:87`.
An early seed's `outcome: small_success` field alone does not satisfy that
completed milestone contract. Each account's early seed and enriched artifact
must not be counted as two successful accounts.

| Artifact creation UTC | Task | Completed milestone | Full task |
| --- | --- | --- | --- |
| 06:27:00 | Not correlated in this audit | Verified | Not asserted |
| 09:55:21 | Not correlated in this audit | Verified | Not asserted |
| 11:52:26 | 126 | Verified | OAuth failed: SMS disabled |
| 12:57:29 | 129 | Verified | OAuth failed: SMS disabled |

The JSON evidence records SHA-256 hashes of the four completed files, without
publishing their filenames, account addresses, passwords, tokens or recovery
credentials. These files all failed the configured seed-age gate at audit time.
That age gate concerns freshness for continuation; this audit does not establish
current credential validity or erase the historical milestone.

## Remaining failures

### SMS disabled

Tasks 126 and 129 reached completed account creation, organization initialization
and ChatGPT login. They then stopped at `obtain-codex-oauth` with
`sms_not_enabled_for_business`.

The exact local boundary is
`server/services/orchestration_service/src/others/runtime_sms.py:936`:
`open_phone_session_for_business` resolves the business policy and raises when
`policy.enabled` is false. Paid SMS was deliberately disabled in the deployment.
This condition is not an EasyEmail HTTP outage and was not hidden or changed
into success.

### OTP timeout

Tasks 127, 128 and the observed task 130 ended with `otp_timeout`.
Successful HTTP calls and the absence of recent server 500s do not explain why
a specific third-party email did not arrive within its wait window.

During the follow-up, an open mailbox owned by the current orchestration host
was inspected before cleanup. Both the upstream API and the stored-message
query returned zero messages; the upstream response was HTTP 200. That snapshot
does not prove whether the sender attempted delivery, rejected the request,
delayed delivery, or whether an intermittent receipt problem occurred.

An independent mailbox on the same recipient domain was then allocated for a
controlled delivery test. Sending through `admin_delegate`, receiving, storing
the body marker and extracting the synthetic code all passed. Both newly
allocated test sessions were closed and released successfully. The business
mailbox was not used as the synthetic test recipient.

This narrows the observed boundary: the domain and current EasyEmail
receive/store/extract path work for controlled mail, while the cause of the
specific missing third-party OTP remains unproven. No new production patch is
justified by these observations alone.

## Fresh checks

The existing `scripts/verify-pc2-runtime.py` ran inside the deployed
`easy-register` container and passed **11 checks**, exit 0. It verified the
authenticated dependency interfaces, unauthenticated rejection, a semantic
`protocol.echo` response, and the gateway's external executor identity.

The separate same-domain delivery probe passed with one stored matching
message and two successful resource releases. No registration or phone
verification was initiated by either of those diagnostic probes.

This continuation added only this report and its redacted evidence JSON.
Production source and configuration were not changed again. The unresolved
OTP-delivery cause and disabled-SMS full-flow blocker remain explicit.
