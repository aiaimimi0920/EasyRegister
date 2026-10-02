# PC2 authorization failure repair, 2026-09-11

The platform-login failure has been reproduced and the internal error-handling
repair is deployed. The final orchestrator revision started at 03:37:18 UTC
(11:37:18 Asia/Shanghai). Three real post-deployment tasks confirm accurate error
metadata, one attempt, resource cleanup, and the preserved 180-second blocked
cooldown. The business workflow still has no new small success: the site continues
to require browser verification, and missing OTP delivery remains unresolved.

## Observed failure boundary

The terminal records previously classified as `authorize_continue_blocked`
actually contained `platform_login status=403`. This is the initial
`GET https://platform.openai.com/login`, before `authorize/continue` is sent.

A single diagnostic GET reused the last observed production proxy and the
deployed HTTP client. It returned HTTP 403 with `server: cloudflare` and
`cf-mitigated: challenge`. The cookie jar contained only `__cf_bm`; no login
session was established. This confirms a browser-verification boundary. It does
not identify the remote service's reason for challenging the request.

The generic word `cloudflare` in earlier task errors was insufficient proof:
the errors also appended `mailbox_provider=cloudflare_temp_email`. The direct
response-header observation is the evidence used for this repair.

Two internal boundaries hid the useful diagnostics. The Python worker queue
kept only the exception message, discarding `stage`, `detail`, and the protocol
category. The EasyRegister HTTP-error branch then stringified the complete error
envelope. The resulting fallback classifier obscured the platform-login stage.

## Implemented changes

- An explicit challenge response now raises `browser_verification_required`
  while retaining its actual stage and detail. Raw challenge HTML is omitted.
- HTTP failure is checked before the login-cookie requirement, so a rejected
  authorization request cannot be mislabeled as a successful request with a
  missing cookie.
- The worker queue preserves protocol metadata under `details.protocol_error`.
  EasyRegister preserves that metadata for both HTTP-error and failed-payload
  responses.
- A challenge does not trigger browser recovery, another provider network
  attempt, a step retry, or a task retry. Existing typed errors and already
  recorded phone milestones retain their classification priority.
- The verification-required result uses the existing authorization-blocked
  cooldown. On PC2 that setting is 180 seconds; the generic creation cooldown
  is 120 seconds. The result must not shorten the previous blocked backoff.

The retry change applies within a failed task. It does not globally pause the
continuous production scheduler. Completing the site's normal browser
verification remains an external prerequisite; no challenge-solving path was
added or enabled by this repair.

The source changes are in `protocol_small_success.py` and `worker_pool.py` in
EasyProtocol, and `error_catalog.py`, `easyprotocol_runtime.py`, and
`dst_flow_runtime.py`, and `runner_failures.py` in EasyRegister. Existing unrelated worktree changes were
not included in the candidate images.

## Verification

Fresh local checks passed:

- EasyProtocol boundary regressions: 9 tests.
- EasyRegister boundary regressions: 7 tests.
- Existing EasyRegister error/profile gate: 53 tests.
- Existing EasyProtocol flow gate: 101 tests, exit 0.
- Syntax, strict UTF-8 without BOM, and whitespace checks across 11 touched
  Python files; all 4 embedded diagnostic/deployment programs compiled.
- Scoped `git diff --check` passed.

The two older flow tests that assumed automatic handling of a challenge were
updated to preserve ordinary invalid-state recovery and require interaction for
an explicit challenge. Normal transport retry and successful-cookie behavior
remain covered.

The packaged runtime checks ran with networking disabled and no production data
mounts. Five provider checks exercised the real packaged HTTP guard and worker
queue. Five orchestrator checks consumed the provider's error packet and verified
both transport paths, stage preservation, retry rejection, and blocked cooldown. These 10 checks
passed in the actual Python 3.10 images.

## Running hotfix images

The provider image is
`easy-register/easy-protocol-python:pc2-auth-boundary-20260911-001`, image ID
`sha256:20374cb52eef5311fd7d73ff368f9efb83186452b2942550e481e35d8952bc77`.

The final orchestrator image is
`easy-register/easy-register:pc2-auth-boundary-20260911-003`, image ID
`sha256:909915e52e9eb5a1010a9dbfa29e045ce8908168a4fa7da6c72f87e0ec5465bf`.
Revision 002 preserves pre-existing typed-error and milestone precedence;
revision 003 additionally preserves the blocked cooldown. Earlier images remain
available and were not overwritten.

Packaging started from the exact running base images. The three orchestrator
source files and worker-pool baseline matched Git HEAD after line-ending
normalization. Only the targeted function/guard changes were applied to the
older deployed protocol module; the larger uncommitted local protocol changes
were not copied into the image. Source is emitted as UTF-8 with LF inside images.

Image configuration and base-layer ancestry were checked. Promotion also checks
the six packaged source hashes and verifies that the rendered environment,
command, and data mounts match the current production containers.

Evidence:

- [Final deployment and runtime evidence](../deploy-evidence/pc2-auth-boundary-final-20260911-003.json)
- [Final candidate verification](../deploy-evidence/pc2-auth-boundary-candidates-20260911-003.json)
- [Revision 002 production promotion](../deploy-evidence/pc2-auth-boundary-promotion-20260911-002.json)
- [Initial drain attempt](../deploy-evidence/pc2-auth-boundary-drain-20260911-001.json)
- [Candidate builder and offline verifier](../scripts/verify-pc2-auth-boundary.py)
- [Guarded production promotion](../scripts/promote-pc2-auth-boundary.py)

## Production promotion status

The first promotion attempt ended at 2026-09-11 02:05:46 UTC with
`failedStage=drain` and `errorCode=drain_timeout`. No container was stopped or
replaced. A second attempt uses the final revision 002 candidate and a longer
drain window. It requires zero active tasks and zero busy provider workers.

Task 41 subsequently completed normally. The second promotion reached zero
active tasks and zero busy workers, then updated the provider at 02:58:53 UTC and
the orchestrator at 02:59:01 UTC. The dashboard returned HTTP 200 and all five
deployed source hashes matched the validated candidates. The protocol gateway
and SMS container retained their identities and start times. No forced task
termination was required.

The revision 003 rollout changed only the orchestrator at 03:37:18 UTC. The
already updated provider was reused. This promotion again drained all active
tasks and busy workers before stopping the orchestrator. The dashboard returned
HTTP 200, and all six live source hashes matched the candidate manifests.

An independent inspection at 03:53:57 UTC confirmed that all four containers
were running with restart count zero and the expected image IDs. No Git commit
or push was performed.

## Actual post-deployment results

The fixed audit at 03:47:30 UTC contains three completed tasks and one active
task. Each completed task reported `browser_verification_required`, with
`stage=stage_platform_login`, `detail=platform_login`, and `category=blocked`.
Each made exactly one task attempt and one account-creation step attempt; mailbox
and proxy release both succeeded. This is actual production evidence of the
repaired worker-to-gateway-to-orchestrator error path, beyond the offline fixtures.

The observed intervals from task completion to the next task start were 180.114
and 180.121 seconds. The new error code therefore retains the previous blocked
backoff. No browser challenge was treated as successful authorization.

The persisted pool still contained 72 files and 36 historical milestone-valid
artifacts, with zero new files and zero new milestone-valid artifacts after this
deployment. The three correctly handled failures do not establish registration
success or stable business operation.

## Remaining business evidence

While preparing the repair, production tasks 38 and 39 progressed beyond the
authorization boundary and later ended with `otp_timeout`. This shows that the
authorization challenge is intermittent and does not account for every failure.

One inspected OTP poll used a marker floor of zero. The submitted address matched
its EasyEmail session, but there were no stored messages or extracted codes.
This observation does not support a scoped-message-ID filtering failure. A later
refresh targeted an already expired session and was skipped, so it is not proof
of a successful upstream inbox query. The cause of absent third-party mail has
not been established by these observations.

The internal error-handling repair is verified and deployed. Restoring the full
business workflow still requires a normal, supported browser verification/login
path and resolution of the absent OTP. No new small success is claimed.
