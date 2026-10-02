# PC2 OTP resend recovery, 2026-09-11

Later acceptance update: the current provider plus the corrected OpenAI mailbox
domain policy produced a new real small-success artifact. The first post-policy
task completed all three milestones, and an independent check at 15:14:52 UTC
validated the artifact, its age, and its matching task identity. See
[the completed small-success acceptance](pc2-real-small-success-2026-09-11.md).
The fixed observations below describe the earlier rollout window.

The remaining OTP recovery changes from session
`01a08dc5-66b7-7c60-892b-51ab556711e2` are implemented, tested, packaged, and
deployed. The provider started at 11:35:42 UTC and the orchestrator at
11:35:50 UTC (19:35 Asia/Shanghai). Promotion waited for zero active tasks and
zero busy provider workers.

Full business recovery remains unverified. At the fixed 12:14:26 UTC observation
(20:14:26 Asia/Shanghai), all twelve completed post-deployment tasks had stopped
at an explicit HTTP 403 browser-verification response. None reached the new OTP
resend branch, and no new small-success artifact had been produced in that
deployment window. Normal site verification is the immediate external
prerequisite for validating that branch with a real registration.

## Recovered scope and corrected baseline

The previous session had deployed the authorization error-boundary repair and
then investigated missing OTP. Its last implementation added one delayed resend
for a passwordless `email_otp_verification` page, but had not packaged or deployed
that work.

Fresh observation corrected the older no-success status. Before this rollout,
the existing authorization-boundary images had completed 48 tasks as of
11:31:16 UTC. Tasks 28, 44, 45, and 48 completed account creation, platform
organization initialization, and ChatGPT login-session initialization. Four new
persisted artifacts also passed the existing milestone validator. These small
successes preceded the resend deployment.

All four subsequently failed at `obtain-codex-oauth` with the fallback code
`obtain_codex_oauth_failed`; this was not complete OAuth success. Their terminal
records lacked a specific protocol stage/detail. At the baseline audit time,
none of the four artifacts passed the current-age check. No artifact age limit
or success criterion was changed.

The baseline pool contained 80 files and 40 historical milestone-valid artifacts.
The eight files new since the earlier authorization rollout comprised four valid
milestones and four intermediate records lacking a platform organization.

## Implemented behavior

The passwordless verification-page branch first waits for the automatically
sent code. If no usable code is found after 60 seconds, it may make one normal
OTP-send request using the existing session and proxy. The existing
`PROTOCOL_ENABLE_EMAIL_OTP_SEND` setting remains effective. A live feature probe
confirmed it is enabled through its default, with no explicit override present.

Both the code endpoint and a fresh inbox snapshot must complete successfully
before the resend can run. A code from either path wins immediately. Mailbox
read failures cannot trigger the side effect. The callback is consumed once,
including when it declines to send because the setting is disabled or the
remaining budget is insufficient.

The original wait deadline remains in force. Request timeout is bounded by the
remaining budget, and the next poll sleep cannot add time after that deadline.
Ambiguous transport failures cannot replay the send through the urllib fallback.
Rejected sends retain `stage_otp_send` / `email_otp_resend`; neither step retry nor
task retry may repeat that operation. Explicit browser verification and HTTP 429
remain terminal for the attempt.

The implementation is in the EasyProtocol mailbox client,
`protocol_small_success.py`, and `_session_request`, with the matching
`email_otp_resend` retry guard in EasyRegister's `dst_flow_runtime.py`.

Two additional regressions were reproduced before fixing them in this
continuation: a failed snapshot still allowed a resend, and a callback ending at
the deadline added another poll sleep. Both new tests failed before the patch
and passed afterward.

## Verification

Fresh local checks passed, 184 tests in total:

- EasyProtocol OTP resend recovery: 13.
- EasyProtocol authorization boundary: 9.
- Existing EasyProtocol flow suite: 101.
- EasyRegister protocol boundary: 8.
- Existing EasyRegister error/profile suite: 53.

The actual Python 3.10.21 candidate images passed 31 offline checks: 13 OTP
tests, 8 orchestrator boundary tests, and the existing 5 provider plus 5
orchestrator authorization probes. These containers had networking disabled,
read-only roots, and no production data mounts.

The builder verifies exact running image IDs, baseline source hashes, relevant
function shapes, and callback AST structure before building. The larger dirty
local protocol modules were not copied into production. The deployed request
function differed from the local version in its existing log formatter, so the
package applies only the two transport-replay edits to the verified live
function. Import edits are limited to the actual import node. AST comparisons
parse both sides with the same interpreter; PC2's build host uses Python 3.13.5.

Candidate ancestry, image configuration, source hashes, and the unchanged
production container snapshot were verified before promotion. Strict UTF-8
without BOM, embedded-program compilation, and scoped whitespace checks passed.
The repository-wide CI suites were not run or claimed.

## Deployment and rollback

Running images:

- `easy-register/easy-protocol-python:pc2-otp-resend-20260911-001`
  (`sha256:1ecdb4d91f341a06570d0e998bc237d57534e228ad6ac674fd6ee3a6714ca96e`).
- `easy-register/easy-register:pc2-otp-resend-20260911-001`
  (`sha256:a2da348213fe60d102c157ad290455527b94deb3928025e29606ffe8a793f20c`).

The guarded promotion verified environment, command, and bind-mount equivalence
before switching images. Both services passed their health checks. The protocol
gateway and SMS container retained their identities and start times. No active
task was forcibly terminated.

An independent inspection at 11:43:51 UTC confirmed the running image IDs,
restart counts of zero, all eight expected source hashes, and Dashboard HTTP
200. The previous images remain available. Image-only rollback overrides exist
under `/home/mjc/easyregister`:

- `otp-resend-provider-rollback-20260911-001.yaml`
- `otp-resend-register-rollback-20260911-001.yaml`

The promotion program retains automatic rollback on a failed switch. A rollback
was not executed because the deployed health and source checks passed.

## Post-deployment evidence and remaining work

The twelve completed tasks in the fixed post-deployment audit all released their
mailboxes and proxy chains successfully. Eleven stopped at `stage_platform_login`;
one stopped at `stage_auth_continue` / `oauth_authorize`. All retained
`browser_verification_required` and HTTP 403. The measured cooldowns remained
approximately 180 seconds. The continuous scheduler remains enabled.

There were zero resend-request or resend-accepted events, zero newly persisted
artifacts, and zero new milestone-valid artifacts in this window. Offline
recovery tests and deployed source verification establish the code change;
successful delivery following a real delayed resend still needs observation
after the normal login/verification boundary is completed. The separate complete
Codex OAuth failure also remains unresolved. No stable full-workflow success is
claimed.

Use the read-only audit with the exact deployment boundary:

```powershell
rtk proxy python scripts/audit-pc2-restart-results.py --since 2026-09-11T11:35:50.987366732Z
```

The candidate builder is deliberately pinned to the pre-rollout image IDs and
rejects a changed live baseline. The saved candidate evidence identifies the
already verified release. Existing unrelated worktree changes were preserved;
no Git commit or push was performed.

## Evidence and tools

- [Candidate and packaged checks](../deploy-evidence/pc2-otp-resend-candidates-20260911-001.json)
- [Pre-deployment business baseline](../deploy-evidence/pc2-otp-resend-before-20260911-001.json)
- [Guarded promotion](../deploy-evidence/pc2-otp-resend-promotion-20260911-001.json)
- [Independent live hash and rollback inspection](../deploy-evidence/pc2-otp-resend-runtime-20260911-001.json)
- [Initial post-deployment observation](../deploy-evidence/pc2-otp-resend-after-20260911-001.json)
- [Final fixed post-deployment observation](../deploy-evidence/pc2-otp-resend-after-20260911-002.json)
- [Candidate builder](../scripts/verify-pc2-otp-resend.py)
- [Promotion and rollback program](../scripts/promote-pc2-auth-boundary.py)
- [Read-only runtime audit](../scripts/audit-pc2-restart-results.py)
