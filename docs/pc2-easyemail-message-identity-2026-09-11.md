# PC2 EasyEmail message identity, 2026-09-11

The Cloudflare message-ID collision repair is deployed and verified through both
actual PC2 caller containers. File persistence is independently verified. The
complete business workflow has not reached stable small success: the fixed
post-deployment sample below contains 0 small successes in 20 completed tasks.

This continues session `01a08c47-15b3-7643-9610-e1511e815761`. The deployment and
first NAS fixture run occurred in that session; the current continuation verified
the running image, tested source, PC2 callers, disk state, and subsequent errors.

## Repair and data boundary

The upstream can reuse a numeric mail ID after a mailbox is deleted. EasyEmail
previously used `cloudflare_temp_email:<upstream-id>` as the global registry key,
allowing a later mailbox's message to replace another mailbox's record.

`EasyEmail/service/base/src/providers/cloudflare_temp_email/connector/client.ts`
now scopes both the subject-code and body-code paths by provider instance and
mailbox session:

```text
cloudflare_temp_email:<encoded-provider-instance>:<encoded-session>:<upstream-id>
```

Repeated polling within one session still returns the same identity. Regression
tests cover reused numeric IDs across sessions and provider instances in both
extraction paths. The targeted change preserves the surrounding transport logic.

The current historical data still contains 45 cross-mailbox references to legacy
IDs. The repair prevents new collisions; it cannot reconstruct messages already
overwritten. No historical content was invented, deleted, or reassigned.

## Fresh verification

The focused EasyEmail suite passed 20 tests in 4 files, including the 4 identity
regressions. The official TypeScript build passed. Effective-line checker tests
passed 19/19, and ratchet mode passed across 1,034 files with no files in the
501-700, 701-1500, or greater-than-1500 tiers. These are ratchet results.

The new `scripts/verify-pc2-email-message-identity.py` completed its real test at
2026-09-10 21:15:05 UTC (2026-09-11 05:15:05 Asia/Shanghai):

- It opened one self-owned sender and two recipients, then sent two synthetic
  fixtures while allowing the upstream numeric ID to be reused.
- Both `easy-register` and `easy-register-protocol-python` returned HTTP 200 for
  both fixtures, matched the synthetic code and session, and agreed on each ID.
- The two fixtures retained distinct IDs and separate observed records.
- All 13 pre-test observed records remained byte-content equivalent under the
  verifier's canonical JSON fingerprint.
- All 3 diagnostic mailboxes were closed and released upstream. Cleanup errors
  were empty, and both caller container identities and start times were unchanged.

API observation alone is insufficient proof of disk persistence in file mode.
The updated read-only `scripts/audit-nas-email-delivery.py` additionally reads the
actual runtime-configured state file. At 21:32:48 UTC it found 15 disk messages;
all 15 API records matched their disk records, with zero missing or different
records. Four records use the new scoped IDs, and none has a cross-session
reference. Eleven legacy records and their 45 historical reference conflicts
remain. The four scoped records come from the two controlled fixture runs.

All three diagnostic Python modules and their embedded JavaScript/Python programs
passed syntax checks. Scoped whitespace checks and UTF-8-without-BOM checks passed.
No additional production source change was required in this continuation.

## Verified running artifact

The final NAS inspection at 2026-09-10 21:51:58 UTC confirmed:

- Image: `easyemail/easy-email-service:nas-message-identity-20260911-002`.
- Image ID: `sha256:ecb5512d7914ed206cf87daf86195697f8707ad9a4d54608282b5951f494152f`.
- Container: `easyemail-sdk`, running, restart count 0.
- Started: `2026-09-10T18:25:41.348098764Z`.
- Compiled connector SHA-256:
  `8acdc3c73303cb405bd4d88cb31324ebf3b929ef2fd73db22f03423d6508a9b5`.
- The compiled message-reading method and its following module text match the
  freshly tested local build, SHA-256
  `e23636d8cc61e645eb9060f45581382a7f29137b79d7dec58ff06e437d188f68`.

No identity-smoke container remains. This continuation did not replace or restart
any production container. The prior guarded deployment tested two identity cases
and two file-store round trips in a network-disabled candidate container before
promotion; current live-image verification does not rerun that one-time upgrade.

## Remaining business failures

The fixed business snapshot was captured at 2026-09-10 21:35:55 UTC
(2026-09-11 05:35:55 Asia/Shanghai). Since the image deployment, 21 tasks had
finished. Task 351 began before deployment and is excluded from the strict
post-deployment denominator. Of the 20 tasks that both started and finished after
deployment, 4 ended with `otp_timeout` and 16 with `authorize_continue_blocked`.
None completed the account, organization, and login initialization milestones;
none completed the full workflow. All 20 acquired and released their mailbox and
released their proxy successfully.

Classification uses `result.stepErrors[result.errorStep].code`. Earlier retry
errors do not override the terminal step's structured code. The earlier 13-task
report was corrected to 5 small successes, 7 OTP timeouts, and 1
`authorize_missing_login_session`; that historical sample must not be mixed with
the new post-identity-deployment sample.

A supplementary read-only probe found HTTP 403 markers in all 18 inspected
`authorize_continue_blocked` terminal records. The latest inspected details also
contained a challenge marker. That probe was later than the fixed 20-task snapshot
and has a different scope. These failures remain at third-party authorization.
No access check, challenge response, or success gate was weakened.

For one active mailbox observed at 20:12:04 UTC, the decoded mailbox address
matched its session and the upstream inbox query succeeded, but both the upstream
inbox and EasyEmail's stored message set were empty. This single observation
locates missing data before local extraction for that mailbox; it does not prove
whether the sender attempted delivery or why a third-party message was absent.

NAS diagnostics since deployment contain 16 bounded upstream retries: 11
`ECONNRESET` and 5 `UND_ERR_CONNECT_TIMEOUT`. The only recorded HTTP failure is a
send request at 18:38:25 UTC with upstream HTTP 400, before the successful PC2
fixture test. There is no evidence that the ID repair alone resolves every OTP
timeout or third-party authorization failure.

## Artifacts and rollback

Sanitized evidence, exact hashes, commands, caller checks, and the fixed task
sample are in
[`pc2-easyemail-message-identity-20260911-002.json`](../deploy-evidence/pc2-easyemail-message-identity-20260911-002.json).
The repeatable PC2 diagnostic is
[`verify-pc2-email-message-identity.py`](../scripts/verify-pc2-email-message-identity.py).
The disk/upstream audit is
[`audit-nas-email-delivery.py`](../scripts/audit-nas-email-delivery.py).

The earlier deployment recorded its protected rollback environment at
`/volume1/docker/easyemail-sdk/deploy/service.env.before-message-identity-20260911-002`,
pointing to the transport-recovery image. The guarded deployment helper is a
one-time, exact-base upgrade and must not be rerun against the upgraded service.
No rollback, new Git commit, tag, or push was performed in this continuation;
the existing Git checkpoint does not yet include this identity repair.
