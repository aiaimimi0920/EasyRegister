# PC2 real small-success acceptance, 2026-09-11

The real small-success acceptance passed after correcting the OpenAI mailbox
domain policy. The first task after the policy became active completed account
creation, Platform organization initialization, and ChatGPT login initialization.
A newly persisted artifact passed both the milestone validator and its normal
age-sensitive check. The artifact was independently matched to that real task.

The closing observation at `2026-09-11T15:25:29Z` (23:25:29 Asia/Shanghai)
confirmed six consecutive completed tasks with all three small-success steps
passing, six new milestone-valid JSON artifacts, and successful mailbox/proxy
cleanup for all six tasks. Four artifacts still passed the normal 900-second
age check at that observation. The six intermediate records without a completed
Platform organization were excluded from the success count.

Complete Codex OAuth remains a separate, unsuccessful downstream step. It is not
included in this small-success acceptance.

## Verified first artifact

The policy became active at `2026-09-11T15:05:13.675084369Z`
(23:05:13 Asia/Shanghai). Task 1 started at `15:05:16.201785Z` and finished at
`15:06:51.477573Z`.

- Artifact root inside `easy-register`: `/shared/register-output/openai/pending`.
- SHA-256: `b094cfe62d6a4eade7a94a775ebb4bbeac30cce675f036e5d61875b88eae1c86`.
- Size: 22,638 bytes.
- Payload `createdAt`: `2026-09-11T15:05:54Z`.
- File modification time: `2026-09-11T15:06:51.496812Z`.
- Mailbox provider: `cloudflare_temp_email`.
- Mailbox domain: `lake.neuroloom.pp.ua`, within the active OpenAI domain pool.

At the independent `15:14:52Z` verification, both creation and file timestamps
were after deployment. `validate_openai_oauth_seed_payload` returned valid with
and without its normal maximum-age enforcement. The age-sensitive result is a
timestamped observation; the existing 900-second seed age limit was retained.

The persisted payload contained a completed Platform organization and completed
ChatGPT login. A personal workspace was present, and client bootstrap reported
`logged_in` with `personal` structure. Access and refresh material were present.
The same account identity did not occur in pre-deployment artifacts.

The task record matched the artifact's private identity fields. These actual
steps were all `ok`:

- `create-openai-account`
- `initialize-platform-organization`
- `initialize-chatgpt-login-session`
- `release-mailbox`
- `release-proxy-chain`

Its later terminal step was `obtain-codex-oauth`, with
`obtain_codex_oauth_failed`. Credentials, account addresses, and the private
account filename are excluded from the local evidence. The original JSON remains
in the production artifact root.

## Investigation and correction

The `14:26:44Z` pre-correction observation recorded 20 completed tasks and no new
milestone-valid artifact: 16 browser-verification failures and four OTP timeouts.
Task 21 was still active. This corrected the earlier assumption that every
current-version attempt stopped before the OTP branch.

A controlled mailbox lifecycle test used a domain that had just experienced an
OTP timeout. The service accepted the test message, received it, and extracted
the expected code. Both test mailboxes were deleted upstream. A subsequent
snapshot confirmed release metadata for both; the recipient retained the local
`resolved` status while the sender was `expired`. These controlled messages were
only mailbox diagnostics and did not create registration artifacts.

The provider's new response diagnostics observed HTTP 200, JSON content,
`email_otp_verification`, the expected `/api/accounts/email-otp/send` path, and
no redirects or response error. This excluded an HTML redirect being mistaken
for a successful resend in the observed attempt.

The configured mailbox service exposed 4,548 domains, while EasyRegister had no
OpenAI-specific domain pool. The four independently inspected earlier small
successes all used `.pp.ua` domains. The current attempts had selected `.cc.cd`
and `.us.ci` domains. The four previously successful domains were still configured
and were not explicitly or dynamically blacklisted.

The correction sets only the `openai` entry's `domainPool` in
`REGISTER_MAILBOX_BUSINESS_POLICIES_JSON` to:

```text
clay.yamiyu.pp.ua
leaf.yamiyu.pp.ua
flora.neuroloom.pp.ua
lake.neuroloom.pp.ua
```

Other business-policy fields were preserved. The actual deployed resolver
accepted this pool and selected a matching Cloudflare provider domain. The first
production task using the corrected policy produced the verified artifact above.
The observations do not establish why the upstream sender failed to deliver to
the other domain groups or prove a long-term success rate.

## Deployment and checks

The provider diagnostic backport changed only
`_resend_passwordless_email_otp` in the verified running base. Its request,
deadline, and no-replay behavior remained covered by the existing tests. The
candidate builder checked that other top-level definitions were unchanged.

This also exposed a deployment defect: when the orchestrator image was unchanged,
promotion stopped its container but skipped the Compose operation that would
restart it. The same issue affected rollback. Two regression tests reproduced
both stopped-container outcomes, then passed after adding the missing restart.
The real provider-only promotion subsequently resumed the same orchestrator
container successfully.

Validation completed:

- 13 targeted OTP recovery tests passed.
- Both promotion/resume regression tests passed after reproducing their failures.
- 31 offline checks passed inside the candidate images with networking disabled
  and without production data mounts.
- Source and embedded-program compilation, UTF-8/no-BOM, and scoped whitespace
  checks passed.
- Both promotions waited for zero active tasks and zero busy provider workers.
- The configuration promotion verified the single changed environment key,
  unchanged image, command and mounts, all eight tracked source hashes, Dashboard
  HTTP 200, and the identities of the other three project containers.

Running provider image:
`easy-register/easy-protocol-python:pc2-otp-resend-20260911-002`
(`sha256:4297f6a2fe88c80ce3f5f4ef984bad0550a255c087d53b65699529f44e84b0ed`).
The orchestrator image remains
`sha256:a2da348213fe60d102c157ad290455527b94deb3928025e29606ffe8a793f20c`.

The OpenAI-only policy override and its validated rollback are retained on PC2:

```text
/home/mjc/easyregister/small-success-mailbox-policy-20260911-001.yaml
/home/mjc/easyregister/small-success-mailbox-policy-rollback-20260911-001.yaml
```

The continuous scheduler remains enabled. No full-repository CI, stable success
rate, or complete Codex OAuth success is claimed.

## Evidence and tools

- [Closing six-task observation](../deploy-evidence/pc2-real-small-success-after-20260911-002.json)
- [First fresh business observation](../deploy-evidence/pc2-real-small-success-after-20260911-001.json)
- [Independent artifact and task identity proof](../deploy-evidence/pc2-real-small-success-artifact-20260911-001.json)
- [OpenAI domain-policy promotion](../deploy-evidence/pc2-small-success-policy-promotion-20260911-001.json)
- [Observation before the policy correction](../deploy-evidence/pc2-small-success-policy-before-20260911-001.json)
- [Controlled mailbox delivery and cleanup](../deploy-evidence/pc2-small-success-mail-diagnostic-20260911-001.json)
- [Diagnostic image and offline checks](../deploy-evidence/pc2-otp-response-candidates-20260911-002.json)
- [Provider diagnostic promotion](../deploy-evidence/pc2-otp-response-promotion-20260911-002.json)
- [Original version pre-correction observation](../deploy-evidence/pc2-otp-response-before-20260911-002.json)
- [Guarded OpenAI mailbox-policy update](../scripts/configure-pc2-small-success-mailbox.py)
- [One-function diagnostic candidate builder](../scripts/verify-pc2-otp-response.py)
- [Promotion/resume regression tests](../tests/test_pc2_promotion_resume.py)
- [Read-only fresh-artifact watcher](../scripts/watch-pc2-small-success.py)

For another timestamp-bounded observation:

```powershell
rtk proxy python scripts/watch-pc2-small-success.py --since 2026-09-11T15:05:13.675084369Z --timeout-seconds 900
```

Deployment builders are deliberately pinned to their original baselines. Do not
rerun an old builder or promotion against a changed deployment without preparing
a new verified baseline.
