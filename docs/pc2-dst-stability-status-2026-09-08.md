# PC2 DST stability status, 2026-09-08

## Outcome

The complete account workflow has **not** reached stable success. The independent
dependency DST diagnostic is now fixed, packaged as a dedicated PC2 image, and
verified by three consecutive real runs. All 18 steps passed, each run used one
task attempt, and every run independently confirmed resource cleanup. This is
short-run acceptance of the dependency path, not evidence of long-term or
complete account workflow stability.

The dependency test rejected PC2's valid `MAILBOX_SERVICE_API_KEY` because
it required the alias `EASY_EMAIL_API_KEY` before calling the existing
normalizer. This produced exit 1 without running any DST step. The repair moves
`ensure_easy_email_env_defaults()` before credential preflight and removes
the redundant later call. Existing precedence, missing-credential rejection,
and the no-secret-output contract are unchanged.

## Packaged diagnostic acceptance

The new acceptance completed at **2026-09-08 15:29:47 UTC**. PC2 now contains:

- Image: `easy-register/diagnostics:nas-dst-preflight-20260908-001`
- Image ID: `sha256:69ec28d7ada561bc409bdd8eef5814ff152394e8fdc545506d9f879f35098843`
- Default command: `python -m nas_dst_smoke`
- Base: the exact existing `easy-register/easy-register:nas-dst-20260908-003` image

The build adds only the corrected diagnostic module. Its SHA-256 matches the
earlier regression-tested source below. The flow file matches the deployed
file, the image's inherited environment is unchanged, and no live credential
was written into the image or a host-side environment file.

| Run | Exit | Steps passed | Task attempts | Elapsed | Cleanup errors |
| --- | --- | --- | --- | --- | --- |
| 1 | 0 | 6 / 6 | 1 | 12.44 s | 0 |
| 2 | 0 | 6 / 6 | 1 | 8.84 s | 0 |
| 3 | 0 | 6 / 6 | 1 | 10.44 s | 0 |

Every run acquired a real proxy lease, probed `https://example.com/`, acquired
and recovered a real mailbox with identity preserved, and released both the
mailbox sessions and proxy lease. There were no DST error markers. The
credential-free container also exited 1 at preflight with its network disabled,
before any DST step, as expected.

The three runtime containers received only `MAILBOX_SERVICE_BASE_URL`,
`MAILBOX_SERVICE_API_KEY`, `EASY_PROXY_BASE_URL`, and
`EASY_PROXY_MANAGEMENT_PASSWORD` from the live container in process memory.
They used the existing `easyregister-pc2` network, a read-only root filesystem,
temporary `/tmp`, no host mounts, and no extra capabilities.

No diagnostic containers remained afterward. The IDs, image IDs, startup
timestamps, and running states of all four business containers matched their
pre-test snapshots. The installed diagnostic in the original business image
remains unchanged; the corrected default entry is in the dedicated image above.

Packaging and validation source:
[`scripts/deploy-pc2-dst-diagnostic.py`](../scripts/deploy-pc2-dst-diagnostic.py).
It checks the base and flow hashes, refuses an existing candidate tag or occupied
diagnostic container name, verifies image contents, and stops at the first failed
run. The recorded invocation was:

```text
rtk proxy python scripts/deploy-pc2-dst-diagnostic.py
```

The script is a guarded one-time packaging operation for this immutable tag;
rerunning it now will refuse the existing tag. Python compilation and the actual
packaging/acceptance invocation passed. The 17 focused product tests recorded
below remain applicable because the diagnostic module did not change during
packaging. No new Git commit or tag was created in this continuation.

Full redacted evidence:
[`pc2-dst-diagnostic-image-20260908-001.json`](../deploy-evidence/pc2-dst-diagnostic-image-20260908-001.json).

## Earlier business observations

At 2026-09-08 15:05:31 UTC, six tasks had completed since the preceding
13:39:11 UTC evidence checkpoint:

| Tasks | Result |
| --- | --- |
| 131, 132, 134 | `otp_timeout` during account creation |
| 133, 135, 136 | Completed account/organization/login milestone, then `sms_not_enabled_for_business` |
| All six | Mailbox and proxy release steps passed |
| Complete successful tasks | 0 |

The dashboard counted 14 pending files. As established by the earlier audit,
that field counts files, including early seeds and enriched versions. It does
not establish 14 successful accounts. This continuation did not reauthenticate
those artifacts or treat their count as a full-success metric.

The NAS email log check at 15:07:59 UTC covered the period after 13:39:11:
three `ECONNRESET` retries, eight `UND_ERR_CONNECT_TIMEOUT` retries, and zero
`easy_email_http_failure` events. The existing bounded transport recovery
remains in use. This aggregate result does not prove third-party OTP delivery.

## Regression proof

Changed files:

- `server/services/orchestration_service/src/nas_dst_smoke.py`
- `tests/test_nas_dst_smoke.py`

The new regression first failed with the same preflight boundary:
`run_dst_flow_once` was called zero times with canonical-only credentials.
The pre-fix run was 1 failed, 2 passed.

After the fix, these focused suites passed: **17 tests**.

```text
tests/test_nas_dst_smoke.py
tests/test_security_defaults.py
tests/test_easy_email_client_provider_probe.py
```

The new test covers canonical-only credentials, alias-only credentials,
whitespace in the canonical field, canonical precedence when both are set,
and omission of fixture keys from the diagnostic output. The existing test
still proves that truly missing credentials cause no DST or network work.

## Real dependency DST

The corrected source was sent through SSH stdin and executed in the existing
`easy-register` container with the deployed module path as `__file__`.
The existing production DST dispatcher, dependency clients, environment and
flow file were used. This was a one-shot diagnostic process; no registration
worker was started.

Source SHA-256:
`1dd44de88d52ffbdcb7c5a8c912399c639efc742f66b2f5e5dbc79fe9b2f3149`.

| Step | Result |
| --- | --- |
| acquire-proxy-chain | ok |
| acquire-mailbox | ok |
| recover-mailbox | ok |
| release-recovered-mailbox | ok |
| release-mailbox | ok |
| release-proxy-chain | ok |

Exit code was 0, task attempts was 1, recovered identity was preserved,
a real lease was used, and independently checked cleanup errors were empty.
The proxy probe targeted `https://example.com/`. No account registration,
SMS purchase, or account verification was performed by this diagnostic.

At this earlier one-shot checkpoint, the running image and its installed
diagnostic module were not replaced. The dedicated image described above now
packages the fixed module; the original running image still has the old
preflight order. The existing Git checkpoint tags remain unchanged.

## Remaining boundary

The current later-stage stop is explicit: the business SMS policy is disabled,
and `open_phone_session_for_business` raises
`sms_not_enabled_for_business` before opening a session. Retrying the same
disabled policy cannot establish complete OAuth success.

The cause of specific missing third-party email codes remains unresolved.
The earlier controlled same-domain mail test passed, so it would be inaccurate
to infer either a general email outage or guaranteed third-party delivery.

Further work can validate self-owned infrastructure, resource cleanup, and
truthful success/error contracts. Stabilizing bulk third-party registration
through disposable verification services, proxy rotation or verification
circumvention is outside the work performed here. Required account checks
must use the provider's official process and user-controlled credentials and
verification methods. This report does not claim full DST stability.

See [the prior DST evidence](pc2-dst-validation-2026-09-08.md) and
[the rollback checkpoint](pc2-git-checkpoint-2026-09-08.md).
