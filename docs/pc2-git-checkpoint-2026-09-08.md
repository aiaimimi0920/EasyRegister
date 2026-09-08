# PC2 EasyEmail Git checkpoint, 2026-09-08

## Local annotated tags

The checkpoint tag is `pc2-easyemail-recovery-20260908` in both independent
repositories. It is a local annotated tag; no remote push was requested.

- EasyEmail: commit `7779b078101ea2c807d7cf26280d5cedc222f32f`.
  Subject: `fix(email): recover transient upstream connections`.
  Parent: `937a196300aa910706bd491d86b9f38b522cad30`.
- EasyRegister: the commit containing this document, the two guarded NAS
  deployment helpers, the investigation and DST reports, and redacted evidence.
  Resolve its exact commit with the tag command below.

From either repository, inspect the checkpoint without changing the worktree:

```sh
git rev-list -n 1 pc2-easyemail-recovery-20260908
git show --stat pc2-easyemail-recovery-20260908
```

## Code scope

The EasyEmail commit contains seven source/test files: the opt-in transport
recovery helper, three Cloudflare call sites, redacted HTTP failure diagnostics,
minimal server wiring, and focused tests. Shared files were staged selectively;
their unrelated changes remain in the working tree.

The exact staged source tree was exported independently for validation:
`26adea1206cb41756397f011b92a030f85a0cd3c`.
Five relevant test files passed with 36 tests, and TypeScript build passed.
The effective-line checker tests passed 19/19; ratchet mode passed across
957 files with none above 500 effective lines. The first temporary export used
Windows CRLF conversion and exposed a pre-existing raw-policy-hash assumption
in a checker test. Exporting the same index with `core.autocrlf=false` restored
the committed LF bytes and passed; no checker or policy source was changed.

The EasyRegister commit records only this investigation's deployment helpers
and evidence. Older NAS migration changes, desktop work, generated artifacts,
temporary logs and secret-bearing configuration are not included. Both working
trees therefore still contain pre-existing uncommitted changes.

These tags preserve the scoped repair and its evidence. They do not claim to
capture every prior uncommitted source change present in the production images.
Checking out the EasyRegister tag alone does not reproduce its deployed
`nas-dst-20260908-003` image.

## Runtime rollback

Git operations do not replace running containers. The currently recorded
EasyEmail image is:

```text
easyemail/easy-email-service:nas-transport-recovery-20260908-001
sha256:0362e4a98c9e66afa925c0300582c62c5fc3200b73c68ac9037e977d7e2ee8c5
```

The NAS environment backup before transport recovery is:

```text
/volume1/docker/easyemail-sdk/deploy/service.env.before-transport-recovery-20260908-001
```

That backup pins `easyemail/easy-email-service:nas-http-diagnostics-20260908-002`.
Its image ID is
`sha256:47b0982593811d9ce384b0dcb68bddc34213276f428222a9f2230d9039abbc72`.
The existing deployment root and compose service are
`/volume1/docker/easyemail-sdk` and `easy-email`. Runtime rollback should use
those recorded image/configuration inputs while preserving data volumes.
No rollback or container recreation was performed for this Git checkpoint.

The checked-in deployment helpers are historical, exact-base guarded upgrades.
Do not rerun them as rollback commands against the already-upgraded service.

## Acceptance limits

The [DST report](pc2-dst-validation-2026-09-08.md) distinguishes four completed
historical small-success milestones from the failed observed task 130.
OTP nondelivery remains unresolved, and the later OAuth stage is constrained
by the intentionally disabled SMS business policy. These tags are not a claim
of full DST or OAuth success. See the
[investigation](pc2-easyemail-investigation-2026-09-08.md) and
[redacted evidence](../deploy-evidence/pc2-dst-validation-20260908-001.json).
