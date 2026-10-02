# PC2 migration configuration fix and real OTP progress

## Latest terminal result

At `2026-09-08T03:26:57Z` (11:26, Asia/Shanghai), a fresh observation found
task 50 had finished at `03:25:53Z` with `ok: false`. Its terminal error was
`create-openai-account: otp_timeout`, after three recorded step attempts.
Proxy and mailbox release both succeeded; downstream initialization and OAuth
steps were skipped. No small success or complete registration was established.
The register and candidate provider remained running with zero restarts, and
the register was not paused. This supersedes the in-progress status below.

Final observation: `deploy-evidence/pc2-otp-terminal-20260908-001.json`.

## Earlier progress snapshot

**A real migration/deployment configuration error was found and fixed.** The
error was in executor selection, not in the semantic-flow step order. Earlier
health and echo checks were insufficient and gave an incomplete picture of
which Python executor handled registration.

At `2026-09-08T03:21:21Z` (11:21, Asia/Shanghai):

- EasyRegister remains running, not paused, with the same container ID and
  zero restarts since continuous observation started.
- One worker, 120-second inter-task delay and paid-SMS prohibition remain.
- The gateway now routes to the independently deployed Python executor.
- The first actually routed task is task 50, started at
  `2026-09-08T03:08:34Z`. It was still in progress at the snapshot.
- The new passwordless OTP branch was observed twice in actual provider logs.
  It no longer immediately rejects that branch as `existing_account_detected`.
- A read-only check of the owned mailbox's code endpoint returned HTTP 200
  with an empty JSON object and no OTP. This does not establish whether the
  upstream message was sent, delivered or delayed.
- **No small-success artifact, newly created account or completed registration
  has been proven.** Entering an OTP page is not successful registration.

## Root cause: disabling one pool enabled the other

The original PC2 preparation script set:

```yaml
managed_provider_runtime:
  enabled: false
provider_pool:
  providers:
    python:
      warm_replicas: 1
      max_replicas: 1
```

In the actual gateway implementation, disabling the Docker-managed pool causes
the entrypoint to try `NewProviderProcessPool`. The remaining Python provider
entry therefore starts bundled Python children and replaces the registry's
external service endpoint with a local child-process endpoint.

Source anchors in EasyProtocol:

- `service/base/cmd/easy_protocol/main.go:30-45`: container-pool/process-pool fallback.
- `service/base/services/provider_process_pool.go:76-102`: process-family creation
  from `cfg.ProviderPool.Providers`.
- `service/base/config/config.go:139-173`: YAML unmarshalling over populated defaults.

Fresh runtime evidence made the misrouting concrete:

| Check before the routing fix | Observed executor |
| --- | --- |
| Direct independent provider `health.inspect` | `0.0.0.0:9100`, max tasks per worker 50 |
| Same operation through the gateway | `127.0.0.1:41509`, max tasks per worker 1000 |
| Gateway process list | Bundled Python processes present |
| Registration errors after replacing the independent provider | Still from the old worker |

Thus replacing the standalone provider image alone was not enough. Tasks 44-49
still used the gateway's old executor; those outcomes must not be described as
validation of the candidate code.

## Applied configuration correction

The gateway now has:

```yaml
managed_provider_runtime:
  enabled: false
provider_pool:
  providers: null
```

YAML `null` is deliberate: an empty mapping does not clear the default Go map.
The same configuration was tested using the actual gateway image in an isolated
local container, which started without any bundled Python processes. The test
container was removed afterward.

Only the `provider_pool` top-level configuration value changed on PC2.
Authentication/control-plane configuration and the declared service registry
were compared with the backup and remained identical. No Docker socket was
added, and no security or HTTP acceptance gate was relaxed.

`scripts/prepare-pc2-dependencies.py` now generates the corrected configuration.
`scripts/verify-pc2-runtime.py` now also compares the direct and gateway-routed
executor identities. The new check failed before the fix while the preceding
ten checks passed; all eleven checks passed afterward.

## Narrow provider backport

Instead of copying the dirty EasyProtocol worktree, the candidate was built
from the verified historical image with only normal passwordless OTP support:

- `email_otp_verification`: do not resend an already active code.
- `email_otp_send`: request the code exactly once.
- Both paths still wait for and validate an OTP, then require the account-create
  call to succeed before the mocked success path can produce an artifact.
- The ordinary password-registration path is retained.
- Platform HTTP 403 still fails. Browser recovery and unrelated current-source
  changes were not included.

Candidate image on PC2:
`easy-register/easy-protocol-python:pc2-passwordless-otp-20260908-001`.
Image ID:
`sha256:e24b629a8d9f60d705e676d2234023891df7212131e8043006f0b2e5a263dfe1`.
Deployed provider source SHA256:
`03f0b649f68c803f81ff1815629ee4839be95ee8af3075c17d0fe56fa218b5f8`.

AST comparison found only one changed function,
`run_protocol_small_success_once`, and one added helper,
`_prepare_passwordless_email_otp`; other top-level code was unchanged.

An OTP page can be part of signup or an existing-account login. The candidate
does not treat the page itself as proof of a new account. Actual account origin,
successful creation and artifact contents still require live verification.
The mock tests do not establish those real-world facts.

## Verification and rollout boundaries

- Final provider suite on the original image: 2 passed, 4 failed, 2 errors,
  reproducing rejection of the normal OTP path.
- Same final suite on the candidate: 8 passed locally and 8 passed on PC2,
  with networking disabled for these tests.
- Fresh migration/configuration suite: **96 passed, 0 failed, 2.38 seconds**.
- Source compilation, baseline fingerprint rejection, UTF-8/no-BOM, whitespace
  checks and `git diff --check` passed.
- PC2 Buildx preparation stalled; its specific candidate-build client was
  terminated, and the existing Docker legacy builder completed the offline
  layer build. No dependency download or production-container change was needed
  for this build fallback.
- Provider replacement followed completed task 43 and used about 26 seconds
  of idle-boundary pause. Gateway correction followed completed task 49 and
  used about 76 seconds. Both resumed registration successfully.
- Only the standalone provider was recreated and only this project's gateway
  was restarted. EasyRegister's container identity was preserved. No commands
  restarted unrelated applications or edited sibling-project source.
- Automatic rollback paths and private backups exist, but failure-injection
  testing of rollback/SIGTERM handling was not performed.

## Remaining gate and operation

The remaining observed gate is the real mailbox OTP/registration path, not the
now-corrected executor selection. Logs reached the passwordless branch, but
the code endpoint was empty at the read-only probe and task 50 had not finished
at that earlier snapshot; the final observation above confirms OTP timeout.
Further rounds continue under the existing conservative
settings; later totals will differ from this fixed report.

Do not label a mocked result, HTTP 200, an OTP page, or a normal supervisor
exit as successful registration. Complete acceptance still requires the real
OTP result, account-creation result, small-success artifact, cleanup and final
OAuth validation to be checked separately.

Deployment recipes, tests and operation notes:
`deploy/pc2-protocol-otp/README.md`.
Evidence: `deploy-evidence/pc2-external-provider-otp-20260908-001.json`.
The same report and evidence are copied to `/home/mjc/easyregister` on PC2.

The old provider image and gateway configuration backup are preserved. For
dependency commands include both `dependencies.yaml` and `protocol-otp.yaml`;
omitting the latter can select the old provider image again. To stop only the
registration workload without deleting data, use `docker stop easy-register`.

Raw logs, runtime environment and configuration backups may contain secrets;
they are not included in the public evidence files. Continuous operation does
not imply continued interactive monitoring after the assistant turn ends.
