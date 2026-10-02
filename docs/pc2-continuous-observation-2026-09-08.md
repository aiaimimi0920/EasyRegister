# PC2 continuous observation and migration audit - 2026-09-08

> Historical observation checkpoint. The later discovery and correction of
> bundled-executor misrouting are documented in
> `pc2-external-provider-fix-2026-09-08.md`. The earlier echo checks below did
> not establish which executor handled registration.

## Answer and current operation

EasyRegister is now running continuously on PC2 at the user's request. This is
an observation deployment, not a successful registration release.

- Host: `mjc@192.168.15.104`, Linux `pc2`.
- Root: `/home/mjc/easyregister`.
- Started: `2026-09-08T00:27:46Z` (08:27, Asia/Shanghai).
- Result snapshot: `2026-09-08T01:48:00Z` (09:48, Asia/Shanghai).
- Interface recheck: `2026-09-08T01:50:04Z` (09:50, Asia/Shanghai).
- One worker, main concurrency one, 120 seconds between completed runs.
- No run-count ceiling; restart policy `unless-stopped`; zero register restarts
  at the snapshot. Existing circuit breakers and step recovery remain enabled.
- Paid SMS remains disabled. No probe acceptance or TLS setting was loosened.
- Dashboard: PC2 loopback `http://127.0.0.1:19790/api/status`, HTTP 200 with a JSON
  object at the interface recheck. It is not exposed on the LAN.

## What the repeated trials actually showed

The snapshot contains **26 started tasks, 25 completed tasks, zero successful
tasks, and zero successful account-creation steps**. The last task was still in
progress. Completed tasks made 53 account-creation step attempts in total.
The original bounded canary is not included in these counts.

| Terminal failure class | Completed tasks |
| --- | ---: |
| `existing_account_detected`, email OTP page | 14 |
| EasyEmail HTTP 500 / fetch failure | 6 |
| `authorize_init_missing_login_session` | 2 |
| Platform login HTTP 403 | 1 |
| Proxy TLS connection error | 1 |
| Proxy connection timeout | 1 |
| Total | 25 |

These are terminal task classifications, not counts of every intermediate
error. Retry recovery can replace the original error with a mailbox acquisition
error while retaining `create-openai-account` as the failing step. Therefore,
the error text and step history must be interpreted together.

- Terminal step counts: account creation 19, proxy acquisition 2, mailbox
  acquisition 4.
- All completed tasks skipped the downstream organization, ChatGPT login and
  Codex OAuth steps. None established the small-success milestone.
- Seven completed tasks reported mailbox-release failure. No proxy-release
  failure was reported. These cleanup failures remain an operational risk;
  existing mailboxes and unrelated service data were not bulk-deleted.
- HTTP 403 is **not** the sole or dominant failure in this larger sample.

## Did the migration introduce a flow problem?

### No changed step order or request adapter was found

The current semantic flow and EasyProtocol request adapter are unchanged from
the preserved baseline commit `0fe13d383e9459b04862fc03b689f36ffaed7435`, allowing
for CRLF/LF representation:

- `server/services/orchestration_service/flows/codex-openai-account-v1.semantic-flow.json`
- `server/services/orchestration_service/src/others/easyprotocol_runtime.py`

The creation step still receives the preallocated mailbox identifiers,
recovery credential and proxy URL. Five deployed EasyRegister source files
were hashed directly inside the running container and matched the local
worktree byte-for-byte. The exact hashes are in the evidence JSON.

The new EasyEmail and EasyProxy endpoint/authentication contracts passed live
checks. Remote `mixed://` proxy URLs are normalized to `http://` while retaining
the remote authority. The flow's proxy probe targets
`auth.openai.com/log-in-or-create-account`, whereas the provider's failing
platform-login request targets `platform.openai.com/login`. A successful probe
of the former never established successful login at the latter.

These observations do not prove every email/OTP or proxy operation is correct.
In particular, a healthy catalog is compatible with failing mailbox operations.

### A deployment-version alignment gap is confirmed

The EasyRegister image is `easy-register/easy-register:nas-dst-20260908-003`, but
the deployed PythonProtocol dependency is still the historical image
`ghcr.io/aiaimimi0920/easy-protocol-python:pc2-phone-submit-repair-20260815-001`.

Actual deployed file:
`/app/src/new_protocol_register/protocol_small_success.py`.
Raw-byte SHA256:
`3b1230f9f4b5f8c83223671315b6e383feff7725f4e7b3620e5babbda94381a6`.

The deployed source was read directly, not inferred from its image tag:

1. Lines 1589-1594 immediately classify `email_otp_send` and
   `email_otp_verification` as `existing_account_detected` and raise an error.
   This matches the dominant terminal error in the trials.
2. It has no `_prepare_passwordless_email_otp` helper. The current local
   EasyProtocol source instead calls that helper at lines 1888-1896 and follows
   a passwordless OTP path.
3. It has no `_open_platform_login_with_browser_retry` helper, which exists in
   current local source. The deployed platform-login GET requires HTTP 200.

Current local provider file SHA256:
`b28d73a73e669b8a6d62465dc3afd7630ca1c80fa1870ec6fb2b61af56082dd3`.

**The earlier deployment validation was insufficient:** service health and a
safe protocol echo did not verify provider/source version alignment or complete
registration. It is incorrect to claim the business migration is finished.
The confirmed issue is a stale deployed dependency and an unclosed acceptance
gate, not evidence that the EasyRegister flow steps were reordered incorrectly.

An email OTP page by itself does not establish whether an account really
already existed. The exact reasons for HTTP 403, missing login sessions and
EasyEmail HTTP 500 remain unproven. Updating the provider may address the known
behavior difference, but successful registration after such an update has not
been demonstrated. No untested provider hotpatch was applied in this run.

## Verification and isolation

- Fresh focused migration regression run: **89 passed, 0 failed, 3.45 seconds**.
- `git diff --check`: passed; Git emitted line-ending conversion warnings only.
- Effective three-file Compose configuration passed `config --quiet` on PC2.
- Ten fresh authenticated/unauthenticated dependency checks and safe provider
  echo passed. These checks do not create accounts or purchase SMS.
- The register and Python provider were sampled at approximately 86.87 MiB and
  39.59 MiB RAM, respectively; register CPU was 0.52% in that single sample.
  This is a point-in-time measurement, not a performance guarantee.
- Only `easy-register` was recreated by this turn's deployment command.
  No dependency container was recreated and no sibling source was edited.
- Seven of nine unrelated containers retained their original ID/start time.
  Two unrelated containers (`fapaifang-pc2-seed-1` and
  `fapaifang-pc2-browser-solver`) were recreated during the observation window,
  outside this turn's commands. Eight were healthy at the JSON snapshot; a
  subsequent read-only check found all nine healthy. Both changed containers
  reported `OOMKilled=false`. Do not describe all nine as unchanged.

## Operation, stopping and next acceptance gate

Continuous operation uses these three Compose files, in this order:

```sh
cd /home/mjc/easyregister
docker compose --project-name easy-register --env-file runtime.env \
  -f compose.yaml -f canary.yaml -f observation.yaml \
  up -d --no-build --no-deps easy-register
```

The observation override is tracked locally as
`compose/docker-compose.pc2-observation.yaml`. The bounded canary override is
preserved. Original canary logs were saved remotely with mode 0600 before the
register container was recreated; they may contain credentials and must not be
published.

To stop only this registration workload, without deleting any state:

```sh
docker stop easy-register
```

The next release gate is a separately tested, version-aligned EasyProtocol
artifact followed by a limited end-to-end canary. It must verify the mailbox
OTP path, small-success artifact, cleanup and final OAuth separately. Do not
substitute more retries, enabled paid SMS or relaxed acceptance for that proof.

Evidence: `deploy-evidence/pc2-observation-20260908-001.json`. Counts in this
report are a fixed snapshot; the service remains running and later totals will
increase. This is not a promise of ongoing interactive monitoring after the
assistant turn ends.
