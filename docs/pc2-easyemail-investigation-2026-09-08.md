# PC2 EasyEmail investigation, 2026-09-08

The subsequent message-ID collision repair, current deployed image, disk-state
verification, and remaining business failures are recorded in
[the 2026-09-11 follow-up](pc2-easyemail-message-identity-2026-09-11.md).

## Latest outcome: transport recovery deployed at 11:50 UTC

New production diagnostics identified NAS-to-upstream connection timeouts and
a socket failure behind intermittent mailbox-open HTTP 500 responses. A bounded,
Cloudflare-only recovery patch is now deployed. The candidate passed fault
injection, and a fresh self-owned mail lifecycle passed through both actual PC2
caller containers. Details and remaining limits are in the final section.
Earlier sections below preserve the chronological investigation; their statements
that the intermittent failure was unidentified describe the earlier checkpoints.

## Current verification

- SSH to `mjc@192.168.15.104` succeeded; the hostname is `pc2`.
- `easy-register` is running on this Linux host. Historical Windows PC2
  deployment instructions do not describe the current deployment.
- At `2026-09-08T04:27:06Z`, a read-only probe from inside `easy-register`
  used its existing mailbox URL and API key without printing the key.
- `GET /mail/catalog` without authentication returned HTTP 401.
- The same request with the configured authentication returned HTTP 200 and
  a JSON `catalog` object.
- These results verify current HTTP reachability and catalog authentication.
  They do not verify incoming delivery, message storage, or message extraction.

## Observed failures

An inspection of the preceding five hours of container logs found 61 completed
task records. Their terminal error codes were:

| Error code | Count |
| --- | ---: |
| mailbox_unavailable | 6 |
| proxy_connect_failed | 2 |
| existing_account_detected | 36 |
| authorize_missing_login_session | 2 |
| authorize_continue_blocked | 11 |
| user_register_400 | 1 |
| otp_timeout | 3 |

Explicit mailbox step errors included HTTP 500 with `fetch failed` at
`00:30:12Z`, `00:35:54Z`, `00:44:54Z`, `00:52:40Z`, and `00:55:34Z`.
Some cleanup failures had no HTTP status. Task terminal codes and individual
step errors are different measurements; their counts must not be equated.

Recent inspected task records included successful mailbox acquisition and
release alongside failures in subsequent third-party authentication steps.
An OTP timeout alone does not identify a fault in the email service.

The earlier HTTP 500 responses establish a service-side error response at the
mailbox API boundary. The text `fetch failed` does not identify the underlying
DNS, TLS, proxy, network, or upstream-provider cause. Server-side evidence is
required before selecting a fix.

## Initial server access blocker (resolved below)

The EasyEmail repository's `docs/synology-nas-service-runbook.md` identifies the
service as NAS container `easyemail-sdk`, served at
`http://192.168.15.200:18081`, with deployment files below
`/volume1/docker/easyemail-sdk`.

A read-only SSH attempt to `mjc@192.168.15.200`, using the existing
`C:/Users/vmjcv/.ssh/id_ed25519` key and strict host-key checking, failed with
`Permission denied (publickey,password)`. The remote Docker inspection command
therefore did not execute. The NAS container name, image, and runtime state
remain unverified in this investigation.

The local Windows Docker instance has a different container,
`easy-email-localproof003`; it must not be treated as the NAS deployment.

## Initial result and next evidence

No application code, deployment configuration, containers, or mailbox data were
modified. The reported fault is not yet repaired, and its underlying cause is
not yet established.

The next required input is a working NAS SSH connection method, or sanitized
EasyEmail server logs for `2026-09-08T00:25:00Z` through `01:00:00Z`, including
the nested causes of the HTTP 500 / fetch failures. Do not include API keys,
mailbox references, recovery credentials, message bodies, or verification codes.
Any delivery check should use an independently owned mailbox and a plain test
message, without running third-party registration or verification workflows.

## Follow-up after NAS access was supplied

Password authentication succeeded for `mjc@192.168.15.200`; no other supplied
account was tried after success. Credentials were used in memory and are not
included in this record. The remote hostname is `mjc_nas`.

Live Docker inspection confirmed:

- Container: `easyemail-sdk`, running, restart count `0`.
- Started: `2026-09-07T16:02:44.347071826Z`.
- Image: `easyemail/easy-email-service:nas-20260907-002`.
- Image ID: `sha256:9ac542fb19fc3dcd3ee442385dfd71db3d7d5f82a6c88465ea7ae9a8a18a2c19`.
- Persistent config and data mounts match the NAS runbook.
- Docker logging uses `json-file`, `max-size=20m`, `max-file=5`.

There were no application log entries in the historical failure window.
Inspection of the last 2,000 log lines returned only a few startup/blank lines,
with no fetch, maintenance, or persistence failure records. Source inspection
explains this limitation: `service/base/src/http/server.ts:395-400` returns
HTTP 500 without logging the original exception or its nested cause.

Three unauthenticated origin requests from inside the NAS container to the
configured upstream `mail.aiaimimi.com` returned HTTP 200. DNS returned IPv4
and IPv6 addresses. These origin checks do not prove every upstream operation
will succeed.

### Controlled message checks

Only newly created diagnostic mailboxes were used. No third-party registration
or authentication was performed. Each acquired diagnostic mailbox was passed
to release in a finally block.

The first isolated send attempt returned HTTP 500. Its response body was not
retained, so it cannot be attributed to the historical `fetch failed` error.
Subsequent sends succeeded using the upstream `admin_delegate` sending path.

A plain message without a synthetic code was not exposed by the observation
API during the test. Source inspection found that this provider's refresh path
uses a code-oriented reader and skips messages without an extracted code:
`service/base/src/providers/cloudflare_temp_email/connector/client.ts:446-447`
and `service/base/src/service/message-operations.ts:183-189`.
This API behavior must not be used to infer whether a plain message was
delivered to the upstream inbox.

Further tests sent a self-owned fixture with an arbitrary synthetic code and
a unique body marker. Direct upstream inbox inspection and service storage
inspection established the following final result:

| Check | Result |
| --- | --- |
| Sender and recipient creation | Passed |
| Send request | Accepted, `admin_delegate` |
| Direct upstream inbox query | HTTP 200, unique marker found |
| Service refresh | Succeeded |
| In-memory observed message detail | Present, body marker matched |
| Synthetic fixture code extraction | Matched |
| Stored observed-message query | One message, marker matched |
| Both diagnostic sessions closed | Passed; `expired` or `resolved` |
| Both upstream mailboxes released | Passed |

The exact original subject marker did not survive in the observed subject,
while the unique body marker did. Earlier probes that required a subject-only
match therefore reported a false negative. The final check used the unique
body marker and compared the upstream inbox with both service detail and list
responses. The subject discrepancy itself was not diagnosed or changed.

### Status before diagnostic instrumentation

NAS access is no longer blocked. Current controlled sending, receiving,
extraction, storage, and release checks passed. This does not prove that
intermittent HTTP failures are eliminated, and the first isolated send also
demonstrated that a 500 can still occur.

No production code, configuration, images, or containers were changed or
restarted. No underlying HTTP 500 fix has been delivered. The remaining
unresolved issue is the intermittent failure: existing server logs omit its
cause, and successful subsequent operations do not establish that cause.
Diagnosing it requires capturing the next failing upstream request's sanitized
nested error and endpoint category. Blind retries of create/send requests
could duplicate side effects and were not added.

## Diagnostic fix deployed

The missing server-side failure diagnostics have now been fixed. This is an
observability fix, not a claim that the intermittent upstream failure itself
has been eliminated.

Source changes in the sibling EasyEmail repository:

- `service/base/src/http/failure-diagnostic.ts`: emits only allowlisted error
  names and network codes, a `fetchFailed` boolean, and normalized route/method
  categories. Traversal is bounded and handles nested causes and aggregates.
- `service/base/src/http/server.ts`: reports both generic and domain HTTP 500
  failures before returning the existing response. Logging failures are caught
  so they cannot prevent the original HTTP response.
- `service/base/tests/http/failure-diagnostic.test.ts`: regression coverage for
  nested connection failures, secret/path redaction, cyclic/large aggregates,
  throwing properties, revoked proxies, and a failed logging sink.

No credentials, email content, session identifiers, URLs, or raw stacks are
included in the new diagnostic records.

### Deployment scope and verification

The NAS now runs:

- Image tag: `easyemail/easy-email-service:nas-http-diagnostics-20260908-002`.
- Image ID: `sha256:47b0982593811d9ce384b0dcb68bddc34213276f428222a9f2230d9039abbc72`.
- Compiled server SHA-256: `857299a8d757e739929bab1e9908f4a6b9c366ff5c32f9a18592be5b383e2284`.
- Diagnostic helper SHA-256: `58398fcf77bf8ccab84c681c9c2c56362bc056d8ebbeb30617d01097a4ad9ebd`.

The derived image overlays only the deployed HTTP server's diagnostic calls
and the new helper. It does not deploy unrelated dirty workspace changes.
`scripts/deploy-nas-email-diagnostics.py` validates the exact base image and
patch locations, builds an image, tests it without external networking, and
updates only the image pin for the existing `easy-email` Compose service.
Application configuration and persistent state were retained.

Validation completed:

- HTTP tests: 6 files, 17 tests passed.
- Effective-line checker tests: 19 passed.
- Official TypeScript build passed; the earlier explicit typecheck also passed.
- Effective-line ratchet passed: 1,033 files; no files in any tier above 500
  effective lines. This is a ratchet result, not a claimed strict-mode run.
- Candidate-image HTTP smoke passed: an injected synthetic connection reset
  produced a redacted diagnostic and the unchanged HTTP 500 response; a
  throwing logger also preserved the HTTP 500 response.
- Post-deployment controlled mail test passed: open, send, upstream receive,
  refresh, detail/body matching, synthetic fixture extraction, stored-message
  query, and closure/upstream release of both test mailboxes.
- Post-deployment PC2 container catalog checks: unauthenticated HTTP 401;
  authenticated HTTP 200 with a `catalog` object.
- Scoped `git diff --check` passed in both repositories; modified/new files
  were checked for absence of UTF-8 BOM.

No real HTTP failure diagnostic occurred during the final live check. The
intermittent upstream failure's cause therefore remains unresolved. A future
HTTP 500 can now expose safe nested codes such as `ECONNRESET`, `ENOTFOUND`, or
`UND_ERR_CONNECT_TIMEOUT`, rather than losing that evidence entirely.

### Rollback

The pre-instrumentation image pin is preserved at:

`/volume1/docker/easyemail-sdk/deploy/service.env.before-http-diagnostics-20260908-001`

To restore the original image, copy that protected backup over `service.env`
and run the existing Compose command with `up -d --no-build --no-deps easy-email`.
The `...20260908-002` backup instead points to the first diagnostic image.
Do not remove volumes or reset persisted state.

## Continuation: current caller boundary verified

The diagnostic image was rechecked after the request to continue. It was
running with restart count zero, started at `2026-09-08T05:14:23.417517039Z`.
No `easy_email_http_failure` records were present in the inspected log window.

Four additional isolated lifecycle runs all passed:

- Four upstream inbox marker matches.
- Four stored-message marker matches.
- Four synthetic fixture extraction matches.
- Eight successful diagnostic mailbox closures and upstream releases.
- No HTTP errors and no new server-side failure diagnostics.

An additional cross-container test then created a separate controlled message
and queried its `/mail/mailboxes/:sessionId/code` endpoint from both actual
PC2 callers, using each container's existing environment:

| Container | HTTP status | Synthetic fixture matched |
| --- | ---: | --- |
| `easy-register` | 200 | Yes |
| `easy-register-protocol-python` | 200 | Yes |

Both returned a `code` object with the expected fields: `sessionId`,
`providerInstanceId`, `code`, `source`, `observedMessageId`, and `receivedAt`.
The two caller containers also have the same configured mailbox credential
and NAS host. Credential equality was checked without outputting the value.
The two additional diagnostic mailboxes were closed and released afterward.

### Current task failures are at a different boundary

A read-only inspection of the latest 30-minute application-log window found
nine completed task records and zero mailbox step errors. The latest records
were:

| Task | Completed UTC | Error code | Mailbox acquire / release |
| --- | --- | --- | --- |
| 77 | 05:14:41 | `authorize_continue_blocked` | Both OK |
| 78 | 05:18:06 | `authorize_missing_login_session` | Both OK |
| 79 | 05:20:23 | `authorize_continue_blocked` | Both OK |
| 80 | 05:23:43 | `authorize_continue_blocked` | Both OK |
| 81 | 05:27:02 | `authorize_continue_blocked` | Both OK |
| 82 | 05:30:27 | `authorize_missing_login_session` | Both OK |

All six listed failures occurred at `create-openai-account`. The evidence
locates their current terminal failure in third-party authorization, while
the controlled EasyEmail API path works from both PC2 callers. No registration
or authorization workflow was changed or started as part of these tests.

No further production changes were made in this continuation: no currently
reproduced EasyEmail fault supports another code or configuration change.
The historical intermittent 500 still has no demonstrated underlying cause;
this must not be reported as conclusively fixed. A current failing request,
timestamp, or new diagnostic event is needed to investigate that intermittent
fault further. Successful controlled tests do not establish what happened to
any specific third-party email.

## Production cause and deployed recovery, 11:50 UTC continuation

The previously deployed safe diagnostics captured these real failures on
`POST /mail/mailboxes/open` (all timestamps UTC, 2026-09-08):

| Timestamp | Diagnostic evidence |
| --- | --- |
| 06:26:16.131714215 | `TypeError`, `fetchFailed: true`, cause `UND_ERR_CONNECT_TIMEOUT` |
| 08:06:45.596900657 | `TypeError`, `fetchFailed: true`, cause `UND_ERR_CONNECT_TIMEOUT` |
| 10:19:33.347825117 | Plain `Error`, no classified network code |
| 11:07:13.180747734 | `TypeError`, `fetchFailed: true`, cause `UND_ERR_SOCKET` |

This locates the demonstrated transport failure at the NAS EasyEmail service's
connection to its Cloudflare mail upstream, `mail.aiaimimi.com`. The connector
propagated a transient connection failure immediately, and the HTTP server
returned 500 to PC2. Authentication and successful controlled calls from PC2
were already verified. The evidence does not distinguish packet loss, routing,
or upstream availability as the underlying cause of the network interruption.

The deployed runtime is Node 22.23.1 with Undici 6.27.0. In that version,
`UND_ERR_CONNECT_TIMEOUT` is raised before the connection callback succeeds;
the timeout is cleared on `connect`/`secureConnect`. This supports retrying a
replayable request after that specific failure, including a POST whose HTTP
request has not been sent. See the primary
[Undici 6.27.0 connection implementation](https://raw.githubusercontent.com/nodejs/undici/v6.27.0/lib/core/connect.js).

### Narrow repair

`EasyEmail/service/base/src/providers/http-policy.ts` adds an opt-in
`fetchProviderResourceWithRecovery` helper. Only the Cloudflare connector's
ordinary requests, admin requests, and direct admin-send request opt in.
The original default transport and other providers keep their previous behavior.

- Retry at most once after `UND_ERR_CONNECT_TIMEOUT`, with a string or absent body.
- For GET/HEAD only, also retry `UND_ERR_SOCKET` or `ECONNRESET` once.
- Preserve the same AbortSignal, body, headers, redirect policy, and total deadline.
- Do not replay POST/PUT/PATCH/DELETE after a socket failure; delivery may have
  happened already. Do not retry HTTP status failures, TLS/unclassified errors,
  stream bodies, or an already-aborted request.
- Emit only allowlisted method, network code, and attempt number in the recovery log.

HTTP failure diagnostics additionally extract an upstream status number or a
connector-timeout flag from a capped message prefix. No raw error message,
response body, URL, credential, or mailbox identifier is logged. This improves
future classification of plain errors; the historical 10:19 error remains
unclassified and cannot be retroactively explained by the new fields.

### Verification and deployment

The missing-recovery regression tests first failed against a delegating helper
(4 failed, 10 passed). After implementation, the focused provider, Cloudflare,
and HTTP diagnostic suites passed: **4 test files, 36 tests**. TypeScript build
passed. The effective-line checker tests passed (19 tests), and the final ratchet
passed across 1,033 files with none above 500 effective lines. Scoped diff checks
passed, and changed source files were confirmed UTF-8 without BOM.

The deployment script is
`EasyRegister/scripts/deploy-nas-email-transport-recovery.py`. It pins and checks
the exact prior image, overlays four compiled modules, tests the candidate before
promotion, saves the prior image environment, and rolls back if startup or
authenticated health validation fails. It does not deploy the dirty sibling
repository wholesale. Its initial prefix guard stopped safely on the unchanged
compiled source-map footer; the guard was corrected to compare the implementation
prefix, then deployment proceeded.

Candidate-container fault injection on the real deployed Node runtime verified:

- Cloudflare POST connect timeout recovers on attempt 2 with the same signal/body.
- Cloudflare POST socket failure is attempted once.
- A real loopback server dropping an accepted request sees two GET requests and
  recovery, but exactly one POST request and a surfaced failure.
- Authenticated `/mail/catalog` succeeds after promotion.

Deployed image:
`easyemail/easy-email-service:nas-transport-recovery-20260908-001`.
Image ID: `sha256:0362e4a98c9e66afa925c0300582c62c5fc3200b73c68ac9037e977d7e2ee8c5`.
Container started at `2026-09-08T11:50:02.977342892Z`; a fresh inspection confirmed
running state and restart count 0. Live file hashes matched the candidate:

| Compiled module | SHA-256 |
| --- | --- |
| `providers/http-policy.js` | `4583b1cded4ece7b98a25344badebf0750f5ca6b3de081f072249f74e0949852` |
| `http/failure-diagnostic.js` | `c591575b5afeb0ed71d41b4133e337f630329e8b91b2bd46966ef074e8a24a9a` |
| `providers/cloudflare_temp_email/connector/http.js` | `31169107f65b1c4caeddc13f8d625176bfdb8230dddf08f49c55cc92a9f47f2b` |
| `providers/cloudflare_temp_email/connector/client.js` | `fde6b080c25901ef14dcec35e78d9f7684421cbf490e499d5d2af85bb6462527` |

Rollback environment:
`/volume1/docker/easyemail-sdk/deploy/service.env.before-transport-recovery-20260908-001`.
This restores the prior diagnostics-002 image when copied back to `service.env`
and applied with the existing compose project's `easy-email` service.

A new self-owned synthetic fixture then passed the live path: open sender and
recipient, send with `admin_delegate`, receive from the upstream with HTTP 200,
refresh, persist one matching message, and extract the expected synthetic code.
Both `easy-register` and `easy-register-protocol-python` on PC2 fetched that same
fixture through `/mail/mailboxes/:sessionId/code`, returned HTTP 200, and matched
the expected code. Both temporary sessions were closed and released successfully.
The body marker matched; the earlier subject-only matching caveat remains.

No recovery or HTTP-failure diagnostic events were present in the short
post-deployment observation window. Recovery behavior was proved with candidate
fault injection, not by claiming a naturally recurring failure during that window.

### Remaining limits

The safe recovery gap for the observed connection timeout is fixed and deployed.
Persistent upstream outages can still exhaust the single retry or total deadline.
POST socket failures and failures while consuming a response body are not blindly
retried, because replaying a completed write could duplicate its side effects.
The historical unclassified error still lacks sufficient evidence. This repair
does not establish that every prior 500 or third-party mail delay is resolved.
Third-party authorization failures in the earlier task table remain outside this
email transport fix; no registration or authorization workflow was modified.
