# PC2 paid SMS repair trace

> Deployment update, 2026-09-13: the later production review fixes and service
> restart are covered by the [deployment follow-up](pc2-review-deployment-2026-09-13.md).
> The isolated repair sequence and timestamps below are retained as history.

Latest ledger reconciliation: 2026-09-12 08:41:03 UTC.
Status: account creation remains blocked by observed HTTP 403 challenges;
a real large-success artifact has not been obtained.

The supplied key remains in the ignored sibling `EasySMS/config.yaml`, under
`serviceBase.runtime.providers.heroSms.apiKey`. This report does not contain
credentials, mailbox references, phone numbers, verification codes, or tokens.

## Required result and spending boundary

A large success requires completed Codex OAuth, successful
`validate-free-personal-oauth`, an identity-matching valid OAuth file on disk,
and reconciled SMS order and cost evidence. A process exit code or an HTTP 200
alone does not satisfy these requirements.

Only one purchase attempt is allowed by the original persistent guard ledger.
The approved service is OpenAI `dr`, country 16, with a 0.05 USD cap. There is no
SMS reuse, number replacement, or resend allowance. A failed paid execution
must be diagnosed, repaired, and verified before another execution is started.
The original guard has not been reset or replaced. No purchase ledger or
receipt existed at the latest purchase-boundary check in this repair sequence.
No paid SMS order has been created by these executions; task SMS spend remains
0 USD. The last provider balance/quote query was at 2026-09-12 08:41:03 UTC:
4.5953 USD balance, 0.045 USD quote, and zero active orders.

## Completed repairs and evidence

The Chromium authenticated-proxy extension was migrated to Manifest V3. The
browser now also receives an explicit proxy server. Private-mode requests use
an isolated temporary ordinary profile so the authentication extension can
run. Real Chromium offline integration tests passed for ordinary and private
profiles, including a proxy password containing reserved characters.

Platform authorization recovery now uses the actual browser navigation status,
the original OAuth transaction, a real login cookie, and an identity-matching
account page. Signed authentication-cookie metadata is decoded from its first
segment; the complete signed cookie remains unchanged in requests. Seed 006
created an account and initialized its Platform organization. Its valid seed,
created at 2026-09-11 23:27:14 UTC, was used while within the normal 900-second
lifetime. That seed is now expired and has not been reused or timestamp-edited.

The EasyProxy step adapter dropped an explicit `false` before passing the
challenge policy to the runtime. A regression test reproduced the failure.
The adapter now preserves explicit false values and the canonical account and
continuation flows check all three required entry points:

- `https://auth.openai.com/log-in-or-create-account`
- `https://chatgpt.com/auth/login`
- `https://chatgpt.com/api/auth/csrf`

All three require HTTP 200 without accepting a challenge. The team-expansion
flow was not changed. The affected local gate passed 75 tests, excluding the
account-availability-audit group, which failed in the broader run. A subsequent
focused regression passed for rejection of each required endpoint. The
candidate passed its three proxy test modules with network access disabled.
This is not a claim that the entire repository test suite is green.

The strict-preflight orchestrator image is
`sha256:bcbb3317704c47377b60b0c833858b20e7e675655e992b684a98c68018ffd5f4`.
The 2026-09-12 00:47:54-00:48:01 UTC live preflight observed all three endpoints
passing, confirmed that explicit false reached the acquisition function, and
released its lease. It submitted no account identity and called no SMS API.

## Serial execution history

The first paid-enabled continuation,
`easyregister-paid-once-20260912-001-verified`, started at
2026-09-11 23:34:43 UTC. It accepted the fresh seed, acquired its proxy and
mailbox, and initialized the organization. It then failed on the actual
`GET https://chatgpt.com/auth/login` request with HTTP 403. The normalized error
was `authorize_continue_blocked`, with detail `chatgpt_login_bootstrap`.
OAuth and final validation were skipped. Cleanup completed. No number was
purchased. This exited container must not be restarted.

The second continuation, `easyregister-paid-once-20260912-002`, subsequently ran
and exited before any purchase. Its separate output root and execution marker
are retained. It shares the still-unused original spending guard. The seed-only
flow used for this continuation contains account creation, organization
initialization, and cleanup; it does not attempt ChatGPT login or SMS itself.

Seed 007 passed the strict proxy check and mailbox acquisition, then received
an actual Cloudflare challenge while validating the email OTP. Proxy and
mailbox cleanup succeeded. Seed 008 reached the same boundary after an opt-in
browser OTP recovery implementation was added. The recovery did not return a
verified response, and the original error was retained. Neither execution
produced a seed admitted to the second continuation.

The browser OTP recovery uses the same signed authentication session and code,
checks the real browser page and identity before submitting, and returns the
observed response status and body. It does not manufacture a successful status
or retry an application rejection. The local browser/authorization/error-boundary
gate passed 23 tests; the five OTP tests also passed in the isolated provider
image. The tested provider image was
`sha256:916eb3367b4556c894edca43607baede851bada2b5cb6e7e237b71ca96330cfc`.
Seed 009 retained private failure snapshots. They showed that the signed
identity and login cookie were correct, but the new browser was still on the
Cloudflare challenge page before it could submit the OTP.

A read-only replay retained the same signed account session. With the protocol
client's Cloudflare cookies imported into Chromium, the page remained HTTP 403
after 40 seconds. Omitting only those client-bound edge cookies returned the
actual email-verification page with HTTP 200 immediately. Neither replay
submitted an OTP. A separate protocol-client comparison still received HTTP
403 with and without those cookies, so waiting alone or deleting protocol
cookies is not claimed as a successful repair.

Browser recovery now transfers the signed account session without copying
Cloudflare cookies between the two clients. A regression first reproduced the
cookie leakage. The updated local authorization/OTP/error-boundary gate passed
24 tests, and the six OTP tests passed in the isolated image with networking
disabled. The resulting provider image is
`sha256:f70195d52adc1afff24bf10e3b09e397a73d06d3b669adb5a4f5c55d4c3c5a84`.
Seed 010 subsequently completed the SMS-disabled account flow and supplied the
second continuation. Later seed and continuation results are recorded below.

## Subsequent browser repairs and serial results

The active Linux browser factory used to kill every `chromedriver` process when
starting another browser. That broke the existing session during browser OTP
recovery. Cleanup is now scoped to the requested profile. Two actual Chromium
integration tests, including coexistence of two drivers, passed with Docker
networking disabled. The unused mirror factory was not modified.

The optional `BrowserLoginSession` keeps supported requests in the same browser
after recovery. Application cookies retain their expiry, HttpOnly and SameSite
properties, while Cloudflare cookies remain in their own browser. Successful,
identity-verified authorization and OTP helpers transfer browser ownership with
an `ExitStack`; rejected responses do not transfer ownership. The signup session
starts with automatic challenge recovery disabled and accepts only these
verified handoffs. Seeds 011-016 exposed profile cleanup, browser ownership,
navigation and premature anonymous-session problems before seed 017 succeeded.

Seed 017 completed all six steps, with `createdAt` 2026-09-12T05:01:09Z and SHA256
`7bca02db195580fd92dacd709eaeb639fd76cf361ffcaf8aa12ec82ab7fbd834`.
Paid continuation 003 ran from 05:01:42 to 05:02:21 UTC and failed during ChatGPT
login initialization. OAuth and final validation were skipped; proxy, mailbox
and artifact cleanup completed. No purchase ledger or receipt was created.

The first navigation repair accepted an interactive DOM as well as a complete
DOM and retained private exception tracebacks. A candidate passed 26 offline
tests and an anonymous four-request ChatGPT bootstrap, including real NextAuth
state and `login_session` cookies. Seed 018 then completed all six steps. Its
`createdAt` was 2026-09-12T05:38:41Z and its SHA256 was
`6f89ef563b2a9f72593da74e1232f3bf021e0313a25e98e058bea5dd7e530d91`.
Its unmodified private backup matched, and the normal validator admitted it at
about 20 seconds old.

Paid continuation 004 ran from 05:39:07 to 05:39:39 UTC. It also failed before
SMS, during ChatGPT login initialization. This time the private traceback
located the timeout in Selenium's `driver.get()` itself, at the login-page GET.
The browser already contained the ChatGPT login document. Merely relaxing the
later DOM wait had not fixed that boundary.

Navigation now tolerates that timeout only when `performance.timeOrigin` proves
that a new document committed and its DOM is interactive or complete. The
normal response conversion still requires an observed HTTP status and an
allowed origin. Old documents and incomplete navigation remain rejected.
Twenty-seven focused tests passed locally and in the candidate image. An
additional real Chromium test reproduced a committed document with a blocked
subresource and verified recovery without fabricating its HTTP 200. This test
uses the normal page-load strategy to deterministically provoke the timeout;
production retains its existing eager strategy. Earlier eager-fixture attempts
did not trigger the timeout and were not counted as passing reproductions.

The committed-navigation candidate was
`sha256:c5bd3160c4034bf0ff600992c023308bcfbe98e2086f8f74f7897cce002cf9ab`, tagged
`easyregister-protocol-python:browser-committed-navigation-20260912-001`.
Its browser-session source SHA256 is
`fbf0afc94cc0dcc08a7e2235144bc2c042b421a38adfa59fb8ffc4477a7e435f`.
Provider `/health` and gateway `/api/health` both returned HTTP 200 after the
isolated replacement. Its anonymous bootstrap passed at 06:06:49 UTC: login,
CSRF, signin and authorize all returned HTTP 200, both required cookies existed,
and the browser and proxy lease were released.

Other anonymous diagnostics observed real CSRF fetch timeouts, including from
a complete document and with both redirect modes. Those observations are
retained as an intermittent transport risk; the successful bootstrap does not
prove that every proxy route is reliable.

Continuation 005 is prepared but has not started. Seed 019 failed at account
creation, with no seed admitted to the continuation and no SMS purchase.
Its private snapshot exposed another local boundary error: an API POST was
sent while its referring browser document was still the HTTP 403 Cloudflare
challenge page. The local repair waits for a real HTTP 200 document at the
expected origin before sending the API request and records when no request was
submitted. This was subsequently deployed and tested. An additional regression
connected the unsubmitted-document challenge to the existing opt-in OTP
recovery, preserving the original error when that recovery fails. The final
gate for these browser changes passed 29 tests locally and in the candidate.

Same-origin API requests now retain their verified browser document rather than
performing another full navigation solely because the referrer path changes.
The resulting provider image was
`sha256:ab8bc5adf402a33a3d9fe6f9718b5c6c7d6f88ca56f247bef6e58f3198df2723`.
Its anonymous ChatGPT bootstrap passed all four requests at 07:18:38 UTC. The
navigation helper AST was unchanged, so its retained real Chromium test was
reused rather than represented as a new browser run.

Seeds 020-022 also failed before admitting a seed to continuation 005. Seed 022
completed real OTP browser recovery, then received an actual HTTP 403 with
`cf-mitigated: challenge` from `POST /api/accounts/create_account`. The browser
remained on the verified email page. Private inspection found matching device
IDs and no application-cookie value difference supporting a cookie-mismatch
diagnosis.

## Account creation boundary investigation

A read-only comparison finished at 07:33:53 UTC. Two fresh browsers, with
resource blocking enabled and disabled respectively, navigated to the real
`/about-you` page with HTTP 200, matching identity and a login session. Neither
comparison submitted an account POST or used SMS.

An opt-in account-recovery candidate was then built. It verifies the real account
page and original identity before submitting the original name and birthday,
does not transfer client-bound Cloudflare cookies, preserves actual rejection
responses, and transfers successful browser ownership for cleanup. Two failing
regressions first demonstrated the missing recovery call. Its complete focused
gate passed 36 tests locally and inside the network-disabled candidate.

The unproven account-recovery candidate was
`sha256:601579d0bb3689e9923870b508657deacede38fe0f4ceb4e209bd06c5d89015a`, tagged
`easyregister-protocol-python:browser-account-recovery-20260912-001`.
Only `_submit_create_account` was transplanted into the existing dirty module;
AST comparison verified that all other definitions stayed unchanged. The new
helper was `protocol_browser_account.py`. Provider and gateway health returned
HTTP 200 after replacement at 07:52:27 UTC. Production was not replaced.

Seed 023 started at 07:53:46 UTC and failed. Both the original account POST and
the recovery POST returned actual HTTP 403 challenges. The recovery had first
verified HTTP 200 at `/about-you` and the correct identity. Proxy and mailbox
cleanup succeeded. Continuation 005 remained in `created` state. This candidate
has not resolved the live account-creation failure and is not a large success.

A native-page diagnostic finished at 08:04:09 UTC. It submitted the real form
once, after checking identity, HTTP status, form validity and the absence of an
additional consent requirement. Captured browser network evidence showed one
account POST to the same API, with exactly the original name/birthday payload
and a page-generated Sentinel header. That request also returned HTTP 403 with
`cf-mitigated: challenge`; the page stayed at `/about-you`. Page telemetry POSTs
were also challenged. Browser and proxy cleanup completed, without SMS.

An anonymous comparison finished at 08:12:51 UTC. A new strict GET-checked proxy
returned a challenge for a protocol POST containing an empty object and no
account identity. The fresh browser's login-page GET then returned HTTP 403.
This does not establish a safe POST acceptance predicate or prove that a new
proxy gate would fix authenticated account creation. No such predicate has
been added based only on this negative observation.

A protocol-client diagnostic finished at 08:23:37 UTC. It used the signed
in-progress account session, the original name/birthday and a newly generated
Sentinel header. Browser account fallback was disabled for this comparison.
The one account POST again returned HTTP 403 with a challenge. No OAuth seed
or paid continuation was admitted from this diagnostic, and cleanup completed.

A separate six-request egress check finished at 08:30:55 UTC. Three fresh
protocol sessions and three browser requests to the auth host's diagnostic
endpoint all returned HTTP 200 and observed one unchanged egress IP. This
confirms transport continuity for that lease at that time; it does not identify
the server's reason for challenging account creation.

The account-recovery experiment did not improve the live result. Its local
helper, tests and call-site change were removed, preserving the earlier browser
fixes and pre-existing worktree changes. The candidate image, stopped provider,
tests and all remote evidence remain retained for inspection. No previously
executed one-shot container was restarted.

At 08:38:33 UTC the isolated provider was restored to
`sha256:ab8bc5adf402a33a3d9fe6f9718b5c6c7d6f88ca56f247bef6e58f3198df2723`.
Provider and gateway health both returned HTTP 200. The 29 retained focused
tests passed again locally and in the restored image with networking disabled.
Production services were not changed.

The final read-only financial check at 08:41:03 UTC called only `getBalance`,
`getPrices` and `getActiveActivations`. It confirmed a 4.5953 USD balance, a
0.045 USD quote for service `dr` / country 16, and zero active orders. The
original 0.05 USD cap remains intact; neither purchase ledger nor receipt
exists. Continuation 005 is still `created`. No valid final OAuth artifact is
available, so this work is explicitly not reported as a large success.

No seed timestamp has been edited and the normal 900-second validator has not
been relaxed. All executed one-shot containers, private evidence and the
original single-purchase ledger location are retained. No executed one-shot
container has been restarted.

## Retained evidence and next boundary

PC2 evidence is under
`/home/mjc/easyregister/paid-sms-canary/20260912-001`:

- `oauth-candidate-001/strict-adapter-fix`: source comparison, image build,
  offline tests, and endpoint-rejection check.
- `strict-preflight-001`: real strict-probe observations and private runtime log.
- `oauth-candidate-001/otp-browser-fix`: narrowly patched provider source,
  offline tests, and isolated runtime health checks.
- `oauth-candidate-001/otp-cookie-boundary`: client-cookie isolation patch,
  offline tests, and candidate image/runtime evidence.
- `paid-run-002`: separate flow, manifest, bounded environment, and offline gate.
- `paid-run-003`, `paid-run-004`: separate executed continuations and preflights.
- `paid-run-005`: prepared continuation, not yet started.
- `oauth-candidate-001/browser-verified-handoff`: successful seed 017 runtime.
- `oauth-candidate-001/browser-fetch-abort-evidence`: first full browser bootstrap
  and deployed timeout diagnostics used by continuation 004.
- `oauth-candidate-001/browser-committed-navigation`: 27-test gate, actual Chromium
  navigation proof, anonymous network trace and isolated deployment evidence.
- `oauth-candidate-001/browser-otp-document-recovery`: unsubmitted-document
  recovery gate and retained Chromium runtime test.
- `oauth-candidate-001/browser-same-origin-document`: 29-test gate, successful
  anonymous bootstrap, isolated deployment and read-only account-page evidence.
- `oauth-candidate-001/browser-account-recovery`: retained 36-test experiment,
  seed 023, native form/protocol/anonymous/egress diagnostics, rollback proof,
  fresh 29-test gate and final read-only ledger reconciliation.
- `guard-state`: the original single-purchase ledger location, not reset.

Private execution output is under
`/home/mjc/easyregister/output/paid-sms-canary-20260912-001`, including
`seed-refresh-006` through `seed-refresh-023`. Continuations 002-005 have distinct
roots named `/home/mjc/easyregister/output/paid-sms-canary-20260912-NNN`.

The next paid start remains conditional on one newly produced seed passing the
normal validator, an unchanged private backup and hash, a refreshed real quote,
and the intact original spending guard. The first run's files and marker are
retained. Production services have not been replaced by these isolated
candidates.
