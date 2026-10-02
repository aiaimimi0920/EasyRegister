# Shared EasyProxy and EasySMS repair

Status at **2026-09-29 11:39:29 UTC**: the approved shared-service cutover is
complete. The repaired gateway passed a manual reload and a later scheduled
subscription reload without the TUN ownership failure. EasyRegister authenticated
to both shared services and completed the strict proxy checks in two serial,
SMS-disabled samples. The latest sample stopped at upstream
`browser_verification_required status=403 cf_mitigated=challenge`.
No valid fresh seed or final OAuth success was obtained. SMS spend remains 0 USD.

This continues [the serial validation report](pc2-serial-sms-validation-2026-09-28.md).

The later [auth-session repair report](auth-session-repair-2026-09-30.md) records
the minimized-cookie compatibility fixes, verified browser authorization,
and the remaining email OTP challenge. The observations below retain their
original 11:39:29 UTC timestamp.

## Actual service wiring

The running PC2 orchestrator uses `http://192.168.15.201:29888` for EasyProxy,
with the existing management password. Its runtime proxy host is
`192.168.15.201`. The Python protocol service has the same management endpoint.

The orchestrator uses `http://easy-sms:8080` with `SMS_SERVICE_API_KEY`.
Docker resolves this name to `172.20.0.4`, the `easy-register-sms` container.
Authenticated health, catalog, runtime and session queries succeeded.
The session query returned zero sessions; the paid catalog fallback returned
exactly `hero_sms`.

The empty paid `selection-plan` result is not itself a failure. The endpoint's
route kind is `list-public-numbers`; the client falls back to the enabled paid
catalog for activation providers. The fallback was exercised read-only using
the deployed client.

The orchestrator owns SMS acquisition/polling. In `easyprotocol_runtime.py`, it
calls `runtime_sms.wait_phone_code_for_session`, then supplies `sms_code` to
`invoke_easyprotocol(step_type="submit_phone_verification_code")`. An ad hoc
`import shared_sms` failure inside PythonProtocol does not demonstrate a broken
production integration.

## Proxy failure and source repair

The shared gateway had zero runtime nodes, and its subscription status reported
`configure tun interface: device or resource busy`. Six `ech-workers` child
processes retained descriptor 200 for `easyproxy0` after the parent box closed.

The pinned `sing-tun v0.7.13` opens `/dev/net/tun` with `O_RDWR`, without
`O_CLOEXEC`. A concurrently executed child inherits that descriptor and keeps
the interface busy across a box reload. This accounts for both the live process
evidence and the isolated reproduction.

EasyProxy now contains a local replacement of that exact dependency version at
`service/base/third_party/sing-tun`. Its only upstream source difference is the
atomic `O_RDWR | O_CLOEXEC` open in `tun_linux.go`; the original license is
retained. `go.mod` and the main Dockerfile use this replacement. No delayed
descriptor sweep or TUN cleanup against the host namespace was introduced.

The Linux regression in `internal/tuncheck` starts a real child process, closes
the parent TUN, and reopens the same interface while the child stays alive.
It ran in a disposable Docker network namespace with `--network none`,
`CAP_NET_ADMIN` and `/dev/net/tun`:

- Original dependency: failed, `inherited=1`, then `device or resource busy`.
- Patched dependency: passed, no inherited TUN descriptor, reopen succeeded.
- Linux service build with the production feature tags: passed.
- `gofmt` and scoped `git diff --check`: passed.
- Dependency comparison: only `tun_linux.go` differs; no missing upstream files
  and no UTF-8 BOMs in the inspected source/configuration files.

The deployed image is `easyproxy-local:tun-cloexec-20260929`, image ID
`sha256:75b326029447d6b2d50d8b042db40d3b2659fc18c17b406630d98ab4487fcdca`.
It replaces only the service binary in the original runtime image. The stopped
candidate was `easy-proxy-gateway-tunfix-20260929`; after approval it became
`easy-proxy-gateway`, started at `2026-09-29T03:30:36.282618698Z`.
The old container remains stopped as `easy-proxy-gateway-before-tunfix-20260929`.
The environment, bind mounts, host networking, capabilities, TUN device and
restart policy match the old container. The final configuration still matches
the saved pre-cutover bytes, SHA256
`c373dcc148665dff63c1c45f2f934049548a3424cade3175c2c1b6ad14478516`.

## SMS failure and completed repair

The production HeroSMS key returned HTTP 401 with `BAD_KEY` / `Unauthorized`.
The mounted `/etc/easy-sms/config.yaml` and the active runtime copy under
`/tmp/easy-sms-runtime` contain the same key. No bootstrap sync configuration
exists that would overwrite a repair.

The existing single-purchase guard holds a different key. Using that key through
the production SMS container's network and provider configuration returned HTTP
200 and `ACCESS_BALANCE:4.5953`. Only the equality result and balance were
reported; neither credential was copied into this report or repository.

`.tmp-repair-shared-sms-20260929.py` completed the approved cutover after checking
the original unused ledger and absence of sessions. It backed up the old YAML
exclusively, changed only `providers.heroSms.apiKey`, preserved ownership/mode,
and restarted the same SMS container at `2026-09-29T03:31:04.226326997Z`.
It did not enable paid worker policies or acquire a number. The final mounted
and active keys match, and the replacement key authenticates with HeroSMS.

## Completed cutover and rollback

Prepared gateway files are on `.201` at
`/home/mjc/easyproxy-tun-fix/20260929`. The private pre-cutover container receipt
is mode 0600. The `apply` operation has already completed; do not repeat it.
If rollback is required, the retained helper has this explicit action:

```sh
python3 /home/mjc/easyproxy-tun-fix/20260929/.tmp-repair-shared-proxy-20260929.py rollback
```

The rollback action restores `easy-proxy-gateway-before-tunfix-20260929`.
The image/config schema is unchanged.

The PC2 SMS helper is at
`/home/mjc/easyregister/.tmp-repair-shared-sms-20260929.py`. Its completed
`--apply` action created `/etc/easy-sms/config.yaml.before-key-repair-20260929`
in the persistent config mount. Restore that file and restart SMS to roll back
the credential change; do not repeat `--apply` against the repaired key.

Runtime checks below cover authenticated nodes, subscription state, target
traffic, TUN ownership, SMS configuration and the real HeroSMS balance.
Local build artifacts are retained under
`EasyProxy/tmp/tun-fix-20260929`; no release was published or pushed.

The original purchase ledger and receipt remain absent. Task SMS spend is
0 USD. A fresh account seed, a guarded SMS purchase, final OAuth and matching
free/personal artifacts remain unverified. The production orchestrator still
has `REGISTER_SMS_ALLOW_PAID=false`; the pre-existing enabled canary policy and
its durable one-purchase guard remain the intended boundary for a later test.

## Runtime verification and remaining boundary

At 11:27:57 UTC, real requests from `easy-register` through the shared explicit
proxy at `192.168.15.201:22323` returned 204 for the connectivity endpoint and
200 for both `https://chatgpt.com/auth/login` and
`https://auth.openai.com/log-in-or-create-account`. TLS verification was enabled.
The diagnostic applies the deployed runtime's `mixed://` to `http://`
normalization; its first raw-lease probe had omitted that existing adapter and
was corrected without changing product code. All diagnostic leases were released.

The authenticated manual `POST /api/reload` completed at 11:28:57 UTC. Before
and after this reload, six ECH children were present and only the `easy-proxy`
parent held `easyproxy0`. The parent PID and container start time were unchanged.
A subsequent scheduled refresh completed at 11:33:45 UTC. At the final check,
the parent still exclusively owned the TUN; no ECH children were then running.
The gateway was applied/enabled, subscription errors were empty, and the shared
node API returned 87 total and 54 available nodes. No container restart occurred.

These checks do not establish uninterrupted upstream availability. A request
after manual reload encountered an ECH-related CONNECT 502. More importantly,
the first account sample overlapped the scheduled reload: it started at
11:32:08 UTC, passed all three strict proxy checks plus mailbox acquisition,
and failed at account creation at 11:33:18 UTC. The subscription began reloading
at 11:33:02 and completed at 11:33:45; at 11:33:16 the dispatcher recorded
`dispatch CONNECT auth.openai.com:443 [PROXY] dial failed: proxy pool not available`.
This identifies the reload gap for that failure. Zero-downtime reload behavior
was not implemented or claimed.

The next launcher added a read-only gate requiring a completed subscription
refresh, available nodes and at least 600 seconds until the next refresh. Its
11:37:30 preflight found 3375 seconds remaining. It also handles the server's
nanosecond timestamps on the deployed older Python parser. No sample was started
when the first parsing preflight failed.

The second sample ran from 11:37:33 to 11:38:26 UTC and exited 1. The proxy and
mailbox steps succeeded once each. Account creation ran once and returned
`browser_verification_required status=403 cf_mitigated=challenge`; organization
initialization was skipped. Both resource-release steps succeeded in both
samples. No third sample or paid continuation was launched, and no challenge
acceptance or seed-age bypass was added. A legitimate browser verification
remains necessary before claiming account/OAuth success.

Final financial observations at 11:39:29 UTC were HTTP 200 for all three
read-only provider queries: balance 4.5953 USD, country 16/service `dr` quote
0.045 USD with stock 191992, and zero active orders. The provider returns a
non-array `activeActivations` value and an empty array in `data`; the diagnostic
counts the actual array instead of assuming success implies zero orders.
Shared SMS had zero sessions. The original ledger and receipt were absent, and
`easyregister-paid-once-20260912-005` remained `created`, never started.

Local evidence helpers:

- `.tmp-verify-shared-services-20260929.py`: observations only by default;
  `--traffic` creates/releases a diagnostic proxy lease; `--reload` is an
  explicit disruptive reload action and must not be used as a passive check.
- `.tmp-pc2-serial-seed-20260929-001.py`: consumed one-sample launcher.
- `.tmp-pc2-serial-seed-20260929-002.py`: consumed guarded launcher, reusing the
  first launcher with a distinct output root. Do not restart either sample.

Private PC2 results remain under
`/home/mjc/easyregister/output/paid-sms-canary-20260912-001/seed-refresh-20260929-001`
and `seed-refresh-20260929-002`. The second preflight receipt is under
`/home/mjc/easyregister/paid-sms-canary/20260912-001/serial-seed-20260929-002.preflight.json`.
All three local helpers passed `python -m py_compile`. Existing dirty product
changes were preserved; this continuation made no further product-source change,
commit or push.
