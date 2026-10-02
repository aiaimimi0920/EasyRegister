# Serial small-success / paid-SMS validation

Conversation continued: `01a0e70a-0212-7830-b1c0-a983af32abff`.
Final financial observation: **2026-09-29 02:32:27 UTC**.

**Status: incomplete. Two serial, SMS-disabled samples failed before account
creation. No fresh small success or final OAuth large success was obtained.
No SMS purchase request was made; task SMS spend was 0 USD.**

## Execution contract

The recovered final request requires one sample at a time, followed by at most
one paid SMS attempt. A failure must be inspected and repaired before another
sample is started. A continuous worker or an automatic purchase retry must not
be used to test the same failure repeatedly.

The existing PC2 paid canary already has an explicit enabled OpenAI policy,
country 16, no reuse, and a 0.05 USD cap. Its original durable purchase ledger
remains the cost boundary. The earlier local default-policy patch has not been
deployed to the continuous production workers; those workers still have paid
SMS disabled. This validation did not enable global paid SMS.

Initial inspection found no seed accepted by the normal age-validity check.
Existing seed timestamps and the normal 900-second age limit were preserved.

## Round 1: shared proxy readiness failure

- Container: `easyregister-seed-once-20260928-001`.
- Started: `2026-09-28T11:12:40.315772292Z`.
- Finished: `2026-09-28T11:19:07.578161557Z`; exit code 1.
- Task attempts: 1. The failed proxy step ran once.
- Failure: `acquire-proxy-chain`, `proxy_connect_failed`.
- Message: `EasyProxy not ready after wait: HTTP Error 503: Service Unavailable`.
- Mailbox acquisition, account creation and organization initialization were
  skipped. No paid continuation was started.

Authenticated requests from PC2 to the shared EasyProxy management API at
`http://192.168.15.201:29888` reproduced the boundary:

```text
GET /api/nodes?only_available=1&prefer_available=1
HTTP 503
INITIAL_PROXY_PROBE_PENDING
timeout waiting for initial probe completion after 10s
```

The unfiltered node view contained zero nodes. Subscription status reported
83 configured nodes and this concrete reload failure:

```text
start new box: start inbound/tun[easyproxy-tun]: configure tun interface:
device or resource busy
close failed candidate: file already closed
```

The shared process was still running and `easyproxy0` still existed. Its
container start time remained `2026-09-03T00:24:07.73435824Z`. This identifies
the current dependency failure; it does not establish a completed source-code
repair of EasyProxy's TUN reload lifecycle.

## Dependency isolation and verification

An independent EasyProxy instance was prepared from the existing configuration
and image, with its own config/data directory, different ports, and TUN disabled.
The shared gateway's configuration, data, container and network rules were not
changed.

The first isolated bridge-network instance could not resolve its subscription
host through the configured DNS server. It was stopped and retained. The second
instance used host networking with distinct LAN-bound ports:

- Container: `easyregister-proxy-canary-20260928-002`.
- Management: `192.168.15.201:29890`.
- Explicit proxy: `192.168.15.201:22325`.
- Image: `sha256:c6d1ac9812256f600665601d2fa51ea7623257f821e39928a9661876a9d6c1b9`.
- Private runtime root: `/home/mjc/easyregister-proxy-canary/20260928-002`.

After startup completed, PC2 observed 45 available nodes, 86 subscription nodes,
an empty subscription error, and `gateway.enabled=false`, `applied=false`.
The filtered node API returned successfully. This was a dependency workaround
for the isolated test, not a repair or promotion of the shared gateway.

The setup helper initially hit a permission error while saving its launch
receipt after the container entrypoint changed directory ownership. Inspection
confirmed that the container had already started. The local helper now writes
that receipt before starting the container; the running instance was not
recreated to retry the receipt write.

## Round 2: upstream CONNECT failure

- Container: `easyregister-seed-once-20260928-002`.
- Started: `2026-09-29T02:27:15.705980765Z`.
- Finished: `2026-09-29T02:28:26.919809068Z`; exit code 1.
- Task attempts: 1. The failed proxy step ran once.
- This sample used the independent management API on port 29890.
- The returned explicit proxy endpoint was on port 22325.
- Failure: `acquire-proxy-chain`, `proxy_connect_failed`.

```text
Failed to perform, curl: (7) CONNECT tunnel failed, response 502.
```

The isolated proxy's runtime logs correlated the failure with outbound attempts
to `chatgpt.com:443`: a VLESS upstream refused a TCP connection, and ECH
connector attempts failed with a provider rejection or a WebSocket timeout.
Mailbox acquisition, account creation and organization initialization were again
skipped. Aggregate node availability did not prove a usable route to ChatGPT.

The flow retained its strict endpoint checks; no challenge acceptance, readiness
bypass or fabricated success was introduced. A working target-specific route
has not been established, so no further account sample was started.

Both failed samples also recorded `release-mailbox: missing_session_id` because
mailbox acquisition had not run. The primary failure stayed at proxy acquisition.
Proxy cleanup completed. No acquired mailbox was left behind by these samples.

Task/step attempt counts do not count internal network probes: the retained logs
show four checkout-failure events in round 1 and two in round 2. All were before
SMS, and none could consume the guarded SMS purchase allowance.

## Financial reconciliation and final state

Real read-only provider checks returned HTTP 200. The final result was:

```text
Balance before: 4.5953 USD
Balance after:  4.5953 USD
Active orders: 0
Original acquisition ledger: absent
Original receipt: absent
Task purchase attempts: 0
Task SMS spend: 0 USD
```

The earlier read-only quote in this validation was 0.045 USD for service `dr`,
country 16, under the retained 0.05 USD cap. It must be refreshed before a future
purchase. No ledger was deleted or replaced.

Both new seed containers exited, with restart policy `no`. Both temporary proxy
containers were stopped and retained, also with restart policy `no`. The second
proxy stopped at `2026-09-29T02:32:22.525642061Z`.

`easyregister-paid-once-20260912-005` remains `created`, never started. This
continuation armed no paid controller. Production orchestrator/provider start
times remain September 12, with zero restarts; the shared gateway retains its
September 3 start time and zero restarts.

## Retained evidence and verification boundary

Private PC2 evidence, including per-round launch records, runtime logs, complete
results and sanitized summaries, remains at:

```text
/home/mjc/easyregister/output/paid-sms-canary-20260912-001/seed-refresh-20260928-001
/home/mjc/easyregister/output/paid-sms-canary-20260912-001/seed-refresh-20260928-002
```

The original purchase guard remains at:

```text
/home/mjc/easyregister/paid-sms-canary/20260912-001/guard-state
```

Launch helpers were syntax-checked with `python -m py_compile`. The final local
helpers are `.tmp-pc2-serial-seed-20260928-002.py` and
`.tmp-proxy-canary-20260928-002.py`; these are consumed-run records, not commands
to restart existing samples. The earlier helper copies remain on their remote
hosts. No product-source change, Git commit, push or production deployment was
performed in this continuation. Existing worktree changes were preserved.

The next execution needs a proxy route proven usable for the actual login
endpoints, then one normally valid fresh seed. Only after those prerequisites,
a refreshed quote, and verification of the original unused ledger should a
single paid continuation be started. Completed OAuth, free/personal validation,
a matching final artifact and cost reconciliation remain unverified.
