# PC2 EasyRegister deployment - 2026-09-08

> Historical staging checkpoint. The subsequent dependency deployment, image
> `003`, and business-canary result are recorded in
> [the current runtime report](pc2-runtime-status-2026-09-08.md).
> EasyProtocol/EasySMS are no longer missing; account creation is now blocked
> by a target-site `platform_login` HTTP 403.

## Scope and acceptance

Target: `mjc@192.168.15.104`, hostname `pc2`, Linux. The current host is not
the historical Windows PC2 deployment. Authentication uses the existing local
SSH key; the server offers public-key authentication, not password login.

Deploy the verified EasyRegister image without modifying unrelated applications.
Use the new EasyEmail and EasyProxy services, and verify the real dependency
flow on PC2. Full acceptance additionally requires a running production
scheduler and a successful account-registration workflow; dependency diagnostics
alone do not establish those outcomes.

## Deployment layout

- Remote root: `/home/mjc/easyregister` (mode 0700).
- Image: `easy-register/easy-register:nas-dst-20260908-002`.
- Expected image ID:
  `sha256:b02b8f06b82727245d5d24ddcdb81c9a92d9f153a4f6dbc1fcee2ed6d80f9677`.
- `compose.yaml`: repository canonical Compose configuration.
- `runtime.env`: runtime settings and credentials, mode 0600; do not publish.
- `staged.yaml`: disables automatic restart while dependencies are unresolved.
- `output/`: persistent account/output data, initially empty.
- `team-auth/`: isolated, initially empty, read-only container mount.
- `images/`: transferred Docker image archive.
- `deployment-evidence.json`: sanitized deployment and diagnostic evidence.
- Docker network: `easyregister-pc2`, isolated from existing applications.
- Dashboard binding: `127.0.0.1:19790` on PC2, not exposed to the LAN.

New dependency endpoints:

- EasyEmail: `http://192.168.15.200:18081`, authenticated with the controlled key.
- EasyProxy: `http://192.168.15.201:29888`, authenticated with the controlled
  management credential.

No secrets are embedded in the deployment script or this report. The temporary
diagnostic credential file is removed after the diagnostic command.

## Deployment state

**Observed final state: staged, not running in production.** The image is loaded
and its exact ID matches. `easy-register` is `Created`, with restart policy `no`.
The last two PC2 DST dependency-flow checks passed all six steps, and the final
configuration preflight passed with four flow definitions. Full registration
has not been started or proven.

The staging script is `scripts/deploy-pc2-staged.py`. It records the local source
archive SHA256, streams the archive through authenticated SSH into Docker,
checks the loaded Docker image ID, runs `python -m nas_dst_smoke` on
PC2, validates Compose, then creates (but does not start) `easy-register`.
It refuses to overwrite an existing runtime environment or EasyRegister
container; it is not an unattended upgrade/retry script.

The authoritative observed result is recorded in
`deploy-evidence/pc2-staged-20260908.json`. The script stopped at an initial
diagnostic failure. Compose was subsequently validated, the container was
created without starting it, and the final diagnostics were run explicitly.

The initial SFTP archive was fully copied, but its remote SHA256 read exceeded
both 120-second and 600-second waits. A live check found the checksum process in
disk-read wait (`folio_wait_bit_common`), with high system I/O pressure. The
deployment therefore uses binary SSH streaming into `docker load` rather than
repeatedly rereading the remote archive. The remote archive hash is explicitly
unverified; the loaded image ID must still match the expected ID exactly.

Earlier PC2 DST checks failed at the proxy probe with curl error 28 after about
20 seconds. A separate service-client check passed mailbox operations and an
explicit, verified TLS CONNECT with a semantic Example Domain response. The
last two actual DST checks then passed, without a source-code or host-network
change. This is evidence of current success, not proof that the earlier
intermittent timeout has been permanently resolved.

The diagnostic checks proxy acquisition, mailbox acquisition, mailbox recovery,
release of both mailbox sessions, and proxy release. It does not start
registration workers, purchase SMS, receive a real external OTP, or prove
successful account registration.

## Remaining prerequisites

PC2 had no EasyRegister, EasyProtocol, or EasySMS containers at initial inventory.
The canonical defaults `http://easy-protocol:9788` and `http://easy-sms:8080`
do not identify services on this new isolated deployment network. No current
LAN replacements or associated API credentials were identified in the scoped
deployment configuration review.

Correction to the initial connectivity assessment: these names do resolve on
PC2, but to synthetic `198.18.32.x` and `fc00::` addresses. Even TCP connection
attempts succeeded under this networking setup. Neither result establishes
the identity, API contract, authentication, or readiness of the required
EasyProtocol/EasySMS services; do not send credentials to unverified defaults.

Before production startup, securely configure:

1. `EASY_PROTOCOL_BASE_URL` and `EASY_PROTOCOL_CONTROL_TOKEN` for the actual
   EasyProtocol service, including its shared-output/bridge contract if remote.
2. `SMS_SERVICE_BASE_URL` and `SMS_SERVICE_API_KEY` for the actual EasySMS service,
   if using the default mixed registration flows.
3. The intended flow set and team-auth inputs where required. No historical
   Windows account data or team credentials were silently copied.

Paid SMS is explicitly disabled and worker/main concurrency is limited to one
in the staged runtime. Do not silently enable spending or restore ten workers
just to obtain a startup result.

After verifying dependencies from the target container, remove the staging
override from the production invocation and run the canonical Compose service.
Then inspect scheduler state, dependency responses, dashboard and task results.
Do not count a running PID or an HTTP 200 alone as registration success.

## Preservation and rollback

No stop, restart, remove, or replacement command was issued against existing
applications. All nine existing business containers reported healthy at the
final check. Unchanged start timestamps are not claimed: other activity on the
shared PC2 occurred during this deployment window. No pre-existing outputs were
moved, deleted, or overwritten.

While the staged container remains unstarted, rollback is simply leaving it
stopped. If removal is desired later, target only the `easy-register` project
using the exact Compose files above; preserve `runtime.env`, `output/`,
`team-auth/`, and the evidence. Do not prune Docker or delete unrelated networks.
