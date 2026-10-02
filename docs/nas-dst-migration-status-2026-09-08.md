# NAS DST migration status - 2026-09-08

## Current deployment checkpoint

PC2 now has EasyRegister image `nas-dst-20260908-003`, plus running EasyProtocol,
PythonProtocol and EasySMS dependencies. Ten interface checks passed. A bounded
registration task reached the provider but failed at `platform_login` HTTP 403;
continuous registration remains disabled. See
[the current runtime report](pc2-runtime-status-2026-09-08.md) and
`deploy-evidence/pc2-runtime-20260908-003.json` for current evidence.

## Earlier checkpoint: source migration and image acceptance; rollout pending

Continuation of conversation `01a07d06-e65d-75b1-8776-5e34371abe7e`.
The new services are usable by the repository's real Python clients. The
deployment launcher now preserves injected NAS service settings. The target
EasyRegister container has NOT been switched or rebuilt in this continuation.

### Latest continuation evidence

Recorded at `2026-09-07T21:55:42Z` (2026-09-08, Asia/Shanghai).

- Runtime, production/test compose, launcher and env example now default to
  EasyEmail `http://192.168.15.200:18081`; the same-host Docker alias remains an
  explicit override, not a cross-host default.
- The retired local EasyEmail configuration/key discovery code was removed.
  Both mailbox environment aliases are normalized consistently, including
  whitespace-only legacy values. No provider/admin secrets were copied into
  EasyRegister.
- The shared EasyProxy client now defaults to `.201:29888` and reads an
  explicitly injected base URL at request time rather than retaining a stale
  import-time loopback endpoint. Explicit method arguments retain precedence.
- A real recovery failure exposed a migration-specific client bug: explicitly
  selecting `cloudflare_temp_email` forced a newly provisioned dedicated
  instance, which lacked the configured shared instance's admin recovery
  credentials. The NAS configuration itself already contained `adminAuth`.
  Switching this provider to the documented `reuse-only` / `shared-instance`
  contract fixed the live HTTP 500. No NAS service configuration was changed.
- The new opt-in `nas-dependencies-smoke-v1.semantic-flow.json` runs the actual
  production dispatcher and service handlers, not mocks: acquire proxy, acquire
  mailbox, recover mailbox, release recovered mailbox, release original mailbox,
  release proxy. All **six steps passed** on the host and inside the built image.
  Recovery retained the address and opaque recovery data; cleanup was checked
  separately against the real APIs.
- Final combined regression command (the previous 11 files plus
  `tests/test_nas_dst_smoke.py`): **220 passed in 78.84 seconds**. No skips.
- Final Docker artifact:
  `easy-register/easy-register:nas-dst-20260908-002`.
  Image ID:
  `sha256:b02b8f06b82727245d5d24ddcdb81c9a92d9f153a4f6dbc1fcee2ed6d80f9677`.
  Built with the canonical `deploy/Dockerfile`, using the just-built `001`
  image as a warm base through its supported build argument. `001` was built
  from the Dockerfile's normal `python:3.10-bookworm` base.
- The image test ran with a read-only root filesystem, temporary `/tmp`, no
  capabilities, no host mounts, and only the two credential environment values.
  No URL overrides were passed, proving the new defaults work from Docker.
  Its named verification container was automatically removed after exit 0.
- SHA-256 comparisons matched **all 10** changed/new packaged source and README
  files between the worktree and the image. `git diff --check` passed.

The production-host uncertainty is now more specific: `.104:22` returned
`SSH-2.0-OpenSSH_10.0p2 Debian-7+deb13u4`; ports 5985, 5986 and 445 were
unavailable. It must NOT be assumed to be the historical Windows PC2. Confirm
the current EasyRegister host/IP and deployment directory before rollout.

See `deploy-evidence/nas-dst-20260908-002.json` for the redacted artifact record.
The full account-registration workflow, real OTP delivery, and production
container replacement remain outside the verified result. No account was
registered, no paid SMS was purchased, and no production scheduler was started.

## Earlier checkpoint evidence (previous continuation turn)

| Requirement | Current evidence | Status |
| --- | --- | --- |
| Preserve old architecture | Annotated tag `旧架构的稳定版本` resolves to commit `0fe13d383e9459b04862fc03b689f36ffaed7435` | Verified |
| Access NAS | Password authentication succeeded for `mjc` on `192.168.15.200`; hostname `mjc_nas`; sudo Docker listing succeeded | Verified |
| Remove old NAS containers | Fresh `docker ps -a` confirms exact names `easy-email` and `easy-proxy` are absent | Desired state already present; zero containers removed here |
| Preserve new email service | `easyemail-sdk` runs image `easyemail/easy-email-service:nas-20260907-002` | Verified, not modified |
| Preserve relay | `easyproxy-overlay-relay` is a distinct existing container, not the old `easy-proxy` | Preserved |
| Email authentication | LAN `.200:18081/mail/catalog`: unauthenticated 401; authenticated response contains a catalog object | Passed |
| Email lifecycle | Actual EasyRegister client: plan, create, refresh, message query, code endpoint, release | Passed; session expiration independently queried |
| Email delivery | No external mail/OTP was sent in this test | NOT verified |
| Proxy authentication | `.201:29888/api/auth` returns `canonical_pair`; authenticated available-node query succeeds | Passed |
| Proxy lifecycle and traffic | Actual client checkout; explicit HTTPS CONNECT through returned lease; verified TLS and `Example Domain` response; release returns `ok: true` | Passed |
| Launcher environment injection | New test first reproduced dropped `EASY_EMAIL_*` values, then passed after the fix | Passed |
| Focused regression gate | Host Python pytest, 11 listed files: **214 passed in 79.59 seconds** | Passed |
| Switch actual EasyRegister runtime | Target host/location not freshly confirmed; historical PC2 SSH rejects available keys | BLOCKED pending target access |
| Full deployed DST workflow | No production registration worker started, no SMS purchased | NOT verified |

## Changes made in this continuation

- `deploy-host.ps1`: external email/proxy settings can opt into process
  environment lookup. Email recognizes `EASY_EMAIL_BASE_URL` and
  `EASY_EMAIL_API_KEY` as aliases for the existing mailbox settings.
- Resolution order remains explicit script parameter, imported bootstrap
  configuration, then injected process environment, then default. Existing
  bootstrap addresses therefore need explicit replacement during migration.
- Scheduler and unrelated service settings retain their existing resolution
  behavior. No broad environment-precedence change was made.
- `tests/test_deploy_host_env.py`: verifies actual `-MaterializeOnly` output,
  NAS environment injection, explicit-parameter precedence, and absence of test
  secrets from stdout/stderr.
- `scripts/verify-nas-dst-services.py`: reusable real-client verification with
  short-lived resources, explicit CONNECT rather than a proxy-bypass-prone
  request, cleanup acknowledgements, and redacted stage-level evidence.
- `README.md`: clarifies same-host Docker aliases versus cross-host LAN URLs,
  environment precedence, and the limits of the service smoke test.

Existing uncommitted migration changes and unrelated temporary files were
preserved. No new commit or tag was created in this continuation.

## Network and data boundaries

The NAS SDK's `easy-email-service:8080` alias is valid, not obsolete. Live
inspection confirmed that `easyemail-sdk` is attached to NAS network `EasyAiMi`
with this alias. It also owns the `easy-email` network alias; an alias is not an
old container and must not be deleted as if it were one.

The SDK bind mounts remain:

- `/volume1/docker/easyemail-sdk/data` -> `/var/lib/easy-email`
- `/volume1/docker/easyemail-sdk/config` -> `/etc/easy-email`

Docker bridge networks with the same name on different hosts do not share DNS.
PC2 or other non-NAS callers must inject `http://192.168.15.200:18081`, not rely
on their own `EasyAiMi` network resolving the NAS alias.

The proxy management endpoint is `http://192.168.15.201:29888`. The real lease
checked during verification advertised `.201:22323`; it successfully carried
the authenticated HTTPS tunnel. The new gateway was not reconfigured.

No volume, image, network, provider credentials, or unrelated container was
deleted. Only resources allocated by the service smoke test were released.
Actual tokens/passwords were injected in process memory and are not included
in these artifacts.

## Reproduce the service verification

Inject these variables from the operator's controlled credential source:

```text
EASY_EMAIL_BASE_URL=http://192.168.15.200:18081
EASY_EMAIL_API_KEY=<controlled EasyEmail operator token>
EASY_PROXY_BASE_URL=http://192.168.15.201:29888
EASY_PROXY_MANAGEMENT_PASSWORD=<controlled management password>
```

Do not retain conflicting `MAILBOX_SERVICE_*` overrides. Then run:

```powershell
python scripts/verify-nas-dst-services.py
```

This creates a test mailbox and lease, performs a small request to
`https://example.com/`, and releases its resources in `finally`. It does not
start the scheduler or execute account registration. Successful evidence was
collected at `2026-09-07T20:59:24Z` (2026-09-08, Asia/Shanghai).

The code endpoint may validly return `{}` for an empty inbox: the supplied
OpenAPI `VerificationCodeResponse` schema does not require a `code` property.
An empty successful query is not proof of receipt or OTP extraction.

## Regression command and runner caveat

```powershell
python -m pytest -q tests/test_security_defaults.py tests/test_typed_config.py tests/test_easy_proxy_client.py tests/test_runtime_proxy_probe.py tests/test_easyproxy_flow.py tests/test_easy_email_client_provider_probe.py tests/test_runtime_mailbox_provider_failover.py tests/test_mailbox_state_recovery.py tests/test_dst_flow_integration.py tests/test_deploy_host_env.py tests/test_compose_smoke.py
```

Final host run: **214 passed**, no skips. `git diff --check` passed. All source,
test, launcher and README files touched here were checked as UTF-8 without BOM.

An earlier context-mode run reported 14 audit-selection failures. Its temporary
directory was nested under `.ctx-mode-*`; the scanner at
`account_availability_audit.py:199` excludes paths with any hidden component.
Normal host temporary directories do not trigger that exclusion. The same
66-test DST file passed on the host, and the final combined gate above passed.
No scanner production behavior was changed to accommodate the analysis tool.

## Remaining work and exact blocker

Historical deployment information identifies `Admin@192.168.15.104` and
`D:\SelfDocker\EasyRegister`. This location is not confirmed-current: attempts
with the current default SSH keys and `nas_openlist` return
`Permission denied (publickey)`. The endpoint advertises public-key-only auth;
the supplied NAS password does not resolve that boundary. Local Docker and
NAS Docker do not contain an EasyRegister container to switch instead.

Required operator input: confirm the current EasyRegister deployment host and
provide an authorized SSH private-key path or another working management
entrypoint for that host.

After access is available:

1. Inspect the exact running image, mounts, compose project, restart policy,
   scheduler activity, and non-secret effective environment. Do not dump env
   or inspect JSON containing credentials into logs.
2. Preserve the current image, operator-only environment/configuration, and
   output mounts as the rollback baseline. Drain active work before recreation.
3. Update the deployment's persisted email/proxy endpoints and controlled
   credentials; account for existing bootstrap precedence. Use the established
   `deploy-host.ps1` -> `scripts/deploy-compose.ps1` path, not an ad hoc parallel
   deployment and not `compose down -v`.
4. Inspect the new container's actual environment and image. Run the service
   verification from that runtime network, then the bounded real DST acceptance
   flow required by the original task.
5. Check real receipt/OTP behavior and downstream output contracts separately
   from catalog/readiness. Keep paid SMS and account-processing side effects
   within the operator's intended test scope.
6. On failure, restore only the recorded EasyRegister deployment baseline;
   preserve the NAS SDK, gateway, relay, and persistent data.

Until these steps are performed, this work must not be described as a completed
production migration or a fully accepted DST deployment.
