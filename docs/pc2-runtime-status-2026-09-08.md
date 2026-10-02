# PC2 runtime deployment result - 2026-09-08

> Historical bounded-canary checkpoint. Continuous operation and the later
> migration audit are recorded in `pc2-continuous-observation-2026-09-08.md`.
> The exited/non-continuous state below describes the earlier checkpoint only.

## Current outcome

Infrastructure deployment and service integration are complete. Successful
account registration is **not** established. Continuous registration is not
enabled while the observed target-site HTTP 403 remains unresolved.

Target: `mjc@192.168.15.104`, Linux host `pc2`.
Deployment root: `/home/mjc/easyregister`.
Final observation: `2026-09-08T00:09:53Z` (08:09, Asia/Shanghai).

| Component | Final state | Notes |
| --- | --- | --- |
| EasyRegister | Exited 0 intentionally | One bounded top-level task completed; the task itself failed |
| EasyProtocol gateway | Running, 0 restarts | Authenticated control plane; automatic container scaling disabled |
| PythonProtocol executor | Running, 0 restarts | One worker, shared output volume |
| EasySMS | Running, 0 restarts | Server authentication and persistent state enabled |
| New EasyEmail | Authenticated catalog passed | NAS `http://192.168.15.200:18081` |
| New EasyProxy | Authenticated API passed | Gateway `http://192.168.15.201:29888` |

The nine pre-existing business containers retained the same IDs and start times
across this dependency-deployment phase. All nine remained healthy.

## Deployed artifacts and configuration

- EasyRegister image: `easy-register/easy-register:nas-dst-20260908-003`.
- Exact image ID:
  `sha256:dab0f426e409201315c933d2840710780b2d6dd9bfd8dc305aa167c6171320a0`.
- Dependency images were existing, specifically versioned artifacts, not newly
  claimed upstream releases. Their exact IDs were checked after transfer and
  are recorded in `deploy-evidence/pc2-runtime-20260908-003.json`.
- Network: `easyregister-pc2`, separate from the existing application network.
- Gateway endpoint inside the network: `http://easy-protocol:9788`.
- SMS endpoint inside the network: `http://easy-sms:8080`.
- Gateway and SMS receive newly generated server/control-plane credentials.
  SMS provider credentials are separate and came from the controlled EasySMS
  configuration; they are not included in repository artifacts or reports.
- Runtime configuration and backups on PC2 have mode 0600. The root directory
  has mode 0700. Existing runtime configuration was backed up before each change.
- Gateway dynamic scaling is disabled and the Docker socket is not mounted.
- Python worker count is one. Gateway/Python/SMS memory limits are respectively
  384 MiB, 1536 MiB, and 512 MiB; the register canary is limited to 512 MiB.
- Paid SMS remained disabled. SMS background maintenance/active probing was
  disabled for this controlled deployment.
- The temporary loopback registry and SSH transfer tunnel were removed.

The provider wrote a unique marker through `/shared/register-output`, and an
EasyRegister image container read and removed it through its own mount. This
verified the actual shared-output contract, not merely matching path strings.

## Fresh verification

Ten interface checks passed after all dependencies were running:

1. Authenticated EasyEmail catalog.
2. EasySMS health.
3. Authenticated EasySMS catalog.
4. EasyProtocol health.
5. Authenticated EasyProtocol control plane.
6. PythonProtocol health.
7. Authenticated EasyProxy nodes API.
8. EasySMS rejects an unauthenticated catalog request with HTTP 401.
9. EasyProtocol rejects an unauthenticated control-plane request with HTTP 401.
10. A real PythonProtocol `protocol.echo` call returned the expected marker.

The dashboard `/api/status` returned HTTP 200 during the canary. It is bound to
PC2 loopback (`127.0.0.1:19790`), not exposed to the LAN. Because the bounded
supervisor has now exited, the dashboard is not currently serving; an earlier
HTTP 200 must not be described as current continuous availability.

## Deployment-discovered retry bug

The first canary stopped at proxy probing. Its curl error was
`Connection timed out after 20001 milliseconds`. The classifier recognized
`timeout`, but not `timed out`, so this network failure was classified as
unknown and missed the existing route-retry path.

The narrow fix adds the `timed out` marker to
`server/services/orchestration_service/src/others/runtime_proxy_probe.py`.
Regression samples cover both connection and operation timeout wording.
The test failed before the change, then passed after it. No TLS behavior,
accepted status codes, or target-site success criteria were relaxed.

- Focused tests: 45 passed.
- Full migration regression group: 220 passed, 0 failed, in 80.54 seconds.
- `git diff --check`: passed.
- Deployment helper compilation and UTF-8/no-BOM checks: passed.
- The packaged modified source SHA256 matches the worktree:
  `f5ace63f2405c30046d2a5496f0d6129f7240868aec09ccad298745fc14c2f06`.

## Actual business canary and remaining blocker

The revised image ran one top-level task. It reached real EasyProtocol and
PythonProtocol execution after successful proxy and mailbox acquisition.
The task's built-in step recovery made three attempts at account creation.

Observed terminal results:

- Proxy acquisition: passed.
- Mailbox acquisition: passed.
- `create-openai-account`: failed, with `platform_login status=403` returned
  through `PythonProtocol-001`.
- Later organization/OAuth/team steps: skipped.
- Proxy and mailbox release steps: completed.
- The supervisor reached its configured maximum run count and exited normally.

**Exit code 0 belongs to the bounded supervisor, not to successful account
creation.** The exact reason for the target site's HTTP 403 has not been
established. It is not the earlier missing-service/configuration blocker, and
it must not be converted into success or hidden by unlimited retries.

Before enabling unattended registration, diagnose the target-site access denial
and validate a complete successful task. Do not enable paid SMS or loosen probe
acceptance merely to obtain a green deployment status.

## Operations and rollback

On PC2, dependency operations use:

```sh
docker compose --project-name easy-register-deps \
  -f /home/mjc/easyregister/dependencies.yaml ps
```

The bounded canary is reproducible with the following command, but running it
starts another real registration task; it is not a read-only health check:

```sh
docker compose --project-name easy-register \
  --env-file /home/mjc/easyregister/runtime.env \
  -f /home/mjc/easyregister/compose.yaml \
  -f /home/mjc/easyregister/canary.yaml \
  up -d --no-build --no-deps easy-register
```

For safe interface-only verification, use `scripts/verify-pc2-runtime.py` as
documented by the deployed `/home/mjc/easyregister/verify-runtime.py` artifact.
It performs no registration or SMS purchase.

Rollback inputs remain on PC2: the prior `nas-dst-20260908-002` image,
`runtime.env.before-register-003`, and the timestamped
`runtime.env.before-dependencies-*` backup. Preserve all output, team-auth,
dependency data and configuration directories. Do not prune Docker or stop
unrelated projects as part of rollback.
