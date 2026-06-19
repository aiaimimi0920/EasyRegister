# NAS Result Pool Storage Design

## Goal

Centralize only EasyRegister credential result pools on the NAS so other LAN machines can consume them, while keeping internal runtime state local to the SelfDocker deployment.

## Approved boundary

Move these result-facing pools to NAS:

- `runtime/register-output/codex` -> `Z:\oauth\codex`
- `runtime/register-output/openai` -> `Z:\oauth\openai`

Keep implementation/runtime details local:

- `runtime/register-output/others`

This preserves a clean shared NAS surface for credentials and avoids exposing claim files, run directories, locks, mailbox/SMS state, bridge staging, and audit implementation details.

## Runtime shape

The container path contract remains unchanged:

```text
/shared/register-output/codex
/shared/register-output/openai
/shared/register-output/others
```

Deployment supplies nested mounts for only the result pools:

```text
host local output root -> /shared/register-output
NAS codex root        -> /shared/register-output/codex
NAS openai root       -> /shared/register-output/openai
```

`others` remains under the host local output root.

## Account-audit requirement

`account-audit` must continue to scan and mutate the credential pools after the split. In particular, when a result is classified as deleted/deactivated/disabled, finalization must remove same-email files from the NAS-backed `openai/*` and `codex/*` result pools. The scanner must not depend on `Path.rglob()` over the `codex` root because that can miss linked result-pool directories on some host layouts.

## Deployment behavior

`deploy-host.ps1` will accept a result credential root and generate a compose override for nested `codex` and `openai` result-pool mounts. It also keeps existing fine-grained overrides for operators who need custom host paths.

The script creates the expected pool directories before compose runs:

- `codex/free`
- `codex/team`
- `codex/plus`
- `codex/team-input`
- `codex/team-mother-input`
- `openai/pending`
- `openai/converted`
- `openai/failed-once`
- `openai/failed-twice`
- local `others`

## Docker mapped-drive caveat

Current live evidence showed Docker Desktop did not reliably accept direct `Z:\oauth` bind-mount probes. The implementation therefore adds script-level support and static validation first. Live migration must still use copy-verify-switch and a Docker mount preflight before replacing the running container.
