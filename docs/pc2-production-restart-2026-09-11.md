# PC2 production restart, 2026-09-11

The user-requested restart of `easy-register` is complete. The latest fixed
81-minute observation contains 24 completed tasks and no new small success.
All 24 failed during account creation: 21 with `authorize_continue_blocked`
and three with `authorize_missing_login_session`. The initial 29-minute sample
is retained below, followed by the continuation snapshot.

## Restart verification

PC2 is the Linux host `mjc@192.168.15.104`. Only its `easy-register` production
container was targeted. Its ID and image were preserved:

- Container ID: `2aeba75d928ef5488e01e7e9725576fb724c306fe43f4f88e19d41571ebf1c16`.
- Image: `easy-register/easy-register:nas-dst-20260908-003`.
- Image ID: `sha256:dab0f426e409201315c933d2840710780b2d6dd9bfd8dc305aa167c6171320a0`.
- Previous start: `2026-09-08T00:27:46.701209638Z`.
- Stop time: `2026-09-10T22:47:23.802015748Z`.
- New start: `2026-09-10T22:50:02.894083822Z`, or September 11 at 06:50:02
  Asia/Shanghai.

The remote orchestration call returned exit 1 without retaining its detailed
error. A subsequent direct Docker inspection confirmed the new start time and
running state. No second restart was issued. The verification snapshot at
23:19:17 UTC confirmed that the container was still running and the dashboard
`/api/status` returned HTTP 200.

The Python executor, protocol gateway, and SMS containers retained their
pre-restart IDs, image IDs, and start times. No dependency container, NAS email
service, production configuration, or production source was changed in this
restart operation.

## Initial post-restart result

Task indexes below belong to the newly started process. The observation cutoff
is 2026-09-10 23:19:17 UTC, or September 11 at 07:19:17 Asia/Shanghai.

| Terminal error | Tasks | Count |
| --- | --- | ---: |
| `authorize_missing_login_session` | 1, 3 | 2 |
| `authorize_continue_blocked` | 2, 4, 5, 6, 7, 8, 9 | 7 |
| New small success | None | 0 |
| Complete workflow success | None | 0 |

All nine tasks acquired a mailbox, released the mailbox, and released the proxy
successfully. Their account, organization, and login initialization milestones
did not all complete. No task was active at this fixed snapshot; the production
supervisor remained running.

The pending artifact directory was independently read through the deployed
`others.common_runtime.validate_openai_oauth_seed_payload` contract. Before and
after the restart it contained 72 files, including 36 structurally complete
historical milestone artifacts. None passed the current age gate, and no new file
or new valid milestone artifact appeared after the restart. These historical
artifacts are not counted as successes caused by this restart. The age gate does
not determine current account credential validity.

The observed restart did not establish small success. The current terminal
failures remain in authorization/session initialization, while mailbox and proxy
resource operations passed. No success criterion or authorization check was
relaxed to change the result.

## Continuation snapshot

Continuation of session `01a08cd9-c6e4-7f02-928b-5f66d3bc758f` captured a fresh
read-only snapshot at 2026-09-11 00:11:34 UTC, or 08:11:34 Asia/Shanghai. This is
81 minutes and 31 seconds after the verified restart. All 24 completed tasks
started after that restart; task indexes remain scoped to the same process.

Tasks 1, 3, and 15 ended with `authorize_missing_login_session`. The other 21
ended with `authorize_continue_blocked`. All stopped at `create-openai-account`;
none reached the three-step small-success milestone or complete workflow success.
The 15 additional tasks since the first snapshot contributed 14 authorization
blocks and one missing login session.

A second read-only log probe used both the same start time and an explicit end
time equal to this snapshot. It selected only `stepErrors[errorStep]` from each
terminal event. All 21 authorization-block records contain an HTTP 403 marker;
no record contains an HTTP 429 marker. Generic challenge-related words occur in
the diagnostics, but none of the selected challenge HTML/header markers was
retained. These records establish authorization failures without identifying
the remote service's reason for rejecting the requests.

All 24 tasks successfully acquired and released their mailbox and proxy. The
dashboard returned HTTP 200, and the production container and three dependencies
were running. Their IDs, image IDs, and start times exactly matched the first
snapshot. No additional restart or task dispatch was performed in this
continuation, and no production source or configuration was changed.

The artifact pool still contained 72 files and 36 historical milestone-valid
artifacts. Current-age-valid artifacts, new files, and new milestone-valid
artifacts all remained zero. No task was active at the fixed snapshot. Both the
completed-task records and the persisted-artifact audit therefore show no new
small success after this restart. Restart verification and resource cleanup
passed; the business milestone remains unachieved at authorization/session
initialization.

The original nine-task evidence is unchanged. The new fixed snapshot, per-task
HTTP status markers, container comparisons, and explicit diagnostic limitations
are saved in
[`pc2-production-restart-20260911-002.json`](../deploy-evidence/pc2-production-restart-20260911-002.json).
The full read-only audit and the bounded terminal-diagnostic probe both exited 0.

## Reproducible audit

The new read-only helper is
[`scripts/audit-pc2-restart-results.py`](../scripts/audit-pc2-restart-results.py).
It reads only container state, logs, dashboard status, and persisted artifacts;
it does not restart a container or start an additional task. Credentials, email
addresses, raw messages, and account tokens are omitted from its output.

```text
rtk proxy python scripts/audit-pc2-restart-results.py --since 2026-09-10T22:50:02.894083822Z
```

The initial artifact audit failed because the deployed Python 3.10.21 rejected
Docker's nine-digit fractional timestamp. The helper now normalizes that input
to microseconds. A real runtime probe reproduced the rejection, and the corrected
full read-only audit exited 0 with the results above.

The sanitized operation and per-task evidence is
[`pc2-production-restart-20260911-001.json`](../deploy-evidence/pc2-production-restart-20260911-001.json).
See the [preceding email-identity report](pc2-easyemail-message-identity-2026-09-11.md)
for the separately verified NAS repair. No Git commit or push was performed here.
