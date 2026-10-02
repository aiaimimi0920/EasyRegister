# PC2 small-success audit, 2026-09-09

Observed at 2026-09-09 00:01:46 Asia/Shanghai
(2026-09-08 16:01:46 UTC), using read-only PC2 logs and persisted artifacts.

Terminal-error classification corrected on 2026-09-11 by rereading the same
historical task records and selecting `stepErrors[errorStep].code`.

The later email-ID repair and its separate post-deployment sample are recorded
in [the 2026-09-11 identity report](pc2-easyemail-message-identity-2026-09-11.md).

## Assessment

Small successes recur, with a high observed failure rate. Current evidence
does not support a stability claim. The recent completed-task sample contains
5 small successes out of 13 tasks (38.5%), 7 OTP timeouts, and 1 missing
authorization login session. The longest consecutive small-success sequence
in this sample was two tasks; the latest
two completed tasks both failed before reaching the milestone.

Here, small success requires successful account creation, organization
initialization, and ChatGPT login initialization. Persisted intermediate
artifacts were also checked using the deployed
`validate_openai_oauth_seed_payload(..., enforce_max_age=False)` contract.
The subsequent Codex OAuth stage is a separate milestone.

## Completed Tasks

The observation window starts at the EasyEmail transport-fix deployment time,
2026-09-08 11:50:02 UTC. It includes tasks that finished after that time.

| Result | Tasks | Count |
| --- | --- | --- |
| Small success reached; later SMS-policy stop | 126, 129, 133, 135, 136 | 5 |
| Account creation failed with `otp_timeout` | 127, 128, 130, 131, 132, 134, 138 | 7 |
| Account creation failed with `authorize_missing_login_session` | 137 | 1 |
| Complete workflow success | None | 0 |

Task 126 started before the deployment and finished afterward. Excluding that
cross-deployment task, the sample of tasks started and completed after the fix
contains **4 small successes out of 12 tasks (33.3%)**, with 7 OTP failures and
1 authorization-session failure. Neither sample is a controlled before/after
comparison, and neither proves that a particular retry caused success.

Some successful tasks required earlier step retries. Their final successful
milestones determine the classification above; an earlier OTP error somewhere
in a task's result is not counted as a terminal OTP failure.

The latest two completed tasks were:

- Task 137: `authorize_missing_login_session`, finished 2026-09-08 15:16:48 UTC.
- Task 138: `otp_timeout`, finished 2026-09-08 15:43:39 UTC.

Task 139 had started and had no terminal event at the snapshot, so it is
excluded from the completed-task denominator. All 13 completed tasks reported
successful mailbox and proxy release steps.

## Artifact Check

The actual `/shared/register-output/openai/pending` pool contained:

- 14 JSON files associated with 7 distinct accounts.
- 7 completed milestone artifacts passing the deployed historical validator.
- 7 early seeds rejected with `missing_platform_organization`.
- 0 completed artifacts passing the current seed-age gate.

The two oldest completed artifacts predate the transport fix. Five were created
after deployment, at 11:52:26, 12:57:29, 14:13:11, 14:43:57, and 14:56:53 UTC
on 2026-09-08. These agree with the five completed-task milestones above.

The age-gate result concerns eligibility for continuation. This read-only audit
did not reauthenticate any account and does not determine current credential
validity. The dashboard's 14-file count must not be reported as 14 successes.

## Remaining Failures

Seven of the 13 recently completed tasks stopped while account creation waited
for an OTP. Task 137 stopped because its authorization login session was missing.
The cause of specific missing third-party codes remains unresolved. Earlier retry
errors must not override the terminal step's structured error code; tasks 126,
129, and 136 also retain earlier OTP errors despite later reaching small success.

All five tasks that reached small success subsequently stopped at
`obtain-codex-oauth` with `sms_not_enabled_for_business`. That later stop does
not invalidate the earlier milestone, and resolving it alone would not remove
the OTP failures that prevent small success.

The earlier independent dependency acceptance passed three runs and 18 steps.
That test covers proxy acquisition, mailbox acquisition/recovery, and cleanup;
it does not exercise third-party code delivery or account login. Its result
does not establish the small-success rate of the business workflow.

No new registration task, account reauthentication, configuration change,
SMS request, or container restart was initiated during this audit.

See [the dependency acceptance report](pc2-dst-stability-status-2026-09-08.md)
and [the earlier business evidence](pc2-dst-validation-2026-09-08.md).
