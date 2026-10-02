# PC2 single paid SMS canary: preflight and guard

Historical snapshot. The supplied replacement key now authenticates; see the
[subsequent live canary report](pc2-paid-sms-live-2026-09-12.md) for the current
OpenAI login blocker and the confirmed zero-spend result.

Local date: 2026-09-12, Asia/Shanghai. Final runtime observation: 2026-09-11
16:19:21 UTC. Status: **blocked before the first paid attempt**.

The user requires one paid test at a time. A failure must stop new purchases;
diagnosis, a fix, and verification must precede a second test. A task retry,
provider fallback, cancellation, or process restart must not create another order.

## Observed production boundary

Real small-success artifacts were already established in the
[small-success report](pc2-real-small-success-2026-09-11.md). A fresh read-only
observation at 15:54:02 UTC found six more completed tasks since 15:25:29 UTC,
with the same small-success milestone and an OAuth failure afterward.

The latest inspected task, task 13, failed at `obtain-codex-oauth` with
`sms_not_enabled_for_business`. The running orchestrator has
`REGISTER_SMS_ALLOW_PAID=false` and an empty `REGISTER_SMS_BUSINESS_POLICIES_JSON`.
The current policy resolver marks an unconfigured SMS business as disabled.
Its phone session acquisition call is reached only after a structured phone
verification requirement.

## Live provider preflight

Only read-only HeroSMS actions were issued: `getServicesList`, `getBalance`,
`getPrices`, and `getActiveActivations`. The service catalog returned HTTP 200
and confirmed `dr` as `OpenAI`.

The authenticated balance, price, and active-order endpoints returned HTTP 401.
A follow-up using the provider's configured user agent confirmed the response
title `BAD_KEY`. The configured key is present. The keys in both the current
local root config and the generated local deployment config match PC2, so there
is no different current local key available to reconcile automatically.

Balance, currency, available quotes, and existing account-wide orders remain
unknown. Unauthorized responses are not evidence of zero balance, no inventory,
or no existing orders.

This task issued **zero real acquisition requests and zero paid SMS send
requests**. No paid order was created by this task. It does not make a claim
about unrelated activity on the provider account.

The provider key to update is
`serviceBase.runtime.providers.heroSms.apiKey` in
`C:\Users\Public\nas_home\AI\GameEditor\EasySMS\config.yaml`.
It is separate from the EasySMS HTTP server's Bearer API key.

## Prepared single-order guard

[paid_sms_canary_guard.py](../scripts/paid_sms_canary_guard.py) is an opt-in
HeroSMS reverse proxy for an isolated canary. It has not been connected to the
production paid path. Starting the proxy alone does not buy a number.

The guard requires an explicit country, service, positive finite price cap, and
an absolute ledger path. Before forwarding `getNumberV2`, it creates the ledger
exclusively and fsyncs it. All later acquisitions using that ledger are denied,
including after timeouts, rejected orders, completion, cancellation, or restart.
This also fences concurrent callers and EasySMS's internal provider retries.

Alternate purchase actions and SMS resend actions are denied. Status polling,
completion, and cancellation are limited to the activation recorded by this
attempt. Upstream redirects are not followed. The ledger and receipt omit the
provider key, phone number, message body, and OTP.

The price is compared in the provider's native unit; the guard does not infer or
convert currency. Currency and the user's per-order cap must be established
before any live launch. Never delete or rotate a consumed ledger as part of an
automatic retry. A second reviewed test needs its own explicit launch after the
first failure has been handled.

## Verification

[test_paid_sms_canary_guard.py](../tests/test_paid_sms_canary_guard.py) passed
all 18 checks on Windows and in a temporary PC2 container using the current
orchestrator image
`sha256:a2da348213fe60d102c157ad290455527b94deb3928025e29606ffe8a793f20c`.
The PC2 container used `--network none`; provider traffic in the tests used
loopback fixtures only. It was removed after the run.

```powershell
rtk proxy python -m unittest discover -s tests -p test_paid_sms_canary_guard.py -v
```

Coverage includes simultaneous callers, uncertain transport results, HTTP
redirects, failed ledger/receipt writes, wrong prices or country/service,
refunds, restarts, ownership checks, and real local HTTP proxy forwarding.
Both source files passed UTF-8 without BOM and whitespace checks.

The final live inspection found all four production containers running with
unchanged identities and start times, zero restarts, and Dashboard HTTP 200.
The orchestrator and Python provider still have paid SMS disabled. No temporary
offline proof container remains.

## Remaining execution

1. Obtain a valid provider key and the user's maximum per-order amount. Validate
   the key and obtain an authenticated quote with its currency before deployment.
2. Connect an isolated SMS instance to the guard. Verify its effective HeroSMS
   base URL and a fixed persistent ledger for attempt 1. Keep continuous workers
   on their existing non-paid policy; a single worker alone does not bound cost.
3. Use one fresh, normally age-valid small-success seed for the one-shot OAuth
   continuation. Enable the SMS business policy only for that isolated attempt.
4. On failure, stop acquisitions, retain stage and receipt evidence, reconcile
   the provider order and any refund, then fix and verify before attempt 2.
5. Claim real large success only after completed Codex OAuth, free personal OAuth
   validation, and a matching persisted final artifact. Renting a number,
   receiving an SMS, or passing phone verification alone is insufficient.

The exact sanitized observations, source hashes, and test results are in
[the preflight evidence](../deploy-evidence/pc2-paid-sms-preflight-20260912-001.json).
