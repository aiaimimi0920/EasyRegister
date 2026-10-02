# PC2 paid SMS canary: valid key, protected route, upstream login blocked

This document preserves the 18:17 UTC checkpoint below. Continued repairs and
later executions are recorded in
[pc2-paid-sms-repair-2026-09-12.md](pc2-paid-sms-repair-2026-09-12.md).
The historical container states and continuation instructions below must not
be used to restart a consumed single-run container.

Observation: 2026-09-12 02:17:39 Asia/Shanghai, or 2026-09-11 18:17:39 UTC.
Status: **incomplete; stopped before the first paid order**.

The supplied HeroSMS key is saved and authenticated. The isolated SMS service
was started and tested against the real provider. No real acquisition request
was forwarded, no paid SMS order was created, and SMS spend was **0 USD**.
The final account balance remained **4.5953 USD**. Codex OAuth and a real large
success artifact have not been obtained in this run.

## Credential and live provider checks

The key is stored in the ignored local file
`C:\Users\Public\nas_home\AI\GameEditor\EasySMS\config.yaml`, at
`serviceBase.runtime.providers.heroSms.apiKey`. Its original case was retained;
the file is UTF-8 without a BOM. The key is not included in this report or the
sanitized evidence. The isolated PC2 configuration uses that saved key and a
separately generated EasySMS server Bearer key. Production configuration and the
older generated local deployment configuration were not overwritten.

After the guard transport fix, the real isolated route returned:

- `getBalance`: HTTP 200, balance 4.5953.
- `getServicesList`: HTTP 200, service `dr` confirmed as OpenAI.
- `getPrices`, country 16: HTTP 200, quoted cost 0.045, inventory 3,121.
- `getActiveActivations`: HTTP 200, status `success`, zero active orders in the
  returned `data` array. The additional activation arrays were also empty.

The configured first-order cap is 0.05 USD for the United Kingdom, country 16.
The provider's [OpenAPI schema](https://hero-sms.com/docs/v1/openapi.json?locale=en)
specifies currency 840 (USD). The guard also rejects an acquisition response
that explicitly reports a different currency. No acquisition response was
received in this run because no order was submitted.

## Single-order isolation and transport fix

[paid_sms_canary_guard.py](../scripts/paid_sms_canary_guard.py) commits an
exclusive, durable ledger before forwarding the first acquisition request.
Rejections, timeouts, cancellations, completion, concurrency, and process
restarts cannot reset this allowance. Status changes are restricted to the
recorded activation. Alternate purchase and resend actions are denied.

The separate EasySMS container has only `hero_sms` enabled, no maintenance or
active probing, no number reuse, separate persistent state, and only an internal
Docker network. Its HeroSMS URL points to the guard. A direct external request
from that container failed; the guarded route succeeded. A request exceeding
the cap was rejected locally with HTTP 400 and
`PAID_SMS_APPROVED_SCOPE_MISMATCH`, without creating a ledger.

The initial guard transport received HTTP 403 with provider title
`Error 1010: Access denied`. A controlled read-only comparison found that Python's
default User-Agent failed while the browser User-Agent already used by EasySMS
returned HTTP 200. The guard now supplies that User-Agent. The regression
fixture reproduced two failures before the fix. All 19 guard tests then passed
on Windows and in a PC2 container using the current orchestrator image and
`--network none`. The latter used local HTTP fixtures only.

The single-run wrapper imports successfully inside the current PC2 image. It
checks the isolated route, business policy, country, price, all retry limits,
one normally age-valid seed, and flow attempt limits before running the real
continuation entry point. Its execution marker also prevents a duplicate run.
The first paid continuation container was created but **never started**.

## Current OpenAI blocker

The earlier [real small-success result](pc2-real-small-success-2026-09-11.md)
remains valid historical evidence. At the final observation, the production
pending pool contained 55 structurally valid historical seeds, all older than
the normal 900-second limit, and 55 incomplete artifacts without completed
platform organization metadata. There were **zero current age-valid seeds**.
The age limit was retained. Historical classification without an age check was
used only for reporting, never to admit a seed to the paid flow.

Recent production tasks were failing at `create-openai-account`, with
`browser_verification_required`, `stage_platform_login`, HTTP 403, and
`cf-mitigated=challenge`. This prevented new complete small-success seeds from
being produced.

One additional isolated, SMS-disabled account flow started at
2026-09-11 18:06:38 UTC. Its task and step retries were limited to one. It also
tested a strict platform-login proxy probe instead of relying on an auth-only
probe. Proxy and mailbox acquisition succeeded, but the real registration
request still hit the same browser-verification response. Mailbox and proxy
release succeeded. The semantic result was `ok=false`; OAuth, phone
verification, personal OAuth validation, and final artifact upload were skipped.
The temporary diagnostic wrapper's process exit code was zero, which is not
used as success evidence.

The stricter probe did not resolve the runtime challenge. No production auth
code was changed on the basis of that unsuccessful experiment. The OpenAI
browser-verification boundary remains unresolved; the HeroSMS transport fix
does not resolve it.

## Final runtime state and continuation boundary

Both isolated paid support containers are stopped, with restart policy `no`.
The first paid continuation container remains in `created` state, and the
SMS-disabled seed diagnostic has exited. The purchase ledger does not exist.
The four production containers retain their original identities, images, and
start times, are running with zero restarts, and the orchestrator and Python
provider still have paid SMS disabled.

The isolated deployment is retained at
`/home/mjc/easyregister/paid-sms-canary/20260912-001`. Private logs and results
are retained under
`/home/mjc/easyregister/output/paid-sms-canary-20260912-001/seed-refresh`.
They contain account material and must not be printed or copied into public
reports. Private directories use mode 700 and credential files use mode 600.

Remaining work is to resolve the OpenAI login challenge and obtain one fresh
seed through the normal successful flow. Copy that seed into the isolated
`seed-pool`, preserving the production original and a private backup. Revalidate
its normal age, restart only the isolated guard/SMS services, refresh the quote,
and start the already prepared paid continuation exactly once. Keep the same
first-attempt ledger. A paid failure must be diagnosed, fixed, and verified
before a separately reviewed second attempt is launched.

A real large success still requires completed Codex OAuth, successful
`validate-free-personal-oauth`, a matching valid final artifact on disk, and
reconciled SMS order/cost evidence. The current result does not meet those gates.

The final sanitized runtime observations are in
[pc2-paid-sms-live-20260912-001.json](../deploy-evidence/pc2-paid-sms-live-20260912-001.json).
Preparation and execution helpers are
[pc2_paid_sms_prepare.py](../scripts/pc2_paid_sms_prepare.py) and
[pc2_paid_sms_once.py](../scripts/pc2_paid_sms_once.py). Preparation is intentionally
not a repeatable launcher for an existing canary directory.
