# OpenAI Community Login DST Design

## Goal

Add a new, isolated DST path that consumes a main-flow `small-success` artifact and verifies that the account can complete web login to `https://community.openai.com/`.

## Scope

- Add a new EasyRegister DST file; do not modify the existing `codex-openai-account-v1`, continue, or team flow semantics.
- Add a new EasyProtocol step type, `login_openai_community`; do not change existing `initialize_chatgpt_login_session`, `obtain_codex_oauth`, or invite behavior.
- Reuse EasyBrowser's existing `openai_web_login` flow for browser-backed OpenAI login.
- Treat Community login as a login-only OIDC flow, not as Codex OAuth callback acquisition.

## Current login entry evidence

The Community site is a Discourse frontend. Its Log In button first calls `https://community.openai.com/session/csrf`, then redirects into OpenAI OIDC:

`https://auth.openai.com/api/accounts/authorize?client_id=app_OAPq7KYs3GglgRo2MROIGfSB&redirect_uri=https%3A%2F%2Fcommunity.openai.com%2Fauth%2Foidc%2Fcallback&response_type=code&scope=openid+email+profile&state=...`

Automated browser inspection reached Cloudflare challenge at `auth.openai.com`, so this should remain browser-backed.

## Architecture

`openai-community-login-v1.semantic-flow.json` claims one configured small-success input artifact, acquires a proxy suitable for Community/Auth, dispatches `login_openai_community` to EasyProtocol, and releases the proxy as cleanup.

EasyProtocol's new step reads the artifact's email/password/mailbox metadata and delegates to a focused EasyBrowser client. The client posts a login flow request with `flow_type=login` and `step_type=openai_web_login`, using `https://community.openai.com/` as the startup URL. The step succeeds only when the browser result returns a Community URL.

## Non-interference rules

- Existing flows are not edited.
- Existing step-type behavior is not repurposed.
- Existing retry classifications remain unchanged unless the new step needs its own classification in a follow-up.
- Existing EasyProtocol dirty changes are preserved; new code must be additive.

## Test strategy

- EasyRegister unit tests load the new DST and prove the new step receives claimed artifact path, proxy, and Community startup URL.
- EasyProtocol unit tests prove the new step builds the EasyBrowser login request from a small-success artifact and rejects non-Community target URLs.
- Existing unittest suites remain the acceptance gate.
