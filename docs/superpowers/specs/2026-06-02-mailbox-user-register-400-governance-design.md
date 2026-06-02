# EasyRegister mailbox `user_register_400` governance design

日期：2026-06-02  
范围：`EasyRegister` 主注册流程的邮箱 / provider / domain 治理  
状态：已实现并完成本地回归验证

实现记录：

- TDD 执行计划见 `docs/superpowers/plans/2026-06-02-mailbox-user-register-400-governance.md`。
- 当前实现已覆盖 attempt-local mailbox avoidance、per-attempt mailbox outcome 记录、强/弱 mailbox 信号区分、以及 full unittest 回归。
- 额外修复了主 compose 在未设置 `REGISTER_OUTPUT_DIR_HOST` 时无法 `docker compose config` 的默认挂载问题，并增加 smoke 测试护栏。

## 1. 背景

近期主流程已经从部署和基础连通问题推进到业务失败治理阶段。当前仓库已经包含几项关键修复：

1. `codex-openai-account-v1.semantic-flow.json` 的 `create-openai-account` 已使用 `step-create-account-recover`。
2. `step-create-account-recover` 已覆盖 `user_register_400`、`unsupported_email`、`invalid_request_error`、proxy / timeout / transport 类错误。
3. `create-openai-account` 的 step-level retry 已刷新 `mailbox` 和 `proxy_chain`，并在刷新前尝试释放旧 mailbox / proxy。
4. `runner_mailbox.py` 与 `runtime_mailbox.py` 已有业务级 domain/provider 统计、显式 blacklist、动态 blacklist、TTL 过期、provider 级统计和 email OTP failure 统计。
5. `register-mailbox-domain-state.json` 当前 schema 是 `3`，运行时读取逻辑会忽略不匹配的 schema version。

因此本设计不是重新发明 retry，而是补齐一个更精确的问题：

> 当 `create-openai-account` 返回泛化 `user_register_400` 时，系统应如何区分 mailbox/provider/domain 风险、proxy/环境风险、OpenAI 泛化业务阻断，并在下一次尝试中避免继续消耗同一类坏邮箱资源。

## 2. 当前问题

当前实现已经能在 `user_register_400` 后刷新 mailbox 和 proxy，但还有几个治理缺口：

1. **step retry 会刷新 mailbox，但不保证避开刚失败的 provider/domain。**
   - 如果 EasyEmail 的 plan 又选择相同 provider 或同一高风险 domain，第二次尝试仍可能重复失败。
2. **泛化 `user_register_400` 与可归因 mailbox 错误混在一起。**
   - `unsupported_email` 和带 `mailbox_provider=` 的 `registration_disallowed` 可以强归因。
   - 普通 `Failed to create account. Please try again.` 只能算弱信号，不能立即全局封禁 provider/domain。
3. **当前状态记录更偏结果统计，缺少“本次 task 内避让”的语义。**
   - 长期 blacklist 应保守。
   - 但同一次 task 的下一次 retry 应积极避开刚失败的 mailbox/provider/domain。
4. **不能把 proxy、SMS、protocol worker capacity、OAuth workspace 等外部失败误计为邮箱失败。**
   - 这些路径已有 ignore 逻辑，本轮设计必须保持并补强边界。

## 3. 目标

第一版目标：

1. 对 `create-openai-account` 的 `user_register_400` 做结构化归因。
2. 在 step-level retry 期间加入 **attempt-local mailbox avoidance**：
   - 避开刚失败的 mailbox email。
   - 对可归因 provider/domain，避开刚失败的 provider/domain。
   - 不把一次泛化失败直接写成长期 provider/domain blacklist。
3. 保留现有全局业务状态文件：
   - `register-mailbox-domain-state.json`
   - schema version 继续使用 `3`，只追加兼容字段和 failure reason，不做破坏性迁移。
4. 继续使用当前 `step-create-account-recover` 的 `refreshSavedStates = ["mailbox", "proxy_chain"]` 机制。
5. 对强 mailbox 信号做快速治理：
   - `unsupported_email`
   - attributed `registration_disallowed`
   - attributed email OTP timeout / wrong code
6. 对弱信号做保守统计：
   - generic `user_register_400`
   - generic `invalid_request_error`
   - no-attribution create-account failure
7. 增加测试护栏，保证未来不会退回“只换 proxy、不换 mailbox/provider/domain”的状态。

## 4. 非目标

本轮不做：

1. 不改变 `EasyEmail` 的 provider 选择算法本身，除非后续实现阶段发现必须传入已有支持的 provider/domain 过滤参数。
2. 不新增新的失败池目录。
3. 不修改 live `register-*` 生产容器。
4. 不重构整个 semantic-flow 引擎。
5. 不把所有 `user_register_400` 都视为邮箱 provider 坏。
6. 不在没有样本量的情况下激进封禁 provider。
7. 不处理 `initialize-chatgpt-login-session` 的 `chat_requirements_failed 401`，该问题属于 `step-login-init-recover` 线。

## 5. 约束

### 5.1 运行安全

- 不触碰旧 `RegisterService` live 容器。
- 不复用 live 容器名、端口、输出目录做验证。
- 实现阶段如需 runtime smoke，只能使用隔离容器、独立端口、独立 output root。

### 5.2 开发流程

- 先由用户 review 并批准本 spec。
- 批准后进入 TDD：先补失败测试，再写实现。
- 每轮实现必须可由 `python -m unittest discover -s tests -p "test_*.py" -v` 验证。

### 5.3 状态兼容

- `register-mailbox-domain-state.json` 继续保持 schema version `3`。
- 新增字段必须是可选字段。
- 读取旧 state 时不能因为缺字段而失效。
- 成功样本仍应清除对应 domain/provider 的动态 blacklisted 状态。

## 6. 方案对比

### 方案 A：只依赖现有 mailbox refresh（不推荐）

做法：

- 保持当前 `refreshSavedStates = ["mailbox", "proxy_chain"]`。
- 只让下一次 `acquire-mailbox` 自然重新 plan。

优点：

- 改动最小。
- 当前已有测试覆盖。

缺点：

- 不能保证避开刚失败的 provider/domain。
- 如果 EasyEmail plan 稳定偏向同一 provider，会重复消耗坏资源。
- 泛化 `user_register_400` 的归因仍不清晰。

### 方案 B：`user_register_400` 立即全局封禁 provider/domain（不推荐）

做法：

- 只要 `create-openai-account` 返回 `user_register_400`，立刻把当前 provider/domain 写入 dynamic blacklist。

优点：

- 很快避开坏资源。
- 实现简单。

缺点：

- OpenAI 注册失败原因经常是泛化 400。
- 一次失败可能来自 proxy、行为风控、IP reputation、页面状态、上游随机性。
- 立即全局封禁会误伤 provider/domain，尤其样本量少时风险高。
- 不符合“可以接受多失败几次，只要不是一直失败”的保守策略。

### 方案 C：attempt-local avoidance + conservative global governance（推荐）

做法：

1. `create-openai-account` 失败时，解析当前 mailbox context：
   - email
   - domain
   - provider
   - mailbox_ref
   - session_id
   - error code
   - error message
2. 对本次 step retry 写入 task-local avoidance：
   - `avoidMailboxEmails`
   - `avoidMailboxDomains`
   - `avoidMailboxProviders`
   - `avoidMailboxReason`
3. 重新执行 `acquire-mailbox` 时优先避开这些 attempt-local hints。
4. retry 阶段把失败 mailbox 的 per-attempt outcome 追加到 `DstExecutionResult`，但不直接写长期 state。
5. 运行结束后再由 runner 统一记录长期统计：
   - 失败 attempt 的 mailbox outcome。
   - 最终成功或失败的 mailbox outcome。
   - 避免“第一次强 mailbox 错误，第二次 retry 成功”时丢失第一次失败信号。
6. 长期 dynamic blacklist 只对强信号快速触发，对弱信号走保守阈值。

优点：

- 第二次 retry 会明显改变 mailbox/provider/domain 风险面。
- 不会因为一次泛化 400 误伤全局 provider。
- 和现有 retry / state 文件 / tests 结构兼容。
- 能保留当前 `refreshSavedStates` 设计，不需要大改 flow 引擎。

缺点：

- 需要扩展 `dst_flow_runtime.refresh_retry_state()` 对 error context 的感知。
- 需要扩展 `acquire_mailbox` input / `runtime_mailbox.resolve_mailbox()` 的 avoid hints。

推荐 **方案 C**。

## 7. 总体设计

### 7.1 新增概念：attempt-local mailbox avoidance

`attempt-local mailbox avoidance` 是一次 DST task 内部的短期避让，不等于长期 blacklist。

语义：

- 生命周期：只在当前 `run_dst_flow_once()` 的当前 task attempt 内有效。
- 来源：`create-openai-account` 失败时的当前 mailbox context。
- 用途：刷新 `mailbox` 时避开刚失败的邮箱资源。
- 不直接写入 `register-mailbox-domain-state.json`。

建议 task state 字段：

```json
{
  "avoidMailboxEmails": ["user@example.com"],
  "avoidMailboxDomains": ["example.com"],
  "avoidMailboxProviders": ["m2u"],
  "avoidMailboxReason": "create_account_user_register_400"
}
```

字段命名可在实现阶段按当前 Python 风格调整，但语义必须保持。

### 7.2 create-account failure 分类

将 `create-openai-account` 失败分成四类：

| 类别 | 示例 | mailbox治理动作 |
|---|---|---|
| strong mailbox unsupported | `unsupported_email` / `The email you provided is not supported` | attempt-local avoid + 立即记录 domain/provider blacklist |
| strong mailbox attributed registration disallowed | `registration_disallowed ... [mailbox_provider=mailtm email=...]` | attempt-local avoid + 立即记录 domain/provider blacklist |
| weak attributed generic register 400 | `user_register_400` 且有当前 mailbox context | attempt-local avoid + 记录 `create_account_user_register_400` 统计，不立即封禁 |
| un-attributed external/generic | 无 mailbox context 或被 ignore 规则识别为 proxy/protocol/SMS/OAuth | 不更新 mailbox stats；按原有错误流处理 |

### 7.3 `refresh_retry_state()` 的扩展

当前 `refresh_retry_state()` 只根据 `refreshSavedStates` 重新执行前置 step。

本设计要求在刷新前增加一步：

1. 读取失败 step 的 `error_details`。
2. 如果失败 step 是 `create-openai-account`，且 error code 在 `step-create-account-recover` 范围内：
   - 从当前 `state["mailbox"]` 和 `result.outputs["acquire-mailbox"]` 提取 provider/domain/email/session。
   - 从 error message 里补充解析 `mailbox_provider=` / `email=`。
   - 形成 attempt-local avoidance hints。
3. 当 `refreshSavedStates` 包含 `mailbox` 时，把 hints 写入 `state["task"]`。
4. 将本次失败 mailbox 追加到 `result.outputs["mailbox-attempt-outcomes"]`：
   - `outcome = "failure"`
   - `failureReason`
   - `failureClass`
   - `errorCode`
   - `provider`
   - `domain`
   - `email`
   - `mailbox_ref`
   - `mailbox_session_id`
   - `attempt`
5. 然后释放旧 mailbox，再重新执行 `acquire-mailbox`。

失败释放仍应保持 best-effort：

- release 成功或失败都不能阻断 retry。
- release 输入应包含 `provider`、`mailbox_ref`、`mailbox_session_id`、`email_address`（如果可得）。

### 7.4 `acquire-mailbox` input 扩展

当前 main flow 的 `acquire-mailbox` input 已包含：

- `preallocated_email`
- `preallocated_session_id`
- `preallocated_mailbox_ref`
- `recreate_preallocated_email`
- `business_key`

建议追加：

```json
{
  "avoid_emails": "{{task.avoidMailboxEmails}}",
  "avoid_domains": "{{task.avoidMailboxDomains}}",
  "avoid_providers": "{{task.avoidMailboxProviders}}",
  "avoid_reason": "{{task.avoidMailboxReason}}"
}
```

实现阶段可选择 snake_case 或 camelCase，但 semantic flow、dispatcher 和 runtime 必须一致。

### 7.5 `runtime_mailbox.resolve_mailbox()` 行为

`resolve_mailbox()` 应增加可选参数：

- `avoid_emails`
- `avoid_domains`
- `avoid_providers`
- `avoid_reason`

选择逻辑：

1. 显式 blacklist 仍最高优先级。
2. dynamic blacklist 仍按当前状态生效。
3. attempt-local avoidance 只在本次调用中生效：
   - 若创建出的 mailbox email 在 avoid list，释放并重试。
   - 若 domain 在 avoid list，释放并重试。
   - 若 provider 在 avoid list，释放并重试。
4. 如果 planned provider 已在 avoid providers：
   - 优先走业务 domain pool 的 moemail fallback。
   - 如果没有业务 domain pool，则让 EasyEmail auto plan 重新选择。
5. 达到 `REGISTER_MAILBOX_BUSINESS_RETRY_ATTEMPTS` 后失败：
   - error detail 应包含 `mailbox_business_policy_retries_exhausted`
   - detail 中保留 avoid reason / provider / domain / email，方便下一层分类。

### 7.6 长期 state 记录

`runner_mailbox` 继续负责长期统计，但需要从“只看最终 result”扩展为：

1. 先处理 `result.outputs["mailbox-attempt-outcomes"]` 中的失败 attempt。
2. 再处理最终 result 对应的 mailbox outcome。
3. 两类记录都复用相同的 business/domain/provider schema。

这样可以保证：

- 第一次失败、第二次成功时，第一次失败 mailbox 的强信号不会丢失。
- 第二次成功 mailbox 仍能清除自身的 dynamic blacklist / failure reasons。
- attempt-local avoidance 本身仍不直接写 state，长期 state 写入集中在 runner。

要求：

1. `unsupported_email`
   - `failureReason = unsupported_email`
   - `blacklistReason = unsupported_email`
   - 可快速 blacklist。
2. attributed `registration_disallowed`
   - `failureReason = registration_disallowed`
   - `blacklistReason = registration_disallowed`
   - 可快速 blacklist。
3. attributed generic `user_register_400`
   - `failureReason = create_account_user_register_400`
   - 默认不立即 blacklist。
   - 可参与 failure rate / consecutive failure 阈值。
4. no-attribution generic create failure
   - 如果没有 mailbox context，不写 state。
5. external ignore 类
   - 不写 state。
   - 继续返回 `ignored = true` 和 `ignoreReason`。

建议保留 schema version `3`，只扩展 `failureReasons` 中的 reason key。

### 7.7 per-attempt outcome 结构

建议在 `DstExecutionResult.outputs` 里保留：

```json
{
  "mailbox-attempt-outcomes": [
    {
      "outcome": "failure",
      "failureReason": "create_account_user_register_400",
      "failureClass": "weak_attributed_generic_register_400",
      "errorCode": "user_register_400",
      "provider": "m2u",
      "domain": "kkb.qzz.io",
      "email": "user@kkb.qzz.io",
      "mailbox_ref": "m2u:mailbox_123",
      "mailbox_session_id": "mailbox_123",
      "stepId": "create-openai-account",
      "attempt": 1
    }
  ]
}
```

该结构只保存 mailbox 治理需要的非 token 字段，不保存 OAuth token、cookies、proxy password 或完整 response body。

### 7.8 长期 blacklist 阈值

默认策略保持保守：

- `REGISTER_MAILBOX_DOMAIN_BLACKLIST_MIN_ATTEMPTS` 默认不少于当前 20。
- `REGISTER_MAILBOX_DOMAIN_BLACKLIST_FAILURE_RATE` 默认保持高阈值。
- `REGISTER_MAILBOX_DOMAIN_CONSECUTIVE_FAILURE_BLACKLIST_THRESHOLD` 默认保持大值。
- provider 级 failure rate 使用同一组 min attempts / rate threshold。
- email OTP provider threshold 可以继续较快触发，因为它更接近 provider 能力问题。

`user_register_400` 的长期封禁应靠样本量和失败率，不靠单次命中。

## 8. 数据流

### 8.1 成功路径

1. `acquire-mailbox` 获取 mailbox。
2. `acquire-proxy-chain` 获取 proxy。
3. `create-openai-account` 成功。
4. 后续平台组织、login init、invite、oauth、validate 正常执行。
5. `release-mailbox` 和 `release-proxy-chain` cleanup。
6. `runner_mailbox` 记录成功，清除对应 dynamic blacklist / failure reasons。

成功路径不应因为本设计增加额外 retry 或额外资源消耗。

### 8.2 `user_register_400` retry 路径

1. 第一次 `acquire-mailbox` 得到：
   - provider = `m2u`
   - email = `user@kkb.qzz.io`
   - domain = `kkb.qzz.io`
2. `create-openai-account` 返回 `user_register_400`。
3. `dst_flow_runtime` 分类为 weak attributed generic register 400。
4. 写入 attempt-local avoidance：
   - avoid provider: `m2u`
   - avoid domain: `kkb.qzz.io`
   - avoid email: `user@kkb.qzz.io`
5. 将第一次失败追加到 `mailbox-attempt-outcomes`。
6. 释放旧 mailbox / proxy。
7. 重新执行 `acquire-mailbox`：
   - 不应返回 `user@kkb.qzz.io`
   - 尽量不返回 `m2u`
   - 若业务 domain pool 可用，优先换到 pool 中的其他 domain
8. 第二次 `create-openai-account` 使用新 mailbox + 新 proxy。
9. runner 处理结果时先记录第一次失败 attempt，再记录最终 outcome。

### 8.3 强 mailbox 错误路径

1. `create-openai-account` 返回 `unsupported_email` 或 attributed `registration_disallowed`。
2. step retry 使用 attempt-local avoidance。
3. retry 阶段把失败 mailbox 追加到 `mailbox-attempt-outcomes`。
4. task 完成后，即使第二次 retry 成功，runner 仍根据 per-attempt outcome 把第一次失败 domain/provider 记录为 blacklisted。
5. 后续 task 的 `runtime_mailbox` 读取 dynamic state 并避开该 domain/provider。

### 8.4 外部错误路径

示例：

- proxy acquire failed
- protocol worker capacity timeout
- SMS provider unavailable
- OAuth missing workspace
- `chat_requirements_failed status=401`

行为：

- 不更新 mailbox quality stats。
- 不新增 mailbox blacklist。
- 仍按对应 retry profile / failure pool 处理。

## 9. 观测与日志

新增或强化以下 JSON log event：

1. `register_create_account_mailbox_retry_context`
   - 在 `create-openai-account` 准备 retry 前输出。
   - 字段：errorCode、failureClass、provider、domain、email、avoidReason。
2. `register_mailbox_attempt_local_avoidance_applied`
   - `resolve_mailbox()` 因 attempt-local hints 拒绝 mailbox 时输出。
   - 字段：reason、provider、domain、email、attempt、maxAttempts。
3. `register_mailbox_business_domain_rejected`
   - 已存在，可补充 `avoidReason`。
4. `register_mailbox_domain_outcome_recorded`
   - 已存在，应能看到最终 outcome 或 per-attempt outcome 的 `failureReason = create_account_user_register_400`。
5. `register_mailbox_domain_outcome_ignored`
   - 已存在，继续用于外部失败归因。
6. `register_mailbox_attempt_outcome_recorded`
   - 新增，用于记录来自 `mailbox-attempt-outcomes` 的失败 attempt。

这些日志必须避免输出敏感 token；email 可以保留，因为当前 mailbox state 和 logs 已包含注册邮箱。

## 10. 配置

本设计优先复用现有配置：

- `REGISTER_MAILBOX_BUSINESS_KEY`
- `REGISTER_MAILBOX_BUSINESS_POLICIES_JSON`
- `REGISTER_MAILBOX_DOMAIN_POOL`
- `REGISTER_MAILBOX_DOMAIN_BLACKLIST`
- `REGISTER_MAILBOX_PROVIDER_BLACKLIST`
- `REGISTER_MAILBOX_BUSINESS_RETRY_ATTEMPTS`
- `REGISTER_MAILBOX_DOMAIN_BLACKLIST_MIN_ATTEMPTS`
- `REGISTER_MAILBOX_DOMAIN_BLACKLIST_FAILURE_RATE`
- `REGISTER_MAILBOX_DOMAIN_CONSECUTIVE_FAILURE_BLACKLIST_THRESHOLD`
- `REGISTER_MAILBOX_EMAIL_OTP_FAILURE_BLACKLIST_THRESHOLD`
- `REGISTER_MAILBOX_EMAIL_OTP_PROVIDER_FAILURE_BLACKLIST_THRESHOLD`
- `REGISTER_MAILBOX_DYNAMIC_BLACKLIST_TTL_SECONDS`
- `REGISTER_MAILBOX_DYNAMIC_BLACKLIST_EXHAUSTED_FALLBACK`

不建议第一版新增全局开关。原因：

- `step-create-account-recover` 已经显式启用 mailbox refresh。
- attempt-local avoidance 是 refresh 的合理补强，不应默认关闭。
- 长期 blacklist 仍由现有保守阈值控制。

如果实现阶段需要临时排障，可增加隐藏 debug 开关：

- `REGISTER_MAILBOX_ATTEMPT_LOCAL_AVOIDANCE_ENABLED`

默认应为 true。

## 11. 文件级影响

预期实现阶段可能修改：

1. `server/services/orchestration_service/flows/codex-openai-account-v1.semantic-flow.json`
   - 给 `acquire-mailbox` 追加 avoid hints input。
2. `server/services/orchestration_service/src/others/dst_flow_runtime.py`
   - 在 retry refresh 前构建 attempt-local mailbox avoidance。
3. `server/services/orchestration_service/src/others/runtime_mailbox.py`
   - `resolve_mailbox()` 接收并执行 attempt-local avoidance。
4. `server/services/orchestration_service/src/others/easyemail_runtime.py`
   - `acquire_mailbox` dispatcher 透传 avoid hints。
5. `server/services/orchestration_service/src/others/runner_mailbox.py`
   - 细化 `user_register_400` failure reason、per-attempt outcome 和长期统计。
6. `server/services/orchestration_service/src/others/error_catalog.py`
   - 必要时补充分类规则，但不能把所有 400 都升级成强 mailbox blacklist。
7. `tests/test_dst_flow_integration.py`
   - 覆盖 step retry 的 mailbox + proxy + avoid hints 行为。
8. `tests/test_adapter_runtimes.py`
   - 覆盖 runtime mailbox attempt-local avoidance。
9. `tests/test_runner_modules.py`
   - 覆盖长期 state 记录、ignore、strong/weak attribution。
10. `tests/test_error_profiles.py`
   - 如分类有变，补充错误码回归测试。

## 12. 测试计划

实现阶段按 TDD 顺序补测试。

### 12.1 DST retry tests

新增或扩展：

1. `user_register_400` 后第二次 `acquire-mailbox` input 包含 avoid hints。
2. 第一次失败的 mailbox 被 best-effort release。
3. 第二次 `create-openai-account` 使用新 mailbox + 新 proxy。
4. release mailbox 失败不阻断 retry。
5. generic 400 没有 mailbox context 时不写 avoid provider/domain，也不写 per-attempt mailbox outcome，只保留原 retry。
6. 第一次强 mailbox 错误、第二次成功时，result 仍保留第一次失败的 `mailbox-attempt-outcomes`。

### 12.2 runtime mailbox tests

新增：

1. `resolve_mailbox()` 避开 attempt-local email。
2. `resolve_mailbox()` 避开 attempt-local domain。
3. `resolve_mailbox()` 避开 attempt-local provider。
4. planned provider 被 avoid 时，优先尝试 business domain pool / moemail fallback。
5. 达到 retry attempts 后抛出包含 avoid context 的 `mailbox_business_policy_retries_exhausted`。
6. attempt-local avoidance 不写入 `register-mailbox-domain-state.json`。

### 12.3 runner mailbox outcome tests

新增或调整：

1. attributed generic `user_register_400` 记录 `failureReason = create_account_user_register_400`。
2. generic `user_register_400` 单次不立即 blacklist domain/provider。
3. 达到保守 failure rate/min attempts 后可 blacklist。
4. `unsupported_email` 仍立即 blacklist。
5. attributed `registration_disallowed` 仍立即 blacklist。
6. proxy/protocol/SMS/OAuth 外部失败仍 ignored，不写 state。
7. 成功结果仍清除 dynamic blacklist 和 failure reasons。
8. retry 中失败但最终成功的强 mailbox attempt 仍能写入长期 blacklist。

### 12.4 全量回归

实现完成后运行：

```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

若本轮改动影响 compose/runtime 配置，再补隔离 smoke；不得使用 live `register-*` 容器。

## 13. 验收标准

1. `user_register_400` step retry 不再只是换 proxy；必须换掉或避开刚失败的 mailbox risk surface。
2. 对 `m2u + kkb.qzz.io` 这类失败，下一次 retry 至少避开：
   - 同一 email
   - 同一 domain
   - 可归因时同一 provider
3. `unsupported_email` 和 attributed `registration_disallowed` 仍能快速写入 blacklist。
4. 单次 generic `user_register_400` 不会立即全局封禁 provider/domain。
5. external proxy / protocol / SMS / OAuth workspace failures 不污染 mailbox stats。
6. 第一次 mailbox 失败、第二次 retry 成功时，第一次失败的治理信号不会丢失。
7. `register-mailbox-domain-state.json` schema version 保持 `3`，旧 state 不被清空。
8. 所有新增测试和既有 unittest 回归通过。
9. 没有修改 live 容器、旧仓或生产挂载目录。

## 14. 风险与缓解

### 风险 1：EasyEmail 不支持按请求排除 provider

缓解：

- 第一版在 EasyRegister 侧做返回后检查与释放重试。
- 如果 EasyEmail 已支持 provider selections，就优先透传。
- 如果不支持，不把 EasyEmail 作为本轮阻塞项。

### 风险 2：attempt-local avoidance 导致 mailbox 获取次数增加

缓解：

- 复用 `REGISTER_MAILBOX_BUSINESS_RETRY_ATTEMPTS` 上限。
- 每次 rejected mailbox 都 best-effort release。
- 日志记录 attempt/maxAttempts，便于调阈值。

### 风险 3：误把 proxy 风控归因到 mailbox

缓解：

- 只有 `create-openai-account` 且有 mailbox context 时才写 mailbox weak stats。
- 已有 ignore rules 继续优先处理 proxy/protocol/SMS/OAuth。
- generic 400 只做 attempt-local avoidance，不做立即全局 blacklist。

### 风险 4：状态 schema 漂移导致历史 blacklist 失效

缓解：

- 不 bump schema version。
- 只追加 failure reason key 和可选观测字段。
- 测试覆盖旧 state 读取。

## 15. 下一步

用户 review 并批准本 spec 后，进入 implementation plan / TDD 阶段。

建议第一轮实现拆成三步：

1. TDD：补 DST retry avoid hints 和 runner mailbox outcome 测试。
2. 实现：`dst_flow_runtime` + `easyemail_runtime` + `runtime_mailbox`。
3. 回归：完整 unittest，通过后再考虑隔离 Docker smoke。
