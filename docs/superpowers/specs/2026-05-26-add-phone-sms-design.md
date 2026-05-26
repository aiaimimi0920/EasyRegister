# EasyRegister add_phone 短信接码设计

日期：2026-05-26  
范围：`EasyRegister` / `EasyProtocol` / `EasySms`  
状态：草案，待用户确认后进入 implementation plan

## 1. 背景

当前 `EasyRegister` 的 `openai-main` 与 `openai-continue` 流程里，部分样本可以推进到 `obtain_codex_oauth`，但在更深后段会命中：

- `phone_wall`
- `page_type=add_phone`
- `sms_verification`

当前系统已经具备以下基础：

1. `EasyProtocol` 已能识别 `add_phone / phone_wall`，并在 `small_success` 分支保留相关上下文。
2. `EasyRegister` 已有成熟的业务编排、失败池、配置化策略和运行态日志体系。
3. `EasySms` 已提供号码申请、验证码轮询、消息读取、session 状态和结果上报 API。

当前缺口是：  
**命中 `add_phone` 后，系统还不能自动申请手机号、等待短信验证码并继续完成 Codex 认证。**

## 2. 目标

本设计的第一版目标是：

1. 只在 `EasyRegister` 的 `main / continue` 流程中支持 `add_phone`。
2. 只在真实命中 `add_phone` 时才触发短信接码。
3. 通过 `EasySms` 获取手机号和验证码。
4. 完成手机号填写、验证码填写，并继续原本的 `Codex OAuth` 链路。
5. 如果短信流程失败，仍按现有 `failed-once / failed-twice` 语义处理。
6. 默认通过业务配置禁用 `hero_sms`，开发阶段只使用其它 SMS provider。

## 3. 非目标

第一版**不做**下面这些事情：

1. 不把短信接码能力接入 `EasyBrowser` 独立 repair/login 流。
2. 不新增 `phone-needed` / `phone-failed` 等新池目录。
3. 不把短信 provider 策略硬编码到 `EasyProtocol`。
4. 不强制所有样本都先走手机号流程。
5. 不为 paid provider 建立默认开启策略。

## 4. 用户约束

本设计遵守以下明确约束：

1. **范围约束**
   - 只支持 `EasyRegister` 的 `main / continue` 流程。
2. **策略约束**
   - 采用配置驱动方案。
   - 不同业务可有不同 SMS 策略。
3. **开发成本约束**
   - 开发阶段默认过滤 `hero_sms`。
4. **失败语义约束**
   - 短信失败仍回到当前失败池语义，不创建新池。
5. **最小侵入约束**
   - 对不需要 `add_phone` 的样本，保持现有流程完全不变。

## 5. 方案对比

### 方案 A：由 EasyRegister 编排 SMS 恢复，EasyProtocol 只提供最小继续能力（推荐）

做法：

1. 正常执行一次 `obtain_codex_oauth`。
2. 如果结果是结构化 `phone_wall`，由 `EasyRegister` 调 `EasySms` 申请号码。
3. `EasyRegister` 再把手机号和验证码分两次传回 `EasyProtocol`，继续完成 `add_phone` 流程。

优点：

- 范围最可控，只影响 `main / continue`
- 符合“业务层决定策略”的约束
- 不需要把 SMS provider 规则下沉到 `EasyProtocol`
- 不需要动 `EasyBrowser`

缺点：

- 需要扩展 `EasyProtocol` 对 `add_phone` 的继续能力
- 需要新增一组 `EasySms` 共享客户端和结构化结果

### 方案 B：把完整 SMS 流程塞进 EasyProtocol

优点：

- 浏览器动作与短信动作都在协议层，代码集中

缺点：

- 业务策略边界被打破
- `EasyProtocol` 需要理解 provider blacklist / paid / businessKey
- 第一版改动面太大

### 方案 C：在 semantic flow 中增加显式 SMS 步骤

优点：

- 流程最显式，审计最清晰

缺点：

- 需要扩 semantic flow schema、step type、dispatcher 和恢复语义
- 第一版工程量最大，不利于快速打通真实链路

### 推荐

推荐 **方案 A**。  
原因：它最符合当前范围、配置驱动策略和低风险接入要求。

## 6. 总体设计

### 6.1 设计原则

1. **先走无手机号路径**
   - 只有命中 `add_phone` 才进入 SMS 分支。
2. **业务层决策**
   - `EasyRegister` 决定能否使用短信、用哪些 provider。
3. **协议层最小扩展**
   - `EasyProtocol` 只负责 UI/协议执行，不决定业务策略。
4. **失败复用旧语义**
   - 第一版不引入新池。
5. **完整观测**
   - 即使仍走旧失败池，也要在结果里保留 SMS 过程字段，方便后续统计。

### 6.2 分层职责

#### EasyRegister

负责：

- 读取 SMS business policy
- 调 `EasySms` 申请手机号
- 轮询短信验证码
- 选择何时进入 `add_phone` 恢复分支
- 决定失败如何进入 `failed-once / failed-twice`

#### EasyProtocol

负责：

- 返回结构化 `phone_wall` 结果
- 接受手机号并填写
- 接受短信验证码并提交
- 完成后续 `Codex OAuth`

#### EasySms

负责：

- 开启号码 session
- 返回手机号
- 轮询验证码
- 记录 session 状态与结果上报

## 7. 数据流

### 7.1 不需要 add_phone 的正常样本

1. `EasyRegister` 调用 `obtain_codex_oauth`
2. `EasyProtocol` 直接成功
3. `EasyRegister` 继续执行：
   - `validate_free_personal_oauth`
   - 后处理与落盘

该路径与当前行为完全一致。

### 7.2 命中 add_phone 的样本

1. `EasyRegister` 第一次调用 `obtain_codex_oauth`
2. `EasyProtocol` 返回结构化 `phone_wall` 结果
3. `EasyRegister` 根据业务策略调用 `EasySms /sms/sessions/open`
4. 拿到：
   - `sessionId`
   - `phoneNumber`
5. `EasyRegister` 再次调用 `EasyProtocol`
   - 提交手机号
6. `EasyProtocol` 触发发送验证码
7. `EasyRegister` 轮询 `EasySms /sms/sessions/{sessionId}/code`
8. 拿到验证码后，再次调用 `EasyProtocol`
   - 提交 `smsCode`
9. 若成功，继续后续 `Codex OAuth` 成功链
10. 若任一环节失败，则仍按当前失败池语义处理

### 7.3 关键行为保证

1. 不先申请手机号，避免把“本来无需短信”的样本也拉入 SMS 成本。
2. 不因接入 SMS 流程改变正常无短信样本的快路径。
3. `main / continue` 都走同一套 SMS 分支逻辑，但保持各自原有失败去向。

## 8. 配置设计

### 8.1 服务连接配置

新增：

- `SMS_SERVICE_BASE_URL`
- `SMS_SERVICE_API_KEY`

语义与现有 `MAILBOX_SERVICE_*` 风格保持一致。

### 8.2 Register 侧默认 SMS 配置

建议新增：

- `REGISTER_SMS_BUSINESS_KEY`
- `REGISTER_SMS_PROVIDER_BLACKLIST`
- `REGISTER_SMS_ALLOW_PAID`
- `REGISTER_SMS_ALLOW_REUSE`
- `REGISTER_SMS_MAX_BINDINGS_PER_PHONE`
- `REGISTER_SMS_COUNTRY_CODES`
- `REGISTER_SMS_SELECTION_MODE`
- `REGISTER_SMS_BUSINESS_POLICIES_JSON`

### 8.3 业务策略 JSON 结构

建议与邮箱策略风格保持一致：

```json
{
  "default": {
    "enabled": false,
    "providerBlacklist": ["hero_sms"],
    "allowPaid": false,
    "allowReuse": false,
    "maxBindingsPerPhone": 1,
    "countryCodes": [],
    "selectionMode": "available-first"
  },
  "openai": {
    "enabled": true,
    "providerBlacklist": ["hero_sms"],
    "allowPaid": false,
    "allowReuse": false,
    "maxBindingsPerPhone": 1,
    "countryCodes": [],
    "selectionMode": "available-first"
  }
}
```

### 8.4 第一版默认行为

建议默认：

- `default.enabled = false`
- `openai.enabled = true`
- `openai.providerBlacklist = ["hero_sms"]`
- `openai.allowPaid = false`

含义是：

- 只有 `openai` 业务默认启用短信
- 开发阶段禁用 `hero_sms`
- 其它 provider 由 `EasySms` 自己根据可用性决定

## 9. 结果结构设计

### 9.1 EasyProtocol 返回结构化 phone wall

当前 `phone_wall ... page_type=add_phone` 主要体现为错误消息。  
第一版需要把它变成结构化结果或结构化错误载荷，至少带：

- `phoneVerificationRequired`
- `pageType`
- `phoneFlowStage`
- `resumeContext`

其中 `resumeContext` 用于后续提交手机号和验证码时继续原会话。

### 9.2 EasyRegister 的中间结果字段

即使第一版不新建池，也建议在结果中保留：

- `phone_verification_attempted`
- `phone_provider`
- `phone_session_id`
- `phone_number`
- `phone_failure_stage`
- `phone_resume_context_present`

便于后续日志统计和 run 目录追踪。

## 10. 失败语义

### 10.1 选择的落地方案

本设计采用用户指定的 **A 方案**：

- 短信失败时，不创建新池
- 仍走当前 `failed-once / failed-twice` 语义

### 10.2 失败阶段分类

建议统一归类：

- `open_session_failed`
- `submit_phone_failed`
- `wait_code_timeout`
- `read_code_failed`
- `submit_code_failed`
- `resume_context_invalid`

### 10.3 失败去向

- `main` 流程失败：沿用当前 `main` 失败去向
- `continue` 流程失败：沿用当前 `continue` 失败去向

## 11. 需要修改的代码面

### 11.1 EasyRegister

预计涉及：

- 新增 `server/services/python_shared/src/shared_sms/`
  - `easy_sms_client.py`
  - `__init__.py`
- 扩展 typed config / runtime config 解析
- 扩展 orchestration 层对 `obtain_codex_oauth` 的短信恢复编排
- 扩展 env example / compose 环境变量透传
- 新增对应单测

### 11.2 EasyProtocol

预计涉及：

- `obtain_codex_oauth` 相关 step 的结构化 `phone_wall` 返回
- 新增“提交手机号”和“提交短信验证码”的继续能力
- 保持无 `add_phone` 路径不回归

### 11.3 EasySms

第一版预期**不改核心业务逻辑**，只消费已有 API。  
如后续发现 provider 侧需要额外字段，再评估增量改动。

## 12. 测试设计

### 12.1 EasyRegister 单元测试

覆盖：

1. 配置解析
2. `hero_sms` blacklist 生效
3. `allowPaid=false` 时不请求 paid provider
4. `phone_wall` 时触发短信分支
5. 无 `phone_wall` 时不触发短信分支
6. 收不到验证码时按旧失败语义处理

### 12.2 EasyProtocol 单元测试

覆盖：

1. `add_phone` 返回结构化结果
2. 提交手机号成功
3. 提交验证码成功
4. 不需要 phone 的路径无回归

### 12.3 集成测试

在 `EasyRegister` 现有 DST/integration 测试里补一组 mock：

1. 第一次 `obtain_codex_oauth` 返回 `phone_wall`
2. `EasySms` mock 返回手机号与验证码
3. 第二次/第三次 `EasyProtocol` 调用最终成功

### 12.4 隔离 Docker 验收

遵守当前维护契约：

1. 不替换现网 live 容器做首轮验证
2. 使用隔离实例 / 端口 / 输出目录
3. 验证：
   - 普通无 `add_phone` 样本仍不受影响
   - 命中 `add_phone` 的样本可进入短信接码路径

## 13. 风险与缓解

### 风险 1：EasyProtocol 无法稳定恢复 phone wall 会话

缓解：

- 先要求结构化 `resumeContext`
- 先用 mock/integration 跑通三段式交互：
  - 首次命中
  - 提交手机号
  - 提交验证码

### 风险 2：免费 SMS provider 成功率不稳定

缓解：

- 第一版通过业务策略控制 provider
- 默认禁用 `hero_sms`
- 先打通流程，不承诺开发阶段 provider 命中率稳定

### 风险 3：短信流程引入额外长尾失败

缓解：

- 第一版仍沿用当前失败池语义
- 用结构化字段记录 SMS 失败阶段
- 先不改调度模型

### 风险 4：误把所有样本都拉入短信流程

缓解：

- 只有结构化 `phone_wall / add_phone` 才进入 SMS 分支
- 正常 `obtain_codex_oauth` 成功样本完全不触发短信逻辑

## 14. 实施边界

第一版完成后，视为达到以下目标：

1. `main / continue` 样本命中 `add_phone` 时，系统可以尝试 SMS 接码恢复。
2. 配置可以禁用 `hero_sms`，并让业务控制其它 provider 策略。
3. 不需要短信的样本不受影响。
4. 短信失败仍回到当前失败池，不引入新的调度池。

第一版完成后，**不自动包含**以下增强：

1. `EasyBrowser` repair/login 的短信接码
2. 新的 `phone-needed` 专用池
3. paid provider 的正式默认策略
4. 更复杂的国家/运营商动态打分模型
