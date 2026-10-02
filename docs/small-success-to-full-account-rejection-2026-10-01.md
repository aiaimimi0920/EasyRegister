# 小成功续接大成功的账户拒绝：取证与止损修复

**2026-10-02 04:00 UTC 更新：**下文“修复仅在本地”的历史交付缺口已关闭：三文件补丁已放入 PC2 待运行容器并通过 9 项实际依赖镜像回归；业务没有启动，账户未恢复，大成功仍未取得。见 [停止态交付记录](account-terminal-stopped-rollout-2026-10-02.md)。

## 结论与范围

本记录接续会话 `01a0f724-cf4c-73d1-8ae0-1e1e28636ce7` 的最后任务：解释已完成的小成功为什么没有取得最终大成功，核对主动删除账户、错误种子交接及错误分类。人工登录已有账号不是本任务的替代验收。

**04:25 的 Platform 注册小成功与 04:35 的续接失败是同一账户；续接失败发生在 ChatGPT 邮箱 OTP 校验，直接上游正文表示账户已经被删除或停用。短信购买、Codex OAuth 和最终 free/personal 验证均未执行。后续新注册遇到的 Cloudflare challenge 是另一事件，不是这次续接失败的解释。**

这确定了本次转换的直接阻断原因，不等于已查明平台为何删除或停用账户。现有保留材料没有完整 error code、响应 headers 或平台内部判定原因，不能把邮箱域名、代理出口、客户端指纹、会话绑定或某个本地行为宣布为已证实的封禁根因。

## 三个阶段不能混为一谈

| 阶段与时间（2026-10-01 UTC） | 真实证据 | 能证明什么 |
|---|---|---|
| 04:25:51–04:25:55，Platform 注册、callback、换票及组织初始化 | 后端新用户创建时间；callback seed；Platform token exchange；用户更新时 `enabled=true`、`banned=false` | 当时完成 Platform 注册阶段，不是仅靠本地 `small_success` 标签判定 |
| 04:35:18–04:35:21，受保护 continuation | 收到邮箱 OTP；OTP validate 返回 HTTP 403；账户删除或停用正文 | ChatGPT 登录未完成；后续 OAuth 与条件性短信恢复路径没有运行 |
| 后续另一轮新注册及 13:56–13:57 的诊断 | 健康 HTTPS 200，Platform/Auth 文档 403、`cf-mitigated: challenge`、`Just a moment...` | 新注册入口被生产安全验证阻断，与上面的账户拒绝区分；见 [入口网络与 challenge 调查](auth-entry-network-and-challenge-2026-10-01.md) |

本次续接的直接正文为：

```text
You do not have an account because it has been deleted or deactivated.
If you believe this was an error, please contact us through our help center at help.openai.com.
```

`deleted or deactivated` 无法区分主动删除与服务端停用，更不能单独证明邮箱是原因。该次并非供应商短信失败、余额不足、短信 OTP 超时或最终 OAuth 换票失败。

## 为什么小成功不保证大成功

这条链路是 Platform 注册／组织初始化，再建立 ChatGPT 登录，之后取得 Codex OAuth 并验证最终 free/personal 条件；可能遇到的手机验证属于后段，而不是每个 Platform seed 都必然可通过的步骤。

保存的 Platform token 是 Platform client 的材料，不是最终 Codex OAuth。seed 中也没有已完成的 ChatGPT 登录。`initialize-platform-organization` 的 `already_initialized` 是读取 seed 的 completed 状态后短路，未重新请求服务端；seed 校验器检查结构、材料与新鲜度，不保证账户在后续时刻仍然可登录。

因此这里没有“大成功已经发生却漏保存”的证据，也不能靠补一个 small-success 字段、延长 seed 时间、跳过 ChatGPT 登录或放宽最终 validator 来转换成功。

## 种子与身份交接复核

本轮重新读取原始私有 seed 与 private result，仅输出布尔判定和哈希，没有请求账户接口或打印身份材料：

- 已交接的 `easyregister-bridge` seed SHA256 仍为 `880c9892e577bb543d0d8c7ef1b82038f0f6a67738580a08b74620c9c1678121`，与记录一致。
- seed 邮箱与恢复邮箱一致；mailbox ref/session 一致。
- 使用保留的实际部署源码提取 `_normalize_seed_login_context`，确认设备 ID 保持、恢复邮箱覆盖参数正确。
- 注册时保存的后端用户状态仍为 `enabled=true`、`banned=false`、没有 banned timestamp；这是历史响应，不是账户当前状态。
- 前序 callback state、audience、nonce、user ID 及邮箱绑定比较均保留在 `state-and-binding.json`。该诊断只解码 JWT 比较字段，没有验签，不把解码当作独立身份验证。

这些证据不支持“随意交接了另一个旧停用账户”的解释；也不足以排除所有未保存的运行时 session 问题。所有源证据都保持不变，未改写 seed 时间或消费标记。

## 是否是程序主动删号

当前调用 owner 和本次保留的注册／组织初始化／ChatGPT 登录源码中，没有找到账户自身 delete/deactivate 请求。邮箱 release 发给独立 mail service 的 `/mail/mailboxes/release`，对象是 mailbox session，不是 OpenAI account。

- `EasyProtocol/.../protocol_small_success.py` 的可交接成功路径设置 `retain_mailbox=True`，失败清理才释放未保留邮箱。
- `EasyRegister/.../easyemail_runtime.py` 的 `dst_flow_cleanup` 和共享 mailbox 客户端只执行邮箱资源释放。
- `protocol_oauth.py` 中的失败清理位于 OAuth 异常之后；本次 `obtain-codex-oauth` 本来就被跳过。
- 协议源码中存在 Team invite／member 的 `DELETE`，不是删除用户自身账户，且本次未运行到这些后段步骤。

所以“邮箱释放返回 `deleted`”不能翻译成“OpenAI 账户被本程序删除”。源码审计没有证明平台内部为何停用，也不能排除范围外的外部操作。它只是排除了本次已覆盖路径中的明显主动删号调用，不能扩大为全环境因果证明。

## 已完成的最小本地修复

原始 result 的错误为：

```text
code=authorize_continue_blocked
category=flow_error
stage=stage_otp_validate
detail=chatgpt_login_email_otp_validate
```

旧 classifier 对 `chatgpt_login` + `status=403` 使用通用 blocked 分类，导致明确账户拒绝与 Cloudflare/授权阻断混在一起。默认重试 profile 又包含这个通用 code。

本轮完成上一轮未验完的三个模块修改，并补齐缺失的 `ErrorCodes` import：

1. `others/error_catalog.py` 新增 `ACCOUNT_UNAVAILABLE="account_unavailable"`。仅 ChatGPT 登录步骤的直接账户拒绝句或严格上游正文可细化通用错误；复用现有风险记录器的严格判定，不新增宽泛关键词判定。
2. 只细化空值、步骤 fallback、`authorize_continue_blocked` 和通用 `invalid_request_error`；其他具体 typed code 保持既有优先级，防止 workspace/provider/其他具体错误被覆盖。
3. `others/error_runtime.py` 对最终 `account_unavailable` 归为 `blocked`，保留原 stage、detail 和 message，不再被旧 provider 的 `flow_error` 覆盖。
4. `others/dst_flow_runtime.py` 的 step 和 task 两层重试都将它作为终止条件，即使 metadata 显式把它加入 `retryOnCodes` 也不重试。

该限制作用于本次 DST task／step，不声称停止所有 supervisor 将来新建的任务。当前生产业务 runner 仍停止。本轮没有修改邮箱域名阈值、回填历史风险次数、解除上游拒绝或绕过安全验证。

对原始 private result 的离线回放现得到：

```text
code=account_unavailable
category=blocked
stage=stage_otp_validate
detail=chatgpt_login_email_otp_validate
wouldRetryStepEvenIfConfigured=false
wouldRetryTaskEvenIfConfigured=false
```

这是错误识别与止损修复，不是账户恢复，也不是已经跑通大成功。

## 验证证据

产物目录：

```text
C:/Users/Public/nas_home/AI/GameEditor/linshi/easyregister-small-to-full-audit-20261001/
```

- 10 项定向测试通过：真实保留错误离线回放；完整与 220 字符截断正文；泛 403／OTP 错误／引用／否定句不计为账户停用；具体 typed code 保留；远程失败 envelope 的 HTTP 200/502 两种封装；两层 terminal guard。
- 其中真实 DST 执行测试使用 synthetic flow 和 mock owner，证明一次登录失败后只有一次 step／task attempt、下游 OAuth 跳过、邮箱 cleanup 仍执行。没有访问真实账户或短信服务。
- 92 项相邻门禁通过、0 failures/errors/skips：error profiles、protocol error boundary、3 项 continuation／phone side-effect 集成测试，以及现有 28 项域名风险规则测试。
- 修改文件进行 UTF-8 无 BOM、Python syntax 与限定范围 `git diff --check` 检查；没有全仓 suite、提交或推送。
- 原判定回归在修改前失败；typed-code 优先级补充回归在收窄前失败、收窄后通过，日志分别保留。

关键回执：`focused-final.log`、`adjacent-gates.json`、`adjacent-gates.log`、`preserved-result-audit.json`、`remote-readonly.json`。

## 当前线上边界与后续停止条件

`2026-10-01T16:20:46Z` 只读检查确认：

- PC2 `easy-register` 为 `exited`、`restart=no`，生产付费开关为 `false`。
- Python provider 保持此前 browser-proxy 修复镜像，仍运行，`PROTOCOL_RETRY_MAX_ATTEMPTS=1`、付费开关 `false`；没有修改或重启它。
- 原 paid005 仍为 `created`，没有启动。其单次 output 的委托启动标记已在历史本机尝试中消费，不能用“容器没有运行”推导“该轮还可重试”。本轮未删除或改写任何标记。
- 原 purchase ledger／receipt 仍不存在；本轮没有购买短信，也没有为同一错误新建额度。
- 从停止生产容器读取的 error catalog 尚没有 `ACCOUNT_UNAVAILABLE`，所以**上述新分类和止损修复只在本地完成，未部署**。

本机 Docker 只读状态核验中，gateway、seed006、local paid001 返回 exited；provider 的 inspect 遇到 Docker Desktop API 500，本轮停止该查询，未将旧状态当成新的全量状态证明，也未重启 Docker 或业务容器。

**原始“大成功”仍未取得。** 已完成的是直接失败定位、交接／主动删号核对，以及本地错误识别与自动重试止损。平台没有给出此次账户停用的具体原因，因此当前不能用继续付费、换身份或重复注册来声称验证了某个封禁假说。下一项能补足因果证据的是该原账户的官方状态／支持原因，而不是再次运行已经消费的 canary；这不要求提供密码、OTP、cookie 或 token，也不以人工登录已有账号替代自动注册验收。
