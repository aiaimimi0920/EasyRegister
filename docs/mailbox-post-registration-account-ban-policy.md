# 注册后账户停用的邮箱域名业务黑名单

**2026-10-01 17:06 UTC 的后续配置：**用户要求排除疑似拒绝的完整邮箱域名后，已将 `lake.neuroloom.pp.ua` 从 `openai` 候选池移除并加入显式业务黑名单；仅替换停止容器的配置，旧容器完整保留，新业务容器为 created、尚未启动。没有改动本页的自动阈值 5，没有回填账户停用次数，也没有认定域名就是根因。历史跨域名核验、定向测试及回滚见 [域名排除与证据核验](mailbox-domain-exclusion-and-evidence-2026-10-01.md)。

## 规则与证据边界

同一业务平台、同一规范化邮箱域名，已完成注册的 **5 个不同账户**连续被明确判定停用、禁用、封禁或挂起时，将该域名加入该业务的持久黑名单。阈值可通过以下环境变量修改，默认值为 `5`，最小值为 `1`：

```dotenv
REGISTER_MAILBOX_DOMAIN_POST_REGISTRATION_BAN_THRESHOLD=5
```

这是一条业务风险控制规则，不是邮箱导致封禁的因果诊断。`deleted_or_disabled` 以及上游的 “deleted or deactivated” 表示账户不可用，不能据此区分主动删除与平台停用；它们作为风险信号计入，但单纯的 `account_deleted` 不计入。

本次没有回填历史结果，也没有因为单次真实账户停用直接将其域名列入黑名单。

## 什么会计入

必须先有与观测邮箱匹配的注册完成证明，再有明确账户不可用信号：

- 当次完成的注册步骤，状态为 `registered` / `completed`，或 `small_success` 且页面为已注册／callback；仅收到邮箱或注册验证码不算完成注册。
- continuation 可使用匹配邮箱且通过现有校验器的完整 OAuth seed；protocol small-success seed 还必须具备完整 `platformAuth`，并到达 callback／已注册页面。仅 phone wall 不作为本规则的注册完成证明。
- 指定认证／登录步骤中的确切账户停用 code，或者直接上游拒绝句子／保存的 HTTP 403 正文。兼容 EasyProtocol 的 `chatgpt_login_otp_validate_failed` 与旧 `otp_validate` 前缀，以及 220 字符正文 preview 的截断。
- 通用业务可提供匹配邮箱的 `account-availability` 输出，明确状态为 `account_banned`、`account_disabled`、`account_deactivated`、`account_suspended` 或 `deleted_or_disabled`。

泛 HTTP 403、Cloudflare challenge、网络失败、OTP 错误／超时、短信资源不足、workspace 停用、邮箱 provider 失败和注册本身失败均不计入。引用、否定或 provider 包装中提到停用文案，不当作账户停用证据。

## 连续计数与去重

- 业务键和域名分别规范化；按 `businessKey + domain` 隔离。A 平台的封禁不会影响 B 平台，也不会扩展到父域、所有子域或整个邮箱 provider。
- 同一规范化邮箱的停用终身只累计一次；重试、不同 claim／session 和 supervisor 再次上报不能把一个账户变成五个。
- 未达到黑名单阈值时，新的实际健康登录会将该域名的连续计数清零。初次注册成功、旧 seed 中保存的健康状态及 `already_initialized` 不清零。
- 健康登录按观测去重，而不是按账户永久去重：同一账户的新健康登录可以清零，同一次观测重复报告不可以。DST 每个新 task attempt 生成独立 `accountRiskObservationId`；旧调用方没有该 ID 时，使用剔除派生风险输出后的稳定结果摘要去重。
- ChatGPT 健康登录要求步骤成功、`status=completed` 和有效的 `personalWorkspaceId`，并绑定相同邮箱，或相同的非空 `mailboxRef` 与 `mailboxSessionId`。
- 已经触发黑名单后，健康登录也不会自动解除。没有 TTL，不受旧收信质量统计成功清零、动态降级或 provider pin 的影响。

## 黑名单的执行范围

当前默认只禁止对应平台后续**新注册所用邮箱**，保留明确的已有账户邮箱恢复路径（`recover_preallocated_email=True`）。重新创建邮箱、指定新邮箱、普通 provider 选择均执行域名限制。

即使更换邮箱 provider、指定原域名、设置 `include_dynamic=False`，或旧逻辑放宽动态黑名单，也不能绕过本规则。provider 返回黑名单域名时，最终业务策略检查仍会拒绝，不用于新注册。

用户尚未要求将已有账户恢复也一并禁止，因此本次不扩大为已有账户的全面禁用。

## 状态与接入位置

风险状态与旧 `schemaVersion=3` 邮箱质量状态分开保存。风险 sidecar 使用 `schemaVersion=1`，路径由旧 domain state 文件名派生：

```text
register-mailbox-domain-state.json
register-mailbox-domain-state.account-risk.json
```

路径优先级为显式 `REGISTER_MAILBOX_DOMAIN_STATE_PATH`，其次 DST 运行作用域内的 `REGISTER_OUTPUT_ROOT`／显式 `output_dir` 派生共享根。没有这些设置时沿用当前工作目录默认。DST 用 `ContextVar` 绑定风险读写路径，不改进程环境；结果的 `taskContext.accountRiskDomainStatePath` 保留该路径，供 supervisor 再次报告时复用。旧收信质量状态路由没有重构。

sidecar 只保存域名、业务键、邮箱 SHA256 摘要、健康观测摘要、计数和时间，不保存邮箱明文、OTP、token 或 cookie。写入带有界文件锁、`fsync` 和原子替换；Windows 瞬时锁权限冲突会有界重试。损坏／不符合 schema 的状态拒绝继续选择新邮箱和更新风险文件，不覆盖损坏文件。

核心实现：

- `others/mailbox_account_risk.py`：账户风险分类、持久化、去重和路径作用域。
- `others/runner_mailbox.py`：注册证据绑定和 supervisor 结果记录。
- `others/dst_flow_runtime.py`：每个 task attempt 的独立结果记录。
- `others/runtime_mailbox.py`：业务域名排除及新邮箱最终拒绝。
- `MailboxRuntimeConfig.post_registration_ban_threshold`：统一配置。

本次已接入现有 OpenAI 注册／continuation 流程；其他平台可以复用业务键、通用注册步骤和账户状态输出约定，但不会凭任意平台错误文案自动猜测其封禁状态。

## 人工解除

本次不提供自动解封或新的管理界面。确需解除时，应先停止相关写入、备份 sidecar 并人工复核账户证据，再只调整目标业务／域名的 `blacklisted=false`、`consecutiveAccountBans=0`，清空对应的黑名单 reason／时间；保留账户和观测去重历史，避免重放旧错误立即再次累计。不得通过删除整个状态文件解除所有业务黑名单。

## 本地验证与交付状态

验证代码与产物位于：

```text
C:\Users\Public\nas_home\AI\GameEditor\linshi\easyregister-domain-ban-rule-20261001
```

新增规则离线测试覆盖 28 项：第五个不同账户触发、业务隔离、重复上报、真实健康重置、误绑健康拒绝、旧 seed 与 protocol callback 证明、真实上游封装与截断正文、损坏状态、并发更新、provider／动态降级绕过拒绝、新邮箱选择和已有账户恢复，以及显式 standalone 输出路径读写一致性。

相邻门禁覆盖配置、邮箱 provider 故障切换、原有 runner 邮箱质量统计、邮箱选择及主要 DST 运行／continuation 入口。测试中的账户、邮箱和 token 均为 synthetic fixture，网络／provider 边界 mock。

首次本地交付时只完成源码和离线验证，尚未发起真实注册／登录请求、购买短信或部署到 PC2／生产。后续线上部署与尝试结果见下节；两阶段均未提交或推送 Git。

## PC2 线上部署与单次验收（2026-10-01 UTC）

### 部署范围与实际加载

在用户授权“部署到线上并继续尝试”后，于 `2026-10-01T08:41:19.736319+00:00` 完成 PC2 生产容器切换。本地工作区存在其他未发布修改，且部分源码与线上基线不同，因此以当前生产镜像为基础，仅叠加本规则的严格增量 overlay，没有整包覆盖本地工作区。

```text
base image: sha256:fa0836171f8ba3523edb5e71c435d85c0e500e19b4c21394bc63b081485f15d4
new image:  sha256:3adf6292ac8d2a0e604dbd9c6835f6a0c214ebce4e5e1b483426265a218ce38f
new tag:    easyregister/orchestrator:account-domain-risk-20261001-0820
release:    /home/mjc/easyregister/releases/account-domain-risk-20261001-0820
```

实际部署候选镜像在 PC2 的 `--network none` 隔离容器中通过 28 项规则测试，退出码为 `0`。新版运行容器中的 5 个规则相关源码文件 SHA256 与本轮 staged 文件一致；这不表示整个本地 dirty worktree 与生产一致，也不表示本轮重新运行了所有相邻门禁。

运行进程已核实以下有效配置：

```text
REGISTER_MAILBOX_DOMAIN_POST_REGISTRATION_BAN_THRESHOLD=5
REGISTER_OUTPUT_ROOT=/shared/register-output/others/mixed-runs
account risk state=/shared/register-output/others/register-mailbox-domain-state.account-risk.json
effective SMS: enabled=false, allowPaid=false, allowReuse=false
```

原任务 34 于 `08:41:13.127560 UTC` 完成后进入 120 秒休眠，再停止旧容器并切换；新任务于 `08:41:23.363943 UTC` 开始。旧日志确认此间没有启动下一任务。保留原输出和只读 team-auth 数据挂载，旧容器改名为 `easy-register-before-domain-risk-20261001-0820`，没有删除旧容器或数据。

### 有界尝试与失败归因

本轮只允许一个 run、一个 task attempt，每个 step 的重试上限为 `1`，代理 acquisition 上限为 `1`，容器 `restart=no`。原 `REGISTER_FLOW_SPECS_JSON` 为空，确认单次 flow 配置未被其他 flow specs 覆盖。

```text
REGISTER_INFINITE_MAX_RUNS=1
REGISTER_TASK_MAX_ATTEMPTS=1
flow taskRetry.maxAttempts=1
step retry.maxAttempts=1
acquire_proxy_chain.max_acquire_attempts=1
```

一次业务尝试于 `2026-10-01T08:41:23.363943+00:00` 开始，`2026-10-01T08:48:26.337624+00:00` 结束；审计记录为 `runStartedCount=1`、`taskAttempts=1`。代理与邮箱获取成功，`create-openai-account` 失败，Platform organization、ChatGPT 登录、Codex OAuth 和最终 free personal OAuth 验证均跳过；代理与邮箱释放成功。

```text
code: browser_verification_required
stage: stage_auth_continue
category: blocked
detail: oauth_authorize
message: browser_verification_required status=403 cf_mitigated=challenge ...
```

这是注册入口的上游浏览器验证／challenge 阻断，不是注册后的 “deleted or deactivated” 账户拒绝，也不能归因为邮箱导致封禁。本轮没有新注册成功、可供 continuation 使用的 fresh seed 或最终 OAuth 产物，因此没有小成功或大成功，未进入付费短信续接。

`post-registration-account-domain-outcome` 为 `null`，没有计入注册后账户停用次数、没有触发域名黑名单。风险 sidecar 尚未创建：本轮验证了真实加载的持久路径，但没有符合规则的真实账户观测，不能声称已写入线上风险记录。

### 费用与最终运行状态

短信财务只读核验时间分别为 `08:36:55 UTC` 和 `08:51:11 UTC`，余额均为 `4.5953 USD`；隔离短信 sessions 和供应商 active orders 均为 `0`。本轮短信购买 `0` 次、支出 `0 USD`。原 guard 的 `attempt.json` 与 receipt 均仍不存在；原 paid005 的 `run.started.json` 仍存在且已消费，未删除或改写，未重启旧 paid005，也没有创建新 guard 或复制购买额度。

失败后停止本轮精确 ID 的新业务容器。`2026-10-01T08:57:25.097952+00:00` 再次只读核实：

- 新 `easy-register`（ID `4536d3d078e2625e3cb882e1ac960e004893b7ae42a6ff90b139f0f13c71dedb`）为 `exited`、`restart=no`。
- 旧回滚容器（ID `22360381e923f8df7b4dc073664726e1c3a0ca4019ba077adaa40814ef1aca99`）为 `exited`、`restart=no`，仍保留。
- 相关 protocol、SMS、OAuth provider／gateway 和 paid guard 依赖容器仍运行；未停止依赖服务或删除数据。

因此，规则镜像已部署并曾在真实运行进程中加载，但当前业务 runner／dashboard 不是持续运行状态。不要仅为恢复 dashboard 而直接 `docker start easy-register`，该入口会再启动一轮业务尝试。下一轮应先针对注册入口的正常浏览器验证完成诊断或处理，不重复启动同一失败路径。

### 脱敏证据

证据保存在上文的 `linshi/easyregister-domain-ban-rule-20261001` 目录，关键文件：

- `online-build-report.v2.json`、`online-overlay-plan.json`：生产基线、增量部署和镜像规则测试。
- `online-cutover-started.json`、`online-live-policy-check.json`、`online-flow-spec-boundary.json`：切换、实际配置和运行源码哈希。
- `online-latest-attempt-audit.json`、`online-failure-diagnosis.json`、`online-failure-stopped.json`：单次尝试、失败归因和停止回执。
- `online-finance-before.json`、`online-finance-after.json`：本轮财务前后核验。
- `online-final-summary.json`、`online-new-final-container.json`、`online-backup-final-container.json`、`online-final-readonly-state.json`：最终摘要与容器状态。

以上验收文档不保存邮箱、session、token、cookie 或 OTP。线上私有容器配置不复制到普通文档。

## 注册入口 challenge 的后续只读调查（2026-10-01 09:16 UTC）

用户要求继续推进后，本轮读取保留结果、实际容器配置和线上 provider 的精确源码，没有再次启动注册或访问账户业务接口。

### 已确认的版本与调用边界

- 新邮箱规则属于 orchestrator；其部署没有顺带更新 Python protocol provider。线上 `easy-register-protocol-python` 仍运行镜像 `sha256:4297f6a2fe88c80ce3f5f4ef984bad0550a255c087d53b65699529f44e84b0ed`。
- 线上 `/app/src/new_protocol_register/protocol_small_success.py` 的 SHA256 为 `0af3bc76897232bb70e277f1912d50a8f414cad3b3cbc7fd017d13376ad02974`。实读该文件第 290 行的 `_raise_if_browser_verification_required`：只有响应头 `cf-mitigated` 为 `challenge` 才产生本轮的 `browser_verification_required` / `blocked`，不是根据邮箱 provider 名称或泛 403 推测。
- 该旧文件第 1613 行在 authorize 请求后直接调用 `_validate_platform_authorize_response`，后者执行 challenge 拒绝检查。该文件没有本地当前源码中的 `_platform_auth0_authorize_with_browser_retry`；本机隔离验收通过过的授权恢复与身份绑定改动，不能视为已在此线上 provider 加载。
- 线上 provider 有 Chromium 和 chromedriver，`HEADLESS=1`。因此已排除简单的“容器未安装浏览器”解释；这不等于已有可供人工操作的可见浏览器通道。

版本差异是可核实的部署缺口，但不能因此断言它是上游返回 challenge 的唯一原因，或升级后一定能够注册成功。保留的 provider stdout/stderr 在本轮时间窗只有两行，没有足够 request ID 关联信息或完整 HTTP 包；不以日志中没有某个 marker 作为请求未发生的证明。

### 当前限制与下一步

浏览器工具实测返回 `Browser is not available: iab`，随后可用 surface 清单为 `apps=[]`、`browsers=[]`，没有创建可见标签页或提交表单。没有改用隐蔽浏览器、复制 clearance cookie、自动解人机验证或放宽账户／域名拒绝判据。

已请用户在 PC2 的普通浏览器打开 `https://platform.openai.com/login`，只观察是否出现人机验证、是否可进入邮箱输入页，不填写账户信息或发送密码、OTP、cookie。该页面观察可区分普通浏览器入口是否可用，但不是对本轮原 OAuth transaction 的同出口、同会话复现，也不构成注册成功。

若页面仍要求验证，应通过上游正常交互流程完成；若普通浏览器可用，再对 provider 的必要会话兼容改动做有边界的隔离验证。不能直接整包部署本地 dirty provider、把匿名页面 200 当作 OAuth 成功，或为了获得成功而重复注册、更换身份／节点绕过账户停用。

`2026-10-01T09:16:53.180852+00:00` 的只读复核确认，新旧业务容器仍为 `exited` / `restart=no`，本轮部署版本的 `runStartedCount` 仍为 `1`，task 与注册 step 的 `maxAttempts` 均为 `1`。此次调查新增业务请求 `0`、表单提交 `0`、短信购买 `0`、容器启动 `0`，未更改线上配置。没有重新查询余额，不把前一轮的余额快照冒充本轮实时财务观测。

最终脱敏证据：`online-challenge-followup.final.json`；可复核的只读脚本：`inspect_challenge_followup.remote.py`，均位于同一 `linshi/easyregister-domain-ban-rule-20261001` 目录。本轮仅补充诊断文档，未修改产品源码、提交或推送 Git。

## Provider 兼容缺口已部署（2026-10-01 10:27 UTC）

后续继续推进已将生产基线的会话兼容增量候选部署到 Python provider，候选通过 43 项隔离测试，并通过现有 EasyProtocol 统一入口的指定服务合成回显验收。09:16 UTC 所述 provider 版本缺口已关闭；该节保留当时的调查证据，不代表当前版本。

业务 runner 仍停止，没有新增注册或短信购买，因此上游 challenge 与最终 OAuth 业务验收仍未闭环。镜像、回滚、源码范围、cookie 边界保护和真实服务链路证据见 [provider 会话兼容增量部署记录](provider-session-compat-rollout-2026-10-01.md)。
