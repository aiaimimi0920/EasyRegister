# Orchestration Runtime

这个目录现在只保留 `EasyRegister` 的顶层 DST 编排与调度适配层。

目录约定：

- `dst_flow.py`
  顶层 DST 解析与调度入口
- `easyemail_flow.py`
  `EasyEmail` 服务客户端适配层
- `easyproxy_flow.py`
  `EasyProxy` 服务客户端适配层
- `easyprotocol_flow.py`
  `EasyProtocol` 服务客户端适配层
- `errors.py`
  调度层本地错误归一化
- `others/`
  公共模型、路径、运行时辅助

当前调度层直接依赖的本地运行时代码来源是：

- `server/services/python_shared/src/shared_proxy/`
  EasyProxy 客户端与代理环境辅助
- `server/services/python_shared/src/shared_mailbox/`
  EasyEmail 客户端封装
- `server/services/python_shared/src/shared_sms/`
  EasySms 客户端封装

当前目录已经去掉 `new_protocol_register/` 这一层，文件直接展开在
`server/services/orchestration_service/src/` 下。

运行时内部命名也已经收口到更中性的 `orchestration` / `register` 语义。
当前只使用下面这组环境变量名称：

- `REGISTER_MAILBOX_TTL_SECONDS`
- `REGISTER_OPENAI_OAUTH_SEED_MAX_AGE_SECONDS`
- `REGISTER_MAILBOX_ROUTING_PROFILE_ID`
- `REGISTER_MAILBOX_STRATEGY_MODE_ID`
- `REGISTER_MAILBOX_PROVIDERS`
- `REGISTER_MAILBOX_PROVIDER_BLACKLIST`
- `REGISTER_MAILBOX_DOMAIN_CONSECUTIVE_FAILURE_BLACKLIST_THRESHOLD`
- `REGISTER_MAILBOX_DYNAMIC_BLACKLIST_TTL_SECONDS`
- `REGISTER_MAILBOX_DYNAMIC_BLACKLIST_EXHAUSTED_FALLBACK`
- `REGISTER_SMS_BUSINESS_KEY`
- `REGISTER_SMS_PROVIDER_BLACKLIST`
- `REGISTER_SMS_ALLOW_PAID`
- `REGISTER_SMS_ALLOW_REUSE`
- `REGISTER_SMS_MAX_BINDINGS_PER_PHONE`
- `REGISTER_SMS_COUNTRY_CODES`
- `REGISTER_SMS_SELECTION_MODE`
- `REGISTER_SMS_BUSINESS_POLICIES_JSON`
- `REGISTER_SMS_TERMINAL_INVALID_PHONE_BLACKLIST_SECONDS`
- `REGISTER_ENABLE_EASY_PROXY`
- `REGISTER_REQUIRE_EASY_PROXY`
- `REGISTER_PROXY_HOST_ID`
- `REGISTER_PROXY_MODE`
  - 代理链获取模式；当前默认 `lease`，使用新版 `/proxy/leases/checkout` 兼容 API
  - `auto` 保留为回滚模式：租约失败后回退到旧的 random-node/multi-port 路径
  - `random-node` 仅用于明确仍启用 legacy multi-port 的 EasyProxy；Local Server 不兼容该路径
- `REGISTER_PROXY_TTL_MINUTES`
- `REGISTER_PROXY_UNIQUE_ATTEMPTS`
- `REGISTER_PROXY_RECENT_WINDOW_SECONDS`
- `REGISTER_PROXY_FAILURE_WINDOW_SECONDS`
- `REGISTER_PROXY_LEASE_FAILURE_COOLDOWN_SECONDS`
  - `auto` 模式下，如果租约 checkout 本身超时或临时不可用，会在这个秒数内跳过租约入口，直接走 random-node fallback；默认 `120`
- `EASY_PROXY_MANAGEMENT_USERNAME`
  - `/api/auth` 返回 `canonical_pair` 时使用的管理用户名；未设置时使用 EasyProxy 的规范用户名 `easyproxy`
- `EASY_PROXY_MANAGEMENT_PASSWORD`
  - EasyProxy 管理密码；优先于旧名称 `EASY_PROXY_API_KEY`
- `EASY_PROXY_API_TIMEOUT_SECONDS`
  - 管理 API 单请求超时，默认 `10`
- `EASY_PROXY_INITIAL_PROBE_MAX_ATTEMPTS`
  - checkout 返回 `503 INITIAL_PROXY_PROBE_PENDING` 时的最大尝试次数，默认 `4`
- `EASY_PROXY_INITIAL_PROBE_BACKOFF_SECONDS`
  - pending 重试的指数退避基数，默认 `1`
- `REGISTER_TEAM_AUTH_TEMP_BLACKLIST_SECONDS`
  当某个 team / mother 账号在刷新后仍然返回 `token_invalidated` 时，会被临时黑名单隔离的秒数，默认 `3600`

当前运行时不再在 `EasyRegister` 本地做 provider 顺序路由。

- 默认直接调用 `EasyEmail` 的 mailbox 能力接口
- 当前未显式设置 `REGISTER_MAILBOX_ROUTING_PROFILE_ID` 时，不再在 `EasyRegister` 侧默认指定 `high-availability`；会直接使用 `EasyEmail` 自己当前的默认 routing 策略
- 如果没有显式设置 `REGISTER_MAILBOX_STRATEGY_MODE_ID` / `REGISTER_MAILBOX_PROVIDERS`
  就使用 `EasyEmail` 自己的默认 strategy mode
- `REGISTER_MAILBOX_PROVIDERS` 现在只作为可选的 provider group 过滤条件透传给 `EasyEmail`；留空时不会再在 `EasyRegister` 本地默认收窄到 `m2u + moemail`
- `REGISTER_MAILBOX_STRATEGY_MODE_ID` 现在只作为可选的 strategy mode 透传给 `EasyEmail`
- `REGISTER_MAILBOX_ROUTING_PROFILE_ID` 现在只作为可选的 routing profile id 透传给 `EasyEmail`

provider 的具体能力差异都由 `EasyEmail` 内部处理。对调度层来说：

- 只关心 open / read / release 这些统一邮箱能力
- 如果某个 provider 不支持 release / delete mailbox，`EasyEmail` 会返回统一 skip/no-op 语义
- 调度层不再根据 provider 名字分支处理 release 成功条件
- 业务层当前只根据“域名黑名单 + 服务商黑名单”决定是否立即释放邮箱并重试，不再因为域名不在某个白名单池里而拒绝
- 对同一业务下同一邮箱域名，连续失败达到阈值后会进入运行态动态黑名单；默认阈值为 `500`
- 运行态动态黑名单默认 6 小时后过期并允许重新试探；显式域名/provider 黑名单不受影响
- 业务域名池全部命中运行态动态黑名单时，不再强制指定 MoEmail 业务域，而是回到 `EasyEmail` 的 `auto` 路由选择其他可用 provider/domain
- 如果 `EasyEmail auto` 仍连续返回已被动态黑名单拦截的 provider/domain，默认直接失败并释放邮箱；只有显式设置 `REGISTER_MAILBOX_DYNAMIC_BLACKLIST_EXHAUSTED_FALLBACK=true` 才会恢复旧的“使用最后一个动态黑名单邮箱兜底”行为
- `obtain_codex_oauth` 现在默认仍先走无手机号路径；只有当 `EasyProtocol` 返回 `phoneVerificationRequired=true` 时，调度层才会调用 `EasySms`
- 当前默认允许付费 `hero_sms`；如需回退到免费-only，可重新设置 `REGISTER_SMS_PROVIDER_BLACKLIST=hero_sms` 或 `REGISTER_SMS_ALLOW_PAID=false`
- `REGISTER_SMS_SELECTION_MODE` 在走 `hero_sms` 这类付费短信 provider 时生效；当前默认值使用 `balanced` 以保持与 `EasySms` 原生 API 的合法枚举一致
- `REGISTER_SMS_TERMINAL_INVALID_PHONE_BLACKLIST_SECONDS` 只影响 OpenAI 明确拒绝某个免费短信号码为 `invalid_phone_number` 后的 provider-phone 黑名单 TTL；默认 `21600` 秒。它不会开启付费短信，也不会放开号码复用。
- `hero_sms` 收到付费验证码后，如果验证码提交没有完成 OAuth，运行时会写入 `providerCircuitBreakers` 并立即阻止新的 `hero_sms` 会话；该保护没有自动过期时间，也不会被动态 provider 黑名单放宽逻辑绕过。
- 修复并完成隔离验证前只能查询 breaker：`python scripts/manage-sms-provider-circuit-breaker.py status --provider hero_sms`。人工恢复必须显式执行 `clear --confirm-fixed`，随后再恢复部署侧 provider blacklist。

协议执行能力已经迁出本目录，当前通过下面的服务边界完成：

- `EasyRegister` -> `EasyProtocol` -> `PythonProtocol`

调度层默认不会把语义步骤钉死到某个具体执行器。像
`upload_file_to_r2` 这样的能力，当前虽然由 `PythonProtocol` 实现，
但 `EasyRegister` 仍然只调用 `EasyProtocol`，并默认使用 `strategy`
模式由它决定当前使用哪个可用执行器。

当前顶层 DST 已经把 `upload_file_to_r2` 接进主链，位置在
所有资源释放步骤之后。它上传的是最终 auth JSON，而不是
`openai_oauth` 种子文件。这样代理链路、邮箱名额和 team 席位可以先释放，
然后再执行上传。启用条件是传入 `r2_target_folder`，或者让调度器使用
DST `platform` 字段作为默认目录；当前这条链默认就是：

- `codex/<最终auth-json文件名>`

如果显式目录和默认目录都没有，这一步会按 `enabledWhen` 语义自动跳过，
不会影响正常注册链。

也就是说，`create_openai_account / invite_codex_member / obtain_codex_oauth / revoke_codex_member`
这些协议语义步骤不再在 `EasyRegister` 内部本地执行，而是交给 `ProtocolService`
中的 `PythonProtocol` 实现。

当前明确仍然外置、不放入本目录的只有：

- EasyProxy / EasyBrowser / EasyEmail 的服务实现
- EasyProtocol / PythonProtocol 的协议执行实现

这些仍然保留在各自原始仓库或容器中，这里只保留调度层调用它们所需的客户端代码。

`EasyRegister` 自身的容器实例编排也已经独立出来，不再放在 `deploy/` 下：

- `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\compose\docker-compose.yaml`

当前 compose 会把所有 `EasyRegister` 容器加入外部 Docker 网络 `EasyAiMi`，
方便直接访问同一网络内的 `EasyEmail`、`EasyProxy`、`EasyProtocol`
和其他 EZ 系服务。

部署时推荐先通过根级脚本：

- `scripts/deploy-compose.ps1`

它会在 `docker compose up` 前调用：

- `scripts/materialize-output-links.ps1`

把统一输出根 `REGISTER_OUTPUT_DIR_HOST` 下的用户层目录按需物化成目录联接。
这样容器内部仍然只看到一个 `/shared/register-output`，但宿主机可以把：

- `openai/pending`
- `openai/converted`
- `openai/failed-once`
- `openai/failed-twice`
- `codex/free`
- `codex/team`
- `codex/plus`
- `codex/team-input`
- `codex/team-mother-input`

分别链接到用户自己提供的目标文件夹。

当前容器入口 [infinite_runner.py](C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\server\services\orchestration_service\src\infinite_runner.py)
已经不是“单轮串行 while 循环”，而是：

- 一个 supervisor 进程
- 多个 worker 子进程
- worker 目录形态：
- `REGISTER_OUTPUT_ROOT/worker-01/run-...`
- `REGISTER_OUTPUT_ROOT/worker-02/run-...`

这样可以保留一个中心控制端，同时让多个 worker 并发执行完整 DST。

当前推荐的本地模块入口是：

- `python -m dst_flow`
- `python -m infinite_runner`

当前 compose 会同时运行三类 supervisor：

- 主注册 flow
  - 默认 `7` 个 worker
  - 默认直接走 `EasyEmail` 的 mailbox strategy
  - 失败时把 `openai_oauth` 复制到 `openai/failed-once`
- `openai_oauth` 续跑 flow
  - 默认 `2` 个 worker
  - 从 `openai/failed-once` claim 一个 `openai_oauth`
  - 默认按 `free` 本地分流比例写入本地目录，不再上传，默认 100%
  - 只有把 `REGISTER_FREE_LOCAL_SPLIT_PERCENT` 调低后，未命中本地分流的 free 成品才会上传
  - 失败则把 claim 文件放回池中
- team 扩容 flow
  - 默认 `1` 个 worker
  - 定时把 `openai/pending` 和 `openai/failed-twice` 的文件随机移动到 `others/team-pre-pool`
  - 等待人工把已订阅 team 的 mother 凭证放进 `codex/team-mother-input`
  - 自动完成 mother 二次登录、workspace 选择、4 个成员邀请、4 个成员 OAuth
  - 未命中本地分流的 team 成品会上传到 `codex-team/<文件名>` 并删除本地文件
  - 命中本地分流的 team 成品会写入本地目录

`main` / `continue` 默认不会从 `codex/team-mother-input` 读取邀请码母号；它们默认读取的是：

- `codex/team-input`

并且当最终 free 上传成功，或者本地分流/团队上传收尾成功后，
worker 会自动删除该轮对应的 `run-...` 目录。

资源容量兜底：

- Codex team 容量满时，supervisor 会在所有 team 凭证都进入容量冷却后
  触发 `cleanup_codex_capacity`，只清理 Codex 相关 pending invite 和成员席位。
- 邮箱容量恢复现在通过 `recover_mailbox_capacity` 统一走 `EasyEmail`。
- supervisor 只在连续 `mailbox_unavailable` 后把失败 detail 上报给 `EasyEmail`。
- 具体要不要执行某个 provider 的恢复动作、执行什么动作，都由 `EasyEmail` 内部决定。
- 当前 `EasyEmail` 内部会在识别到 `MoEmail` 容量故障特征时执行对应清理。
- 邮箱容量恢复由这些环境变量控制：
- `REGISTER_MAILBOX_CLEANUP_FAILURE_THRESHOLD`
- `REGISTER_MAILBOX_CLEANUP_COOLDOWN_SECONDS`
- `REGISTER_MAILBOX_CLEANUP_MAX_DELETE_COUNT`

主注册 supervisor 还会暴露一个轻量运行态面板，默认宿主机地址是：

- `http://127.0.0.1:19790/`

机器可读状态接口是：

- `http://127.0.0.1:19790/api/status`

状态数据来自共享输出目录中的 worker 状态文件和 `EasyProtocol`
control-plane stats。面板会展示：

- `PythonProtocol-01` 到 `PythonProtocol-10` 的活跃请求数和命中数
- 主注册 / `openai_oauth` 续跑两条流水线的配置 worker 数和当前活跃 worker 数
- `openai/pending` 当前积压数量
- `others/team-pre-pool` / `codex/team-mother-input` / `codex/team` 这些目录不会进入主注册面板聚合统计
- 最近成功上传到 R2 的 auth JSON

### OpenAI 账号可用性审计 DST

当前新增了一条独立审计流，用来批量检阅已有账号文件是否仍可完成邮箱恢复 + 标准 OpenAI 网页登录。

流文件：

- `server/services/orchestration_service/flows/openai-account-availability-audit-v1.semantic-flow.json`

入口参数支持两种模式：

- 单文件模式：`--account-file`
- 目录批处理模式：`--account-dir`

目录批处理是推荐方式。runner 只要把某个 flow spec 的 `inputSourceDir` 指到账号目录，就会把该目录视为可运行输入，不需要额外改 scheduler 语义。审计流会优先读取目录下的 `*.json` 账号文件，并支持把已尝试账号移动到单独目录。

推荐 CLI 形式：

```powershell
python -m dst_flow `
  --flow-path server/services/orchestration_service/flows/openai-account-availability-audit-v1.semantic-flow.json `
  --account-dir C:\path\to\accounts
```

也可以显式指定输出归档目录：

- `--loginable-dir`：登录成功后移动到这里，默认源目录下 `可登录账号`
- `--deleted-dir`：判定为已删除/已停用后移动到这里，默认源目录下 `deleted-confirmed`
- `--audit-path`：JSONL 审计文件，默认源目录下 `account-availability-audit.jsonl`

如果账号文件里已经包含 `recoveryDataCredential` / `recovery_data_credential`，调度层会原样保留并透传给 EasyProtocol / EasyEmail 恢复链路；这也是后续继续恢复邮箱所必需的字段。

runner 接入方式：

- 在 `RunnerFlowSpec` 里把 `instance_role` 设成 `account-audit`
- 把 `input_source_dir` 设成账号目录
- 可选设置 `input_claims_dir` 用于 claim 模式

最小 flow spec 示例：

```json
[
  {
    "name": "openai-account-availability-audit",
    "flowPath": "server/services/orchestration_service/flows/openai-account-availability-audit-v1.semantic-flow.json",
    "instanceRole": "account-audit",
    "weight": 1,
    "taskMaxAttempts": 1,
    "inputSourceDir": "C:/account-audit/input",
    "inputClaimsDir": "C:/account-audit/input/_claims",
    "concurrencyLimit": 1
  }
]
```

这样 worker 会把该目录作为输入源传入 `run_dst_flow_once(...)`，由新审计流完成“选择账号 -> 恢复邮箱 -> 登录校验 -> 归档”的完整步骤。
