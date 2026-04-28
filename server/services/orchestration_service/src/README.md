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

当前目录已经去掉 `new_protocol_register/` 这一层，文件直接展开在
`server/services/orchestration_service/src/` 下。

运行时内部命名也已经收口到更中性的 `orchestration` / `register` 语义。
当前只使用下面这组环境变量名称：

- `REGISTER_MAILBOX_TTL_SECONDS`
- `REGISTER_SMALL_SUCCESS_SEED_MAX_AGE_SECONDS`
- `REGISTER_MAILBOX_ROUTING_PROFILE_ID`
- `REGISTER_MAILBOX_STRATEGY_MODE_ID`
- `REGISTER_MAILBOX_PROVIDERS`
- `REGISTER_ENABLE_EASY_PROXY`
- `REGISTER_REQUIRE_EASY_PROXY`
- `REGISTER_PROXY_HOST_ID`
- `REGISTER_PROXY_MODE`
  - 代理链获取模式；当前默认 `auto` 语义会先走 `EasyProxy` 的租约 checkout，再回退到 random-node
  - 如果要强制只走租约，显式设为 `lease`
  - 如果要强制只走随机节点，显式设为 `random-node`
- `REGISTER_PROXY_TTL_MINUTES`
- `REGISTER_PROXY_UNIQUE_ATTEMPTS`
- `REGISTER_PROXY_RECENT_WINDOW_SECONDS`
- `REGISTER_PROXY_FAILURE_WINDOW_SECONDS`
- `REGISTER_TEAM_AUTH_TEMP_BLACKLIST_SECONDS`
  当某个 team / mother 账号在刷新后仍然返回 `token_invalidated` 时，会被临时黑名单隔离的秒数，默认 `3600`

当前运行时不再在 `EasyRegister` 本地做 provider 顺序路由。

- 默认直接调用 `EasyEmail` 的 mailbox 能力接口
- 当前默认请求 `EasyEmail` 的 `high-availability` routing profile
- 如果没有显式设置 `REGISTER_MAILBOX_STRATEGY_MODE_ID` / `REGISTER_MAILBOX_PROVIDERS`
  就使用 `EasyEmail` 自己的默认 strategy mode
- `REGISTER_MAILBOX_PROVIDERS` 现在只作为可选的 provider group 过滤条件透传给 `EasyEmail`
- `REGISTER_MAILBOX_STRATEGY_MODE_ID` 现在只作为可选的 strategy mode 透传给 `EasyEmail`
- `REGISTER_MAILBOX_ROUTING_PROFILE_ID` 现在只作为可选的 routing profile id 透传给 `EasyEmail`

provider 的具体能力差异都由 `EasyEmail` 内部处理。对调度层来说：

- 只关心 open / read / release 这些统一邮箱能力
- 如果某个 provider 不支持 release / delete mailbox，`EasyEmail` 会返回统一 skip/no-op 语义
- 调度层不再根据 provider 名字分支处理 release 成功条件

协议执行能力已经迁出本目录，当前通过下面的服务边界完成：

- `EasyRegister` -> `EasyProtocol` -> `PythonProtocol`

调度层默认不会把语义步骤钉死到某个具体执行器。像
`upload_file_to_r2` 这样的能力，当前虽然由 `PythonProtocol` 实现，
但 `EasyRegister` 仍然只调用 `EasyProtocol`，并默认使用 `strategy`
模式由它决定当前使用哪个可用执行器。

当前顶层 DST 已经把 `upload_file_to_r2` 接进主链，位置在
所有资源释放步骤之后。它上传的是最终 auth JSON，而不是
`small_success` 种子文件。这样代理链路、邮箱名额和 team 席位可以先释放，
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
  - 失败时把 `small_success` 复制到 `small-success-pool`
- 小成功续跑 flow
  - 默认 `2` 个 worker
  - 从 `small-success-pool` claim 一个 `small_success`
  - 默认按 `free` 本地分流比例写入本地目录，不再上传，默认 100%
  - 只有把 `REGISTER_FREE_LOCAL_SPLIT_PERCENT` 调低后，未命中本地分流的 free 成品才会上传
  - 失败则把 claim 文件放回池中
- team 扩容 flow
  - 默认 `1` 个 worker
  - 定时把 `small-success-pool` 的文件随机移动到 `others/team-pre-pool`
  - 等待人工把已订阅 team 的 mother 凭证放进 `team-mother-pool`
  - 自动完成 mother 二次登录、workspace 选择、4 个成员邀请、4 个成员 OAuth
  - 未命中本地分流的 team 成品会上传到 `codex-team/<文件名>` 并删除本地文件
  - 命中本地分流的 team 成品会写入本地目录

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
- 主注册 / 续跑两条流水线的配置 worker 数和当前活跃 worker 数
- `small-success-pool` 当前积压数量
- `others/team-pre-pool` / `team-mother-pool` / `team-oauth-pool` 这些目录不会进入主注册面板聚合统计
- 最近成功上传到 R2 的 auth JSON
