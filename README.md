# EasyRegister

这个目录是当前独立维护的 `EasyRegister` 注册运行时闭包。

目标不是复制整个旧工程，而是保留当前已经跑通的：

- DST 顶层编排
- DST 统一调度器
- EasyEmail / EasyProxy / EasyProtocol 调度适配器
- 通过 `EasyProtocol -> PythonProtocol` 执行协议语义步骤
- 调用 Easy 网络中各服务实例所需的客户端代码

当前刻意 **没有** 迁移的内容：

- EasyProxy 服务实现
- EasyEmail 服务实现
- EasyBrowser 服务实现
- 旧工程中的测试、调试、快照、历史产物

这些 EasyXXX 服务仍然应由外部仓库或容器实例提供，`EasyRegister` 只负责调用它们。

另外，迁移过程中已经移除了不属于当前 DST 主链热路径的旧入口文件，例如：

- `first_register.py`
- `second_oauth.py`
- `protocol_runtime/platform_protocol_register.py`
- `protocol_runtime/semantic_auth_flow.py`

## 开发规范

当前统一的跨仓开发规范已经落在：

- `docs/development-workflow.md`
- `docs/root-host-deploy-standard.md`

后续请按这份文档执行新仓开发、本地优先验证、临时测试资产归档到 `linshi`、
以及最终 GHCR 验收流程。

## 当前目录结构

- `compose/`
  - `EasyRegister` 容器实例的单独编排入口
- `server/services/orchestration_service/flows/`
  - 顶层 DST / semantic-flow
- `server/services/orchestration_service/src/`
  - 顶层调度器
  - EasyEmail / EasyProxy / EasyProtocol 适配层
  - `others/` 公共模型、路径、运行时辅助
- `server/services/python_shared/src/`
  - 调用 EasyEmail / EasyProxy 的客户端代码

`EasyRegister` 当前不再内嵌本地协议执行器，协议执行边界已经变成：

- `EasyRegister` -> `EasyProtocol` -> `PythonProtocol`

## 当前主流程

顶层 DST 入口：

- `server/services/orchestration_service/src/dst_flow.py`

当前 supervisor 入口：

- `server/services/orchestration_service/src/infinite_runner.py`
  - 这是当前薄入口文件
  - 实际 supervisor 实现在 `server/services/orchestration_service/src/others/runner_supervisor.py`
  - 当前推荐模块入口是 `python -m infinite_runner`

当前顶层步骤：

1. `acquire_mailbox`
2. `acquire_proxy_chain`
3. `create_openai_account`
4. `invite_codex_member`
5. `obtain_codex_oauth`
6. `revoke_codex_member`
7. `release_proxy_chain`
8. `release_mailbox`
9. `upload_file_to_r2`（启用时上传最终 auth JSON）

## 运行前提

运行这套代码需要外部 Easy 网络实例已经可用：

- EasyEmail
- EasyProxy
- EasyProtocol
- PythonProtocol

并且这些 EZ 系容器应统一加入外部 Docker 网络：

- `EasyAiMi`

当前 `compose/docker-compose.yaml` 和 `compose/docker-compose.test.yaml` 都会直接挂到这个外部网络，
这样 `EasyRegister` 才能通过容器名访问 `easy-email`、`easy-proxy`、
`easy-protocol` 以及其他 EZ 系服务实例。

并且需要提供：

- team auth json
- 可访问的 EasyEmail / EasyProxy 容器实例
- 可访问的 EasyProtocol / PythonProtocol 容器实例

如果未显式传入环境变量，当前代码会优先尝试：

- `MAILBOX_SERVICE_BASE_URL = http://localhost:18080`
- `MAILBOX_SERVICE_API_KEY`
  - 从当前工作树向上搜索已有的 `EmailService/deploy/EasyEmail/config.yaml`
  - 不在 `EasyRegister` 内部单独维护一份 EasyEmail 配置

当前邮箱策略已经收口成“由 `EasyEmail` 决定 provider 路由”：

- `EasyRegister` 默认不再本地维护 provider 主次顺序
- 主注册 / 续跑 / team 三类实例默认都直接调用 `EasyEmail` 的 mailbox 能力接口
- 如果不显式设置邮箱策略相关环境变量，就使用 `EasyEmail` 自己的默认 strategy mode
- 当前默认会请求 `EasyEmail` 的 `high-availability` routing profile
- `REGISTER_MAILBOX_PROVIDERS` 现在只作为可选的 provider group 过滤条件透传给 `EasyEmail`
- `REGISTER_MAILBOX_STRATEGY_MODE_ID` 现在只作为可选的 strategy mode 透传给 `EasyEmail`
- `REGISTER_MAILBOX_ROUTING_PROFILE_ID` 现在只作为可选的 routing profile id 透传给 `EasyEmail`
- `REGISTER_MAILBOX_BUSINESS_KEY` 现在只作为默认业务标签兜底；真正的业务标签应由具体 DST / task 传入
- `REGISTER_MAILBOX_DOMAIN_BLACKLIST` 是默认业务策略的显式域名黑名单
- `REGISTER_MAILBOX_DOMAIN_POOL` 是默认业务策略的偏好域名池
- `REGISTER_MAILBOX_BUSINESS_POLICIES_JSON` 可以在同一个镜像实例里声明多套业务邮箱策略，按业务 key 选不同域名池和黑名单

当前邮箱域名黑名单是业务级别的，而不是全局级别的：

- `openai` 业务里拉黑的域名，不会自动污染其他业务的域名判断
- 运行态统计会按 task / flow 实际携带的 `businessKey` 分桶记录
- 如果当前业务显式拉黑某个域名，后续申请到该域名会立即释放并重新申请
- 对 `moemail` 这类业务邮箱，如果实际返回域名不在当前业务配置的域名池里，也会立即丢弃并重申请
- 当前默认策略下，只有明确命中 `unsupported_email` 这类“业务明确不支持该邮箱域”的结果，才建议进入业务黑名单
- 像 `cksa.eu.cc` 这种当前阶段高失败、但仍可能偶发通过的域名，默认只做统计，不会因为失败率高就自动进入业务黑名单

推荐做法是让每个 DST 在自己的 flow metadata 里声明 `mailbox.businessKey`，然后由统一镜像实例按业务 key 选策略，而不是为每个业务单独做一份“当前 DST 专用镜像”。

`high-availability` 是 `EasyEmail` 内部的通用路由档位，不是 `EasyRegister`
里的业务白名单。当前它会把高可用邮箱优先收敛到 `m2u + moemail`，以后如果你要
调整高可用池，应优先在 `EasyEmail` 内部变更这个档位，而不是改业务侧代码。

其中 provider 的具体能力差异都应由 `EasyEmail` 内部处理。

- `EasyRegister` 默认只关心 open / read / release 这些统一邮箱能力
- 如果某个 provider 不支持 delete / release mailbox，`EasyEmail` 会返回统一的 skip/no-op 语义
- `EasyRegister` 不再根据 provider 名字分支处理 release 成功条件

Team 凭证读取推荐通过环境变量控制：

- `REGISTER_TEAM_AUTH_PATH`
  - 直接指定某一个 team json 文件
- `REGISTER_TEAM_AUTH_PATHS`
  - 直接指定多个 team json 文件，优先级最高
- `REGISTER_TEAM_AUTH_DIR`
  - 指定 team 凭证目录，系统会按 glob 选择最新文件
- `REGISTER_TEAM_AUTH_LOCAL_DIR`
  - 本地 team 凭证目录，默认优先搜索
- `REGISTER_TEAM_AUTH_DEFAULT_DIR`
  - 默认 team 凭证目录，本地目录为空时再回退搜索
- `REGISTER_TEAM_AUTH_DIRS`
  - 指定多个 team 凭证目录，使用系统路径分隔符分隔
- `REGISTER_TEAM_AUTH_GLOB`
  - 指定目录搜索模式，默认是 `*-team.json`

默认优先级是：

1. `REGISTER_TEAM_AUTH_PATHS`
2. `REGISTER_TEAM_AUTH_PATH`
3. `REGISTER_TEAM_AUTH_DIRS`
4. `REGISTER_TEAM_AUTH_LOCAL_DIR`
5. `REGISTER_TEAM_AUTH_DEFAULT_DIR`

team 凭证选择规则：

- 优先匹配文件名以 `codex-team-mother-` 开头的母号凭证
- 如果没有显式母号文件，则扫描 team 凭证内容，优先识别 `auth_provider=passwordless` 且 `amr` 包含 `otp` / `otp_email` 的母号候选
- 从可用母号候选中先排除冷却中的凭证，然后随机选取，避免始终挑最新文件

R2 上传如果要在 DST 主链里启用，至少需要提供目标文件夹：

- `--r2-target-folder`

如果没有显式指定 `--r2-target-folder`，当前调度器会默认使用
DST 的 `platform` 字段作为上传目录。以当前流程为例，最终 auth JSON
默认会组织成：

- `codex/<最终auth-json文件名>`

其他参数既可以通过 DST CLI 显式传入，也可以由 `PythonProtocol` 执行器环境变量提供：

- `--r2-bucket`
- `--r2-object-name`
- `--r2-account-id`
- `--r2-endpoint-url`
- `--r2-access-key-id`
- `--r2-secret-access-key`
- `--r2-region`
- `--r2-public-base-url`

## 直接运行示例

```powershell
python -m dst_flow `
  --output-dir "C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\tmp\run" `
  --team-auth "C:\Users\vmjcv\.cli-proxy-api\codex-1dfcda64-moddc8da@sall.cc-team.json"
```

也可以直接启动 supervisor 模块入口：

```powershell
python -m infinite_runner
```

## 容器编排入口

`EasyRegister` 的容器实例编排不再放在 `deploy/` 下，当前单独入口是：

- `compose/docker-compose.yaml`

这份 compose 会把所有 `EasyRegister` 容器直接加入外部网络 `EasyAiMi`。

当前推荐的宿主机一键部署入口是仓库根目录下的：

- `deploy-host.ps1`

即使操作者只单独下载这一份脚本，它也可以先自举拉取本仓所需文件，再继续完成部署。
在 blank-host 路径下，脚本还会自动补齐：
- `EASY_PROXY_BASE_URL=http://easy-proxy:29888`
- 一个本地安全的 `EASY_PROTOCOL_CONTROL_TOKEN`
- `REGISTER_DASHBOARD_LISTEN=0.0.0.0:9790`
- `REGISTER_DASHBOARD_ALLOW_REMOTE=true`

这样宿主机发布的 dashboard 端口可以直接从 host 访问，不会因为容器内默认绑定 `127.0.0.1` 而失效。

它默认会：

- 使用仓库内 `runtime/register-output` 作为统一输出根
- 把 `main` / `continue` 的 `team` 输入挂到：
  - `C:\Users\vmjcv\.cli-proxy-api\team`
- 把用户层输出目录默认映射为：
  - `codex/free -> C:\Users\vmjcv\.cli-proxy-api`
  - `codex/team -> C:\Users\vmjcv\.cli-proxy-api\team`
  - `codex/team-input -> C:\Users\vmjcv\.cli-proxy-api\team`
  - `codex/team-mother-input`
    - 默认不做宿主别名映射
- 自动物化目录联接
- 然后执行 `docker compose up`

典型启动命令：

```powershell
powershell -ExecutionPolicy Bypass -File "C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\deploy-host.ps1"
```

如果你要在别的宿主机上改映射，可以直接覆盖脚本参数，例如：

```powershell
powershell -ExecutionPolicy Bypass -File "C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\deploy-host.ps1" `
  -OutputDirHost "D:\EasyRegister\runtime\register-output" `
  -CodexFreeDirHost "D:\vault\cli-proxy-api" `
  -CodexTeamDirHost "D:\vault\cli-proxy-api\team" `
  -CodexTeamInputDirHost "D:\vault\cli-proxy-api\team"
```

底层通用 compose 包装入口仍然是：

- `scripts/deploy-compose.ps1`

而目录联接物化脚本是：

- `scripts/materialize-output-links.ps1`

如果你只想直接用通用入口，也可以这样调用：

```powershell
powershell -ExecutionPolicy Bypass -File "C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\scripts\deploy-compose.ps1"
```

如果你只想直接查看 compose 配置，或者已经提前物化过输出目录链接，也可以继续手工执行：

```powershell
docker compose -f "C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\compose\docker-compose.yaml" up -d
```

`deploy-compose.ps1` 会在 `docker compose up` 前先执行：

- `scripts/materialize-output-links.ps1`

它会在 `REGISTER_OUTPUT_DIR_HOST` 指向的统一输出根下面，按用户层目录契约物化这些目录：

- `openai/pending`
- `openai/converted`
- `openai/failed-once`
- `openai/failed-twice`
- `codex/free`
- `codex/team`
- `codex/plus`
- `codex/team-input`
- `codex/team-mother-input`

如果用户额外提供了本地目标目录，脚本会在统一输出根下创建对应的目录联接：

- `REGISTER_OUTPUT_ALIAS_ROOT_HOST`
  - 给所有用户层目录提供一个统一的别名根
- `REGISTER_OPENAI_PENDING_DIR_HOST`
- `REGISTER_OPENAI_CONVERTED_DIR_HOST`
- `REGISTER_OPENAI_FAILED_ONCE_DIR_HOST`
- `REGISTER_OPENAI_FAILED_TWICE_DIR_HOST`
- `REGISTER_CODEX_FREE_DIR_HOST`
- `REGISTER_CODEX_TEAM_DIR_HOST`
- `REGISTER_CODEX_PLUS_DIR_HOST`
- `REGISTER_CODEX_TEAM_INPUT_DIR_HOST`
- `REGISTER_CODEX_TEAM_MOTHER_INPUT_DIR_HOST`

推荐用法是：

```powershell
$env:REGISTER_OUTPUT_DIR_HOST = "C:\EasyRegister\output"
$env:REGISTER_OUTPUT_ALIAS_ROOT_HOST = "D:\EasyRegisterVault"
powershell -ExecutionPolicy Bypass -File "C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\scripts\deploy-compose.ps1"
```

这时容器仍然只挂一个：

- `REGISTER_OUTPUT_DIR_HOST -> /shared/register-output`

但宿主机会在 `C:\EasyRegister\output` 下看到指向 `D:\EasyRegisterVault\...` 的目录联接。

## 隔离测试 compose

用于本仓开发验证的隔离入口是：

- `compose/docker-compose.test.yaml`

这份 compose 与当前线上 `register-*` 容器做了明确隔离：

- 容器名前缀固定为 `easyregister-test-*`
- 默认镜像名为 `easyregister/easyregister-test:local`
- 默认 dashboard 宿主机端口为 `29790`
- 默认宿主机输出目录使用仓库内 `tmp/easyregister-test-output`
- 默认本地 free / team 输出目录分别使用 `tmp/easyregister-test-free` 与 `tmp/easyregister-test-team`
- 仍然加入外部网络 `EasyAiMi`，方便和其他 EZ 系容器联调

典型启动命令：

```powershell
powershell -ExecutionPolicy Bypass -File "C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\scripts\deploy-compose.ps1" `
  -ComposeFile "C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\compose\docker-compose.test.yaml" `
  -Build
```

本地迭代时，compose 默认会用已有的 `easy-register/easy-register:local`
作为构建基底，只覆盖当前代码层，避免每次代码更新都重新拉取基础镜像和 PyPI
依赖。如果是全新机器首次构建，可以显式设置：

```powershell
$env:REGISTER_SERVICE_BASE_IMAGE="python:3.10-bookworm"
```

## 本地测试入口

当前仓库已经补了最小工程化测试入口：

- `pyproject.toml`
- `requirements-dev.txt`

典型本地验证命令：

```powershell
python -m unittest discover -s "C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\tests" -v
```

## GitHub Actions

当前仓库已经补了两条 GitHub Actions 工作流：

- `.github/workflows/validate.yml`
  - 在 `main`、`pull_request` 和手动触发时执行
  - 运行 `python -m unittest discover -s tests -v`
  - 运行 `docker compose -f compose/docker-compose.test.yaml config`
- `.github/workflows/publish-ghcr-image.yml`
  - 在 `v*` / `release-*` tag 推送时执行
  - 也支持 `workflow_dispatch`
  - 构建并推送 `ghcr.io/<owner>/easyregister`
  - 发布前会做一次镜像 smoke

用于 GitHub Actions 的运行时配置示例文件是：

- `deploy/easyregister.runtime.env.example`

配置与 secrets 方案采用和现有仓一致的“在 Action 中物化运行配置”思路：

- 优先使用完整的 Base64 env secret：
  - `EASYREGISTER_RUNTIME_ENV_B64`
- 如果不提供完整 env，也可以按单项 secret 覆盖：
  - 约定前缀是 `EASYREGISTER_ENV_`
  - 例如：
    - `EASYREGISTER_ENV_MAILBOX_SERVICE_API_KEY`
    - `EASYREGISTER_ENV_EASY_PROTOCOL_CONTROL_TOKEN`
    - `EASYREGISTER_ENV_REGISTER_MAILBOX_ROUTING_PROFILE_ID`

GHCR 登录也支持和参考仓同样的双路径：

- 优先使用：
  - `EASYREGISTER_PUBLISH_GHCR_USERNAME`
  - `EASYREGISTER_PUBLISH_GHCR_TOKEN`
- 如果不提供，则回退到 GitHub Actions 默认的 `GITHUB_TOKEN`

当前默认 compose 会拉起一个 `EasyRegister` 混跑实例：

- `easy-register`
  - 主调度容器
  - 默认以 `REGISTER_INSTANCE_ROLE=mixed` 运行
  - 通过 `REGISTER_FLOW_SPECS_JSON` 同时挂载 `main`、`continue`、`team` 三条 flow
  - 暴露运行态面板
  - 默认 `10` 个 worker

这个实例采用“单实例 supervisor + 多 worker 进程”模型：

- 容器内只有一个中心控制端
- supervisor 负责拉起多个独立 worker 进程
- 每个 worker 串行执行一整条 DST
- 多个 worker 可以并发跑不同任务，也可以在同一个实例里混跑不同 flow
- worker 之间通过进程隔离，避免代理/邮箱运行时全局状态互相污染

相关环境变量：

- `REGISTER_WORKER_COUNT`
  - supervisor 拉起的总 worker 数量
- `REGISTER_WORKER_STAGGER_SECONDS`
  - worker 启动错峰秒数
- `REGISTER_LOOP_DELAY_SECONDS`
  - 每个 worker 完成一轮后的等待秒数
- `REGISTER_INFINITE_MAX_RUNS`
  - 整个 supervisor 总共允许启动的任务数；`0` 表示无限
- `REGISTER_FLOW_PATH`
  - 兼容旧模式的单 flow 入口；混跑部署下默认留空
- `REGISTER_FLOW_SPECS_JSON`
  - 新的混跑入口；可以在同一个实例里声明多条 flow spec，每条 spec 自带 `path`、`role`、`weight`，worker 每轮会从可运行 flow 中按权重选择一条
- `REGISTER_INSTANCE_ID`
  - service 级实例标识；建议混跑实例使用类似 `mixed`
- `REGISTER_INSTANCE_ROLE`
  - service 级标签；混跑实例建议设成 `mixed`

一个典型的混跑配置示例：

```json
[
  {
    "name": "openai-main",
    "path": "server/services/orchestration_service/flows/codex-openai-account-v1.semantic-flow.json",
    "role": "main",
    "weight": 5
  },
  {
    "name": "openai-continue",
    "path": "server/services/orchestration_service/flows/codex-openai-oauth-continue-v1.semantic-flow.json",
    "role": "continue",
    "weight": 2
  },
  {
    "name": "codex-team-expand",
    "path": "server/services/orchestration_service/flows/codex-team-expand-v1.semantic-flow.json",
    "role": "team",
    "weight": 1
  }
]
```

其中：

- `main` flow 默认消费 `openai/pending`
- `continue` flow 默认消费 `openai/failed-once`
- `team` flow 默认从 `openai/pending` 和 `openai/failed-twice` 补 `team-pre-pool`，并等待 `codex/team-mother-input` 有可用 mother 后再被调度
- `REGISTER_TEAM_PRE_FILL_COUNT`
  - 每轮最多从 `openai/pending` 和 `openai/failed-twice` 随机移动到 `others/team-pre-pool` 的文件数，默认 `1`
- `REGISTER_TEAM_MEMBER_COUNT`
  - 每个 mother 凭证扩容时要从 `others/team-pre-pool` claim 的成员数，默认 `4`
- `REGISTER_TEAM_PRE_POOL_DIR`
  - 默认 `/shared/register-output/others/team-pre-pool`
- `REGISTER_TEAM_MOTHER_POOL_DIR`
- 默认 `/shared/register-output/codex/team-mother-input`
- `REGISTER_TEAM_AUTH_LOCAL_DIR`
  - `main` / `continue` 默认读取 `/shared/register-output/codex/team-input`
- `REGISTER_TEAM_AUTH_DEFAULT_DIR`
  - `main` / `continue` 默认读取 `/shared/register-output/codex/team-input`
- `REGISTER_TEAM_AUTH_DIR`
  - `main` / `continue` 默认读取 `/shared/register-output/codex/team-input`
- `REGISTER_TEAM_MOTHER_CLAIMS_DIR`
  - 默认 `/shared/register-output/others/team-mother-claims`
- `REGISTER_TEAM_MEMBER_CLAIMS_DIR`
  - 默认 `/shared/register-output/others/team-member-claims`
- `REGISTER_TEAM_POST_POOL_DIR`
  - 默认 `/shared/register-output/others/team-post-pool`
- `REGISTER_TEAM_POOL_DIR`
- 默认 `/shared/register-output/codex/team`
- `REGISTER_TEAM_WORKSPACE_SELECTOR`
  - 传给协议执行器的 workspace 选择策略，默认 `first_team`
- `REGISTER_FREE_LOCAL_SPLIT_PERCENT`
  - `free` 最终凭证本地分流比例，支持 `0-100` 或 `0-1` 写法，默认 `100`
- `REGISTER_FREE_LOCAL_DIR`
  - `free` 最终凭证的容器内目标目录，默认 `/shared/register-output/codex/free`
- `REGISTER_TEAM_LOCAL_SPLIT_PERCENT`
  - `team` 最终凭证本地分流比例，支持 `0-100` 或 `0-1` 写法，默认 `100`
- `REGISTER_TEAM_LOCAL_DIR`
  - `team` 最终凭证的容器内目标目录，默认 `/shared/register-output/codex/team`

## 资源容量兜底

主注册和续跑 flow 都会在失败或异常退出时执行资源释放。除此之外，
supervisor 还内置了两类容量兜底：

- Codex team 容量兜底
  - 当邀请持续失败并判定所有 team 凭证都进入容量冷却时，会触发一次
    `cleanup_codex_capacity`
  - 该步骤只清理 Codex 相关席位和 pending invite，不清理 owner
- 邮箱容量兜底
  - 当 `acquire-mailbox` 连续命中 `mailbox_unavailable` 时，supervisor 会触发一次
    `recover_mailbox_capacity`
  - `EasyRegister` 只把失败 detail 上报给 `EasyEmail`，由 `EasyEmail` 内部判断是否需要执行
    某个 provider 的容量恢复动作
  - 当前 `EasyEmail` 内部会在匹配到 `MoEmail` 容量故障特征时执行对应清理，默认最多处理 `30` 个邮箱
  - 这个操作可能使正在运行的任务失去邮箱，但可以接受，用于快速释放被卡住的 provider 容量

邮箱容量恢复相关环境变量：

- `REGISTER_MAILBOX_CLEANUP_FAILURE_THRESHOLD`
  - 连续多少次 mailbox 容量失败后触发强制清理，默认 `3`
- `REGISTER_MAILBOX_CLEANUP_COOLDOWN_SECONDS`
  - 两次强制清理之间的最小间隔，默认 `120`
- `REGISTER_MAILBOX_CLEANUP_MAX_DELETE_COUNT`
  - 单次强制清理最多删除的 MoEmail mailbox 数，默认 `30`

## Team 扩容流程

新增第三个 flow 的目录语义是：

- `openai/pending/`
  - 尚未进入 `codex` 转换的 `openai_oauth` 凭证
- `openai/converted/`
  - 已成功完成 `codex` 转换的 `openai_oauth` 凭证
- `openai/failed-once/`
  - 已完成第 1 次 `codex` 转换尝试但失败的 `openai_oauth` 凭证
- `openai/failed-twice/`
  - 已完成第 2 次 `codex` 转换尝试但失败的 `openai_oauth` 凭证
- `codex/team-mother-input/`
  - 用户手动提供的 team mother 凭证池
- `codex/team-input/`
  - `main` / `continue` 邀请链默认读取的 team 邀请凭证输入池
- `codex/free/`
  - `main` / `continue` 成功产出的 `codex-oauth` 凭证池
- `codex/team/`
  - `team` 转换成功后的 mother 和成员 `codex-oauth` 凭证池
- `codex/plus/`
  - 预留给 `plus` 类别的 `codex-oauth` 凭证池
- `others/`
  - 存放内部运行目录和中间目录，例如 `main-runs/`、`continue-runs/`、`team-runs/`、`team-pre-pool/`、`team-post-pool/`、claims、dashboard/state、debug 等
  - 不再承载用户层 free / team 成品目录

这条链路分为人工阶段和自动阶段：

- 人工阶段
  - `easy-register` 中的 `team` flow 会把随机挑出的预备账号移动到 `others/team-pre-pool`
  - 你手动从 `others/team-pre-pool` 选一个账号，登录并开通 team 套餐
  - 完成后你把这个 mother 凭证移动到 `codex/team-mother-input`
- 自动阶段
  - flow 监控 `codex/team-mother-input`，claim 一个 mother 凭证
  - 通过 `EasyProtocol -> PythonProtocol` 做 mother 二次登录
  - 如果登录后存在多个 workspace，优先选择有效 team workspace，当前默认策略是第一个 team space
  - 生成新的 mother team 凭证，并先写入 `codex/team`
  - 再从 `others/team-pre-pool` 随机 claim 4 个成员账号
  - 邀请这 4 个邮箱加入 mother 的 team workspace
  - 对这 4 个成员账号做一次 OAuth，拿到对应的 team 凭证
  - 将这 4 个成员 team 凭证写入 `codex/team`
  - 对未命中本地分流的成品自动上传到 `codex-team/<文件名>` 并删除本地文件

如果自动阶段失败：

- mother 凭证会放回 `codex/team-mother-input`
- 已 claim 的 4 个成员账号会放回 `others/team-pre-pool`
- 已经命中本地分流并写入本地目录的结果文件不会自动回滚

## 运行态面板

主注册实例内置一个轻量 HTTP 面板，默认地址是：

- `http://127.0.0.1:19790/`

机器可读 JSON 地址是：

- `http://127.0.0.1:19790/api/status`

面板会展示：

- 每个 `PythonProtocol` 执行器的当前活跃请求数
- 每个执行器的命中次数、成功次数、失败次数
- 主注册实例的配置 worker 数和当前活跃 worker 数
- `openai_oauth` 续跑实例的配置 worker 数和当前活跃 worker 数
- `openai/pending` 当前文件数
- 最近成功上传到 R2 的 auth JSON

相关环境变量：

- `REGISTER_DASHBOARD_ENABLED`
  - 是否在主注册实例中启用面板，默认 `true`
- `REGISTER_DASHBOARD_PORT_HOST`
  - 宿主机映射端口，默认 `19790`
- `REGISTER_DASHBOARD_LISTEN`
  - 容器内监听地址，默认 `0.0.0.0:9790`
- `REGISTER_DASHBOARD_RECENT_WINDOW_SECONDS`
  - 最近上传统计窗口，默认 `900`
- `EASY_PROTOCOL_CONTROL_TOKEN`
  - 读取 `EasyProtocol` internal stats 的控制面 token
  - `deploy-host.ps1` 在 blank-host 路径下会自动注入本地安全 token，避免 dashboard 因空值或 `123456` 被静默禁用
- `EASY_PROXY_BASE_URL`
  - 默认 `http://easy-proxy:29888`

当一轮任务最终完成并且 `upload_file_to_r2` 已成功上传 auth JSON 后，
对应的整轮输出目录会自动删除，避免 `docker-output` 持续膨胀。
也就是说，默认会清理：

- `worker-XX/run-...`

不会保留已经成功上传到 R2 的本地运行产物。主注册 flow 如果在上传前失败，
会把 `openai_oauth/*.json` 复制进：

- `openai/pending/`

然后删除失败轮次目录。续跑 flow 会从这个池里 claim 文件，成功后删除 claim，
失败后放回池中。

## 当前 smoke 结果

`EasyRegister` 当前仓内的 DST smoke 已跑过：

- `create_openai_account` 成功
- `create_openai_account` 已通过 `EasyProtocol -> PythonProtocol` 路径成功
- `EasyProxy` 任务级代理链路正常
- `release_proxy_chain / release_mailbox` 正常

如果后续失败，当前最常见的不是迁移代码本身，而是：

- team workspace 席位已满
- 某条 EasyProxy 线路瞬时不可用
