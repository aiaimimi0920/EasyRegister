# Provider 重试上限与单次禁付费验收（2026-10-01 UTC）

> 后续补充：用户已提供普通浏览器邮箱输入页截图；12:10 UTC 上线了不改变授权判定的拒绝原因日志补丁。另一次 PC2 隔离 Chromium 匿名打开公开登录页的检查，在未填写账号时即得到 403 和 `Sorry, you have been blocked`。详见 [登录入口差异与浏览器拒绝诊断](provider-auth-entry-diagnostics-2026-10-01.md)。下文保留 11:25 UTC canary 的历史证据。

## 当前结论

从会话 `01a0f630-9db1-7e93-baa7-eab1e162a604` 的实际停点续接。此前已经部署 provider 重试上限，本轮重新核对线上镜像、源码哈希和部署回执后，完成了一个新的独立禁付费业务样本，而非重启旧 runner。

**内部重试保护已部署并得到本次调用证据支持，但注册阻断尚未解决。** 本次仍在 `create-openai-account` 返回：

```text
code: browser_verification_required
stage: stage_auth_continue
category: blocked
detail: oauth_authorize
message: browser_verification_required status=403 cf_mitigated=challenge
```

没有新成功账户、可用于付费续接的 fresh seed 或最终 OAuth 产物。不能把健康检查、离线测试或合成 echo 成功当作业务成功。普通浏览器是否能正常进入官方登录页仍缺少人工页面证据。

## 已部署的重试保护

```text
image: sha256:93a1c94787bedfd201117439c0c2a88c8c3dff609e6cb67088bd3f4be310fa2b
tag: easyprotocol/provider:attempt-boundary-20261001-1050
container: easy-register-protocol-python
container ID: aa46d115cd35e900248e1116c325445a146c51ec63bf34fbbc930c30b52fa97a
deployedAt: 2026-10-01T11:03:12.943968+00:00
release: /home/mjc/easyregister/releases/provider-attempt-boundary-20261001-1050
rollback: easy-register-protocol-python-before-attempt-boundary-20261001-1050
```

新增 `protocol_runtime/attempt_limits.py` 的 `bounded_attempts(default)`，以 `PROTOCOL_RETRY_MAX_ATTEMPTS` 收紧已有重试次数。未配置时保留各调用点原来的次数；配置值不能扩大默认上限；显式非法值拒绝执行。线上有效值为 `1`。

覆盖注册网络会话、登录初始化请求、注册候选提交、ChatGPT 登录请求／恢复以及 transport fallback。另将 `browser_verification_required` 明确排除出 ChatGPT 新会话重试条件。补丁基于前一版精确生产镜像增量构建，不把本地其他 dirty 修改带入部署。

这里的上限不是“整个注册流程只允许一个 HTTP 请求”：必要的多步骤请求，以及已有的同一 OAuth transaction 浏览器观测仍然存在。不能据此声称所有网络请求合计为 1。它约束的是已覆盖调用点的重复尝试与候选数量；本次业务 task、step、provider 网络会话均实际只进入一次。

上一会话的真实 `--network none` 回归回执显示，旧实现存在预期失败，新镜像 **55 项通过，0 failures/errors/skipped**。本轮读取并复核该回执，没有重复运行这 55 项，也没有将其表述为本轮新跑。`11:37:36 UTC` 再次核实 4 个补丁文件 SHA256 全部与 manifest 一致，provider 健康且 `busyWorkers=0`。

## 新 canary 的隔离与费用限制

```text
container: easyregister-compat-once-20261001-1120
container ID: bee38351642ce1b94690443580ebfbb8c5a68f531b3869e488b22b65e1c0b0a5
image: sha256:3adf6292ac8d2a0e604dbd9c6835f6a0c214ebce4e5e1b483426265a218ce38f
release: /home/mjc/easyregister/releases/compat-once-20261001-1120
output: /home/mjc/easyregister/output/compat-once-20261001-1120
flow SHA256: 7c4f645724758eb1903f441bcd6dec76f8f08a75433ce05168fb0501b32875ce
```

- 使用生产 orchestrator 的精确镜像和共享服务配置，但创建新容器、新输出目录和独占启动回执，`restart=no`，不开放新的宿主端口。
- 仅保留代理获取、邮箱获取、注册、Platform organization 初始化、ChatGPT 登录初始化、代理释放、邮箱释放 7 步。后两步 `alwaysRun=true`；短信、团队邀请、最终 OAuth 和上传不在此 flow 内。
- task、step、代理获取上限均为 `1`；直接调用 `run_dst_flow_once`，不运行循环 supervisor。
- 实际解析后的 SMS policy 为 `enabled=false`、`allowPaid=false`、`allowReuse=false`。provider 本身也保持 `REGISTER_SMS_ALLOW_PAID=false`。
- `EASY_PROTOCOL_REQUEST_MODE=specified`、`EASY_PROTOCOL_REQUESTED_SERVICE=PythonProtocol-001`。路由源码在 specified 模式强制关闭 fallback 并设上限为 1。
- 先在相同 orchestrator 镜像、`--network none` 容器中运行新增入口的配置断言，退出码 `0`；未调用外部服务。这是配置门禁，不是重新运行整套单元测试。
- 实际启动后再次解析政策并创建独占 `run.started.json`，检查订阅非刷新状态、有 43 个可用节点、距下次刷新 2273 秒，并经统一入口完成一次指定 provider 的 synthetic echo：HTTP 200、`attemptCount=1`、`retried=false`。

所有私有配置、完整日志、邮箱信息和凭据仅保留在 PC2 受限文件。本文和下载到本地的结果只包含脱敏证据。

## 业务实测结果

`2026-10-01T11:25:57Z` 启动，`11:32:44Z` 自动退出，退出码 `2`。结果如下：

| 步骤 | 结果 | 实际次数 |
|---|---|---:|
| acquire-proxy-chain | ok | 1 |
| acquire-mailbox | ok | 1 |
| create-openai-account | failed | 1 |
| initialize-platform-organization | skipped | 0 |
| initialize-chatgpt-login-session | skipped | 0 |
| release-proxy-chain | ok | 1 |
| release-mailbox | ok | 1 |

仅记录一个注册 provider 请求：`register-c9b06fc2-7668-4b9d-bc82-1476829b93fc`，请求构建时明确为 `specified / PythonProtocol-001`。对应线上 provider 时间窗日志为：

```text
[protocol-small-success] network attempt index=1 email=[email] proxy=[url]
[protocol-small-success] public landing page blocked; continuing to authoritative OAuth validation
[protocol-small-success] platform Auth0 authorize response needs browser retry status=403 challenge=yes
[protocol-small-success] browser authorize has no verified account page
```

这证明新代码实际执行，协议授权响应仍为 challenge，浏览器恢复没有获得通过校验的账户页面；该时间窗没有记录浏览器启动异常。日志没有给出浏览器失败校验的具体子条件，因此不能推断 PC2 普通浏览器也一定返回 403，也不能断言邮箱、节点或账户状态就是原因。

附加的内部 route trace 只读查询返回 HTTP 401，未取得该管理接口的 trace；未修改认证配置或绕过鉴权。实际调用次数证据来自独占启动回执、DST result、单个请求记录和 provider 日志；echo 的 route metadata 仅证明合成调用，不冒充注册调用 trace。

## 财务、容器与后续边界

`11:36:34 UTC` 财务只读核验：HeroSMS `getBalance` 为 **4.5953 USD**，与 `11:05:21 UTC` 前置快照一致；活动订单 `0`，短信会话 `0`。原购买账本 `attempt.json` 和 `attempt.json.receipt.json` 均未创建；旧 paid005 的 `run.started.json` 仍存在，未删除或复用。

`11:37:36 UTC` 最终状态：本次 canary 为 `exited / restart=no`；原生产业务 runner 仍 `exited / restart=no`；新 provider 继续运行且空闲。没有新增短信购买，没有重新启动旧 canary。所有旧容器、数据和回滚条件均保留。

下一步先在 **PC2 的普通浏览器**打开 `https://platform.openai.com/login`，只确认是否进入邮箱输入页、要求人机验证或被拒绝访问。不要求提供邮箱、密码、OTP 或 cookie。需要验证时走官方正常交互；在得到新的有效证据前，不因同一个 challenge 更换身份／节点反复注册，不把 challenge 改判为成功。

本次入口的 `launch.*.json`、`run.started.json` 已消费，**不能直接重跑 `launch.remote.py`，不能重启该 canary，不能删除启动标记取得新额度**。如后续有确定修复并需新样本，应重新建立独立作用域和同样的费用边界。

## 本地证据

```text
C:/Users/Public/nas_home/AI/GameEditor/linshi/easyregister-compat-once-20261001-1120/
  launch.remote.py
  inspect.remote.py
  final-inspection.json
  finance-before.json
  finance-after.json
```

前一阶段补丁、manifest 和本地修改保留证明在 `linshi/easyregister-attempt-boundary-20261001-1050/`。本轮未修改产品源码、未提交或推送 Git、未更新 memory；只新增有界验证工具、证据和本文，并为旧部署记录补充续接链接。
