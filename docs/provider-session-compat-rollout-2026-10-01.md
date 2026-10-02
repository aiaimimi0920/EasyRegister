# Python provider 会话兼容增量部署（2026-10-01 UTC）

> 后续状态：已部署 provider 重试上限，并于 11:25–11:32 UTC 完成一个新的单次禁付费 canary；注册仍返回真实 `browser_verification_required / 403 / cf_mitigated=challenge`，失败后停止且无短信购买。详见 [重试上限与单次验收记录](provider-attempt-boundary-canary-2026-10-01.md)。下文保留 10:21 UTC 会话兼容切换的历史证据。

## 当前结果与验收边界

用户继续授权推进后，已补齐 09:16 UTC 调查确认的生产 provider 版本缺口。于 `2026-10-01T10:21:47.694536+00:00` 完成切换，随后实际通过现有 EasyProtocol 统一入口验证指定 provider 的合成回显。业务 runner 始终保持停止，没有新增注册、登录、OTP 或短信购买请求。

这关闭的是会话兼容代码的部署与服务链路验收，不是上游注册 challenge 或最终 OAuth 业务验收。不能把本次 HTTP 200 合成回显当成注册成功。

## 镜像、代码范围与回滚

```text
production base: sha256:4297f6a2fe88c80ce3f5f4ef984bad0550a255c087d53b65699529f44e84b0ed
verified reference: sha256:73fe4635f25195b3dce8dff658d6e51632cb8be5223f9387081b5642393e34a2
new image: sha256:d166a92be39d73b282a167e80f6605b920e1d3d0fe4fbac28caee0568afa8f65
new tag: easyprotocol/provider:compat-20261001-1005
release: /home/mjc/easyregister/releases/provider-compat-20261001-1005
new container: 6f97f305f04970a3c4dd34938e6e8b4c1014a69198d42f503a010ca3af1f7f5b
rollback container: easy-register-protocol-python-before-compat-20261001-1005
rollback ID: a02766f671b0840cc6e5b040041ee91e57d3578d559082a136459ec6ab120eba
```

以精确生产镜像为基础，只叠加从已验证 reference 容器提取的 5 个源码文件，未将本地 dirty worktree 或整个 reference 镜像覆盖生产。AST 比对确认原文件的其他顶层定义和模块状态保持不变：

- 新增 `protocol_auth_state.py`、`protocol_browser_session.py`，提供浏览器实测会话绑定与同浏览器 transport 保留。
- `protocol_small_success.py`：同步 5 个新增函数和 5 个已有函数的必要会话／授权兼容改动。
- `protocol_chatgpt_login.py`：只同步 `run_protocol_chatgpt_login_init_from_path` 的可选 transport 接入。
- `protocol_runtime/protocol_register.py`：只同步 `_import_browser_driver_cookies_into_session` 的 `navigate=False` 支持，并加上两行边缘 cookie 排除保护。

新增保护只作用于已验证页面的 `navigate=False` 导入路径：排除 `cf_clearance`、`_cf*`、`__cf*`，避免把边缘验证 cookie 搬到另一个客户端。默认旧调用路径的导航和去重行为不变。保留 HTTP 状态、官方 origin/path、实际页面、同一 OAuth transaction、邮箱身份和会话绑定检查；缺失、错绑、未观测状态或 challenge 仍不得当作成功。

本地只新增对应的 3 项 synthetic 回归测试并修改该 helper 的两行，逐字节证明其余已有本地修改未被覆盖；local helper 的 AST 与实际测试候选中的 helper 一致。

## 隔离验证

先在 reference 镜像的 `--network none` 容器中运行 3 项新增回归：2 项通过，边缘 cookie 导入边界测试失败，实际为 `AssertionError: 2 != 5`。这是 synthetic fixture 的旧行为复现，不是真实账户响应。

生产基线的增量候选随后在 `--network none` 下通过 **43 项**测试，`failures=0`、`errors=0`、`skipped=0`。覆盖已有浏览器 transport、minimized session、真实 cookie-import helper、loader 延迟、授权与 OTP 拒绝边界，以及新增 cookie 保护。所有外部接口和浏览器交互使用 synthetic/mock；没有真实上游请求。

这不是本轮重新运行邮箱域名规则的 28 项测试或全部历史相邻门禁。

## 线上切换和运行配置

切换前核实业务 runner 已停止，没有运行中的 seed／paid canary，并连续确认旧 provider 的真实健康接口为 `ok`、`pool.busyWorkers=0`。保留原 Env、端口、数据挂载、资源限制、权限和网络别名，只更换镜像并增加以下两项配置：

```dotenv
PROTOCOL_ENABLE_BROWSER_AUTHORIZE_FALLBACK=1
PROTOCOL_ENABLE_BROWSER_SIGNUP_SESSION=1
```

`REGISTER_SMS_ALLOW_PAID=false` 保持不变；`PROTOCOL_ENABLE_BROWSER_OTP_FALLBACK` 仍为有效默认 `0`。授权恢复只接受真实浏览器观测且通过完整绑定检查的响应，不提供自动人机验证求解，也不放宽账户停用或邮箱域名拒绝。

新 provider 为 `running`，沿用原 `unless-stopped` 重启策略。旧 provider 停止并保留为回滚容器，`restart=no`；原新旧业务 runner 仍为 `exited`、`restart=no`。切换脚本在任何创建／验收失败时会停用新 provider、保留失败容器并恢复原容器，不删除数据或容器。

## 真实服务链路验收

- 新 provider `/health` 为 `ok`，`pool.busyWorkers=0`。
- 5 个线上源码文件 SHA256 与候选 manifest 一致。
- provider 内部 `/invoke` 的 synthetic `protocol.echo` 成功。
- `2026-10-01T10:27:33.318404+00:00`，通过现有 `easy-protocol` 的 `/api/public/request` 执行 `mode=specified`、`requested_service=PythonProtocol-001` 的 synthetic `protocol.echo`：HTTP `200`、`status=succeeded`、回显内容一致、`attemptCount=1`、`retried=false`。
- `2026-10-01T10:32:47.370412+00:00` 最终只读复核：新 provider 仍运行，旧 provider 与新旧业务 runner 均停止；原业务 `runStartedCount=1`，原购买账本／receipt 仍不存在，风险 sidecar 仍未创建。

最初的路由验收脚本将服务名写作 `PythonProtocol`，在发出 POST 前被断言拒绝。只读能力清单核实线上注册名实际为 `PythonProtocol-001` 后修正脚本，再完成上述唯一一次合成执行；未改路由配置或打开 fallback。

本轮新增注册请求 `0`、短信购买 `0`。未查询实时余额，不将上一轮财务快照冒充本轮观测。

## 管理通道异常与执行去重

初次大体积暂存调用的 Windows SSH 回执超时，RTK wrapper 退出后留下该调用的 SSH／ProxyJump 子进程。只针对精确进程、创建时间和目标主机核实后终止本轮孤立上传进程，未停止其他 SSH 或业务进程。

关闭孤立管理会话后，复核发现远端完整暂存脚本已经执行，目录、构建回执及两个隔离测试容器均已存在，43 项证明已完成；不能仅凭终端回执确定具体执行时点。第二次暂存被 `release already staged; inspect before retry` 拦住；没有重复构建、重跑测试或启动注册。后续以实际远端回执和容器状态为准，不把管理终端超时当成远端未执行。

## 证据与下一步

脱敏证据位于：

```text
C:/Users/Public/nas_home/AI/GameEditor/linshi/easyregister-domain-ban-rule-20261001/provider-compatibility/
```

关键文件：`comparison.json`、`overlay-manifest.json`、`remote-progress-after-upload-stop.json`、`provider-cutover.json`、`provider-router-proof.json`、`final-state.json`、`local-patch-verification.json`。私有部署配置只留在远端受限文件，不复制或输出其中的凭据。

后续如推进真实业务，应使用新的有界运行入口，重新核实任务／步骤及 provider 内部网络尝试上限、当前服务状态和费用保护，不重启已消费的旧 canary。若上游正常浏览器仍要求验证，就停止并按正常交互处理；本轮没有证明 challenge 已消失，也没有新的 fresh seed 或最终 OAuth 产物。未提交、推送 Git，未更新 memory。
