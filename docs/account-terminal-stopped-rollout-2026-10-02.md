# 账户拒绝止损补丁：PC2 停止态交付

## 本轮结论

接续用户“继续推进完成我们的大成功”。已将上一轮仅在本地验证的 `account_unavailable` 分类及 step/task 禁止重试修复，交付到 PC2 的待运行 `easy-register` 容器。没有启动注册、登录或短信流程。

**这是修复交付，不是大成功。** 原小成功账户在 ChatGPT 邮箱 OTP 校验时收到明确账户删除或停用拒绝；后续公开注册入口另有真实安全验证阻断。排除一个邮箱域名不能证明任一阻断已经解除。本轮没有取得账户恢复确认、新有效 seed 或最终 free/personal OAuth 产物，也没有用人工登录已有账号替代原验收。

时间均为 UTC；`2026-10-02 04:00 UTC` 对应用户本地 `2026-10-01 21:00 America/Los_Angeles`。

## 当前状态与精确交付范围

`2026-10-02T03:59:24.726801+00:00` 完成切换，`04:00:00.911753 UTC` 独立复核：

```text
container: easy-register
container ID: 10650df48dcf70a26bab612beb7ef82d3629132e65d151328b0a853e6cbc8c14
image: sha256:ef124442136f131c02d35b98d86963b0a54167bfb0bd5871d3c685286af82d3a
tag: easyregister/orchestrator:account-terminal-20261002-0355
state: created
startedAt: 0001-01-01T00:00:00Z
restart: no
REGISTER_SMS_ALLOW_PAID: false
REGISTER_INFINITE_MAX_RUNS: 1
REGISTER_TASK_MAX_ATTEMPTS: 1
```

从实际部署容器提取基线，仅叠加以下三个已有源码文件的账户拒绝修复，没有整包复制 dirty worktree：

- `others/error_catalog.py`：严格账户拒绝归为 `account_unavailable`；保留具体 typed code 优先级。
- `others/error_runtime.py`：该错误固定归为 `blocked`，保留原 stage、detail、message。
- `others/dst_flow_runtime.py`：step 和 task 两层都禁止重试，即使 metadata 显式要求重试。

其余源码保持基线。被复用的 `mailbox_account_risk.py` 与已部署版本内容一致，未修改域名风险阈值、计数或状态。准备阶段证明：撤回限定补丁后逐字恢复部署基线；保留未改动行的原始行尾，新增文本为 UTF-8 无 BOM。

环境变量变更为零，启动命令、完整 HostConfig、数据挂载和邮箱策略均保留；`lake.neuroloom.pp.ua` 仍在 `openai` 显式黑名单且不在候选池，自动域名风险阈值仍为 `5`。没有覆盖 provider 或共享服务，没有改动其重试配置。

该终止保护作用于当前 DST step/task，不宣称它会永久禁止 supervisor 在未来新建任务。本轮仍以业务容器未启动、`restart=no` 和单次上限维持停止边界。

## 定向验证

在实际生产依赖镜像中、`--network none`、只读 rootfs、仅临时 fixture 可写的隔离容器执行：

1. 旧镜像的单项分类回归明确失败：完整正文、220 字符截断正文和直接拒绝句三个子用例均错误返回 `authorize_continue_blocked`；没有 import/dependency error。
2. 新候选 **9 项通过，0 failures/errors/skips**。覆盖严格拒绝与误报边界、typed code 保留、HTTP 200/502 失败 envelope、step/task 终止保护，以及真实 DST 执行器在 synthetic owner 下只尝试一次、跳过 OAuth、仍执行邮箱清理。
3. 从新停止容器再次提取源码核验，三个变更文件及风险模块的 SHA256 与已测试候选一致。
4. 独立检查确认新业务容器从未启动；provider 和原 paid runner 的身份、镜像、状态、启动时间不变；无活跃业务 canary。两个本轮测试容器均 exited、restart=no。

没有把本机私有账户结果上传到测试容器。此前真实错误离线回放及 92 项相邻门禁属于上一轮证据，本轮未重跑，也不计入上述 9 项。没有重新访问账户或公开登录入口。

第一次构建失败属于本轮部署脚本的基线引用错误：`FROM sha256:...` 被 BuildKit 解析为远端 `docker.io/library/sha256:...`，镜像元数据请求返回 403。`--network none` 不会禁止 BuildKit 获取基础镜像元数据。已核对原始日志后为本地精确镜像创建独立标签并校验 ID，以该标签和 `--pull=false` 重新构建成功；未修改 Docker registry 配置。这不是账户业务的 403，也没有触发新的注册重试。两次构建日志均保留。

## 费用与单次标记

- 本轮注册请求 `0`、短信购买请求 `0`；未查询或声称最新供应商余额。
- 原 `guard-state/attempt.json` 与 `attempt.json.receipt.json` 仍不存在。
- 原 `/home/mjc/easyregister/output/paid-sms-canary-20260912-005/run.started.json` 仍存在，切换前后 SHA256 均为 `130e3c680c090bc33fe5e073892e715b459e17338824d4dc80e2be56b7289623`。
- 原 paid005 虽仍为 created，但委托启动标记已消费，不能重启或删除标记重用；本轮没有新建购买额度。

最初快速检查使用了错误的 `output-005` 推测路径；正式采集已依据实际 launcher/manifest 布局纠正到上面的真实 output 路径，后续所有前后比较均使用真实路径。没有用第一次的不存在结果作为启动许可。

## 回滚与证据

旧容器完整保留：

```text
easy-register-before-account-terminal-20261002-0355
ID: 16db2324eb15a288423a76f07ac6c21a6bb5650e4118af29ffb41fada96cdebd
image: sha256:3adf6292ac8d2a0e604dbd9c6835f6a0c214ebce4e5e1b483426265a218ce38f
state: created, restart=no
```

如需回滚，应先核验双方仍未运行，保留新容器并重命名，再将旧容器恢复为 `easy-register`；不自动启动业务、不删除容器/镜像/输出，不更改单次消费标记。远端完整创建配置只保留在权限受限的 release 目录，没有下载到普通证据。

```text
本地证据：C:/Users/Public/nas_home/AI/GameEditor/linshi/easyregister-account-terminal-20261002/
远端 release：/home/mjc/easyregister/releases/account-terminal-20261002-0355/
```

关键文件：`before.json`、`overlay-plan.json`、`baseline-proof.log`、`candidate-proof.log`、`build-report.json`、`cutover-receipt.json`、`final-verification.json`。`verify_final.py` 为只读复核入口；构建/切换脚本不是再次启动业务的入口。

继续原目标仍需账户可用性和站点正常验证条件；本轮已询问原账户是否获官方恢复或允许继续测试确认，未收到确认，不重复注册、更换身份或付费试探来替代该证据。不能复用过期 seed 或已消费的 canary。未提交、推送 Git，未更新 memory。

相关记录：[原小成功续接拒绝](small-success-to-full-account-rejection-2026-10-01.md)、[域名排除及因果证据边界](mailbox-domain-exclusion-and-evidence-2026-10-01.md)、[公开入口安全验证](auth-entry-network-and-challenge-2026-10-01.md)。
