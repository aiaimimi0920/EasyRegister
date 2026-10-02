# 登录入口差异与浏览器拒绝诊断（2026-10-01 UTC）

后续进展：已独立确认并交付 Chromium 认证代理修复，详见 [浏览器代理修复与公开入口观测](provider-browser-proxy-rollout-2026-10-01.md)。下面保留当时的历史证据；其中要求先核对用户截图环境的建议不再作为继续工作的前置条件。代理修复已部署，但原 OAuth 注册仍未验收通过。

## 结论与新增页面证据

用户在上一轮单次 canary 失败后提供了正常的 OpenAI API Platform 邮箱输入页截图：包含 `Email address`、`Continue` 和第三方登录按钮，没有可见 challenge。这证明截图中的浏览器已显示登录入口，**不证明后续 OAuth 授权或注册已成功**。截图没有地址栏；浏览器所在机器、完整跳转路径和出网方式尚未得到独立核实。

本轮另做了一次不涉及账户的公开页面检查：在 **PC2 新建隔离容器**中，使用镜像内的原生 Chromium，以 headless 模式打开 `https://platform.openai.com/login`。没有使用项目自定义浏览器工厂，没有显式配置代理，没有导入 cookie，没有填写或提交表单，没有重新执行注册。

`12:15:54–12:16:10 UTC` 的结果为：

```text
navigation calls: 1
target/final page: https://platform.openai.com/login
navigation response status: 403
Network.responseReceived Document status: 403
document.readyState: complete
visible email input: false
form submissions: 0
```

已实际查看保存的截图，页面原文是：

```text
Sorry, you have been blocked
You are unable to access openai.com
```

这是安全服务拒绝访问页，不是已到达邮箱输入页，也不能仅凭程序通用 `challenge` 分类推断存在一个可点击完成的 CAPTCHA。

**可确认：该次容器公开入口访问在任何账号提交之前就遭到拒绝。** 因此，至少这次失败不依赖用户名提交、邮箱验证码或短信代码。不能由此排除后续流程所有兼容问题，也不能归因到某个单一因素：容器浏览器、已有会话、网络出口与用户截图中的环境尚未对齐。本次未指定代理的公开导航也不等同于上一 canary 使用 EasyProxy lease 的出网路径，不把二者称为同出口对照。

## 修复了什么

原 `_recover_platform_auth0_authorize_in_browser` 将以下情况统一记录为 `browser authorize has no verified account page`：

- navigation status 未观测到或不是 200；
- origin/path 不符合账户页面条件，包括仍停在邮箱输入页；
- 页面含安全验证或拒绝访问标记；
- 缺少可接受的认证 cookie。

这使上一轮日志无法区分“普通邮箱输入页尚需用户交互”和“确实被拒绝访问”。

本轮在 `EasyProtocol/providers/python/src/new_protocol_register/protocol_small_success.py` 新增 `_platform_authorize_browser_rejection_summary`，只在原有失败日志后增加脱敏字段：

```text
reason, status, page, challenge
```

原因包括 `email_entry_requires_interaction`、`challenge_page`、`navigation_status_unavailable`、`unexpected_http_status`、`unexpected_origin`、`unexpected_page`、`missing_auth_cookie`。不记录完整 URL、query、邮箱、state、device ID、HTML、cookie 或 token。

**这是可观测性修复，不是授权放行修复。** 原有拒绝条件未改；邮箱输入页仍不得被当作已绑定用户名的账户页面；真实 403、OAuth transaction 和身份绑定检查保留。没有增加浏览器求解、clearance cookie 导入、节点切换或注册重试。

逐字节反向替换验证证明，除新增 helper 和该失败日志外，原本的本地源码保持不变。去除日志调用后的 recovery 函数 AST 与修改前完全相同。

## 验证与部署

新增 `EasyProtocol/tests/test_platform_authorize_browser_diagnostics.py` 的 9 项测试，加上已有 `test_platform_browser_authorize.py` 的 7 项相邻测试，在候选镜像内以 `--network none` 运行：

```text
testsRun: 16
failures: 0
errors: 0
skipped: 0
```

先在旧镜像运行新增测试，真实 recovery 分支的“日志缺少原因”断言失败，其余诊断 helper 测试报告旧版本缺失 helper。这个回执是新契约的回归证明，不代表线上存在 8 次业务错误。新候选验证了：邮箱输入页仍返回失败、只导航一次、不导入 cookie；错误身份和 challenge 仍被拒绝；可接受页面的原有行为不变；诊断字段不泄露输入中的秘密。

没有重跑先前的 43/55 项完整证据集。

候选只在精确生产基线上叠加 **1 个源码文件**。初次构建时 `FROM sha256:<image ID>` 被 BuildKit 误解为镜像仓库名并请求远端元数据，返回 403；这不是业务登录错误。核实本地已有 tag 对应同一 image ID 后，改用该 tag 构建，保留原失败日志和独占恢复回执，没有重新运行旧基线测试或启动业务。

部署信息：

```text
tag: easyprotocol/provider:auth-entry-diagnostics-20261001-1200
image: sha256:ddb495a7d34880dd38415dd4e5ae8bd723c71af00101e3d2d5c84722aaa3d905
container: easy-register-protocol-python
container ID: 446164f1f16aa455acc1d9669924cad83092dfcc770fdd8e0f78a0c87a50c240
deployedAt: 2026-10-01T12:10:12.487501+00:00
release: /home/mjc/easyregister/releases/provider-auth-entry-diagnostics-20261001-1200
rollback: easy-register-protocol-python-before-auth-entry-diagnostics-20261001-1200
changed environment keys: []
```

线上源码 SHA256 与候选 manifest 一致；健康检查通过，工作池空闲。原端口、挂载、权限、资源限制和业务配置均保留，`PROTOCOL_RETRY_MAX_ATTEMPTS=1`、禁付费设置不变。旧容器停止并保留为回滚入口。

## 当前边界

截至 `12:17:51 UTC`：

- 新 provider 运行且健康；旧 provider 回滚容器停止并保留。
- 生产业务 runner、上一轮 canary 和本次匿名页面检查容器均已停止。
- 本轮新增注册步骤调用 **0**，新增短信购买 **0**，没有新的 seed 或 OAuth 产物。
- 原购买账本／receipt 仍未创建；旧 paid005 的已消费启动标记仍保留。
- 未重新查询余额，不把 11:36 UTC 的旧余额快照表述为本轮最新余额。

下一步应先核对用户截图的实际机器、地址栏域名／路径和正常浏览器出网方式，或通过官方支持处理已观测的拒绝访问。不要自动复制浏览器 cookie、切换身份／节点或重复注册来绕过安全服务。本轮没有证明原 `oauth_authorize` challenge 已解决，也没有重新请求那个已失败的 OAuth transaction。

## 证据位置

```text
C:/Users/Public/nas_home/AI/GameEditor/linshi/easyregister-auth-entry-diagnostics-20261001-1200/
  user-email-entry.png
  public-entry.png
  final-evidence.json
  local-preservation.json
  before/protocol_small_success.py
  prepare.py
  stage.remote.py
  resume_build.remote.py
  deploy.remote.py
  public_entry_probe.remote.py
  collect.py
```

`final-evidence.json` 含隔离测试、部署、匿名页面导航和最终容器状态回执。部署及页面检查均有已消费的独占回执；不要直接重跑对应脚本或重启已消费容器。

本轮未提交或推送 Git，未更新 memory，未清理旧容器、历史文件或用户改动。
