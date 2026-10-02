# 登录入口网络诊断与安全验证边界（2026-10-01 UTC）

> 后续实现：已新增 [本机交互式 Codex 登录入口](interactive-codex-login.md)，通过隔离的官方 CLI 登录生成本地待验证产物。合成端到端验证已通过；真实用户登录、在线验证和生产入池尚未完成，不代表下文注册阻断已解除。

## 当前结论

本记录接续 [Chromium 认证代理修复交付](provider-browser-proxy-rollout-2026-10-01.md)。此前“15 秒后仍为加载图标、最终状态未确定”的结论保留为历史观测；本轮已取得加载完成的安全验证页、浏览器网络事件和 Gateway 同时间窗日志。

**代理实现缺陷已修复并部署，但注册尚未成功。** 本轮明确区分出两个阻断：

1. 第一份新租约的上游节点连接超时，Chromium 返回 `net::ERR_TUNNEL_CONNECTION_FAILED`，没有取得 OpenAI HTTP 响应。这不是 HTTP 403。
2. 该失效节点被现有健康池排除后，普通新租约下 trace 请求返回 200，但两个公开登录入口均返回 `403 / cf-mitigated: challenge`；页面加载完成，显示 `Performing security verification`。这是生产安全验证，不是把代理连接错误误分类为 challenge。

这些证据足以停止重复注册试错，但不足以推断站点具体使用了哪一个风控信号，也不证明普通交互式浏览器一定能通过。没有新增注册、账号表单提交或短信购买；没有尝试求解、绕过或修改 challenge。

## 第一段：传输失败的直接证据

时间窗：`2026-10-01T13:49:31Z` 至 `13:50:47Z`。隔离容器为 `easyregister-entry-network-proof-20261001-1340`，普通租约选中 `us-netflix-n2-4`。

对以下两个入口的浏览器导航都收到 `net::ERR_TUNNEL_CONNECTION_FAILED`，`navigationStatus=0`：

- `https://platform.openai.com/login`
- `https://auth.openai.com/log-in-or-create-account`

Gateway 同时间窗日志包含：

```text
dispatch CONNECT platform.openai.com:443 [PROXY] dial failed:
proxy: (dial tcp 134.195.101.86:2377: i/o timeout |
        dial tcp 134.195.101.89:2377: i/o timeout)

dispatch CONNECT auth.openai.com:443 [PROXY] dial failed:
proxy: no healthy proxy available
```

旧节点列表中的 `available=true` 不能单独作为实时连通证明。随后读取 `/api/nodes?only_available=1&prefer_available=1`，确认失效节点已不在可用列表中。本轮没有修改 EasyProxy 源码、健康策略、节点黑名单或订阅，也没有手动指定替代节点。

## 第二段：连通正常时的生产 challenge

只追加一次有界观测：先断言失效节点不在可用列表，再由既有租约分配逻辑选择 `us-dedicated-p1-3`。这不是为了消除 403 而循环换节点。

时间窗：`2026-10-01T13:56:18Z` 至 `13:57:21Z`。容器为 `easyregister-entry-network-after-transport-20261001-1340`，浏览器版本为 `151.0.7922.137`。按下表顺序访问：

| 目标 | 浏览器导航状态 | 最终页面证据 |
| --- | --- | --- |
| `https://www.cloudflare.com/cdn-cgi/trace` | 200 | 普通 HTTPS 请求完成 |
| `https://platform.openai.com/login` | 403 | `cf-mitigated: challenge`、`Just a moment...`、`readyState=complete`、无邮箱输入框 |
| `https://auth.openai.com/log-in-or-create-account` | 403 | `cf-mitigated: challenge`、`Just a moment...`、`readyState=complete`、无邮箱输入框 |

两份入口记录的 `Network.responseReceived` 和 `Network.responseReceivedExtraInfo` 都记录到 403。主要 challenge 脚本与 Turnstile `api.js` 返回 200；不能再仅用“登录 JS/CSS 未下载”解释当前页面。Platform 另有一个附加 challenge 资源 404，未据此认定它是唯一根因，更未逆向或修改验证协议。

Auth 截图可见原文：

```text
auth.openai.com
Performing security verification
This website uses a security service to protect against malicious bots.
This page is displayed while the website verifies you are not a bot.
```

页面仍有转动图标，但这是安全验证页，不是此前未加载完成的 OpenAI 应用页面。`readyState=complete` 只证明文档加载完成，不代表安全验证或登录成功。

可供站点方关联的 Ray ID 保留其事件来源，不将不同阶段合并为一个响应：

| 入口 | `responseHeaders.cf-ray` | `wireHeaders.cf-ray` |
| --- | --- | --- |
| Platform | `a43bf882cf22f31f-SJC` | `a43bf87d7bfbf31f-SJC` |
| Auth | `a43bf9379fc21be5-SJC` | `a43bf9354a811be5-SJC` |

同一租约不自动证明跨域始终同出口。本轮也不是原 `11:25 UTC` canary 的出口：原 canary 选中 `balancer-b1-4`；此前 `13:30/13:34 UTC` 公开观测选中 `us-netflix-n2-4`。不得混用这些轮次的结论。

## 授权契约与现有交互能力核对

`EasyProtocol/providers/python/src/new_protocol_register/protocol_small_success.py` 中，`_open_platform_login_with_browser_retry` 只是可选 landing page 预热。主流程随后构造新的 state、nonce、PKCE、device 和 login hint，并调用真实授权事务。因此，换一个能打开的公开 URL 不能修复或替代授权成功验证。

同文件 `_recover_platform_auth0_authorize_in_browser`（953–1016 行）要求保留原授权事务，检查受信任 origin、实际 200、正确账户页面和认证 cookie，并拒绝 challenge。本轮没有放宽这些条件，也没有将匿名邮箱页面或仅有 cookie 的状态判为成功。

已亲自核对以下可复用基础能力及其不足：

- EasyBrowser `service/base/internal/httpapi/router.go:27–31` 提供会话 `acquire/renew/release/steps/flows`；`internal/browser/browser.go:154–179` 将 runtime/resource、过期时间与 `attach` 存入会话。这是浏览器会话管理，不等于已接入 EasyRegister 的人工验证/恢复闭环。
- EasyRegister `server/services/orchestration_service/src/others/artifact_pool_claims.py:458–494` 的 `preserved_for_manual_oauth` 是已有产物的保存分支，不会打开人工操作界面。`common_runtime.py:11–13,69–84` 中它默认关闭，默认适用错误也不是 `browser_verification_required`。不能打开该开关就宣称当前注册入口问题已解决。
- `others/error_catalog.py:35–50` 已将 `browser_verification_required` 归为 `blocked`，将 `proxy_connect_failed` 归为 `proxy_error`；前者不在默认重试 profiles 中。当前没有证据要求修改分类或重新加入重试。

本轮核对的上述调用链中，没有发现现成的“人工完成站点安全验证，然后恢复这次注册事务”的产品闭环。没有搭建 VNC、开放调试端口、复制用户浏览器 cookie 或把远程自动化会话包装成普通浏览器。

## 解决路径与停止条件

本轮已联网读取 Cloudflare 官方说明，两篇均返回 HTTP 200，正文快照保存在证据目录：

- [Supported browsers](https://developers.cloudflare.com/cloudflare-challenges/reference/supported-browsers/)
- [Challenge solve issues](https://developers.cloudflare.com/cloudflare-challenges/troubleshooting/challenge-solve-issues/)

官方原文：`Automated browsers are not supported for solving production challenges.` 同页明确列出 Selenium、Puppeteer、Playwright 和 Cypress 不受支持。

因此，不把增加等待、伪装指纹、反复轮换出口、自动求解 challenge 或导入 clearance cookie 当作本项目的修复方案。继续推进应走以下边界清晰的方向：

1. **账号建立与授权走站点正常交互流程。** 由用户在受支持浏览器中完成必要的身份和安全验证；程序只消费站点正式返回、经过校验的授权结果。不承诺人工完成一次后未来永不再验证。
2. **已有有效账号优先使用其支持的正式授权/API 接入。** 这是一条接入方向，不代表 EasyRegister 已实现，也不等于自动注册恢复成功。不能把网页登录 cookie 抽取当成正式授权结果。
3. **如继续建设交互式授权功能，复用现有会话管理，而不是另造自动过验证器。** 必须补足授权事务绑定、受保护的交互入口、明确的等待/取消/过期状态、验证后身份核对及一次性继续。只有在真实成功结果到达并校验通过后才允许下游运行；这些尚未开发或验收。
4. **正常交互仍无法通过时，交由站点方检查。** 已准备不含秘密的时间窗、目标、浏览器版本、状态码与 Ray ID；未代用户向外部支持提交。

当前停止公开注册重试和短信流程。后续不能直接重启旧 canary，不能复用已释放租约、已消费 launcher/receipt 或旧 paid runner，也不能伪造 fresh seed 时间。只有获得新的有效授权结果或有证据的实现修复后，再按单次、禁付费优先的边界继续业务验收。

## 运行状态与费用边界

`2026-10-01T14:04:16Z` 只读收尾快照：

```text
provider: easy-register-protocol-python
container ID: a2457543a926f0b78cfa9f2c8a392fb452e7e3348b425b99bee8678187b4752b
image: sha256:0dada39b6789c647a3c8cc1187e8e9ec987ec953b98733eba89f79a75dba28b9
health=ok
busyWorkers=0
PROTOCOL_RETRY_MAX_ATTEMPTS=1
REGISTER_SMS_ALLOW_PAID=false
```

两个本轮诊断容器均 `exited / restart=no`；业务 runner 和旧 canary 均停止；旧 paid runner 为 `created / restart=no`，未启动。没有活跃业务 canary。

`14:12:53 UTC` 再次只读核对：provider 容器 ID/镜像未变，仍为 `running / health=ok / busyWorkers=0`，单次上限与禁付费配置保持不变，上述诊断和业务任务仍未运行。回执为 `final-state-check.json`。

本轮成功获取 2 个新 lease，两次 release 均确认 `ok=true`。显式浏览器顶层导航共 5 次；注册调用 0、账号表单提交 0、短信购买 0。页面自身仍会发送 challenge 子请求及 POST，不能把“未提交账号表单”误写成所有 HTTP POST 为零。

没有再次修改产品源码、部署镜像、重跑已通过且源码未变的回归集，未提交或推送 Git，未更新 memory，未删除旧容器和历史数据。

## 证据位置

```text
C:/Users/Public/nas_home/AI/GameEditor/linshi/easyregister-entry-network-20261001-1340/
  final-evidence.json
  final-state-check.json
  network-before.json
  network-after.json
  gateway-history.json
  gateway-tunnel-failures.json
  platform-challenge.png
  authEntry-challenge.png
  cloudflare-supported-browsers.html
  cloudflare-challenge-troubleshooting.html
  support-report.txt
  verify_evidence.py
  verification-result.json
```

远端原始文件保留在 `/home/mjc/easyregister/releases/provider-entry-network-20261001-1340` 和 `/home/mjc/easyregister/releases/provider-entry-network-after-transport-20261001-1340`。私有 lease、环境、运行日志、DOM 和原始文档响应仍在受限目录；本地仅保留脱敏网络记录与无账号截图，不复制或公开 cookie、OTP、管理凭据及带认证 query 的完整 URL。
