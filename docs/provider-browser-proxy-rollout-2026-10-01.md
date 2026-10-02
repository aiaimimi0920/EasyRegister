# Chromium 认证代理修复交付与公开入口观测（2026-10-01 UTC）

> 后续进展：13:49–13:57 UTC 的网络观测已区分上游 CONNECT 失败与真实安全验证；健康租约下两个公开入口均返回 `403 / cf-mitigated: challenge`，页面加载完成并显示 `Performing security verification`。详见 [登录入口网络诊断与安全验证边界](auth-entry-network-and-challenge-2026-10-01.md)。下文“15 秒仍为加载图标”等表述保留为对应历史阶段的证据，注册与最终 OAuth 仍未验收通过。

## 结论

已确认并修复一个真实的传输层缺陷：PC2 的 Chromium 151 无法加载旧 Manifest V2 代理扩展，而旧浏览器工厂在带认证代理分支没有设置 `--proxy-server`，因此扩展加载失败后会静默直连。**这不是原 `oauth_authorize` 403 全部原因已经查明的声明。**

本次复用 EasyBrowser 工作区中已经存在的修复，将其交付到此前遗漏的 EasyProtocol 构建副本和 PC2 provider 镜像。没有重写一套代理实现，没有调整授权成功条件，没有修改 stealth、CAPTCHA 或安全验证放行逻辑。

修复后的隔离 HTTP/HTTPS 代理测试通过，真实新租约下 curl 与 Chromium 的 trace 出口一致。但公开 `https://platform.openai.com/login` 仍观测到 HTTP 403；浏览器观察 15 秒后仍为加载图标，没有出现邮箱输入框。原注册、fresh seed、短信和最终 OAuth **尚未验收通过**。

## 缺陷证据与真实源码归属

PC2 当前浏览器版本：

```text
Chromium 151.0.7922.137
ChromeDriver 151.0.7922.137
```

旧生产镜像的 `driver_factory.py` SHA256：

```text
6c2457a0f8581cc84659f6cd0036a06ed6e1a88882d66c004023406e9b18b39f
```

在 `--network none` 容器内，用本地 HTTP 源站、带 Basic 认证的本地代理和受控 DNS 映射复现。旧实现的 Chromium 日志明确报告：

```text
Cannot install extension because it uses an unsupported manifest version.
```

源站收到请求，但代理没有收到任何请求；断言原文：

```text
HTTP reached origin without authenticated proxy
('GET', True) not found in [('origin', False), ('origin', False)]
```

该复现不访问 OpenAI，不获取邮箱，不购买短信，不使用真实代理凭据。

本次还联网读取了 Chrome 官方文档：

- [Manifest V2 support timeline](https://developer.chrome.com/docs/extensions/develop/migrate/mv2-deprecation-timeline)：Chrome 139 及后续版本不再支持 Manifest V2。
- [chrome.webRequest](https://developer.chrome.com/docs/extensions/reference/api/webRequest)：Manifest V3 的认证回调使用 `webRequestAuthProvider` 和 `asyncBlocking`。

真实源码位于相邻独立仓库：

```text
EasyBrowser/runtimes/chrome/src/browser_runtime/driver_factory.py
```

`EasyProtocol/python_browser_service/` 是 `.gitignore` 明确标记的物化构建目录，不是需要独立维护的第二份源码。正式 `EasyProtocol/scripts/compile-provider-image.ps1` 第 64–96 行从 EasyBrowser 读取并复制运行时；本次旧副本与旧线上镜像的哈希相同，而 EasyBrowser 工作区已有新实现。

实际 provider 加载链为 `protocol_runtime.protocol_register._load_protocol_browser_new_driver` → `browser_runtime.runner._new_driver` → `browser_runtime.driver_factory.new_driver`。没有把未使用的 `chrome_runtime/driver_factory.py` 当成本次部署入口，也没有顺手修改该旧兼容路径。

## 交付范围

复用的已有改动包括：

- 生成 Manifest V3 service worker 扩展，支持当前 Chromium 的代理认证回调。
- 在启动参数中显式设置不含凭据的 `--proxy-server`，扩展失效不能导致外部目标静默直连；扩展生成失败直接报错。
- 解析并解码 URL 中的认证信息；只向指定代理的认证 challenge 提供凭据，不响应源站的普通 401 认证。
- 带认证代理的匿名会话使用可清理的临时 profile，避免 incognito 禁用扩展。
- 保留同一源码内已有的进程隔离修复：不再清理所有不属于当前 profile 的 chromedriver。

本轮没有再修改 EasyBrowser 的上述生产源码，只新增 `EasyBrowser/tests/test_chromium_proxy_fail_closed.py`，复用另外两个已有测试文件，并同步一个构建副本文件。没有执行会递归替换整个构建目录的 materialize 脚本。

AST 对比确认：变化仅涉及 `create_proxy_extension`、`new_driver`、`_cleanup_stale_browser_startup_state`，新增 `_parse_authenticated_proxy`；其余 16 个顶层函数/类不变，三个 runtime stealth 函数不变。

## 定向验证

候选镜像中使用原生 Chromium、仅 loopback fixture、`--network none` 执行：

```text
testsRun: 8
failures: 0
errors: 0
skipped: 0
```

覆盖 HTTP 认证代理、HTTPS CONNECT、普通/临时 profile、扩展不可加载、错误密码、源站 401 凭据隔离和浏览器进程隔离。候选容器未发生 OOM 或 PID 限额命中。

第一次旧版本 fixture 运行出现 `tab crashed`，其结果不作为路由缺陷证明。随后改用容器内临时目录并放宽测试容器资源限制，取得了上面的明确直连复现及 Chromium manifest 报错，保留全部原始记录。没有把测试容器崩溃冒充业务失败。

没有重跑此前已通过的 55 项重试边界或 16 项授权诊断测试；候选基于精确生产镜像，只覆盖一个浏览器源码文件。provider 的实际 loader 也已在候选中导入，并校验所加载文件的 SHA256。

## 部署与回滚

```text
tag: easyprotocol/provider:browser-proxy-20261001-1310
image: sha256:0dada39b6789c647a3c8cc1187e8e9ec987ec953b98733eba89f79a75dba28b9
container: easy-register-protocol-python
container ID: a2457543a926f0b78cfa9f2c8a392fb452e7e3348b425b99bee8678187b4752b
deployedAt: 2026-10-01T13:21:47.476763+00:00
source SHA256: 135746e3f40e03935ec1a3d50698f6359916dafefcbc9133d72e752dbfdd80da
rollback: easy-register-protocol-python-before-browser-proxy-20261001-1310
release: /home/mjc/easyregister/releases/provider-browser-proxy-20261001-1310
```

部署前工作池空闲，业务 runner 已停止。旧容器停止并保留，重启策略设为 `no`；新容器保留原来的 `unless-stopped`。环境变量变化为空，端口、挂载、权限和资源限制均保留。

截至 `13:36:03 UTC` 再次核实：新 provider `running`、健康状态 `ok`、`busyWorkers=0`，线上源码哈希与 EasyBrowser 源码及物化副本一致。`PROTOCOL_RETRY_MAX_ATTEMPTS=1`、`REGISTER_SMS_ALLOW_PAID=false` 保持不变。

## 真实公开入口检查及其边界

本次最终完成两次公开页面观测，均申请新的普通 EasyProxy lease，客户端与浏览器在每轮内使用同一个 URL，不覆写节点、不复用已释放 lease、不导入 cookie、不填写表单。

| 观测 | curl | Chromium |
| --- | --- | --- |
| `https://www.cloudflare.com/cdn-cgi/trace` | 200 | 200 |
| 同轮 trace 出口 IP | 与浏览器一致 | 与 curl 一致 |
| `https://platform.openai.com/login` | 403，`cf-mitigated=challenge` | navigation response 403，无邮箱输入框 |

第一轮 `13:30:31–13:30:43 UTC` 的截图仍是加载图标，浏览器工厂采用 `eager`，因此不能把即时截图认定为最终页面。第二轮 `13:34:01–13:34:39 UTC` 补充了最多 15 秒的被动观察：16 个采样全部为 `document.readyState=interactive`、`visibleEmailInput=false`，截图仍为 OpenAI 加载图标。没有点击、脚本提交、刷新或 challenge 求解。

**浏览器截图不是此前的 “Sorry, you have been blocked” 页面，也没有匹配到已列出的 challenge 文本。** 可以确认导航响应 403，且观察窗口内没有到达邮箱页；不能声称浏览器最终完成加载、一定存在可点击 CAPTCHA，或等待更久绝不可能变化。

trace 的同出口结论只适用于对应 trace 请求，不把它扩展为所有域名始终同出口，更不把它与此前已释放的 canary lease 视作相同出口。公开 `/login` 也不等同于原先带事务状态的 `/api/accounts/authorize`；后者没有重试，注册故障未被宣告解决。

正式观测前，诊断脚本误用 provider 内的旧管理客户端，在管理就绪检查失败，未获取 lease、未发送任何公开请求。修正诊断脚本为读取实际 EasyRegister 镜像中支持现有管理认证的客户端，并传入现有管理凭据后才开始上述观测。该诊断脚本调整没有覆盖 provider 产品代码。相关第一次失败回执保留。

累计实际公开请求为 4 次 curl GET、4 次浏览器顶层导航（每轮各访问 trace 和 login 一次，不代表子资源请求总数）；成功获取 2 个新 lease，两个 release 响应均确认 `ok=true`。

新增注册调用 0、表单提交 0、短信购买 0。所有本轮观测容器已停止且 `restart=no`，没有活跃业务 canary。原购买账本与 receipt 仍不存在；没有重启或复用旧 paid runner，没有查询或宣称最新余额。

## 证据与下一段边界

```text
C:/Users/Public/nas_home/AI/GameEditor/linshi/easyregister-browser-proxy-20261001-1310/
  final-evidence.json
  source-preservation.json
  local-sync.json
  candidate.log
  fixture-diagnostic-result.log
  chromium-fixture.log
  provider-cutover.json
  platform-login.png
  platform-login-after-15s.png
  public-probe-v3-result.json
```

临时凭据、lease 私有内容和管理响应只保留在远端受限目录，没有输出到公开回执或文档。下一段必须针对仍未到达邮箱页的公开入口/真实授权事务建立新证据，不能再把失败归结为已修复的 MV2 代理问题，也不能以单纯等待、改身份、轮换出口、导入 clearance cookie 或重复付费替代诊断。

本轮未提交、推送、更新 memory 或删除旧容器及历史数据。
