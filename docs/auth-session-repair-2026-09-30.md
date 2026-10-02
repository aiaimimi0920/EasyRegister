# Browser auth-session continuation repair

**2026-10-02 04:00 UTC 的后续交付：**已将 `account_unavailable` 分类及 step/task 禁止重试的三文件补丁交付到 PC2 待运行容器，实际依赖镜像中 9 项定向回归通过，旧容器完整保留。新业务容器为 created、从未启动，provider 与单次消费标记未变；本轮无注册或短信购买，原账户可用性与大成功仍未闭环。详见 [账户拒绝止损补丁的停止态交付](account-terminal-stopped-rollout-2026-10-02.md)。

**2026-10-01 17:06 UTC 的后续配置：**按用户要求暂时排除 `openai` 业务的完整域名 `lake.neuroloom.pp.ua`，从候选池移除并进入显式黑名单；已切换停止状态的下一次运行配置，业务未启动。已对原样本及 PC2 近期记录做域名关联核验，但没有独立域名的最终成功对照，未证明域名导致停用。详见 [域名排除与证据核验](mailbox-domain-exclusion-and-evidence-2026-10-01.md)。

**2026-10-01 16:20 UTC 的后续调查：**已对原小成功／continuation 私有产物重新脱敏复核，确认 seed 哈希与邮箱／设备交接保持一致；04:35 的直接阻断是 ChatGPT 邮箱 OTP 校验返回账户已删除或停用，不是短信失败，也不能用后续另一轮 Cloudflare challenge 解释。本地已完成 `account_unavailable` 分类及 step/task 禁止重试，10 项定向、92 项相邻测试通过；PC2 生产未部署此新修复、业务 runner 仍停止，大成功与平台停用具体原因仍未闭环。详见 [小成功续接的账户拒绝与止损修复](small-success-to-full-account-rejection-2026-10-01.md)。

**2026-10-01 10:27 UTC 的最新生产状态：**Python provider 的会话兼容增量候选已上线，43 项隔离测试及 EasyProtocol 统一入口的指定服务合成回显通过；旧 provider 保留回滚，业务 runner 仍停止，没有新增注册或短信购买。此处关闭的是版本缺口与服务链路验收，未证明上游 challenge 已解决，详见 [provider 会话兼容部署记录](provider-session-compat-rollout-2026-10-01.md)。

**2026-10-01 09:16 UTC 的生产边界快照：**邮箱域名停用规则已增量部署到 PC2 orchestrator 并完成一次未付费尝试，但注册入口被 `browser_verification_required` 阻断。Python protocol provider 当时仍是旧生产镜像，没有加载本机隔离验收过的授权恢复兼容改动；新旧业务 runner 均已停止，依赖服务保留。部署与后续精确源码调查见 [邮箱域名规则的线上验收记录](mailbox-post-registration-account-ban-policy.md)。以下“PC2 生产未修改”等表述保留其对应历史阶段的范围，不代表后续部署后的当前状态。

**本机隔离续接及账户状态调查请以文末对应的 2026-10-01 记录为准；后续生产边界见上方更新。** 用户已批准并实际完成一次
本机隔离尝试：新 seed 006 小成功，受保护续接 local-001 在 ChatGPT 邮箱 OTP 校验
收到 HTTP 403，正文明确表示账户已删除或停用。最终 OAuth/free-personal 未完成。
本轮短信购买 0 次、支出 0 USD；原购买账本/receipt 未消费，但原 paid005 的实际
output 启动标记已记录本机委托执行，不能重启该轮或删除标记。临时服务和转发已停止。
PC2 生产未修改；此前 003/004/005 及本轮 006/local-001 均不是可重新启动的入口。

**后续只读调查：**保存的后端用户创建时间为 04:25:51 UTC，Platform 换票及用户更新
均实际完成，更新时 `enabled=true`、`banned=false`。callback state、token audience/nonce、
user ID、设备及邮箱会话绑定检查一致。续接的 Platform `already_initialized` 是缓存
短路，并未重新核验账户状态；04:35 的停用拒绝原因仍需平台确认。已复现一个独立的
摘要问题：组织更新响应为 `completed_platform_onboarding=true`，摘要却保留更新前的
false。尚未修改产品源码，不能把该摘要问题说成此次账户停用的根因。

This continues [the shared-service repair](shared-services-repair-2026-09-29.md).
The historical PC2 canary passed the browser authorization and identity boundary.
The latest account sample in that phase reached email OTP validation, where the actual native
browser POST received HTTP 403 with `cf-mitigated: challenge`. No valid seed or
final free/personal OAuth artifact had been produced at that point. No SMS was purchased.

**最新续接（2026-10-01 00:46 UTC）：**原任务仍在网络门禁前。同步完整探针抓包确认
33 个请求 NAS 已见、PC2 未见，47 个已到 PC2 的请求均正常回复；10 Mbps 三次
1 MiB 对照全部失败，独立 timer 已实际恢复原 1000 Mbps/`0x62ff`。广播目的 MAC
对照也丢包，不能仅归因于单播 MAC 学习或过滤。最新真实容器完整 nodes 仍超时，
003 未创建、005 从未启动，生产付费关闭、原账本/receipt/marker 均未消费，SMS
活动订单与 sessions 均为 0。应先换一项已知良好的网线或同 LAN 端口复测，未宣称
已确定网卡硬件根因，也未放宽任何 seed/费用保护。详见
[接收路径调查的最新记录](pc2-receive-path-investigation-2026-09-30.md)。

The subsequent continuation rechecked this deployment at 11:36 UTC. Sample
`20260930-003` is still uncreated: PC2 cannot reliably read the shared proxy's
node-list response, even after subscription refresh completes. See
[the network preflight continuation](pc2-network-preflight-2026-09-30.md).

**后续最新状态（2026-09-30 19:33:58 UTC）：**已完成重启到 `.107` 及单次 `r8168`
驱动对照，两项均未恢复 PC2 大响应接收。候选已由独立 timer 自动回退至 `r8169`
并卸载；原 IP/路由、17 个运行容器及镜像已核验。没有新 seed 或最终 OAuth 产物；
003 未创建、005 从未启动、生产付费开关 false、原购买账本及开始标记仍不存在。
需要现场网线/交换机端口等独立链路对照，不能跳过网络门禁继续注册或购买。
详情及实际传输数据见 [接收路径调查](pc2-receive-path-investigation-2026-09-30.md)。

**用户确认开机后的续接（20:06 UTC）：**PC2 已在线，但醒着时的大响应故障仍在。
完整传输、实际 MSS 和大包 ICMP 对照均未构成健康通路；真实容器的 nodes/available
也仍超时。先前部分整体断联另有系统睡眠证据，现以最长一小时临时 inhibitor 保护
诊断，未改永久电源设置。003/005 与账本仍未消费，没有新 seed 或最终 OAuth 产物。
后续需现有网线/端口等独立链路对照；不能用开机或小包成功替代业务网络门禁。

## Confirmed causes and changes

The upstream account page now uses an ES256 JWT `auth-session-minimized` cookie.
Its metadata contains session, logging and client identifiers, without the email
address. The server's React Router loader contains the account identity. The
new `protocol_auth_state.py` binds those three identifiers and the requested
email/username to the browser-observed loader. It decodes metadata only; it does
not claim to verify the JWT signature or synthesize an authentication cookie.
Legacy cookie support and final OAuth/identity checks remain in place.

The first minimized-cookie candidate still failed. The real cookie-import helper
visited ten additional URLs, leaving the driver on the Sentinel page before the
loader was read. The old unit fixture mocked this helper and hid its navigation
side effect. A regression using the real helper failed with eleven navigations
instead of one. The helper now accepts `navigate=False`, used only by this
authorization recovery path. Existing callers retain the default navigation.

The next sample isolated another timing boundary: page navigation can complete
before the React Router loader is hydrated. A real browser initially had no
loader, still had none after one second, and had matching identity/session data
after another four seconds. Authorization recovery now waits at most ten seconds
and rejects document URL changes, missing data, and mismatched identity. HTTP
200, the HTTPS account origin/path, and absence of challenge markers are still
required.

Read-only browser observation 007 exercised the product wait without an injected
delay. It took 25 reads to obtain the loader and returned a verified response:

```text
browser authorize verified status=200 path=/email-verification identity_matches=yes
```

There were zero form submissions and zero SMS requests. Observation 006 proved
the delayed loader but supplied an incomplete diagnostic sentinel context, so its
final recovery returned `AttributeError`; observation 007 corrected that fixture
and proved the complete authorization recovery. Neither observation is an OTP or
OAuth success claim.

## Serial samples and the next boundary

All samples below used one task attempt, one attempt per step, and disabled SMS.
Each later sample followed a diagnosed, regression-tested code change.

- `seed-refresh-20260929-003`: rejected at authorization identity binding because
  cookie import navigated away from the verified document.
- `seed-refresh-20260930-001`: rejected at authorization identity binding before
  the loader became available.
- `seed-refresh-20260930-002`: ran from approximately 02:15:09 to 02:17:18 UTC.
  Authorization and identity binding passed; the initial email code was obtained.
  The native POST to `/api/accounts/email-otp/validate` returned HTTP 403 and an
  actual Cloudflare challenge response. Organization initialization was skipped.

The latest native failure snapshot has a normal `Check your inbox - OpenAI`
document at `/email-verification`, a real minimized session cookie, and no
challenge in the document. The API response itself contains the challenge. Do
not report this as a missing-cookie authorization failure or accept it as OTP
success.

The bounded OTP browser fallback encountered a separate network failure. Its
saved navigation has status 0, no cookies/loader, and Chromium
`ERR_INTERNET_DISCONNECTED` / `ERR_TIMED_OUT`. The sample's mailbox release also
failed with `No route to host`. Later TCP checks from the production orchestrator
to the mailbox service and both shared proxy ports succeeded. The same mailbox
was released once through the normal client after connectivity recovered:
`released=true`, `detail=deleted`, `sessionStatus=resolved`. The proxy release
had already succeeded in the sample.

The OTP fallback also retained a legacy-cookie-only identity check. A new test
proved that a valid minimized session at an observed HTTP 200 page was rejected.
The candidate now binds its browser loader with the same checks and bounded wait
used for authorization. Status 0/403 and mismatched identity still prevent OTP
submission; an application rejection is still returned unchanged. This fixes a
compatibility defect, but does not establish that the upstream API challenge is
resolved.

## Verification and deployment boundaries

Fresh tests ran in PC2's actual provider dependency image, with networking
disabled. The final local-source run passed 38 tests. The OTP candidate image
passed 35 applicable tests. Three pre-existing local error-boundary tests name
two functions absent from the deployed baseline and were excluded only from the
candidate-image run; all three passed in the local-source run:

- `test_authorize_continue_challenge_does_not_start_browser_recovery`
- `test_landing_challenge_allows_authoritative_oauth_without_faking_success`
- `test_landing_challenge_does_not_relax_authoritative_oauth_boundary`

The real-import regression and delayed-loader regression each failed on the
preceding image and passed after its targeted repair. The minimized OTP regression
failed on the loader image and passed on the OTP candidate. The tests preserve
legacy behavior, challenge rejection, cookie rotation invalidation, identity
mismatch rejection, real response status, and browser cleanup/ownership.

Only exact function replacements were copied into deployment candidates. Each
builder checked the live image and deployed source, then proved that reversing
its narrow change restored the prior function. Unrelated dirty local changes
were not copied into the images. The production Python provider was not changed.

The verified authorization/loader image is
`easyprotocol-local:auth-loader-20260930`, ID
`sha256:90984bd970f9f37aa54a4d07ac50f4c90f6b53590aab4dfdab16b0165d4655c4`.
Its predecessor remains stopped as
`easyregister-oauth-provider-before-loader-20260930`.

The regression-tested OTP candidate is `easyprotocol-local:auth-otp-20260930`, ID
`sha256:73fe4635f25195b3dce8dff658d6e51632cb8be5223f9387081b5642393e34a2`.
It now runs as `easyregister-oauth-provider-20260912-001`, with HTTP 200 health.
Its predecessor remains stopped as
`easyregister-oauth-provider-before-otp-session-20260930`. No commit, push,
production provider replacement, or release publication has occurred.

At 03:23:47 UTC, the next sample launcher
`serial-seed-20260930-003.py` was stopped by its read-only readiness gate before
creating a sample. The next subscription refresh was 03:33:07 UTC, less than
the required 600 seconds away. The launcher has not consumed a sample at this
checkpoint; run it only after the refresh completes and the same gate passes.

## Financial guard and continuation

The 02:04:19 UTC read-only financial check found balance 4.5953 USD, quote
0.045 USD for country 16/service `dr`, stock 271627, and zero active orders.
Shared SMS had zero sessions. Both original ledger files were absent and
`easyregister-paid-once-20260912-005` remained `created`, never started.
Production still had `REGISTER_SMS_ALLOW_PAID=false`.

The paid runner's route, single-attempt limits, disabled same-session resend,
country 16 and 0.05 USD cap were rechecked. Do not change its original ledger,
weaken seed validity, or restart consumed samples. If a later sample yields a
normally valid fresh seed, stage the original bytes and SHA256 under paid output
root **005**, then recheck prices, balance, active orders, seed age and the guard
before starting the existing paid runner once. `scripts/pc2_wait_seed.py` is
hard-coded to output root 001 and must not be run directly for runner 005.

## Evidence and operational notes

PC2 private evidence is under
`/home/mjc/easyregister/paid-sms-canary/20260912-001` and the matching output root.
It includes `auth-state-candidate-20260929`,
`auth-document-candidate-20260929`, `auth-loader-candidate-20260930`, and
`auth-otp-candidate-20260930`. Candidate folders contain original deployed source,
the narrowly patched source, build helpers and private deployment receipts.

The latest API snapshot is
`browser-session-diagnostics/50d997c7-1953-4cb8-9170-28f145777e09/private-failure.json`.
The failed OTP navigation is
`otp-browser-diagnostics/d901f398-6030-491a-ad4d-3720e2aa5bec/private-navigation.json`.
Their adjacent private files contain cookies, account identity and OTP material;
never print them raw. Browser observations 001 through 008 are consumed probes;
do not restart their containers.

Observation 008 replayed the previous signed account cookies without submitting
the saved OTP. Its page returned HTTP 200 for 25 seconds and retained the same
session/client identifiers. The saved HTML contains the expected identity, but
the live React Router context had no hydrated `state`, so identity binding was
rejected. This probe is not proof of successful OTP recovery. It also seeded a
new device cookie after importing the old snapshot; any later replay probe must
retain the original device cookie to avoid that diagnostic ambiguity.

Direct Windows-to-PC2 SSH timed out during the latest sample. ICMP still worked,
and the shared proxy host could immediately read PC2's SSH banner. Access via
`ssh -J mjc@192.168.15.201 mjc@192.168.15.104` restored inspection without changing
server/network configuration or copying keys. This SSH management route is
separate from application traffic through the existing explicit proxy.

## 2026-10-01 代理修复后的最新状态

PC2 能访问百度，不能将本任务描述为整机离线。EasyProxy `/api/nodes` 的大诊断
响应现在有可协商 gzip，实际 PC2 管理面读取已恢复。更重要的是，pool checkout
原来把共享 22323 误标为 dedicated，却没有 pin selected tag；现已提供 opt-in
strict username binding，保证一个租约在 EasyProxy 这一层不会跨节点 failover
或退为 DIRECT。普通共享池保留原行为，ECH connector 本身未被擅自删除。

生产网关为 `easyproxy-local:strict-lease-20261001`，image
`sha256:4c960277ddb96494eb0ac7a16a5d859625623a1dabc62f3b0cc252c676fab7c7`。
最终窄化生产 orchestrator 为 `easyregister-local:proxy-strict-narrow-20261001`，
image `sha256:fa0836171f8ba3523edb5e71c435d85c0e500e19b4c21394bc63b081485f15d4`。
生产/canary 的 acquire 补丁分别从各自实际基线生成，只规范 strict route identity，
未覆盖其余历史行为。旧容器、节点库及精确 provider pin 均保留。

canary image 为
`sha256:e093b5e4806a04a6a33d5f2cd01fea4a4cd7de0da130c6d4ce0d8f68a641d9b4`，
以旧 `bcbb...` 为基线，仅装入修复后的 client 和对应的窄 route-key 补丁。
003 launcher 将旧模板 image 校验与新执行 image 分开；原 001 launcher 未改。
paid 005 由自身完整 inspect 克隆，Env/Cmd/mount/network/restart 原样保留，仍
created/never-started，旧 prepared 容器名为
`easyregister-paid-once-20260912-005-before-strict-20261001`。无网络检查确认政策、
四项 retry=1、resend=0、maxBindings=1、0.05 USD cap 和正常 age-enforced validator。

003 于 02:17:14 UTC 启动，最终在 acquire-proxy-chain 失败，未取得新 seed。
具体错误是 curl code 28、20 秒内收到部分大页面后没有结束；对真实三个预检
地址的同节点诊断确认 auth 登录页/CSRF 完整，但 ChatGPT auth/login 大页面仍
不完整。因此未启动付费，未放宽 challenge/status/900 秒规则，未清除账本或
marker。003 已消费，不得重启。最终 OAuth/free-personal 目标仍未验收。

## 02:42 UTC registration-only 004

HTTP/1.1 对照也没有解决大 ChatGPT landing HTML 的完整响应超时，不能归因于
HTTP/2。实际六步 seed 不执行 ChatGPT login-init；provider 注册实现先进入
Platform/Auth0，并没有要求先下载 ChatGPT `/auth/login` 全 HTML。独立 ChatGPT
login-init 则确有该 GET 调用，不能据此删除它或更改完整/付费 flow。

新配置 `/canary/registration-probe-scope-20261001/seed-flow.json` 仅从这个
registration-only seed 的 `probe_urls` 移除大 landing 页面，保留完整 auth 登录页
与 CSRF 200、拒绝 challenge、单次 task/step/proxy 限制和原 timeout。
恢复这一个 URL 后配置与 003 的原 flow 结构完全相同，3 项聚焦配置验证通过。
原 flow SHA256 `521aab49e7619c6b7601150d638b34848d49c9031cf58478b4345b243d83549e`；
新 flow SHA256 `d1e13dc2c24fdced97892b747bac9f8be09b43264f48179a8e1dca901bf91284`。
原 003、原 launcher、production/full flow 及 paid005 均未修改。

004 于 02:42:14 UTC 启动，刷新门禁通过：46 个可用节点、距下次 refresh 1283 秒。
02:43:57 的只读观察已出现 `register_easy_proxy_checkout_selected`，即两个完整
预检通过并领到业务代理；注册仍在运行中，不能提前宣称获得 seed 或最终成功。

02:39:21–02:39:25 UTC 的只读财务复核：余额 4.5953 USD、活动订单=0、SMS
sessions=0、country16/service `dr` 报价 0.045 USD、库存 271320。paid005 仍
created/never-started，原购买账本及 receipt 不存在，生产付费关闭。购买前必须
重新检查，不把此快照当成永久许可。

## 004 终态、节点 EOF 与静态 strict 005

004 已于 02:53:26 UTC 退出（exit=1），不可重启。它的代理领取、邮箱领取、
代理清理、邮箱清理均为 ok；注册失败，Platform 初始化 skipped。实际错误为
`transport_error` / `protocol_small_success` / curl code 7，具体是
`CONNECT tunnel failed, response 502`，不是笼统的 PC2 断网。

004 的严格节点为 `fast-b2-2`，网关日志确认真实 auth、Platform、ChatGPT、
Sentinel 请求均走此 tag。02:49:23、02:49:40 的 EOF 先累计到 2/3，02:53:21
第三次 EOF 导致该节点 TCP 被拉黑 24 小时；02:53:22 的 auth CONNECT 因
`proxy: no healthy proxy available` 被拒绝。strict 模式未换其他节点或 DIRECT，
不能为了继续注册而清除 blacklist 或撤掉 pin。

03:03 的正常自动 checkout 选 `us-netflix-n2-1`，auth 登录页仍收到部分正文后
15 秒超时，其余三个小目标完整，已正确反馈 route_failure 并 release。没有因为
该小接口成功就启动注册或付费，也没有把诊断的 15 秒改为产品 timeout。

03:09:22–03:09:31 的独立 strict SG-X5-3 对照：auth 登录页完整 83547 bytes / 200 /
3.359 秒；CSRF 完整 80 bytes / 200；Platform login 完整 4875 bytes / 200；
Sentinel 根路径完整 9 bytes / 404。最后一项仅证明 TLS/HTTP 传输可达，不是
Sentinel API 或注册业务成功。这次手工指定 tag 的诊断不冒充自动 checkout 的
selected tag；原管理租约正常 release，未用错误节点归属提交业务反馈。

005 使用现有 `REGISTER_PROXY_MODE=static`，只在这个独立 unpaid 样本中设置
`pin-strict=sg-x5-3+nosplit` 固定代理；没有修改生产的自动选择、paid005 或整个
节点池。`REGISTER_STATIC_PROXY_SKIP_PROBE=false`，两个完整严格预检仍运行。
静态路线私有文件及环境文件权限为 0600，不打印认证 URL，不清空失败状态。
原 004 launcher、flow、输出、marker 均保留不变。

005 于 03:11:47 UTC 启动；当时有 45 个可用节点、距下一次 refresh 3114 秒。
03:12:59 观察到 `register_static_proxy_selected`，注册仍在进行。task/step=1、
SMS=false、正常 seed age validator 与费用保护均保持，未取得有效 seed 前
不得启动 paid runner。新样本的 flow SHA256 仍为 `d1e13dc2...`。

## 03:29 UTC 最终检查点：真实小成功，付费仍未启动

005 于 03:16:57 UTC 正常退出（exit=0），六步全部 ok：代理、邮箱领取，账户
创建、Platform 初始化步骤，以及代理、邮箱清理。taskAttempts=1，stepErrors
为空，实际 acquired route 为 `.201:22323` / `pin-strict=sg-x5-3+nosplit`。
这是实际注册链路成功，不再只是公开接口探针；005 已消费，不可重新启动。

03:22:17 UTC，在无网络的检查容器中使用 **paid005 实际镜像** 的未修改
`validate_openai_oauth_seed_payload` 默认参数（age enforced，900 秒）检查通过。
原始 seed 字节已暂存至 output root **005** 的 `seed-pool/fresh-seed-20261001-005.json`，
源/目标 SHA256 同为 `f43865154376e1bce851178209ef2592ae982d924586729d1c45ca5069cfe60d`。
没有更改时间戳、账户身份、密码、cookie 或组织数据。初始化返回
`status=completed`，但 `completedPlatformOnboarding=false`；这里只记录步骤
成功和标准 seed 有效，不把它宣称为完整手动 UI onboarding 或最终 Codex OAuth。

前两个离线 staging 辅助容器分别因 `/canary` 导入路径缺失和额外 UI completion
断言退出，均在写 seed 之前停止，没有执行业务入口或触碰购买账本。v3 使用
正常 validator 决定 seed 是否有效，保持原 age/身份/元数据校验；并非将 UI flag
写成 true 或改变产品 validator。离线容器保留，没有删除任何数据。

后续 paid flow 实际包含 `initialize_chatgpt_login_session`，所以不能沿用
registration-only 的 landing-page 预检缩窄。03:24:27 UTC 的同一 SG-X5-3 三目标
对照中，auth/login 完整 83032 bytes / 200，CSRF 完整 80 bytes / 200；ChatGPT
`/auth/login` 虽有 200 头和 523158 decoded bytes，却仍未完整结束。诊断使用
Session timeout=20 与独立 22 秒 deadline，close 等待使该阶段总时间为 39.021 秒；
不能将这些部分数据判为通过，产品 timeout 没有被修改。原 paid flow 和真实
ChatGPT init 均未删调用、放宽 status/challenge 或偷偷跳过预检。因此 paid005
仍未启动，最终 OAuth/free-personal 未验收。种子暂存不等于下一次付费许可，
启动时必须重新做正常 age 校验；过期就不能使用，不能改时间戳。

03:29:17 UTC 最终只读复核：生产 running / paid=false / restarts=0，网关运行
strict image `4c9602...`，paid005 created/never-started。active canaries=0、SMS
sessions=0、供应商活动订单=0，原购买账本与 receipt、paid-start marker 均不存在。
余额 4.5953 USD，country16/service `dr` 0.045 USD、库存 271745。本轮购买
0 次，支出 0 USD。证据位于 `linshi/easyproxy-pc2-business-repair-20261001/`：
`seed005-final.json`、`seed005-standard-validation.json`、`strict-sg3-paid-targets.json`、
`final-readonly-preflight.json`、`final-deployment-images.json`。

## 2026-10-01 03:57 UTC：同路线异机对照，保留真实登录调用

恢复会话 `01a0f28b-8f81-7420-a5ff-2ff757e62f1d` 后，未重新执行 003/004/005，
也未删除 ChatGPT 登录调用、提高超时、接受部分正文或放宽 challenge/status。

03:40:51–03:40:54 UTC 的只读复核确认：PC2 生产 orchestrator/provider 的镜像
与上一检查点一致，生产 paid=false、restarts=0；88 个节点、46 个可用节点完整
读取，subscription enabled/non-refreshing/no-error，gateway applied=true。
paid005 仍 created/never-started，active canaries=0，SMS sessions=0、活动订单=0，
原账本与 receipt 未消费。余额 4.5953 USD，country16/service dr 报价 0.045 USD，
库存 272010。这些是该时点的数据，不是未来购买许可。

### 传输对照

对照均使用 `.201:22323`、`pin-strict=sg-x5-3+nosplit`，只访问公开页面，不提交
注册、登录表单或短信请求；诊断租约经正常接口释放。未输出代理凭据、cookie、
账户或正文内容。

| 执行环境 | 客户端 | ChatGPT `/auth/login` 结果 |
| --- | --- | --- |
| PC2 生产容器 | curl_cffi 0.16.3，默认编码 | 20 秒超时，收到 36961 wire bytes |
| PC2 生产容器 | 同版本，强制 gzip | 20 秒超时，收到 54889 wire bytes |
| PC2 生产容器 | 同版本，identity | 20 秒超时，收到 113966 wire bytes |
| 本机 Windows | curl_cffi 0.16.3 | 完整 200、无 challenge、1009893 decoded bytes，3.953 秒 |
| 本机 Windows，同一 session 第二次 | 同上 | 完整 200、无 challenge、1009893 decoded bytes，0.438 秒 |
| 本机 Docker Linux，既有 provider 基线镜像 | curl_cffi 0.16.0 | 完整 200、无 challenge、1182683 decoded bytes，1.150 秒 |

本机两种环境的 auth 登录页也完整 200；成功响应记录了 SHA256，并包含结束 HTML。
Linux 诊断容器显式覆盖入口为 Python 公共 GET，read-only、256 MiB、1 CPU，未挂载
业务数据、未运行 provider 服务或注册入口。其版本与 Windows/PC2 的同版本对照
不同，因此只作为本机 Docker 通路证据，不冒充精确部署镜像验收。

普通系统 curl 在代理 VM 和 PC2 都收到完整 403 challenge。这个控制组没有拿到
大页面，不能据此声称代理 VM 的大响应已通过，也不能把 challenge 当业务成功。

这些证据表明 `sg-x5-3` 能正常提供大页面，剩余差异集中在 PC2 执行环境及接收
路径；它们不区分网线、交换机、NIC 或内核的具体根因，也不等于底层网络修复。
没有重复此前已失败的驱动、降速、MSS 或重启试验。

### seed 与临时执行方案边界

03:57:02 UTC，新建 network=none、read-only 的离线检查容器，使用 paid005 精确
镜像中未修改的默认 validator。原 seed 被拒绝：`openai_oauth_seed_too_old:2424`。
SHA256 仍为 `f43865154376e1bce851178209ef2592ae982d924586729d1c45ca5069cfe60d`，
实际 paid output root 的 `run.started.json` 不存在。不能使用或改写此 seed 付费。

已向用户提出仅将一次隔离验收任务临时放在本机 Docker 的方案；生产不迁移，
保留原 PC2 单次购买账本、短信服务、0.05 USD 上限及正常新鲜 seed 规则。
当前仅验证本机通路并准备精确镜像，尚未创建本机注册/付费业务容器、迁移凭据、
开设到私有短信服务的转发或执行新的注册/购买。

后续若采用该方案，须先通过受限的受认证连接复用 PC2 **原短信服务和原 guard**，
不可复制账本成为第二份购买额度；检查单样本隔离、环境/挂载差异、输出归属与
完整业务目标，随后才允许新鲜 seed 与单次续接。原 paid005 不得同时启动。

本轮证据位于 `linshi/easyregister-continuation-20261001/`：
`login_encoding_probe.json`、`compare_login_hosts.json`、`local_login_control.json`、
`local_docker_control.json`、`runtime_inventory.json`、`guard_topology.json`、
`verify_staged_seed_age.json`。无产品源码修改、commit、push 或删除数据。

### 04:01 UTC：精确镜像已准备，未切换业务

03:59:46 UTC，已将原 canary orchestrator `sha256:e093b5e4...` 和 provider
`sha256:73fe4635...` 镜像复制到本机 Docker，按完整 image ID 校验一致；原 gateway
`sha256:69e369a6...` 已存在于本机。不是重新从 dirty worktree 构建的替代镜像。
镜像归档 1374950766 bytes，SHA256 为
`8023e8ef2322970b59578d03e65d8b460a3fbcba587f2a7d95d5b96a00a7e682`，
存于本轮 linshi 目录。未创建注册/付费业务容器或拷贝业务凭据。

04:01:03 UTC，使用**原 provider 精确镜像**在本机运行入口被覆盖为公共 GET 的
只读隔离诊断容器：ChatGPT 登录页完整 200、1009505 decoded bytes / 2.952 秒；
auth 登录页完整 200、82988 bytes / 6.309 秒；CSRF 完整 200、80 bytes / 0.182 秒。
三者均无 challenge，保留默认 20 秒和完整响应要求。该 provider 的 curl_cffi 为
0.16.0，生产 orchestrator 为 0.16.3，两者不能混称为相同依赖环境。

03:59:13–03:59:25 UTC 的最终只读复核仍为 paid005 created/never-started、生产
paid=false、无 active canary、SMS sessions/活动订单均为 0；原账本/receipt 和
**实际 paid output root** 的启动 marker 均不存在。余额 4.5953 USD、报价
0.045 USD、库存 271721。没有新的注册、短信购买或业务切换。

新增证据：`exact-images-staged.json`、`local_docker_exact_control.json`、
`final_preflight.json`。下一步仍需用户选择是否采用临时本机隔离执行；镜像准备
和公开 GET 通过不等于注册/OAuth 业务完成，也没有给本机创建第二份购买账本。

04:02:44 UTC 的反向对照补齐了**相同 provider 镜像、相同 curl_cffi 0.16.0、
相同 strict 节点**的主机差异：PC2 上 ChatGPT 登录页 20.003 秒超时，auth 登录
页完整 200 / 81424 bytes / 2.405 秒，CSRF 也在 20.003 秒超时。因此不是“只有
大页面失败、所有小 API 永远正常”，而是 PC2 侧传输不稳定；同镜像在本机三目标
均完整通过，排除了仅由 provider 镜像或依赖版本不同造成上述对照差异。
此结果仍不定位具体网卡/线缆/交换机故障。证据：`pc2_exact_provider_control.json`。

## 2026-10-01 04:41 UTC：本机隔离尝试完成，账户停用阻止大成功

用户明确批准“继续推进，尝试完成一次大成功”，并再次要求继续。本轮采用先前
验证可完整读取页面的本机 Docker，不改 PC2 生产、不重启旧样本。

### 隔离环境与费用边界

- 使用原精确 orchestrator/provider/gateway 镜像，未从 dirty worktree 重新构建。
- 单独 Docker 网络 `easyregister-local-canary-20261001-001`；gateway/provider 只在
  此网络提供服务，未发布到宿主或 LAN 端口。资源限制为 provider 3 GiB/2 CPU，
  gateway 512 MiB/1 CPU，单次 runner 768 MiB/1 CPU，所有 restart=no。
- 私有配置、凭据、输出置于本轮 `local-canary-001/private`，Windows ACL 仅允许
  当前用户、SYSTEM、Administrators。没有输出 token、cookie、OTP 或账户地址。
- SMS 客户端只改为本机回环 SSH 转发，实际目标仍是 PC2 原隔离 SMS 容器
  `bc9dd3a1...` 的 `172.21.0.3:8080`，原 guard `ef35e892...` 和原购买账本不变。
  未在本机创建新的 SMS/guard 或复制购买额度。
- 初始脱离会话的启动命令被执行策略拒绝，未产生转发；随后由工具管理的可观察
  会话启动，监听仅为 `127.0.0.1:19081`，最长 3600 秒，最终已提前结束并核实无监听。
- 两次真实客户端门禁完整通过 auth 登录页、ChatGPT 大登录页和 CSRF，严格 200、
  拒绝 challenge、20 秒超时、完整正文、600 秒刷新窗口均保留。SMS 查询认证成功。
- 临时检查脚本首次因 Python 3.10 不接受纳秒 ISO 时间而停止，已修正只读 refresh
  时间解析后验证；不涉及 seed 时间戳或产品代码。准备耗时超过 5 分钟后重新刷新
  预检，期间没有创建额外注册样本或购买短信。

### 实际一次小成功和一次续接

`easyregister-seed-once-local-20261001-006` 于 04:25:19 UTC 请求启动，04:26:11 UTC
观察到 exit=0。taskAttempts=1；领取代理、领取邮箱、创建账户、Platform 初始化、
释放代理和释放邮箱六步全部 ok。它使用现有 static strict SG-X5-3 路线。

新 seed 经原 paid 镜像未修改的默认 validator 校验通过，保持 900 秒有效期规则。
原字节交接的 SHA256：

```text
880c9892e577bb543d0d8c7ef1b82038f0f6a67738580a08b74620c9c1678121
```

Platform 初始化返回 completed，但 `completedPlatformOnboarding=false`，不把它
改为 true，也不据此宣称 UI onboarding 或最终 OAuth 完成。续接前 PC2 直连 SSH
核验超时，尚未写启动标记；改用已验证的 `.201` ProxyJump 管理路径后完成费用
核验，没有重跑注册或放宽业务 HTTP 超时。

04:34:56 UTC，在原 PC2 output root **005** 的 `run.started.json` 以 O_EXCL/fsync
记录本机委托执行及 seed SHA256，随后启动
`easyregister-paid-once-local-20261001-001`。原 PC2 paid005 容器仍 created/never-started，
但该轮启动标记已消费，防止被误认为还能再启动。原 PC2 旧 seed 原样保留，未换写。
本机 runner 于 04:34:58 启动、04:35:21 结束，exit=1，taskAttempts=1。

已通过：seed 领取、严格代理预检、邮箱恢复、Platform 初始化。失败：

```text
step: initialize-chatgpt-login-session
category: flow_error
code: authorize_continue_blocked
stage: stage_otp_validate
detail: chatgpt_login_email_otp_validate
HTTP: 403
```

保存的上游错误正文明确为：

> You do not have an account because it has been deleted or deactivated. If you
> believe this was an error, please contact us through our help center at help.openai.com.

错误预览在后续 `type` 字段处截断，不能凭空补出完整 error.type/code。这里的
`authorize_continue_blocked` 是编排错误码，实际失败 detail 指向邮箱 OTP 校验，
不是之前的 curl 大页面超时，也没有证据把它解释成验证码输错。观察到的 seed、
邮箱恢复和续接输入账户身份全部一致，原 seed SHA256 不变。接口未给出删除/停用
的具体原因；不能认定某个邮箱供应商、节点或本地代码已经被确诊为原因。

Codex OAuth、free/personal 验证及上传均 skipped，没有最终 OAuth 产物。邮箱、
代理释放及 artifact finalize 均 ok。本轮立即停止，没有重启 runner、替换身份
继续试探、放宽错误判断或再次购买。后续应先确认账户在官方服务中的正常状态，
或处理平台停用问题，再考虑新的、独立授权的正常验收；不删除本轮标记重新执行。

### 财务与收尾

04:38:56 UTC 只读检查：原购买账本、receipt 均不存在，原启动标记准确指向本机
local-001；PC2 production 镜像、启动时间、paid=false 保持不变。首次合并财务
检查未完成，未将其当作成功；04:40:54 UTC 按动作取得新的完整财务证据：

- 原隔离 SMS sessions=0，供应商活动订单=0。
- 原 guard 路由返回余额 4.5953 USD，与开始前一致。
- country16/service dr 报价 0.045 USD、库存 272148。
- 本轮受保护短信购买 **0 次，支出 0 USD**，没有进入短信购买阶段。

04:41:43 UTC，两个业务 runner 已退出后，仅停止本轮精确 ID 的 provider/gateway；
其停止退出码不作为业务成功或失败证据。04:41:47 UTC 停止本轮精确 SSH 进程，
复核 19081 无监听。保留容器、隔离网络、镜像、私有输出及回滚证据，未删除数据，
未停止 PC2 原服务，未提交或推送代码。

本轮完整证据目录：

```text
linshi/easyregister-continuation-20261001/local-canary-001/
```

关键文件：`prepared.json`、`before-seed-attestation.json`、`seed-final.json`、
`before-paid-attestation.json`、`paid-final-preflight.json`、`seed-staged.json`、
`original-start-reserved.json`、`paid-final.json`、`failure-detail.json`、
`final-audit.json`、`final-finance.json`、`local-services-stopped.json`、`tunnel-stopped.json`。

## 2026-10-01：续接最后任务的只读账户状态调查

用户要求恢复会话 `01a0f588-9a45-7992-9d94-c19e02704cf7` 的最后任务。准确任务是调查
账户删除/停用拒绝的来源，而不是重跑注册、重启消费过的 runner 或再购买短信。
本轮只读取既有私有结果、保留的 Docker 日志和停止容器内的精确源码；未发出注册、
登录、换票、账户 API 或短信购买请求，未启动任何业务容器，未修改 PC2 生产。

### 小成功依据和新建账户证据

脱敏提取的真实时间线如下，均为 2026-10-01 UTC：

| 时间 | 保留证据 |
|---|---|
| 04:25:26 | 第一次、也是日志中唯一的注册 network attempt |
| 04:25:38 | `authorize_continue_email_otp`，进入 passwordless 邮箱验证分支 |
| 04:25:48 | 注册验证码被观察到；不输出验证码或 message ID |
| 04:25:51 | 创建账户 Sentinel 调用；Platform 后端 user、organization、project 的创建时间也为此时 |
| 04:25:53 | `platform_callback` seed 落盘，ID/access token 的 iat 同为此时 |
| 04:25:54 | Platform OAuth token exchange 完成 |
| 04:25:55 | Platform 后端 session 的创建时间 |
| 04:35:18 | 续接邮箱验证码被观察到 |
| 04:35:19 | 续接 OTP validate Sentinel 调用 |
| 04:35:21 | 上轮续接结束；保留的错误为 OTP validate HTTP 403、账户删除或停用正文 |

`winningUserRegisterAttempt=null`、`userRegisterAttempts=[]` 在本次 passwordless
分支是正常结果：代码不调用 password `user_register`，但仍进行 OTP 校验和
`create_account`。实际 `pageType=platform_callback`、后端新用户创建时间、OAuth
材料及后端用户更新响应共同构成证据，不支持“仅凭本地 small_success 标签误判”
或“复用了旧停用账户”的解释。HTTP 200 的判断依据是精确部署代码中各强制状态
检查和成功产物，并非本轮重新请求；未保存的完整 HTTP 包不能凭空补出。

注册阶段保存的 Platform `userUpdate.user` 为 `enabled=true`、`banned=false`、
`banned_at=null`，session 为 `enabled=true`、`deleted_at=null`。这些只证明当时
Platform 返回的状态，不能推断 04:35 时仍然可登录，更不能据此否定后续明确拒绝。

### 会话、client 和 artifact 交接

- 实际传入续接的是 `easyregister-bridge` 的已初始化 seed，其 SHA256 仍为
  `880c9892e577bb543d0d8c7ef1b82038f0f6a67738580a08b74620c9c1678121`。
  原始 `run/small_success` 和 `run/openai/pending` 是不同阶段的副本，hash 不同
  不代表已交接 seed 被修改，不能混用它们进行完整性判断。
- callback 的 state 与 seed 的非空预期 state 一致；ID token audience 是单元素
  list，包含 Platform client；nonce、email 及 email_verified 与预期一致。
  access token 的 user_id 与后端 user ID、后续 userUpdate user ID 一致。
  本轮只解码 JWT 做字段比较，没有验证签名，也没有把解码声称为独立认证证明。
- 从停止 provider 提取的 `_normalize_seed_login_context` 保留 seed device ID，
  使用恢复的 mailbox ref/session ID；恢复值与 seed 也一致。精确 semantic caller
  在 `easyprotocol_flow.py:457-463` 传递这两个覆盖参数。
- seed 没有保存 auth cookie。ChatGPT initializer 在
  `protocol_chatgpt_login.py:564-626` 新建 session，再执行自身 login/CSRF/signin
  引导；signin 返回的 auth URL 和同 session 的 NextAuth state cookie 被检查
  （`:377-497`），没有用 Platform token 冒充 ChatGPT 已登录状态。
- `BrowserLoginSession` 先请求 curl，只有 challenge 或已采用浏览器才切原生
  请求。现有错误预览不足以恢复完整 HTTP headers、cookie/session 快照和完整
  error.type/code；不能声称已取得完整浏览器交互包或排除所有运行时绑定可能。

### 发现的独立本地状态摘要问题

精确 `protocol_platform_org.py:486-521` 先获取 onboarding login 的组织状态；
`:531-559` 随后更新组织。保存的真实更新响应
`organizationUpdate.settings.completed_platform_onboarding=true`，但
`:585-597` 构建摘要时仍读取更新前 context 中的 false。因此先前的
`completedPlatformOnboarding=false` 是更新前快照，不是更新后 API 状态。
这不构成实际 UI onboarding 验收，也没有证据表明摘要旧值导致了账户停用。

此外，续接的 `initialize-platform-organization` 实际结果为 `already_initialized`：
该函数 `:401-410` 根据 seed 的 completed 字段直接返回，未重新访问 Platform。
原 validator `orchestrator-common_runtime.py:87-165` 是结构、材料和新鲜度检查；
允许已有 Platform refresh material 的 seed 进入后续 ChatGPT 登录，不是账户当前
健康状态检查，也不是 free/personal Codex OAuth 验收。

### 有界离线验证和结论

使用停止容器提取源码中的 AST 函数、伪造 fixture 和 stub 边界执行 4 项测试，全部
通过；不导入业务模块、不访问网络、不使用真实凭据，不改原 seed 时间戳或 validator：

1. 设备保持和 mailbox override 参数行为正确。
2. 复现 organization 更新为 true、摘要仍 false 的旧值问题；这证明问题存在，不代表已修复。
3. completed Platform seed 确实跳过 live revalidation。
4. OTP validate 的账户停用 HTTP 403 保持 `stage_otp_validate` /
   `chatgpt_login_email_otp_validate` / `flow_error`，只有一个 session、一次 OTP
   validate 调用并关闭 session，不触发第二次 network attempt。

本轮完成了最后任务的本地证据诊断；没有取得“大成功”，未修改产品源码或部署。
准确阻断仍为 ChatGPT 登录端对该账户的停用/删除拒绝。现有证据不能确定这是稍后
的状态变化、产品间状态差异，或具体哪一项平台判定，也不能归因于邮箱供应商、
代理节点或摘要旧值。应由账户持有人核实官方账户状态，按上游正文提供的
`help.openai.com` 联系支持；本轮未代为提交申诉。不能通过换身份、换节点、
重复注册、修改标记或放宽成功判据绕过拒绝。

最新脱敏证据位于：

```text
linshi/easyregister-continuation-20261001/local-canary-001/account-state-diagnostic/
  evidence.json
  state-and-binding.json
  offline-test-result.json
  test_deployed_contract_offline.py
```

源码快照、diff 和 synthetic fixture 均保留在同一诊断目录。包含凭据的原 artifact
以及新读取的原始 timestamped provider log 只留在原受限 private 目录，未输出。
本轮没有购买操作；此前 04:40:54 的余额 4.5953 USD、零购买/零活动订单是上轮
财务快照，本轮未联网刷新余额，不能把它当作新的实时余额。
