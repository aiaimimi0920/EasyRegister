# PC2 续接核验：节点接口响应传输超时

续接会话：`01a0f065-184a-7b63-88f7-0c0a37df8403`。
最后完整核验区间：**2026-09-30 11:35:29–11:36:02 UTC**。

**状态：未完成。未获得新的有效 seed，也未完成最终 OAuth / free/personal
验收。本轮没有创建注册样本、没有购买短信，短信支出为 0 USD。**

**最新续接（2026-10-01 00:46 UTC）：**同步完整抓包确认本轮 33 个请求在 NAS
已见而 PC2 未见，47 个已到 PC2 的请求均正常回复并返回 NAS；10 Mbps 首次对照
三次 1 MiB 均失败，独立 timer 已实际恢复原 `0x62ff`、1000 Mbps。广播目的 MAC
对照仍丢包，不能用单播 MAC 学习/过滤作为唯一解释。最新真实容器 nodes 仍超时，
available 本次以 11.163 秒返回 48 节点；003 仍不存在、005 created/unstarted、
生产付费关闭、账本及 marker 未消费。需要一项已知良好的网线/同 LAN 端口对照，
而不是重复软件参数试验。详细数据和证据见
[接收路径调查的最新记录](pc2-receive-path-investigation-2026-09-30.md)。下文保留原采样时间点。

后续接续增加了 NAS 本机、Windows、IPv6 与网卡/链路对照：问题不只发生在共享代理
API；正常客户端能够 checkout/release，但 PC2 大数据接收仍不可靠。相关临时设置已
恢复；后续受控重启到现有 `.107` 并恢复 17 个原运行容器后，大响应仍失败。详见
[PC2 接收路径续接调查](pc2-receive-path-investigation-2026-09-30.md)。

**后续最新状态（19:33:58 UTC）：**已实际完成精确 `.107` 内核的 `r8168` 单次驱动
对照，三次 1 MiB 传输仍全部失败；300 秒本机 timer 已自动恢复 `r8169`，随后卸载
候选。原 IP/路由/profile、17 个原运行容器及镜像已核验。003 仍未消费，005 从未启动，
生产付费开关仍 false，账本/receipt/开始标记仍不存在。需要独立现场链路对照；
不能把成功编译或绑定候选当作网络修复。下文启动时间、财务等保留为原时间点快照。

**用户确认开机后的续接（20:06 UTC）：**已确认 PC2 在线，但完整 1 MiB、实际小 MSS
及大包 ICMP 对照仍失败。涉及 PC2 的 1500-byte ICMP 包丢失 50%–75%，`.201` 到
NAS 的控制组 20/20 成功；不能只归咎于代理应用。内核确认此前曾睡眠，临时防睡眠
保护最多一小时且不改永久设置。19:59 UTC 真实容器的 nodes/available 请求仍超时；
guard 状态未消费，最新只读财务余额 4.5953 USD、活动订单 0、报价 0.045 USD。
具体结果和现场链路对照要求见上述调查文档，本轮没有继续注册或购买。

## 接手位置与当前状态

上轮的 OTP minimized-session 兼容候选仍运行于隔离 provider：

- 容器：`easyregister-oauth-provider-20260912-001`。
- 镜像：`sha256:73fe4635f25195b3dce8dff658d6e51632cb8be5223f9387081b5642393e34a2`。
- 启动时间：`2026-09-30T03:22:51.540982178Z`；重启次数为 0。

`easyregister-seed-once-20260930-003` 不存在，对应 preflight 标记也不存在。
`easyregister-paid-once-20260912-005` 仍为 `created`，从未启动。
原始 `attempt.json`、`attempt.json.receipt.json` 与 paid `run.started.json`
均不存在，未修改或重建任何账本。没有活动中的 seed / paid canary。

生产 orchestrator 与 Python provider 仍保持 9 月 12 日的启动时间、0 次重启，
生产 `REGISTER_SMS_ALLOW_PAID=false`。本轮没有更换生产或隔离 provider 镜像。

## 已确认的阻塞

上轮最后一次共享服务核验仅显示 `RuntimeError: command_failed:docker:1`。
本轮将检查拆分后，确认失败在 PC2 读取共享 EasyProxy 节点接口的响应阶段。

| 观察位置 / 接口 | 本轮结果 |
| --- | --- |
| 代理主机 `.201` 本机 `/api/nodes` | HTTP 200；88 个节点，650974 字节，约 0.04 秒 |
| 代理主机本机可用节点接口 | HTTP 200；该次为 48 个节点，289779 字节，约 0.01 秒 |
| PC2 宿主机可用节点接口 | 约 0.01 秒收到 HTTP 200 头，读取 chunked 正文超时 |
| PC2 生产容器可用节点接口 | 同样在收到 HTTP 200 头后，读取正文超时 |
| 最终容器 `/api/nodes` | `TimeoutError`，10.679 秒 |
| 最终容器可用节点接口 | `TimeoutError`，12.018 秒，受诊断总时限约束 |
| 最终容器订阅状态接口 | 0.014 秒成功；无错误，未刷新中，88 个节点 |
| 最终容器网关状态接口 | 0.005 秒成功；`enabled=true, applied=true` |
| 最终容器短信会话接口 | 成功，0 个会话 |

最终检查前订阅已在 `11:32:53 UTC` 刷新完成，下一次刷新为 `12:32:53 UTC`。
因此当前失败不能归因于“距下一次刷新不足 600 秒”，也不能仅靠等待刷新解决。

PC2 网卡上的限定 TCP 头部抓包观察到数据序列缺口、重复 ACK / SACK，以及后续
补发的数据段。未输出或保存 HTTP 授权头、账号、OTP、cookie 或令牌正文。
网卡计数和 TCP checksum 错误未提供足以定位具体故障设备的证据。

这把故障范围缩小到 **`.201` 到 PC2 的响应传输路径**；尚未确定丢包发生在
发送端虚拟网卡、交换链路还是接收端，不能宣称已经修复网络根因。

## 有界对照与恢复

仅在单个诊断 socket 上限制 TCP MSS，没有更改主机路由或 MTU：

- 默认 MSS：读取 98304 字节后超时，约 15.12 秒。
- MSS 1200：读取 114688 字节后超时，约 11.78 秒。
- MSS 900：一次完整读取 284895 字节，但耗时约 16.28 秒。

缩小 MSS 没有形成足够稳定、及时的链路，不将其视为修复。

另外对 PC2 的 `enp2s0` 做了一次短时 GRO 对照：原设置下读取超时；临时关闭
GRO 后约 15.35 秒仍未读完整。`finally` 中立即恢复 `gro on`，前后完整
`ethtool -k` 输出一致，随后再次确认 GRO 为 on。没有重启网卡、修改防火墙、
修改持久网络配置，或保留测试用网络调整。

## 诊断辅助脚本与验证

所有本轮测试代码和本地运行产物均在：

```text
C:/Users/Public/nas_home/AI/GameEditor/linshi/easyregister-continuation-20260930/
  pc2_readonly_preflight.py
  test_readonly_preflight.py
  preflight-final.json
  preflight-bounded-final.json
```

`preflight-final.json` 是初版诊断结果：整体超时 100 秒，只保留了 client
`TimeoutExpired`，但财务检查继续成功。相应远程 stdin Python 进程在清理复核前
已自行退出，没有强制终止其他进程。

改进后的脚本为每个客户端阶段设置 12 秒总时限，避免 socket 每次读取计时被
少量持续到达的数据不断延后；错误仅输出类型，不打印敏感异常正文。一项失败
不跳过其他只读阶段。短信供应商只调用 `getBalance`、`getActiveActivations`
和 `getPrices`。`preflight-bounded-final.json` 为最终成功结束的分阶段证据。

执行的本地验证：

```powershell
rtk proxy python C:/Users/Public/nas_home/AI/GameEditor/linshi/easyregister-continuation-20260930/test_readonly_preflight.py
```

结果：**3 项通过**，覆盖只读 GET、阶段隔离、敏感值不进入输出、deadline
设置/取消及 UTF-8 无 BOM / Python 编译。该测试不等同于注册或 OAuth 验收。
本轮没有修改产品源码，没有重跑历史 provider 的 38 项测试，也没有 commit/push。

## 财务闭环

最后一轮供应商真实只读请求均返回 HTTP 200：

- 余额：**4.5953 USD**。
- 活动订单：**0**，根据实际返回的活动订单数组确认。
- country 16 / service `dr`：报价 **0.045 USD**，库存 **274108**。
- 付费上限保持 **0.05 USD**；未启动付费 runner。
- 本轮短信购买次数 **0**，短信支出 **0 USD**。

余额、库存及订单数量是上述时间点的快照，不能直接作为下一次付费前检查。

## 下一步的准确入口

1. 先恢复 PC2 对共享代理的可靠响应传输，或明确选择并验证独立健康路径。
   需要通过实际 orchestrator 容器完整读取节点接口，并检查真实登录目标流量；
   不用仅有 HTTP 200 头、健康状态或代理主机本机成功代替。
2. 通过依赖检查及原有刷新时间门禁后，才运行尚未消费的
   `serial-seed-20260930-003.py`。失败先诊断，不能连续重启同一样本。
3. 只有正常有效的新 seed 才能进入 output root **005**，保留原始字节和
   SHA256；不能修改时间戳、放宽 900 秒有效窗或修改原始购买账本。
4. 重新核验报价、余额、活动订单和单次限制后，最多启动一次既有 paid runner。
   最终验收仍要求 OAuth 完成、free/personal claims 与身份匹配的最终 artifact。

不要因为本轮已定位预检失败，就把 OTP 候选或整个注册流程视为已实测通过。

## 2026-10-01 最新接续状态

节点管理响应已通过 gzip 修复实际恢复，共享 pool 的伪 dedicated 租约也已修复
为显式 `pinned-node` 连接绑定。真实生产容器已完成同一自动选中节点下的完整
providers/login 请求与 release；细节见
`pc2-receive-path-investigation-2026-09-30.md` 的最新业务修复章节。

003 的 wrapper 已换用窄补丁 canary 镜像，并于 02:17 UTC 消费一次。它在严格
proxy preflight 失败，未取得 mailbox、未注册、未购买短信。真实错误是收到部分
ChatGPT 大登录页后超时，不是认证头缺失或整体互联网不可达。不得继续直接执行
已消费的 `serial-seed-20260930-003.py`、删 marker 或增大超时来伪造通过。

paid 005 仅更新了 prepared image，仍为 `created`、从未启动。原旧 prepared
容器保留，`guard-state/attempt.json` 和 receipt 不变；900 秒 seed 有效窗、
country16/service `dr`、单次尝试、禁止重发/复用、0.05 USD 上限均在无网络
检查容器中核验。只有真实正常的新 seed 才能继续原来的单次 paid 闭环。

## 02:42 UTC 的实际接续

HTTP/1.1 对照也未完整读完 ChatGPT 大 landing HTML，不作 HTTP/2 根因或提高
timeout 的结论。六步 unpaid seed 不执行 ChatGPT login-init，新 004 仅对该
registration-only owner 的预检去除大 landing 页面；auth 登录页与 CSRF 的完整
200、拒绝 challenge、原 timeout 和单次限制保留。生产/full flow、paid005 和
真实 ChatGPT init 未改。原 003 的 marker 与输出保留，不可重启。

3 项配置验证通过；004 刷新门禁通过后于 02:42:14 UTC 启动，02:43:57 已观察
到正常业务 checkout selected，注册仍在进行中。这个结果只证明对齐后的代理
预检通过，不是新 seed 或最终 OAuth 验收。当前财务与容器复核记录见
`auth-session-repair-2026-09-30.md` 文末和 `current-readonly-preflight.json`。

## 03:11 UTC 的进一步定位

004 已结束：代理和邮箱领取/释放通过，但注册端 curl code 7 / CONNECT 502。
严格选中的 `fast-b2-2` 出现三次 EOF 后被拉黑，网关随后 fail closed；没有
跨节点或 DIRECT。这个具体节点/目标链路失败不能等同于 PC2 整体无法上网。
原 004 已消费，不可重启。

自动选出的另一节点 auth 大页也不完整；SG-X5-3 独立对照则完整通过 auth、
CSRF、Platform 三个目标，并读取到 Sentinel 根路径完整 404（仅传输对照）。
新 005 只通过现有 native static 模式绑定这个严格 tag，保留完整预检且不
skip、不改 timeout、不增次数。生产/full/paid flow 未切换，005 03:11:47
启动后仍在运行中。最新细节和终态入口以 auth-session 文末为准。

## 03:29 UTC 最终状态

005 已于 03:16:57 正常退出，六步均 ok，实际 SG-X5-3 固定路线跑通账户创建
及 Platform 初始化步骤。03:22:17，paid 镜像的原默认 age-enforced validator
接受该新 seed，原字节/SHA256 已保存到 output005，未更改内容或时间戳。
004/005 均已消费，不能再用上述 launcher 重启它们。

后续 paid flow 包含真实 ChatGPT login-init，所以它的完整 landing-page 检查
未被删掉。同 SG-X5-3 的大页面仍是 200 / 部分正文 / 未完整结束，付费门禁
没有通过，paid runner 未启动，最终 OAuth 尚未完成。下一次启动必须再次检查
seed age、余额/订单、路线和原账本，暂存的 seed 不是永久有效的授权。

最终完整 nodes=88 / available=46，gateway applied=true。active canaries=0、
SMS sessions=0、供应商活动订单=0；余额 4.5953 USD，单次报价 0.045 USD、
上限 0.05 USD。本轮购买 0 次。详情以 auth-session 文末和脱敏 JSON 为准。
