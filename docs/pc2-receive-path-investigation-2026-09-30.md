# PC2 接收路径续接调查：重启及驱动对照均未恢复通路

接续会话：`01a0f1f9-be09-78a3-b365-0eebb2db5cbd`。
接手断点见 [前轮网络预检](pc2-network-preflight-2026-09-30.md)。

## 当前结论

**最新续接（2026-10-01 00:46 UTC）：网络仍未修复。**同步完整 ICMP 抓包已把本轮
33 个缺失请求夹在 NAS 发送侧已观察与 PC2 主机抓包未观察之间；已到 PC2 的全部
47 个请求均有回复并回到 NAS。单次 10 Mbps 对照的三次 1 MiB 仍全部失败，独立
timer 已实际恢复原 `0x62ff`、1000 Mbps。新增广播目的 MAC 对照也丢包，不能仅用
单播 MAC 学习或单播地址过滤解释全部故障。现场换一项已知良好的网线/同 LAN 端口
仍是下一项有区分价值的对照；不能把这些结果写成网卡硬件已确诊。详见文末最新记录。

**本轮没有完成网络修复，也没有产生新的 seed 或最终 OAuth 产物。**
不得将服务健康、代理 checkout 成功、收到 HTTP 响应头或一次小响应成功视为完整验收。

用户随后要求“继续推进”，已经完成一次受控重启及单次可回退驱动对照：实际启动到
已有的 `6.12.107+deb13-amd64`，并临时绑定候选 `r8168`，但两项对照的 1 MiB 传输
均失败。候选已自动回退至 `r8169` 并卸载，17 个原运行容器及镜像均保持不变。
没有创建 seed、启动付费 runner 或安装持久驱动/黑名单配置。

**后续开机确认后的最新复测（20:06 UTC）：**PC2 已在线，但连续三次 1 MiB 门禁
仍失败；补充的真实 MSS 对照及大包 ICMP 对照也失败。内核日志确认之前的部分
整体断联时段存在系统睡眠，已用有界 inhibitor 保护这轮检查；不能把睡眠与醒着时
的大包丢失混为同一个原因。详见文末本轮追加记录。

当前至少需要区分以下三层：

1. **管理及租约层并非完全中断。**最后一次正常客户端 checkout 要求专用节点，
   成功取得代理 URL，并在 `finally` 中通过正常客户端释放。未绕过客户端 readiness。
2. **PC2 大数据接收仍不可靠。**完整 `/api/nodes` 的读取仍在 12 秒阶段时限内超时；
   小型状态、过滤后较小的可用节点响应能成功。不同 LAN 来源的普通 TCP 也有相同现象。
3. **只读 OpenAI 请求出现独立的应用挑战。**同一租约的 Cloudflare trace 完整返回
   HTTP 200；ChatGPT providers 和 auth.openai.com 完整返回 HTTP 403，并带
   `cf-mitigated: challenge`。本次使用 Python 标准库 HTTP 客户端，不是原生浏览器；
   不能据此断言相同代理的真实浏览器必然失败，也不能将 403 视为目标站点验收通过。

## 传输证据及结论边界

无凭据的临时 TCP 服务只绑定指定主机，接收来自 PC2 的限定诊断请求，
每个响应不超过 1 MiB；客户端校验完整长度与 SHA256，并有总阶段时限。
没有输出账号、cookie、OTP、令牌或代理 URL。

- `.201` 向 PC2 发送 1 MiB，多个对照均未在限定时间内完成。
- NAS `.200` 直接向 PC2 发送 1 MiB，同样失败，排除了 EasyProxy 应用逻辑作为唯一原因。
- Windows 通过 SSH 向 PC2 发送 64 KiB，曾在约 0.297 秒完整成功；1 MiB 在 20 秒超时。
- IPv6 link-local 和已有 StarVPN 路径未构成健康替代通路。
- 12 次 ARP 探测只收到 `40:8d:5c:11:26:b4` 的响应，未发现第二个 MAC；
  这不是对所有潜在二层问题的穷尽证明。

NAS 的虚拟机 TAP 为 `tap0211322d33eb`，对应 MAC `02:11:32:2d:33:eb`。
该 VM 位于 `ovs_bond0`，bond 模式 `balance-slb`，通常把其 hash 31 分到 `eth0`。

一次只抓取诊断 TCP 端口头部的跨接口对照显示：NAS `eth0` 的数据段记录为 93 条，
PC2 为 46 条，并有序列缺口、SACK 及重传。之后把相关流量改到 `eth1`，仍未恢复。
**因此不能把最初的 `eth0` 观察写成“eth0 已确诊故障”，也不能宣称 NAS 单网口切换
就是修复。**证据指向包含 PC2 接收侧及其链路的低层路径，但尚未区分交换设备、物理
链路、网卡硬件和驱动。

## 本轮可逆对照及恢复

以下对照均未恢复可靠的 1 MiB 传输：

- `.201` 的 TSO 单项关闭，以及发送卸载组合关闭。
- NAS VM hash 暂时迁移、停止自动 rebalance 的有界迁移对照、自动过期的双主机限定流表。
- PC2 RX checksum 关闭、`rx-all` 开启。
- PC2 暂时协商 100 Mbps 全双工，保留流控广告能力。
- PC2 现有驱动的接口关闭/重新打开。
- 小数据块及发送间隔对照：结果仍不稳定，不能作为可靠修复。

原始链路广告掩码通过 ioctl 读取为 `0x62ff`；100 Mbps 对照使用 `0x62c8`，
PC2 本机诊断进程在 `finally` 中恢复原始 `0x62ff`。
接口重新初始化由一次性 `easyregister-nic-reset-20260930.service` 执行，
退出处理强制恢复接口；最终 `Result=success`、`ExecMainStatus=0`，服务已 inactive。
该操作未修改持久网卡配置、IP、路由或防火墙。

最终恢复核验：

- `.201` 的 GRO/GSO/RX/SG/TSO/TX 六项读值均为原来的 1。
- PC2 RX checksum on、GRO on、rx-all off，链路 1000 Mbps/Full，原广告能力恢复。
- NAS `other_config={}`，自动 rebalance 恢复；诊断 cookie `0xe2010104` 已自动消失，
  原来的三条 OpenFlow 规则仍在。
- 指定诊断端口 `39091` 没有遗留监听；没有持续运行的测试服务。
- 未更换生产或隔离 provider 镜像，没有重启生产容器，没有 commit/push。

本轮未重新执行前一轮 GRO 对照；前一轮已记录其无改善及恢复结果。
临时诊断中的导入缺失、恢复 helper 对不支持的 RX setter 报错和早期 pacing 参数上限
错误都属于测试辅助代码问题，不能算作网络结果。缺失 HTTP 依赖已改用标准库；
恢复状态另行实际确认；pacing 修正参数限值后重新运行，结果仍不稳定。

## 原任务及费用边界

财务与原任务最后完整快照：**2026-09-30 16:42:21–16:42:40 UTC**。

| 项目 | 结果 |
| --- | --- |
| 完整节点接口 | 12 秒阶段超时 |
| 可用节点接口 | 4 个节点，约 0.219 秒完整读取 |
| 订阅 | 88 个节点，无刷新错误，未在刷新中 |
| gateway | enabled/applied 为 true |
| 生产 `REGISTER_SMS_ALLOW_PAID` | false |
| seed `20260930-003` | 不存在，未创建 |
| paid runner `20260912-005` | created，从未启动 |
| 原始购买账本及 receipt | 不存在，未修改 |
| 活动 canary | 无 |
| 共享 SMS 会话 | 0 |
| HeroSMS 活动订单 | 0，按实际数组计数 |
| 余额 | 4.5953 USD |
| country 16 / service dr 报价 | 0.045 USD，库存 92342 |
| 本轮短信购买/支出 | 0 次 / 0 USD |

这些余额、报价、库存是快照；以后购买前仍须重新验证，不能沿用历史检查。
后续只读代理租约测试没有短信动作，正常释放成功。

## 受控重启前的边界与本轮执行结果

前一轮请求重启确认；用户随后以“继续推进”授权下一步，本轮已执行一次受控重启。
重启是诊断对照；结果证明不能把它作为本故障的有效修复。

重启前运行 `6.12.105+deb13-amd64`；已有 `.105` 和 `.107` 的 vmlinuz、initrd 和
`r8169` 模块。没有现成 `r8168`、DKMS 或可用 headers，不能从当前唯一实体 NIC 的
SSH 会话盲目卸载现有驱动。也没有第二块实体/USB Ethernet 网卡。

本轮 sudo 只读核验了准确 menuentry 和 linux 路径：`GRUB_DEFAULT=0`、
`GRUB_TIMEOUT=5`，默认项为 `/boot/vmlinuz-6.12.107+deb13-amd64`，grubenv 为空，
旧 `.105` 正常/恢复启动项仍存在。没有修改 GRUB 默认值、删除旧内核或安装驱动。

恢复准备的重点：

- `docker`、`crow-engine-controller`、`stars-service`、`tailscaled` 已 enabled 且 active。
- 生产 EasyRegister 主服务及依赖为 `unless-stopped`。
- 隔离 OAuth provider/gateway、paid-sms、paid-guard 的 RestartPolicy 为 `no`，
  重启后需要按准确名单恢复，不能假定自动恢复。
- `paid-once-005` 和其他 created 付费容器也为 `no`。
  **禁止批量启动全部容器**，避免误启动尚未消费的一次性付费任务。

重启由一次性 systemd timer 发起；通过 boot ID 变化和 `uname -r` 确认实际重启及 `.107`。
手动恢复仅限重启前在运行的 4 个隔离依赖，并核对原容器 ID/image，按 guard、SMS、
provider、gateway 的顺序启动。最后核对 17 个原运行容器全部 running，镜像全部不变；
Docker、Crow controller、Stars、Tailscale、NetworkManager 均 active/enabled。
这是运行及镜像恢复证明，不是这些产品全部业务流程的验收。

`easyregister-paid-once-20260912-005` 仍为 `created/no`，两个原始购买账本、
003 preflight marker、paid 005 started marker 均不存在，生产付费开关仍 false。
没有批量启动历史/created 容器，没有创建新的注册样本或调用付费短信购买。

### 重启后传输结果

`transport-after-reboot.json` 的执行结束时间为 `2026-09-30T17:35:53.243644+00:00`。

| 期望响应 | 发送方式 | 实际完整接收 | 耗时 |
| --- | --- | --- | --- |
| 65,536 bytes | 普通 64 KiB 写入 | 是 | 2.963 秒 |
| 1,048,576 bytes | 普通 64 KiB 写入 | 否，仅 1,448 bytes | 6.004 秒，socket timeout |
| 1,048,576 bytes | 1 KiB/1 ms 写入 | 否，仅 29,136 bytes | 7.668 秒，socket timeout |
| 1,048,576 bytes | 普通 64 KiB 写入 | 否，0 bytes | 6.005 秒，socket timeout |

三次大响应均失败，完整长度与 SHA256 门禁未通过；不能继续消费单样本 003。
管理 SSH 随后也出现间歇性超时：一次直连预检 exit 255，随后一次 `.201` jump
预检在 banner exchange 超时。本轮没有获得新的完整 post-reboot 财务快照，
不能把前面的余额/报价快照当作已刷新，也没有因此重启任何已消费样本。

### 下一步隔离

已向用户询问下一步选择：

1. 现场换 PC2 网线或交换机端口，若有 USB Ethernet 则优先用独立网卡对照。
2. 不方便现场操作时，先准备可回退的 `r8168` 候选；不能因版本名称或相似报告就
   假定有效，必须先完成编译兼容及回退准备，再安排唯一实体网卡的切换。

本轮没有执行第二项安装/切换，不宣称已确定网卡硬件或驱动根因。
无论采取哪条路径，都需先通过连续完整 1 MiB 传输、生产容器完整读取节点响应及
真实业务客户端的目标探测和释放。之后才运行单样本；只允许正常新鲜 seed 进入原有
单次费用保护，不放宽 900 秒有效窗、不重置账本。

## 后续推进：单次 r8168 对照及自动回退已完成

此节更新至 **2026-09-30 19:33:58 UTC**，覆盖前文“本轮没有执行第二项”的旧停点。
用户再次要求继续后，实际完成了编译、上传、回退演练、一次切换及恢复核验。
**驱动对照失败，网络问题仍未解决。**

### 精确候选及隔离准备

通过 Debian 官方 APT 源及签名校验核实：`r8168-dkms=8.055.00-1`、
`linux-headers-6.12.107+deb13-amd64=6.12.107-1`、相同版本 common headers/kbuild。
在本机 Docker Desktop Linux 容器内使用 GCC 14 单线程编译，不在 PC2 上安装 DKMS。
初次工具安装遭遇本机构建容器 OOM；随后对自身容器限定资源并使用最小编译工具及
直接解包的 headers，补齐 pahole 后编译成功。这些构建问题不是 PC2 网络对照结果。

候选 `vermagic` 为 `6.12.107+deb13-amd64 SMP preempt mod_unload modversions`，
包含 `[10ec:8168]` 的 PCI alias。上传版本仅去除调试/BTF 元数据，不改动驱动源码；
大小 1,212,104 bytes，SHA256：

```text
649b6511e48861aeeb557efeff9f81e6c6d9547f640bf15505a99f4965402225
```

候选不是已证明有效的修复。编译与 modinfo 通过只证明此项构建/元数据门禁；
真实加载和绑定另在 PC2 对照中验证。

PC2 在准备期间曾出现直连和跳板 SSH 超时、`.201` ping/ARP 无响应，之后自行恢复
到相同 MAC。不能只凭这个现象确定设备关机、网线、交换设备或 NIC 故障。
首次正常上传仍在约 31 KiB 后停止确认；改为压缩、24 KiB 分段、逐块确认及可续传后，
167,048-byte bundle 完整上传并在 PC2 核对候选原字节 SHA256。未覆盖不同内容的旧文件。

### 回退安全边界与实际切换

回退 helper 已做只读交叉审查，修正日志错误阻断恢复、切换/回退竞态和短暂 NM 故障
导致恢复提前退出的问题。10 项离线检查通过，包括缺少定时器/剩余时间不足不切换、
日志不可写仍恢复、限定 PCI 回绑和失败恢复。离线检查不代替实际 PC2 对照。

- 固定设备 `0000:02:00.0`，核对内核、vendor/device、模块 hash 和原 profile UUID。
- 在私有 root `/run/easyregister-r8168-trial-20260930` 中保存候选、脚本和前状态。
- 先在原 `r8169` 状态运行回退演练，确认 profile/IP/路由后再布置 300 秒本机 timer。
- 保留 `r8169` 已加载；只对精确 PCI 设备 unbind/bind，不卸载原驱动。
- 切换与回退使用同一进程间锁；两项 service 均有有界执行配置。
- 未安装包、改 blacklist、GRUB、持久 NM profile，未删除模块或旧内核。
- 独立 timer 不依赖 SSH；内核驱动回调挂死仍是无法绝对保证软件回退的边界。

`19:28:06 UTC` 定时器已布置；`19:28:09 UTC` 实际绑定 `r8168`，切换 service
`Result=success / ExecMainStatus=0`。候选状态下 SSH 可读并核对 driver，随后运行
同一无凭据传输门禁：

| 期望 | 发送方式 | 实际接收 | 结果/耗时 |
| --- | --- | --- | --- |
| 65,536 bytes | 64 KiB 写入 | 65,536 bytes | 完整长度及 SHA256 通过，4.832 秒 |
| 1,048,576 bytes | 64 KiB 写入 | 124,528 bytes | 总阶段超时，16.000 秒 |
| 1,048,576 bytes | 1 KiB/1 ms | 106,056 bytes | 总阶段超时，16.000 秒 |
| 1,048,576 bytes | 64 KiB 写入 | 0 bytes | socket timeout，6.007 秒 |

虽然前两次大响应比某些旧样本多接收了一部分字节，但三次均没有完整成功，
不能称为修复或通过门禁。没有据此运行生产节点/浏览器验收、新 seed 或付费流程。

### 自动回退及费用保护的最新核验

没有提前取消 timer 或靠 SSH 手动回绑。独立 timer 在 `19:33:07 UTC` 发起回退，
`19:33:10 UTC` 记录 `rollback_local_state_verified`；回退 service 为
`Result=success / ExecMainStatus=0`。`19:33:58 UTC` 外部 SSH 核验：

- 当前为 `r8169`，`enp2s0` 为 `UP/LOWER_UP`，原 `192.168.15.104/24` 已恢复。
- 原 NM profile active；默认路由仍经 `192.168.15.1`，接口 `enp2s0`，metric 100。
- 核验回退后卸载无绑定的候选 `r8168`；原 `r8169` 保持加载。
- NetworkManager、Docker、Crow controller、Stars、Tailscale 均 active。
- 原 17 个运行容器全部 running，逐名比较镜像均未改变；不等于全部业务验收。
- 生产 `REGISTER_SMS_ALLOW_PAID=false`，paid `005` 仍 `created/no`，从未启动。
- seed `003` 容器、原购买账本/receipt、003 preflight、005 started marker 均不存在。
- 本轮无短信供应商购买调用，短信购买/支出仍为 0 次/0 USD；未刷新余额/报价快照。

本机构建容器已停止，未删除容器或证据。没有第二次重启、持久驱动部署、产品源码
改动、commit/push。候选虽已卸载，外部模块加载可能留下当前 boot 的 taint 诊断标志；
未为清除此标志而再次重启。

本节主要证据位于 `linshi/easyregister-network-repair-20260930/`：
`r8168-build/build.log`、`r8168-build/manifest.json`、`r8168-offline-tests.log`、
`r8168-upload-report.json`、`transport-r8168.json`、`r8168-post-trial-audit.json`。
PC2 root helper 和事件记录保留在上述 `/run` 目录；持久上传原件在
`/home/mjc/easyregister/network-repair-20260930/r8168-trial`。

### 当前下一步

重启到另一个已有内核、原驱动参数对照、不同来源/IPv6/StarVPN 及替代驱动均未恢复
可靠的大响应通路。证据仍不足以区分具体物理设备，不能再以重复远端参数试验或
提高超时来代替修复。已请求现场确认 PC2 开机和网口指示灯，并做一项已知良好网线
或交换机端口对照；若已有备用 USB Ethernet，也可用其形成独立 NIC 路径。
无需先购买硬件。对照完成后复用完整长度/SHA256 门禁及真实容器客户端验证；
通过后才消费未运行的 003，并保留原单次费用保护。

## 用户确认 PC2 开机后的续接：睡眠与大包丢失分开核验

记录更新至 **2026-09-30 20:06 UTC**。本次 SSH 确认 PC2 在线、内核仍为 `.107`、
驱动为 `r8169`、NetworkManager/Docker active、原 IP 和默认路由正常。
boot ID 为 `50d4a42e-6c9b-4d53-91bb-2630eb447da6`，与前轮相同；本轮没有执行重启。

### 确认系统曾睡眠，不再把所有 ARP/SSH 消失都归因于网卡

内核 journal 记录两次 `PM: suspend entry (deep)` / `PM: suspend exit`：
按 UTC 换算分别为 `17:39:04–18:11:52` 和 `18:26:54–19:06:14`。
因此此前部分整体不可达时段有真实 sleep/resume 证据，不能全部写成随机 NIC/链路
故障。当前在线期间的大响应失败仍独立存在。

为防止本轮诊断被再次睡眠打断，在 `19:51:39 UTC` 启动了系统级
`easyregister-network-inhibit-20260930.service`，使用 `systemd-inhibit sleep:idle/block`，
`RuntimeMaxSec=3600s`，最长至约 `20:51:39 UTC` 自动结束。20:06 UTC 核验仍 active，
inhibitor 列表确实含本项。没有屏蔽关机、改动永久电源设置或停止其他服务。
当前 gsettings 的 AC sleep type 已为 `nothing`、timeout 900；UPower `OnBattery=false`。
这些当前值不能反推过去睡眠的具体发起者；本轮没有擅自改 AC/battery 配置。

### 醒着状态的完整传输及实际 MSS 对照

`19:44:35–19:45:17 UTC`，复用原有门禁：64 KiB 完整成功；三次 1 MiB 分别只收到
24,616、0、94,120 bytes 后超时。不能把开机、SSH 成功或小响应成功视为通路已修复。

只读交叉检查确认之前的 1 KiB 应用 write/pacing 并未设置 `TCP_MAXSEG`；原抓包
SYN 为 MSS 1460，常见 TCP payload 1448。同一 1448-byte 段首发缺失、同尺寸重传
成功的记录，反驳“所有大于固定 MTU 阈值的数据包必丢”的简单结论，但未排除概率性
大小相关丢失。

`19:48:19–19:49:07 UTC` 只对诊断 socket 在 connect 前设置 MSS，没有修改网卡
MTU、路由或防火墙。客户端与发送端均核对实际协商值：

| 请求 MSS | 实际发送 MSS | 期望 | 实际接收 | 结果 |
| --- | --- | --- | --- | --- |
| 默认 | 1448 | 1,048,576 bytes | 10,136 bytes | 超时，6.627 秒 |
| 1000 | 988 | 1,048,576 bytes | 251,940 bytes | 超时，16.232 秒 |
| 600 | 588 | 1,048,576 bytes | 0 bytes | 超时，6.007 秒 |
| 256 | 244 | 1,048,576 bytes | 730,292 bytes | 总阶段超时，18.000 秒 |

实际 MSS 已缩小，并非仅修改应用 write；四项均未完成长度/SHA256 门禁。
不通过提高超时或保留小 MSS 设置把失败包装成修复。诊断监听已正常结束，未留下
持久网络规则或新驱动状态。

### 与 TCP、代理服务无关的大包丢失证据及控制组

使用 DF 的 IPv4 ICMP echo，1472-byte payload 对应 1500-byte IP 包；每项 20 个
样本，间隔 0.1 秒。结果：

| 方向 | ICMP payload | 成功/发送 | 丢包率 |
| --- | --- | --- | --- |
| PC2 → `.201` | 56 bytes | 20/20 | 0% |
| PC2 → NAS `.200` | 56 bytes | 19/20 | 5% |
| PC2 → `.201` | 1472 bytes | 5/20 | 75% |
| PC2 → NAS `.200` | 1472 bytes | 10/20 | 50% |
| `.201` → NAS `.200`，控制组 | 1472 bytes | 20/20 | 0% |
| `.201` → PC2，反向控制 | 1472 bytes | 10/20 | 50% |

故障不仅是 EasyProxy API、OpenAI 挑战或 TCP 应用读取问题。涉及 PC2 的路径存在
显著大包损失；控制组未复现同样损失。但 ICMP 是往返测试，不能仅凭这些结果区分
请求/回复哪一方向损失，也不能直接定位到网线、交换设备、NIC 或特定驱动。

本轮只读检查未发现 PC2 当前 EEE 实际激活（`eee_active=0`），未为了试错改 EEE。
现有 nftables 规则及 conntrack 参数均未修改。当前 RX CRC/missed 计数为 0，IP/TCP
backlog/receive-queue 丢弃计数也为 0；这些计数不构成物理链路健康证明。
上一轮外部模块加载留下的当前 boot taint 为 12288，候选仍已卸载，没有再次重启清除。

### 正常业务客户端和费用保护的最新只读快照

`19:59:13–19:59:39 UTC`，生产容器中的真实客户端：完整节点及 available 节点请求
均超时；subscription 有 88 个节点、enabled、非刷新、无错误；gateway 为
enabled/applied。只看后两项健康状态仍不足以运行新样本。

- 生产及隔离 provider 镜像不变、running，生产付费开关仍 false。
- 003 容器及 preflight 不存在；005 仍 created、从未启动；无 active canary。
- 原购买账本、receipt、005 started marker 均不存在，未修改。
- 共享 SMS session 为 0。供应商只读接口均 HTTP 200：余额 4.5953 USD，活动订单 0，
  country16/service dr 报价 0.045 USD、库存 114247，上限仍 0.05 USD。
- 余额/报价/库存是该时间点快照；以后购买前仍须刷新。本轮没有购买或短信支出。

证据：`transport-r8169-after-trial.json`、`mss-probe-20260930-1947.json`、
`network-state-20260930-1950.json`、`network-controls-20260930-1953.json`、
`icmp-power-20260930-2003.json`、`icmp-control-20260930-2010.json`、
`eee-power-readonly.json`、`preflight-20260930-2008.json`。其中部分文件名后缀是标签，
不是精确采样时刻；精确时刻优先取 JSON 元数据及本节记录。

已再次请求用户说明本次是否只开机，或也换过网线/交换机端口；若未更换，优先使用
现有已知良好的网线或同 LAN 端口做一次独立对照。未因此要求采购或扩大部署架构。
网络门禁未通过，003 和后续单次 paid 005 均继续保留为未消费状态。

## 联网核实与证据边界

Debian 相似报告 [#1110193](https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=1110193;msg=5)
是 rev15，不是本机 rev06；报告者追报问题自行消失，并说运行中换 `r8168` 没有改善，
仅猜测启动初始化状态。没有维护者确认的根因或可直接套用的修复 commit。

[Debian r8168 包说明](https://packages.debian.org/trixie/r8168-dkms)包含 RTL8168E，
但安装会禁用 r8169，并需要 headers。不能把它当成当前可无风险热切换的修复。
本轮没有安装该包。

## 本地证据

```text
C:/Users/Public/nas_home/AI/GameEditor/linshi/easyregister-network-repair-20260930/
  transport_probe.py、各单项对照脚本、nic_features.go
  capture-*-eth0.txt、capture-*-eth1.txt、capture-*-tap0211322d33eb.txt、capture-*-pc2.txt
  preflight-after-diagnostics.json
  live-proxy-probe.json
  pc2_reboot_control.py
  reboot-before.json、reboot-request.json、reboot-after.json、reboot-recovered.json
  transport-after-reboot.json
```

此前 15 份 Python 辅助源码 UTF-8 无 BOM、AST/编译检查通过；Go helper 已 gofmt。
本轮新增 reboot helper 的 UTF-8 无 BOM、AST/编译、恢复名单无 canary、旧启动项保留、
原财务 guard 约束检查通过。没有重跑产品测试，也没有提交或推送。
这些检查不是产品测试、网络修复成功或注册/OAuth 验收。

## 2026-10-01 续接：同步完整抓包、10 Mbps 回退及二层目的地址对照

记录更新至 **2026-10-01 00:46:08 UTC**。本轮没有重启、安装持久驱动、修改持久
NetworkManager 配置、IP、路由、防火墙或产品源码，也没有创建注册样本或购买短信。

### 同步完整 ICMP 抓包进一步确定丢失方向

`00:24:28–00:24:56 UTC`，在 NAS `eth0`、`eth1`、VM TAP `tap0211322d33eb`
及 PC2 `enp2s0` 同时抓取限定 ICMP identifier 的无凭据合成探针。完整包用来比对
`(identifier, seq)`、MAC、VLAN、IPv4/ICMP checksum 和 payload SHA256。

| identifier / payload / pattern | NAS eth0/TAP 请求 | PC2 请求 | PC2 回复 | NAS 回复 |
| --- | ---: | ---: | ---: | ---: |
| 39301 / 56 bytes / a5 | 20 | 17 | 17 | 17 |
| 39302 / 1472 bytes / a5 | 20 | 7 | 7 | 7 |
| 39303 / 1472 bytes / 00 | 20 | 10 | 10 | 10 |
| 39304 / 512 bytes / a5 | 20 | 13 | 13 | 13 |
| 合计 | 80 | 47 | 47 | 47 |

独立重新解码 pcap 确认：33 个缺失请求均为 NAS eth0/TAP 已见、PC2 未见；
已到 PC2 的每个请求都有回复，且全部回复返回 NAS。请求目的 MAC 均正确，未带
VLAN tag，IPv4/ICMP checksum 错误为 0，跨点 payload hash 一致。所有 tcpdump
均报告 `0 packets dropped by kernel`；eth1 未出现这轮匹配报文。

同组同大小报文既有成功也有缺失，缺失序号不构成固定大小阈值或固定序号规则。
本轮不支持把丢失归因于 PC2 ICMP 回复逻辑或回复回 NAS 的路径。丢失发生在 NAS
发送侧抓包观察之后、PC2 AF_PACKET 抓包观察之前；仍缺少交换机向 PC2 端口实际
发出的观察，不能区分 NAS 实际上线路、交换/线缆、NIC 接收、驱动或更早内核路径。
主机抓包不是成功上线路的证明，IP/ICMP checksum 也不替代 Ethernet FCS 核验。

### 首次 10 Mbps 对照失败，独立回退已实际完成

`00:34:13.835 UTC` 先建立 PC2 本机 180 秒 rollback timer，再派发独立 switch
service；两项 helper 使用同一个 `flock`。`00:34:16.936 UTC` 实际协商为
10 Mbps/full duplex，原广告 `0x62ff` 暂改为 `0x62c2`，保留其他能力广告。

| 期望字节数 | 发送方式 | 实际接收字节数 | 完整长度/SHA256 |
| ---: | --- | ---: | --- |
| 65,536 | 64 KiB 写入 | 65,536 | 通过 |
| 1,048,576 | 64 KiB 写入 | 112,944 | 超时，未通过 |
| 1,048,576 | 1 KiB/1 ms | 77,872 | 超时，未通过 |
| 1,048,576 | 64 KiB 写入 | 108,600 | 超时，未通过 |

没有延长低速模式或取消回退 timer。`00:37:14.031 UTC` 自动开始恢复，
`00:37:17.042 UTC` 记录 `restore_verified`。`00:37:45 UTC` SSH 核验广告
25343/`0x62ff`、autoneg 1、1000 Mbps、carrier 1，rollback service 为
`Result=success / ExecMainStatus=0`，timer/service 均 inactive。随后再次只读
核验 `192.168.15.104/24` 和原默认路由 `via 192.168.15.1 metric 100`。
这证明本次实际恢复完成，不宣称所有失败情形都能绝对保证软件回退。

### 单播与广播目的 MAC 都存在请求缺失

`00:43:05–00:43:31 UTC`，NAS 通过原 `ovs_bond0`、自身 IP/MAC 发送低速
合成 ICMP 请求，交替使用 PC2 单播目的 MAC 和 `ff:ff:ff:ff:ff:ff`，不改变
PC2 网卡状态、邻居表、地址或路由。比较 NAS eth0/eth1 和 PC2 抓包：

| payload | L2 目的地址 | 发送请求 | NAS eth1 已见 | PC2 已见 |
| --- | --- | ---: | ---: | ---: |
| 56 bytes | unicast | 40 | 40 | 39 |
| 56 bytes | broadcast | 40 | 40 | 38 |
| 1472 bytes | unicast | 40 | 40 | 27 |
| 1472 bytes | broadcast | 40 | 40 | 22 |

独立 pcap 重解码确认 checksum 错误为 0、各接口内部 `(id,seq)` 重复为 0、
跨点 payload hash 不一致为 0；两种大小分别是 84/1500-byte IPv4 包，
98/1514-byte Ethernet frame。广播丢包足以否定“只有 PC2 单播目的 MAC 学习
或单播目的地址过滤故障”作为全部丢失的唯一解释；不能排除交换设备、NIC 或公共
接收路径，也不能排除多个故障并存。

广播报文也出现在 NAS eth0，**不等于两个物理口都已成功在线路上发送**：本轮通过
逻辑 `ovs_bond0` 发包，主机抓包不含足以证明实际线缆发送方向的证据。

前两次 sender helper 准备失败分别是 NAS 旧版 `ip` 不支持 `-j`、探针 marker
长度计算错误导致 `Message too long`；修正并通过帧长断言后才完成上述最终采样。
这些辅助脚本失败不算网络丢包结果；早期 timeout 抓包均有 25 秒本机上限。

### 当前接口、业务和费用边界

现场只读盘点确认仅有 `enp2s0` 实体 Ethernet，没有已接入的 USB Ethernet 或
Wi-Fi 网络接口；USB 中的 Realtek Bluetooth Radio 不是备用网络接口。未见
enp2s0 XDP 附着、tc ingress/egress filter 或 nftables netdev table；RX errors、
missed、align errors 计数均为 0。零计数仍不构成物理通路健康证明。

本轮 `easyregister-network-inhibit-20261001.service` 在约 `00:21 UTC` 启动，
`RuntimeMaxSec=1800s`，只临时阻止 sleep/idle，约 `00:51 UTC` 自动结束，未改
永久电源策略；`00:39 UTC` 核验仍 active。本机 Windows 是 ASUS PC、Intel
I219-V Ethernet，不是 NAS 虚拟机，之前 Windows 到 PC2 的失败不能都归于 NAS
虚拟网络。

最新只读业务/财务快照：**00:45:37–00:46:07 UTC**。

- 实际生产容器的 `/api/nodes` 12.001 秒超时；available 本次完整返回 48 个节点，
  但耗时 11.163 秒。一次较小响应成功不替代三次完整 1 MiB 门禁及实际目标验收。
- subscription 88 个节点、enabled、非刷新、无错误；gateway enabled/applied。
- 生产/隔离 provider 均 running，生产付费开关 false；provider 精确 image 不变。
- 003 容器不存在，003 preflight 不存在；005 为 created，StartedAt 仍为零值。
- 原购买账本、receipt、005 started marker 均不存在，无 active canary。
- SMS sessions 0；供应商活动订单 0，余额 4.5953 USD；country16/service dr 报价
  0.045 USD、库存 274919。上述财务信息是快照，真正购买前仍须刷新。
- 本轮没有 SMS 购买或支出，也没有绕过/消费独占 marker 和费用保护。

下一步只做一项已知良好网线或同 LAN 交换端口对照，完成后复用现有
`transport_probe.py`，要求三次 1 MiB 完整长度/SHA256 全通过，再验证真实容器
完整节点响应、正常 checkout/目标探测/release。若网线/端口对照仍失败，再用已有
备用 NIC 或交换机 PC2 端口 egress 镜像进一步隔离；不要求先采购，不再重复已失败
的驱动/降速/卸载/MSS 对照或通过提高超时掩盖丢包。

本轮证据位于 `linshi/easyregister-network-repair-20261001/`：
`icmp-path-report.json`、四份同步 `*.pcap`、`link10-{prepare,arm,status,transport}.json`、
`l2-destination-report.json`、三份 `l2-*.pcap`、`network-inventory.json`、
`preflight-20261001-final.json`。所有探针仅为无凭据合成流量，没有抓取账号、
授权头、cookie、OTP、令牌或代理 URL；未 commit/push 或清理已有工作树。

`00:49:56 UTC` 最终远端状态再次确认：相同 boot ID、`r8169`、1000 Mbps、
原 `192.168.15.104/24` 和默认路由；rollback service success/inactive，PC2
诊断端口 39091 无监听。临时 inhibitor 当时仍 active，将按原 1800 秒上限结束。
`network-final-state.json` 保留实际输出；`evidence-verification.json` 保留本地
重新核算的包集合/校验和、回退事件和 UTF-8 无 BOM/AST 检查。没有重跑失败的
传输对照，没有遗留持续诊断服务器；这些检查不等于网络或最终业务验收通过。

## 2026-10-01 业务层修复与新证据

本节是后续最新状态。PC2 可以正常访问百度；此前对某些大响应的传输失败，
不能概括成“PC2 整体网络不可用”。目前已在实际业务代码中找到并修复两项问题：

1. EasyProxy 节点列表原来返回约 445 KB 的未压缩诊断快照；实际同一内容 gzip
   约 26 KB。原客户端不请求/解压 gzip。服务端现在按明确的 `Accept-Encoding`
   压缩 `/api/nodes`，客户端兼容 gzip 和旧 identity 响应，不删 URI、timeline
   或改变筛选语义。PC2 的实际生产容器在部署后连续三轮完整读取 all/available。
2. `pool` 模式每个节点快照的端口都是共享的 22323，compat checkout 却只凭
   正端口误标 `dedicated-node`。返回的 selected tag 没有约束真实连接，因而
   OpenAI 请求仍可进入不支持该目标的 ECH Worker；日志已有 SOCKS5 code 4。
   现在客户端向服务端发送 `requireDedicatedNode`，共享 dispatcher 支持时
   返回 `pinned-node`，代理用户名携带 `pin-strict=<tag>`，在连接层限制同一
   tag；失败不偷偷换节点或 DIRECT。默认共享池请求仍保留原来的自动切换。

02:04:05–02:04:20 UTC，生产容器通过正常租约自动选中 `sg-x5-3`，同一租约的
ChatGPT providers 返回完整 827 bytes、HTTP 200；OpenAI
`/log-in-or-create-account` 返回完整 83244 bytes、HTTP 200、无 challenge。
网关真实日志确认两个目标均拨 `outbound/shadowsocks[sg-x5-3]`，租约正常释放。
这证明存在有效的业务代理通路，但不是全部节点、所有大页面或完整 OAuth 验收。

生产和 canary 的 route-key 补丁分别以各自实际已部署源码为基线派生，只加入
strict 节点身份规范化，反向替换后与基线的 UTF-8/LF 内容一致；没有把整个 dirty
工作树部署给 canary。节点库、网关配置、local-server、22323/29888 和凭据未改。

02:17:14 UTC，使用更新 canary 镜像启动一次 SMS-disabled 003 样本。样本在
`acquire-proxy-chain` 失败，邮箱及注册步骤均 skipped；proxy release 为 ok，
没有启动 paid runner。两次内部候选预检实际错误分别为 curl code 28，在 20 秒
期限内收到 111116 / 67691 bytes 后响应仍未结束，不能视为有效完整页面。
003 已消费，不得重启或删除其 preflight/run marker。

对三个真实 seed 预检目标的一次同节点对照进一步定位到大 ChatGPT 登录页面：
Canary strict pin 下，auth 登录页完整 82952 bytes，CSRF 接口完整 80 bytes，
均 HTTP 200；`https://chatgpt.com/auth/login` 返回 HTTP 200、Brotli、HTTP/2，
收到 195116 decoded bytes 后仍未在 18 秒诊断期限内结束。正在区分该页面的
上游/协议行为与 PC2 接收路径；不能把它再次笼统归为“不能上网”，也不能仅凭
200 响应头宣布预检通过。旧物理通路证据没有因此被否定或宣称已修复。

本节证据位于 `linshi/easyproxy-pc2-business-repair-20261001/`：
`gzip-transport-control.json`、`strict-business-020420.json`、
`strict-live-routing-log.json`、`acquire-overlay-preservation.json`、
`seed003-proxy-failure.json`、`strict_seed_target_probe.json`。这些文件只保存脱敏
元信息，不含节点 URI、cookie、OTP、账号身份或认证值。
