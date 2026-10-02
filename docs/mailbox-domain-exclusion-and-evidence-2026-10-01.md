# 疑似拒绝域名排除与域名因素核验

## 用户要求与处理范围

用户要求排除被拒绝的邮箱后缀，并多做核验，判断小成功无法转大成功是否由自有邮箱域名导致。

本轮先排除原账户使用的完整域名 **`lake.neuroloom.pp.ua`**，范围仅为 `openai` 业务的新邮箱选择。它是用户要求的疑似域名排除，不是已经证明平台拒绝该域名，也不是把整个 `.pp.ua` 顶级后缀、`neuroloom.pp.ua` 的所有子域或全部邮箱 provider 一并封禁。

没有通过更换用户名、域名或身份连续创建新账户来试探／规避账户停用；没有登录、验证码提交或短信购买。已有账户恢复路径没有扩大禁用范围，现有“5 个不同账户连续停用”的自动域名风险阈值仍为 `5`，未伪造观测或回填计数。

## 已持久配置的排除

当前停止状态的 PC2 `easy-register` 配置中，`REGISTER_MAILBOX_BUSINESS_POLICIES_JSON` 已修改为：

```json
{
  "openai": {
    "domainPool": [
      "clay.yamiyu.pp.ua",
      "leaf.yamiyu.pp.ua",
      "flora.neuroloom.pp.ua"
    ],
    "domainBlacklist": [
      "lake.neuroloom.pp.ua"
    ]
  }
}
```

该完整域名既从候选池移除，也进入显式黑名单。不是只删除一个邮箱地址；更换同域 local part 不会恢复它的候选资格。该规则已配置到下一次获准运行的业务容器，但业务容器没有启动，因此不声称已完成一次新的真实注册／授权验收。

## 保留结果的跨域名核验

先对本机原小成功／continuation 私有结果离线核验，再通过 PC2 原生产镜像的隔离容器读取现有输出。PC2 audit 使用 `--network none`、只读 rootfs、只读业务输出挂载；未执行 `infinite_runner.py`、注册入口或账户接口。

扫描范围为最近 48 小时的既有 `private-result.json` / `result.json`，至多读取 1000 个不超过 2 MB 的文件；同内容文件去重，账户按规范化邮箱的 SHA256 去重，只导出域名和计数。此次没有达到文件上限。严格账户拒绝判定复用现有 `_explicit_account_rejection`，不把泛 403、provider 引用或 challenge 算作账户停用。

| 来源 | 完整邮箱域名 | 不同账户数 | 已观察到的主要结果 |
|---|---|---:|---|
| 本机保留的原样本，2 份结果 | `lake.neuroloom.pp.ua` | 1 | Platform 小成功并完成组织初始化；同一账户后续登录被明确拒绝为已删除或停用 |
| PC2 近期结果 | `clay.yamiyu.pp.ua` | 3 | 1 个 Platform 小成功；另 2 个注册入口 challenge；未取得最终 free/personal 验证 |
| PC2 近期结果 | `leaf.yamiyu.pp.ua` | 1 | 网络／传输失败，未完成 Platform 注册 |

PC2 使用了 4 份有邮箱身份的 DST 结果；1 份没有邮箱身份的文件未用于域名分组，29 份早于 48 小时的结果不在本次窗口。此表来自历史运行记录，不是本轮新发起的 5 次注册测试。

**这些样本没有明确的 `unsupported_email`／“The email you provided is not supported”拒绝。** 原 `lake` 样本曾完成 Platform 注册，`clay` 也曾在同为 `.pp.ua` 的地址上完成该阶段，所以不能说所有 `.pp.ua` 地址都被注册接口直接拒绝。

但这也不能排除域名信誉影响注册后的风控；目前样本极小、失败阶段不同，且运行时间／出口／会话与版本不是受控一致。没有独立正常域名的最终成功对照。因此只能记录关联，**尚未建立域名导致停用的因果结论，也没有证明换域名后大成功已可用**。

## 定向测试与实际加载核验

- 本地 10 项域名策略测试通过。
- PC2 实际生产依赖镜像中，同样 10 项测试在 `--network none`、只读 rootfs 的隔离环境通过：候选池只移除指定域名、显式黑名单、dynamic fallback 不放宽、provider scope 不放宽、provider 返回该域名时最终策略拒绝、大小写、兄弟域保留、业务隔离、原配置不被准备函数修改、阈值仍为 5。
- 历史汇总器 5 项测试通过：同账户多记录不变成多个样本、同内容文件去重、引用句不算直接账户证据、账户／challenge／unsupported 分类分离、完整域名隔离及邮箱地址不导出。
- 配置切换后，从新停止容器读取实际环境，在同镜像、只读真实输出挂载、无网络的独立容器中执行实际 runtime reader：候选池确实为上述三个域名，`lake.neuroloom.pp.ua` 在排除列表中，最终策略返回 `explicit_business_blacklist`。

这些测试证明排除规则和证据汇总有效，不是“域名因果实验”的替代品。第一份隔离历史 audit 因诊断脚本在 stdin 下处理 `__file__` 的错误退出；查到 `IndexError: 2` 后修复脚本并通过 stdin 回归，第二份 audit 才成功。未把该失败计为账户失败或合格验收。

## 配置切换与保留证明

`2026-10-01T17:04:10Z` 完成仅配置的停止容器切换：

```text
new easy-register:
  16db2324eb15a288423a76f07ac6c21a6bb5650e4118af29ffb41fada96cdebd
  state=created, restart=no, startedAt=0001-01-01T00:00:00Z

backup:
  easy-register-before-domain-exclusion-20261001-1705
  4536d3d078e2625e3cb882e1ac960e004893b7ae42a6ff90b139f0f13c71dedb
  state=exited

unchanged image:
  sha256:3adf6292ac8d2a0e604dbd9c6835f6a0c214ebce4e5e1b483426265a218ce38f
```

比对确认只改了 `REGISTER_MAILBOX_BUSINESS_POLICIES_JSON`；镜像、业务命令、working directory、user、labels、数据挂载、端口／资源限制和 network mode 保持。没有将本地 dirty worktree 或上一轮本地 `account_unavailable` 补丁顺带部署。

业务容器未启动；provider 和共享服务未重启；所有本轮隔离 audit/proof 容器结束后均为 exited、restart=no。没有删除容器、镜像、业务数据或认证材料。

`17:06 UTC` 复核：生产 `REGISTER_SMS_ALLOW_PAID=false`；原购买 ledger／receipt 不存在，原 paid005 的委托启动 marker 仍存在且已消费。不得因其原容器还是 created 就重新使用该单次轮次。

## 回滚边界

旧停止容器完整保留，包含原四域名配置。如需恢复，应先确认新旧容器都未运行，将新容器改名为保留名称，再把 `easy-register-before-domain-exclusion-20261001-1705` 改回 `easy-register`，并核验状态与配置；**仅恢复名字和配置，不自动启动业务**。不要删除任何容器或清空输出，不能借回滚重用已消费的 canary／购买 marker。

远端受限的完整创建配置保存在：

```text
/home/mjc/easyregister/releases/mailbox-domain-exclusion-20261001-1705/create-config.private.json
```

它可能含服务认证环境，未下载到普通本地证据或写入本文。

## 后续真实对照的必要条件

本轮已询问用户是否有分别覆盖原域名和另一域名的正常测试账号，只询问有无，不索要密码、OTP 或 token。若继续进行真实对照，应使用明确可用、合法授权的测试资源，单次顺序执行，先匹配时间、版本、正常出口和流程阶段；不得把不可用账户与正常账户的身份差异当作单一域名实验。

已注册账号的正常登录对照只能检验该登录路径，不能替代新注册后的存活性实验。新的账户停用或 challenge 一旦出现应停止并保留该次原因，不通过批量换域／换身份注册或自动解挑战继续碰运气；短信仍保持关闭。

## 本地证据入口

```text
C:/Users/Public/nas_home/AI/GameEditor/linshi/easyregister-domain-exclusion-20261001/
  local-domain-history.json
  pc2-domain-history.json
  pc2-domain-history-v2.json
  local-policy-tests.log
  online-policy-proof.json
  history-aggregation-tests.log
  stopped-config-receipt.json
  effective-policy-readonly.json
```

原转换失败的账户／会话调查见 [小成功续接账户拒绝](small-success-to-full-account-rejection-2026-10-01.md)。本轮没有提交或推送 Git，也没有声称最终大成功。
