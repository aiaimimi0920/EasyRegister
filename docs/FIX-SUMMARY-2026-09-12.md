# 修复记录与复核 - 2026-09-12

> 2026-09-13 部署更新：复核候选已部署，生成服务已重启，线上浏览器验证已通过。付费认证尚未启动，本轮短信支出为 0 USD；运行及费用证据见 [部署跟进报告](pc2-review-deployment-2026-09-13.md)。此前的未部署说明保留为历史记录。

> 2026-09-13 复核：`99211ed` 包含成功判定和 Dashboard 回归，已在当前工作区定点纠正，尚未部署。完整发现和验证结果见 [复核报告](claude-review-2026-09-13.md)。本页的历史测试记录不代表完整 CI 或真实大成功已经通过。

## 任务完成状态

### 任务1: 安全相关改动复核
本次确认并修复了以下具体问题，未完成全仓安全审计：

1. **Dashboard HTTP认证** (HIGH)
   - `dashboard_http.py` 的 `/`、`/index.html`、`/api/status` 要求认证，未认证请求返回 401。
   - API 支持 Bearer；浏览器支持 Basic，用户名 `dashboard`，密码使用现有 `EASY_PROTOCOL_CONTROL_TOKEN`。
   - 页面检查 HTTP 错误，避免把认证失败显示成空指标；审计脚本同步携带认证头。
   
2. **Token验证强化** (HIGH)
   - `dashboard_server.py:36-43` - 最小16字符
   - 弱密码黑名单: "123456", "password", "admin", "test", "secret", "token", "default", "changeme"
   
3. **JWT 元数据解析语义纠正**
   - 删除了不执行验签、只返回空对象的 `verify` 参数。
   - `decode_jwt_payload()` 仅提取未验证的元数据；当前实现未提供 JWT 签名验证。
   
4. **EasyProtocol 内部统计请求修复**
   - 允许运维配置的内网和 loopback HTTP/HTTPS 服务地址，校验 URL 结构。
   - 禁止跟随重定向，避免控制凭据被发送到重定向目标。
   
5. **文件名碰撞修复**
   - 保留路径分隔符替换及首尾点清理。
   - 允许名称内部的 `..`，避免 `artifact..one`、`artifact..two` 都变成同一个 fallback 文件名。
   
6. **默认监听地址** (LOW)
   - 保留进程在未指定 host 时使用 `127.0.0.1` 的回退。
   - Compose 显式使用 `0.0.0.0:9790` 并启用 remote opt-in；容器内改为 loopback 会影响发布端口访问。

**本次相关测试**：安全默认值 8/8、Dashboard 9/9、凭据辅助函数 2/2 通过；真实 Chromium 认证和指标渲染通过。完整结果见复核报告。

---

### 任务2: 纠正小成功与最终 OAuth 成功判定

**已确认的回归**：
- `99211ed` 将三个部分手机验证状态改成 `ok: True`，导致缺少 free/personal claims 的结果通过最终验证。
- 小成功在生成时已经进入池；现有集成测试明确验证了最终结果失败时，seed 仍然保留在池中。

**修复内容**：恢复三个分支的 `ok: False`，保留状态和失败详情。最终成功仍要求真实 free/personal OAuth claims。

**影响的状态**:
1. `phone_verification_terminal_small_success` - 手机号达到终止条件
2. `phone_verification_submitted_small_success` - 已提交手机验证步骤，但尚未确认完整 OAuth
3. `phone_verification_attempted_small_success` - 已尝试手机验证，但尚未确认完整 OAuth

**业务核验**：
- 2026-09-12 16:30:06 UTC 的 PC2 快照中，34 个完成任务产生 8 个真实小成功、0 个大成功。
- 其中 5 个小成功任务报 `sms_not_enabled_for_business`，另 3 个在 OAuth 步骤超时。
- 这支持继续验证 EasySMS 路径；实际付费转换成功率仍未测得。继续流程还需要有效期内的 seed、可用会话、完整 OAuth 和最终验证。

**测试验证**：三个已有部分手机验证回归测试在修复前失败、修复后通过；`test_run_dst_flow_once_collects_openai_pool_as_soon_as_small_success_is_created` 在恢复 `False` 后仍通过。

---

### 任务3: 部署范围与状态
本次执行了本地修复和 PC2 只读核验，未部署、未注册账号、未调用付费 SMS API。生产变更以当前用户指令和既有执行边界为准。原有单次购号 guard、0.05 USD 上限及账本必须保留。

**部署文档已创建**: `docs/DEPLOYMENT-2026-09-12.md`

---

## Commits

1. **99211ed** - `fix(security): address HIGH/MEDIUM security findings + fix small_success artifact pooling`
   - 7 files changed, 360 insertions(+), 37 deletions(-)
   
2. **2bdfe87** - `docs: add deployment guide for security fixes and small_success pooling`
   - 1 file changed, 146 insertions(+)

---

## 原记录中的选择性测试

以下 73/73 为原报告记录，本次未重新运行该组合。该选择没有覆盖已复现的三个验证器回归和 Dashboard 浏览器认证问题。

| 测试套件 | 状态 | 通过/总数 |
|---------|------|----------|
| test_security_defaults.py | ✅ | 8/8 |
| test_dst_flow_integration.py (small_success) | ✅ | 1/1 |
| test_easyproxy_flow.py | ✅ | 5/5 |
| test_runtime_mailbox_provider_failover.py | ✅ | 27/27 |
| test_runtime_proxy_acquire.py | ✅ | 20/20 |
| test_runtime_proxy_probe.py | ✅ | 12/12 |
| **原记录合计** | **历史记录** | **73/73** |

本次检查的模块合计 117 通过、12 失败、2 错误。14 个未通过项集中于此前已有失败记录的 account-availability 审计组。完整 unittest discovery 在 240 秒后超时，未获得完整 CI 结果。

---

## 下一步建议

### 发布前核对
部署说明见 [DEPLOYMENT-2026-09-12.md](DEPLOYMENT-2026-09-12.md)。实际 Compose 服务名为 `easy-register`。先记录现有镜像、挂载、Compose override 和运行状态，再准备定点候选版本；避免对整个在线栈执行 down 或直接部署 `99211ed`。

Dashboard 使用与 EasyProtocol 共享的 `EASY_PROTOCOL_CONTROL_TOKEN`，没有独立的 `REGISTER_DASHBOARD_TOKEN`。保留现有匹配凭据；轮换时需要协调所有使用方。Compose 默认宿主机端口为 19790，容器端口为 9790。

### 监控要点
1. **Dashboard认证**: 观察401/200响应比例
2. **OAuth流程**: 分别记录 seed 里程碑、有效期、短信策略拒绝和 OAuth 超时。
3. **大成功率**: 仅统计完整 OAuth、最终 claims 验证和有效产物；付费样本还需完成订单及费用核对。

### 后续任务 (非紧急)
- P2: 添加自动化安全扫描到CI/CD (Bandit, Semgrep)
- P2: 建立安全编码规范文档
- P2: 第三方依赖漏洞扫描

---

## 问题排查

如遇到问题:

1. **Dashboard启动失败**
   - 检查现有 `EASY_PROTOCOL_CONTROL_TOKEN` 长度 ≥16，且与 EasyProtocol 匹配。
   - 检查token不在弱密码黑名单
   
2. **OAuth流程仍卡在小成功**
   - 检查有效 seed、实际 SMS 业务策略和 OAuth 失败边界。
   - 保持三个部分手机验证分支为 `ok: False`；单独核对 seed 是否已经入池。
   
3. **401错误过多**
   - 浏览器使用 Basic 登录，用户名 `dashboard`；API 客户端发送 Bearer 认证头。
   - 先核对调用方是否适配认证，再根据访问记录判断异常流量。

---

**原记录时间**: 2026-09-12；**复核时间**: 2026-09-13

**修复范围**: 五类已确认回归及相关调用方、文档

**当前状态**: 本地修复已验证；完整 CI 和真实大成功验证仍未完成，修复尚未部署。
