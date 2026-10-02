# EasyRegister 部署指南 - 2026-09-12

> 2026-09-13 部署更新：定点候选已在 2026-09-12 18:39:04 UTC 完成部署并重启生成服务，线上浏览器验证已通过。后续状态及付费验证证据见 [部署跟进报告](pc2-review-deployment-2026-09-13.md)。下文的未部署说明保留为此前核验时的历史记录。

> 2026-09-13 复核：本指南已纠正 `99211ed` 的错误成功判定、认证适配和部署说明。当前修复仅存在于本地工作区，尚未部署；实际验证及剩余条件见 [复核报告](claude-review-2026-09-13.md)。

## 本次修复概览

### 复核后的安全相关行为
1. Dashboard 的页面及 `/api/status` 均要求认证；支持 API Bearer 和浏览器 Basic。
2. 保留控制 token 最小 16 字符及弱密码检查。
3. `decode_jwt_payload()` 明确只解析未验证的元数据，未提供签名验证功能。
4. 允许配置的内部 EasyProtocol HTTP/HTTPS 地址，校验 URL 并拒绝重定向。
5. 文件名保留分隔符替换和首尾点清理，修复内部 `..` 导致的名称碰撞。
6. 保留进程的 loopback 回退；Compose 的显式监听配置见下文。

### 业务逻辑修复
- `artifact_pool_claims.py` 的三个 `phone_verification_*_small_success` 分支恢复 `ok: False`。
- 小成功 seed 的提前入池机制保持有效，继续流程仍需满足 seed 有效期和会话等条件。
- 只有完整 OAuth 和 free/personal claims 验证通过，才可计为大成功。本次尚未取得真实大成功。

## 部署步骤

### 1. 确认候选代码和运行基线
```bash
git log -1 --oneline
docker compose -f compose/docker-compose.yaml config --services
```

实际服务名为 `easy-register`。当前工作区有大量既有改动，应定点打包本次修复，记录候选内容和镜像摘要。`99211ed` 含已确认回归，不可直接作为修复完成的部署基线。

### 2. 环境变量更新
核对目标部署实际使用的 env 文件和 Compose override。Dashboard 使用与 EasyProtocol 共享的 `EASY_PROTOCOL_CONTROL_TOKEN`；本项目没有独立的 `REGISTER_DASHBOARD_TOKEN` 配置。

```bash
# Compose 容器内的既有默认值；保留当前匹配的共享凭据
REGISTER_DASHBOARD_ENABLED=true
REGISTER_DASHBOARD_LISTEN=0.0.0.0:9790
REGISTER_DASHBOARD_ALLOW_REMOTE=true
EASY_PROTOCOL_CONTROL_TOKEN=[REDACTED_SECRET]
```

示例中的凭据仅表示脱敏值，不可直接写入运行配置。若轮换共享 token，需同时更新 EasyProtocol、Dashboard 和调用脚本，避免单边变更导致认证失配。

直接运行进程时可监听 `127.0.0.1:9790`。Compose 默认容器监听 `0.0.0.0:9790`，宿主机映射 `${REGISTER_DASHBOARD_PORT_HOST:-19790}:9790`；容器内监听 loopback 会阻断发布端口访问。宿主机访问范围应通过端口绑定或现有反向代理控制。Basic/Bearer 认证本身不加密传输，远程访问沿用受信网络、TLS 或 SSH 隧道。

### 3. 准备定点部署
记录现有镜像摘要、挂载、Compose override、重启次数和任务状态，先验证候选镜像，再面向 `easy-register` 单服务安排切换。保留其他在线服务、生产调度状态和 canary 的原始账本，避免整栈 down/up。

本次未执行生产切换。2026-09-12 16:38:57 UTC 核验时，生产使用复核报告记录的镜像，其三个部分手机验证分支均为 `False`。任何后续执行都需要重新核对当时的运行基线。

### 4. 验证部署

#### 4.1 Dashboard认证验证
```bash
# Compose 默认宿主机端口；实际 override 可能不同
# 匿名访问应返回 401
curl -i http://127.0.0.1:19790/api/status

# curl 会交互读取密码；使用现有 EASY_PROTOCOL_CONTROL_TOKEN
# 有效凭据应返回 200 和状态 JSON
curl --user dashboard http://127.0.0.1:19790/api/status
```

浏览器打开 `/` 或 `/index.html`，Basic 用户名为 `dashboard`，密码为同一共享 token。API 和监控脚本可发送 Bearer 头；凭据不应写入 URL、HTML 或日志。真实浏览器还需确认 `/api/status` 成功且指标正常渲染。

#### 4.2 小成功池化验证
核对 seed 创建、Platform/ChatGPT 初始化里程碑、池中记录和正常 900 秒有效期。三个部分手机验证状态必须保留失败判定；不得用 `ok: True` 替代最终 claims 验证。使用脱敏审计脚本检查实际任务及产物：

```bash
python scripts/audit-pc2-restart-results.py --since 2026-09-12T14:30:00Z
```

该命令只读 PC2；时间参数应按核验窗口调整。原 paid canary 仅允许一次购号尝试，服务 `dr`、国家 `16`、上限 0.05 USD。保留原 guard，先提供新鲜有效 seed，再在隔离的受限任务中验证短信步骤；保持生产全局付费开关关闭。

#### 4.3 运行测试套件
```bash
# 与本次修复直接相关的模块
python -m unittest discover -s tests -p "test_artifact_pool_modules.py" -v
python -m unittest discover -s tests -p "test_dashboard*.py" -v
python -m unittest discover -s tests -p "test_common_credentials_security.py" -v
python -m unittest discover -s tests -p "test_security_defaults.py" -v

# CI 使用的完整命令；本次执行在 240 秒后超时，未取得全量结果
python -m unittest discover -s tests -v
```

本次所查模块合计 117 通过、12 失败、2 错误；14 个未通过项均位于此前已有失败记录的 account-availability 审计组。不要将原报告的 73 个选择性测试作为完整发布门禁。

## 预期效果

### 安全改进
- Dashboard端点不再暴露未认证访问
- 弱token (如"password", "admin", "123456") 被拒绝
- 进程的远程监听需要 opt-in；当前 Compose 默认显式开启该选项。
- 内部统计地址可用，服务端重定向不能转发控制凭据。

### 业务改进
- 小成功继续保留在池中，最终验证拒绝尚未完成的手机验证结果。
- 2026-09-12 16:30:06 UTC 快照的 8 个真实小成功中，5 个停在禁用的 SMS 业务策略，3 个发生 OAuth 超时。
- 实际短信接码、完整 OAuth 和付费转换成功率仍需受限运行证据。

## 回滚计划

若候选验证失败，停止切换并保留现有镜像。已切换后的回滚应复用预先记录的镜像摘要、env 和 override，仅切回受影响服务；保留产物、任务记录和 guard 账本。对混有既有改动的工作区，整提交 revert 和整栈重启会扩大影响范围。

## 监控要点

### 1. Dashboard访问日志
- 401响应 → 预期(未认证请求被拒绝)
- 200响应 + 有效token → 正常
- 大量 401 → 先确认浏览器和监控脚本已携带正确认证，再检查异常来源。

### 2. OAuth流程指标
- 分别统计小成功、仍在有效期内的 seed、SMS 策略拒绝、OAuth 超时和最终大成功。
- 入池或 HTTP 200 均不能单独证明最终大成功；需核对身份匹配的产物及付费账目。

### 3. 错误率
- Dashboard启动失败 → 检查token是否≥16字符且不在弱密码黑名单
- OAuth流程卡住 → 检查 seed 有效期、会话初始化、SMS 策略及实际失败步骤。

## 联系和支持

- 审计报告: `docs/security-audit-2026-09-12-preliminary.md`
- 技术问题: 对照 `99211ed` 和本地复核修复 diff。
- 实际测试及 PC2 证据: `docs/claude-review-2026-09-13.md`。

---

**部署检查清单**:
- [ ] 候选包含本地回归修复，并记录来源和镜像摘要。
- [ ] 共享 token 与调用方匹配，保留正确的容器监听及宿主机端口配置。
- [ ] Dashboard 的匿名拒绝、有效认证和真实浏览器渲染均通过。
- [ ] 相关测试通过；完整 CI 未通过或未完成的部分如实记录。
- [ ] 生产切换、回滚及单次购号边界明确，原 guard 账本保留。
- [ ] 大成功以完整 OAuth、最终 claims、产物和费用核对为验收条件。
