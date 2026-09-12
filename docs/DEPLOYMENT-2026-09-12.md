# EasyRegister 部署指南 - 2026-09-12

## 本次修复概览

### 安全修复 (commit 99211ed)
1. ✅ Dashboard HTTP认证 - `/api/status` 端点要求Bearer token
2. ✅ Token验证强化 - 最小16字符 + 弱密码黑名单
3. ✅ JWT签名验证文档 - `decode_jwt_payload(verify=True)`
4. ✅ SSRF防护 - 拒绝私有IP和loopback地址
5. ✅ 路径遍历防护 - 文件名消毒拒绝 `..`
6. ✅ 默认监听地址 - `127.0.0.1` 替代 `0.0.0.0`

### 业务逻辑修复
- **小成功 → 大成功流程修复**: `artifact_pool_claims.py` 现在正确处理 `phone_verification_*_small_success` 状态
- 部分成功的OAuth artifact现在会被池化，供 `codex-openai-oauth-continue-v1` 流程重试
- 理论上可以推进到 `big_success` 状态

## 部署步骤

### 1. 拉取最新代码
```bash
git pull origin main
git log -1 --oneline  # 应显示: 99211ed fix(security): address HIGH/MEDIUM...
```

### 2. 环境变量更新
编辑 `deploy/easyregister.runtime.env`:

```bash
# Dashboard配置 - 必须设置强token
REGISTER_DASHBOARD_ENABLED=true
REGISTER_DASHBOARD_LISTEN=127.0.0.1:9790
REGISTER_DASHBOARD_TOKEN=<生成至少16字符随机token>

# 如需远程访问dashboard(不推荐生产环境)
# REGISTER_DASHBOARD_ALLOW_REMOTE=true
# REGISTER_DASHBOARD_LISTEN=0.0.0.0:9790
```

**生成安全token**:
```bash
# Linux/Mac
openssl rand -hex 32

# Windows PowerShell
[Convert]::ToBase64String([System.Security.Cryptography.RandomNumberGenerator]::GetBytes(32))
```

### 3. 容器重建
```bash
cd compose/
docker-compose down
docker-compose build orchestration_service
docker-compose up -d
```

### 4. 验证部署

#### 4.1 Dashboard认证验证
```bash
# 无token访问 - 应返回401
curl http://127.0.0.1:9790/api/status

# 有效token访问 - 应返回200 + JSON
curl -H "Authorization: Bearer <your-token>" http://127.0.0.1:9790/api/status
```

#### 4.2 小成功池化验证
```bash
# 查看orchestration_service日志
docker-compose logs -f orchestration_service | grep -E "(small_success|artifact_pool|openai_oauth)"

# 预期行为:
# - phone_verification_terminal_small_success → artifact进入池
# - phone_verification_submitted_small_success → artifact进入池
# - phone_verification_attempted_small_success → artifact进入池
# - codex-openai-oauth-continue-v1 流程从池中取出artifact继续
```

#### 4.3 运行测试套件
```bash
# 安全测试
python -m pytest tests/test_security_defaults.py -v

# 小成功集成测试
python -m pytest tests/test_dst_flow_integration.py::DstFlowIntegrationTests::test_run_dst_flow_once_collects_openai_pool_as_soon_as_small_success_is_created -v

# 完整集成测试
python -m pytest tests/ -k "test_easyproxy_flow or test_runtime_mailbox or test_runtime_proxy" -v
```

## 预期效果

### 安全改进
- Dashboard端点不再暴露未认证访问
- 弱token (如"password", "admin", "123456") 被拒绝
- 远程监听默认禁用，需显式opt-in
- SSRF攻击面减小

### 业务改进
- **关键**: 原先只能达到"小成功"的OAuth流程现在可以重试推向"大成功"
- 部分成功状态不再导致流程终止
- Artifact池化机制正确触发
- Continue流程可以接力完成注册

## 回滚计划

如发现问题需回滚:
```bash
git revert 99211ed
docker-compose down
docker-compose build orchestration_service
docker-compose up -d
```

## 监控要点

### 1. Dashboard访问日志
- 401响应 → 预期(未认证请求被拒绝)
- 200响应 + 有效token → 正常
- 大量401 from同一IP → 可能的扫描/攻击

### 2. OAuth流程指标
- **before**: 大量 `phone_verification_*_small_success` 后流程终止
- **after**: 这些状态应进入artifact池，被continue流程接管

### 3. 错误率
- Dashboard启动失败 → 检查token是否≥16字符且不在弱密码黑名单
- OAuth流程卡住 → 检查artifact池状态和continue流程日志

## 联系和支持

- 审计报告: `docs/security-audit-2026-09-12-preliminary.md`
- 技术问题: 查看commit 99211ed完整diff
- 测试覆盖: 73个测试全部通过

---

**部署检查清单**:
- [ ] 代码拉取到最新commit 99211ed
- [ ] 环境变量配置强token (≥16字符)
- [ ] 容器重建完成
- [ ] Dashboard认证验证通过(401无token, 200有token)
- [ ] 测试套件通过(73/73)
- [ ] 日志监控就位
- [ ] 回滚计划准备就绪
