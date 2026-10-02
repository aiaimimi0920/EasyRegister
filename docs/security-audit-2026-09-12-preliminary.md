# EasyRegister Python 代码安全审计（初步报告）

> 2026-09-13 部署更新：定点复核候选已完成生产部署和生成服务重启，实际范围及验证见 [部署跟进报告](pc2-review-deployment-2026-09-13.md)。下文保留此前审计时的状态；完整 CI 和真实大成功仍未确认。

> 2026-09-13 复核说明：本报告保留原扫描记录，旧代码片段和行号描述修复前状态。`99211ed` 的部分修复引入了回归，已在当前工作区纠正；未完成全仓安全认证、完整 CI 或生产部署。当前结论以 [复核报告](claude-review-2026-09-13.md) 为准。

**审计日期**: 2026-09-12  
**审计范围**: `server/` 目录下 90 个 Python 文件  
**审计方法**: 静态分析 + 模式匹配 + 人工代码审查

---

## 执行摘要

**原扫描风险分级**: MEDIUM，未经本次全量重评

**原模式匹配记录**: 26 处异常处理、1 个弱 token 检查、1 个无认证 HTTP 端点；匹配数量不能直接视为已确认漏洞数量。

**优势** ✅:
- 原扫描未发现不安全的 SQL 查询构造，覆盖范围有限。
- 原扫描未发现 `shell=True` 或 `os.system` 滥用，未据此证明不存在命令注入。
- 有敏感信息掩码机制（`_redact_sensitive_payload`, `redact_easy_proxy_error`, `mask_account_emails`）
- 配置使用环境变量；本次未重新扫描全部秘密信息来源。

**主要风险** ⚠️:
- 26处静默异常处理可能掩盖关键错误
- 修复前 Dashboard HTTP 服务缺少认证，暴露运行时信息。
- 修复前 token 检查过弱；当前保留最小 16 字符检查。
- EasyProtocol 地址来自运维配置，需核对 URL 和重定向信任边界。

---

## 发现详情

### HIGH - 高危问题

#### 1. Dashboard HTTP 服务无认证
**文件**: `server/services/orchestration_service/src/others/dashboard_http.py:106`  
**描述**: `/api/status` 端点完全开放，无需认证即可访问  
**影响**: 
- 暴露工作进程状态、配置、失败计数
- 暴露 OAuth 池大小和上传统计
- 虽然邮件地址被掩码，但仍泄露系统运行时信息

**当前修复**：`dashboard_http.py` 保护 `/`、`/index.html` 和 `/api/status`，采用恒定时间 token 比较。API 使用 Bearer，浏览器使用 Basic，用户名 `dashboard`、密码为现有 `EASY_PROTOCOL_CONTROL_TOKEN`。认证失败返回 401 和 challenge；页面检查 HTTP 状态，调用脚本同步携带认证。页面不嵌入凭据，响应设置 `Cache-Control: no-store`。

原改动只保护 API，却没有同步浏览器 fetch 和监控调用方，已复现访问回归。真实 Chromium 认证和指标渲染在修复后通过。

#### 2. 弱token验证
**文件**: `server/services/orchestration_service/src/dashboard_server.py:36-40`  
**代码**:
```python
def _control_token_is_secure(token: str) -> bool:
    normalized = str(token or "").strip()
    if not normalized:
        return False
    return normalized not in {"123456"}  # ❌ 只拒绝一个弱密码
```

**影响**: 攻击者可以使用 `"password"`, `"admin"`, `"123"` 等常见弱密码  
**建议**: 
```python
def _control_token_is_secure(token: str) -> bool:
    normalized = str(token or "").strip()
    if len(normalized) < 16:  # 至少16字符
        return False
    # 拒绝常见弱密码列表
    weak_tokens = {"123456", "password", "admin", "test", "secret"}
    if normalized.lower() in weak_tokens:
        return False
    return True
```

---

### MEDIUM - 中危问题

#### 3. 静默异常处理 (26 处)
**位置分布**:
- `runner_process_supervisor.py`: 8 处
- `dst_flow_runtime.py`: 3 处
- `easyprotocol_runtime.py`: 2 处
- `runner_flow_scheduler.py`: 2 处
- 其他文件: 11 处

**模式**:
```python
except Exception:
    pass  # ❌ 错误被完全吞掉
```

**待核实影响**：部分捕获可能隐藏故障，部分属于有意降级；需按实际调用边界逐项区分，不能仅凭 `except` 模式判定漏洞。

**建议**:
```python
except ExpectedOperationError:
    logger.warning("Operation failed at the audited boundary")
    # 按业务语义选择降级或重新抛出；避免记录原始秘密信息。
```

**示例位置**:
- `dst_flow_runtime.py:458` - refresh_retry_state 失败
- `dst_flow_runtime.py:718` - 错误记录失败
- `runner_process_supervisor.py:182` - 获取进程状态失败

#### 4. EasyProtocol 服务地址与重定向边界
**文件**: `dashboard_http.py:239-256`  
**代码**:
```python
def _fetch_easy_protocol_stats(self) -> dict[str, Any]:
    base = self._easy_protocol_base_url.rstrip("/")  # 来自环境变量
    # ...
    url = base + "/api/internal/stats"
    req = urllib.request.Request(url, ...)
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))
```

**复核**：地址由运维配置，EasyProtocol 正常部署可以位于内网或 loopback。未发现普通 Dashboard 请求可以选择该地址。原改动拒绝私有 IP，破坏正常统计请求，同时仍放行域名，不能构成完整的 SSRF 防护。

**当前修复**：允许运维配置的 HTTP/HTTPS 内部服务，拒绝缺少 host、包含 userinfo、query 或 fragment 的地址。`_NoRedirectHandler` 禁止跟随重定向，避免发送控制凭据到另一个目标。真实 loopback HTTP 测试覆盖内部统计读取和重定向拒绝。

#### 5. JWT 元数据解析的信任边界
**文件**: `common_credentials.py:32-48`  
**代码**:
```python
def decode_jwt_payload(token: str) -> dict[str, Any]:
    parts = raw.split(".")
    # ...
    decoded = base64.urlsafe_b64decode((payload + padding).encode("utf-8"))
    claims = json.loads(decoded.decode("utf-8"))  # 仅提取未验证元数据
    return dict(claims)
```

**影响**：该辅助函数不验证签名，不应作为接受任意外部 JWT 的认证接口。现有 claims 规则用于产物语义检查，不能替代密码学认证。

**当前修复**：`99211ed` 添加的 `verify=True` 分支只返回空对象，未执行验签，也没有调用方使用该参数。现已移除该伪接口，并明确函数只提取未验证元数据。本次没有实现 JWKS、签名、issuer、audience 或有效期认证，不将此项标为“JWT 签名验证已完成”。

---

### LOW - 低危问题

#### 6. 默认远程监听风险
**文件**: `dashboard_http.py:76`  
**代码**:
```python
return host or "0.0.0.0", int(port_text)  # 解析失败时默认 0.0.0.0
```

**影响**: 配置错误时意外监听全网  
**当前修复**：保留未指定 host 时回退为 `127.0.0.1`。Compose 显式配置 `0.0.0.0:9790`、remote opt-in 和宿主机端口 19790；容器端与宿主机监听范围需要分别处理。

#### 7. 文件名规范化与碰撞
**文件**: `common_credentials.py:10-17`  
**代码**:
```python
def sanitize_filename_component(value: str, *, fallback: str) -> str:
    for bad in ('<', '>', ':', '"', '/', "\\", "|", "?", "*"):
        text = text.replace(bad, "_")  # 将路径分隔符压平成单个文件名
```

**复核**：原函数还会清理首尾点；路径分隔符已经被替换。禁止任意内部 `..` 会把合法的 `artifact..one`、`artifact..two` 都改为同一个 fallback，产生名称碰撞。

**当前修复**：恢复原有分隔符替换和首尾点清理，保留有效名称内部的点。回归测试验证名称可区分，并验证输出在 Windows/POSIX 下仍为单个文件名组件。

---

## 统计数据

以下为原模式扫描记录，本次未重新执行全目录扫描，未逐项确认这些严重等级或统计口径。

| 指标 | 数值 |
|------|------|
| 扫描文件数 | 90 |
| 代码总行数 | ~25,000 |
| 发现问题总数 | 32 |
| HIGH严重性 | 2 |
| MEDIUM中等 | 5 |
| LOW较低 | 25 (主要是静默异常) |

---

## 下一步行动

### 2026-09-13 已完成的本地修正
1. Dashboard 页面/API 认证、浏览器错误处理和调用脚本适配。
2. 保留最小 16 字符控制 token 检查。
3. 移除伪 JWT 验证接口，准确标注元数据解析语义。
4. 恢复内部 EasyProtocol 统计访问，拒绝重定向。
5. 恢复单文件名规范化并修复内部 `..` 名称碰撞。
6. 保留进程 loopback 回退，纠正 Compose 监听说明。
7. 恢复三个部分手机验证分支的 `ok: False`，避免错误通过最终 OAuth 验证。

相关模块检查合计 117 通过、12 失败、2 错误；失败集中于此前已有失败记录的 account-availability 组。完整 CI 在 240 秒后超时，本次未部署。

### 高优先级 (P1 - 2周内)
8. 静默异常处理 - 本次未逐处复核；根据可复现故障和既有降级语义决定是否修改。

### 中优先级 (P2 - 1个月)
9. 扩展仍未覆盖的安全边界测试。
10. 在 CI/CD 中集成自动化安全扫描（Bandit, Semgrep）。
11. 建立安全编码规范文档。

---

## 审计覆盖范围

**原扫描涉及的范围**：
- HTTP 服务入口点
- 凭证管理和 JWT 处理
- 环境变量使用
- 异常处理模式
- 网络请求（SSRF风险）
- 文件操作（路径遍历）

**未覆盖** ⚠️:
- 完整运行时与生产验证；本次仅补充 Dashboard 的真实 HTTP 和浏览器检查。
- 第三方依赖漏洞扫描
- 配置文件安全性
- 数据库连接（如果有）
- Docker 镜像安全

---

## 附录：工具和方法

- **静态分析**: 正则匹配高危模式
- **代码审查**: 人工审查关键模块
- **工具**: context-mode (ctx_execute, ctx_batch_execute)
- **参考标准**: OWASP Top 10, CWE Top 25

---

**审计人员**: Claude Opus 5  
**审计时长**: ~30分钟  
**复核日期**: 2026-09-13

**备注**: 原报告未附深度审计代理的完成证据；当前已确认发现、局限和运行快照见复核报告。
