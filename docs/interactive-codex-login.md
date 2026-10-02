# 本机交互式 Codex 登录入口

## 用途和边界

此入口用于将必须由用户完成的登录和安全验证交给本机正常浏览器，通过已安装的官方 Codex CLI 获取授权缓存，再转换为 EasyRegister 现有凭据格式的**隔离待验证产物**。

它不是自动注册器，不恢复之前已结束的注册事务，不调用 EasySms，不修改 PC2 服务，不自动入池、上传 R2 或启动下游任务。无需在 PC2 上新建 VNC 或开放浏览器调试端口，也不抽取用户现有浏览器 cookie。

这是接续 [登录入口网络诊断](auth-entry-network-and-challenge-2026-10-01.md) 的第一个可操作入口。它提供真实可运行的本机登录启动器，但本轮只完成了合成登录的端到端检查，尚未由用户完成真实登录；不能据此宣称已取得有效账号、fresh seed 或注册大成功。

## 使用

在当前 Windows 机器直接双击：

```text
C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\scripts\Start-InteractiveCodexLogin.cmd
```

或在 PowerShell 中执行：

```powershell
& 'C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\scripts\Start-InteractiveCodexLogin.ps1'
```

入口会询问**本次预期登录的邮箱**，用于阻止浏览器误选其他已登录账号。也可显式传入 `-ExpectedEmail '你的邮箱'`。邮箱不是密码；不要把密码、OTP 或 token 当成此参数。

随后由官方 `codex login` 打开正常浏览器，用户自行完成登录、安全验证和授权。无法通过或选错账号时取消即可，本入口不会自动重试。会话有效期为 15 分钟；超过有效期不会导出，需要退出后重新启动新会话。该期限是本地验收期限，不保证外部登录窗口会自动关闭。

前置要求是本机已有 `rtk`、`python`（3.11+）和官方 `codex`。本轮核对版本为 `codex-cli 0.155.1`。不自动安装或升级任何工具。

## 隔离和保存规则

- 每次在 `C:\Users\Public\nas_home\AI\GameEditor\linshi\interactive-codex-<随机ID>` 创建全新的目录；可通过 `-SessionRoot` 指定另一个已存在的本地目录。
- Windows 目录 DACL 仅允许当前用户和 SYSTEM，落实权限后才开始登录或写凭据。失败时不以放宽权限作为 fallback。
- 独立 `CODEX_HOME` 仅配置 `cli_auth_credentials_store="file"` 和 `forced_login_method="chatgpt"`，不复制主 HOME 的配置、认证或 provider 路由。
- 子进程清除当前进程中的 API key、base URL 和 access-token 入口环境变量；脚本结束后恢复原环境和工作目录。主 HOME 的 `auth.json` 不读、不写。
- 不覆盖历史会话、不修改原 CLI `auth.json`，同一会话不能重复生成 staging 产物。
- 失败和取消后保留私有目录以供用户排查，不删除或“重置”旧认证文件。
- 不从 UNC 工作目录启动。使用当前本地路径；已有映射盘在解析后确认为本地路径时才允许继续，不创建临时盘符。

成功完成本地检查后会显示：

```text
interactive-codex-<随机ID>/
  request.private.json
  codex-home/
    config.toml
    auth.json
    ...官方 CLI 自身日志和状态...
  staged/
    credential.private.json
    receipt.public.json
```

`auth.json`、`credential.private.json` 和 CLI 日志都必须按秘密处理，不要粘贴到聊天、工单、截图或 Git。仅 `receipt.public.json` 是本入口生成的脱敏回执；官方 CLI 日志不属于脱敏回执。

## 本地检查与真实验证必须区分

内部辅助入口调用 `others.interactive_codex_login`，复用现有 `standardize_export_credential_payload` 和 `has_free_personal_oauth_claims`，不另写注册或授权协议。

当前检查包括：

- 必须为 `chatgpt` 登录，不能把 API key 登录误当成账号 OAuth。
- 必须包含 ID/access/refresh token 和 account ID。
- ID/access token 的 issuer、有效期、颁发时间、主体、账号及可用 client/audience 信息必须一致；目标邮箱和可用 profile 邮箱必须匹配。
- 保留官方缓存的 `last_refresh`，不伪造刷新时间；只接受本次新会话产生的缓存。
- 复用现有 free/personal 判定，不将 Plus、Team 或缺少 Personal claims 的结果冒充 free 产物。
- 拒绝过期会话、重复 staging、重复 JSON key、非 JSON 和不合要求的路径。

**以上 JWT 读取只是未验签的声明检查，不等于密码学验签、账号在线可用或任务完成。** 即使满足 free/personal 条件，回执仍为：

```json
{
  "status": "awaiting_upstream_validation",
  "claimChecksOnly": true,
  "signatureVerified": false,
  "upstreamVerified": false,
  "productionPoolWritten": false,
  "registrationResumed": false
}
```

不满足 free/personal 条件时，状态为 `free_personal_claims_missing`；仍不进入生产池。这避免把官方登录成功、token 文件存在或未验签的 JWT 字段直接等同于“大成功”。

下一段应在用户实际完成本机官方登录后，对该次隔离产物做受控的在线可用性验证并确定正确接入目的地；不能将本地结构检查当成生产放行凭证。本入口没有实现这一段，也没有改变既有 `validate_free_personal_oauth` 或付费保护。

## 本轮验证

测试和证据位于：

```text
C:/Users/Public/nas_home/AI/GameEditor/linshi/easyregister-interactive-auth-20261001/
```

- `test_interactive_login.py`：7 项测试通过，含 17 种错误/身份不一致拒绝情况和 3 种不满足 free/personal 的情况。
- `test_launcher.ps1` + `fake_codex.py`：用明确标注的合成 CLI 替身执行完整 PowerShell 启动器，成功和失败两条路径均通过。没有网络请求、真实账号登录或短信购买。
- 原生 Windows ACL 已实际核对；成功/失败后环境和目录均恢复，原认证哨兵文件哈希不变，失败不导出、不重试。
- 4 项相邻既有测试通过：标准凭据格式，以及电话验证被拒绝、仅提交或仅尝试都不能算作 free/personal 成功。
- 真实 `.ps1` 与 `.cmd` 的 `PrepareOnly` 路径均通过；PowerShell 解析和 Python 编译检查通过。当前 Python 未安装 Ruff，未将 Ruff 检查记为通过。
- 第一次 ACL 实现使用全新 security descriptor 的 `Set-Acl`，遇到 `SeSecurityPrivilege` 错误；已改为在新建目录上读取原 ACL、仅修改 DACL 并用 .NET 写回，复测通过。没有请求管理员权限或放宽目录权限。

可只验证真实本机准备链而不启动登录：

```powershell
& '.\scripts\Start-InteractiveCodexLogin.ps1' -ExpectedEmail 'operator@example.test' -PrepareOnly
```

此命令会创建一个不含真实认证的私有目录，不会读取主 HOME 凭据，也不会打开浏览器。

## 官方依据

本轮已联网读取官方文档；`developers.openai.com/codex/auth` 重定向至 [Authentication](https://learn.chatgpt.com/docs/auth)，最终 HTTP 200。文档确认 `codex login`、独立 `CODEX_HOME` 下的 file credential storage，以及无头环境可使用的 device-code 登录路径。

本入口选择当前 Windows 的正常浏览器登录，没有自动启用 device-code 设置。官方文档允许将 Codex 授权缓存复制到另一台自用 Codex 机器，不等于承诺任意第三方服务、所有账号套餐或本项目原注册流程均被官方支持；后续接入仍需按实际服务契约验证。
