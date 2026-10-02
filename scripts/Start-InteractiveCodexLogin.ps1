[CmdletBinding()]
param(
    [string]$ExpectedEmail,
    [string]$SessionRoot = 'C:\Users\Public\nas_home\AI\GameEditor\linshi',
    [switch]$PrepareOnly
)

$ErrorActionPreference = 'Stop'
$utf8 = [System.Text.UTF8Encoding]::new($false)
$OutputEncoding = $utf8
[Console]::OutputEncoding = $utf8
$helper = Join-Path $PSScriptRoot 'export-interactive-codex-login.py'
if ($PSScriptRoot.StartsWith('\\') -or $SessionRoot.StartsWith('\\')) {
    throw '请从本地路径或已有 Z: 映射盘运行，不使用 UNC 工作目录。'
}
foreach ($command in @('rtk', 'python', 'codex')) {
    if (-not (Get-Command $command -ErrorAction SilentlyContinue)) {
        throw "缺少必要命令：$command"
    }
}
if (-not $ExpectedEmail) {
    $ExpectedEmail = Read-Host '请输入本次预期登录的邮箱（用于阻止浏览器误选其他账号）'
}
$preparedJson = & rtk proxy python $helper prepare --root $SessionRoot --expected-email $ExpectedEmail
if ($LASTEXITCODE -ne 0) { throw "无法准备私有登录目录：$preparedJson" }
$prepared = ($preparedJson -join "`n") | ConvertFrom-Json
Write-Host "隔离会话：$($prepared.sessionDirectory)"
Write-Host '不会读取或覆盖现有 Codex 认证，不会购买短信、启动注册或写入生产池。'
if ($PrepareOnly) {
    Write-Host '准备完成。PrepareOnly 未启动登录，也未读取任何现有凭据。'
    return
}

# 只影响当前脚本和子进程，退出时恢复，不修改用户/系统环境。
$isolatedVariables = @('CODEX_HOME', 'OPENAI_API_KEY', 'OPENAI_BASE_URL', 'CODEX_ACCESS_TOKEN',
    'CHATGPT_BASE_URL', 'CODEX_API_KEY')
$original = @{}
foreach ($name in $isolatedVariables) {
    $original[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
    [Environment]::SetEnvironmentVariable($name, $null, 'Process')
}
[Environment]::SetEnvironmentVariable('CODEX_HOME', $prepared.codexHome, 'Process')
$pushed = $false
try {
    Push-Location -LiteralPath $prepared.sessionDirectory
    $pushed = $true
    Write-Host '接下来由官方 Codex CLI 打开正常浏览器。请自行完成登录、安全验证与授权。'
    Write-Host '如无法通过或选择了错误账号，请取消；本入口不会自动重试。会话有效期为 15 分钟。'
    & rtk proxy codex login
    if ($LASTEXITCODE -ne 0) { throw '官方登录未成功，未导出凭据；私有目录已保留，不自动重试。' }
    & rtk proxy codex login status
    if ($LASTEXITCODE -ne 0) { throw '官方 CLI 未确认本地登录状态，未导出凭据。' }
    $resultJson = & rtk proxy python $helper stage --session $prepared.sessionDirectory
    if ($LASTEXITCODE -ne 0) { throw "授权缓存未通过本地检查，未写入生产池：$resultJson" }
    $result = ($resultJson -join "`n") | ConvertFrom-Json
    Write-Host "隔离产物：$($result.stagingDirectory)"
    Write-Host "状态：$($result.status)"
    Write-Host '仅完成本地结构/一致性检查，未验签或在线验证；这不是注册大成功。'
    Write-Host '不要把 auth.json 或 credential.private.json 粘贴到聊天、工单或 Git。'
} finally {
    if ($pushed) { Pop-Location }
    foreach ($name in $isolatedVariables) {
        [Environment]::SetEnvironmentVariable($name, $original[$name], 'Process')
    }
}
