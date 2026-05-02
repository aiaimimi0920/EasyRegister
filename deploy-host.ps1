param(
    [string]$OutputDirHost = "",
    [string]$ComposeFile = "",
    [string]$TeamAuthDirHost = "C:\Users\vmjcv\.cli-proxy-api\team",
    [string]$CodexFreeDirHost = "C:\Users\vmjcv\.cli-proxy-api",
    [string]$CodexTeamDirHost = "C:\Users\vmjcv\.cli-proxy-api\team",
    [string]$CodexTeamInputDirHost = "C:\Users\vmjcv\.cli-proxy-api\team",
    [string]$CodexTeamMotherInputDirHost = "",
    [string]$DashboardPortHost = "19790",
    [ValidateSet("Auto", "Junction", "SymbolicLink")]
    [string]$LinkType = "Auto",
    [switch]$ForceLinks,
    [switch]$NoBuild,
    [switch]$NoDetach,
    [switch]$MaterializeOnly,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Services
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-AbsolutePath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [string]$BaseDir
    )

    if ([System.IO.Path]::IsPathRooted($Path)) {
        return [System.IO.Path]::GetFullPath($Path)
    }
    return [System.IO.Path]::GetFullPath((Join-Path $BaseDir $Path))
}

$repoRoot = Split-Path -Parent $PSCommandPath
$resolvedOutputDirHost = if ([string]::IsNullOrWhiteSpace($OutputDirHost)) {
    Resolve-AbsolutePath -Path "runtime\register-output" -BaseDir $repoRoot
} else {
    Resolve-AbsolutePath -Path $OutputDirHost -BaseDir $repoRoot
}
$resolvedComposeFile = if ([string]::IsNullOrWhiteSpace($ComposeFile)) {
    Resolve-AbsolutePath -Path "compose\docker-compose.yaml" -BaseDir $repoRoot
} else {
    Resolve-AbsolutePath -Path $ComposeFile -BaseDir $repoRoot
}

$env:REGISTER_OUTPUT_DIR_HOST = $resolvedOutputDirHost
$env:REGISTER_TEAM_AUTH_DIR_HOST = $TeamAuthDirHost
$env:REGISTER_DASHBOARD_PORT_HOST = $DashboardPortHost

if (-not [string]::IsNullOrWhiteSpace($CodexFreeDirHost)) {
    $env:REGISTER_CODEX_FREE_DIR_HOST = $CodexFreeDirHost
}
if (-not [string]::IsNullOrWhiteSpace($CodexTeamDirHost)) {
    $env:REGISTER_CODEX_TEAM_DIR_HOST = $CodexTeamDirHost
}
if (-not [string]::IsNullOrWhiteSpace($CodexTeamInputDirHost)) {
    $env:REGISTER_CODEX_TEAM_INPUT_DIR_HOST = $CodexTeamInputDirHost
}
if (-not [string]::IsNullOrWhiteSpace($CodexTeamMotherInputDirHost)) {
    $env:REGISTER_CODEX_TEAM_MOTHER_INPUT_DIR_HOST = $CodexTeamMotherInputDirHost
}

$materializeArgs = @(
    "-ExecutionPolicy", "Bypass",
    "-File", (Join-Path $repoRoot "scripts\materialize-output-links.ps1"),
    "-OutputDirHost", $resolvedOutputDirHost,
    "-PathBaseDir", $repoRoot,
    "-LinkType", $LinkType
)
if ($ForceLinks) {
    $materializeArgs += "-Force"
}

& powershell @materializeArgs

if ($MaterializeOnly) {
    return
}

$deployArgs = @(
    "-ExecutionPolicy", "Bypass",
    "-File", (Join-Path $repoRoot "scripts\deploy-compose.ps1"),
    "-ComposeFile", $resolvedComposeFile,
    "-OutputDirHost", $resolvedOutputDirHost,
    "-LinkType", $LinkType
)
if ($ForceLinks) {
    $deployArgs += "-ForceLinks"
}
if (-not $NoBuild) {
    $deployArgs += "-Build"
}
if ($NoDetach) {
    $deployArgs += "-NoDetach"
}
if ($Services) {
    $deployArgs += $Services
}

& powershell @deployArgs
