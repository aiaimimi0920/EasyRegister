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
    [string]$RepoOwner = "aiaimimi0920",
    [string]$RepoName = "EasyRegister",
    [string]$RepoRef = "main",
    [ValidateSet("branch", "tag")]
    [string]$RepoRefKind = "branch",
    [string]$RepoArchiveUrl = "",
    [string]$RepoCacheRoot = "",
    [switch]$ForceRefreshRepo,
    [switch]$ResolveRepoOnly,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Services
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$defaultEasyProxyBaseUrl = "http://easy-proxy-service:29888"
$defaultDashboardControlToken = "easyregister-dashboard-local-token"

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

function Test-RepoLayout {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Root,
        [Parameter(Mandatory = $true)]
        [string[]]$RequiredRelativePaths
    )

    foreach ($relativePath in $RequiredRelativePaths) {
        if (-not (Test-Path -LiteralPath (Join-Path $Root $relativePath))) {
            return $false
        }
    }
    return $true
}

function Get-RepoArchiveUrlValue {
    param(
        [string]$Owner,
        [string]$Name,
        [string]$Ref,
        [string]$Kind,
        [string]$ExplicitUrl
    )

    if (-not [string]::IsNullOrWhiteSpace($ExplicitUrl)) {
        return $ExplicitUrl
    }
    if ($Kind -eq "tag") {
        return "https://codeload.github.com/$Owner/$Name/zip/refs/tags/$Ref"
    }
    return "https://codeload.github.com/$Owner/$Name/zip/refs/heads/$Ref"
}

function Ensure-RepoRoot {
    param(
        [Parameter(Mandatory = $true)]
        [string]$LauncherRoot,
        [Parameter(Mandatory = $true)]
        [string]$Owner,
        [Parameter(Mandatory = $true)]
        [string]$Name,
        [Parameter(Mandatory = $true)]
        [string]$Ref,
        [Parameter(Mandatory = $true)]
        [string]$RefKind,
        [Parameter(Mandatory = $true)]
        [string[]]$RequiredRelativePaths,
        [string]$ArchiveUrl = "",
        [string]$CacheRoot = "",
        [switch]$ForceRefresh
    )

    if (Test-RepoLayout -Root $LauncherRoot -RequiredRelativePaths $RequiredRelativePaths) {
        return [pscustomobject]@{
            RepoRoot = $LauncherRoot
            Source = "local"
            ArchiveUrl = $null
        }
    }

    $sanitizedRef = ($Ref -replace '[^A-Za-z0-9._-]', '_')
    $resolvedCacheRoot = if ([string]::IsNullOrWhiteSpace($CacheRoot)) {
        Join-Path $LauncherRoot ".repo-cache\$Name-$RefKind-$sanitizedRef"
    } else {
        Resolve-AbsolutePath -Path $CacheRoot -BaseDir $LauncherRoot
    }
    $archiveUrlValue = Get-RepoArchiveUrlValue -Owner $Owner -Name $Name -Ref $Ref -Kind $RefKind -ExplicitUrl $ArchiveUrl
    $repoRoot = Join-Path $resolvedCacheRoot "repo"

    if ($ForceRefresh -and (Test-Path -LiteralPath $resolvedCacheRoot)) {
        Remove-Item -LiteralPath $resolvedCacheRoot -Recurse -Force
    }

    if (-not (Test-RepoLayout -Root $repoRoot -RequiredRelativePaths $RequiredRelativePaths)) {
        New-Item -ItemType Directory -Force -Path $resolvedCacheRoot | Out-Null
        $archivePath = Join-Path $resolvedCacheRoot "$Name-$sanitizedRef.zip"
        $expandedRoot = Join-Path $resolvedCacheRoot "expanded"

        if (Test-Path -LiteralPath $archivePath) {
            Remove-Item -LiteralPath $archivePath -Force
        }
        if (Test-Path -LiteralPath $expandedRoot) {
            Remove-Item -LiteralPath $expandedRoot -Recurse -Force
        }
        if (Test-Path -LiteralPath $repoRoot) {
            Remove-Item -LiteralPath $repoRoot -Recurse -Force
        }

        Write-Host "[deploy-host] downloading repository archive: $archiveUrlValue" -ForegroundColor Cyan
        $previousProgressPreference = $global:ProgressPreference
        $global:ProgressPreference = "SilentlyContinue"
        try {
            Invoke-WebRequest -Uri $archiveUrlValue -OutFile $archivePath
        } finally {
            $global:ProgressPreference = $previousProgressPreference
        }
        Expand-Archive -LiteralPath $archivePath -DestinationPath $expandedRoot -Force

        $extractedRoot = Get-ChildItem -LiteralPath $expandedRoot -Directory | Select-Object -First 1
        if ($null -eq $extractedRoot) {
            throw "Repository archive did not contain an extractable root directory: $archiveUrlValue"
        }

        Move-Item -LiteralPath $extractedRoot.FullName -Destination $repoRoot
    }

    if (-not (Test-RepoLayout -Root $repoRoot -RequiredRelativePaths $RequiredRelativePaths)) {
        throw "Bootstrapped repository root is missing required paths: $repoRoot"
    }

    return [pscustomobject]@{
        RepoRoot = $repoRoot
        Source = "bootstrapped"
        ArchiveUrl = $archiveUrlValue
    }
}

$launcherRoot = Split-Path -Parent $PSCommandPath
$repoInfo = Ensure-RepoRoot `
    -LauncherRoot $launcherRoot `
    -Owner $RepoOwner `
    -Name $RepoName `
    -Ref $RepoRef `
    -RefKind $RepoRefKind `
    -RequiredRelativePaths @("README.md", "scripts\deploy-compose.ps1", "scripts\materialize-output-links.ps1") `
    -ArchiveUrl $RepoArchiveUrl `
    -CacheRoot $RepoCacheRoot `
    -ForceRefresh:$ForceRefreshRepo

if ($ResolveRepoOnly) {
    [pscustomobject]@{
        LauncherRoot = $launcherRoot
        RepoRoot = $repoInfo.RepoRoot
        Source = $repoInfo.Source
        ArchiveUrl = $repoInfo.ArchiveUrl
    } | Format-List
    return
}

$repoRoot = $repoInfo.RepoRoot
$resolvedOutputDirHost = if ([string]::IsNullOrWhiteSpace($OutputDirHost)) {
    Resolve-AbsolutePath -Path "runtime\register-output" -BaseDir $launcherRoot
} else {
    Resolve-AbsolutePath -Path $OutputDirHost -BaseDir $launcherRoot
}
$resolvedComposeFile = if ([string]::IsNullOrWhiteSpace($ComposeFile)) {
    Resolve-AbsolutePath -Path "compose\docker-compose.yaml" -BaseDir $repoRoot
} elseif ([System.IO.Path]::IsPathRooted($ComposeFile)) {
    Resolve-AbsolutePath -Path $ComposeFile -BaseDir $launcherRoot
} elseif (Test-Path -LiteralPath (Join-Path $repoRoot $ComposeFile)) {
    Resolve-AbsolutePath -Path $ComposeFile -BaseDir $repoRoot
} else {
    Resolve-AbsolutePath -Path $ComposeFile -BaseDir $launcherRoot
}

$env:REGISTER_OUTPUT_DIR_HOST = $resolvedOutputDirHost
$env:REGISTER_TEAM_AUTH_DIR_HOST = $TeamAuthDirHost
$env:REGISTER_DASHBOARD_PORT_HOST = $DashboardPortHost

if ([string]::IsNullOrWhiteSpace($env:EASY_PROXY_BASE_URL)) {
    $env:EASY_PROXY_BASE_URL = $defaultEasyProxyBaseUrl
}
if ([string]::IsNullOrWhiteSpace($env:EASYREGISTER_TEST_EASY_PROXY_BASE_URL)) {
    $env:EASYREGISTER_TEST_EASY_PROXY_BASE_URL = $defaultEasyProxyBaseUrl
}
if ([string]::IsNullOrWhiteSpace($env:EASY_PROTOCOL_CONTROL_TOKEN)) {
    $env:EASY_PROTOCOL_CONTROL_TOKEN = $defaultDashboardControlToken
}
if ([string]::IsNullOrWhiteSpace($env:EASYREGISTER_TEST_EASY_PROTOCOL_CONTROL_TOKEN)) {
    $env:EASYREGISTER_TEST_EASY_PROTOCOL_CONTROL_TOKEN = $defaultDashboardControlToken
}

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
    "-PathBaseDir", $launcherRoot,
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
