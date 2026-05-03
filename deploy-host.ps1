param(
    [string]$OutputDirHost = "",
    [string]$ComposeFile = "",
    [string]$MailboxServiceBaseUrl = "http://easy-email:8080",
    [string]$MailboxServiceApiKey = "J7L+RCwLIBEcMZHzz0rXjm4oyR9rymq9",
    [string]$EasyProxyBaseUrl = "http://easy-proxy:29888",
    [string]$EasyProxyApiKey = "YP9l2DecuS_MRhARQu5v829VFOWKar7S",
    [string]$TeamAuthDirHost = "C:\Users\vmjcv\.cli-proxy-api\team",
    [string]$CodexFreeDirHost = "C:\Users\vmjcv\.cli-proxy-api\free",
    [string]$CodexTeamDirHost = "C:\Users\vmjcv\.cli-proxy-api\team",
    [string]$CodexTeamInputDirHost = "C:\Users\vmjcv\.cli-proxy-api\team",
    [string]$CodexTeamMotherInputDirHost = "",
    [int]$WorkerCount = 10,
    [int]$MainConcurrencyLimit = 5,
    [int]$ContinueConcurrencyLimit = 2,
    [int]$TeamConcurrencyLimit = 1,
    [double]$OpenaiUploadPercent = 0,
    [double]$CodexFreeUploadPercent = 0,
    [double]$CodexTeamUploadPercent = 0,
    [double]$CodexPlusUploadPercent = 0,
    [string]$DashboardPortHost = "19790",
    [string]$ComposeProjectName = "easy-register",
    [ValidateSet("Auto", "Junction", "SymbolicLink")]
    [string]$LinkType = "Auto",
    [switch]$ForceLinks,
    [switch]$NoBuild,
    [string]$Image = "",
    [switch]$Pull,
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

$defaultEasyProxyBaseUrl = "http://easy-proxy:29888"
$defaultDashboardControlToken = "easyregister-dashboard-local-token"
$defaultDashboardListen = "0.0.0.0:9790"

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

function Write-ComposeEnvFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [hashtable]$Values
    )

    $lines = New-Object System.Collections.Generic.List[string]
    foreach ($key in ($Values.Keys | Sort-Object)) {
        $name = [string]$key
        if ([string]::IsNullOrWhiteSpace($name)) {
            continue
        }
        $rawValue = $Values[$key]
        $value = if ($null -eq $rawValue) { "" } else { [string]$rawValue }
        $lines.Add("$name=$value")
    }
    $parent = Split-Path -Parent $Path
    if (-not [string]::IsNullOrWhiteSpace($parent)) {
        New-Item -ItemType Directory -Force -Path $parent | Out-Null
    }
    Set-Content -LiteralPath $Path -Value $lines -Encoding ASCII
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
$env:REGISTER_WORKER_COUNT = [string]$WorkerCount
$env:REGISTER_MAIN_CONCURRENCY_LIMIT = [string]$MainConcurrencyLimit
$env:REGISTER_CONTINUE_CONCURRENCY_LIMIT = [string]$ContinueConcurrencyLimit
$env:REGISTER_TEAM_CONCURRENCY_LIMIT = [string]$TeamConcurrencyLimit
$env:MAILBOX_SERVICE_BASE_URL = $MailboxServiceBaseUrl
$env:MAILBOX_SERVICE_API_KEY = $MailboxServiceApiKey
$env:EASY_PROXY_BASE_URL = $EasyProxyBaseUrl
$env:EASY_PROXY_API_KEY = $EasyProxyApiKey

if ([string]::IsNullOrWhiteSpace($env:EASYREGISTER_TEST_EASY_PROXY_BASE_URL)) {
    $env:EASYREGISTER_TEST_EASY_PROXY_BASE_URL = $EasyProxyBaseUrl
}
if ([string]::IsNullOrWhiteSpace($env:EASY_PROTOCOL_CONTROL_TOKEN)) {
    $env:EASY_PROTOCOL_CONTROL_TOKEN = $defaultDashboardControlToken
}
if ([string]::IsNullOrWhiteSpace($env:EASYREGISTER_TEST_EASY_PROTOCOL_CONTROL_TOKEN)) {
    $env:EASYREGISTER_TEST_EASY_PROTOCOL_CONTROL_TOKEN = $defaultDashboardControlToken
}
if ([string]::IsNullOrWhiteSpace($env:REGISTER_DASHBOARD_LISTEN)) {
    $env:REGISTER_DASHBOARD_LISTEN = $defaultDashboardListen
}
if ([string]::IsNullOrWhiteSpace($env:EASYREGISTER_TEST_DASHBOARD_LISTEN)) {
    $env:EASYREGISTER_TEST_DASHBOARD_LISTEN = $defaultDashboardListen
}
if ([string]::IsNullOrWhiteSpace($env:REGISTER_DASHBOARD_ALLOW_REMOTE)) {
    $env:REGISTER_DASHBOARD_ALLOW_REMOTE = "true"
}
if ([string]::IsNullOrWhiteSpace($env:EASYREGISTER_TEST_DASHBOARD_ALLOW_REMOTE)) {
    $env:EASYREGISTER_TEST_DASHBOARD_ALLOW_REMOTE = "true"
}

if (-not [string]::IsNullOrWhiteSpace($Image)) {
    $env:REGISTER_SERVICE_IMAGE = $Image
    $env:EASYREGISTER_TEST_IMAGE = $Image
    if ($Pull) {
        Write-Host "[deploy-host] pulling image: $Image" -ForegroundColor Cyan
        & docker pull $Image
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to pull docker image: $Image"
        }
    }
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
$env:REGISTER_OPENAI_UPLOAD_PERCENT = [string]$OpenaiUploadPercent
$env:REGISTER_CODEX_FREE_UPLOAD_PERCENT = [string]$CodexFreeUploadPercent
$env:REGISTER_CODEX_TEAM_UPLOAD_PERCENT = [string]$CodexTeamUploadPercent
$env:REGISTER_CODEX_PLUS_UPLOAD_PERCENT = [string]$CodexPlusUploadPercent

$composeEnvFilePath = Join-Path $launcherRoot ".deploy-compose.env"
Write-ComposeEnvFile -Path $composeEnvFilePath -Values @{
    REGISTER_OUTPUT_DIR_HOST              = $env:REGISTER_OUTPUT_DIR_HOST
    REGISTER_TEAM_AUTH_DIR_HOST           = $env:REGISTER_TEAM_AUTH_DIR_HOST
    REGISTER_DASHBOARD_PORT_HOST          = $env:REGISTER_DASHBOARD_PORT_HOST
    REGISTER_WORKER_COUNT                 = $env:REGISTER_WORKER_COUNT
    REGISTER_MAIN_CONCURRENCY_LIMIT       = $env:REGISTER_MAIN_CONCURRENCY_LIMIT
    REGISTER_CONTINUE_CONCURRENCY_LIMIT   = $env:REGISTER_CONTINUE_CONCURRENCY_LIMIT
    REGISTER_TEAM_CONCURRENCY_LIMIT       = $env:REGISTER_TEAM_CONCURRENCY_LIMIT
    MAILBOX_SERVICE_BASE_URL              = $env:MAILBOX_SERVICE_BASE_URL
    MAILBOX_SERVICE_API_KEY               = $env:MAILBOX_SERVICE_API_KEY
    EASY_PROXY_BASE_URL                   = $env:EASY_PROXY_BASE_URL
    EASY_PROXY_API_KEY                    = $env:EASY_PROXY_API_KEY
    REGISTER_OPENAI_UPLOAD_PERCENT        = $env:REGISTER_OPENAI_UPLOAD_PERCENT
    REGISTER_CODEX_FREE_UPLOAD_PERCENT    = $env:REGISTER_CODEX_FREE_UPLOAD_PERCENT
    REGISTER_CODEX_TEAM_UPLOAD_PERCENT    = $env:REGISTER_CODEX_TEAM_UPLOAD_PERCENT
    REGISTER_CODEX_PLUS_UPLOAD_PERCENT    = $env:REGISTER_CODEX_PLUS_UPLOAD_PERCENT
    REGISTER_CODEX_FREE_DIR_HOST          = $env:REGISTER_CODEX_FREE_DIR_HOST
    REGISTER_CODEX_TEAM_DIR_HOST          = $env:REGISTER_CODEX_TEAM_DIR_HOST
    REGISTER_CODEX_TEAM_INPUT_DIR_HOST    = $env:REGISTER_CODEX_TEAM_INPUT_DIR_HOST
    REGISTER_CODEX_TEAM_MOTHER_INPUT_DIR_HOST = $env:REGISTER_CODEX_TEAM_MOTHER_INPUT_DIR_HOST
    EASY_PROTOCOL_BASE_URL                = $env:EASY_PROTOCOL_BASE_URL
    EASY_PROTOCOL_CONTROL_TOKEN           = $env:EASY_PROTOCOL_CONTROL_TOKEN
    REGISTER_DASHBOARD_LISTEN             = $env:REGISTER_DASHBOARD_LISTEN
    REGISTER_DASHBOARD_ALLOW_REMOTE       = $env:REGISTER_DASHBOARD_ALLOW_REMOTE
    REGISTER_SERVICE_IMAGE                = $env:REGISTER_SERVICE_IMAGE
}

$materializeScript = Join-Path $repoRoot "scripts\materialize-output-links.ps1"
$materializeParams = @{
    OutputDirHost = $resolvedOutputDirHost
    PathBaseDir   = $launcherRoot
    LinkType      = $LinkType
}
if ($ForceLinks) {
    $materializeParams["Force"] = $true
}

& $materializeScript @materializeParams

if ($MaterializeOnly) {
    return
}

$deployComposeScript = Join-Path $repoRoot "scripts\deploy-compose.ps1"
$deployComposeParams = @{
    ComposeFile        = $resolvedComposeFile
    ComposeProjectName = $ComposeProjectName
    OutputDirHost      = $resolvedOutputDirHost
    EnvFilePath        = $composeEnvFilePath
    LinkType           = $LinkType
}
if ($ForceLinks) {
    $deployComposeParams["ForceLinks"] = $true
}
if ((-not $NoBuild) -and [string]::IsNullOrWhiteSpace($Image)) {
    $deployComposeParams["Build"] = $true
}
if ($NoDetach) {
    $deployComposeParams["NoDetach"] = $true
}

& $deployComposeScript @deployComposeParams @Services
