param(
    [string]$ComposeFile = "",
    [string]$ComposeProjectName = "",
    [string]$OutputDirHost = $env:REGISTER_OUTPUT_DIR_HOST,
    [string]$AliasRootHost = $env:REGISTER_OUTPUT_ALIAS_ROOT_HOST,
    [ValidateSet("Auto", "Junction", "SymbolicLink")]
    [string]$LinkType = "Auto",
    [switch]$ForceLinks,
    [switch]$SkipMaterialize,
    [switch]$Build,
    [switch]$NoDetach,
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

$repoRoot = Resolve-AbsolutePath -Path (Join-Path $PSScriptRoot "..") -BaseDir $PWD.Path
$resolvedComposeFile = if ([string]::IsNullOrWhiteSpace($ComposeFile)) {
    Resolve-AbsolutePath -Path "compose/docker-compose.yaml" -BaseDir $repoRoot
} else {
    Resolve-AbsolutePath -Path $ComposeFile -BaseDir $repoRoot
}
$composeDir = Split-Path -Parent $resolvedComposeFile

if (-not $SkipMaterialize) {
    $materializeArgs = @(
        "-ExecutionPolicy", "Bypass",
        "-File", (Join-Path $PSScriptRoot "materialize-output-links.ps1"),
        "-PathBaseDir", $composeDir,
        "-LinkType", $LinkType
    )
    if ($OutputDirHost) {
        $materializeArgs += @("-OutputDirHost", $OutputDirHost)
    }
    if ($AliasRootHost) {
        $materializeArgs += @("-AliasRootHost", $AliasRootHost)
    }
    if ($ForceLinks) {
        $materializeArgs += "-Force"
    }
    & powershell @materializeArgs
}

$resolvedComposeProjectName = if ([string]::IsNullOrWhiteSpace($ComposeProjectName)) {
    'easy-register'
} else {
    $ComposeProjectName
}

$composeArgs = @("compose", "-p", $resolvedComposeProjectName, "-f", $resolvedComposeFile, "up")
if (-not $NoDetach) {
    $composeArgs += "-d"
}
if ($Build) {
    $composeArgs += "--build"
}
if ($Services) {
    $composeArgs += $Services
}

& docker @composeArgs
