param(
    [string]$OutputPath = 'deploy/bootstrap/easyregister-r2-bootstrap.json',
    [string]$ImportCode = '',
    [string]$ManifestPath = '',
    [string]$AccountId = '',
    [string]$Bucket = '',
    [string]$ManifestObjectKey = '',
    [string]$RuntimeEnvObjectKey = '',
    [string]$AccessKeyId = '',
    [string]$SecretAccessKey = '',
    [string]$Endpoint = '',
    [string]$ExpectedRuntimeEnvSha256 = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot

function Resolve-EasyRegisterPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if ([System.IO.Path]::IsPathRooted($Path)) {
        return [System.IO.Path]::GetFullPath($Path)
    }

    return [System.IO.Path]::GetFullPath((Join-Path $repoRoot $Path))
}

$importSyncEnabled = $null
$importSyncIntervalSeconds = $null

if (-not [string]::IsNullOrWhiteSpace($ImportCode)) {
    $importPayloadPath = Join-Path ([System.IO.Path]::GetTempPath()) ("easyregister-import-code-" + [guid]::NewGuid().ToString('N') + ".json")
    try {
        & python (Join-Path $PSScriptRoot 'easyregister-import-code.py') inspect `
            --import-code $ImportCode `
            --output $importPayloadPath
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to decode import code with exit code $LASTEXITCODE"
        }

        $payload = Get-Content -LiteralPath $importPayloadPath -Raw | ConvertFrom-Json
        if (-not $AccountId) { $AccountId = [string]$payload.accountId }
        if (-not $Bucket) { $Bucket = [string]$payload.bucket }
        if (-not $Endpoint) { $Endpoint = [string]$payload.endpoint }
        if (-not $ManifestObjectKey) { $ManifestObjectKey = [string]$payload.manifestObjectKey }
        if (-not $AccessKeyId) { $AccessKeyId = [string]$payload.accessKeyId }
        if (-not $SecretAccessKey) { $SecretAccessKey = [string]$payload.secretAccessKey }
        $importSyncEnabled = $payload.syncEnabled
        $importSyncIntervalSeconds = $payload.syncIntervalSeconds
    } finally {
        Remove-Item -LiteralPath $importPayloadPath -ErrorAction SilentlyContinue
    }
}

if (-not [string]::IsNullOrWhiteSpace($ManifestPath)) {
    $resolvedManifestPath = Resolve-EasyRegisterPath -Path $ManifestPath
    if (-not (Test-Path -LiteralPath $resolvedManifestPath)) {
        throw "ManifestPath not found: $resolvedManifestPath"
    }

    $manifest = Get-Content -LiteralPath $resolvedManifestPath -Raw | ConvertFrom-Json
    if (-not $AccountId) { $AccountId = [string]$manifest.accountId }
    if (-not $Bucket) { $Bucket = [string]$manifest.bucket }
    if (-not $Endpoint) { $Endpoint = [string]$manifest.endpoint }
    if (-not $ManifestObjectKey) { $ManifestObjectKey = [string]$manifest.manifestObjectKey }
    if (-not $RuntimeEnvObjectKey) { $RuntimeEnvObjectKey = [string]$manifest.runtime.env.objectKey }
    if (-not $ExpectedRuntimeEnvSha256) { $ExpectedRuntimeEnvSha256 = [string]$manifest.runtime.env.sha256 }
}

foreach ($required in @(
    @{ Name = 'AccountId'; Value = $AccountId },
    @{ Name = 'Bucket'; Value = $Bucket },
    @{ Name = 'ManifestObjectKey or RuntimeEnvObjectKey'; Value = if ([string]::IsNullOrWhiteSpace($ManifestObjectKey)) { $RuntimeEnvObjectKey } else { $ManifestObjectKey } },
    @{ Name = 'AccessKeyId'; Value = $AccessKeyId },
    @{ Name = 'SecretAccessKey'; Value = $SecretAccessKey }
)) {
    if ([string]::IsNullOrWhiteSpace([string]$required.Value)) {
        throw "$($required.Name) is required."
    }
}

$bootstrap = [ordered]@{
    accountId = $AccountId
    endpoint = if ([string]::IsNullOrWhiteSpace($Endpoint)) {
        "https://$AccountId.r2.cloudflarestorage.com"
    } else {
        $Endpoint
    }
    bucket = $Bucket
    accessKeyId = $AccessKeyId
    secretAccessKey = $SecretAccessKey
}

if (-not [string]::IsNullOrWhiteSpace($ManifestObjectKey)) {
    $bootstrap.manifestObjectKey = $ManifestObjectKey
}
if (-not [string]::IsNullOrWhiteSpace($RuntimeEnvObjectKey)) {
    $bootstrap.runtimeEnvObjectKey = $RuntimeEnvObjectKey
}
if (-not [string]::IsNullOrWhiteSpace($ExpectedRuntimeEnvSha256)) {
    $bootstrap.expectedRuntimeEnvSha256 = $ExpectedRuntimeEnvSha256
}
if ($null -ne $importSyncEnabled) {
    $bootstrap.syncEnabled = [bool]$importSyncEnabled
}
if ($null -ne $importSyncIntervalSeconds -and [int]$importSyncIntervalSeconds -gt 0) {
    $bootstrap.syncIntervalSeconds = [int]$importSyncIntervalSeconds
}

$resolvedOutputPath = Resolve-EasyRegisterPath -Path $OutputPath
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $resolvedOutputPath) | Out-Null
$bootstrap | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $resolvedOutputPath -Encoding UTF8
Write-Host "Bootstrap file written: $resolvedOutputPath"
