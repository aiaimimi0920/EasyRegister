param(
    [string]$RuntimeEnvPath,
    [string]$AccountId = '',
    [string]$Bucket = '',
    [string]$AccessKeyId = '',
    [string]$SecretAccessKey = '',
    [string]$RuntimeEnvObjectKey = '',
    [string]$ManifestObjectKey = '',
    [string]$Endpoint = '',
    [string]$ReleaseVersion = '',
    [string]$ManifestOutput = ''
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

foreach ($required in @(
    @{ Name = 'RuntimeEnvPath'; Value = $RuntimeEnvPath },
    @{ Name = 'AccountId'; Value = $AccountId },
    @{ Name = 'Bucket'; Value = $Bucket },
    @{ Name = 'AccessKeyId'; Value = $AccessKeyId },
    @{ Name = 'SecretAccessKey'; Value = $SecretAccessKey },
    @{ Name = 'RuntimeEnvObjectKey'; Value = $RuntimeEnvObjectKey },
    @{ Name = 'ManifestObjectKey'; Value = $ManifestObjectKey }
)) {
    if ([string]::IsNullOrWhiteSpace([string]$required.Value)) {
        throw "$($required.Name) is required."
    }
}

$resolvedRuntimeEnvPath = Resolve-EasyRegisterPath -Path $RuntimeEnvPath
if (-not (Test-Path -LiteralPath $resolvedRuntimeEnvPath)) {
    throw "RuntimeEnvPath not found: $resolvedRuntimeEnvPath"
}

$pythonArgs = @(
    (Join-Path $PSScriptRoot 'upload-runtime-r2-config.py'),
    '--account-id', $AccountId,
    '--bucket', $Bucket,
    '--access-key-id', $AccessKeyId,
    '--secret-access-key', $SecretAccessKey,
    '--runtime-env-path', $resolvedRuntimeEnvPath,
    '--runtime-env-object-key', $RuntimeEnvObjectKey,
    '--manifest-object-key', $ManifestObjectKey
)
if (-not [string]::IsNullOrWhiteSpace($Endpoint)) {
    $pythonArgs += @('--endpoint', $Endpoint)
}
if (-not [string]::IsNullOrWhiteSpace($ReleaseVersion)) {
    $pythonArgs += @('--release-version', $ReleaseVersion)
}
if (-not [string]::IsNullOrWhiteSpace($ManifestOutput)) {
    $pythonArgs += @('--manifest-output', (Resolve-EasyRegisterPath -Path $ManifestOutput))
}

& python @pythonArgs
if ($LASTEXITCODE -ne 0) {
    throw "R2 upload failed with exit code $LASTEXITCODE"
}
