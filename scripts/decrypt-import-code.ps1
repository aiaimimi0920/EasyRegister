param(
    [Parameter(Mandatory = $true)]
    [string]$EncryptedFilePath,
    [Parameter(Mandatory = $true)]
    [string]$PrivateKeyPath,
    [switch]$ImportCodeOnly,
    [string]$OutputPath = ''
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

$args = @(
    (Join-Path $PSScriptRoot 'easyregister-import-code.py'),
    'decrypt',
    '--encrypted-file', (Resolve-EasyRegisterPath -Path $EncryptedFilePath),
    '--private-key-file', (Resolve-EasyRegisterPath -Path $PrivateKeyPath)
)

if ($ImportCodeOnly) {
    $args += '--import-code-only'
}
if (-not [string]::IsNullOrWhiteSpace($OutputPath)) {
    $args += @('--output', (Resolve-EasyRegisterPath -Path $OutputPath))
}

& python @args
if ($LASTEXITCODE -ne 0) {
    throw "Failed to decrypt import code with exit code $LASTEXITCODE"
}
