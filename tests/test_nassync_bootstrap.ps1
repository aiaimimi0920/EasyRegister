$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Assert-Equal {
    param(
        [Parameter(Mandatory = $true)]$Actual,
        [Parameter(Mandatory = $true)]$Expected,
        [Parameter(Mandatory = $true)][string]$Message
    )

    if ($Actual -ne $Expected) {
        throw ("ASSERT FAILED: {0}`nExpected: {1}`nActual: {2}" -f $Message, $Expected, $Actual)
    }
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$sourcePath = Join-Path $repoRoot 'tools\NasSyncBootstrap\NasSyncBootstrap.cs'
$buildDir = Join-Path $repoRoot '.tmp\nassync-bootstrap-test'
$exePath = Join-Path $buildDir 'NasSyncBootstrap.exe'
$credPath = Join-Path $buildDir 'sample-cred.xml'
$csc = 'C:\Windows\Microsoft.NET\Framework64\v4.0.30319\csc.exe'

New-Item -ItemType Directory -Force -Path $buildDir | Out-Null
if (Test-Path -LiteralPath $exePath) {
    Remove-Item -LiteralPath $exePath -Force
}

& $csc /nologo /target:exe /out:$exePath $sourcePath | Out-Null
if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $exePath)) {
    throw 'compile_failed'
}

$secure = ConvertTo-SecureString 'pass123!' -AsPlainText -Force
$credential = [pscredential]::new('user1', $secure)
$credential | Export-Clixml -Path $credPath

$json = & $exePath dump-credential $credPath
if ($LASTEXITCODE -ne 0) {
    throw 'dump_credential_failed'
}

$parsed = $json | ConvertFrom-Json
Assert-Equal $parsed.username 'user1' 'dump-credential should return username'
Assert-Equal $parsed.password 'pass123!' 'dump-credential should decrypt password'

Write-Output 'PASS test_nassync_bootstrap'
