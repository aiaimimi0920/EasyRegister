$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$scriptPath = Join-Path $repoRoot 'scripts\heal-docker-desktop-engine.ps1'
$tokens = $null
$parseErrors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    $scriptPath,
    [ref]$tokens,
    [ref]$parseErrors
)

if ($parseErrors.Count -gt 0) {
    throw "heal script has $($parseErrors.Count) parse error(s)"
}

$unsupportedStopJobForce = @(
    $ast.FindAll(
        {
            param($node)

            if ($node -isnot [System.Management.Automation.Language.CommandAst]) {
                return $false
            }
            if ($node.GetCommandName() -ne 'Stop-Job') {
                return $false
            }
            return @(
                $node.CommandElements |
                    Where-Object {
                        $_ -is [System.Management.Automation.Language.CommandParameterAst] -and
                        $_.ParameterName -eq 'Force'
                    }
            ).Count -gt 0
        },
        $true
    )
)

if ($unsupportedStopJobForce.Count -gt 0) {
    throw 'heal script must not use unsupported Windows PowerShell 5.1 syntax: Stop-Job -Force'
}

Write-Output 'PASS: Docker engine heal script uses Windows PowerShell 5.1-compatible Stop-Job syntax.'

$tempRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("easyregister-heal-test-{0}" -f [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $tempRoot -Force | Out-Null
try {
    $fakeDocker = Join-Path $tempRoot 'fake-docker.exe'
    $fakeWsl = Join-Path $tempRoot 'fake-wsl.exe'
    $fakeEmpty = Join-Path $tempRoot 'fake-empty.exe'
    $stateRoot = Join-Path $tempRoot 'state'
    $fakeProbeSource = @'
using System;
using System.IO;
using System.Reflection;
using System.Threading;

public static class FakeDockerProbe
{
    public static int Main(string[] args)
    {
        var name = Path.GetFileNameWithoutExtension(Assembly.GetEntryAssembly().Location);
        if (name.IndexOf("empty", StringComparison.OrdinalIgnoreCase) >= 0)
        {
            return 1;
        }
        if (name.IndexOf("docker", StringComparison.OrdinalIgnoreCase) >= 0)
        {
            Thread.Sleep(20000);
            return 0;
        }
        foreach (var arg in args)
        {
            if (string.Equals(arg, "pidof", StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine("144");
                return 0;
            }
        }
        Console.WriteLine("29.5.3");
        return 0;
    }
}
'@
    Add-Type -TypeDefinition $fakeProbeSource -OutputAssembly $fakeWsl -OutputType ConsoleApplication
    Copy-Item -LiteralPath $fakeWsl -Destination $fakeDocker
    Copy-Item -LiteralPath $fakeWsl -Destination $fakeEmpty

    $childArgs = @(
        '-NoLogo',
        '-NoProfile',
        '-ExecutionPolicy',
        'Bypass',
        '-File',
        $scriptPath,
        '-StateRoot',
        $stateRoot,
        '-DockerExe',
        $fakeDocker,
        '-WslExe',
        $fakeWsl,
        '-ProbeTimeoutSeconds',
        '8',
        '-ReadyAttempts',
        '1',
        '-ReadySleepSeconds',
        '1',
        '-SkipDesktopStart'
    )
    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    $probeStopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    try {
        $output = (& powershell.exe @childArgs 2>&1 | Out-String)
        $exitCode = $LASTEXITCODE
    } finally {
        $probeStopwatch.Stop()
        $ErrorActionPreference = $previousErrorActionPreference
    }

    if ($exitCode -ne 0) {
        $logPath = Join-Path $stateRoot 'docker-engine-heal.log'
        $logText = if (Test-Path -LiteralPath $logPath) {
            Get-Content -LiteralPath $logPath -Raw -Encoding utf8
        } else {
            '<missing>'
        }
        throw "heal script did not accept the healthy WSL daemon fallback (exit=$exitCode): $output`nlog=$logText"
    }
    if ($output -notmatch 'initial_ready=True .*server=29\.5\.3 .*source=wsl') {
        throw "heal script did not report the healthy WSL daemon fallback: $output"
    }
    if ($probeStopwatch.Elapsed.TotalSeconds -gt 15) {
        throw "heal script exceeded its bounded native-probe budget: $($probeStopwatch.Elapsed.TotalSeconds) seconds"
    }

    $cacheStateRoot = Join-Path $tempRoot 'cache-state'
    New-Item -ItemType Directory -Path $cacheStateRoot -Force | Out-Null
    $cachedDataDiskId = '123e4567-e89b-12d3-a456-426614174000'
    [pscustomobject]@{
        dataDiskId = $cachedDataDiskId
        source = 'test'
        updatedAt = (Get-Date).ToString('o')
    } | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $cacheStateRoot 'docker-engine-bootstrap.json') -Encoding utf8

    $cacheArgs = @(
        '-NoLogo',
        '-NoProfile',
        '-ExecutionPolicy',
        'Bypass',
        '-File',
        $scriptPath,
        '-StateRoot',
        $cacheStateRoot,
        '-DockerExe',
        $fakeEmpty,
        '-WslExe',
        $fakeEmpty,
        '-ProbeTimeoutSeconds',
        '5',
        '-ReadyAttempts',
        '1',
        '-ReadySleepSeconds',
        '1',
        '-SkipDesktopStart'
    )
    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $cacheOutput = (& powershell.exe @cacheArgs 2>&1 | Out-String)
    } finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    $cacheLog = Get-Content -LiteralPath (Join-Path $cacheStateRoot 'docker-engine-heal.log') -Raw -Encoding utf8
    if ($cacheLog -notmatch "bootstrap_launch dataDiskId=$([regex]::Escape($cachedDataDiskId))") {
        throw "heal script did not reuse the cached data-disk id when no live bootstrap process existed: $cacheOutput`nlog=$cacheLog"
    }
} finally {
    Remove-Item -LiteralPath $tempRoot -Recurse -Force -ErrorAction SilentlyContinue
}

Write-Output 'PASS: Docker engine heal script accepts a healthy WSL daemon when the Windows Docker CLI hangs.'
Write-Output 'PASS: Docker engine heal script can bootstrap from a cached data-disk id when the live command is absent.'
