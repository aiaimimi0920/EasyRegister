$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$scriptPath = Join-Path $repoRoot 'scripts\start-docker-desktop-pc2.ps1'
$tokens = $null
$parseErrors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    $scriptPath,
    [ref]$tokens,
    [ref]$parseErrors
)
if ($parseErrors.Count -gt 0) {
    throw "PC2 Docker start script has $($parseErrors.Count) parse error(s)"
}

$unboundedStops = @(
    $ast.FindAll(
        {
            param($node)

            $node -is [System.Management.Automation.Language.CommandAst] -and
                $node.GetCommandName() -eq 'Stop-Process'
        },
        $true
    )
)
if ($unboundedStops.Count -gt 0) {
    throw 'PC2 Docker start script must not use unbounded Stop-Process calls'
}

$tempRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("pc2-docker-start-test-{0}" -f [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $tempRoot -Force | Out-Null
try {
    $fakeWsl = Join-Path $tempRoot 'fake-wsl.exe'
    $fakeTaskKill = Join-Path $tempRoot 'fake-taskkill.exe'
    $fakeNetsh = Join-Path $tempRoot 'fake-netsh.exe'
    $fakeDesktop = Join-Path $tempRoot 'fake-desktop.exe'
    $externalLog = Join-Path $tempRoot 'external.log'
    $scriptLog = Join-Path $tempRoot 'script.log'
    $fakeSource = @'
using System;
using System.IO;
using System.Reflection;

public static class FakePc2DockerCommand
{
    public static int Main(string[] args)
    {
        var logPath = Environment.GetEnvironmentVariable("PC2_DOCKER_START_TEST_LOG");
        if (!string.IsNullOrWhiteSpace(logPath))
        {
            var executable = Path.GetFileName(Assembly.GetEntryAssembly().Location);
            File.AppendAllText(logPath, executable + "|" + string.Join(" ", args) + Environment.NewLine);
        }
        return 0;
    }
}
'@
    Add-Type -TypeDefinition $fakeSource -OutputAssembly $fakeWsl -OutputType ConsoleApplication
    Copy-Item -LiteralPath $fakeWsl -Destination $fakeTaskKill
    Copy-Item -LiteralPath $fakeWsl -Destination $fakeNetsh
    Copy-Item -LiteralPath $fakeWsl -Destination $fakeDesktop

    $env:PC2_DOCKER_START_TEST_LOG = $externalLog
    $childArgs = @(
        '-NoLogo',
        '-NoProfile',
        '-ExecutionPolicy',
        'Bypass',
        '-File',
        $scriptPath,
        '-DockerDesktopExe',
        $fakeDesktop,
        '-WslExe',
        $fakeWsl,
        '-TaskKillExe',
        $fakeTaskKill,
        '-NetshExe',
        $fakeNetsh,
        '-DockerProcessNames',
        'pc2-docker-start-test-no-such-process',
        '-LogPath',
        $scriptLog,
        '-StopTimeoutSeconds',
        '2',
        '-PostTerminateSleepSeconds',
        '0'
    )
    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $output = (& powershell.exe @childArgs 2>&1 | Out-String)
        $exitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousErrorActionPreference
        $stopwatch.Stop()
    }

    if ($exitCode -ne 0) {
        throw "PC2 Docker start script test invocation failed (exit=$exitCode): $output"
    }
    if ($stopwatch.Elapsed.TotalSeconds -gt 10) {
        throw "PC2 Docker start script exceeded its bounded test runtime: $($stopwatch.Elapsed.TotalSeconds) seconds"
    }

    $deadline = (Get-Date).AddSeconds(5)
    do {
        if ((Test-Path -LiteralPath $externalLog) -and (Get-Content -LiteralPath $externalLog -Raw) -match 'fake-desktop\.exe') {
            break
        }
        Start-Sleep -Milliseconds 100
    } while ((Get-Date) -lt $deadline)

    $externalText = Get-Content -LiteralPath $externalLog -Raw
    if ($externalText -notmatch 'fake-wsl\.exe\|--terminate docker-desktop') {
        throw "PC2 Docker start script did not terminate the docker-desktop WSL distro: $externalText"
    }
    if ($externalText -notmatch 'fake-desktop\.exe\|') {
        throw "PC2 Docker start script did not launch Docker Desktop: $externalText"
    }
    $scriptText = Get-Content -LiteralPath $scriptLog -Raw -Encoding utf8
    if ($scriptText -notmatch 'phase=wsl_terminate_completed' -or $scriptText -notmatch 'phase=desktop_start_requested') {
        throw "PC2 Docker start script did not record restart phases: $scriptText"
    }
} finally {
    Remove-Item Env:PC2_DOCKER_START_TEST_LOG -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $tempRoot -Recurse -Force -ErrorAction SilentlyContinue
}

Write-Output 'PASS: PC2 Docker start script uses bounded process termination and completes the cold-start sequence.'
