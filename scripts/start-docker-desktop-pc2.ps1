param(
    [string]$DockerDesktopExe = 'C:\Program Files\Docker\Docker\Docker Desktop.exe',
    [string]$WslExe = 'C:\Windows\System32\wsl.exe',
    [string]$TaskKillExe = 'C:\Windows\System32\taskkill.exe',
    [string]$NetshExe = 'C:\Windows\System32\netsh.exe',
    [string]$LogPath = 'D:\SelfDocker\EasyRegister\runtime\pc2_start_docker_desktop.log',
    [int]$StopTimeoutSeconds = 8,
    [int]$PostTerminateSleepSeconds = 5,
    [string[]]$DockerProcessNames = @(
        'Docker Desktop',
        'com.docker.backend',
        'com.docker.proxy',
        'com.docker.build',
        'docker-agent',
        'docker-ai',
        'docker-buildx',
        'docker-compose',
        'docker-debug',
        'docker-dhi',
        'docker-extension',
        'docker-init',
        'docker-mcp',
        'docker-model',
        'docker-offload',
        'docker-pass',
        'docker-sandbox',
        'docker-scout'
    )
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$logDirectory = Split-Path -Parent $LogPath
if (-not [string]::IsNullOrWhiteSpace($logDirectory)) {
    New-Item -ItemType Directory -Path $logDirectory -Force | Out-Null
}

function Write-RestartLog {
    param([Parameter(Mandatory = $true)][string]$Message)

    $line = '{0} {1}' -f (Get-Date).ToString('s'), $Message
    Add-Content -LiteralPath $LogPath -Value $line -Encoding utf8
}

function ConvertTo-ProcessArgument {
    param([Parameter(Mandatory = $true)][AllowEmptyString()][string]$Value)

    if ($Value.Length -gt 0 -and $Value -notmatch '[\s"]') {
        return $Value
    }
    return '"' + $Value.Replace('"', '\"') + '"'
}

function Invoke-BoundedProcess {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [string[]]$Arguments = @(),
        [int]$TimeoutSeconds = 8
    )

    $startInfo = New-Object System.Diagnostics.ProcessStartInfo
    $startInfo.FileName = $FilePath
    $startInfo.Arguments = (@($Arguments | ForEach-Object { ConvertTo-ProcessArgument -Value ([string]$_) }) -join ' ')
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true

    $process = New-Object System.Diagnostics.Process
    $process.StartInfo = $startInfo
    try {
        if (-not $process.Start()) {
            throw "failed to start process: $FilePath"
        }
        if (-not $process.WaitForExit([Math]::Max($TimeoutSeconds, 1) * 1000)) {
            try {
                $process.Kill()
            } catch {
            }
            [void]$process.WaitForExit(3000)
            return [pscustomobject]@{
                timedOut = $true
                exitCode = $null
                stdout = ''
                stderr = ''
            }
        }
        return [pscustomobject]@{
            timedOut = $false
            exitCode = $process.ExitCode
            stdout = $process.StandardOutput.ReadToEnd()
            stderr = $process.StandardError.ReadToEnd()
        }
    } finally {
        $process.Dispose()
    }
}

function Stop-DockerDesktopProcesses {
    param([int]$Rounds = 3)

    for ($round = 1; $round -le [Math]::Max($Rounds, 1); $round++) {
        $targets = @(
            Get-Process -ErrorAction SilentlyContinue |
                Where-Object { $DockerProcessNames -contains $_.ProcessName }
        )
        Write-RestartLog "phase=process_stop round=$round count=$($targets.Count)"
        if ($targets.Count -eq 0) {
            return 0
        }

        foreach ($target in $targets) {
            $result = Invoke-BoundedProcess -FilePath $TaskKillExe -Arguments @(
                '/PID',
                [string]$target.Id,
                '/T',
                '/F'
            ) -TimeoutSeconds $StopTimeoutSeconds
            Write-RestartLog "phase=process_stop_result pid=$($target.Id) timedOut=$($result.timedOut) exit=$($result.exitCode)"
        }
        Start-Sleep -Seconds 1
    }

    return @(
        Get-Process -ErrorAction SilentlyContinue |
            Where-Object { $DockerProcessNames -contains $_.ProcessName }
    ).Count
}

Write-RestartLog 'phase=restart_enter'
[void](Stop-DockerDesktopProcesses -Rounds 3)

$wslResult = Invoke-BoundedProcess -FilePath $WslExe -Arguments @(
    '--terminate',
    'docker-desktop'
) -TimeoutSeconds ([Math]::Max($StopTimeoutSeconds, 20))
Write-RestartLog "phase=wsl_terminate_completed timedOut=$($wslResult.timedOut) exit=$($wslResult.exitCode)"
if ($wslResult.timedOut) {
    throw 'docker-desktop WSL termination timed out'
}

$remainingCount = Stop-DockerDesktopProcesses -Rounds 2
if ($remainingCount -gt 0) {
    throw "Docker Desktop processes remained after bounded termination: $remainingCount"
}

if ($PostTerminateSleepSeconds -gt 0) {
    Start-Sleep -Seconds $PostTerminateSleepSeconds
}

foreach ($family in @('ipv4', 'ipv6')) {
    $netshResult = Invoke-BoundedProcess -FilePath $NetshExe -Arguments @(
        'interface',
        $family,
        'show',
        'excludedportrange',
        'protocol=tcp'
    ) -TimeoutSeconds $StopTimeoutSeconds
    Write-RestartLog "phase=netsh_prewarm family=$family timedOut=$($netshResult.timedOut) exit=$($netshResult.exitCode)"
}

if (-not (Test-Path -LiteralPath $DockerDesktopExe)) {
    throw "Docker Desktop executable is missing: $DockerDesktopExe"
}

Write-RestartLog 'phase=desktop_start_requested'
Start-Process -FilePath $DockerDesktopExe -WindowStyle Hidden | Out-Null
Write-RestartLog 'phase=restart_dispatched'
exit 0
