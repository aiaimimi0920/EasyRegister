param(
    [string]$StateRoot = 'D:\SelfDocker\EasyRegister\runtime',
    [string]$DockerDesktopExe = 'C:\Program Files\Docker\Docker\Docker Desktop.exe',
    [string]$DockerExe = 'C:\Program Files\Docker\Docker\resources\bin\docker.exe',
    [string]$WslExe = 'wsl.exe',
    [string]$WslDistroName = 'docker-desktop',
    [int]$ProbeTimeoutSeconds = 8,
    [int]$ReadyAttempts = 30,
    [int]$ReadySleepSeconds = 4,
    [switch]$ForceBootstrap,
    [switch]$SkipDesktopStart
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$resolvedStateRoot = [System.IO.Path]::GetFullPath($StateRoot)
$null = New-Item -ItemType Directory -Force -Path $resolvedStateRoot
$logPath = Join-Path $resolvedStateRoot 'docker-engine-heal.log'
$lockPath = Join-Path $resolvedStateRoot 'docker-engine-heal.lock'
$cachePath = Join-Path $resolvedStateRoot 'docker-engine-bootstrap.json'

function Write-Log {
    param([Parameter(Mandatory = $true)][string]$Message)

    $line = '{0} {1}' -f (Get-Date).ToString('s'), $Message
    Add-Content -LiteralPath $logPath -Value $line -Encoding UTF8
    Write-Output $line
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
        $timeoutMilliseconds = [Math]::Max($TimeoutSeconds, 1) * 1000
        if (-not $process.WaitForExit($timeoutMilliseconds)) {
            try {
                $process.Kill()
            } catch {
            }
            [void]$process.WaitForExit(5000)
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

function Get-WslBootstrapCommandLine {
    try {
        $result = Invoke-BoundedProcess -FilePath $WslExe -Arguments @(
            '-d',
            $WslDistroName,
            '--',
            'ps',
            '-ef'
        ) -TimeoutSeconds $ProbeTimeoutSeconds
        if ($result.timedOut -or $result.exitCode -ne 0) {
            return ''
        }
        $line = @(
            $result.stdout -split "`r?`n" |
                Where-Object { $_ -match 'wsl-bootstrap run' }
        ) | Select-Object -First 1
        return ([string]$line).Trim()
    } catch {
        return ''
    }
}

function Get-DataDiskIdFromCommandLine {
    param([Parameter(Mandatory = $true)][AllowEmptyString()][string]$CommandLine)

    if ($CommandLine -match '--data-disk\s+([0-9a-fA-F-]+)') {
        return $Matches[1].ToLowerInvariant()
    }
    return ''
}

function Save-BootstrapCache {
    param(
        [Parameter(Mandatory = $true)][string]$DataDiskId,
        [Parameter(Mandatory = $true)][string]$Source
    )

    $payload = [pscustomobject]@{
        dataDiskId = $DataDiskId
        source = $Source
        updatedAt = (Get-Date).ToString('o')
    }
    $payload | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $cachePath -Encoding UTF8
}

function Get-CachedDataDiskId {
    if (-not (Test-Path -LiteralPath $cachePath)) {
        return ''
    }

    try {
        $json = Get-Content -LiteralPath $cachePath -Raw | ConvertFrom-Json
        if ($null -eq $json) {
            return ''
        }
        $value = [string]$json.dataDiskId
        return $value.Trim().ToLowerInvariant()
    } catch {
        Write-Log ('cache_read_error message=' + $_.Exception.Message)
        return ''
    }
}

function Resolve-DataDiskId {
    $commandLine = Get-WslBootstrapCommandLine
    $liveId = Get-DataDiskIdFromCommandLine -CommandLine $commandLine
    if (-not [string]::IsNullOrWhiteSpace($liveId)) {
        Save-BootstrapCache -DataDiskId $liveId -Source 'live-process'
        return $liveId
    }

    $cachedId = Get-CachedDataDiskId
    if (-not [string]::IsNullOrWhiteSpace($cachedId)) {
        return $cachedId
    }

    throw 'Unable to resolve Docker Desktop data-disk id from live process or cache.'
}

function Invoke-DockerServerVersion {
    if (-not (Test-Path -LiteralPath $DockerExe)) {
        return ''
    }

    try {
        $result = Invoke-BoundedProcess -FilePath $DockerExe -Arguments @(
            'version',
            '--format',
            '{{.Server.Version}}'
        ) -TimeoutSeconds $ProbeTimeoutSeconds
        if ($result.timedOut -or $result.exitCode -ne 0) {
            return ''
        }
        $output = ([string]$result.stdout).Trim()
        if ($output -match '^[0-9]') {
            return $output
        }
        return ''
    } catch {
        return ''
    }
}

function Invoke-WslDockerServerVersion {
    try {
        $pidResult = Invoke-BoundedProcess -FilePath $WslExe -Arguments @(
            '-d',
            $WslDistroName,
            '-u',
            'root',
            '--',
            'pidof',
            'dockerd'
        ) -TimeoutSeconds $ProbeTimeoutSeconds
        if ($pidResult.timedOut -or $pidResult.exitCode -ne 0) {
            return ''
        }
        $pidMatch = [regex]::Match(([string]$pidResult.stdout), '\b\d+\b')
        if (-not $pidMatch.Success) {
            return ''
        }
        $dockerdPid = $pidMatch.Value
        $versionResult = Invoke-BoundedProcess -FilePath $WslExe -Arguments @(
            '-d',
            $WslDistroName,
            '-u',
            'root',
            '--',
            'nsenter',
            '-t',
            $dockerdPid,
            '-m',
            '-n',
            '-u',
            '-i',
            '-p',
            '--',
            '/mnt/host/wsl/docker-desktop/cli-tools/usr/bin/docker',
            '-H',
            'unix:///run/docker.sock',
            'version',
            '--format',
            '{{.Server.Version}}'
        ) -TimeoutSeconds $ProbeTimeoutSeconds
        if ($versionResult.timedOut -or $versionResult.exitCode -ne 0) {
            return ''
        }
        $output = ([string]$versionResult.stdout).Trim()
        if ($output -match '^[0-9]') {
            return $output
        }
        return ''
    } catch {
        return ''
    }
}

function Test-DockerReady {
    $pipeReady = Test-Path '\\.\pipe\dockerDesktopLinuxEngine'
    $windowsServerVersion = Invoke-DockerServerVersion
    $wslServerVersion = ''
    if ([string]::IsNullOrWhiteSpace($windowsServerVersion)) {
        $wslServerVersion = Invoke-WslDockerServerVersion
    }
    $serverVersion = if (-not [string]::IsNullOrWhiteSpace($windowsServerVersion)) {
        $windowsServerVersion
    } else {
        $wslServerVersion
    }
    return [pscustomobject]@{
        pipeReady = $pipeReady
        serverVersion = $serverVersion
        probeSource = if (-not [string]::IsNullOrWhiteSpace($windowsServerVersion)) { 'windows' } elseif (-not [string]::IsNullOrWhiteSpace($wslServerVersion)) { 'wsl' } else { 'none' }
        ready = (
            ($pipeReady -and -not [string]::IsNullOrWhiteSpace($windowsServerVersion)) -or
            -not [string]::IsNullOrWhiteSpace($wslServerVersion)
        )
    }
}

function Start-DockerDesktopIfNeeded {
    if ($SkipDesktopStart) {
        return
    }

    $desktopRunning = @(Get-Process -Name 'Docker Desktop' -ErrorAction SilentlyContinue).Count -gt 0
    if ($desktopRunning) {
        return
    }

    if (-not (Test-Path -LiteralPath $DockerDesktopExe)) {
        Write-Log ('desktop_exe_missing path=' + $DockerDesktopExe)
        return
    }

    Write-Log 'desktop_start requested'
    Start-Process -FilePath $DockerDesktopExe -WindowStyle Hidden
}

function Start-WslBootstrapIfNeeded {
    param([Parameter(Mandatory = $true)][string]$DataDiskId)

    $existing = Get-WslBootstrapCommandLine
    if (-not [string]::IsNullOrWhiteSpace($existing) -and -not $ForceBootstrap) {
        Write-Log ('bootstrap_already_running command=' + $existing)
        return
    }

    $linuxBaseIso = '/c/Program Files/Docker/Docker/resources/docker-desktop.iso'
    $linuxCliIso = '/c/Program Files/Docker/Docker/resources/wsl/docker-wsl-cli.iso'
    $bootstrapCmd = "nohup /usr/local/bin/wsl-bootstrap run --base-image '$linuxBaseIso' --cli-iso '$linuxCliIso' --data-disk $DataDiskId >/tmp/easyregister-docker-engine-bootstrap.log 2>&1 &"

    Write-Log ('bootstrap_launch dataDiskId=' + $DataDiskId)
    $result = Invoke-BoundedProcess -FilePath $WslExe -Arguments @(
        '-d',
        $WslDistroName,
        '--',
        'sh',
        '-lc',
        $bootstrapCmd
    ) -TimeoutSeconds $ProbeTimeoutSeconds
    if ($result.timedOut -or $result.exitCode -ne 0) {
        throw 'Docker Desktop WSL bootstrap command failed or timed out.'
    }
}

$lockHandle = $null
try {
    $lockHandle = [System.IO.File]::Open(
        $lockPath,
        [System.IO.FileMode]::OpenOrCreate,
        [System.IO.FileAccess]::ReadWrite,
        [System.IO.FileShare]::None
    )
} catch {
    Write-Output ('lock_busy path=' + $lockPath)
    exit 0
}

try {
    $writer = New-Object System.IO.StreamWriter($lockHandle)
    $writer.BaseStream.SetLength(0)
    $writer.WriteLine((Get-Date).ToString('o'))
    $writer.WriteLine($PID)
    $writer.Flush()

    Write-Log 'engine_heal_enter'

    $initialStatus = Test-DockerReady
    Write-Log ('initial_ready=' + $initialStatus.ready + ' pipe=' + $initialStatus.pipeReady + ' server=' + $initialStatus.serverVersion + ' source=' + $initialStatus.probeSource)
    if ($initialStatus.ready -and -not $ForceBootstrap) {
        exit 0
    }

    Start-DockerDesktopIfNeeded

    $dataDiskId = Resolve-DataDiskId
    Start-WslBootstrapIfNeeded -DataDiskId $dataDiskId

    for ($attempt = 1; $attempt -le [Math]::Max($ReadyAttempts, 1); $attempt++) {
        Start-Sleep -Seconds ([Math]::Max($ReadySleepSeconds, 1))
        $status = Test-DockerReady
        Write-Log ('probe=' + $attempt + ' ready=' + $status.ready + ' pipe=' + $status.pipeReady + ' server=' + $status.serverVersion + ' source=' + $status.probeSource)
        if ($status.ready) {
            $liveCommand = Get-WslBootstrapCommandLine
            $liveId = Get-DataDiskIdFromCommandLine -CommandLine $liveCommand
            if (-not [string]::IsNullOrWhiteSpace($liveId)) {
                Save-BootstrapCache -DataDiskId $liveId -Source 'post-heal-live-process'
            }
            Write-Log 'engine_heal_success'
            exit 0
        }
    }

    throw 'Docker Desktop engine did not become ready before timeout.'
} finally {
    if ($null -ne $lockHandle) {
        $lockHandle.Dispose()
    }
    Remove-Item -LiteralPath $lockPath -Force -ErrorAction SilentlyContinue
}
