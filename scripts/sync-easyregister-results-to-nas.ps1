param(
    [switch]$Once,
    [int]$IntervalSeconds = 300
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

. (Join-Path $PSScriptRoot 'lib\nas-sync-lock-utils.ps1')

$Root = 'D:\SelfDocker\EasyRegister'
$Runtime = Join-Path $Root 'runtime'
$StateDir = Join-Path $Runtime 'nas-sync-state'
$LogDir = Join-Path $Runtime 'nas-sync-logs'
$CredentialPath = Join-Path $StateDir 'nas-smb.credential.xml'
$ActivationPath = Join-Path $StateDir 'activation-utc.txt'
$LockPath = Join-Path $StateDir 'sync.lock'
$LockStaleSeconds = 600
$MaxPortablePathLength = 240
$NasDriveName = 'Y'
$NasShare = '\\192.168.15.200\home'
$SyncScriptMarker = 'sync-easyregister-results-to-nas.ps1'

New-Item -ItemType Directory -Force -Path $StateDir, $LogDir | Out-Null

$Pairs = @(
    @{
        Name = 'codex'
        Local = Join-Path $Runtime 'register-output\codex'
        NasSub = 'oauth\codex'
        Index = Join-Path $StateDir 'codex-index.txt'
        SkipState = Join-Path $StateDir 'codex-skip-detail-state.txt'
    },
    @{
        Name = 'openai'
        Local = Join-Path $Runtime 'register-output\openai'
        NasSub = 'oauth\openai'
        Index = Join-Path $StateDir 'openai-index.txt'
        SkipState = Join-Path $StateDir 'openai-skip-detail-state.txt'
    }
)

function Write-Log {
    param([Parameter(Mandatory = $true)][string]$Message)

    $log = Join-Path $LogDir ('owned-sync-{0}.log' -f (Get-Date -Format 'yyyyMMdd'))
    ('{0} {1}' -f (Get-Date -Format o), $Message) | Add-Content -Encoding UTF8 -Path $log
}

function Get-PlainPassword {
    param([Parameter(Mandatory = $true)][securestring]$Secure)

    $ptr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Secure)
    try {
        return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($ptr)
    } finally {
        if ($ptr -ne [IntPtr]::Zero) {
            [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ptr)
        }
    }
}

function Ensure-NasDrive {
    if (-not (Test-Path -LiteralPath $CredentialPath)) {
        throw "NAS credential file missing: $CredentialPath"
    }

    $driveRoot = ('{0}:\' -f $NasDriveName)
    if (Test-Path -LiteralPath $driveRoot) {
        return $driveRoot
    }

    $cred = Import-Clixml -LiteralPath $CredentialPath
    $password = Get-PlainPassword $cred.Password
    try {
        cmd /c "net use ${NasDriveName}: /delete /y >nul 2>&1" | Out-Null
        cmd /c "net use ${NasDriveName}: $NasShare /user:$($cred.UserName) $password /persistent:no" | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "net use failed with exit code $LASTEXITCODE"
        }
    } finally {
        $password = $null
    }

    if (-not (Test-Path -LiteralPath $driveRoot)) {
        throw "NAS drive $driveRoot unavailable after net use"
    }

    return $driveRoot
}

function Get-ActivationUtc {
    if (-not (Test-Path -LiteralPath $ActivationPath)) {
        (Get-Date).ToUniversalTime().ToString('o') |
            Set-Content -LiteralPath $ActivationPath -Encoding ASCII
    }

    return [DateTime]::Parse((Get-Content -LiteralPath $ActivationPath -Raw).Trim()).ToUniversalTime()
}

function Get-RelativePath {
    param(
        [Parameter(Mandatory = $true)][string]$Base,
        [Parameter(Mandatory = $true)][string]$FullName
    )

    $baseFull = [IO.Path]::GetFullPath($Base).TrimEnd('\') + '\'
    $full = [IO.Path]::GetFullPath($FullName)
    return $full.Substring($baseFull.Length).Replace('\', '/')
}

function New-StringSet {
    return ,(New-Object 'System.Collections.Generic.HashSet[string]')
}

function Read-Index {
    param([Parameter(Mandatory = $true)][string]$Path)

    $set = New-StringSet
    if (Test-Path -LiteralPath $Path) {
        Get-Content -LiteralPath $Path | ForEach-Object {
            $value = $_.Trim()
            if ($value) {
                [void]$set.Add($value)
            }
        }
    }
    return ,$set
}

function Read-StateSet {
    param([Parameter(Mandatory = $true)][string]$Path)

    return Read-Index -Path $Path
}

function Write-Index {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)]$Set
    )

    $tmp = "$Path.tmp"
    [IO.File]::WriteAllLines($tmp, @($Set | Sort-Object), (New-Object Text.UTF8Encoding($false)))
    Move-Item -LiteralPath $tmp -Destination $Path -Force
}

function Write-StateSet {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)]$Set
    )

    Write-Index -Path $Path -Set $Set
}

function Acquire-SyncLock {
    $now = Get-Date
    if (Test-Path -LiteralPath $LockPath) {
        $item = Get-Item -LiteralPath $LockPath -ErrorAction SilentlyContinue
        if ($item) {
            $ageSeconds = [int](($now.ToUniversalTime() - $item.LastWriteTimeUtc).TotalSeconds)
            $lockText = Get-Content -LiteralPath $LockPath -Raw -ErrorAction SilentlyContinue
            $lockMeta = Parse-SyncLockContent -Text $lockText
            $commandLine = $null
            if ($lockMeta.Pid) {
                try {
                    $proc = Get-CimInstance Win32_Process -Filter ("ProcessId = {0}" -f $lockMeta.Pid) -ErrorAction Stop
                    $commandLine = [string]$proc.CommandLine
                } catch {
                    $commandLine = $null
                }
            }

            $disposition = Get-SyncLockDisposition `
                -AgeSeconds $ageSeconds `
                -StaleSeconds $LockStaleSeconds `
                -LockPid $lockMeta.Pid `
                -CommandLine $commandLine `
                -ExpectedScriptMarker $SyncScriptMarker

            if ($disposition -eq 'busy') {
                Write-Log ("lock_busy path={0} ageSeconds={1}" -f $LockPath, $ageSeconds)
                return $false
            }

            if ($disposition -eq 'kill_and_remove' -and $lockMeta.Pid) {
                try {
                    Stop-Process -Id $lockMeta.Pid -Force -ErrorAction Stop
                    Write-Log ("lock_stale_killed pid={0} ageSeconds={1}" -f $lockMeta.Pid, $ageSeconds)
                } catch {
                    Write-Log ("lock_stale_kill_failed pid={0} ageSeconds={1} message={2}" -f $lockMeta.Pid, $ageSeconds, $_.Exception.Message)
                }
            }
        }
        Write-Log ("lock_stale_removed path={0}" -f $LockPath)
        Remove-Item -LiteralPath $LockPath -Force -ErrorAction SilentlyContinue
    }

    try {
        [IO.File]::WriteAllText(
            $LockPath,
            (New-SyncLockContent -LockPid $PID -Started (Get-Date)),
            (New-Object Text.UTF8Encoding($false))
        )
        return $true
    } catch {
        Write-Log ("lock_acquire_failed path={0} message={1}" -f $LockPath, $_.Exception.Message)
        return $false
    }
}

function Update-SyncLockHeartbeat {
    if (-not (Test-Path -LiteralPath $LockPath)) {
        return
    }

    try {
        [IO.File]::WriteAllText(
            $LockPath,
            (New-SyncLockContent -LockPid $PID -Started (Get-Date)),
            (New-Object Text.UTF8Encoding($false))
        )
    } catch {
        Write-Log ("lock_heartbeat_failed path={0} message={1}" -f $LockPath, $_.Exception.Message)
    }
}

function Release-SyncLock {
    Remove-Item -LiteralPath $LockPath -Force -ErrorAction SilentlyContinue
}

function Add-SyncError {
    param(
        [Parameter(Mandatory = $true)]$Errors,
        [Parameter(Mandatory = $true)][string]$Kind,
        [Parameter(Mandatory = $true)][string]$RelativePath,
        [Parameter(Mandatory = $true)][string]$Message
    )

    $Errors.Add([pscustomobject]@{
            kind = $Kind
            rel = $RelativePath
            message = $Message
        }) | Out-Null
}

function Add-SyncSkip {
    param(
        [Parameter(Mandatory = $true)]$Skips,
        [Parameter(Mandatory = $true)][string]$Kind,
        [Parameter(Mandatory = $true)][string]$RelativePath,
        [Parameter(Mandatory = $true)][string]$Message
    )

    $Skips.Add([pscustomobject]@{
            kind = $Kind
            rel = $RelativePath
            message = $Message
        }) | Out-Null
}

function Sync-Pair {
    param(
        [Parameter(Mandatory = $true)]$Pair,
        [Parameter(Mandatory = $true)][string]$NasRoot,
        [Parameter(Mandatory = $true)][DateTime]$ActivationUtc
    )

    New-Item -ItemType Directory -Force -Path $Pair.Local | Out-Null
    $nasPath = Join-Path $NasRoot $Pair.NasSub
    New-Item -ItemType Directory -Force -Path $nasPath | Out-Null

    $previous = Read-Index $Pair.Index
    $current = New-StringSet
    $copied = 0
    $deleted = 0
    $errors = New-Object 'System.Collections.Generic.List[object]'
    $skips = New-Object 'System.Collections.Generic.List[object]'
    $copiedSmallRels = New-Object 'System.Collections.Generic.List[string]'
    $skipDetailState = Read-StateSet $Pair.SkipState
    $newSkipDetailCount = 0
    $processed = 0

    Write-Log ("pair={0} begin" -f $Pair.Name)

    Get-ChildItem -LiteralPath $Pair.Local -Recurse -Force -ErrorAction SilentlyContinue |
        Where-Object { -not $_.PSIsContainer } |
        ForEach-Object {
            $rel = Get-RelativePath $Pair.Local $_.FullName
            $owned = ($_.LastWriteTimeUtc -ge $ActivationUtc) -or $previous.Contains($rel)
            if (-not $owned) {
                return
            }

            $processed++
            if (($processed % 100) -eq 0) {
                Update-SyncLockHeartbeat
            }

            $sourcePath = $_.FullName
            if ($sourcePath.Length -gt $MaxPortablePathLength) {
                Add-SyncSkip $skips 'source_path_too_long' $rel ("pathLength={0}" -f $sourcePath.Length)
                return
            }

            $dest = Join-Path $nasPath ($rel -replace '/', '\')
            if ($dest.Length -gt $MaxPortablePathLength) {
                Add-SyncSkip $skips 'dest_path_too_long' $rel ("pathLength={0}" -f $dest.Length)
                return
            }

            $destDir = Split-Path -Parent $dest
            try {
                if (-not (Test-Path -LiteralPath $sourcePath)) {
                    Add-SyncError $errors 'source_missing' $rel 'source disappeared before copy'
                    return
                }

                New-Item -ItemType Directory -Force -Path $destDir | Out-Null
                $needsCopy = $true
                if (Test-Path -LiteralPath $dest) {
                    $sourceItem = Get-Item -LiteralPath $sourcePath -ErrorAction Stop
                    $destItem = Get-Item -LiteralPath $dest -ErrorAction Stop
                    $needsCopy = ($destItem.Length -ne $sourceItem.Length) -or
                        ($destItem.LastWriteTimeUtc -lt $sourceItem.LastWriteTimeUtc.AddSeconds(-2))
                }

                if ($needsCopy) {
                    $sourceItem = Get-Item -LiteralPath $sourcePath -ErrorAction Stop
                    $tmpDest = "{0}.tmp-{1}-{2}" -f $dest, $PID, ([Guid]::NewGuid().ToString('N'))
                    try {
                        Copy-Item -LiteralPath $sourcePath -Destination $tmpDest -Force -ErrorAction Stop
                        $tmpItem = Get-Item -LiteralPath $tmpDest -ErrorAction Stop
                        if ($tmpItem.Length -ne $sourceItem.Length) {
                            throw "temporary copy length mismatch: source=$($sourceItem.Length) temp=$($tmpItem.Length)"
                        }
                        Move-Item -LiteralPath $tmpDest -Destination $dest -Force -ErrorAction Stop
                    } catch {
                        Remove-Item -LiteralPath $tmpDest -Force -ErrorAction SilentlyContinue
                        throw
                    }
                    $copied++
                    if ([IO.Path]::GetFileName($rel) -like 'small-*.json') {
                        $copiedSmallRels.Add($rel) | Out-Null
                    }
                }

                [void]$current.Add($rel)
            } catch {
                if (Test-Path -LiteralPath $dest) {
                    try {
                        $partial = Get-Item -LiteralPath $dest -ErrorAction Stop
                        if ($partial.Length -eq 0) {
                            Remove-Item -LiteralPath $dest -Force -ErrorAction Stop
                        }
                    } catch {
                        Add-SyncError $errors 'partial_cleanup_failed' $rel $_.Exception.Message
                    }
                }
                Add-SyncError $errors 'copy_failed' $rel $_.Exception.Message
            }
        }

    foreach ($rel in $previous) {
        if (-not $current.Contains($rel)) {
            $dest = Join-Path $nasPath ($rel -replace '/', '\')
            try {
                if (Test-Path -LiteralPath $dest) {
                    Remove-Item -LiteralPath $dest -Force -ErrorAction Stop
                    $deleted++
                }
            } catch {
                Add-SyncError $errors 'delete_failed' $rel $_.Exception.Message
            }
        }
    }

    Write-Index $Pair.Index $current
    Write-Log ("pair={0} owned={1} copied={2} deleted={3} errors={4} skipped={5}" -f $Pair.Name, $current.Count, $copied, $deleted, $errors.Count, $skips.Count)

    foreach ($rel in $copiedSmallRels | Select-Object -First 10) {
        Write-Log ("pair={0} copied_small rel={1}" -f $Pair.Name, $rel)
    }
    foreach ($errorItem in $errors | Select-Object -First 20) {
        Write-Log ("pair={0} {1} rel={2} message={3}" -f $Pair.Name, $errorItem.kind, $errorItem.rel, $errorItem.message)
    }
    foreach ($skipItem in $skips) {
        $skipStateKey = '{0}|{1}|{2}' -f $skipItem.kind, $skipItem.rel, $skipItem.message
        if ($skipDetailState.Add($skipStateKey)) {
            $newSkipDetailCount++
            if ($newSkipDetailCount -le 5) {
                Write-Log ("pair={0} skipped_{1} rel={2} message={3}" -f $Pair.Name, $skipItem.kind, $skipItem.rel, $skipItem.message)
            }
        }
    }
    if ($newSkipDetailCount -gt 0) {
        Write-Log ("pair={0} new_skip_detail_entries={1}" -f $Pair.Name, $newSkipDetailCount)
    }
    Write-StateSet -Path $Pair.SkipState -Set $skipDetailState
}

function Sync-Once {
    if (-not (Acquire-SyncLock)) {
        return
    }

    try {
        $nasRoot = Ensure-NasDrive
        $activationUtc = Get-ActivationUtc
        foreach ($pair in $Pairs) {
            Update-SyncLockHeartbeat
            Sync-Pair $pair $nasRoot $activationUtc
            Update-SyncLockHeartbeat
        }
    } finally {
        Release-SyncLock
    }
}

if ($Once) {
    Sync-Once
    exit 0
}

while ($true) {
    try {
        Sync-Once
    } catch {
        Write-Log ('ERROR ' + $_.Exception.Message)
    }
    Start-Sleep -Seconds $IntervalSeconds
}
