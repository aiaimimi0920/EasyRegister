param(
    [string]$BridgeDir = 'D:\SelfDocker\EasyProtocol\runtime\register-output\easyregister-bridge',
    [string]$TargetDir = 'D:\SelfDocker\EasyRegister\runtime\register-output\openai\converted',
    [string]$PendingDir = 'D:\SelfDocker\EasyRegister\runtime\register-output\openai\pending',
    [string]$FailedOnceDir = 'D:\SelfDocker\EasyRegister\runtime\register-output\openai\failed-once',
    [string]$FailedTwiceDir = 'D:\SelfDocker\EasyRegister\runtime\register-output\openai\failed-twice',
    [string]$ClaimsDir = 'D:\SelfDocker\EasyRegister\runtime\register-output\others\openai-oauth-claims',
    [string]$ArchiveDuplicateBridgeDir = 'D:\SelfDocker\EasyProtocol\runtime\register-output\easyregister-bridge-archive',
    [string]$LogDir = 'D:\SelfDocker\EasyRegister\runtime\bridge-promotion-logs',
    [string]$LockPath = 'D:\SelfDocker\EasyRegister\runtime\bridge-promotion.lock',
    [int]$LockStaleSeconds = 1800,
    [int]$MinSourceAgeSeconds = 20,
    [int]$PendingSeedMaxAgeSeconds = -1,
    [int]$ArchiveDuplicateOlderThanSeconds = 900
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Continue'
$ProgressPreference = 'SilentlyContinue'

New-Item -ItemType Directory -Force -Path $TargetDir | Out-Null
New-Item -ItemType Directory -Force -Path $PendingDir | Out-Null
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
foreach ($optionalDirName in @('FailedOnceDir', 'FailedTwiceDir', 'ClaimsDir', 'ArchiveDuplicateBridgeDir')) {
    $dirPath = Get-Variable -Name $optionalDirName -ValueOnly
    if ([string]::IsNullOrWhiteSpace($dirPath)) {
        continue
    }
    try {
        New-Item -ItemType Directory -Force -Path $dirPath -ErrorAction Stop | Out-Null
    } catch {
        Set-Variable -Name $optionalDirName -Value ''
    }
}
$logPath = Join-Path $LogDir ('bridge-promotion-' + (Get-Date -Format 'yyyyMMdd') + '.log')

function Write-Log {
    param([Parameter(Mandatory = $true)][string]$Message)

    Add-Content -Path $logPath -Value ((Get-Date -Format o) + ' ' + $Message)
}

function Get-ObjectPropertyValue {
    param(
        [Parameter()]$Object,
        [Parameter(Mandatory = $true)][string]$Name
    )

    if ($null -eq $Object) {
        return $null
    }

    $property = $Object.PSObject.Properties[$Name]
    if ($null -eq $property) {
        return $null
    }

    return $property.Value
}

function Get-NestedObjectPropertyValue {
    param(
        [Parameter()]$Object,
        [Parameter(Mandatory = $true)][string[]]$Path
    )

    $current = $Object
    foreach ($segment in $Path) {
        $current = Get-ObjectPropertyValue -Object $current -Name $segment
        if ($null -eq $current) {
            return $null
        }
    }

    return $current
}

function Get-LockInfo {
    param([Parameter(Mandatory = $true)][string]$Path)

    $pidValue = $null
    $startedValue = ''

    if (-not (Test-Path -LiteralPath $Path)) {
        return [pscustomobject]@{
            Exists = $false
            Pid = $null
            Started = ''
            AgeSeconds = 0
        }
    }

    $item = Get-Item -LiteralPath $Path -ErrorAction SilentlyContinue
    $ageSeconds = 0
    if ($item) {
        $ageSeconds = [int][Math]::Floor(((Get-Date).ToUniversalTime() - $item.LastWriteTimeUtc).TotalSeconds)
    }

    try {
        foreach ($line in Get-Content -LiteralPath $Path -ErrorAction Stop) {
            if ($line -match '^pid=(\d+)$') {
                $pidValue = [int]$Matches[1]
                continue
            }
            if ($line -match '^started=(.+)$') {
                $startedValue = $Matches[1]
            }
        }
    } catch {
    }

    return [pscustomobject]@{
        Exists = $true
        Pid = $pidValue
        Started = $startedValue
        AgeSeconds = $ageSeconds
    }
}

function Test-LockProcessAlive {
    param([Nullable[int]]$PidValue)

    if ($null -eq $PidValue) {
        return $false
    }

    return $null -ne (Get-Process -Id $PidValue -ErrorAction SilentlyContinue)
}

function Test-ScriptProcessAliveWithoutMetadata {
    param([Parameter(Mandatory = $true)][string]$ScriptPath)

    $scriptName = [System.IO.Path]::GetFileName($ScriptPath)
    $candidates = Get-CimInstance Win32_Process -Filter "Name='powershell.exe'" -ErrorAction SilentlyContinue
    foreach ($candidate in $candidates) {
        if ($candidate.ProcessId -eq $PID) {
            continue
        }

        $commandLine = [string]$candidate.CommandLine
        if (-not $commandLine) {
            continue
        }

        $mentionsScript = ($commandLine -like "*$ScriptPath*") -or ($commandLine -like "*$scriptName*")
        $isFileInvocation = $commandLine -match '(^|[\s"]) -File([\s"]|$)' -or $commandLine -like "* -File *"
        if ($mentionsScript -and $isFileInvocation) {
            return $true
        }
    }

    return $false
}

function Remove-StaleLockIfNeeded {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$ScriptPath
    )

    $info = Get-LockInfo -Path $Path
    if (-not $info.Exists) {
        return $true
    }

    if (($null -ne $info.Pid) -and (-not (Test-LockProcessAlive -PidValue $info.Pid))) {
        Write-Log ('dead_pid_lock_removed path={0} pid={1} ageSeconds={2}' -f $Path, $info.Pid, $info.AgeSeconds)
        Remove-Item -LiteralPath $Path -Force -ErrorAction SilentlyContinue
        return $true
    }

    if (($null -eq $info.Pid) -and (-not (Test-ScriptProcessAliveWithoutMetadata -ScriptPath $ScriptPath))) {
        Write-Log ('metadata_missing_lock_removed path={0} ageSeconds={1}' -f $Path, $info.AgeSeconds)
        Remove-Item -LiteralPath $Path -Force -ErrorAction SilentlyContinue
        return $true
    }

    if ($info.AgeSeconds -ge $LockStaleSeconds) {
        Write-Log ('stale_lock_removed path={0} ageSeconds={1}' -f $Path, $info.AgeSeconds)
        Remove-Item -LiteralPath $Path -Force -ErrorAction SilentlyContinue
        return $true
    }

    Write-Log ('lock_busy path={0} ageSeconds={1}' -f $Path, $info.AgeSeconds)
    return $false
}

function Acquire-Lock {
    param([Parameter(Mandatory = $true)][string]$Path)

    if (-not (Remove-StaleLockIfNeeded -Path $Path -ScriptPath $PSCommandPath)) {
        return $null
    }

    try {
        $handle = [System.IO.File]::Open(
            $Path,
            [System.IO.FileMode]::Create,
            [System.IO.FileAccess]::ReadWrite,
            [System.IO.FileShare]::None
        )
        $writer = New-Object System.IO.StreamWriter($handle)
        $writer.BaseStream.SetLength(0)
        $writer.WriteLine(('pid={0}' -f $PID))
        $writer.WriteLine(('started={0}' -f (Get-Date -Format o)))
        $writer.Flush()
        return $handle
    } catch {
        $info = Get-LockInfo -Path $Path
        Write-Log ('lock_busy path={0} ageSeconds={1}' -f $Path, $info.AgeSeconds)
        return $null
    }
}

function Release-Lock {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter()]$Handle
    )

    if ($null -ne $Handle) {
        $Handle.Dispose()
    }
    Remove-Item -LiteralPath $Path -Force -ErrorAction SilentlyContinue
}

function Resolve-PendingSeedMaxAgeSeconds {
    if ($PendingSeedMaxAgeSeconds -ge 0) {
        return $PendingSeedMaxAgeSeconds
    }

    foreach ($envName in @('REGISTER_OPENAI_OAUTH_SEED_MAX_AGE_SECONDS', 'REGISTER_SMALL_SUCCESS_SEED_MAX_AGE_SECONDS')) {
        $rawValue = [string][Environment]::GetEnvironmentVariable($envName)
        if (-not [string]::IsNullOrWhiteSpace($rawValue)) {
            $parsedValue = 0
            if ([int]::TryParse($rawValue, [ref]$parsedValue)) {
                return [Math]::Max(0, $parsedValue)
            }
        }
    }

    return 900
}

function Test-ExistingLifecycleCopy {
    param(
        [Parameter(Mandatory = $true)]$SourceItem,
        [Parameter(Mandatory = $true)][string]$SelectedDestination,
        [Parameter(Mandatory = $true)][object[]]$LifecycleRoots
    )

    foreach ($root in $LifecycleRoots) {
        $rootPath = [string](Get-ObjectPropertyValue -Object $root -Name 'Path')
        if (-not $rootPath) {
            continue
        }

        $candidatePath = Join-Path $rootPath $SourceItem.Name
        if (-not (Test-Path -LiteralPath $candidatePath)) {
            continue
        }

        $candidateItem = Get-Item -LiteralPath $candidatePath -ErrorAction SilentlyContinue
        if (-not $candidateItem) {
            continue
        }

        $needsRefresh = ($candidateItem.Length -ne $SourceItem.Length) -or
            ($candidateItem.LastWriteTimeUtc -lt $SourceItem.LastWriteTimeUtc.AddSeconds(-2))
        if ($needsRefresh) {
            continue
        }

        return [pscustomobject]@{
            Exists = $true
            Label = [string](Get-ObjectPropertyValue -Object $root -Name 'Label')
            Path = $candidatePath
        }
    }

    return $null
}

function Try-ArchiveDuplicateBridgeSource {
    param(
        [Parameter(Mandatory = $true)]$SourceItem,
        [Parameter(Mandatory = $true)][double]$AgeSeconds,
        [Parameter(Mandatory = $true)][string]$Reason,
        [Parameter()][string]$Detail = ''
    )

    if ([string]::IsNullOrWhiteSpace($ArchiveDuplicateBridgeDir)) {
        return $false
    }
    if ($ArchiveDuplicateOlderThanSeconds -lt 0) {
        return $false
    }
    if ($AgeSeconds -lt $ArchiveDuplicateOlderThanSeconds) {
        return $false
    }

    $archivePath = Join-Path $ArchiveDuplicateBridgeDir $SourceItem.Name
    try {
        if (Test-Path -LiteralPath $archivePath) {
            Remove-Item -LiteralPath $archivePath -Force -ErrorAction Stop
        }
        Move-Item -LiteralPath $SourceItem.FullName -Destination $archivePath -Force -ErrorAction Stop
        Write-Log ('archived_{0} source={1} archive={2} ageSeconds={3} {4}' -f $Reason, $SourceItem.FullName, $archivePath, [int][Math]::Floor($AgeSeconds), $Detail)
        return $true
    } catch {
        Write-Log ('archive_failed reason={0} source={1} archive={2} error={3}' -f $Reason, $SourceItem.FullName, $archivePath, $_.Exception.Message)
        return $false
    }
}

$lockHandle = Acquire-Lock -Path $LockPath
if ($null -eq $lockHandle) {
    exit 0
}

try {
    $resolvedPendingSeedMaxAgeSeconds = Resolve-PendingSeedMaxAgeSeconds
    $copied = 0
    $refreshed = 0
    $skipped = 0
    $unsettled = 0
    $parseFailed = 0
    $ineligible = 0

    Get-ChildItem -LiteralPath $BridgeDir -Filter '*.json' -File -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime |
        ForEach-Object {
            $sourceItem = $_
            $source = $sourceItem.FullName
            $sourceAgeSeconds = ((Get-Date).ToUniversalTime() - $sourceItem.LastWriteTimeUtc).TotalSeconds
            if ($sourceAgeSeconds -lt $MinSourceAgeSeconds) {
                $unsettled += 1
                Write-Log ('source_unsettled source={0} ageSeconds={1}' -f $source, [int][Math]::Floor($sourceAgeSeconds))
                return
            }

            try {
                $payload = Get-Content -LiteralPath $source -Raw | ConvertFrom-Json
            } catch {
                $parseFailed += 1
                Write-Log ('parse_failed source={0} error={1}' -f $source, $_.Exception.Message)
                return
            }

            $platformOk = [string](Get-NestedObjectPropertyValue -Object $payload -Path @('platformOrganization', 'status')) -eq 'completed'
            $chatOk = [string](Get-NestedObjectPropertyValue -Object $payload -Path @('chatgptLogin', 'status')) -eq 'completed'
            $outcome = [string](Get-ObjectPropertyValue -Object $payload -Name 'outcome')
            $sourceKind = [string](Get-ObjectPropertyValue -Object $payload -Name 'source')
            $mailboxRef = [string](Get-ObjectPropertyValue -Object $payload -Name 'mailboxRef')
            $mailboxSessionId = [string](Get-ObjectPropertyValue -Object $payload -Name 'mailboxSessionId')
            $createdAt = [string](Get-ObjectPropertyValue -Object $payload -Name 'createdAt')
            $accessToken = [string](Get-ObjectPropertyValue -Object $payload -Name 'accessToken')
            $refreshToken = [string](Get-ObjectPropertyValue -Object $payload -Name 'refreshToken')
            $platformAuth = Get-ObjectPropertyValue -Object $payload -Name 'platformAuth'
            $hasProtocolPlatformAuth = $platformAuth -is [System.Management.Automation.PSObject] -or $platformAuth -is [hashtable]
            foreach ($fieldName in @('clientId', 'redirectUri', 'codeVerifier', 'state', 'nonce')) {
                if (-not [string](Get-NestedObjectPropertyValue -Object $payload -Path @('platformAuth', $fieldName))) {
                    $hasProtocolPlatformAuth = $false
                    break
                }
            }

            $platformSmallSuccess = $platformOk -and
                $outcome -eq 'small_success' -and
                $sourceKind -eq 'protocol_small_success' -and
                $accessToken -and
                $refreshToken
            $continuePendingSeed = $outcome -eq 'small_success' -and
                $sourceKind -eq 'protocol_small_success' -and
                $hasProtocolPlatformAuth
            $promotionEligible = ($platformOk -and $chatOk) -or $platformSmallSuccess
            $pendingEligible = $continuePendingSeed -and $mailboxRef -and $mailboxSessionId -and $createdAt
            if ((-not $promotionEligible) -and $pendingEligible -and $resolvedPendingSeedMaxAgeSeconds -gt 0) {
                try {
                    $parsedCreatedAt = [DateTimeOffset]::Parse(
                        $createdAt,
                        [System.Globalization.CultureInfo]::InvariantCulture,
                        [System.Globalization.DateTimeStyles]::AssumeUniversal
                    )
                } catch {
                    $ineligible += 1
                    Write-Log ('skip_pending_seed_invalid_created_at source={0} createdAt={1}' -f $source, $createdAt)
                    return
                }

                $pendingSeedAgeSeconds = [int][Math]::Floor(((Get-Date).ToUniversalTime() - $parsedCreatedAt.UtcDateTime).TotalSeconds)
                if ($pendingSeedAgeSeconds -gt $resolvedPendingSeedMaxAgeSeconds) {
                    $skipped += 1
                    Write-Log ('skip_pending_seed_too_old source={0} ageSeconds={1} maxAgeSeconds={2}' -f $source, $pendingSeedAgeSeconds, $resolvedPendingSeedMaxAgeSeconds)
                    return
                }
            }
            if (-not (($promotionEligible -or $pendingEligible) -and $mailboxRef -and $mailboxSessionId -and $createdAt)) {
                $ineligible += 1
                Write-Log ('skip_ineligible source={0} outcome={1} sourceKind={2} platformOk={3} chatOk={4} hasAccess={5} hasRefresh={6} hasPlatformAuth={7} hasMailboxRef={8} hasMailboxSessionId={9} hasCreatedAt={10}' -f $source, $outcome, $sourceKind, $platformOk, $chatOk, [bool]$accessToken, [bool]$refreshToken, $hasProtocolPlatformAuth, [bool]$mailboxRef, [bool]$mailboxSessionId, [bool]$createdAt)
                return
            }

            $destinationRoot = if ($promotionEligible) { $TargetDir } else { $PendingDir }
            $destinationLabel = if ($promotionEligible) { 'converted' } else { 'pending' }
            $dest = Join-Path $destinationRoot $sourceItem.Name
            $existingLifecycleCopy = Test-ExistingLifecycleCopy -SourceItem $sourceItem -SelectedDestination $dest -LifecycleRoots @(
                [pscustomobject]@{ Label = 'converted'; Path = $TargetDir },
                [pscustomobject]@{ Label = 'pending'; Path = $PendingDir },
                [pscustomobject]@{ Label = 'failed-once'; Path = $FailedOnceDir },
                [pscustomobject]@{ Label = 'failed-twice'; Path = $FailedTwiceDir },
                [pscustomobject]@{ Label = 'claims'; Path = $ClaimsDir }
            )
            if ($null -ne $existingLifecycleCopy) {
                $skipped += 1
                $archived = Try-ArchiveDuplicateBridgeSource `
                    -SourceItem $sourceItem `
                    -AgeSeconds $sourceAgeSeconds `
                    -Reason 'existing_lifecycle' `
                    -Detail ('route={0} existingLabel={1} existingPath={2}' -f $destinationLabel, $existingLifecycleCopy.Label, $existingLifecycleCopy.Path)
                if (-not $archived) {
                    Write-Log ('skip_existing_lifecycle route={0} source={1} existingLabel={2} existingPath={3}' -f $destinationLabel, $source, $existingLifecycleCopy.Label, $existingLifecycleCopy.Path)
                }
                return
            }
            $copyMode = 'new'
            if (Test-Path -LiteralPath $dest) {
                $destItem = Get-Item -LiteralPath $dest -ErrorAction SilentlyContinue
                if ($destItem) {
                    $needsRefresh = ($destItem.Length -ne $sourceItem.Length) -or
                        ($destItem.LastWriteTimeUtc -lt $sourceItem.LastWriteTimeUtc.AddSeconds(-2))
                    if (-not $needsRefresh) {
                        $skipped += 1
                        return
                    }
                    $copyMode = 'refresh'
                }
            }

            $tmpDest = '{0}.tmp-{1}-{2}' -f $dest, $PID, ([Guid]::NewGuid().ToString('N'))
            try {
                Copy-Item -LiteralPath $source -Destination $tmpDest -Force -ErrorAction Stop
                $tmpItem = Get-Item -LiteralPath $tmpDest -ErrorAction Stop
                if ($tmpItem.Length -ne $sourceItem.Length) {
                    throw ('temporary copy length mismatch: source={0} temp={1}' -f $sourceItem.Length, $tmpItem.Length)
                }
                Move-Item -LiteralPath $tmpDest -Destination $dest -Force -ErrorAction Stop
            } catch {
                Remove-Item -LiteralPath $tmpDest -Force -ErrorAction SilentlyContinue
                Write-Log ('copy_failed mode={0} source={1} dest={2} error={3}' -f $copyMode, $source, $dest, $_.Exception.Message)
                return
            }

            if ($copyMode -eq 'refresh') {
                $refreshed += 1
            } else {
                $copied += 1
            }
            Write-Log ('{0} route={1} source={2} dest={3} sourceLength={4} outcome={5}' -f $copyMode, $destinationLabel, $source, $dest, $sourceItem.Length, $outcome)
        }

    Write-Log ('summary new_copied={0} refreshed={1} skipped_existing={2} unsettled={3} parse_failed={4} ineligible={5}' -f $copied, $refreshed, $skipped, $unsettled, $parseFailed, $ineligible)
} finally {
    Release-Lock -Path $LockPath -Handle $lockHandle
}
