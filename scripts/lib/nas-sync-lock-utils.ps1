Set-StrictMode -Version Latest

function Parse-SyncLockContent {
    param([string]$Text)

    $lockPid = $null
    $started = $null

    foreach ($line in @($Text -split "`r?`n")) {
        if ($line -match '^pid=(\d+)$') {
            $lockPid = [int]$Matches[1]
            continue
        }
        if ($line -match '^started=(.+)$') {
            try {
                $started = [DateTimeOffset]::Parse($Matches[1])
            } catch {
                $started = $null
            }
        }
    }

    return [pscustomobject]@{
        Pid = $lockPid
        Started = $started
    }
}

function Get-SyncLockDisposition {
    param(
        [Parameter(Mandatory = $true)][double]$AgeSeconds,
        [Parameter(Mandatory = $true)][int]$StaleSeconds,
        [int]$LockPid,
        [string]$CommandLine,
        [Parameter(Mandatory = $true)][string]$ExpectedScriptMarker
    )

    if ($AgeSeconds -lt $StaleSeconds) {
        return 'busy'
    }

    if ($LockPid -and -not [string]::IsNullOrWhiteSpace($CommandLine) -and $CommandLine -like ("*{0}*" -f $ExpectedScriptMarker)) {
        return 'kill_and_remove'
    }

    return 'remove_only'
}

function New-SyncLockContent {
    param(
        [Parameter(Mandatory = $true)][int]$LockPid,
        [datetime]$Started = (Get-Date)
    )

    return ("pid={0}`nstarted={1}`n" -f $LockPid, $Started.ToString('o'))
}
