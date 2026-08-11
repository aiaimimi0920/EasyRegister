$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

. "$PSScriptRoot\..\scripts\lib\nas-sync-lock-utils.ps1"

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

$lockText = "pid=2436`nstarted=2026-07-06T13:25:03.8703517+08:00`n"
$parsed = Parse-SyncLockContent -Text $lockText
Assert-Equal $parsed.Pid 2436 'Parse-SyncLockContent should read pid'
Assert-Equal $parsed.Started.ToString('o') '2026-07-06T13:25:03.8703517+08:00' 'Parse-SyncLockContent should read started timestamp'

$busy = Get-SyncLockDisposition -AgeSeconds 120 -StaleSeconds 600 -LockPid 2436 -CommandLine '"powershell.exe" -File sync-easyregister-results-to-nas.ps1 -Once' -ExpectedScriptMarker 'sync-easyregister-results-to-nas.ps1'
Assert-Equal $busy 'busy' 'fresh lock should stay busy'

$killAndRemove = Get-SyncLockDisposition -AgeSeconds 1200 -StaleSeconds 600 -LockPid 2436 -CommandLine '"powershell.exe" -File sync-easyregister-results-to-nas.ps1 -Once' -ExpectedScriptMarker 'sync-easyregister-results-to-nas.ps1'
Assert-Equal $killAndRemove 'kill_and_remove' 'stale lock from matching sync process should be killed and removed'

$removeOnly = Get-SyncLockDisposition -AgeSeconds 1200 -StaleSeconds 600 -LockPid 2436 -CommandLine '"powershell.exe" -File some-other-script.ps1' -ExpectedScriptMarker 'sync-easyregister-results-to-nas.ps1'
Assert-Equal $removeOnly 'remove_only' 'stale lock from unrelated process should only be removed'

$roundTrip = New-SyncLockContent -LockPid 9527 -Started ([DateTimeOffset]'2026-07-06T13:44:00+08:00').DateTime
if ($roundTrip -notmatch 'pid=9527') {
    throw 'ASSERT FAILED: New-SyncLockContent should contain pid'
}

Write-Output 'PASS test_sync_nas_lock'
