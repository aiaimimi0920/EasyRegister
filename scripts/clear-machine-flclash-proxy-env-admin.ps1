[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [string]$ExpectedProxy = "http://127.0.0.1:42344"
)

$ErrorActionPreference = "Stop"

$principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw "This script must be run from an elevated PowerShell window (Run as Administrator)."
}

$regPath = "HKLM:\SYSTEM\CurrentControlSet\Control\Session Manager\Environment"
$backupDir = Join-Path $env:TEMP "socket-pressure-fix-backups"
New-Item -ItemType Directory -Path $backupDir -Force | Out-Null

$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$backupFile = Join-Path $backupDir "machine-environment-$timestamp.reg"
& reg.exe export "HKLM\SYSTEM\CurrentControlSet\Control\Session Manager\Environment" $backupFile /y | Out-Null

$targetNames = @("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY")
$removed = @()
$kept = @()

foreach ($name in $targetNames) {
    $prop = Get-ItemProperty -LiteralPath $regPath -Name $name -ErrorAction SilentlyContinue
    if ($null -eq $prop) {
        continue
    }

    $value = $prop.$name
    if ($value -eq $ExpectedProxy) {
        if ($PSCmdlet.ShouldProcess("Machine environment variable $name", "Remove value $value")) {
            Remove-ItemProperty -LiteralPath $regPath -Name $name -ErrorAction Stop
            $removed += "$name=$value"
        }
    } else {
        $kept += "$name=$value"
    }
}

# Broadcast the environment change to new processes without forcing a reboot.
Add-Type -Namespace Win32 -Name NativeMethods -MemberDefinition @"
[System.Runtime.InteropServices.DllImport("user32.dll", SetLastError=true, CharSet=System.Runtime.InteropServices.CharSet.Auto)]
public static extern System.IntPtr SendMessageTimeout(
    System.IntPtr hWnd,
    uint Msg,
    System.IntPtr wParam,
    string lParam,
    uint fuFlags,
    uint uTimeout,
    out System.IntPtr lpdwResult);
"@

$result = [IntPtr]::Zero
[void][Win32.NativeMethods]::SendMessageTimeout(
    [IntPtr]0xffff,
    0x1A,
    [IntPtr]::Zero,
    "Environment",
    0x0002,
    5000,
    [ref]$result
)

[pscustomobject]@{
    BackupFile = $backupFile
    Removed = $removed
    KeptDifferentValues = $kept
    CurrentMachineProxyVars = (
        [Environment]::GetEnvironmentVariables("Machine").GetEnumerator() |
            Where-Object { $_.Key -match "proxy" } |
            ForEach-Object { "$($_.Key)=$($_.Value)" }
    )
} | Format-List
