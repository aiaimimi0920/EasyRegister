param(
    [string]$OutputDirHost = "",
    [string]$CredentialRootHost = "",
    [string]$CodexRootDirHost = "",
    [string]$OpenaiRootDirHost = "",
    [string]$CodexRootDockerSource = "",
    [string]$OpenaiRootDockerSource = "",
    [string]$CodexRootDockerVolume = "",
    [string]$OpenaiRootDockerVolume = "",
    [string]$ImportCode = "",
    [string]$BootstrapFile = "",
    [string]$ComposeFile = "",
    [string]$ProtocolRegisterOutputDirHost = "",
    [string]$ProtocolContainerName = "easy-protocol",
    [string]$ProtocolRegisterOutputContainerPath = "/shared/register-output",
    [string]$ProtocolOutputMirrorContainerPath = "/shared/protocol-register-output",
    [string]$ProtocolBridgeSubdir = "easyregister-bridge",
    [string]$ProtocolBridgeDockerVolume = "",
    [string]$MailboxServiceBaseUrl = "http://easy-email:8080",
    [string]$MailboxServiceApiKey = "",
    [string]$MailboxDomainPool = "",
    [string]$MailboxDomainBlacklist = "",
    [string]$MailboxProviderBlacklist = "",
    [int]$MailboxDomainConsecutiveFailureBlacklistThreshold = 500,
    [int]$MailboxDomainBlacklistMinAttempts = 50,
    [double]$MailboxDomainBlacklistFailureRate = 95,
    [int]$MailboxEmailOtpFailureBlacklistThreshold = 6,
    [int]$MailboxEmailOtpProviderFailureBlacklistThreshold = 6,
    [int]$MailboxDynamicBlacklistTtlSeconds = 21600,
    [ValidateSet("true", "false")]
    [string]$MailboxDynamicBlacklistExhaustedFallback = "",
    [string]$MailboxBusinessPoliciesJson = "",
    [string]$SmsServiceBaseUrl = "http://easy-sms:8080",
    [string]$SmsServiceApiKey = "",
    [string]$SmsSelectionPlanTimeoutSeconds = "",
    [string]$SmsSelectionPlanAttempts = "",
    [string]$PhoneTerminalRetryAttempts = "",
    [string]$PhoneSmsCodeWaitRetryAttempts = "",
    [string]$SmsBusinessKey = "",
    [string]$SmsProviderBlacklist = "",
    [string]$SmsAllowPaid = "",
    [string]$SmsAllowReuse = "",
    [string]$SmsMaxBindingsPerPhone = "",
    [string]$SmsCountryCodes = "",
    [string]$SmsSelectionMode = "",
    [string]$SmsBusinessPoliciesJson = "",
    [string]$SmsTerminalInvalidPhoneBlacklistSeconds = "",
    [string]$EasyProxyBaseUrl = "http://easy-proxy:29888",
    [string]$EasyProxyRuntimeHost = "easy-proxy",
    [string]$EasyProxyManagementUsername = "easyproxy",
    [string]$EasyProxyManagementPassword = "",
    [string]$EasyProxyApiKey = "",
    [string]$TeamAuthDirHost = "C:\Users\vmjcv\.cli-proxy-api\team",
    [string]$CodexFreeDirHost = "C:\Users\vmjcv\.cli-proxy-api\free",
    [string]$CodexTeamDirHost = "C:\Users\vmjcv\.cli-proxy-api\team",
    [string]$CodexTeamInputDirHost = "C:\Users\vmjcv\.cli-proxy-api\team",
    [string]$CodexTeamMotherInputDirHost = "",
    [int]$WorkerCount = 10,
    [int]$MainConcurrencyLimit = 5,
    [int]$ContinueConcurrencyLimit = 2,
    [int]$TeamConcurrencyLimit = 1,
    [double]$OpenaiUploadPercent = 0,
    [double]$CodexFreeUploadPercent = 0,
    [double]$CodexTeamUploadPercent = 0,
    [double]$CodexPlusUploadPercent = 0,
    [string]$DashboardPortHost = "19790",
    [ValidateSet("true", "false")]
    [string]$DashboardEnabled = "true",
    [string]$ComposeProjectName = "easy-register",
    [string]$ContainerName = "easy-register",
    [string]$InstanceId = "easy-register",
    [string]$NetworkAlias = "easy-register",
    [string]$DockerNetworkName = "EasyAiMi",
    [ValidateSet("true", "false")]
    [string]$DockerNetworkExternal = "true",
    [ValidateSet("Auto", "Junction", "SymbolicLink")]
    [string]$LinkType = "Auto",
    [switch]$ForceLinks,
    [switch]$NoBuild,
    [string]$Image = "",
    [switch]$Pull,
    [switch]$NoDetach,
    [switch]$MaterializeOnly,
    [string]$RepoOwner = "aiaimimi0920",
    [string]$RepoName = "EasyRegister",
    [string]$RepoRef = "main",
    [ValidateSet("branch", "tag")]
    [string]$RepoRefKind = "branch",
    [string]$RepoArchiveUrl = "",
    [string]$RepoCacheRoot = "",
    [switch]$ForceRefreshRepo,
    [switch]$ResolveRepoOnly,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Services
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Invoke-NativeCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string]$FilePath,
        [string[]]$Arguments = @()
    )

    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        & $FilePath @Arguments
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    if ($exitCode -ne 0) {
        throw "Native command failed with exit code ${exitCode}: $FilePath $($Arguments -join ' ')"
    }
}

$deployBoundParameters = @{}
foreach ($entry in $PSBoundParameters.GetEnumerator()) {
    $deployBoundParameters[[string]$entry.Key] = $true
}

$defaultEasyProxyBaseUrl = "http://easy-proxy:29888"
$defaultDashboardEnabled = "true"
$defaultDashboardControlToken = "easyregister-dashboard-local-token"
$defaultDashboardListen = "0.0.0.0:9790"
$defaultMailboxDomainBlacklistCsv = 'coolkid.icu,shaole.me,cpu.edu.kg,tmail.bio,do4.tech'
$defaultMailboxProviderBlacklistCsv = ''
$defaultMailboxBusinessPoliciesJson = '{"default":{"explicitBlacklistDomains":["coolkid.icu","shaole.me","cpu.edu.kg","tmail.bio","do4.tech"],"providerBlacklist":[]},"openai":{"explicitBlacklistDomains":["coolkid.icu","shaole.me","cpu.edu.kg","tmail.bio","do4.tech"],"providerBlacklist":[]}}'
$defaultSmsServiceBaseUrl = "http://easy-sms:8080"
$defaultSmsSelectionPlanTimeoutSeconds = ""
$defaultSmsSelectionPlanAttempts = ""
$defaultPhoneTerminalRetryAttempts = "1"
$defaultPhoneSmsCodeWaitRetryAttempts = "1"
$defaultSmsBusinessKey = "openai"
$defaultSmsProviderBlacklist = ""
$defaultSmsAllowPaid = "true"
$defaultSmsAllowReuse = "false"
$defaultSmsMaxBindingsPerPhone = "1"
$defaultSmsCountryCodes = ""
$defaultSmsSelectionMode = "balanced"
$defaultSmsBusinessPoliciesJson = '{"default":{"enabled":false,"providerBlacklist":[],"allowPaid":true},"openai":{"enabled":true,"providerBlacklist":[],"allowPaid":true,"allowReuse":false,"maxBindingsPerPhone":1,"countryCodes":[],"selectionMode":"balanced"}}'
$defaultSmsTerminalInvalidPhoneBlacklistSeconds = "21600"

function Resolve-AbsolutePath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [string]$BaseDir
    )

    if ([System.IO.Path]::IsPathRooted($Path)) {
        return [System.IO.Path]::GetFullPath($Path)
    }
    return [System.IO.Path]::GetFullPath((Join-Path $BaseDir $Path))
}

function Test-RepoLayout {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Root,
        [Parameter(Mandatory = $true)]
        [string[]]$RequiredRelativePaths
    )

    foreach ($relativePath in $RequiredRelativePaths) {
        if (-not (Test-Path -LiteralPath (Join-Path $Root $relativePath))) {
            return $false
        }
    }
    return $true
}

function Write-ComposeEnvFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [hashtable]$Values
    )

    $lines = New-Object System.Collections.Generic.List[string]
    foreach ($key in ($Values.Keys | Sort-Object)) {
        $name = [string]$key
        if ([string]::IsNullOrWhiteSpace($name)) {
            continue
        }
        $rawValue = $Values[$key]
        $value = if ($null -eq $rawValue) { "" } else { [string]$rawValue }
        $lines.Add("$name=$value")
    }
    $parent = Split-Path -Parent $Path
    if (-not [string]::IsNullOrWhiteSpace($parent)) {
        New-Item -ItemType Directory -Force -Path $parent | Out-Null
    }
    Set-Content -LiteralPath $Path -Value $lines -Encoding ASCII
}

function Get-LinkTargetPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    try {
        $resolved = [System.IO.Directory]::ResolveLinkTarget($Path, $false)
        if ($null -ne $resolved) {
            return [System.IO.Path]::GetFullPath($resolved.FullName)
        }
    } catch {
    }

    $item = Get-Item -LiteralPath $Path -Force -ErrorAction Stop
    $candidates = @()
    if ($item.PSObject.Properties.Name -contains "LinkTarget") {
        $candidates += $item.LinkTarget
    }
    if ($item.PSObject.Properties.Name -contains "Target") {
        $candidates += $item.Target
    }
    foreach ($candidate in $candidates) {
        if ($null -eq $candidate) {
            continue
        }
        foreach ($value in @($candidate)) {
            $text = [string]$value
            if ([string]::IsNullOrWhiteSpace($text)) {
                continue
            }
            if (-not [System.IO.Path]::IsPathRooted($text)) {
                $text = Join-Path (Split-Path -Parent $Path) $text
            }
            return [System.IO.Path]::GetFullPath($text)
        }
    }
    return $null
}

function Convert-HostPathToContainerMirrorPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    $normalized = [System.IO.Path]::GetFullPath($Path)
    if ($normalized -match '^(?<drive>[A-Za-z]):\\(?<rest>.*)$') {
        $drive = $matches['drive'].ToLowerInvariant()
        $rest = ($matches['rest'] -replace '\\', '/').TrimStart('/')
        if ([string]::IsNullOrWhiteSpace($rest)) {
            return "/mnt/host/$drive"
        }
        return "/mnt/host/$drive/$rest"
    }
    if ($normalized.StartsWith('\\')) {
        $trimmed = $normalized.TrimStart('\')
        $parts = $trimmed -split '\\+' | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
        if ($parts.Count -ge 2) {
            return "/mnt/host/unc/$($parts -join '/')"
        }
    }
    throw "Cannot derive container mirror path for host path: $normalized"
}

function Convert-HostPathToComposeSource {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    return ([System.IO.Path]::GetFullPath($Path) -replace '\\', '/')
}

function Convert-DockerBindSourceToHostPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Source
    )

    $text = $Source.Trim()
    if ($text -match '^/(?:run/desktop/)?mnt/host/(?<drive>[A-Za-z])(?:/(?<rest>.*))?$') {
        $drive = $matches['drive'].ToUpperInvariant()
        $rest = if ($matches.ContainsKey('rest')) { [string]$matches['rest'] } else { "" }
        $rest = $rest -replace '/', '\'
        if ([string]::IsNullOrWhiteSpace($rest)) {
            return "$($drive):\"
        }
        return "$($drive):\$rest"
    }
    if ($text -match '^/host_mnt/(?<drive>[A-Za-z])(?:/(?<rest>.*))?$') {
        $drive = $matches['drive'].ToUpperInvariant()
        $rest = if ($matches.ContainsKey('rest')) { [string]$matches['rest'] } else { "" }
        $rest = $rest -replace '/', '\'
        if ([string]::IsNullOrWhiteSpace($rest)) {
            return "$($drive):\"
        }
        return "$($drive):\$rest"
    }
    return $text
}

function Join-ContainerPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Root,
        [Parameter(Mandatory = $true)]
        [string]$Child
    )

    $normalizedRoot = $Root.Trim().TrimEnd('/', '\')
    $normalizedChild = $Child.Trim().TrimStart('/', '\')
    if ([string]::IsNullOrWhiteSpace($normalizedChild)) {
        return $normalizedRoot
    }
    return "$normalizedRoot/$normalizedChild"
}

function Get-DockerBindSourceForContainerTarget {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ContainerName,
        [Parameter(Mandatory = $true)]
        [string]$TargetPath
    )

    if ([string]::IsNullOrWhiteSpace($ContainerName) -or [string]::IsNullOrWhiteSpace($TargetPath)) {
        return ""
    }
    try {
        $mountsJson = & docker inspect --format '{{json .Mounts}}' $ContainerName 2>$null
        if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($mountsJson)) {
            return ""
        }
        $mounts = $mountsJson | ConvertFrom-Json
        foreach ($mount in @($mounts)) {
            $destination = ""
            if ($mount.PSObject.Properties.Name -contains "Destination") {
                $destination = [string]$mount.Destination
            }
            if ($destination -ne $TargetPath) {
                continue
            }
            if ($mount.PSObject.Properties.Name -contains "Source") {
                return Convert-DockerBindSourceToHostPath -Source ([string]$mount.Source)
            }
        }
    } catch {
        return ""
    }
    return ""
}

function Get-DockerBindSourceForProtocolTarget {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ContainerName,
        [Parameter(Mandatory = $true)]
        [string]$TargetPath
    )

    $directSource = Get-DockerBindSourceForContainerTarget -ContainerName $ContainerName -TargetPath $TargetPath
    if (-not [string]::IsNullOrWhiteSpace($directSource)) {
        return $directSource
    }

    try {
        $providerNamePattern = "$ContainerName-python-*"
        $containerNames = & docker ps --format '{{.Names}}' 2>$null
        if ($LASTEXITCODE -ne 0) {
            return ""
        }
        $providerSources = New-Object System.Collections.Generic.List[string]
        foreach ($providerContainerName in @($containerNames)) {
            $candidate = [string]$providerContainerName
            if ([string]::IsNullOrWhiteSpace($candidate) -or $candidate -notlike $providerNamePattern) {
                continue
            }
            $providerSource = Get-DockerBindSourceForContainerTarget -ContainerName $candidate -TargetPath $TargetPath
            if (-not [string]::IsNullOrWhiteSpace($providerSource)) {
                $providerSources.Add($providerSource) | Out-Null
            }
        }
        if ($providerSources.Count -gt 0) {
            $selected = $providerSources | Group-Object | Sort-Object -Property Count -Descending | Select-Object -First 1
            return [string]$selected.Name
        }
    } catch {
        return ""
    }
    return ""
}

function New-ProtocolBridgeMountOverrideFile {
    param(
        [string]$ProtocolRegisterOutputDirHost,
        [Parameter(Mandatory = $true)]
        [string]$ProtocolOutputMirrorContainerPath,
        [Parameter(Mandatory = $true)]
        [string]$ProtocolBridgeSubdir,
        [string]$ProtocolBridgeDockerVolume = "",
        [Parameter(Mandatory = $true)]
        [string]$OverridePath
    )

    $hasBindMount = -not [string]::IsNullOrWhiteSpace($ProtocolRegisterOutputDirHost)
    $hasBridgeVolume = -not [string]::IsNullOrWhiteSpace($ProtocolBridgeDockerVolume)
    if (-not $hasBindMount -and -not $hasBridgeVolume) {
        if (Test-Path -LiteralPath $OverridePath) {
            Remove-Item -LiteralPath $OverridePath -Force
        }
        return $null
    }

    $resolvedSource = ""
    if ($hasBindMount) {
        $resolvedSource = [System.IO.Path]::GetFullPath($ProtocolRegisterOutputDirHost)
        New-Item -ItemType Directory -Force -Path $resolvedSource | Out-Null
    }
    $normalizedBridgeVolume = $ProtocolBridgeDockerVolume.Trim()
    if ($hasBridgeVolume -and $normalizedBridgeVolume -notmatch '^[A-Za-z0-9][A-Za-z0-9_.-]*$') {
        throw "Invalid protocol bridge Docker volume name: $normalizedBridgeVolume"
    }
    $mirrorTarget = $ProtocolOutputMirrorContainerPath.TrimEnd('/', '\')
    $bridgeTarget = Join-ContainerPath -Root $mirrorTarget -Child $ProtocolBridgeSubdir

    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add('services:')
    $lines.Add('  easy-register:')
    $lines.Add('    volumes:')
    if ($hasBindMount) {
        $lines.Add('      - type: bind')
        $lines.Add(("        source: ""{0}""" -f (Convert-HostPathToComposeSource -Path $resolvedSource)))
        $lines.Add(("        target: ""{0}""" -f $mirrorTarget))
    }
    if ($hasBridgeVolume) {
        $lines.Add('      - type: volume')
        $lines.Add(("        source: ""{0}""" -f $normalizedBridgeVolume))
        $lines.Add(("        target: ""{0}""" -f $bridgeTarget))
        $lines.Add('volumes:')
        $lines.Add(("  {0}:" -f $normalizedBridgeVolume))
        $lines.Add('    external: true')
    }
    Set-Content -LiteralPath $OverridePath -Value $lines -Encoding ASCII

    return [pscustomobject]@{
        SourcePath = $resolvedSource
        TargetPath = $mirrorTarget
        BridgeVolume = $normalizedBridgeVolume
        BridgeTargetPath = $bridgeTarget
    }
}

function Ensure-ResultPoolDirectories {
    param(
        [string]$OutputDirHost,
        [string]$CodexRootDirHost,
        [string]$OpenaiRootDirHost
    )

    if (-not [string]::IsNullOrWhiteSpace($OutputDirHost)) {
        New-Item -ItemType Directory -Force -Path (Join-Path $OutputDirHost "others") | Out-Null
    }

    if (-not [string]::IsNullOrWhiteSpace($CodexRootDirHost)) {
        foreach ($relative in @(
            "free",
            "team",
            "plus",
            "team-input",
            "team-mother-input"
        )) {
            New-Item -ItemType Directory -Force -Path (Join-Path $CodexRootDirHost $relative) | Out-Null
        }
    }

    if (-not [string]::IsNullOrWhiteSpace($OpenaiRootDirHost)) {
        foreach ($relative in @(
            "pending",
            "converted",
            "failed-once",
            "failed-twice"
        )) {
            New-Item -ItemType Directory -Force -Path (Join-Path $OpenaiRootDirHost $relative) | Out-Null
        }
    }
}

function Convert-ResultPoolMountSource {
    param(
        [Parameter(Mandatory = $true)]
        [string]$HostPath,
        [string]$DockerSource = ""
    )

    if (-not [string]::IsNullOrWhiteSpace($DockerSource)) {
        return $DockerSource
    }
    return Convert-HostPathToComposeSource -Path $HostPath
}

function New-ResultPoolMountSpec {
    param(
        [Parameter(Mandatory = $true)]
        [string]$HostPath,
        [Parameter(Mandatory = $true)]
        [string]$TargetPath,
        [string]$DockerSource = "",
        [string]$DockerVolume = ""
    )

    if ((-not [string]::IsNullOrWhiteSpace($DockerSource)) -and (-not [string]::IsNullOrWhiteSpace($DockerVolume))) {
        throw "Specify either DockerSource or DockerVolume for ${TargetPath}, not both."
    }

    if (-not [string]::IsNullOrWhiteSpace($DockerVolume)) {
        return [pscustomobject]@{
            Type       = "volume"
            SourcePath = $HostPath
            Source     = $DockerVolume
            TargetPath = $TargetPath
        }
    }

    return [pscustomobject]@{
        Type       = "bind"
        SourcePath = $HostPath
        Source     = Convert-ResultPoolMountSource -HostPath $HostPath -DockerSource $DockerSource
        TargetPath = $TargetPath
    }
}

function New-ResultPoolMountOverrideFile {
    param(
        [string]$CodexRootDirHost,
        [string]$OpenaiRootDirHost,
        [string]$CodexRootDockerSource = "",
        [string]$OpenaiRootDockerSource = "",
        [string]$CodexRootDockerVolume = "",
        [string]$OpenaiRootDockerVolume = "",
        [Parameter(Mandatory = $true)]
        [string]$OverridePath
    )

    $mounts = @()
    if (-not [string]::IsNullOrWhiteSpace($CodexRootDirHost)) {
        $mounts += New-ResultPoolMountSpec `
            -HostPath $CodexRootDirHost `
            -TargetPath "/shared/register-output/codex" `
            -DockerSource $CodexRootDockerSource `
            -DockerVolume $CodexRootDockerVolume
    }
    if (-not [string]::IsNullOrWhiteSpace($OpenaiRootDirHost)) {
        $mounts += New-ResultPoolMountSpec `
            -HostPath $OpenaiRootDirHost `
            -TargetPath "/shared/register-output/openai" `
            -DockerSource $OpenaiRootDockerSource `
            -DockerVolume $OpenaiRootDockerVolume
    }

    if ($mounts.Count -eq 0) {
        if (Test-Path -LiteralPath $OverridePath) {
            Remove-Item -LiteralPath $OverridePath -Force
        }
        return @()
    }

    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add('services:')
    $lines.Add('  easy-register:')
    $lines.Add('    volumes:')
    foreach ($mount in $mounts) {
        $lines.Add(("      - type: {0}" -f $mount.Type))
        $lines.Add(("        source: ""{0}""" -f $mount.Source))
        $lines.Add(("        target: ""{0}""" -f $mount.TargetPath))
    }
    $volumeMounts = @($mounts | Where-Object { $_.Type -eq "volume" })
    if ($volumeMounts.Count -gt 0) {
        $lines.Add('volumes:')
        foreach ($mount in $volumeMounts) {
            $lines.Add(("  {0}:" -f $mount.Source))
            $lines.Add('    external: true')
        }
    }
    Set-Content -LiteralPath $OverridePath -Value $lines -Encoding ASCII
    return $mounts
}

function New-AliasMountOverrideFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$OutputDirHost,
        [Parameter(Mandatory = $true)]
        [string]$OverridePath
    )

    $aliasMappings = @(
        'codex/free',
        'codex/team',
        'codex/plus',
        'codex/team-input',
        'codex/team-mother-input'
    )

    $mounts = @()
    foreach ($relative in $aliasMappings) {
        $localPath = Resolve-AbsolutePath -Path $relative -BaseDir $OutputDirHost
        if (-not (Test-Path -LiteralPath $localPath)) {
            continue
        }
        $item = Get-Item -LiteralPath $localPath -Force -ErrorAction Stop
        $isReparsePoint = [bool]($item.Attributes -band [IO.FileAttributes]::ReparsePoint)
        if (-not $isReparsePoint) {
            continue
        }
        $targetPath = Get-LinkTargetPath -Path $localPath
        if ([string]::IsNullOrWhiteSpace($targetPath)) {
            continue
        }
        $normalizedLocal = [System.IO.Path]::GetFullPath($localPath)
        $normalizedTarget = [System.IO.Path]::GetFullPath($targetPath)
        if ($normalizedLocal -eq $normalizedTarget) {
            continue
        }
        $mounts += [pscustomobject]@{
            RelativePath = $relative
            SourcePath = $normalizedTarget
            TargetPath = Convert-HostPathToContainerMirrorPath -Path $normalizedTarget
        }
    }

    if ($mounts.Count -eq 0) {
        if (Test-Path -LiteralPath $OverridePath) {
            Remove-Item -LiteralPath $OverridePath -Force
        }
        return @()
    }

    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add('services:')
    $lines.Add('  easy-register:')
    $lines.Add('    volumes:')
    foreach ($mount in $mounts) {
        $lines.Add('      - type: bind')
        $lines.Add(("        source: ""{0}""" -f (Convert-HostPathToComposeSource -Path $mount.SourcePath)))
        $lines.Add(("        target: ""{0}""" -f $mount.TargetPath))
    }
    Set-Content -LiteralPath $OverridePath -Value $lines -Encoding ASCII
    return $mounts
}

function Read-DotEnvFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    $values = @{}
    foreach ($line in Get-Content -LiteralPath $Path) {
        $trimmed = $line.Trim()
        if ([string]::IsNullOrWhiteSpace($trimmed) -or $trimmed.StartsWith('#') -or -not $line.Contains('=')) {
            continue
        }
        $key, $value = $line.Split('=', 2)
        $normalizedKey = $key.Trim()
        if ([string]::IsNullOrWhiteSpace($normalizedKey)) {
            continue
        }
        $values[$normalizedKey] = $value
    }
    return $values
}

function Get-RepoArchiveUrlValue {
    param(
        [string]$Owner,
        [string]$Name,
        [string]$Ref,
        [string]$Kind,
        [string]$ExplicitUrl
    )

    if (-not [string]::IsNullOrWhiteSpace($ExplicitUrl)) {
        return $ExplicitUrl
    }
    if ($Kind -eq "tag") {
        return "https://codeload.github.com/$Owner/$Name/zip/refs/tags/$Ref"
    }
    return "https://codeload.github.com/$Owner/$Name/zip/refs/heads/$Ref"
}

function Ensure-RepoRoot {
    param(
        [Parameter(Mandatory = $true)]
        [string]$LauncherRoot,
        [Parameter(Mandatory = $true)]
        [string]$Owner,
        [Parameter(Mandatory = $true)]
        [string]$Name,
        [Parameter(Mandatory = $true)]
        [string]$Ref,
        [Parameter(Mandatory = $true)]
        [string]$RefKind,
        [Parameter(Mandatory = $true)]
        [string[]]$RequiredRelativePaths,
        [string]$ArchiveUrl = "",
        [string]$CacheRoot = "",
        [switch]$ForceRefresh
    )

    if (Test-RepoLayout -Root $LauncherRoot -RequiredRelativePaths $RequiredRelativePaths) {
        return [pscustomobject]@{
            RepoRoot = $LauncherRoot
            Source = "local"
            ArchiveUrl = $null
        }
    }

    $sanitizedRef = ($Ref -replace '[^A-Za-z0-9._-]', '_')
    $resolvedCacheRoot = if ([string]::IsNullOrWhiteSpace($CacheRoot)) {
        Join-Path $LauncherRoot ".repo-cache\$Name-$RefKind-$sanitizedRef"
    } else {
        Resolve-AbsolutePath -Path $CacheRoot -BaseDir $LauncherRoot
    }
    $archiveUrlValue = Get-RepoArchiveUrlValue -Owner $Owner -Name $Name -Ref $Ref -Kind $RefKind -ExplicitUrl $ArchiveUrl
    $repoRoot = Join-Path $resolvedCacheRoot "repo"

    $refreshCachedBranch = [string]::IsNullOrWhiteSpace($CacheRoot) -and ($RefKind -eq "branch") -and (Test-Path -LiteralPath $repoRoot)
    if (($ForceRefresh -or $refreshCachedBranch) -and (Test-Path -LiteralPath $resolvedCacheRoot)) {
        if ($refreshCachedBranch -and -not $ForceRefresh) {
            Write-Host "[deploy-host] refreshing cached branch repository: $Name/$Ref" -ForegroundColor Cyan
        }
        Remove-Item -LiteralPath $resolvedCacheRoot -Recurse -Force
    }

    if (-not (Test-RepoLayout -Root $repoRoot -RequiredRelativePaths $RequiredRelativePaths)) {
        New-Item -ItemType Directory -Force -Path $resolvedCacheRoot | Out-Null
        $archivePath = Join-Path $resolvedCacheRoot "$Name-$sanitizedRef.zip"
        $expandedRoot = Join-Path $resolvedCacheRoot "expanded"

        if (Test-Path -LiteralPath $archivePath) {
            Remove-Item -LiteralPath $archivePath -Force
        }
        if (Test-Path -LiteralPath $expandedRoot) {
            Remove-Item -LiteralPath $expandedRoot -Recurse -Force
        }
        if (Test-Path -LiteralPath $repoRoot) {
            Remove-Item -LiteralPath $repoRoot -Recurse -Force
        }

        Write-Host "[deploy-host] downloading repository archive: $archiveUrlValue" -ForegroundColor Cyan
        $previousProgressPreference = $global:ProgressPreference
        $global:ProgressPreference = "SilentlyContinue"
        try {
            Invoke-WebRequest -Uri $archiveUrlValue -OutFile $archivePath
        } finally {
            $global:ProgressPreference = $previousProgressPreference
        }
        Expand-Archive -LiteralPath $archivePath -DestinationPath $expandedRoot -Force

        $extractedRoot = Get-ChildItem -LiteralPath $expandedRoot -Directory | Select-Object -First 1
        if ($null -eq $extractedRoot) {
            throw "Repository archive did not contain an extractable root directory: $archiveUrlValue"
        }

        Move-Item -LiteralPath $extractedRoot.FullName -Destination $repoRoot
    }

    if (-not (Test-RepoLayout -Root $repoRoot -RequiredRelativePaths $RequiredRelativePaths)) {
        throw "Bootstrapped repository root is missing required paths: $repoRoot"
    }

    return [pscustomobject]@{
        RepoRoot = $repoRoot
        Source = "bootstrapped"
        ArchiveUrl = $archiveUrlValue
    }
}

$launcherRoot = Split-Path -Parent $PSCommandPath
$repoInfo = Ensure-RepoRoot `
    -LauncherRoot $launcherRoot `
    -Owner $RepoOwner `
    -Name $RepoName `
    -Ref $RepoRef `
    -RefKind $RepoRefKind `
    -RequiredRelativePaths @("README.md", "scripts\deploy-compose.ps1", "scripts\materialize-output-links.ps1") `
    -ArchiveUrl $RepoArchiveUrl `
    -CacheRoot $RepoCacheRoot `
    -ForceRefresh:$ForceRefreshRepo

if ($ResolveRepoOnly) {
    [pscustomobject]@{
        LauncherRoot = $launcherRoot
        RepoRoot = $repoInfo.RepoRoot
        Source = $repoInfo.Source
        ArchiveUrl = $repoInfo.ArchiveUrl
    } | Format-List
    return
}

$repoRoot = $repoInfo.RepoRoot
$resolvedOutputDirHost = if ([string]::IsNullOrWhiteSpace($OutputDirHost)) {
    Resolve-AbsolutePath -Path "runtime\register-output" -BaseDir $launcherRoot
} else {
    Resolve-AbsolutePath -Path $OutputDirHost -BaseDir $launcherRoot
}
$resolvedComposeFile = if ([string]::IsNullOrWhiteSpace($ComposeFile)) {
    Resolve-AbsolutePath -Path "compose\docker-compose.yaml" -BaseDir $repoRoot
} elseif ([System.IO.Path]::IsPathRooted($ComposeFile)) {
    Resolve-AbsolutePath -Path $ComposeFile -BaseDir $launcherRoot
} elseif (Test-Path -LiteralPath (Join-Path $repoRoot $ComposeFile)) {
    Resolve-AbsolutePath -Path $ComposeFile -BaseDir $repoRoot
} else {
    Resolve-AbsolutePath -Path $ComposeFile -BaseDir $launcherRoot
}

if (-not [string]::IsNullOrWhiteSpace($ImportCode) -and -not [string]::IsNullOrWhiteSpace($BootstrapFile)) {
    throw 'Specify either ImportCode or BootstrapFile, not both.'
}

$bootstrapRoot = Resolve-AbsolutePath -Path '.bootstrap' -BaseDir $launcherRoot
$bootstrapPath = Join-Path $bootstrapRoot 'easyregister-r2-bootstrap.json'
$importedRuntimeEnvPath = Join-Path $bootstrapRoot 'easyregister.runtime.imported.env'
$importedRuntimeValues = @{}

if (-not [string]::IsNullOrWhiteSpace($ImportCode) -or -not [string]::IsNullOrWhiteSpace($BootstrapFile)) {
    New-Item -ItemType Directory -Force -Path $bootstrapRoot | Out-Null
    if (-not [string]::IsNullOrWhiteSpace($ImportCode)) {
        & (Join-Path $repoRoot 'scripts\write-runtime-r2-bootstrap.ps1') `
            -ImportCode $ImportCode `
            -OutputPath $bootstrapPath
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to materialize EasyRegister bootstrap file from import code with exit code $LASTEXITCODE"
        }
    } else {
        $resolvedBootstrapFile = Resolve-AbsolutePath -Path $BootstrapFile -BaseDir $launcherRoot
        if (-not (Test-Path -LiteralPath $resolvedBootstrapFile)) {
            throw "Bootstrap file not found: $resolvedBootstrapFile"
        }
        Copy-Item -LiteralPath $resolvedBootstrapFile -Destination $bootstrapPath -Force
    }

    Invoke-NativeCommand -FilePath "python" -Arguments @(
        (Join-Path $repoRoot 'scripts\bootstrap-runtime-config.py'),
        "--bootstrap-path",
        $bootstrapPath,
        "--runtime-env-path",
        $importedRuntimeEnvPath
    )
    $importedRuntimeValues = Read-DotEnvFile -Path $importedRuntimeEnvPath
}

function Resolve-EnvValue {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ParameterName,
        [Parameter(Mandatory = $true)]
        [string]$RuntimeKey,
        [string]$Fallback = '',
        [switch]$UseFallbackWhenBlank
    )

    if ($deployBoundParameters.ContainsKey($ParameterName)) {
        $value = [string](Get-Variable -Name $ParameterName -ValueOnly)
        if ($UseFallbackWhenBlank -and [string]::IsNullOrWhiteSpace($value)) {
            return $Fallback
        }
        return $value
    }
    if ($importedRuntimeValues.ContainsKey($RuntimeKey)) {
        $value = [string]$importedRuntimeValues[$RuntimeKey]
        if ($UseFallbackWhenBlank -and [string]::IsNullOrWhiteSpace($value)) {
            return $Fallback
        }
        return $value
    }
    return $Fallback
}

$resolvedMailboxServiceBaseUrl = Resolve-EnvValue -ParameterName 'MailboxServiceBaseUrl' -RuntimeKey 'MAILBOX_SERVICE_BASE_URL' -Fallback 'http://easy-email:8080'
$resolvedMailboxServiceApiKey = Resolve-EnvValue -ParameterName 'MailboxServiceApiKey' -RuntimeKey 'MAILBOX_SERVICE_API_KEY' -Fallback ''
$resolvedMailboxDomainPool = Resolve-EnvValue -ParameterName 'MailboxDomainPool' -RuntimeKey 'REGISTER_MAILBOX_DOMAIN_POOL' -Fallback ''
$resolvedMailboxDomainBlacklist = Resolve-EnvValue -ParameterName 'MailboxDomainBlacklist' -RuntimeKey 'REGISTER_MAILBOX_DOMAIN_BLACKLIST' -Fallback $defaultMailboxDomainBlacklistCsv
$resolvedMailboxProviderBlacklist = Resolve-EnvValue -ParameterName 'MailboxProviderBlacklist' -RuntimeKey 'REGISTER_MAILBOX_PROVIDER_BLACKLIST' -Fallback $defaultMailboxProviderBlacklistCsv
$resolvedMailboxDomainConsecutiveFailureBlacklistThreshold = Resolve-EnvValue -ParameterName 'MailboxDomainConsecutiveFailureBlacklistThreshold' -RuntimeKey 'REGISTER_MAILBOX_DOMAIN_CONSECUTIVE_FAILURE_BLACKLIST_THRESHOLD' -Fallback '500'
$resolvedMailboxDomainBlacklistMinAttempts = Resolve-EnvValue -ParameterName 'MailboxDomainBlacklistMinAttempts' -RuntimeKey 'REGISTER_MAILBOX_DOMAIN_BLACKLIST_MIN_ATTEMPTS' -Fallback '50'
$resolvedMailboxDomainBlacklistFailureRate = Resolve-EnvValue -ParameterName 'MailboxDomainBlacklistFailureRate' -RuntimeKey 'REGISTER_MAILBOX_DOMAIN_BLACKLIST_FAILURE_RATE' -Fallback '95'
$resolvedMailboxEmailOtpFailureBlacklistThreshold = Resolve-EnvValue -ParameterName 'MailboxEmailOtpFailureBlacklistThreshold' -RuntimeKey 'REGISTER_MAILBOX_EMAIL_OTP_FAILURE_BLACKLIST_THRESHOLD' -Fallback '6'
$resolvedMailboxEmailOtpProviderFailureBlacklistThreshold = Resolve-EnvValue -ParameterName 'MailboxEmailOtpProviderFailureBlacklistThreshold' -RuntimeKey 'REGISTER_MAILBOX_EMAIL_OTP_PROVIDER_FAILURE_BLACKLIST_THRESHOLD' -Fallback '6'
$resolvedMailboxDynamicBlacklistTtlSeconds = Resolve-EnvValue -ParameterName 'MailboxDynamicBlacklistTtlSeconds' -RuntimeKey 'REGISTER_MAILBOX_DYNAMIC_BLACKLIST_TTL_SECONDS' -Fallback '21600'
$resolvedMailboxDynamicBlacklistExhaustedFallback = Resolve-EnvValue -ParameterName 'MailboxDynamicBlacklistExhaustedFallback' -RuntimeKey 'REGISTER_MAILBOX_DYNAMIC_BLACKLIST_EXHAUSTED_FALLBACK' -Fallback 'false' -UseFallbackWhenBlank
$resolvedMailboxBusinessPoliciesJson = Resolve-EnvValue -ParameterName 'MailboxBusinessPoliciesJson' -RuntimeKey 'REGISTER_MAILBOX_BUSINESS_POLICIES_JSON' -Fallback $defaultMailboxBusinessPoliciesJson
$resolvedSmsServiceBaseUrl = Resolve-EnvValue -ParameterName 'SmsServiceBaseUrl' -RuntimeKey 'SMS_SERVICE_BASE_URL' -Fallback $defaultSmsServiceBaseUrl -UseFallbackWhenBlank
$resolvedSmsServiceApiKey = Resolve-EnvValue -ParameterName 'SmsServiceApiKey' -RuntimeKey 'SMS_SERVICE_API_KEY' -Fallback ''
$resolvedSmsSelectionPlanTimeoutSeconds = Resolve-EnvValue -ParameterName 'SmsSelectionPlanTimeoutSeconds' -RuntimeKey 'SMS_SERVICE_SELECTION_PLAN_TIMEOUT_SECONDS' -Fallback $defaultSmsSelectionPlanTimeoutSeconds
$resolvedSmsSelectionPlanAttempts = Resolve-EnvValue -ParameterName 'SmsSelectionPlanAttempts' -RuntimeKey 'SMS_SERVICE_SELECTION_PLAN_ATTEMPTS' -Fallback $defaultSmsSelectionPlanAttempts
$resolvedPhoneTerminalRetryAttempts = Resolve-EnvValue -ParameterName 'PhoneTerminalRetryAttempts' -RuntimeKey 'REGISTER_PHONE_VERIFICATION_TERMINAL_RETRY_ATTEMPTS' -Fallback $defaultPhoneTerminalRetryAttempts -UseFallbackWhenBlank
$resolvedPhoneSmsCodeWaitRetryAttempts = Resolve-EnvValue -ParameterName 'PhoneSmsCodeWaitRetryAttempts' -RuntimeKey 'REGISTER_PHONE_VERIFICATION_SMS_CODE_WAIT_RETRY_ATTEMPTS' -Fallback $defaultPhoneSmsCodeWaitRetryAttempts -UseFallbackWhenBlank
$resolvedSmsBusinessKey = Resolve-EnvValue -ParameterName 'SmsBusinessKey' -RuntimeKey 'REGISTER_SMS_BUSINESS_KEY' -Fallback $defaultSmsBusinessKey -UseFallbackWhenBlank
$resolvedSmsProviderBlacklist = Resolve-EnvValue -ParameterName 'SmsProviderBlacklist' -RuntimeKey 'REGISTER_SMS_PROVIDER_BLACKLIST' -Fallback $defaultSmsProviderBlacklist -UseFallbackWhenBlank
$resolvedSmsAllowPaid = Resolve-EnvValue -ParameterName 'SmsAllowPaid' -RuntimeKey 'REGISTER_SMS_ALLOW_PAID' -Fallback $defaultSmsAllowPaid -UseFallbackWhenBlank
$resolvedSmsAllowReuse = Resolve-EnvValue -ParameterName 'SmsAllowReuse' -RuntimeKey 'REGISTER_SMS_ALLOW_REUSE' -Fallback $defaultSmsAllowReuse -UseFallbackWhenBlank
$resolvedSmsMaxBindingsPerPhone = Resolve-EnvValue -ParameterName 'SmsMaxBindingsPerPhone' -RuntimeKey 'REGISTER_SMS_MAX_BINDINGS_PER_PHONE' -Fallback $defaultSmsMaxBindingsPerPhone -UseFallbackWhenBlank
$resolvedSmsCountryCodes = Resolve-EnvValue -ParameterName 'SmsCountryCodes' -RuntimeKey 'REGISTER_SMS_COUNTRY_CODES' -Fallback $defaultSmsCountryCodes
$resolvedSmsSelectionMode = Resolve-EnvValue -ParameterName 'SmsSelectionMode' -RuntimeKey 'REGISTER_SMS_SELECTION_MODE' -Fallback $defaultSmsSelectionMode -UseFallbackWhenBlank
$resolvedSmsBusinessPoliciesJson = Resolve-EnvValue -ParameterName 'SmsBusinessPoliciesJson' -RuntimeKey 'REGISTER_SMS_BUSINESS_POLICIES_JSON' -Fallback $defaultSmsBusinessPoliciesJson -UseFallbackWhenBlank
$resolvedSmsTerminalInvalidPhoneBlacklistSeconds = Resolve-EnvValue -ParameterName 'SmsTerminalInvalidPhoneBlacklistSeconds' -RuntimeKey 'REGISTER_SMS_TERMINAL_INVALID_PHONE_BLACKLIST_SECONDS' -Fallback $defaultSmsTerminalInvalidPhoneBlacklistSeconds -UseFallbackWhenBlank
$resolvedEasyProxyBaseUrl = Resolve-EnvValue -ParameterName 'EasyProxyBaseUrl' -RuntimeKey 'EASY_PROXY_BASE_URL' -Fallback 'http://easy-proxy:29888'
$resolvedEasyProxyRuntimeHost = Resolve-EnvValue -ParameterName 'EasyProxyRuntimeHost' -RuntimeKey 'EASY_PROXY_RUNTIME_HOST' -Fallback 'easy-proxy' -UseFallbackWhenBlank
$resolvedEasyProxyApiKey = Resolve-EnvValue -ParameterName 'EasyProxyApiKey' -RuntimeKey 'EASY_PROXY_API_KEY' -Fallback ''
$resolvedEasyProxyManagementUsername = Resolve-EnvValue -ParameterName 'EasyProxyManagementUsername' -RuntimeKey 'EASY_PROXY_MANAGEMENT_USERNAME' -Fallback 'easyproxy' -UseFallbackWhenBlank
$resolvedEasyProxyManagementPassword = Resolve-EnvValue -ParameterName 'EasyProxyManagementPassword' -RuntimeKey 'EASY_PROXY_MANAGEMENT_PASSWORD' -Fallback $resolvedEasyProxyApiKey
$resolvedWorkerCount = Resolve-EnvValue -ParameterName 'WorkerCount' -RuntimeKey 'REGISTER_WORKER_COUNT' -Fallback '10'
$resolvedMainConcurrencyLimit = Resolve-EnvValue -ParameterName 'MainConcurrencyLimit' -RuntimeKey 'REGISTER_MAIN_CONCURRENCY_LIMIT' -Fallback '5'
$resolvedContinueConcurrencyLimit = Resolve-EnvValue -ParameterName 'ContinueConcurrencyLimit' -RuntimeKey 'REGISTER_CONTINUE_CONCURRENCY_LIMIT' -Fallback '2'
$resolvedTeamConcurrencyLimit = Resolve-EnvValue -ParameterName 'TeamConcurrencyLimit' -RuntimeKey 'REGISTER_TEAM_CONCURRENCY_LIMIT' -Fallback '1'
$resolvedOpenaiUploadPercent = Resolve-EnvValue -ParameterName 'OpenaiUploadPercent' -RuntimeKey 'REGISTER_OPENAI_UPLOAD_PERCENT' -Fallback '0'
$resolvedCodexFreeUploadPercent = Resolve-EnvValue -ParameterName 'CodexFreeUploadPercent' -RuntimeKey 'REGISTER_CODEX_FREE_UPLOAD_PERCENT' -Fallback '0'
$resolvedCodexTeamUploadPercent = Resolve-EnvValue -ParameterName 'CodexTeamUploadPercent' -RuntimeKey 'REGISTER_CODEX_TEAM_UPLOAD_PERCENT' -Fallback '0'
$resolvedCodexPlusUploadPercent = Resolve-EnvValue -ParameterName 'CodexPlusUploadPercent' -RuntimeKey 'REGISTER_CODEX_PLUS_UPLOAD_PERCENT' -Fallback '0'
$resolvedCredentialRootHost = Resolve-EnvValue -ParameterName 'CredentialRootHost' -RuntimeKey 'REGISTER_CREDENTIAL_ROOT_HOST' -Fallback ''
$resolvedCodexRootDirHost = Resolve-EnvValue -ParameterName 'CodexRootDirHost' -RuntimeKey 'REGISTER_CODEX_ROOT_DIR_HOST' -Fallback ''
$resolvedOpenaiRootDirHost = Resolve-EnvValue -ParameterName 'OpenaiRootDirHost' -RuntimeKey 'REGISTER_OPENAI_ROOT_DIR_HOST' -Fallback ''
$resolvedCodexRootDockerSource = Resolve-EnvValue -ParameterName 'CodexRootDockerSource' -RuntimeKey 'REGISTER_CODEX_ROOT_DOCKER_SOURCE' -Fallback ''
$resolvedOpenaiRootDockerSource = Resolve-EnvValue -ParameterName 'OpenaiRootDockerSource' -RuntimeKey 'REGISTER_OPENAI_ROOT_DOCKER_SOURCE' -Fallback ''
$resolvedCodexRootDockerVolume = Resolve-EnvValue -ParameterName 'CodexRootDockerVolume' -RuntimeKey 'REGISTER_CODEX_ROOT_DOCKER_VOLUME' -Fallback ''
$resolvedOpenaiRootDockerVolume = Resolve-EnvValue -ParameterName 'OpenaiRootDockerVolume' -RuntimeKey 'REGISTER_OPENAI_ROOT_DOCKER_VOLUME' -Fallback ''
$resolvedProtocolBridgeDockerVolume = Resolve-EnvValue -ParameterName 'ProtocolBridgeDockerVolume' -RuntimeKey 'REGISTER_PROTOCOL_BRIDGE_DOCKER_VOLUME' -Fallback ''
if (-not [string]::IsNullOrWhiteSpace($resolvedCredentialRootHost)) {
    $resolvedCredentialRootHost = Resolve-AbsolutePath -Path $resolvedCredentialRootHost -BaseDir $launcherRoot
}
if (-not [string]::IsNullOrWhiteSpace($resolvedCodexRootDirHost)) {
    $resolvedCodexRootDirHost = Resolve-AbsolutePath -Path $resolvedCodexRootDirHost -BaseDir $launcherRoot
} elseif (-not [string]::IsNullOrWhiteSpace($resolvedCredentialRootHost)) {
    $resolvedCodexRootDirHost = Join-Path $resolvedCredentialRootHost "codex"
}
if (-not [string]::IsNullOrWhiteSpace($resolvedOpenaiRootDirHost)) {
    $resolvedOpenaiRootDirHost = Resolve-AbsolutePath -Path $resolvedOpenaiRootDirHost -BaseDir $launcherRoot
} elseif (-not [string]::IsNullOrWhiteSpace($resolvedCredentialRootHost)) {
    $resolvedOpenaiRootDirHost = Join-Path $resolvedCredentialRootHost "openai"
}
$resolvedEasyProtocolBaseUrl = if ($importedRuntimeValues.ContainsKey('EASY_PROTOCOL_BASE_URL')) { [string]$importedRuntimeValues['EASY_PROTOCOL_BASE_URL'] } else { [string]$env:EASY_PROTOCOL_BASE_URL }
$resolvedEasyProtocolControlToken = if ($importedRuntimeValues.ContainsKey('EASY_PROTOCOL_CONTROL_TOKEN')) { [string]$importedRuntimeValues['EASY_PROTOCOL_CONTROL_TOKEN'] } else { [string]$env:EASY_PROTOCOL_CONTROL_TOKEN }
$resolvedDashboardEnabled = if ($deployBoundParameters.ContainsKey('DashboardEnabled')) {
    [string]$DashboardEnabled
} elseif (-not [string]::IsNullOrWhiteSpace($env:REGISTER_DASHBOARD_ENABLED)) {
    [string]$env:REGISTER_DASHBOARD_ENABLED
} else {
    $defaultDashboardEnabled
}
$resolvedDashboardListen = if ($importedRuntimeValues.ContainsKey('REGISTER_DASHBOARD_LISTEN')) { [string]$importedRuntimeValues['REGISTER_DASHBOARD_LISTEN'] } else { [string]$env:REGISTER_DASHBOARD_LISTEN }
$resolvedDashboardAllowRemote = if ($importedRuntimeValues.ContainsKey('REGISTER_DASHBOARD_ALLOW_REMOTE')) { [string]$importedRuntimeValues['REGISTER_DASHBOARD_ALLOW_REMOTE'] } else { [string]$env:REGISTER_DASHBOARD_ALLOW_REMOTE }
$resolvedProtocolOutputTargetContainerPath = if ([string]::IsNullOrWhiteSpace($ProtocolRegisterOutputContainerPath)) {
    "/shared/register-output"
} else {
    $ProtocolRegisterOutputContainerPath.TrimEnd('/', '\')
}
$resolvedProtocolOutputMirrorContainerPath = if ([string]::IsNullOrWhiteSpace($ProtocolOutputMirrorContainerPath)) {
    "/shared/protocol-register-output"
} else {
    $ProtocolOutputMirrorContainerPath.TrimEnd('/', '\')
}
$resolvedProtocolBridgeSubdir = if ([string]::IsNullOrWhiteSpace($ProtocolBridgeSubdir)) {
    "easyregister-bridge"
} else {
    $ProtocolBridgeSubdir.Trim().Trim('/', '\')
}
$resolvedProtocolRegisterOutputDirHost = if ($deployBoundParameters.ContainsKey('ProtocolRegisterOutputDirHost')) {
    [string]$ProtocolRegisterOutputDirHost
} elseif (-not [string]::IsNullOrWhiteSpace($env:REGISTER_PROTOCOL_REGISTER_OUTPUT_DIR_HOST)) {
    [string]$env:REGISTER_PROTOCOL_REGISTER_OUTPUT_DIR_HOST
} else {
    Get-DockerBindSourceForProtocolTarget `
        -ContainerName $ProtocolContainerName `
        -TargetPath $resolvedProtocolOutputTargetContainerPath
}
if (-not [string]::IsNullOrWhiteSpace($resolvedProtocolRegisterOutputDirHost)) {
    $resolvedProtocolRegisterOutputDirHost = Resolve-AbsolutePath -Path $resolvedProtocolRegisterOutputDirHost -BaseDir $launcherRoot
}
$resolvedProtocolBridgeDir = if ($importedRuntimeValues.ContainsKey('REGISTER_PROTOCOL_BRIDGE_DIR')) { [string]$importedRuntimeValues['REGISTER_PROTOCOL_BRIDGE_DIR'] } else { [string]$env:REGISTER_PROTOCOL_BRIDGE_DIR }
$resolvedProtocolBridgeTargetDir = if ($importedRuntimeValues.ContainsKey('REGISTER_PROTOCOL_BRIDGE_TARGET_DIR')) { [string]$importedRuntimeValues['REGISTER_PROTOCOL_BRIDGE_TARGET_DIR'] } else { [string]$env:REGISTER_PROTOCOL_BRIDGE_TARGET_DIR }
$resolvedProtocolOutputMirrorDir = if ($importedRuntimeValues.ContainsKey('REGISTER_PROTOCOL_OUTPUT_MIRROR_DIR')) { [string]$importedRuntimeValues['REGISTER_PROTOCOL_OUTPUT_MIRROR_DIR'] } else { [string]$env:REGISTER_PROTOCOL_OUTPUT_MIRROR_DIR }
$resolvedProtocolOutputTargetDir = if ($importedRuntimeValues.ContainsKey('REGISTER_PROTOCOL_OUTPUT_TARGET_DIR')) { [string]$importedRuntimeValues['REGISTER_PROTOCOL_OUTPUT_TARGET_DIR'] } else { [string]$env:REGISTER_PROTOCOL_OUTPUT_TARGET_DIR }
if (-not [string]::IsNullOrWhiteSpace($resolvedProtocolRegisterOutputDirHost)) {
    $resolvedProtocolOutputMirrorDir = $resolvedProtocolOutputMirrorContainerPath
    $resolvedProtocolOutputTargetDir = $resolvedProtocolOutputTargetContainerPath
    $resolvedProtocolBridgeDir = Join-ContainerPath -Root $resolvedProtocolOutputMirrorContainerPath -Child $resolvedProtocolBridgeSubdir
    $resolvedProtocolBridgeTargetDir = Join-ContainerPath -Root $resolvedProtocolOutputTargetContainerPath -Child $resolvedProtocolBridgeSubdir
    New-Item -ItemType Directory -Force -Path (Join-Path $resolvedProtocolRegisterOutputDirHost $resolvedProtocolBridgeSubdir) | Out-Null
} elseif ([string]::IsNullOrWhiteSpace($resolvedProtocolOutputTargetDir)) {
    $resolvedProtocolOutputTargetDir = "/shared/register-output"
}

$usesResultPoolRoot = (-not [string]::IsNullOrWhiteSpace($resolvedCodexRootDirHost)) -or (-not [string]::IsNullOrWhiteSpace($resolvedOpenaiRootDirHost))
$effectiveTeamAuthDirHost = if ($deployBoundParameters.ContainsKey('TeamAuthDirHost')) {
    $TeamAuthDirHost
} elseif ($usesResultPoolRoot -and -not [string]::IsNullOrWhiteSpace($resolvedCodexRootDirHost)) {
    Join-Path $resolvedCodexRootDirHost "team-input"
} else {
    $TeamAuthDirHost
}
$effectiveCodexFreeDirHost = if ($deployBoundParameters.ContainsKey('CodexFreeDirHost') -or -not $usesResultPoolRoot) { $CodexFreeDirHost } else { "" }
$effectiveCodexTeamDirHost = if ($deployBoundParameters.ContainsKey('CodexTeamDirHost') -or -not $usesResultPoolRoot) { $CodexTeamDirHost } else { "" }
$effectiveCodexTeamInputDirHost = if ($deployBoundParameters.ContainsKey('CodexTeamInputDirHost') -or -not $usesResultPoolRoot) { $CodexTeamInputDirHost } else { "" }
$effectiveCodexTeamMotherInputDirHost = if ($deployBoundParameters.ContainsKey('CodexTeamMotherInputDirHost') -or -not $usesResultPoolRoot) { $CodexTeamMotherInputDirHost } else { "" }

$env:REGISTER_OUTPUT_DIR_HOST = $resolvedOutputDirHost
$env:REGISTER_TEAM_AUTH_DIR_HOST = $effectiveTeamAuthDirHost
$env:REGISTER_DASHBOARD_PORT_HOST = $DashboardPortHost
$env:REGISTER_CONTAINER_NAME = $ContainerName
$env:REGISTER_INSTANCE_ID = $InstanceId
$env:REGISTER_NETWORK_ALIAS = $NetworkAlias
$env:REGISTER_DOCKER_NETWORK_NAME = $DockerNetworkName
$env:REGISTER_DOCKER_NETWORK_EXTERNAL = $DockerNetworkExternal
$env:REGISTER_WORKER_COUNT = [string]$resolvedWorkerCount
$env:REGISTER_MAIN_CONCURRENCY_LIMIT = [string]$resolvedMainConcurrencyLimit
$env:REGISTER_CONTINUE_CONCURRENCY_LIMIT = [string]$resolvedContinueConcurrencyLimit
$env:REGISTER_TEAM_CONCURRENCY_LIMIT = [string]$resolvedTeamConcurrencyLimit
$env:MAILBOX_SERVICE_BASE_URL = $resolvedMailboxServiceBaseUrl
$env:MAILBOX_SERVICE_API_KEY = $resolvedMailboxServiceApiKey
$env:REGISTER_MAILBOX_DOMAIN_POOL = $resolvedMailboxDomainPool
$env:REGISTER_MAILBOX_DOMAIN_BLACKLIST = $resolvedMailboxDomainBlacklist
$env:REGISTER_MAILBOX_PROVIDER_BLACKLIST = $resolvedMailboxProviderBlacklist
$env:REGISTER_MAILBOX_DOMAIN_CONSECUTIVE_FAILURE_BLACKLIST_THRESHOLD = [string]$resolvedMailboxDomainConsecutiveFailureBlacklistThreshold
$env:REGISTER_MAILBOX_DOMAIN_BLACKLIST_MIN_ATTEMPTS = [string]$resolvedMailboxDomainBlacklistMinAttempts
$env:REGISTER_MAILBOX_DOMAIN_BLACKLIST_FAILURE_RATE = [string]$resolvedMailboxDomainBlacklistFailureRate
$env:REGISTER_MAILBOX_EMAIL_OTP_FAILURE_BLACKLIST_THRESHOLD = [string]$resolvedMailboxEmailOtpFailureBlacklistThreshold
$env:REGISTER_MAILBOX_EMAIL_OTP_PROVIDER_FAILURE_BLACKLIST_THRESHOLD = [string]$resolvedMailboxEmailOtpProviderFailureBlacklistThreshold
$env:REGISTER_MAILBOX_DYNAMIC_BLACKLIST_TTL_SECONDS = [string]$resolvedMailboxDynamicBlacklistTtlSeconds
$env:REGISTER_MAILBOX_DYNAMIC_BLACKLIST_EXHAUSTED_FALLBACK = $resolvedMailboxDynamicBlacklistExhaustedFallback
$env:REGISTER_MAILBOX_BUSINESS_POLICIES_JSON = $resolvedMailboxBusinessPoliciesJson
$env:SMS_SERVICE_BASE_URL = $resolvedSmsServiceBaseUrl
$env:SMS_SERVICE_API_KEY = $resolvedSmsServiceApiKey
$env:SMS_SERVICE_SELECTION_PLAN_TIMEOUT_SECONDS = $resolvedSmsSelectionPlanTimeoutSeconds
$env:SMS_SERVICE_SELECTION_PLAN_ATTEMPTS = $resolvedSmsSelectionPlanAttempts
$env:REGISTER_PHONE_VERIFICATION_TERMINAL_RETRY_ATTEMPTS = $resolvedPhoneTerminalRetryAttempts
$env:REGISTER_PHONE_VERIFICATION_SMS_CODE_WAIT_RETRY_ATTEMPTS = $resolvedPhoneSmsCodeWaitRetryAttempts
$env:REGISTER_SMS_BUSINESS_KEY = $resolvedSmsBusinessKey
$env:REGISTER_SMS_PROVIDER_BLACKLIST = $resolvedSmsProviderBlacklist
$env:REGISTER_SMS_ALLOW_PAID = $resolvedSmsAllowPaid
$env:REGISTER_SMS_ALLOW_REUSE = $resolvedSmsAllowReuse
$env:REGISTER_SMS_MAX_BINDINGS_PER_PHONE = $resolvedSmsMaxBindingsPerPhone
$env:REGISTER_SMS_COUNTRY_CODES = $resolvedSmsCountryCodes
$env:REGISTER_SMS_SELECTION_MODE = $resolvedSmsSelectionMode
$env:REGISTER_SMS_BUSINESS_POLICIES_JSON = $resolvedSmsBusinessPoliciesJson
$env:REGISTER_SMS_TERMINAL_INVALID_PHONE_BLACKLIST_SECONDS = $resolvedSmsTerminalInvalidPhoneBlacklistSeconds
$env:EASY_PROXY_BASE_URL = $resolvedEasyProxyBaseUrl
$env:EASY_PROXY_RUNTIME_HOST = $resolvedEasyProxyRuntimeHost
$env:EASY_PROXY_MANAGEMENT_USERNAME = $resolvedEasyProxyManagementUsername
$env:EASY_PROXY_MANAGEMENT_PASSWORD = $resolvedEasyProxyManagementPassword
$env:EASY_PROXY_API_KEY = $resolvedEasyProxyApiKey

if ([string]::IsNullOrWhiteSpace($env:EASYREGISTER_TEST_EASY_PROXY_BASE_URL)) {
    $env:EASYREGISTER_TEST_EASY_PROXY_BASE_URL = $resolvedEasyProxyBaseUrl
}
if ([string]::IsNullOrWhiteSpace($resolvedEasyProtocolControlToken)) {
    $resolvedEasyProtocolControlToken = $defaultDashboardControlToken
}
$env:EASY_PROTOCOL_CONTROL_TOKEN = $resolvedEasyProtocolControlToken
if ([string]::IsNullOrWhiteSpace($env:EASYREGISTER_TEST_EASY_PROTOCOL_CONTROL_TOKEN)) {
    $env:EASYREGISTER_TEST_EASY_PROTOCOL_CONTROL_TOKEN = $resolvedEasyProtocolControlToken
}
if ([string]::IsNullOrWhiteSpace($resolvedDashboardListen)) {
    $resolvedDashboardListen = $defaultDashboardListen
}
$env:REGISTER_DASHBOARD_ENABLED = $resolvedDashboardEnabled
$env:REGISTER_DASHBOARD_LISTEN = $resolvedDashboardListen
if ([string]::IsNullOrWhiteSpace($env:EASYREGISTER_TEST_DASHBOARD_LISTEN)) {
    $env:EASYREGISTER_TEST_DASHBOARD_LISTEN = $resolvedDashboardListen
}
if ([string]::IsNullOrWhiteSpace($resolvedDashboardAllowRemote)) {
    $resolvedDashboardAllowRemote = "true"
}
$env:REGISTER_DASHBOARD_ALLOW_REMOTE = $resolvedDashboardAllowRemote
if ([string]::IsNullOrWhiteSpace($env:EASYREGISTER_TEST_DASHBOARD_ALLOW_REMOTE)) {
    $env:EASYREGISTER_TEST_DASHBOARD_ALLOW_REMOTE = $resolvedDashboardAllowRemote
}
if (-not [string]::IsNullOrWhiteSpace($resolvedEasyProtocolBaseUrl)) {
    $env:EASY_PROTOCOL_BASE_URL = $resolvedEasyProtocolBaseUrl
}
elseif ([string]::IsNullOrWhiteSpace($env:EASY_PROTOCOL_BASE_URL)) {
    $env:EASY_PROTOCOL_BASE_URL = ''
}
if ([string]::IsNullOrWhiteSpace($env:EASYREGISTER_TEST_EASY_PROTOCOL_BASE_URL)) {
    $env:EASYREGISTER_TEST_EASY_PROTOCOL_BASE_URL = $env:EASY_PROTOCOL_BASE_URL
}
$env:REGISTER_PROTOCOL_BRIDGE_DIR = $resolvedProtocolBridgeDir
$env:REGISTER_PROTOCOL_BRIDGE_TARGET_DIR = $resolvedProtocolBridgeTargetDir
$env:REGISTER_PROTOCOL_OUTPUT_MIRROR_DIR = $resolvedProtocolOutputMirrorDir
$env:REGISTER_PROTOCOL_OUTPUT_TARGET_DIR = $resolvedProtocolOutputTargetDir

if (-not [string]::IsNullOrWhiteSpace($Image)) {
    $env:REGISTER_SERVICE_IMAGE = $Image
    $env:EASYREGISTER_TEST_IMAGE = $Image
    if ($Pull) {
        Write-Host "[deploy-host] pulling image: $Image" -ForegroundColor Cyan
        Invoke-NativeCommand -FilePath "docker" -Arguments @("pull", $Image)
    }
}

if (-not [string]::IsNullOrWhiteSpace($resolvedCredentialRootHost)) {
    $env:REGISTER_CREDENTIAL_ROOT_HOST = $resolvedCredentialRootHost
}
if (-not [string]::IsNullOrWhiteSpace($resolvedCodexRootDirHost)) {
    $env:REGISTER_CODEX_ROOT_DIR_HOST = $resolvedCodexRootDirHost
}
if (-not [string]::IsNullOrWhiteSpace($resolvedOpenaiRootDirHost)) {
    $env:REGISTER_OPENAI_ROOT_DIR_HOST = $resolvedOpenaiRootDirHost
}
if (-not [string]::IsNullOrWhiteSpace($resolvedCodexRootDockerSource)) {
    $env:REGISTER_CODEX_ROOT_DOCKER_SOURCE = $resolvedCodexRootDockerSource
}
if (-not [string]::IsNullOrWhiteSpace($resolvedOpenaiRootDockerSource)) {
    $env:REGISTER_OPENAI_ROOT_DOCKER_SOURCE = $resolvedOpenaiRootDockerSource
}
if (-not [string]::IsNullOrWhiteSpace($resolvedCodexRootDockerVolume)) {
    $env:REGISTER_CODEX_ROOT_DOCKER_VOLUME = $resolvedCodexRootDockerVolume
}
if (-not [string]::IsNullOrWhiteSpace($resolvedOpenaiRootDockerVolume)) {
    $env:REGISTER_OPENAI_ROOT_DOCKER_VOLUME = $resolvedOpenaiRootDockerVolume
}
if (-not [string]::IsNullOrWhiteSpace($resolvedProtocolBridgeDockerVolume)) {
    $env:REGISTER_PROTOCOL_BRIDGE_DOCKER_VOLUME = $resolvedProtocolBridgeDockerVolume
}
if (-not [string]::IsNullOrWhiteSpace($effectiveCodexFreeDirHost)) {
    $env:REGISTER_CODEX_FREE_DIR_HOST = $effectiveCodexFreeDirHost
}
if (-not [string]::IsNullOrWhiteSpace($effectiveCodexTeamDirHost)) {
    $env:REGISTER_CODEX_TEAM_DIR_HOST = $effectiveCodexTeamDirHost
}
if (-not [string]::IsNullOrWhiteSpace($effectiveCodexTeamInputDirHost)) {
    $env:REGISTER_CODEX_TEAM_INPUT_DIR_HOST = $effectiveCodexTeamInputDirHost
}
if (-not [string]::IsNullOrWhiteSpace($effectiveCodexTeamMotherInputDirHost)) {
    $env:REGISTER_CODEX_TEAM_MOTHER_INPUT_DIR_HOST = $effectiveCodexTeamMotherInputDirHost
}
$env:REGISTER_OPENAI_UPLOAD_PERCENT = [string]$resolvedOpenaiUploadPercent
$env:REGISTER_CODEX_FREE_UPLOAD_PERCENT = [string]$resolvedCodexFreeUploadPercent
$env:REGISTER_CODEX_TEAM_UPLOAD_PERCENT = [string]$resolvedCodexTeamUploadPercent
$env:REGISTER_CODEX_PLUS_UPLOAD_PERCENT = [string]$resolvedCodexPlusUploadPercent

$composeEnvFilePath = Join-Path $launcherRoot ".deploy-compose.env"
$composeEnvValues = @{}
foreach ($entry in $importedRuntimeValues.GetEnumerator()) {
    $composeEnvValues[[string]$entry.Key] = [string]$entry.Value
}
foreach ($entry in @{
    REGISTER_OUTPUT_DIR_HOST                  = $env:REGISTER_OUTPUT_DIR_HOST
    REGISTER_TEAM_AUTH_DIR_HOST               = $env:REGISTER_TEAM_AUTH_DIR_HOST
    REGISTER_DASHBOARD_PORT_HOST              = $env:REGISTER_DASHBOARD_PORT_HOST
    REGISTER_CONTAINER_NAME                   = $env:REGISTER_CONTAINER_NAME
    REGISTER_INSTANCE_ID                      = $env:REGISTER_INSTANCE_ID
    REGISTER_NETWORK_ALIAS                    = $env:REGISTER_NETWORK_ALIAS
    REGISTER_DOCKER_NETWORK_NAME              = $env:REGISTER_DOCKER_NETWORK_NAME
    REGISTER_DOCKER_NETWORK_EXTERNAL          = $env:REGISTER_DOCKER_NETWORK_EXTERNAL
    REGISTER_WORKER_COUNT                     = $env:REGISTER_WORKER_COUNT
    REGISTER_MAIN_CONCURRENCY_LIMIT           = $env:REGISTER_MAIN_CONCURRENCY_LIMIT
    REGISTER_CONTINUE_CONCURRENCY_LIMIT       = $env:REGISTER_CONTINUE_CONCURRENCY_LIMIT
    REGISTER_TEAM_CONCURRENCY_LIMIT           = $env:REGISTER_TEAM_CONCURRENCY_LIMIT
    MAILBOX_SERVICE_BASE_URL                  = $env:MAILBOX_SERVICE_BASE_URL
    MAILBOX_SERVICE_API_KEY                   = $env:MAILBOX_SERVICE_API_KEY
    REGISTER_MAILBOX_DOMAIN_POOL              = $env:REGISTER_MAILBOX_DOMAIN_POOL
    REGISTER_MAILBOX_DOMAIN_BLACKLIST         = $env:REGISTER_MAILBOX_DOMAIN_BLACKLIST
    REGISTER_MAILBOX_PROVIDER_BLACKLIST       = $env:REGISTER_MAILBOX_PROVIDER_BLACKLIST
    REGISTER_MAILBOX_DOMAIN_CONSECUTIVE_FAILURE_BLACKLIST_THRESHOLD = $env:REGISTER_MAILBOX_DOMAIN_CONSECUTIVE_FAILURE_BLACKLIST_THRESHOLD
    REGISTER_MAILBOX_DOMAIN_BLACKLIST_MIN_ATTEMPTS = $env:REGISTER_MAILBOX_DOMAIN_BLACKLIST_MIN_ATTEMPTS
    REGISTER_MAILBOX_DOMAIN_BLACKLIST_FAILURE_RATE = $env:REGISTER_MAILBOX_DOMAIN_BLACKLIST_FAILURE_RATE
    REGISTER_MAILBOX_EMAIL_OTP_FAILURE_BLACKLIST_THRESHOLD = $env:REGISTER_MAILBOX_EMAIL_OTP_FAILURE_BLACKLIST_THRESHOLD
    REGISTER_MAILBOX_EMAIL_OTP_PROVIDER_FAILURE_BLACKLIST_THRESHOLD = $env:REGISTER_MAILBOX_EMAIL_OTP_PROVIDER_FAILURE_BLACKLIST_THRESHOLD
    REGISTER_MAILBOX_DYNAMIC_BLACKLIST_TTL_SECONDS = $env:REGISTER_MAILBOX_DYNAMIC_BLACKLIST_TTL_SECONDS
    REGISTER_MAILBOX_DYNAMIC_BLACKLIST_EXHAUSTED_FALLBACK = $env:REGISTER_MAILBOX_DYNAMIC_BLACKLIST_EXHAUSTED_FALLBACK
    REGISTER_MAILBOX_BUSINESS_POLICIES_JSON   = $env:REGISTER_MAILBOX_BUSINESS_POLICIES_JSON
    SMS_SERVICE_BASE_URL                      = $env:SMS_SERVICE_BASE_URL
    SMS_SERVICE_API_KEY                       = $env:SMS_SERVICE_API_KEY
    SMS_SERVICE_SELECTION_PLAN_TIMEOUT_SECONDS = $env:SMS_SERVICE_SELECTION_PLAN_TIMEOUT_SECONDS
    SMS_SERVICE_SELECTION_PLAN_ATTEMPTS       = $env:SMS_SERVICE_SELECTION_PLAN_ATTEMPTS
    REGISTER_PHONE_VERIFICATION_TERMINAL_RETRY_ATTEMPTS = $env:REGISTER_PHONE_VERIFICATION_TERMINAL_RETRY_ATTEMPTS
    REGISTER_PHONE_VERIFICATION_SMS_CODE_WAIT_RETRY_ATTEMPTS = $env:REGISTER_PHONE_VERIFICATION_SMS_CODE_WAIT_RETRY_ATTEMPTS
    REGISTER_SMS_BUSINESS_KEY                 = $env:REGISTER_SMS_BUSINESS_KEY
    REGISTER_SMS_PROVIDER_BLACKLIST           = $env:REGISTER_SMS_PROVIDER_BLACKLIST
    REGISTER_SMS_ALLOW_PAID                   = $env:REGISTER_SMS_ALLOW_PAID
    REGISTER_SMS_ALLOW_REUSE                  = $env:REGISTER_SMS_ALLOW_REUSE
    REGISTER_SMS_MAX_BINDINGS_PER_PHONE       = $env:REGISTER_SMS_MAX_BINDINGS_PER_PHONE
    REGISTER_SMS_COUNTRY_CODES                = $env:REGISTER_SMS_COUNTRY_CODES
    REGISTER_SMS_SELECTION_MODE               = $env:REGISTER_SMS_SELECTION_MODE
    REGISTER_SMS_BUSINESS_POLICIES_JSON       = $env:REGISTER_SMS_BUSINESS_POLICIES_JSON
    REGISTER_SMS_TERMINAL_INVALID_PHONE_BLACKLIST_SECONDS = $env:REGISTER_SMS_TERMINAL_INVALID_PHONE_BLACKLIST_SECONDS
    EASY_PROXY_BASE_URL                       = $env:EASY_PROXY_BASE_URL
    EASY_PROXY_RUNTIME_HOST                   = $env:EASY_PROXY_RUNTIME_HOST
    EASY_PROXY_MANAGEMENT_USERNAME            = $env:EASY_PROXY_MANAGEMENT_USERNAME
    EASY_PROXY_MANAGEMENT_PASSWORD            = $env:EASY_PROXY_MANAGEMENT_PASSWORD
    EASY_PROXY_API_KEY                        = $env:EASY_PROXY_API_KEY
    REGISTER_OPENAI_UPLOAD_PERCENT            = $env:REGISTER_OPENAI_UPLOAD_PERCENT
    REGISTER_CODEX_FREE_UPLOAD_PERCENT        = $env:REGISTER_CODEX_FREE_UPLOAD_PERCENT
    REGISTER_CODEX_TEAM_UPLOAD_PERCENT        = $env:REGISTER_CODEX_TEAM_UPLOAD_PERCENT
    REGISTER_CODEX_PLUS_UPLOAD_PERCENT        = $env:REGISTER_CODEX_PLUS_UPLOAD_PERCENT
    REGISTER_CREDENTIAL_ROOT_HOST             = $env:REGISTER_CREDENTIAL_ROOT_HOST
    REGISTER_CODEX_ROOT_DIR_HOST              = $env:REGISTER_CODEX_ROOT_DIR_HOST
    REGISTER_OPENAI_ROOT_DIR_HOST             = $env:REGISTER_OPENAI_ROOT_DIR_HOST
    REGISTER_CODEX_ROOT_DOCKER_SOURCE         = $env:REGISTER_CODEX_ROOT_DOCKER_SOURCE
    REGISTER_OPENAI_ROOT_DOCKER_SOURCE        = $env:REGISTER_OPENAI_ROOT_DOCKER_SOURCE
    REGISTER_CODEX_ROOT_DOCKER_VOLUME         = $env:REGISTER_CODEX_ROOT_DOCKER_VOLUME
    REGISTER_OPENAI_ROOT_DOCKER_VOLUME        = $env:REGISTER_OPENAI_ROOT_DOCKER_VOLUME
    REGISTER_PROTOCOL_BRIDGE_DOCKER_VOLUME    = $env:REGISTER_PROTOCOL_BRIDGE_DOCKER_VOLUME
    REGISTER_CODEX_FREE_DIR_HOST              = $env:REGISTER_CODEX_FREE_DIR_HOST
    REGISTER_CODEX_TEAM_DIR_HOST              = $env:REGISTER_CODEX_TEAM_DIR_HOST
    REGISTER_CODEX_TEAM_INPUT_DIR_HOST        = $env:REGISTER_CODEX_TEAM_INPUT_DIR_HOST
    REGISTER_CODEX_TEAM_MOTHER_INPUT_DIR_HOST = $env:REGISTER_CODEX_TEAM_MOTHER_INPUT_DIR_HOST
    EASY_PROTOCOL_BASE_URL                    = $env:EASY_PROTOCOL_BASE_URL
    EASY_PROTOCOL_CONTROL_TOKEN               = $env:EASY_PROTOCOL_CONTROL_TOKEN
    REGISTER_PROTOCOL_BRIDGE_DIR              = $env:REGISTER_PROTOCOL_BRIDGE_DIR
    REGISTER_PROTOCOL_BRIDGE_TARGET_DIR       = $env:REGISTER_PROTOCOL_BRIDGE_TARGET_DIR
    REGISTER_PROTOCOL_OUTPUT_MIRROR_DIR       = $env:REGISTER_PROTOCOL_OUTPUT_MIRROR_DIR
    REGISTER_PROTOCOL_OUTPUT_TARGET_DIR       = $env:REGISTER_PROTOCOL_OUTPUT_TARGET_DIR
    REGISTER_DASHBOARD_ENABLED                = $env:REGISTER_DASHBOARD_ENABLED
    REGISTER_DASHBOARD_LISTEN                 = $env:REGISTER_DASHBOARD_LISTEN
    REGISTER_DASHBOARD_ALLOW_REMOTE           = $env:REGISTER_DASHBOARD_ALLOW_REMOTE
    REGISTER_SERVICE_IMAGE                    = $env:REGISTER_SERVICE_IMAGE
}.GetEnumerator()) {
    $composeEnvValues[[string]$entry.Key] = [string]$entry.Value
}
Write-ComposeEnvFile -Path $composeEnvFilePath -Values $composeEnvValues

$materializeScript = Join-Path $repoRoot "scripts\materialize-output-links.ps1"
$materializeParams = @{
    OutputDirHost = $resolvedOutputDirHost
    PathBaseDir   = $launcherRoot
    LinkType      = $LinkType
}
if ($ForceLinks) {
    $materializeParams["Force"] = $true
}

Ensure-ResultPoolDirectories `
    -OutputDirHost $resolvedOutputDirHost `
    -CodexRootDirHost $resolvedCodexRootDirHost `
    -OpenaiRootDirHost $resolvedOpenaiRootDirHost

& $materializeScript @materializeParams

$resultPoolOverridePath = Join-Path $launcherRoot '.deploy-compose.result-pools.generated.yaml'
$resultPoolMounts = @(
    New-ResultPoolMountOverrideFile `
        -CodexRootDirHost $resolvedCodexRootDirHost `
        -OpenaiRootDirHost $resolvedOpenaiRootDirHost `
        -CodexRootDockerSource $resolvedCodexRootDockerSource `
        -OpenaiRootDockerSource $resolvedOpenaiRootDockerSource `
        -CodexRootDockerVolume $resolvedCodexRootDockerVolume `
        -OpenaiRootDockerVolume $resolvedOpenaiRootDockerVolume `
        -OverridePath $resultPoolOverridePath
)
$aliasMountOverridePath = Join-Path $launcherRoot '.deploy-compose.alias-mounts.generated.yaml'
$aliasMounts = @(
    New-AliasMountOverrideFile `
        -OutputDirHost $resolvedOutputDirHost `
        -OverridePath $aliasMountOverridePath
)
$protocolBridgeOverridePath = Join-Path $launcherRoot '.deploy-compose.protocol-bridge.generated.yaml'
$protocolBridgeMount = New-ProtocolBridgeMountOverrideFile `
    -ProtocolRegisterOutputDirHost $resolvedProtocolRegisterOutputDirHost `
    -ProtocolOutputMirrorContainerPath $resolvedProtocolOutputMirrorContainerPath `
    -ProtocolBridgeSubdir $resolvedProtocolBridgeSubdir `
    -ProtocolBridgeDockerVolume $resolvedProtocolBridgeDockerVolume `
    -OverridePath $protocolBridgeOverridePath

if ($MaterializeOnly) {
    if ($resultPoolMounts.Count -gt 0) {
        $resultPoolMounts | Format-Table -AutoSize
    }
    if ($aliasMounts.Count -gt 0) {
        $aliasMounts | Format-Table -AutoSize
    }
    if ($null -ne $protocolBridgeMount) {
        $protocolBridgeMount | Format-List
    }
    return
}

$deployComposeScript = Join-Path $repoRoot "scripts\deploy-compose.ps1"
$deployComposeParams = @{
    ComposeFile        = $resolvedComposeFile
    ComposeProjectName = $ComposeProjectName
    OutputDirHost      = $resolvedOutputDirHost
    EnvFilePath        = $composeEnvFilePath
    LinkType           = $LinkType
}
if ($aliasMounts.Count -gt 0) {
    $deployComposeParams["AdditionalComposeFiles"] = @($aliasMountOverridePath)
}
if ($resultPoolMounts.Count -gt 0) {
    if ($deployComposeParams.ContainsKey("AdditionalComposeFiles")) {
        $deployComposeParams["AdditionalComposeFiles"] = @($deployComposeParams["AdditionalComposeFiles"]) + $resultPoolOverridePath
    } else {
        $deployComposeParams["AdditionalComposeFiles"] = @($resultPoolOverridePath)
    }
}
if ($null -ne $protocolBridgeMount) {
    if ($deployComposeParams.ContainsKey("AdditionalComposeFiles")) {
        $deployComposeParams["AdditionalComposeFiles"] = @($deployComposeParams["AdditionalComposeFiles"]) + $protocolBridgeOverridePath
    } else {
        $deployComposeParams["AdditionalComposeFiles"] = @($protocolBridgeOverridePath)
    }
}
if ($ForceLinks) {
    $deployComposeParams["ForceLinks"] = $true
}
if ((-not $NoBuild) -and [string]::IsNullOrWhiteSpace($Image)) {
    $deployComposeParams["Build"] = $true
}
if ($NoDetach) {
    $deployComposeParams["NoDetach"] = $true
}

& $deployComposeScript @deployComposeParams @Services
