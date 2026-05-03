param(
    [string]$OutputDirHost = $env:REGISTER_OUTPUT_DIR_HOST,
    [string]$PathBaseDir = "",
    [string]$AliasRootHost = $env:REGISTER_OUTPUT_ALIAS_ROOT_HOST,
    [ValidateSet("Auto", "Junction", "SymbolicLink")]
    [string]$LinkType = "Auto",
    [switch]$Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-AbsolutePath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [string]$BaseDir
    )

    $trimmed = $Path.Trim()
    if (-not $trimmed) {
        throw "Cannot resolve an empty path."
    }
    if ([System.IO.Path]::IsPathRooted($trimmed)) {
        return [System.IO.Path]::GetFullPath($trimmed)
    }
    return [System.IO.Path]::GetFullPath((Join-Path $BaseDir $trimmed))
}

function Resolve-OptionalAbsolutePath {
    param(
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [string]$BaseDir
    )

    if ([string]::IsNullOrWhiteSpace($Path)) {
        return $null
    }
    return Resolve-AbsolutePath -Path $Path -BaseDir $BaseDir
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

function Get-LinkKind {
    param(
        [Parameter(Mandatory = $true)]
        [string]$LinkPath,
        [Parameter(Mandatory = $true)]
        [string]$TargetPath,
        [Parameter(Mandatory = $true)]
        [string]$RequestedKind
    )

    if ($RequestedKind -eq "Junction" -or $RequestedKind -eq "SymbolicLink") {
        return $RequestedKind
    }
    if ($TargetPath.StartsWith("\\") -or $TargetPath.StartsWith("//")) {
        return "SymbolicLink"
    }
    if ([System.IO.Path]::GetPathRoot($LinkPath) -ne [System.IO.Path]::GetPathRoot($TargetPath)) {
        return "SymbolicLink"
    }
    return "Junction"
}

function Remove-ExistingDirectoryLink {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return
    }

    $item = Get-Item -LiteralPath $Path -Force -ErrorAction Stop
    $isReparsePoint = [bool]($item.Attributes -band [IO.FileAttributes]::ReparsePoint)
    if (-not $isReparsePoint) {
        throw "Path '$Path' is not a directory link."
    }

    $cmdResult = cmd /c rmdir "$Path" 2>&1
    if ($LASTEXITCODE -ne 0 -and (Test-Path -LiteralPath $Path)) {
        throw "Failed to remove existing directory link '$Path': $cmdResult"
    }
}

function Ensure-DirectoryAlias {
    param(
        [Parameter(Mandatory = $true)]
        [string]$LinkPath,
        [Parameter(Mandatory = $true)]
        [string]$TargetPath,
        [Parameter(Mandatory = $true)]
        [string]$RequestedKind,
        [switch]$ForceReplace
    )

    $normalizedLinkPath = [System.IO.Path]::GetFullPath($LinkPath)
    $normalizedTargetPath = [System.IO.Path]::GetFullPath($TargetPath)
    if ($normalizedLinkPath -eq $normalizedTargetPath) {
        New-Item -ItemType Directory -Path $normalizedLinkPath -Force | Out-Null
        return "materialized-directory"
    }

    New-Item -ItemType Directory -Path $normalizedTargetPath -Force | Out-Null
    New-Item -ItemType Directory -Path (Split-Path -Parent $normalizedLinkPath) -Force | Out-Null

    if (Test-Path -LiteralPath $normalizedLinkPath) {
        $existing = Get-Item -LiteralPath $normalizedLinkPath -Force -ErrorAction Stop
        $isReparsePoint = [bool]($existing.Attributes -band [IO.FileAttributes]::ReparsePoint)
        if ($isReparsePoint) {
            $currentTarget = Get-LinkTargetPath -Path $normalizedLinkPath
            if ($currentTarget -and ([System.IO.Path]::GetFullPath($currentTarget) -eq $normalizedTargetPath)) {
                return "existing-link"
            }
            if (-not $ForceReplace) {
                throw "Existing link '$normalizedLinkPath' points to '$currentTarget', not '$normalizedTargetPath'. Use -Force to replace it."
            }
            Remove-ExistingDirectoryLink -Path $normalizedLinkPath
        } elseif ($existing.PSIsContainer) {
            $entries = @(Get-ChildItem -LiteralPath $normalizedLinkPath -Force)
            if ($entries.Count -gt 0) {
                throw "Cannot replace non-empty directory '$normalizedLinkPath' with a link."
            }
            Remove-Item -LiteralPath $normalizedLinkPath -Force
        } else {
            throw "Path '$normalizedLinkPath' exists and is not a directory."
        }
    }

    $itemType = Get-LinkKind -LinkPath $normalizedLinkPath -TargetPath $normalizedTargetPath -RequestedKind $RequestedKind
    New-Item -ItemType $itemType -Path $normalizedLinkPath -Target $normalizedTargetPath | Out-Null
    return "created-$($itemType.ToLowerInvariant())"
}

function Ensure-DirectoryMaterialized {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if (Test-Path -LiteralPath $Path) {
        $existing = Get-Item -LiteralPath $Path -Force -ErrorAction Stop
        if ([bool]($existing.Attributes -band [IO.FileAttributes]::ReparsePoint)) {
            return "kept-existing-link"
        }
        if (-not $existing.PSIsContainer) {
            throw "Path '$Path' exists and is not a directory."
        }
        return "existing-directory"
    }
    New-Item -ItemType Directory -Path $Path -Force | Out-Null
    return "created-directory"
}

$repoRoot = Resolve-AbsolutePath -Path (Join-Path $PSScriptRoot "..") -BaseDir $PWD.Path
$resolvedBaseDir = if ([string]::IsNullOrWhiteSpace($PathBaseDir)) {
    $repoRoot
} else {
    Resolve-AbsolutePath -Path $PathBaseDir -BaseDir $repoRoot
}
$resolvedOutputDirHost = Resolve-OptionalAbsolutePath -Path $OutputDirHost -BaseDir $resolvedBaseDir
if ([string]::IsNullOrWhiteSpace($resolvedOutputDirHost)) {
    throw "REGISTER_OUTPUT_DIR_HOST or -OutputDirHost is required."
}
$resolvedAliasRootHost = Resolve-OptionalAbsolutePath -Path $AliasRootHost -BaseDir $resolvedBaseDir

$mappings = @(
    @{ Relative = "openai/pending"; EnvName = "REGISTER_OPENAI_PENDING_DIR_HOST" },
    @{ Relative = "openai/converted"; EnvName = "REGISTER_OPENAI_CONVERTED_DIR_HOST" },
    @{ Relative = "openai/failed-once"; EnvName = "REGISTER_OPENAI_FAILED_ONCE_DIR_HOST" },
    @{ Relative = "openai/failed-twice"; EnvName = "REGISTER_OPENAI_FAILED_TWICE_DIR_HOST" },
    @{ Relative = "codex/free"; EnvName = "REGISTER_CODEX_FREE_DIR_HOST" },
    @{ Relative = "codex/team"; EnvName = "REGISTER_CODEX_TEAM_DIR_HOST" },
    @{ Relative = "codex/plus"; EnvName = "REGISTER_CODEX_PLUS_DIR_HOST" },
    @{ Relative = "codex/team-input"; EnvName = "REGISTER_CODEX_TEAM_INPUT_DIR_HOST" },
    @{ Relative = "codex/team-mother-input"; EnvName = "REGISTER_CODEX_TEAM_MOTHER_INPUT_DIR_HOST" }
)

New-Item -ItemType Directory -Path $resolvedOutputDirHost -Force | Out-Null

$results = @()
foreach ($mapping in $mappings) {
    $relative = [string]$mapping.Relative
    $envName = [string]$mapping.EnvName
    $linkPath = Resolve-AbsolutePath -Path $relative -BaseDir $resolvedOutputDirHost
    $envEntry = Get-Item -Path "Env:$envName" -ErrorAction SilentlyContinue
    $explicitTargetValue = if ($null -ne $envEntry) { [string]$envEntry.Value } else { $null }
    $explicitTarget = Resolve-OptionalAbsolutePath -Path $explicitTargetValue -BaseDir $resolvedBaseDir
    $targetPath = $explicitTarget
    if (-not $targetPath -and $resolvedAliasRootHost) {
        $targetPath = Resolve-AbsolutePath -Path $relative -BaseDir $resolvedAliasRootHost
    }

    $action = if ($targetPath) {
        Ensure-DirectoryAlias -LinkPath $linkPath -TargetPath $targetPath -RequestedKind $LinkType -ForceReplace:$Force
    } else {
        Ensure-DirectoryMaterialized -Path $linkPath
    }

    $results += [pscustomobject]@{
        RelativePath = $relative
        LocalPath = $linkPath
        TargetPath = $targetPath
        Action = $action
    }
}

$results | Format-Table -AutoSize
