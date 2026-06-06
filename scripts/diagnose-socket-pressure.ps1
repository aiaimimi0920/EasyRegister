[CmdletBinding()]
param(
    [switch]$Once,
    [int]$IntervalSeconds = 60,
    [int]$Samples = 0,
    [string]$OutputDir = ".tmp\socket-pressure",
    [switch]$SkipDetailedTcpQuery,
    [switch]$SkipDockerInspect
)

$ErrorActionPreference = "SilentlyContinue"

function ConvertTo-Hashtable {
    param([object[]]$Items)

    $table = [ordered]@{}
    foreach ($item in $Items) {
        $table[[string]$item.Name] = [int]$item.Count
    }
    return $table
}

function Get-DynamicPortRange {
    param(
        [ValidateSet("ipv4", "ipv6")]
        [string]$AddressFamily,
        [ValidateSet("tcp", "udp")]
        [string]$Protocol
    )

    $text = netsh int $AddressFamily show dynamicport $Protocol 2>$null
    $start = $null
    $count = $null
    foreach ($line in $text) {
        if ($line -match "Start Port\s*:\s*(\d+)") {
            $start = [int]$Matches[1]
        }
        if ($line -match "Number of Ports\s*:\s*(\d+)") {
            $count = [int]$Matches[1]
        }
    }

    $end = $null
    if ($null -ne $start -and $null -ne $count) {
        $end = $start + $count - 1
    }

    [ordered]@{
        addressFamily = $AddressFamily
        protocol = $Protocol
        start = $start
        count = $count
        end = $end
    }
}

function Get-NetstatTcpRows {
    $rows = New-Object System.Collections.Generic.List[object]
    $lines = netstat -ano -p tcp 2>$null | Select-Object -Skip 4
    foreach ($line in $lines) {
        $parts = ($line -replace "^\s+", "") -split "\s+"
        if ($parts.Count -ge 5 -and $parts[0] -eq "TCP") {
            $rows.Add([pscustomobject]@{
                local = $parts[1]
                foreign = $parts[2]
                state = $parts[3]
                pid = [int]$parts[4]
            })
        }
    }
    return $rows
}

function Get-ProcessSummary {
    param([int]$ProcessId)

    $process = Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
    if (-not $process) {
        return [ordered]@{
            pid = $ProcessId
            process = $null
            handles = $null
            threads = $null
            workingSetMB = $null
            privateMB = $null
            path = $null
        }
    }

    [ordered]@{
        pid = $ProcessId
        process = $process.ProcessName
        handles = $process.HandleCount
        threads = $process.Threads.Count
        workingSetMB = [math]::Round($process.WorkingSet64 / 1MB, 1)
        privateMB = [math]::Round($process.PrivateMemorySize64 / 1MB, 1)
        path = $process.Path
    }
}

function Get-TopProcessTcpCounts {
    param([object[]]$TcpRows)

    $TcpRows |
        Group-Object pid |
        Sort-Object Count -Descending |
        Select-Object -First 25 |
        ForEach-Object {
            $summary = Get-ProcessSummary -ProcessId ([int]$_.Name)
            $summary["tcpCount"] = [int]$_.Count
            $summary
        }
}

function Get-DockerBackendSnapshot {
    param(
        [object[]]$TcpRows,
        [switch]$SkipDetailedConnectionQuery
    )

    $processes = @(Get-Process -Name "com.docker.backend" -ErrorAction SilentlyContinue | Sort-Object HandleCount -Descending)
    if ($processes.Count -eq 0) {
        return [ordered]@{
            present = $false
            processes = @()
            netstatStateCounts = [ordered]@{}
            detailedStateCounts = [ordered]@{}
            bound = $null
        }
    }

    $primary = $processes[0]
    $rows = @($TcpRows | Where-Object { $_.pid -eq $primary.Id })
    $netstatStateCounts = ConvertTo-Hashtable ($rows | Group-Object state)
    $detailedStateCounts = [ordered]@{}
    $bound = $null

    if (-not $SkipDetailedConnectionQuery) {
        $connections = @(Get-NetTCPConnection -OwningProcess $primary.Id -ErrorAction SilentlyContinue)
        $detailedStateCounts = ConvertTo-Hashtable ($connections | Group-Object State)
        $boundConnections = @($connections | Where-Object { $_.State -eq "Bound" })
        $boundInCurrentTcpRange = $null
        $tcpRange = Get-DynamicPortRange -AddressFamily "ipv4" -Protocol "tcp"
        if ($null -ne $tcpRange.start -and $null -ne $tcpRange.end) {
            $boundInCurrentTcpRange = @($boundConnections | Where-Object {
                $_.LocalPort -ge $tcpRange.start -and $_.LocalPort -le $tcpRange.end
            }).Count
        }

        $bound = [ordered]@{
            count = $boundConnections.Count
            inCurrentTcpRange = $boundInCurrentTcpRange
            minPort = ($boundConnections.LocalPort | Measure-Object -Minimum).Minimum
            maxPort = ($boundConnections.LocalPort | Measure-Object -Maximum).Maximum
            uniquePorts = @($boundConnections.LocalPort | Sort-Object -Unique).Count
        }
    }

    [ordered]@{
        present = $true
        primaryPid = $primary.Id
        processes = @($processes | ForEach-Object { Get-ProcessSummary -ProcessId $_.Id })
        netstatTcpCount = $rows.Count
        netstatStateCounts = $netstatStateCounts
        detailedStateCounts = $detailedStateCounts
        bound = $bound
    }
}

function Get-ProxyPortSnapshot {
    param(
        [object[]]$TcpRows,
        [int]$Port = 42344
    )

    $suffix = ":$Port"
    $rows = @($TcpRows | Where-Object {
        $_.local.EndsWith($suffix) -or $_.foreign.EndsWith($suffix)
    })

    [ordered]@{
        port = $Port
        totalTcpRows = $rows.Count
        stateCounts = ConvertTo-Hashtable ($rows | Group-Object state)
        pidCounts = @(
            $rows |
                Group-Object pid |
                Sort-Object Count -Descending |
                Select-Object -First 20 |
                ForEach-Object {
                    $summary = Get-ProcessSummary -ProcessId ([int]$_.Name)
                    $summary["tcpCount"] = [int]$_.Count
                    $summary
                }
        )
    }
}

function Get-DockerContainerSnapshot {
    param([switch]$SkipInspect)

    $result = [ordered]@{
        dockerAvailable = $false
        runningCount = 0
        easyRunningCount = 0
        easyContainers = @()
        publishedHostPorts = @()
        publishedHostPortCount = 0
        exposedOnlyPortEntries = 0
    }

    if ($SkipInspect) {
        $result["skipped"] = $true
        return $result
    }

    $ids = @(docker ps -q 2>$null)
    if ($LASTEXITCODE -ne 0 -or $ids.Count -eq 0) {
        return $result
    }

    $result["dockerAvailable"] = $true
    $result["runningCount"] = $ids.Count

    $json = docker inspect @ids 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $json) {
        return $result
    }

    $parsedContainers = ($json -join [Environment]::NewLine) | ConvertFrom-Json
    $containers = @($parsedContainers)
    $easyContainers = @($containers | Where-Object { $_.Name -match "^/easy-" })
    $result["easyRunningCount"] = $easyContainers.Count

    $publishedHostPorts = New-Object System.Collections.Generic.List[object]
    $exposedOnly = 0
    $containerRows = foreach ($container in $easyContainers) {
        $portsObject = $container.NetworkSettings.Ports
        $bindings = @()
        if ($portsObject) {
            foreach ($property in $portsObject.PSObject.Properties) {
                $containerPort = $property.Name
                if ($null -eq $property.Value) {
                    $exposedOnly += 1
                    continue
                }
                foreach ($binding in @($property.Value)) {
                    $row = [ordered]@{
                        container = $container.Name.TrimStart("/")
                        image = $container.Config.Image
                        containerPort = $containerPort
                        hostIp = $binding.HostIp
                        hostPort = $binding.HostPort
                    }
                    $publishedHostPorts.Add($row)
                    $bindings += $row
                }
            }
        }

        [ordered]@{
            id = $container.Id.Substring(0, 12)
            name = $container.Name.TrimStart("/")
            image = $container.Config.Image
            state = $container.State.Status
            networkMode = $container.HostConfig.NetworkMode
            publishedBindingCount = $bindings.Count
            publishedBindings = $bindings
        }
    }

    $uniqueHostPorts = @(
        $publishedHostPorts |
            Where-Object { $_.hostPort } |
            ForEach-Object { "$($_.hostIp):$($_.hostPort)" } |
            Sort-Object -Unique
    )

    $result["easyContainers"] = @($containerRows)
    $result["publishedHostPorts"] = @($publishedHostPorts.ToArray())
    $result["publishedHostPortCount"] = $uniqueHostPorts.Count
    $result["exposedOnlyPortEntries"] = $exposedOnly
    return $result
}

function New-SocketPressureSnapshot {
    param([switch]$SkipDockerInspect)

    $tcpRows = @(Get-NetstatTcpRows)
    $tcpDynamicRange = [ordered]@{
        ipv4Tcp = Get-DynamicPortRange -AddressFamily "ipv4" -Protocol "tcp"
        ipv4Udp = Get-DynamicPortRange -AddressFamily "ipv4" -Protocol "udp"
        ipv6Tcp = Get-DynamicPortRange -AddressFamily "ipv6" -Protocol "tcp"
        ipv6Udp = Get-DynamicPortRange -AddressFamily "ipv6" -Protocol "udp"
    }

    [ordered]@{
        schemaVersion = 1
        timestamp = (Get-Date).ToString("o")
        computerName = $env:COMPUTERNAME
        tcpDynamicRange = $tcpDynamicRange
        tcpNetstatTotal = $tcpRows.Count
        tcpStateCounts = ConvertTo-Hashtable ($tcpRows | Group-Object state)
        processTcpTop = @(Get-TopProcessTcpCounts -TcpRows $tcpRows)
        dockerBackend = Get-DockerBackendSnapshot -TcpRows $tcpRows -SkipDetailedConnectionQuery:($SkipDockerInspect -or $SkipDetailedTcpQuery)
        proxyPort42344 = Get-ProxyPortSnapshot -TcpRows $tcpRows -Port 42344
        easyContainers = Get-DockerContainerSnapshot -SkipInspect:$SkipDockerInspect
        recentTcpipPortEvents = @(
            Get-WinEvent -FilterHashtable @{
                LogName = "System"
                ProviderName = "Tcpip"
                StartTime = (Get-Date).AddDays(-7)
                Id = 4227, 4231, 4266
            } -ErrorAction SilentlyContinue |
                Group-Object Id |
                Sort-Object Name |
                ForEach-Object {
                    [ordered]@{
                        eventId = [int]$_.Name
                        count = [int]$_.Count
                    }
                }
        )
    }
}

if ($Once -and $Samples -eq 0) {
    $Samples = 1
}

if ($Samples -lt 0) {
    throw "Samples must be zero or positive."
}

New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null
$logFile = Join-Path $OutputDir ("socket-pressure-{0}.jsonl" -f (Get-Date -Format "yyyyMMdd-HHmmss"))
$encoding = New-Object System.Text.UTF8Encoding($false)
$sampleIndex = 0

do {
    $sampleIndex += 1
    $snapshot = New-SocketPressureSnapshot -SkipDockerInspect:$SkipDockerInspect
    $line = $snapshot | ConvertTo-Json -Depth 12 -Compress
    [System.IO.File]::AppendAllText($logFile, $line + [Environment]::NewLine, $encoding)

    $summary = [ordered]@{
        sample = $sampleIndex
        logFile = (Resolve-Path -LiteralPath $logFile).Path
        tcpNetstatTotal = $snapshot.tcpNetstatTotal
        tcpStateCounts = $snapshot.tcpStateCounts
        dockerBackendPrimaryPid = $snapshot.dockerBackend.primaryPid
        dockerBackendBound = $snapshot.dockerBackend.bound
        proxyPort42344Rows = $snapshot.proxyPort42344.totalTcpRows
        easyRunningCount = $snapshot.easyContainers.easyRunningCount
        easyPublishedHostPortCount = $snapshot.easyContainers.publishedHostPortCount
    }
    $summary | ConvertTo-Json -Depth 8

    if ($Once -or ($Samples -gt 0 -and $sampleIndex -ge $Samples)) {
        break
    }

    Start-Sleep -Seconds $IntervalSeconds
} while ($true)
