param(
    [string]$DistroName = "Ubuntu-22.04",
    [int]$InitialDelaySeconds = 40,
    [int]$DockerReadyTimeoutSeconds = 240,
    [int]$SupersetReadyTimeoutSeconds = 480
)

$ErrorActionPreference = "Stop"
$logDir = "C:\ProgramData\SupersetBoot"
$logFile = Join-Path $logDir "superset_boot.log"

function Write-Log {
    param([string]$Message)
    $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    Add-Content -Path $logFile -Value "[$ts] $Message"
}

function Wait-DockerReady {
    param([int]$TimeoutSeconds)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            docker version *> $null
            return $true
        } catch {
            Start-Sleep -Seconds 3
        }
    }
    return $false
}

function Ensure-ContainerRunning {
    param([string]$Name)
    $isRunning = docker inspect -f "{{.State.Running}}" $Name 2>$null
    if ($LASTEXITCODE -ne 0) {
        Write-Log "Container '$Name' not found."
        return
    }
    if ($isRunning -eq "true") {
        Write-Log "Container '$Name' already running."
        return
    }
    docker start $Name | Out-Null
    Write-Log "Container '$Name' started."
}

function Get-WslIPv4 {
    param([string]$Distro)
    try {
        $ip = (wsl.exe -d $Distro -- bash -lc "ip -4 -o addr show eth0 | awk '{print \$4}' | cut -d/ -f1" 2>$null).Trim()
        if ($ip -match "^\d+\.\d+\.\d+\.\d+$") {
            return $ip
        }
    } catch {
        Write-Log "Failed to get WSL IPv4: $($_.Exception.Message)"
    }
    return $null
}

function Set-PortProxy8088 {
    param([string]$ConnectAddress)
    & netsh interface portproxy delete v4tov4 listenaddress=0.0.0.0 listenport=8088 | Out-Null
    & netsh interface portproxy add v4tov4 listenaddress=0.0.0.0 listenport=8088 connectaddress=$ConnectAddress connectport=8088 | Out-Null
    Write-Log "Portproxy 0.0.0.0:8088 -> $($ConnectAddress):8088 configured."
}

function Wait-ContainerHealthy {
    param(
        [string]$Name,
        [int]$TimeoutSeconds
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $status = docker inspect -f "{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}" $Name 2>$null
        if ($LASTEXITCODE -ne 0) {
            Write-Log "Container '$Name' not found while waiting for health."
            return $false
        }
        if ($status -eq "healthy") {
            Write-Log "Container '$Name' health is healthy."
            return $true
        }
        Write-Log "Container '$Name' health is '$status', waiting..."
        Start-Sleep -Seconds 5
    }
    return $false
}

function Wait-HttpHealth {
    param(
        [string]$Url,
        [int]$TimeoutSeconds
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $r = Invoke-WebRequest -UseBasicParsing $Url -TimeoutSec 5
            if ($r.StatusCode -eq 200) {
                return $true
            }
            Write-Log "HTTP health returned status $($r.StatusCode), waiting..."
        } catch {
            Write-Log "HTTP health probe error: $($_.Exception.Message)"
        }
        Start-Sleep -Seconds 5
    }
    return $false
}

New-Item -ItemType Directory -Path $logDir -Force | Out-Null
Write-Log "===== Boot recovery started ====="

Start-Sleep -Seconds $InitialDelaySeconds
Write-Log "Initial delay completed ($InitialDelaySeconds sec)."

try {
    if (-not (Get-Process -Name "Docker Desktop" -ErrorAction SilentlyContinue)) {
        Start-Process -FilePath "C:\Program Files\Docker\Docker\Docker Desktop.exe" | Out-Null
        Write-Log "Docker Desktop start requested."
    } else {
        Write-Log "Docker Desktop process already present."
    }
} catch {
    Write-Log "Failed to start Docker Desktop: $($_.Exception.Message)"
}

if (-not (Wait-DockerReady -TimeoutSeconds $DockerReadyTimeoutSeconds)) {
    Write-Log "Docker did not become ready in $DockerReadyTimeoutSeconds seconds."
    exit 1
}
Write-Log "Docker is ready."

Ensure-ContainerRunning -Name "superset-db-local"
Ensure-ContainerRunning -Name "superset-redis-local"
Ensure-ContainerRunning -Name "superset-local"

$wslIp = Get-WslIPv4 -Distro $DistroName
if ($wslIp) {
    Set-PortProxy8088 -ConnectAddress $wslIp
} else {
    Set-PortProxy8088 -ConnectAddress "127.0.0.1"
    Write-Log "Fallback to 127.0.0.1 for portproxy."
}

if (-not (Wait-ContainerHealthy -Name "superset-local" -TimeoutSeconds $SupersetReadyTimeoutSeconds)) {
    Write-Log "Superset container did not become healthy in $SupersetReadyTimeoutSeconds seconds."
    Write-Log "===== Boot recovery finished with errors ====="
    exit 2
}

if (Wait-HttpHealth -Url "http://127.0.0.1:8088/health" -TimeoutSeconds 90) {
    Write-Log "Superset health check is OK."
    Write-Log "===== Boot recovery finished successfully ====="
    exit 0
}

Write-Log "WARNING: HTTP health check failed in task context, but container is healthy."
Write-Log "===== Boot recovery finished successfully ====="
exit 0
