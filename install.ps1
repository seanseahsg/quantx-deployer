<#
.SYNOPSIS
    QuantX Deployer — Windows VPS Installer
.DESCRIPTION
    Installs Python 3.11, sets up the QuantX Deployer app, creates a
    scheduled task for auto-start, and launches the student portal.
#>

param(
    [string]$CentralApiUrl = "",
    [string]$InstallDir = "C:\QuantXDeployer"
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

Write-Host ""
Write-Host "  ================================================================" -ForegroundColor Cyan
Write-Host "    QUANTX DEPLOYER — WINDOWS VPS INSTALLER" -ForegroundColor Cyan
Write-Host "  ================================================================" -ForegroundColor Cyan
Write-Host ""

# ── 1. Check/Install Python 3.11 ──────────────────────────────────────────

function Get-PythonPath {
    $candidates = @(
        "python",
        "python3",
        "py -3.11",
        "C:\Python311\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe"
    )
    foreach ($cmd in $candidates) {
        try {
            $ver = & cmd /c "$cmd --version 2>&1"
            if ($ver -match "Python 3\.1[1-9]") {
                if ($cmd -match "^py ") { return "py -3.11" }
                return $cmd
            }
        } catch {}
    }
    return $null
}

$pythonCmd = Get-PythonPath
if (-not $pythonCmd) {
    Write-Host "[*] Python 3.11 not found. Downloading installer..." -ForegroundColor Yellow
    $installerUrl = "https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe"
    $installerPath = "$env:TEMP\python-3.11.9-amd64.exe"
    Invoke-WebRequest -Uri $installerUrl -OutFile $installerPath -UseBasicParsing
    Write-Host "[*] Installing Python 3.11.9 silently..." -ForegroundColor Yellow
    Start-Process -FilePath $installerPath -ArgumentList "/quiet", "InstallAllUsers=1", "PrependPath=1", "Include_pip=1" -Wait -NoNewWindow
    Remove-Item $installerPath -Force -ErrorAction SilentlyContinue

    # Refresh PATH
    $env:Path = [System.Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path", "User")
    $pythonCmd = Get-PythonPath
    if (-not $pythonCmd) {
        Write-Host "[!] Python installation failed. Please install Python 3.11 manually." -ForegroundColor Red
        exit 1
    }
}
Write-Host "[OK] Python found: $pythonCmd" -ForegroundColor Green

# ── 2. Get Central API URL ─────────────────────────────────────────────────

if (-not $CentralApiUrl) {
    Write-Host ""
    $CentralApiUrl = Read-Host "Enter the Central API URL (e.g. https://quantx-central.up.railway.app)"
}
if ($CentralApiUrl) {
    Write-Host "[OK] Central API: $CentralApiUrl" -ForegroundColor Green
} else {
    Write-Host "[!] No Central API URL provided. You can set it later in the portal." -ForegroundColor Yellow
}

# ── 3. Copy app to install directory ───────────────────────────────────────

Write-Host "[*] Installing to $InstallDir..." -ForegroundColor Yellow

if (Test-Path $InstallDir) {
    # Preserve database and keys
    $backups = @()
    foreach ($f in @("quantx_deployer.db", ".fernet.key")) {
        $fp = Join-Path $InstallDir $f
        if (Test-Path $fp) {
            $bak = "$fp.bak"
            Copy-Item $fp $bak -Force
            $backups += @{original=$fp; backup=$bak}
        }
    }
    Remove-Item "$InstallDir\api" -Recurse -Force -ErrorAction SilentlyContinue
    Remove-Item "$InstallDir\static" -Recurse -Force -ErrorAction SilentlyContinue
}

New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
New-Item -ItemType Directory -Path "$InstallDir\bots" -Force | Out-Null
New-Item -ItemType Directory -Path "$InstallDir\logs" -Force | Out-Null

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Copy-Item "$scriptDir\api" "$InstallDir\api" -Recurse -Force
Copy-Item "$scriptDir\static" "$InstallDir\static" -Recurse -Force
Copy-Item "$scriptDir\requirements.txt" "$InstallDir\requirements.txt" -Force

# Restore backups
foreach ($b in $backups) {
    if (Test-Path $b.backup) {
        Move-Item $b.backup $b.original -Force
    }
}

Write-Host "[OK] Files copied." -ForegroundColor Green

# ── 4. Install pip dependencies ────────────────────────────────────────────

Write-Host "[*] Installing Python dependencies..." -ForegroundColor Yellow
& cmd /c "$pythonCmd -m pip install --upgrade pip 2>&1" | Out-Null
& cmd /c "$pythonCmd -m pip install -r $InstallDir\requirements.txt 2>&1"
Write-Host "[OK] Dependencies installed." -ForegroundColor Green

# ── 5. Create scheduled task ──────────────────────────────────────────────

$taskName = "QuantXDeployer"
$existing = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($existing) {
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
    Write-Host "[*] Removed old scheduled task." -ForegroundColor Yellow
}

# Resolve python to absolute path
$pythonExe = (& cmd /c "where $pythonCmd 2>&1").Trim().Split("`n")[0]
if (-not (Test-Path $pythonExe)) { $pythonExe = $pythonCmd }

$action = New-ScheduledTaskAction `
    -Execute $pythonExe `
    -Argument "-m uvicorn api.main:app --host 0.0.0.0 --port 8080" `
    -WorkingDirectory $InstallDir

$trigger = New-ScheduledTaskTrigger -AtStartup
$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType S4U -RunLevel Highest
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)

Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Description "QuantX Deployer — Student Trading Bot Manager" | Out-Null
Write-Host "[OK] Scheduled task '$taskName' created (runs on startup)." -ForegroundColor Green

# ── 6. Start the app now ──────────────────────────────────────────────────

Write-Host "[*] Starting QuantX Deployer..." -ForegroundColor Yellow
$proc = Start-Process -FilePath $pythonExe `
    -ArgumentList "-m uvicorn api.main:app --host 0.0.0.0 --port 8080" `
    -WorkingDirectory $InstallDir `
    -WindowStyle Minimized `
    -PassThru

Start-Sleep -Seconds 3

# ── 7. Open browser ───────────────────────────────────────────────────────

Start-Process "http://localhost:8080"

# ── 8. Print summary ─────────────────────────────────────────────────────

$ip = (Get-NetIPAddress -AddressFamily IPv4 | Where-Object { $_.InterfaceAlias -notmatch "Loopback" -and $_.IPAddress -ne "127.0.0.1" } | Select-Object -First 1).IPAddress

Write-Host ""
Write-Host "  ================================================================" -ForegroundColor Green
Write-Host "    QUANTX DEPLOYER INSTALLED SUCCESSFULLY" -ForegroundColor Green
Write-Host "  ================================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  Install Directory : $InstallDir" -ForegroundColor White
Write-Host "  Local Portal      : http://localhost:8080" -ForegroundColor Cyan
Write-Host "  VPS IP Address    : $ip" -ForegroundColor Cyan
Write-Host "  Central API       : $CentralApiUrl" -ForegroundColor Cyan
Write-Host "  Scheduled Task    : $taskName (auto-starts on boot)" -ForegroundColor White
Write-Host "  PID               : $($proc.Id)" -ForegroundColor White
Write-Host ""
Write-Host "  Next steps:" -ForegroundColor Yellow
Write-Host "    1. Open http://localhost:8080 in your browser" -ForegroundColor White
Write-Host "    2. Register with your email and LongPort credentials" -ForegroundColor White
Write-Host "    3. Add your trading strategies" -ForegroundColor White
Write-Host "    4. Deploy the master bot" -ForegroundColor White
Write-Host ""
