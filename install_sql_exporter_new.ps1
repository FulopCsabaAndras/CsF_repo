$ErrorActionPreference = "Continue"
$InstallDir = "C:\Monitoring\sql_exporter"
$taskName   = "SQL_Exporter"
$port       = 9399
# $vmIP       = "10.174.24.21"
$results    = @()
 
# =============================================================
Write-Host "`n=== SCHRITT 1: Voraussetzungen ===" -ForegroundColor Cyan
# =============================================================
if (!(Test-Path "$InstallDir\sql_exporter.exe")) {
    Write-Host "FEHLER: sql_exporter.exe nicht gefunden!" -ForegroundColor Red
    $results += "SCHRITT 1: FEHLER - exe fehlt"; return
}
if (!(Test-Path "$InstallDir\mssql_standard.collector.yml")) {
    Write-Host "FEHLER: mssql_standard.collector.yml nicht gefunden!" -ForegroundColor Red
    $results += "SCHRITT 1: FEHLER - collector yml fehlt"; return
}
 
$mssql = Get-Service "MSSQLSERVER" -ErrorAction SilentlyContinue
if ($mssql.Status -ne "Running") {
    Write-Host "FEHLER: MSSQL Server laeuft nicht (Status: $($mssql.Status))" -ForegroundColor Red
    $results += "SCHRITT 1: FEHLER - MSSQL nicht Running"; return
}
 
try {
    $tcp = New-Object System.Net.Sockets.TcpClient
    $tcp.Connect("localhost", 1433); $tcp.Close()
    Write-Host "[OK] MSSQL laeuft, Port 1433 erreichbar" -ForegroundColor Green
} catch {
    Write-Host "FEHLER: Port 1433 nicht erreichbar" -ForegroundColor Red
    $results += "SCHRITT 1: FEHLER - Port 1433 nicht erreichbar"; return
}
 
$configOK = (Get-Content "$InstallDir\sql_exporter.yml" -First 1 -ErrorAction SilentlyContinue).Trim() -eq "global:"
if (!$configOK) {
    Write-Host "FEHLER: sql_exporter.yml fehlt oder ist korrupt" -ForegroundColor Red
    $results += "SCHRITT 1: FEHLER - Config korrupt"; return
}
Write-Host "[OK] Config vorhanden und gueltig" -ForegroundColor Green
$results += "SCHRITT 1: OK - Voraussetzungen erfuellt"
 
# =============================================================
Write-Host "`n=== SCHRITT 2: Alles stoppen und aufraeumen ===" -ForegroundColor Cyan
# =============================================================
$existingProc = netstat -ano 2>$null | findstr "LISTENING" | findstr ":$port "
if ($existingProc) {
    Write-Host "Port $port ist belegt:" -ForegroundColor Yellow
    Write-Host $existingProc
}
 
Stop-Service $taskName -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2
Get-Process -Name "sql_exporter" -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Sleep -Seconds 1
 
sc.exe delete $taskName 2>$null | Out-Null
Start-Sleep -Seconds 3
 
$svcCheck = Get-Service -Name $taskName -ErrorAction SilentlyContinue
if ($svcCheck) {
    Write-Host "WARNUNG: Alter Service noch vorhanden (marked for deletion)" -ForegroundColor Yellow
    Write-Host "Wird ignoriert, Scheduled Task wird trotzdem erstellt." -ForegroundColor Yellow
}
 
schtasks /Delete /TN $taskName /F 2>$null | Out-Null
 
Get-Process -Name "sql_exporter" -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Sleep -Seconds 1
 
$portStillUsed = netstat -ano 2>$null | findstr "LISTENING" | findstr ":$port "
if ($portStillUsed) {
    Write-Host "FEHLER: Port $port noch belegt nach Cleanup!" -ForegroundColor Red
    Write-Host $portStillUsed
    $results += "SCHRITT 2: FEHLER - Port $port belegt"; return
}
$results += "SCHRITT 2: OK - Cleanup erledigt"
Write-Host "[OK] Altes Service/Task/Prozess entfernt" -ForegroundColor Green
 
# =============================================================
Write-Host "`n=== SCHRITT 3: Scheduled Task erstellen ===" -ForegroundColor Cyan
# =============================================================
$exePath = "$InstallDir\sql_exporter.exe"
$exeArgs = "-config.file `"$InstallDir\sql_exporter.yml`" -web.listen-address `":$port`""
 
$action  = New-ScheduledTaskAction -Execute $exePath -Argument $exeArgs -WorkingDirectory $InstallDir
$trigger = New-ScheduledTaskTrigger -AtStartup
$principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit ([TimeSpan]::Zero) -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)
 
try {
    Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Force -ErrorAction Stop | Out-Null
    $results += "SCHRITT 3: OK - Scheduled Task erstellt"
    Write-Host "[OK] Scheduled Task '$taskName' erstellt" -ForegroundColor Green
    Write-Host "  - Startet automatisch bei Systemstart" -ForegroundColor Gray
    Write-Host "  - Neustart bei Fehler (max 3x, alle 60s)" -ForegroundColor Gray
} catch {
    $results += "SCHRITT 3: FEHLER - $($_.Exception.Message)"
    Write-Host "FEHLER beim Erstellen: $_" -ForegroundColor Red
    return
}
 
# =============================================================
Write-Host "`n=== SCHRITT 4: Task starten ===" -ForegroundColor Cyan
# =============================================================
Start-ScheduledTask -TaskName $taskName
Start-Sleep -Seconds 8
 
$task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
$taskInfo = Get-ScheduledTaskInfo -TaskName $taskName -ErrorAction SilentlyContinue
$proc = Get-Process -Name "sql_exporter" -ErrorAction SilentlyContinue
 
Write-Host "Task Status: $($task.State)" -ForegroundColor Yellow
Write-Host "Last Result: $($taskInfo.LastTaskResult)" -ForegroundColor Yellow
 
if ($proc) {
    Write-Host "[OK] sql_exporter Prozess laeuft (PID: $($proc.Id))" -ForegroundColor Green
    $results += "SCHRITT 4: OK - Prozess laeuft (PID: $($proc.Id))"
} else {
    Write-Host "FEHLER: Prozess nicht gefunden" -ForegroundColor Red
    Write-Host "`n--- Starte manuell fuer Fehleranalyse ---" -ForegroundColor Yellow
    $manualProc = Start-Process -FilePath $exePath -ArgumentList $exeArgs `
        -PassThru -NoNewWindow `
        -RedirectStandardError "$InstallDir\stderr_task.log" `
        -RedirectStandardOutput "$InstallDir\stdout_task.log"
    Start-Sleep -Seconds 10
    Write-Host "--- STDERR ---" -ForegroundColor Yellow
    if (Test-Path "$InstallDir\stderr_task.log") { Get-Content "$InstallDir\stderr_task.log" }
    Write-Host "--- STDOUT ---" -ForegroundColor Yellow
    if (Test-Path "$InstallDir\stdout_task.log") { Get-Content "$InstallDir\stdout_task.log" }
    if ($manualProc -and !$manualProc.HasExited) { $manualProc | Stop-Process -Force }
    $results += "SCHRITT 4: FEHLER - Prozess nicht gestartet"
}
 
# =============================================================
Write-Host "`n=== SCHRITT 5: Firewall ===" -ForegroundColor Cyan
# =============================================================
netsh advfirewall firewall delete rule name="sql_exporter" 2>$null | Out-Null
netsh advfirewall firewall delete rule name="windows_exporter" 2>$null | Out-Null
netsh advfirewall firewall add rule name="sql_exporter" dir=in action=allow protocol=TCP localport=$port | Out-Null
netsh advfirewall firewall add rule name="windows_exporter" dir=in action=allow protocol=TCP localport=9182 | Out-Null
$results += "SCHRITT 5: OK - Firewall"
Write-Host "[OK] Firewall OK ($port + 9182)" -ForegroundColor Green
 
# =============================================================
Write-Host "`n=== ERGEBNIS ===" -ForegroundColor Cyan
# =============================================================
Write-Host "`n--- Prozesse ---" -ForegroundColor Yellow
Get-Process -Name "sql_exporter" -ErrorAction SilentlyContinue | Select-Object Id, ProcessName, StartTime | Format-Table -AutoSize


$ErrorActionPreference = "Continue"
$InstallDir = "C:\Monitoring\sql_exporter"
$taskName   = "SQL_Exporter"
$results    = @()
 
# =============================================================
Write-Host "`n=== SCHRITT 1: Stoppen ===" -ForegroundColor Cyan
# =============================================================
Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2
Get-Process -Name "sql_exporter" -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Sleep -Seconds 2
 
$portCheck = netstat -ano 2>$null | findstr "LISTENING" | findstr ":9399 "
if ($portCheck) {
    Write-Host "WARNUNG: Port 9399 noch belegt, warte..." -ForegroundColor Yellow
    Start-Sleep -Seconds 5
    Get-Process -Name "sql_exporter" -ErrorAction SilentlyContinue | Stop-Process -Force
    Start-Sleep -Seconds 2
    $portCheck2 = netstat -ano 2>$null | findstr "LISTENING" | findstr ":9399 "
    if ($portCheck2) {
        Write-Host "FEHLER: Port 9399 noch belegt!" -ForegroundColor Red
        Write-Host $portCheck2
        $results += "SCHRITT 1: FEHLER - Port 9399 belegt"
        Write-Host "Ergebnis: $($results -join ' | ')" -ForegroundColor Red
        return
    }
}
$results += "SCHRITT 1: OK - Gestoppt"
Write-Host "[OK] sql_exporter gestoppt, Port 9399 frei" -ForegroundColor Green
 
# =============================================================
Write-Host "`n=== SCHRITT 2: Config mit URL-encoded Passwort ===" -ForegroundColor Cyan
# =============================================================
# Passwort: Wm8#auDB!2475$
# URL-encoded: # = %23, ! = %21, $ = %24
Set-Content -Path "$InstallDir\sql_exporter.yml" -Encoding UTF8 -Value @'
global:
  scrape_timeout_offset: 500ms
  max_connections: 3
  max_idle_connections: 3
 
target:
  data_source_name: 'sqlserver://sa:Wm8%23auDB%212475%24@localhost:1433'
  collectors: [mssql_standard]
 
collector_files:
  - "C:/Monitoring/sql_exporter/mssql_standard.collector.yml"
'@
 
$firstLine = (Get-Content "$InstallDir\sql_exporter.yml" -First 1).Trim()
$hasDSN = (Get-Content "$InstallDir\sql_exporter.yml" -Raw) -match "%23"
if ($firstLine -eq "global:" -and $hasDSN) {
    $results += "SCHRITT 2: OK - Config geschrieben"
    Write-Host "[OK] Config verifiziert (URL-encoded Passwort)" -ForegroundColor Green
} else {
    $results += "SCHRITT 2: FEHLER - Config korrupt"
    Write-Host "FEHLER: Config nicht korrekt geschrieben!" -ForegroundColor Red
    Get-Content "$InstallDir\sql_exporter.yml"
    Write-Host "Ergebnis: $($results -join ' | ')" -ForegroundColor Red
    return
}
 
# =============================================================
Write-Host "`n=== SCHRITT 3: Task starten ===" -ForegroundColor Cyan
# =============================================================
Start-ScheduledTask -TaskName $taskName
Start-Sleep -Seconds 8
 
$proc = Get-Process -Name "sql_exporter" -ErrorAction SilentlyContinue
$portUp = netstat -ano 2>$null | findstr "LISTENING" | findstr ":9399 "
if ($proc -and $portUp) {
    $results += "SCHRITT 3: OK - Prozess laeuft (PID: $($proc.Id)), Port 9399 offen"
    Write-Host "[OK] sql_exporter laeuft (PID: $($proc.Id)), Port 9399 lauscht" -ForegroundColor Green
} elseif ($proc) {
    $results += "SCHRITT 3: WARNUNG - Prozess da aber Port 9399 nicht offen"
    Write-Host "WARNUNG: Prozess laeuft aber Port 9399 nicht offen" -ForegroundColor Yellow
} else {
    $results += "SCHRITT 3: FEHLER - Prozess nicht gestartet"
    Write-Host "FEHLER: Prozess nicht gefunden" -ForegroundColor Red
    Write-Host "`n--- Starte manuell fuer Fehleranalyse ---" -ForegroundColor Yellow
    $manualProc = Start-Process -FilePath "$InstallDir\sql_exporter.exe" `
        -ArgumentList "-config.file `"$InstallDir\sql_exporter.yml`" -web.listen-address `":9399`"" `
        -PassThru -NoNewWindow `
        -RedirectStandardError "$InstallDir\stderr_fix.log" `
        -RedirectStandardOutput "$InstallDir\stdout_fix.log"
    Start-Sleep -Seconds 10
    Write-Host "--- STDERR ---" -ForegroundColor Yellow
    if (Test-Path "$InstallDir\stderr_fix.log") { Get-Content "$InstallDir\stderr_fix.log" }
    Write-Host "--- STDOUT ---" -ForegroundColor Yellow
    if (Test-Path "$InstallDir\stdout_fix.log") { Get-Content "$InstallDir\stdout_fix.log" }
    if ($manualProc -and !$manualProc.HasExited) { $manualProc | Stop-Process -Force }
    Write-Host "Ergebnis: $($results -join ' | ')" -ForegroundColor Red
    return
}
 
# =============================================================
Write-Host "`n=== ERGEBNIS ===" -ForegroundColor Cyan
# =============================================================
Write-Host "`n--- Ports ---" -ForegroundColor Yellow
netstat -an | findstr "LISTENING" | findstr "1433 9399 9182"
 
Write-Host "`n--- Test sql_exporter (9399) ---" -ForegroundColor Yellow
try {
    $r = Invoke-WebRequest http://localhost:9399/metrics -UseBasicParsing -TimeoutSec 15
    Write-Host "[OK] localhost:9399 -> $($r.StatusCode)" -ForegroundColor Green
    $mssqlMetrics = $r.Content -split "`n" | Select-String "mssql_" | Select-Object -First 10
    if ($mssqlMetrics) {
        $mssqlMetrics
        $results += "TEST 9399 lokal: OK (MSSQL Metriken vorhanden)"
    } else {
        Write-Host "WARNUNG: Endpoint erreichbar aber keine mssql_ Metriken" -ForegroundColor Yellow
        $r.Content -split "`n" | Select-String "sql_exporter_|error|Error" | Select-Object -First 5
        $results += "TEST 9399 lokal: WARNUNG - keine mssql_ Metriken"
    }
} catch {
    Write-Host "[FEHLER] 9399 lokal: $_" -ForegroundColor Red
    $results += "TEST 9399 lokal: FEHLER"
}
 
Write-Host "`n--- Test windows_exporter (9182) ---" -ForegroundColor Yellow
try {
    $r = Invoke-WebRequest http://localhost:9182/metrics -UseBasicParsing -TimeoutSec 10
    Write-Host "[OK] localhost:9182 -> $($r.StatusCode)" -ForegroundColor Green
    $results += "TEST 9182 lokal: OK"
} catch {
    Write-Host "[FEHLER] 9182 lokal: $_" -ForegroundColor Red
    $results += "TEST 9182 lokal: FEHLER"
}
 
Write-Host "`n--- Externer Zugriff ---" -ForegroundColor Yellow
try {
    $r = Invoke-WebRequest http://localhost:9399/metrics -UseBasicParsing -TimeoutSec 10
    Write-Host "[OK] 9399 extern: $($r.StatusCode)" -ForegroundColor Green
    $results += "TEST 9399 extern: OK"
} catch {
    Write-Host "[FEHLER] 9399 extern: $_" -ForegroundColor Red
    $results += "TEST 9399 extern: FEHLER"
}
try {
    $r = Invoke-WebRequest http://localhost:9182/metrics -UseBasicParsing -TimeoutSec 10
    Write-Host "[OK] 9182 extern: $($r.StatusCode)" -ForegroundColor Green
    $results += "TEST 9182 extern: OK"
} catch {
    Write-Host "[FEHLER] 9182 extern: $_" -ForegroundColor Red
    $results += "TEST 9182 extern: FEHLER"
}
 
Write-Host "`n--- Config ---" -ForegroundColor Yellow
Get-Content "$InstallDir\sql_exporter.yml"
 
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  ZUSAMMENFASSUNG" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
$results | ForEach-Object {
    if ($_ -match "FEHLER") { Write-Host "  $_" -ForegroundColor Red }
    elseif ($_ -match "WARNUNG") { Write-Host "  $_" -ForegroundColor Yellow }
    else { Write-Host "  $_" -ForegroundColor Green }
}
Write-Host "========================================`n" -ForegroundColor Cyan