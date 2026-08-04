# Install SQL_EXPORTER

<#
.SYNOPSIS
  Installiert sql_exporter als Windows-Dienst mit Windows-Auth (winsspi/ntlm).
.PARAMETER Version
  sql_exporter Version (z.B. 0.5)
.PARAMETER ListenPort
  Exporter-Port (Standard: 9399)
.PARAMETER SqlHost
  SQL Server Host/Instanz (z.B. WIN-SQL01 oder WIN-SQL01\\MSSQLSERVER)
.PARAMETER Auth
  winsspi (empfohlen im AD) oder ntlm (DOMAIN\USER muss im Dienstkonto konfiguriert sein)
.PARAMETER ServiceAccount
  Domänenkonto, unter dem der Dienst läuft (für winsspi muss dieses Konto SQL-Rechte haben)
.PARAMETER InstallDir
  Installationsverzeichnis
#>

param(
  [string]$Version        = "0.5",
  [int]   $ListenPort     = 9399,
  [string]$SqlHost        = "DV-SRW-7UPI-GBS",
  [ValidateSet("winsspi","ntlm")][string]$Auth = "winsspi",
  [string]$ServiceAccount = "MSSQL_SQL_Exporter@tc.corp",
  [string]$InstallDir     = "C:\Monitoring\Install_SQLExporter\SQL_EX_Source\sql_exporter-0.5.windows-amd64"
)

$ErrorActionPreference = "Stop"

# 1) Download
# $zipUrl = "https://github.com/free/sql_exporter/releases/download/$Version/sql_exporter-$Version.windows-amd64.zip"
# $tempZip = Join-Path $env:TEMP "sql_exporter-$Version.zip"
# Invoke-WebRequest -Uri $zipUrl -OutFile $tempZip
# if (!(Test-Path $InstallDir)) { New-Item -ItemType Directory -Path $InstallDir | Out-Null }
# Expand-Archive -Path $tempZip -DestinationPath $InstallDir -Force
# Remove-Item $tempZip -Force

$exe = Join-Path $InstallDir "sql_exporter.exe"
$svcName = "SQL_Exporter"

# 2) Konfiguration schreiben (winsspi/ntlm im DSN)
#    DSN-Beispiele gemäß sql_exporter/Alloy-Doku:
#      sqlserver://@HOST:1433?authenticator=winsspi
#      sqlserver://DOMAIN%5CUSER:PASSWORD@HOST:1433?authenticator=ntlm
#  (URL-encoding des Backslash für NTLM beachten)  [Quelle zitiert im Begleittext]
$sqlExporterYml = @"
global:
  scrape_timeout_offset: 500ms
  max_connections: 3
  max_idle_connections: 3

target:
  data_source_name: 'sqlserver://@$SqlHost:1433?authenticator=$Auth'
  collectors: [mssql_standard, mssql_extended]

collector_files:
  - "*.collector.yml"
"@
$sqlExporterYml | Set-Content -Path (Join-Path $InstallDir "sql_exporter.yml") -Encoding UTF8

# 3) Erweiterter Collector (TempDB, Backups, Blocking, Deadlocks)
$collectorYml = @"
collector_name: mssql_extended
metrics:
  - metric_name: mssql_deadlocks_total
    type: counter
    values: [deadlocks]
    key_labels: [instance]
    query: |
      SELECT @@SERVERNAME AS instance, CAST(cntr_value AS bigint) AS deadlocks
      FROM sys.dm_os_performance_counters
      WHERE counter_name='Number of Deadlocks/sec' AND instance_name='_Total';

  - metric_name: mssql_blocking_sessions
    type: gauge
    values: [blocked]
    key_labels: [instance]
    query: |
      SELECT @@SERVERNAME AS instance, COUNT(*) AS blocked
      FROM sys.dm_exec_requests WHERE blocking_session_id <> 0;

  - metric_name: mssql_tempdb_file_size_kb
    type: gauge
    key_labels: [file_id, file_name, type_desc]
    values: [size_kb]
    query: |
      SELECT df.file_id, df.name AS file_name, df.type_desc, CAST(df.size AS bigint)*8 AS size_kb
      FROM tempdb.sys.database_files df;

  - metric_name: mssql_tempdb_file_used_kb
    type: gauge
    key_labels: [file_id, file_name, type_desc]
    values: [used_kb]
    query: |
      SELECT df.file_id, df.name AS file_name, df.type_desc, CAST(FILEPROPERTY(df.name,'SpaceUsed') AS bigint)*8 AS used_kb
      FROM tempdb.sys.database_files df;

  - metric_name: mssql_last_full_backup_timestamp
    type: gauge
    key_labels: [database_name]
    values: [last_full_backup_ts]
    query: |
      SELECT bs.database_name, DATEDIFF(second, '19700101', MAX(bs.backup_finish_date)) AS last_full_backup_ts
      FROM msdb.dbo.backupset bs WHERE bs.type='D' GROUP BY bs.database_name;
"@
$collectorYml | Set-Content -Path (Join-Path $InstallDir "mssql_extended.collector.yml") -Encoding UTF8

# 4) Dienst neu anlegen – läuft unter DOMAIN-Servicekonto (erfordert das Recht "Als Dienst anmelden")
if (Get-Service -Name $svcName -ErrorAction SilentlyContinue) {
  & sc.exe stop $svcName | Out-Null
  & sc.exe delete $svcName | Out-Null
}

$svcArgs = "-config.file `"$InstallDir\\sql_exporter.yml`" -web.listen-address `":$ListenPort`""
$binPath = "`"$exe`" $svcArgs"
& sc.exe create $svcName binPath= $binPath start= auto obj= $ServiceAccount password= "Start#123456789*" | Out-Null
# Hinweis: Passwort sicher mit 'sc.exe config ... password=' setzen oder via GMSA laufen lassen.

# 5) Firewall öffnen
# if (-not (Get-NetFirewallRule -DisplayName "sql_exporter_$ListenPort" -ErrorAction SilentlyContinue)) {
#  New-NetFirewallRule -DisplayName "sql_exporter_$ListenPort" -Direction Inbound -Action Allow -Protocol TCP -LocalPort $ListenPort | Out-Null
# }

# 6) Start & Validierung
Start-Service $svcName
#Start-Sleep -Seconds 2
#$metricsUrl = "http://localhost:$ListenPort/metrics"
#try {
#  $res = Invoke-WebRequest -Uri $metricsUrl -UseBasicParsing -TimeoutSec 5
#  if ($res.StatusCode -eq 200) {
#    Write-Host "OK: sql_exporter liefert Metriken auf $metricsUrl"
#    Write-Host "DSN nutzt $Auth (Windows-Auth)."
#  }
#} catch {
#  Write-Warning "Konnte $metricsUrl nicht abrufen. Prüfe Dienst/Firewall/SQL-Rechte."
#}
