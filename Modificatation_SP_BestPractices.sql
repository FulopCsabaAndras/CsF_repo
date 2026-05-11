/* =========================================
   SharePoint SQL Best Practice Script
   ========================================= */

-- Enable advanced options
EXEC sp_configure 'show advanced options', 1;
RECONFIGURE;
GO

/* =========================================
   1. MAXDOP (SharePoint Empfehlung = 1)
   ========================================= */
PRINT 'Setting MAXDOP to 1...';
EXEC sp_configure 'max degree of parallelism', 1;
RECONFIGURE;
GO

/* =========================================
   2. Cost Threshold for Parallelism (empfohlen: 50)
   ========================================= */
PRINT 'Setting Cost Threshold to 50...';
EXEC sp_configure 'cost threshold for parallelism', 50;
RECONFIGURE;
GO

/* =========================================
   3. Max Server Memory setzen
   Beispiel: 32 GB = 32768 MB
   -> Anpassen an eure Umgebung!
   ========================================= */
PRINT 'Setting Max Server Memory...';
EXEC sp_configure 'max server memory (MB)', 32768;
RECONFIGURE;
GO

/* =========================================
   4. Datenbank-Einstellungen prüfen und korrigieren
   ========================================= */

DECLARE @SQL NVARCHAR(MAX) = '';

SELECT @SQL = @SQL + '
ALTER DATABASE [' + name + '] SET AUTO_CLOSE OFF;
ALTER DATABASE [' + name + '] SET AUTO_SHRINK OFF;
ALTER DATABASE [' + name + '] SET AUTO_UPDATE_STATISTICS ON;
ALTER DATABASE [' + name + '] SET AUTO_CREATE_STATISTICS ON;
'
FROM sys.databases
WHERE name NOT IN ('tempdb'); -- tempdb separat behandeln

PRINT 'Applying DB settings...';
EXEC sp_executesql @SQL;
GO

/* =========================================
   5. Autogrowth Settings (ALLE DB Files!)
   ========================================= */
/*
Empfehlung:
- FIXE Größe (MB), NICHT Prozent
- Data Files: 512 MB oder 1024 MB
- Log Files: 256 MB oder 512 MB
*/

DECLARE @GrowthSQL NVARCHAR(MAX) = '';

SELECT @GrowthSQL = @GrowthSQL + '
ALTER DATABASE [' + DB_NAME(database_id) + '] 
MODIFY FILE ( NAME = ' + name + ', FILEGROWTH = 512MB );'
FROM sys.master_files
WHERE type_desc = 'ROWS';

-- LOG Files separat
SELECT @GrowthSQL = @GrowthSQL + '
ALTER DATABASE [' + DB_NAME(database_id) + '] 
MODIFY FILE ( NAME = ' + name + ', FILEGROWTH = 256MB );'
FROM sys.master_files
WHERE type_desc = 'LOG';

PRINT 'Setting Autogrowth settings...';
EXEC sp_executesql @GrowthSQL;
GO

/* =========================================
   6. TempDB Empfehlung prüfen (nur Anzeige)
   ========================================= */
PRINT 'TempDB Files:';
SELECT name, size*8/1024 AS Size_MB, physical_name
FROM sys.master_files
WHERE database_id = DB_ID('tempdb');
GO

/* =========================================
   7. Aktuelle Einstellungen anzeigen
   ========================================= */

PRINT 'Current Server Settings:';

EXEC sp_configure 'max degree of parallelism';
EXEC sp_configure 'cost threshold for parallelism';
EXEC sp_configure 'max server memory (MB)';
GO

PRINT 'Database Settings Overview:';

SELECT 
    name,
    is_auto_close_on,
    is_auto_shrink_on,
    is_auto_update_stats_on,
    is_auto_create_stats_on
FROM sys.databases;
GO

PRINT 'Autogrowth Settings:';

SELECT 
    DB_NAME(database_id) AS DBName,
    name AS LogicalName,
    type_desc,
    growth,
    is_percent_growth
FROM sys.master_files;
GO