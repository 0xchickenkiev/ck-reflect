USE [Lake_Control]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [Control].[RemoteDBMS]
	(	@Server		sysname = NULL,
		@Proxy		sysname = NULL,					-- Optional switch to force metadata behaviour to that of a Source location (rather than a Target location) when deriving default DML Dates (only relevant for Non-specific forms where default DML date is current date/time for sources and start of time for targets)
		@DBMS		varchar(10) = NULL OUTPUT,
		@RecallAutonomous bit = 1
	)
AS
	SET NOCOUNT ON

	DECLARE @SQL nvarchar(max)

	-- As there are no common set of metadata tables for all DBMS and specifically Oracle and MySQL do not share common metadata tables, we have to speculatively attempt to access a known metadata 
	-- table or view for each potential DBMS, interpreting a failure as non-existence of that metadata table/view and there fore an indication the nominated server is not running on that DBMS
	--
	-- Unfortunately, triggering a failure due to non-existence of an object in the query via an ODBC linked server is so toxic that it can trigger a failure in the requestin stored procedure,
	-- or even the stored procedure calling that stored procedure, and even if that is survived create an unstable transaction state for subsequent accesses to the linked server!
	--
	-- So, we formulated the approach below whereby this procedure calls itself without propagating any transaction (via the predefined linked server LOOPBACK_AUTONOMOUS for this purpose)
	-- In this way any evil done in this routine stays in this routine with only an OUTPUT parameter returned to the calling process 
	IF @RecallAutonomous = 1 
		BEGIN
			SET @SQL = 'EXEC LOOPBACK_AUTONOMOUS.['+db_name()+'].['+OBJECT_SCHEMA_NAME(@@PROCID)+'].['+OBJECT_NAME(@@PROCID)+'] @Server, @Proxy, @DBMS OUTPUT, @RecallAutonomous = 0' -- @RecallAutonomous = 0 to ensure it doesn't recall itself again... and again....etc
			EXECUTE dbo.sp_executesql @SQL, N'@Server sysname, @Proxy sysname, @DBMS varchar(10) OUTPUT', @Server=@Server, @Proxy=@Proxy, @DBMS=@DBMS OUTPUT 
		END
	ELSE
		BEGIN
			BEGIN TRY -- Try MySQL
				SET @SQL = 'IF NOT EXISTS (SELECT 1 FROM '+Control.RemoteQuery('SELECT * FROM INFORMATION_SCHEMA.engines WHERE ENGINE = ''InnoDB''',@Server,@Proxy)+' x) RAISERROR(''Not MySQL'',16,1)'
				EXECUTE (@SQL)
				SET @DBMS = 'MySQL' -- If it gets this far, the speculative attempt to read the metadata was successful and DBMS is assumed to be MySQL
			END TRY 
			BEGIN CATCH 
				SET @DBMS = NULL
			END CATCH
			IF @DBMS IS NULL
				BEGIN TRY -- Try Oracle
					SET @SQL = 'IF NOT EXISTS (SELECT 1 FROM '+Control.RemoteQuery('SELECT * FROM V$DATABASE',@Server,@Proxy)+' x) RAISERROR(''Not Oracle'',16,1)'
					EXECUTE (@SQL)
					SET @DBMS = 'Oracle' -- If it gets this far, the speculative attempt to read the metadata was successful and DBMS is assumed to be Oracle
				END TRY 
				BEGIN CATCH
					SET @DBMS = NULL
				END CATCH
			PRINT '@DBMS: '+ISNULL(@DBMS,'NULL')
		END
GO


