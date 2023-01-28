USE [Lake_Control]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [Control].[Complete]
	(	@Server			sysname = NULL,			-- Optional - Specifies a linked server as the location for the __$Complete table, default is local server
		@Database		sysname = NULL,			-- Optional - Database containing __$Complete table, default is current database
		@Schema			sysname = NULL,			-- Madatory - Schema containing __$Complete table, default is dbo schema
		@CompleteDate	datetime2(3) = NULL,	-- Optional - Value to be assigned to __$CompleteDate column in __$Complete table, NULL = leave value unchanged, default if no current value exists is 1900-01-01
		@LoadedDate		datetime2(7) = NULL,	-- Optional - Value to be assigned toLoadedDate column in __$Complete table, NULL = leave value unchanged, default if no current value exists is 1900-01-01
		@RetentionDate	datetime2(7) = NULL		-- Optional - Value to be assigned to RetentionDate column in __$Complete table, NULL = leave value unchanged, default if no current value exists is 1900-01-01
	)
AS
	SET NOCOUNT ON

	DECLARE	@SQL nvarchar(MAX)
	DECLARE @i bit

	SET @Server = REPLACE(REPLACE(@Server,']',''),'[','')
	SET @Database = ISNULL(REPLACE(REPLACE(@Database,']',''),'[',''),db_name())
	SET @Schema = ISNULL(REPLACE(REPLACE(@Schema,']',''),'[',''),'dbo')

	SET @SQL = 'SELECT @i = ISNULL(MAX(x),0) FROM '+Control.RemoteQuery('SELECT	1 x
																		FROM	['+@Database+'].sys.schemas s WITH (NOLOCK)
																				INNER JOIN ['+@Database+'].sys.tables t WITH (NOLOCK) ON (t.schema_id = s.schema_id)
																		WHERE	s.name = '''+@Schema+'''
																				AND t.name = ''__$Complete''
																		',@Server,DEFAULT)+' x'
	EXECUTE dbo.sp_executesql @SQL, N'@i bit OUTPUT', @i=@i OUTPUT
	IF @i = 0
	BEGIN
		SET @SQL = 'CREATE TABLE ['+@Database+'].['+@Schema+'].__$Complete (CompleteDate datetime2(3), LoadedDate datetime2(7), RetentionDate datetime2(7))' 
		IF ISNULL(@Server,@@SERVERNAME) <> @@SERVERNAME SET @SQL = 'EXEC('''+@SQL+''') AT ['+@Server+']'
		EXECUTE dbo.sp_executesql @SQL
	END

	SET @SQL = 'MERGE ['+@Database+'].['+@Schema+'].__$Complete y
				USING (SELECT '+ISNULL(''''+CONVERT(varchar,@CompleteDate)+'''','NULL')+' CompleteDate, '+ISNULL(''''+CONVERT(varchar,@LoadedDate)+'''','NULL')+' LoadedDate, '+ISNULL(''''+CONVERT(varchar,@RetentionDate)+'''','NULL')+' RetentionDate) x ON 1=1
				WHEN MATCHED THEN UPDATE SET CompleteDate = COALESCE(x.CompleteDate, y.CompleteDate, ''1900-01-01''), LoadedDate = COALESCE(x.LoadedDate, y.LoadedDate, ''1900-01-01''), RetentionDate = COALESCE(x.RetentionDate, y.RetentionDate, ''1900-01-01'')
				WHEN NOT MATCHED THEN INSERT (CompleteDate, LoadedDate, RetentionDate) VALUES (ISNULL(x.CompleteDate, ''1900-01-01''), ISNULL(x.LoadedDate, ''1900-01-01''), ISNULL(x.RetentionDate, ''1900-01-01''));' 
	IF ISNULL(@Server,@@SERVERNAME) <> @@SERVERNAME SET @SQL = 'EXEC('''+REPLACE(@SQL,'''','''''')+''') AT ['+@Server+']'
	EXECUTE dbo.sp_executesql @SQL
GO


