USE [Lake_Control]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [Control].[Sequence]
(	
	@Database			sysname = NULL,		
	@Schema				sysname = NULL,			-- Optional - Focusses any compression changes to just one schema rather than all tables using the nominated partition scheme 
	@Table				sysname = NULL,			-- Optional - Nominates a single table which will be assessed for index defragmentation
	@DryRun				bit = 0					-- Optional - (0=FALSE/1=TRUE) - Allows the procedure to be dry run whereby it will report all of its intended actions but not actually execute them
)
AS

	SET NOCOUNT ON 

	DECLARE @Sequence TABLE (	i int IDENTITY(1,1), 
								SchemaName varchar(128), 
								TableName varchar(128), 
								ddl varchar(MAX),
								StartTime datetime2(3),
								EndTime	datetime2(3),
								OutOfSequence varchar(10),
								ErrorMessage varchar(MAX)
							)
	DECLARE @Issue TABLE	(	__$From datetime2(7),
								__$To datetime2(7),
								__$Load datetime2(7),
								__$Action varchar(128)
							)


	DECLARE	@StartTime datetime2(0)

	DECLARE @i int,
			@IssueCount int

	DECLARE @SQL nvarchar(MAX)

	DECLARE @Alert			varchar(MAX),
			@Message		varchar(MAX),
			@Error			int,
			@ErrorMessage	varchar(MAX)

BEGIN

	SET @Database = ISNULL(@Database,db_name()) -- default to current database

	-- Ensure Proxy/Database/Schema name parameters are not supplied in square brackets
	SET @Database = REPLACE(REPLACE(@Database,'[',''),']','')
	SET @Schema = REPLACE(REPLACE(@Schema,'[',''),']','')

	SET @StartTime = GETDATE()

	SET @Message = CHAR(13) + CHAR(10) + CONVERT(varchar,SYSDATETIME()) + ' - Compiling Table List'

	-- Get a list of Schemas, Tables and their index keys to build the ddl
	SET @SQL = 'SELECT	s.Name SchemaName, t.name TableName, c.name PartitionColumn
							INTO #k
							FROM	['+@Database+'].sys.schemas s
									INNER JOIN ['+@Database+'].sys.tables t ON t.schema_id = s.schema_id
									INNER JOIN ['+@Database+'].sys.indexes i ON i.object_id = t.object_id
									INNER JOIN ['+@Database+'].sys.index_columns ic ON (ic.object_id = i.object_id AND ic.index_id = i.index_id)
									INNER JOIN ['+@Database+'].sys.columns c ON (c.object_id = i.object_id AND c.column_id = ic.column_id)
							WHERE t.name NOT LIKE ''%_XX'' AND (i.type = 1 OR i.name LIKE ''PK%'') AND c.name NOT LIKE ''__$%'' AND t.name NOT LIKE ''__$%''' + CASE WHEN @Schema IS NOT NULL THEN '
									AND s.name = '''+@Schema+'''' ELSE '' END + CASE WHEN @Table IS NOT NULL THEN '
									AND t.name = '''+@Table+'''' ELSE '' END + '
							GROUP BY s.Name, t.name, c.name
														
				SELECT				p.SchemaName, p.TableName, 
									''SELECT __$From, __$To, __$Load, __$Action FROM ( SELECT LEAD(__$From) OVER (PARTITION BY '' + 
									STUFF( (SELECT '', '' + PartitionColumn FROM #k as c WHERE c.SchemaName = p.SchemaName AND c.TableName = p.TableName ORDER BY SchemaName, TableName FOR XML PATH('''') ), 1, 1, '''') +
									''  ORDER BY __$Load) Next__$From, * from Lake_Cerillion.'' + SchemaName + ''.'' + TableName + '') x where Next__$From <= __$From'' as ddl
							FROM #k	as p
							GROUP BY SchemaName, TableName
							ORDER BY SchemaName DESC, TableName DESC'

	IF @DryRun = 1 SELECT @SQL

	INSERT @Sequence (SchemaName, TableName, ddl) EXECUTE (@SQL)
	SET @i = @@ROWCOUNT

	IF @DryRun = 1 SELECT * FROM @Sequence

	-- For each Table, run the ddl statement and return a count of out of order records into @IssueCount
	SET @Message = @Message + CHAR(13) + CHAR(10) + CONVERT(varchar,SYSDATETIME()) + ' - Analysing Sequencing:'
	WHILE @i > 0 AND @DryRun = 0
	BEGIN
		SET	@Error = NULL
		SET	@ErrorMessage = NULL
			BEGIN
				UPDATE @Sequence SET StartTime = SYSDATETIME() WHERE i = @i
				SELECT @SQL = ddl FROM @Sequence WHERE i = @i
				BEGIN TRY
					INSERT @Issue EXEC sp_executesql @SQL
					SET @IssueCount = @@ROWCOUNT
				END TRY
				BEGIN CATCH
					SET	@Error = ERROR_NUMBER()
					SET	@ErrorMessage = ERROR_MESSAGE()
				END CATCH
				UPDATE @Sequence SET EndTime = SYSDATETIME(), OutOfSequence = @IssueCount, ErrorMessage = CONVERT(varchar,@Error) + ' - ' + @ErrorMessage WHERE i = @i
			END

		SELECT @Message = @Message + CHAR(13) + CHAR(10) + CHAR(9) 
						+ '[' + SchemaName + '].[' + TableName + ']' + CHAR(9) 
						+ '(' + CONVERT(varchar,StartTime) + ' - ' + CONVERT(varchar,EndTime) + ')' + CHAR(9) 
						+ 'Out Of Sequence Records: ' + OutOfSequence + CHAR(9) 
						+ CASE WHEN ErrorMessage IS NULL THEN '' ELSE CHAR(13) + CHAR(10) + CHAR(9) + CHAR(9) + CHAR(9) + CHAR(9) + CHAR(9) + ErrorMessage END
		FROM	@Sequence 
		WHERE	i = @i AND OutOfSequence <> 0
		SET @i = @i - 1
	END

	SET @Alert = @@SERVERNAME + ' - ' + OBJECT_NAME(@@PROCID) + ' Finished for ['+ISNULL(@Database,DB_NAME(DB_ID()))+']'+ISNULL('.['+@Schema+']','')
	SET @Message = ISNULL(@Database,DB_NAME(DB_ID())) + CHAR(13) + CHAR(10)
						+ CHAR(13) + CHAR(10) + 'Supplied parameters: '
						+ CHAR(13) + CHAR(10) + CHAR(9) + '@Database = ' + ISNULL('''' + @Database + '''','NULL')
						+ CHAR(13) + CHAR(10) + CHAR(9) + '@Schema = ' + ISNULL('''' + @Schema + '''','NULL')
						+ CHAR(13) + CHAR(10) + CHAR(9) + '@Table = ' + ISNULL('''' + @Table + '''','NULL')
						+ CHAR(13) + CHAR(10) + @Message
						+ CHAR(13) + CHAR(10) + CONVERT(varchar,SYSDATETIME()) + ' - Finished'

	IF @DryRun = 0 AND COUNT(@i) > 0
		EXEC [msdb].dbo.sp_send_dbmail
			@profile_name = 'mt-itp-smtp',
			@recipients = 'some@emailaddress.com',
			@body = @Message,
			@subject = @Alert
	
END
GO


