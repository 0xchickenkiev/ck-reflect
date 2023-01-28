USE [Lake_Control]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [Control].[Transfer]
(	
	@Task				varchar(50),					-- Mandatory - Limits the data transfer to a specific task (e.g. CDC-LAK or LAK-LAK)
	@SourceServer		varchar(128) = @@SERVERNAME,	-- Optional - Specifies a linked server as the location for the source database, default is local server
	@SourceDatabase		varchar(128),					-- Madatory - Database containing source table(s)
	@SourceSchema		varchar(128),					-- Madatory - Schema containing source table(s)
	@SourceProxy		varchar(128) = @@SERVERNAME,	-- Optional - Specifies a linked server as the Lake proxy for the source database to localise intensive metadata gethering activities
	@TargetServer		varchar(128) = @@SERVERNAME,	-- Optional - Specifies a linked server as the location for the target database, default is local server
	@TargetDatabase		varchar(128),					-- Madatory - Database containing target table(s)
	@TargetSchema		varchar(128),					-- Madatory - Schema containing target table(s)
	@Table				varchar(128) = NULL,			-- Optional - Focusses the data transfer on a specific table. The default is all tables present in both Source and Target schemas which conform to the types required by the task
	@DateBound			datetime2(7) = NULL,			-- Optional - Applies an upper date bound to the source data to run upto but not beyond a defined point in time. Default of NULL runs up to current defined CompleteDate in source
	@PreserveBatch		bit = 1,						-- Optional - 1 = only process complete batches from any Lake source (i.e. upt but not past CompleteDate), 0 = process any tables up to latest point of consistency even if this goes beyond CompleteDate for the source. Has no effect on non Lake sources
	@DryRun				bit = 0							-- Optional - 1 = execute process as a dry run, with full logging to check gtenerated SQL and From/To dates without actually running any code, 0 = run as normal
)
AS

	DECLARE @Actions	TABLE (Action varchar(20), SourceForm char(3), TargetForm char(3), SQL varchar(MAX), ActionPriority int, Optional char(1), ModifiedDate datetime2(3))
	DECLARE @Tables		TABLE (i int IDENTITY(1,1), OriginTableName varchar(128), SourceTableName varchar(128), SourceDBMS varchar(10), SourceCompatibilityLevel tinyint, SourceForm char(3), TargetTableName varchar(128), TargetDBMS varchar(10), TargetCompatibilityLevel tinyint, TargetForm char(3), TargetPartitionFunction varchar(128), TargetPartitionKey varchar(128), FromDate datetime2(7), ToDate datetime2(7), RetentionDate datetime2(7), Source smallint, ColumnNames varchar(MAX), ColumnCollations varchar(MAX), ColumnPrefixes varchar(MAX), KeyNames varchar(MAX), KeyPrefixes varchar(MAX), Conditions varchar(MAX), Assignments varchar(MAX), SQL varchar(MAX))
	DECLARE @Columns	TABLE (SourceTableName varchar(128), TargetTableName varchar(128), ColumnOrdinal int, ColumnName varchar(128), KeyColumn bit, ColumnType sysname, ColumnCollation sysname NULL, PRIMARY KEY(SourceTableName, TargetTableName, ColumnOrdinal))
	DECLARE @Queries	TABLE (i int IDENTITY(1,1), Action varchar(20), TargetObject varchar(128), SourceObject varchar(128), ActionPriority int, Optional char(1), SQL varchar(MAX), ColumnNames varchar(MAX), KeyNames varchar(MAX), TemplateModifiedDate datetime2(3))

	DECLARE	@SourceMetadata TABLE (TableName sysname, DBMS varchar(10), CompatibilityLevel tinyint, Form char(3), OriginTableName sysname NULL, OriginSchemaName sysname NULL, PartitionFunction sysname NULL, PartitionKey sysname NULL, DDLDate datetime2(3), DMLDate datetime2(7), RetentionDate datetime2(7), LSN binary(10), ColumnOrdinal int, ColumnName sysname, ColumnType sysname, ColumnLength int, ColumnScale tinyint, ColumnPrecision tinyint, ColumnCollation sysname NULL, KeyOrdinal int)
	DECLARE	@TargetMetadata TABLE (TableName sysname, DBMS varchar(10), CompatibilityLevel tinyint, Form char(3), OriginTableName sysname NULL, OriginSchemaName sysname NULL, PartitionFunction sysname NULL, PartitionKey sysname NULL, DDLDate datetime2(3), DMLDate datetime2(7), RetentionDate datetime2(7), LSN binary(10), ColumnOrdinal int, ColumnName sysname, ColumnType sysname, ColumnLength int, ColumnScale tinyint, ColumnPrecision tinyint, ColumnCollation sysname NULL, KeyOrdinal int)

	DECLARE	@SourcePath		varchar(400)
	DECLARE	@TargetPath		varchar(400)

	DECLARE	@BatchDate					datetime2(7),
			@BatchSuffix				varchar(25),
			@TableName					varchar(128),
			@Step						varchar(128),
			@PartitionFunction			varchar(128),
			@FromDate					datetime2(7),
			@ToDate						datetime2(7),
			@MinFromDate				datetime2(7),
			@MaxFromDate				datetime2(7),
			@MaxToDate					datetime2(7),
			@CurrentCompleteDate		datetime2(7),
			@NewLoadedDate				datetime2(7),
			@NewCompleteDate			datetime2(7),
			@PartitionDate				datetime2(7),
			@SourceForm					char(3),
			@TargetForm					char(3)

	DECLARE	@i				int,
			@SQL			nvarchar(MAX),
			@InsertCount	int = 0,
			@UpdateCount	int = 0,
			@DeleteCount	int = 0,
			@Error			int,
			@ErrorMessage	varchar(MAX),
			@Message		varchar(MAX),
			@Alert			varchar(MAX),
			@Failures		int

SET NOCOUNT ON 
	
BEGIN TRY

	------------------------------------------------------ COLLECT METADATA ----------------------------------------------------------------

	-- The Task nominated in the calling parameter can have any number of Templated queries for different actions - retrieve them or raise error if none are defined
	INSERT	@Actions (Action, SourceForm, TargetForm, SQL, ActionPriority, Optional, ModifiedDate) SELECT Action, SourceForm, TargetForm, SQL, ActionPriority, Optional, ModifiedDate FROM Control.Template WITH (NOLOCK) WHERE Task = @Task AND Enabled = 'Y' 
	IF @@ROWCOUNT = 0 RAISERROR('There are no Templates in Control.Query for specified Task: %s',16,1,@Task)
	SELECT DISTINCT @SourceForm = SourceForm, @TargetForm = TargetForm FROM @Actions

	-- Source and Target Table/Column Metadata
	INSERT @SourceMetadata (TableName, DBMS, CompatibilityLevel, Form, OriginTableName, OriginSchemaName, PartitionFunction, PartitionKey, DDLDate, DMLDate, RetentionDate, LSN, ColumnOrdinal, ColumnName, ColumnType, ColumnLength, ColumnScale, ColumnPrecision, ColumnCollation, KeyOrdinal) 
		EXEC Control.Metadata @Server = @SourceServer, @Database = @SourceDatabase, @Schema = @SourceSchema, @Proxy = @SourceProxy, @Table = DEFAULT, @DateBound = @DateBound, @Consistent = @PreserveBatch, @Source = 1
	INSERT @TargetMetadata (TableName, DBMS, CompatibilityLevel, Form, OriginTableName, OriginSchemaName, PartitionFunction, PartitionKey, DDLDate, DMLDate, RetentionDate, LSN, ColumnOrdinal, ColumnName, ColumnType, ColumnLength, ColumnScale, ColumnPrecision, ColumnCollation, KeyOrdinal) 
		EXEC Control.Metadata @Server = @TargetServer, @Database = @TargetDatabase, @Schema = @TargetSchema, @Proxy = DEFAULT, @Table = @Table, @DateBound = DEFAULT, @Source = 0 -- Only replay the supplied @Table parameter to Target table metadata due to fuzzy matching applied to Source table names to cope with derived CDC names - the @Table parameter will eventually be applied to source tables when metadata is joined to the @Table filtered Target tables 
	-- Construct a list of all Tables which are common to both Source and Target Server/Database/Schema which conform to the Form transition of the retrieved actions and whether they have a corresponding Archive table
	INSERT	@Tables (OriginTableName, SourceTableName, SourceDBMS, SourceCompatibilityLevel, SourceForm, TargetTableName, TargetDBMS, TargetCompatibilityLevel, TargetForm, TargetPartitionFunction, TargetPartitionKey, FromDate, ToDate, RetentionDate, Source)
	SELECT	s.OriginTableName, 
			s.TableName SourceTableName, 
			s.DBMS SourceDBMS,
			s.CompatibilityLevel SourceCompatibilityLevel,
			s.Form SourceForm,
			t.TableName TargetTableName, 
			t.DBMS TargetDBMS,
			t.CompatibilityLevel TargetCompatibilityLevel,
			t.Form TargetForm,
			t.PartitionFunction TargetPartitionFunction,
			t.PartitionKey TargetPartitionKey,
			COALESCE(t.DMLDate,CONVERT(datetime2(7),'1900-01-01')) FromDate,
			COALESCE(s.DMLDate,t.DMLDate,CONVERT(datetime2(7),'1900-01-01')) ToDate,
			COALESCE(t.RetentionDate,CONVERT(datetime2(7),'1900-01-01')) RetentionDate,
			CONVERT(smallint,RIGHT(REPLACE(s.OriginTableName,t.OriginTableName,''), 4)) Source
	FROM	(SELECT DISTINCT TableName, DBMS, CompatibilityLevel, Form, OriginTableName, DDLDate, DMLDate FROM @SourceMetadata) s
			INNER HASH JOIN (SELECT DISTINCT SourceForm, TargetForm FROM @Actions) x ON (x.SourceForm = s.Form)
			INNER HASH JOIN (SELECT DISTINCT TableName, DBMS, CompatibilityLevel, Form, OriginTableName, PartitionFunction, PartitionKey, DDLDate, DMLDate, RetentionDate FROM @TargetMetadata) t ON (t.Form = x.TargetForm AND (t.OriginTableName = s.OriginTableName OR s.OriginTableName LIKE t.OriginTableName+'_20[0-2][0-9][0-1][0-9]' OR s.OriginTableName LIKE t.OriginTableName+'20[0-2][0-9][0-1][0-9]')) 
	ORDER BY s.OriginTableName DESC, t.TableName DESC

	------------------------------------------------------ BATCH DATES  ----------------------------------------------------------------

	SELECT @MinFromDate = MIN(CASE WHEN FromDate <> ToDate THEN FromDate ELSE NULL END), @MaxFromDate = MAX(FromDate), @MaxToDate = MAX(ToDate) FROM @Tables

	-- Derive the current and new CompleteDates and consequently the identifer for this batch 

	SELECT	@CurrentCompleteDate = ISNULL(MAX(DMLDate),CONVERT(datetime2(3),'1900-01-01')) FROM @TargetMetadata WHERE TableName = '__$Complete' AND ColumnName = 'CompleteDate'
	SELECT	@NewCompleteDate = ISNULL(MAX(DMLDate),CONVERT(datetime2(3),'1900-01-01')) FROM @SourceMetadata WHERE TableName = '__$Complete' AND ColumnName = 'CompleteDate'	-- Complete Date from source (if exists)

	IF @DateBound < @NewCompleteDate SET @NewCompleteDate = @DateBound -- if @DateBound parameter enforces a lower processing ceiling than NewCompleteDate, then use @DateBound as NewCompleteDate
	IF @NewCompleteDate < @CurrentCompleteDate SET @NewCompleteDate = @CurrentCompleteDate -- we cant move backwrds, something must have happened in source - set New Complete date to Current Complete date

	SET @BatchDate = @MaxToDate
	SET @BatchSuffix = CASE 
							WHEN @DryRun = 1 THEN ' (Dry Run)' 
							WHEN @MaxToDate <= @MaxFromDate THEN ' (Waiting)' 
							WHEN @MaxToDate > @NewCompleteDate THEN ' (Incomplete)' 
							WHEN @NewCompleteDate <= @CurrentCompleteDate THEN ' (Waiting)' 
							ELSE '' 
						END

	-- Time mapping
	EXEC Control.Time @SourceServer, @SourceDatabase, @SourceSchema, @SourceProxy, @SourceForm, @TargetServer, @TargetDatabase, @TargetSchema, @TargetForm, @MinFromDate, @NewCompleteDate, @LastLoad = @NewLoadedDate OUTPUT, @DryRun = @DryRun


	------------------------------------------------------ PARTITION MANAGEMENT  ----------------------------------------------------------------

	-- If the batch high watermark crosses the midnight boundary manage partitions before the data crosses the midnight
	IF CONVERT(date,CONVERT(varchar(10),@MaxFromDate,121)) < CONVERT(date,CONVERT(varchar(10),@MaxToDate,121))
		BEGIN
			SET @PartitionDate = CONVERT(date,CONVERT(varchar(10),@MaxToDate,121))
			-- Cycle through as many partition functions as are used by the target tables in scope
			SELECT @PartitionFunction = MIN(TargetPartitionFunction) FROM @Tables
			WHILE @PartitionFunction IS NOT NULL
				BEGIN
					EXEC Control.Partitioning @PartitionFunction, @PartitionDate, @RowBoundary = NULL, @Server = @TargetServer, @Database = @TargetDatabase -- Limit partition management to adding the new boundary at midnight and defer gap filling and compression to an independent asynchronous process
					SELECT @PartitionFunction = MIN(TargetPartitionFunction) FROM @Tables WHERE TargetPartitionFunction > @PartitionFunction
				END 
		END

	------------------------------------------------------ CONSTRUCT QUERIES  ----------------------------------------------------------------

	-- Expand above source/target table list to full common column list for all source/target tables
	INSERT @Columns (SourceTableName, TargetTableName,ColumnOrdinal,ColumnName,KeyColumn,ColumnType,ColumnCollation)
	SELECT	x.SourceTableName,
			x.TargetTableName,
			RANK() OVER (PARTITION BY SourceTableName ORDER BY ISNULL(CASE s.Form WHEN 'LAK' THEN s.KeyOrdinal ELSE t.KeyOrdinal END,999), CASE s.Form WHEN 'LAK' THEN s.ColumnOrdinal ELSE t.ColumnOrdinal END) ColumnOrdinal, -- Order columns as per Lake table but with key columns pushed to front of record
			REPLACE(s.ColumnName,'&','#ampersand#') ColumnName, -- Necessary to prevent an & in column names being replaced with &amp; in the FOR XML PATH clauses below (then returned to & in the subsequent final statement) 
			CASE WHEN t.Form = 'LAK' AND t.KeyOrdinal > 0 THEN 1 WHEN t.Form <> 'LAK' AND s.KeyOrdinal > 0 THEN 1 ELSE 0 END KeyColumn,
			s.ColumnType,
			NULLIF(t.ColumnCollation,s.ColumnCollation) ColumnCollation
	FROM	@Tables x
			INNER JOIN @SourceMetadata s ON (s.TableName = x.SourceTableName)
			INNER JOIN @TargetMetadata t ON (t.TableName = x.TargetTableName AND t.ColumnName = s.ColumnName)

	-- Reduce column metadata into concatenated list strings and update the table metadata
	UPDATE	t
	SET		ColumnNames		= STUFF (	(	SELECT  ', ['+ColumnName+']'  -- String together all column names using this pattern and use STUFF to remove superfluous comma-space in first 2 characters
											FROM	@columns c
											WHERE	c.SourceTableName = t.SourceTableName AND c.TargetTableName = t.TargetTableName
											ORDER BY ColumnOrdinal
											FOR XML PATH('')
										),1,2,''), 
			ColumnCollations = STUFF (	(	SELECT  ', '+CASE ColumnType WHEN 'image' THEN 'CONVERT(varbinary(MAX),['+ColumnName+']) ['+ColumnName+']' ELSE '['+ColumnName+']' END+ISNULL(' COLLATE '+ColumnCollation+' ['+ColumnName+']','')  -- String together all column names using this pattern and use STUFF to remove superfluous comma-space in first 2 characters
											FROM	@columns c
											WHERE	c.SourceTableName = t.SourceTableName AND c.TargetTableName = t.TargetTableName
											ORDER BY ColumnOrdinal
											FOR XML PATH('')
										),1,2,''), 
			ColumnPrefixes	= STUFF	(	(	SELECT  ', a.['+ColumnName+']'  -- String together all column names using this pattern and use STUFF to remove superfluous comma-space in first 2 characters
											FROM	@columns c
											WHERE	c.SourceTableName = t.SourceTableName AND c.TargetTableName = t.TargetTableName
											ORDER BY ColumnOrdinal
											FOR XML PATH('')
										),1,2,''), 
			KeyNames		= STUFF	(	(	SELECT  CASE KeyColumn WHEN 1 THEN ', ['+ColumnName+']' ELSE '' END  -- String together all key column names using this pattern and use STUFF to remove superfluous comma-space in first 2 characters 
											FROM	@columns c
											WHERE	c.SourceTableName = t.SourceTableName AND c.TargetTableName = t.TargetTableName
											ORDER BY ColumnOrdinal
											FOR XML PATH('')
										),1,2,''), 
			KeyPrefixes		= STUFF	(	(	SELECT  CASE KeyColumn WHEN 1 THEN ', a.['+ColumnName+']' ELSE '' END  -- String together all key column names using this pattern and use STUFF to remove superfluous comma-space in first 2 characters
											FROM	@columns c
											WHERE	c.SourceTableName = t.SourceTableName AND c.TargetTableName = t.TargetTableName
											ORDER BY ColumnOrdinal
											FOR XML PATH('')
										),1,2,''), 
			Conditions		= STUFF	(	(	SELECT  CASE KeyColumn WHEN 1 THEN ' AND b.['+ColumnName+'] = a.['+ColumnName+']' ELSE '' END   -- String together all key column names using this pattern and use STUFF to remove superfluous AND in first 5 characters
											FROM	@columns c
											WHERE	c.SourceTableName = t.SourceTableName AND c.TargetTableName = t.TargetTableName
											ORDER BY ColumnOrdinal
											FOR XML PATH('')
										),1,5,''), 
			Assignments		= STUFF	(	(	SELECT  CASE KeyColumn WHEN 1 THEN '' ELSE ', b.['+ColumnName+'] = a.['+ColumnName+']' END  -- String together all non-key column names using this pattern and use STUFF to remove superfluous comma-space in first 2 characters
											FROM	@columns c
											WHERE	c.SourceTableName = t.SourceTableName AND c.TargetTableName = t.TargetTableName
											ORDER BY ColumnOrdinal
											FOR XML PATH('')
										),1,2,'')
	FROM	@Tables t
	UPDATE @Tables SET Assignments = REPLACE(Conditions,' AND',',') WHERE Assignments IS NULL -- A bit of a cludge, but easy way to deal with tables that are all key columns - put those in the otherwise empty assignemnts clause, doesn't break the resultant UPDATE clause but does slow down processing having key columns in update if not otherwise necessary

	SET @SourcePath = CASE @SourceServer WHEN @@SERVERNAME THEN '' ELSE '['+@SourceServer+'].' END + '['+@SourceDatabase+'].['+@SourceSchema+']'
	SET @TargetPath = CASE @TargetServer WHEN @@SERVERNAME THEN '' ELSE '['+@TargetServer+'].' END + '['+@TargetDatabase+'].['+@TargetSchema+']'

	-- Bring together action templates and table list (column strings) to generate one SQL statement per Action per table (only where there is some work to be done according to gap between FromDate and ToDate)
	INSERT INTO @Queries (Action, TargetObject, SourceObject, ActionPriority, Optional, SQL, ColumnNames, KeyNames, TemplateModifiedDate)
	SELECT	*
	FROM	(	SELECT	a.Action,
						t.TargetTableName TargetObject,
						t.SourceTableName SourceObject,
						a.ActionPriority,
						a.Optional,
						REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(a.SQL,
								'#SourceProxy#',ISNULL('['+@SourceProxy+']','**MISSING SUBSTITUTION: #SourceProxy#**')),
								'#TargetSchema#',ISNULL(@TargetPath,'**MISSING SUBSTITUTION: #SourceSchema#**')),
								'#SourceTable#',ISNULL(Control.Path(t.SourceTableName,@SourceSchema,@SourceDatabase,@SourceServer,@SourceProxy,t.SourceDBMS,t.SourceCompatibilityLevel,t.ColumnNames,t.SourceForm),'**MISSING SUBSTITUTION: #SourceTable#**')),
								'#TargetTable#',ISNULL(Control.Path(t.TargetTableName,@TargetSchema,@TargetDatabase,@TargetServer,DEFAULT,t.TargetDBMS,t.TargetCompatibilityLevel,DEFAULT,DEFAULT),'**MISSING SUBSTITUTION: #TargetTable#**')),
								'#BufferTable#',ISNULL('['+@TargetServer+'.'+@TargetDatabase+'.'+@TargetSchema+']','**MISSING SUBSTITUTION: #BufferTable#**')),
								'#Columns#',ISNULL(t.ColumnNames,'**MISSING SUBSTITUTION: #Columns#**')),
								'#Columns_a#',ISNULL(t.ColumnPrefixes,'**MISSING SUBSTITUTION: #Columns_a#**')),
								'#Keys#',ISNULL(t.KeyNames,'**MISSING SUBSTITUTION: #Keys#**')),
								'#Keys_a#',ISNULL(t.KeyPrefixes,'**MISSING SUBSTITUTION: #Keys_a#**')),
								'#RetentionKey#',ISNULL(t.TargetPartitionKey,'__$To')),
								'#Condition_b_a#',ISNULL(t.Conditions,'**MISSING SUBSTITUTION: #Condition_b_a#**')),
								'#Assignment_b_a#',ISNULL(t.Assignments,'**MISSING SUBSTITUTION: #Assignment_b_a#**')),
								'#ampersand#','&')SQL,
						t.ColumnNames,
						t.KeyNames,
						a.ModifiedDate
				FROM	@Tables t 
						INNER JOIN @Actions a ON (a.SourceForm = t.SourceForm AND a.TargetForm = t.TargetForm)
				WHERE	FromDate < ToDate OR @DateBound = CONVERT(datetime2(7),'9999-12-31') -- Dummy value for date bound to force every step to be run irrespective of whether metadata indicates any net changes are present
			) x
	WHERE	CHARINDEX('**MISSING SUBSTITUTION: ',SQL) = 0 OR Optional = 'N' -- If the Action is defined as optional, then do not generate query if any substitutions are not defined (for cases such as Archive action which is not generated for all tables), otherwsie generate query because we want undefined substitutions to fail and be alerted
	ORDER BY TargetObject, SourceObject, ActionPriority

	-- Concatenate all queries for the current task into a single SQL statement per object
	;WITH	conc AS (	SELECT	i, ActionPriority, SourceObject, TargetObject, SQL, 1 Queries
						FROM	@Queries
						UNION ALL
						SELECT	q.i, q.ActionPriority, c.SourceObject, c.TargetObject, c.SQL + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) + q.SQL SQL, c.Queries + 1 Queries
						FROM	conc c
								INNER JOIN @Queries q ON (q.SourceObject = c.SourceObject AND q.TargetObject = c.TargetObject AND q.i > c.i) -- queries have been loaded into table variable in execution order, so ordering this step by the identity column of the table variable will ensure correct execution
					)
	UPDATE	t	 
	SET     SQL = REPLACE(REPLACE(REPLACE(REPLACE(x.SQL,'@FromDate',''''+CONVERT(varchar,t.FromDate)+''''),'@ToDate',''''+CONVERT(varchar,t.ToDate)+''''),'@RetentionDate',''''+CONVERT(varchar,t.RetentionDate)+''''),'@Source',CONVERT(varchar,t.Source))
	FROM	@Tables t
			INNER JOIN	(	SELECT *, MAX(Queries) OVER (PARTITION BY SourceObject, TargetObject) QueriesMax 
							FROM conc
						) x ON (x.SourceObject = t.SourceTableName AND x.TargetObject = t.TargetTableName AND x.Queries = x.QueriesMax)


	------------------------------------------------------ EXECUTE QUERIES  ----------------------------------------------------------------

	-- Run the SQL single concatenated statement for each table in turn
	SET @Failures = 0
	SET @Message = ''
	SELECT @i = COUNT(*) FROM @Tables
	WHILE @i > 0
		BEGIN
			SELECT @TableName = TargetTableName, @Step = CASE TargetTableName WHEN SourceTableName THEN 'N/A' ELSE SourceTableName END, @FromDate = FromDate, @ToDate = ToDate, @SQL = SQL FROM @Tables WHERE i = @i

			IF @SQL IS NULL 
				EXEC [Control].Logging @BatchDate, @BatchSuffix, @Task, @Step, @TargetServer, @TargetDatabase, @TargetSchema, @TableName, 'Pass', 
					@FromDate = @FromDate, @ToDate = @ToDate, @InsertCount = 0, @UpdateCount = 0, @DeleteCount = 0
			ELSE
				BEGIN 
					EXEC [Control].Logging @BatchDate, @BatchSuffix, @Task, @Step, @TargetServer, @TargetDatabase, @TargetSchema, @TableName, 'Running', 
							@FromDate = @FromDate, @ToDate = @ToDate, @Query = @SQL
					BEGIN TRY	
						BEGIN TRAN LakeTransfer 
						IF @DryRun = 0 	EXECUTE dbo.sp_executesql @SQL, N'@InsertCount int OUTPUT, @UpdateCount int OUTPUT, @DeleteCount int OUTPUT', @InsertCount=@InsertCount OUTPUT, @UpdateCount=@UpdateCount OUTPUT, @DeleteCount=@DeleteCount OUTPUT
						EXEC [Control].Logging @BatchDate, @BatchSuffix, @Task, @Step, @TargetServer, @TargetDatabase, @TargetSchema, @TableName, 'Success', 
								@InsertCount = @InsertCount, @UpdateCount = @UpdateCount, @DeleteCount = @DeleteCount
						COMMIT TRAN LakeTransfer
					END TRY
					BEGIN CATCH
						ROLLBACK TRAN LakeTransfer
						-- Log the failure at the object level
						SET	@Error = ERROR_NUMBER()
						SET	@ErrorMessage = ERROR_MESSAGE()
						EXEC [Control].Logging @BatchDate, @BatchSuffix, @Task, @Step, @TargetServer, @TargetDatabase, @TargetSchema, @TableName, 'Failure', 
								@Error = @Error, @ErrorMessage = @ErrorMessage, @Query = @SQL
						-- Add the failure to the overall batch failure status/message for all tables
						SET @Failures = @Failures + 1
						SET @Message = @Message + CHAR(13) + CHAR(10) + CHAR(9) + @TableName + ' : ' + CHAR(9) + CONVERT(varchar,@FromDate) + ' - ' + CONVERT(varchar,@ToDate) + ' : ' + CHAR(9) + @Task + ' : ' + CHAR(9) + 'Error = ' +  CONVERT(varchar,@Error) + ' - ' + @ErrorMessage

						-- Carry on processing as many tables as possible before raising a summary email alert for the entire batch
					END CATCH
				END
			SET @i = @i - 1 
		END

	------------------------------------------------------ RECORD OUTCOMES  ----------------------------------------------------------------

	-- Check for any table failures during the transfer and alert if any exist
	IF @Failures > 0
		BEGIN
			SET @Alert = @@SERVERNAME + ' - ' + @Task + ' Failed'
			SET @Message = CONVERT(varchar,@Failures) + ' tables failed '+@Task+' processing:' + CHAR(13) + CHAR(10) + @Message
			RAISERROR (@Message, 16, 1)
		END
	ELSE IF @DryRun = 0 AND @Table IS NULL -- Only update CompleteDate if not a dry run and all tables in the receiving schema have been processed (not just one tale by name)
		EXEC Control.Complete @TargetServer, @TargetDatabase, @TargetSchema, @CompleteDate = @NewCompleteDate, @LoadedDate = @NewLoadedDate
END TRY
BEGIN CATCH 
	IF @@TRANCOUNT > 0 ROLLBACK
	SET @Alert = @@SERVERNAME + ' - ' + @Task + ' Failed'
	SET @Message =  OBJECT_NAME(@@PROCID) + '  Failed with error message: ' + ERROR_MESSAGE() + CHAR(13) + CHAR(10)
					+ CHAR(13) + CHAR(10) + CHAR(9) + 'Batch = ' + ISNULL(CONVERT(varchar,@BatchDate)+ISNULL(@BatchSuffix,''),'NULL')
					+ CHAR(13) + CHAR(10) + 'Supplied parameters: '
					+ CHAR(13) + CHAR(10) + CHAR(9) + '@Task = ''' + @Task + ''''
					+ CHAR(13) + CHAR(10) + CHAR(9) + '@SourceServer = ' + ISNULL('''' + @SourceServer + '''','NULL')
					+ CHAR(13) + CHAR(10) + CHAR(9) + '@SourceDatabase = ' + ISNULL('''' + @SourceDatabase + '''','NULL')
					+ CHAR(13) + CHAR(10) + CHAR(9) + '@SourceSchema = ' + ISNULL('''' + @SourceSchema + '''','NULL')
					+ CHAR(13) + CHAR(10) + CHAR(9) + '@SourceProxy = ' + ISNULL('''' + @SourceProxy + '''','NULL')
					+ CHAR(13) + CHAR(10) + CHAR(9) + '@TargetServer = ' + ISNULL('''' + @TargetServer + '''','NULL')
					+ CHAR(13) + CHAR(10) + CHAR(9) + '@TargetDatabase = ' + ISNULL('''' + @TargetDatabase + '''','NULL')
					+ CHAR(13) + CHAR(10) + CHAR(9) + '@TargetSchema = ' + ISNULL('''' + @TargetSchema + '''','NULL')
					+ CHAR(13) + CHAR(10) + CHAR(9) + '@Table = ' + ISNULL('''' + @Table + '''','NULL')
					+ CHAR(13) + CHAR(10) + CHAR(9) + '@DateBound = ' + ISNULL('''' + CONVERT(varchar,@DateBound) + '''','NULL')
					+ CHAR(13) + CHAR(10) + CHAR(9) + '@PreserveBatch = ' + CONVERT(varchar,@PreserveBatch)
					+ CHAR(13) + CHAR(10) + CHAR(9) + '@DryRun = ' + CONVERT(varchar,@DryRun)
	IF @DryRun = 0
		EXEC [msdb].dbo.sp_send_dbmail
			@profile_name = 'Public',
			@recipients = 'some@emailaddress.com',
			@body = @Message,
			@subject = @Alert

	;THROW 
END CATCH
GO


