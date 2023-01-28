USE [Lake_Control]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [Control].[Tables]
(	
	@SourceServer		varchar(128) = @@SERVERNAME,	-- Optional - Specifies a linked server as the location for the source database, default is local server
	@SourceDatabase		varchar(128) = NULL,			-- Optional - Database as the source for table definition, default is current/default database
	@SourceSchema		varchar(128) = NULL,			-- Optional - Schema as the source for table definition, default is dbo schema
	@SourceProxy		varchar(128) = @@SERVERNAME,	-- Optional - Specifies a linked server as the Lake proxy for the source database to localise intensive metadata gethering activities
	@TargetServer		varchar(128) = @@SERVERNAME,	-- Optional - Specifies a linked server as the location for the target database, default is local server
	@TargetDatabase		varchar(128) = NULL,			-- Optional - Database as the target for table definition, default is current/default database
	@TargetSchema		varchar(128) = NULL,			-- Optional - Schema as the target for table definition, default is dbo schema
	@Form				char(3) = 'LAK',				-- Optional - Specifies the form of the target table(s) and consequently the metadata and indexing strategy.
	@Table				varchar(128) = NULL,			-- Optional - Focusses the data transfer on a specific table. The default is all tables present in both Source and Target schemas which conform to the types required by the task
	@Scheme				sysname = NULL,					-- Optional - Specifies a new/changed partition scheme to be used for index creation. If not specified, the current scheme (if one exists) is maintained, otherwise no partitioning
	@PartitionKey		varchar(128) = NULL,			-- Optional - Specifies a partition key column to be used for index target. If not specified, defaults to whatever is currently defined for target table or if new table, no partitioning 
--	@DateBound			datetime2(3) = NULL,			-- Optional - Applies an upper date bound to the source data to run upto but not beyond a defined point in time. Default of NULL runs up to current defined CompleteDate in source
	@AddMissingTables	bit = 1,						-- Optional - (1=Yes/0=No) Add any tables which are found in source schema but not already in target schema
	@PreserveTargetKey	bit = 1,						-- Optional - (1=Yes/0=No) Where a viable primary key is already defined on an existing target table, do not replace it if a different key is defined on the source table - allows us to select a more optimal unique key which isn't the primary key for the target 
	@DryRun				bit = 0							-- Optional - 1 = execute process as a dry run, with full logging to check gtenerated SQL and From/To dates without actually running any code, 0 = run as normal
)
AS

	DECLARE	@SourceMetadata TABLE (TableName sysname, DBMS varchar(10), CompatibilityLevel tinyint, Form char(3), OriginTableName sysname NULL, OriginSchemaName sysname NULL, PartitionFunction sysname NULL, PartitionKey sysname NULL, DDLDate datetime2(3), DMLDate datetime2(7), RetentionDate datetime2(7), LSN binary(10), ColumnOrdinal int, ColumnName sysname, ColumnType sysname, ColumnLength int, ColumnScale tinyint, ColumnPrecision tinyint, ColumnCollation sysname NULL, KeyOrdinal int)
	DECLARE	@TargetMetadata TABLE (TableName sysname, DBMS varchar(10), CompatibilityLevel tinyint, Form char(3), OriginTableName sysname NULL, OriginSchemaName sysname NULL, PartitionFunction sysname NULL, PartitionKey sysname NULL, DDLDate datetime2(3), DMLDate datetime2(7), RetentionDate datetime2(7), LSN binary(10), ColumnOrdinal int, ColumnName sysname, ColumnType sysname, ColumnLength int, ColumnScale tinyint, ColumnPrecision tinyint, ColumnCollation sysname NULL, KeyOrdinal int)
	DECLARE	@DifferenceMetadata TABLE (TableName sysname, ColumnOrdinal int, ColumnName sysname, ColumnType sysname, ColumnLength int, ColumnScale tinyint, ColumnPrecision tinyint, Column_Status char(1), TableStatus char(1))

	DECLARE	@SourceDBMS					varchar(10),
			@SourceCompatibilityLevel	tinyint,
			@TargetDBMS					varchar(10),
			@TargetCompatibilityLevel	tinyint

	DECLARE	@PrimaryKey		Control.NameList

	DECLARE	@TableName		sysname,
			@TableStatus	char(1)

	DECLARE	@i				int,
			@SQL			nvarchar(MAX),
			@Error			int,
			@ErrorMessage	varchar(MAX),
			@Message		varchar(MAX),
			@Alert			varchar(MAX),
			@Failures		int

BEGIN

	SET NOCOUNT ON 

	SET @Scheme = ISNULL(@Scheme,@TargetSchema)
	
	BEGIN TRY
	-- Source and Target Table/Column Metadata
	INSERT @SourceMetadata (TableName, DBMS, CompatibilityLevel, Form, OriginTableName, OriginSchemaName, PartitionFunction, PartitionKey, DDLDate, DMLDate, RetentionDate, LSN, ColumnOrdinal, ColumnName, ColumnType, ColumnLength, ColumnScale, ColumnPrecision, ColumnCollation, KeyOrdinal) 
		EXEC Control.Metadata @Server = @SourceServer, @Database = @SourceDatabase, @Schema = @SourceSchema, @Proxy = @SourceProxy, @Table = @Table, @DMLDates = 0
	INSERT @TargetMetadata (TableName, DBMS, CompatibilityLevel, Form, OriginTableName, OriginSchemaName, PartitionFunction, PartitionKey, DDLDate, DMLDate, RetentionDate, LSN, ColumnOrdinal, ColumnName, ColumnType, ColumnLength, ColumnScale, ColumnPrecision, ColumnCollation, KeyOrdinal) 
		EXEC Control.Metadata @Server = @TargetServer, @Database = @TargetDatabase, @Schema = @TargetSchema, @Proxy = DEFAULT, @Table = @Table, @DMLDates = 0

		SELECT TOP 1 @SourceDBMS = DBMS, @SourceCompatibilityLevel = CompatibilityLevel FROM @SourceMetadata -- Just get DBMS from any row in metadata as all rows from same server should have same DBMS
		SELECT TOP 1 @TargetDBMS = DBMS, @TargetCompatibilityLevel = CompatibilityLevel FROM @TargetMetadata

		-- Construct a list of all differences between Source and Target 
		INSERT	@DifferenceMetadata
		SELECT	*
		FROM	(	SELECT	TableName, ColumnOrdinal, ColumnName, ColumnType, ColumnLength, ColumnScale, ColumnPrecision, ColumnStatus, 
							CASE 
								WHEN SUM(CASE WHEN ColumnStatus <> '=' THEN 1 ELSE 0 END) OVER (PARTITION BY TableName) = 0 THEN '='
								WHEN SUM(CASE WHEN ColumnStatus <> 'A' THEN 1 ELSE 0 END) OVER (PARTITION BY TableName) = 0 THEN 'A'
								ELSE 'M'
							END TableStatus
					FROM	(	SELECT	s.OriginTableName TableName, s.ColumnOrdinal, s.ColumnName, s.ColumnType, s.ColumnLength, s.ColumnScale, s.ColumnPrecision,
										CASE 
											WHEN t.ColumnName IS NULL THEN 'A' 
											WHEN s.ColumnType <> t.ColumnType THEN 'M' 
											WHEN s.ColumnLength <> t.ColumnLength THEN 'M' 
											WHEN s.ColumnScale <> t.ColumnScale THEN 'M' 
											WHEN s.ColumnPrecision <> t.ColumnPrecision THEN 'M' 
											ELSE '=' 
										END ColumnStatus
								FROM	@SourceMetadata s
										LEFT JOIN @TargetMetadata t ON t.OriginTableName = s.OriginTableName AND t.ColumnName = s.ColumnName
								WHERE	s.Form <> 'CTL'
							) x
				) x
		WHERE	TableStatus <> 'A' OR @AddMissingTables = 1 OR @Table IS NOT NULL
		IF @DryRun = 1 SELECT * FROM @DifferenceMetadata ORDER BY 1,2

		SET @Failures = 0
		SET @Message = ''

		SELECT TOP 1 @TableName = TableName, @TableStatus = TableStatus FROM @DifferenceMetadata ORDER BY TableName
		WHILE @@ROWCOUNT = 1
			BEGIN
				BEGIN TRY

					------------------------------------------------------ CONSTRUCT DDL FOR TABLE  ----------------------------------------------------------------
					IF @TableStatus = 'A'
						BEGIN
							SET @SQL = ''
							SELECT	@SQL += N',[' + name + '] ' + REPLACE(REPLACE(REPLACE(REPLACE(system_type_name,'timestamp','varbinary(8)'),'image','varbinary(max)'),'text','varchar(max)'),'ntext','nvarchar(max)')
							FROM	sys.dm_exec_describe_first_result_set(N'SELECT * FROM '+Control.Path(@TableName,@SourceSchema,@SourceDatabase,@SourceServer,@SourceProxy,@SourceDBMS,@SourceCompatibilityLevel,DEFAULT,DEFAULT)+' x', NULL, 1)
							WHERE	name NOT IN ('__$From','__$To','__$Load','__$Action','__$Source','DateFrom','DateTo')
							SET @SQL = 'CREATE TABLE '+Control.Path(@TableName,@TargetSchema,@TargetDatabase,@TargetServer,DEFAULT,@TargetDBMS,@TargetCompatibilityLevel,DEFAULT,DEFAULT)+'
											(__$From datetime2(7) NOT NULL, '+CASE @Form WHEN 'LAK' THEN '__$To datetime2(7) NOT NULL, ' ELSE '' END+'__$Load datetime2(7) NOT NULL, __$Action char(1) NOT NULL, __$Source Smallint NOT NULL,'+STUFF(@SQL, 1, 1, N'')+')'
							IF @DryRun = 0 	EXECUTE dbo.sp_executesql @SQL ELSE SELECT @SQL
						END

					------------------------------------------------------ MANAGE INDEXES  ----------------------------------------------------------------
					DELETE @PrimaryKey
					IF @PreserveTargetKey = 1
						BEGIN -- Preserve any viable key already defined on the target table and only defer to the source table if no key exists on the target
							INSERT @PrimaryKey SELECT ColumnName FROM @TargetMetadata WHERE KeyOrdinal IS NOT NULL AND TableName = @TableName
							IF @@ROWCOUNT = 0 INSERT @PrimaryKey SELECT ColumnName FROM @SourceMetadata WHERE KeyOrdinal IS NOT NULL AND TableName = @TableName -- if there isn't a viable primary key defined on the source table, then preserve any key which may have already been manually defined on the target
						END
					ELSE
						BEGIN -- Source key is king, if it is there, use it and only defer to the target if nothing defined on source
							INSERT @PrimaryKey SELECT ColumnName FROM @SourceMetadata WHERE KeyOrdinal IS NOT NULL AND TableName = @TableName
							IF @@ROWCOUNT = 0 INSERT @PrimaryKey SELECT ColumnName FROM @TargetMetadata WHERE KeyOrdinal IS NOT NULL AND TableName = @TableName -- if there isn't a viable primary key defined on the source table, then preserve any key which may have already been manually defined on the target
						END
					IF NOT EXISTS(SELECT 1 FROM @PrimaryKey)
						BEGIN -- No viable primary key for current table. Add the failure to the overall batch failure status/message for all tables
							SET @Failures = @Failures + 1
							SET @Message = @Message + CHAR(13) + CHAR(10) + CHAR(9) + @TableName + ' : Warning = No viable primary key found on source or target table - target table currently has dummy clustered index which must be updated with a valid primary key before data can be loaded into it'
						END
					EXECUTE Control.Indexing @TargetServer, @TargetDatabase, @TargetSchema, @TableName, @Scheme = @Scheme, @PartitionKey = @PartitionKey, @PrimaryKey = @PrimaryKey, @Form=@Form, @DryRun = @DryRun


					------------------------------------------------------ MANAGE PARTITIONS  ----------------------------------------------------------------
					EXECUTE Control.Partitioning @Scheme, @Server = @TargetServer, @Database = @TargetDatabase, @Schema = @TargetSchema, @Table = @TableName, @DryRun = @DryRun

				END TRY
				BEGIN CATCH
					SET	@Error = ERROR_NUMBER()
					SET	@ErrorMessage = ERROR_MESSAGE()
					-- Add the failure to the overall failure status/message for all tables
					SET @Failures = @Failures + 1
					SET @Message = @Message + CHAR(13) + CHAR(10) + CHAR(9) + @TableName + ' : Error = ' +  CONVERT(varchar,@Error) + ' - ' + @ErrorMessage
				END CATCH

				SELECT TOP 1 @TableName = TableName, @TableStatus = TableStatus FROM @DifferenceMetadata WHERE TableName > @TableName ORDER BY TableName
			END

		IF @Failures > 0
			BEGIN
				SET @Message = CONVERT(varchar,@Failures) + ' tables failed managment processing:' + CHAR(13) + CHAR(10) + @Message
				RAISERROR (@Message, 16, 1)
			END

	END TRY
	BEGIN CATCH 
		IF @@TRANCOUNT > 0 ROLLBACK
		;THROW 
	END CATCH
END
GO


