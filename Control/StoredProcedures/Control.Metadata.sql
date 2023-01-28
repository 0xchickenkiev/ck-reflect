USE [Lake_Control]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [Control].[Metadata]
	(	@Server		sysname = NULL,
		@Database	sysname = NULL,		
		@Schema		sysname = NULL,
		@Proxy		sysname = NULL,					-- Optional Lake proxy server allows the execution of this procedure on another nominated Lake server so that only one result set for all tables is returned where WAN latency for individual queries is an issue
		@Table		sysname = NULL,					-- Optional Table Name to limit the MaxDate process to a specific table (default is all tables in nominated database/schema)
		@DateBound	datetime2(7) = NULL,			-- Optional upper date/time bound to apply an artificial upper limit on the returned Max DMLDate/LSN values. Default value is no bound, A NULL value in this parameter will apply any CompletedDate metadata in the schema as the upper bound 
		@DMLDates	bit = 1,						-- Optional switch to inhibit the significant overhead of retrieving DMLDate/LSN for every table when the procedure is being used only to retrieve DDL metadata
		@Consistent	bit = 0,						-- Optional switch to enforce time consistency across the DML Dates for all tables - 1 = All DMLDate values will be set to last changed date for consistency across all tables, 0 = All DMLDates will be set to highest available value for just the table it belongs to
		@Source		bit = 1,						-- Optional switch to force metadata behaviour to that of a Source location (rather than a Target location) when deriving default DML Dates (only relevant for Non-specific forms where default DML date is current date/time for sources and start of time for targets)
		@DryRun		bit = 0							-- Optional - 1 = execute process as a dry run, with full logging to check gtenerated SQL and From/To dates without actually running any code, 0 = run as normal
	)
AS
	SET NOCOUNT ON

	DECLARE	@Provider			sysname,
			@DBMS				varchar(10),
			@CompatibilityLevel	tinyint,
			@ServerCollation	sysname,
			@DatabaseCollation	sysname,
			@TableName			sysname,
			@PartitionKey		sysname,
			@Form				char(3),
			@LakeProxy			bit
	DECLARE @SCN_MAPPING		sysname
	DECLARE	@LimitDate			datetime2(7),
			@LimitLSN			binary(10),
			@MaxDate			datetime2(7),
			@MaxLSN				binary(10)
	DECLARE	@SQL				nvarchar(MAX)
	DECLARE @i					int

	DECLARE @CDCLatencyBuffer int = 1 -- The delay, in seconds, introduced into CDC capture to cope with LSNs arriving out of timestamp sequence - at time of writing, a maximum of 16 milliseconds out of sequence has been observed, hence waiting for 1 second should be more than sufficient

BEGIN
	CREATE TABLE #Tables (i int IDENTITY(1,1), TableName sysname, PartitionKey sysname NULL, Form char(3))
	CREATE TABLE #Metadata (TableName sysname, DBMS varchar(10), CompatibilityLevel tinyint, Form char(3), OriginTableName sysname NULL, OriginSchemaName sysname NULL, PartitionFunction sysname NULL, PartitionKey sysname NULL, DDLDate datetime2(3) NULL, DMLDate datetime2(7) NULL, RetentionDate datetime2(7) NULL, LSN binary(10), ColumnOrdinal int, ColumnName sysname, ColumnType sysname, ColumnLength int, ColumnScale tinyint, ColumnPrecision tinyint, ColumnCollation sysname NULL, KeyOrdinal int)

BEGIN TRY

	-- Ensure Server/Proxy/Database/Schema name parameters are not supplied in square brackets
	SET @Proxy = REPLACE(REPLACE(@Proxy,'[',''),']','')
	SET @Server = REPLACE(REPLACE(@Server,'[',''),']','')
	SET @Database = REPLACE(REPLACE(@Database,'[',''),']','')
	SET @Schema = REPLACE(REPLACE(@Schema,'[',''),']','')

	IF @Proxy IS NOT NULL AND @Server IS NULL RAISERROR('A Proxy server may only be nominated if an ultimate destination Server is also specified',16,1,@Proxy, @@SERVERNAME)

	SELECT @Proxy = name, @Provider = provider FROM sys.servers WHERE name = ISNULL(@Proxy,@@SERVERNAME)
	IF @Provider IS NULL RAISERROR('Proxy server %s does not exist or is inaccessible from server %s',16,1,@Proxy, @@SERVERNAME)
	IF @Provider NOT LIKE 'SQLNCLI%' RAISERROR('Proxy must be SQL Server, server [%s] provider is %s',16,1,@Proxy, @Provider)

	SET @SQL = 'SELECT	@LakeProxy = ISNULL(MAX(1),0) 
				FROM	'+Control.RemoteQuery('SELECT name FROM sys.databases',@Proxy,DEFAULT)+' x
				WHERE	name = '''+db_name()+''' -- Take existence of a database on Proxy with same name as current Lake database as confirmation it is a Lake Proxy
						AND @Proxy <> @@SERVERNAME' -- If Proxy is set to current server, there is no proxy - only check for lake proxy where a distinct proxy server is nominated
	EXECUTE dbo.sp_executesql @SQL, N'@Proxy sysname, @LakeProxy bit OUTPUT', @Proxy = @Proxy, @LakeProxy=@LakeProxy OUTPUT 

	IF @LakeProxy = 1
		-- The request is not for this server - a proxy for remote execution via a remote Lake server has been nominated
		-- This option is provided for transfers across the WAN were the latency of multiple table and metdata queries to assemble the metadata for remote table(s) would be significantly less eficient than 
		-- calling an instance of this procedure local to the table(s) and the metadata then being returned as a single result set in response to the one procedure call
		BEGIN
			SET TRANSACTION ISOLATION LEVEL READ COMMITTED
			-- Hand over execution to another instance of this module as indicated by the proxy parameter
			SET @SQL = 'INSERT	#Metadata (TableName, DBMS, CompatibilityLevel, Form, OriginTableName, OriginSchemaName, PartitionFunction, PartitionKey, DDLDate, DMLDate, RetentionDate, LSN, ColumnOrdinal, ColumnName, ColumnType, ColumnLength, ColumnScale, ColumnPrecision, ColumnCollation, KeyOrdinal)
						SELECT	TableName, DBMS, CompatibilityLevel, Form, OriginTableName, OriginSchemaName, PartitionFunction, PartitionKey, DDLDate, DMLDate, RetentionDate, LSN, ColumnOrdinal, ColumnName, ColumnType, ColumnLength, ColumnScale, ColumnPrecision, ColumnCollation, KeyOrdinal
						FROM	OPENQUERY(['+@Proxy+'], 
											''['+db_name()+'].['+OBJECT_SCHEMA_NAME(@@PROCID)+'].['+OBJECT_NAME(@@PROCID)+'] -- NOTE Lake software is assumed to always be installed in databases and schemas with the same name as the currently invoked stored procedure
  													@Server = '+ISNULL(''''''+@Server+'''''','NULL')+',
													@Database = '+ISNULL(''''''+@Database+'''''','NULL')+',
													@Schema = '+ISNULL(''''''+@Schema+'''''','NULL')+',
													@Table = '+ISNULL(''''''+@Table+'''''','NULL')+',
													@DateBound = '+ISNULL(''''''+CONVERT(varchar,@DateBound)+'''''','NULL')+',
													@DMLDates = '+CONVERT(varchar(1),@DMLDates)+', 
													@Source = '+CONVERT(varchar(1),@Source)+' 
												WITH RESULT SETS ((TableName sysname, DBMS varchar(10), CompatibilityLevel tinyint, Form char(3), OriginTableName sysname, OriginSchemaName sysname, PartitionFunction sysname NULL, PartitionKey sysname NULL, DDLDate datetime2(3), DMLDate datetime2(7), RetentionDate datetime2(7), LSN binary(10), ColumnOrdinal int, ColumnName sysname, ColumnType sysname, ColumnLength int, ColumnScale tinyint, ColumnPrecision tinyint, ColumnCollation sysname, KeyOrdinal int))'')' 
			EXECUTE dbo.sp_executesql @SQL
		END
	ELSE
		-- The request is for this server, 
		BEGIN
--			SET TRANSACTION ISOLATION LEVEL SNAPSHOT
			---- Fix new high watermark to aim for (note we always back it off by number of seconds defined in @CDCLatencyBuffer from current time to avoid issues seen in the past with most recent LSNs arriving out of sequence to within a few milliseconds of current)
			--SET @SQL = 'SELECT @tran_end_time = tran_end_time FROM '+Control.RemoteQuery('SELECT TOP 1 tran_end_time FROM ['+@SourceDatabase+'].['+@SourceSchema+'].lsn_time_mapping WHERE tran_end_time <= DATEADD(second,-'+CONVERT(varchar,@CDCLatencyBuffer)+',getdate()) ORDER BY start_lsn DESC',@SourceServer,@SourceProxy)+' x'
			--EXECUTE dbo.sp_executesql @SQL, N'@tran_end_time datetime2(3) OUTPUT', @tran_end_time=@ToLoad OUTPUT

			SET @SQL = 'SELECT	@Server = name, @Provider =	provider, @ServerCollation = ISNULL(collation_name,CONVERT(varchar,SERVERPROPERTY(''collation''))) 
						FROM	'+Control.RemoteQuery('SELECT @@SERVERNAME default_name, name, provider, collation_name FROM sys.servers',@Proxy,DEFAULT)+' x
						WHERE	name = ISNULL(@Server,default_name)' -- If a server parameter is not supplied, go for the default server
			EXECUTE dbo.sp_executesql @SQL, N'@Server sysname OUTPUT, @Provider varchar(10) OUTPUT, @ServerCollation sysname OUTPUT', @Server=@Server OUTPUT, @Provider=@Provider OUTPUT, @ServerCollation=@ServerCollation OUTPUT 
			IF @ServerCollation IS NULL RAISERROR('Server [%s] does not exist or is inaccessible from server [%s]',16,1,@Server, @Proxy)

			IF @Provider LIKE 'SQLNCLI%' SET @DBMS = 'SQL Server' -- SQL Server
			ELSE IF @Provider = 'MSDASQL' EXEC Control.RemoteDBMS @Server,@Proxy,@DBMS OUTPUT-- ODBC, but now identify which DBMS type is being accessed (NOTE the logic to speculatively attempt to access DBMS specific metadata tables to identify the DBMS is so toxic that it has to be quarantined in this stored procedure to prevent it poluting this procedure)
			ELSE RAISERROR('Provider %s used by server %s is not currently supported',16,1,@Provider, @Server)

			IF @DBMS IN ('Oracle','MySQL') AND @Database IS NOT NULL RAISERROR('Specifying a database name [%s] is invalid for %s on [%s]',16,1,@Database, @DBMS, @Server)

			-- DBMS specific logic - get additional information and set up the core of the main metadata query bssed on the different metadata conventions of the DBMS
			IF @DBMS = 'SQL Server'
				BEGIN
					-- Keep this first step compatability-level-agnostic.... then.... once compatibility level is ascertained, break subsequent logic by catalogue views or information schema depending upon compatibility level found
					SET @SQL = 'SELECT @Database = name, @DatabaseCollation = collation_name, @CompatibilityLevel = cmptlevel 
								FROM '+Control.RemoteQuery('SELECT db_name() default_name, name,CONVERT (varchar, DATABASEPROPERTYEX(name,''collation'')) collation_name, cmptlevel from master.dbo.sysdatabases',@Server,@Proxy)+' x
								WHERE name = ISNULL(@Database,default_name)' -- If a database parameter is not supplied, go for the default database on the server in current context
					EXECUTE dbo.sp_executesql @SQL, N'@Database sysname OUTPUT, @DatabaseCollation sysname OUTPUT, @CompatibilityLevel sysname OUTPUT', @Database=@Database OUTPUT, @DatabaseCollation=@DatabaseCollation OUTPUT, @CompatibilityLevel=@CompatibilityLevel OUTPUT
					IF @DatabaseCollation IS NULL RAISERROR('Database [%s] does not exist or is inaccessible on server [%s]',16,1,@Database, @Server)
					IF @CompatibilityLevel = 80 -- SQL Server 2000
						BEGIN
							SET @i = 0
							SET @SQL = 'SELECT @i = 1, @Schema = name FROM '+Control.RemoteQuery('SELECT name FROM ['+@Database+'].dbo.sysusers',@Server,@Proxy)+' x WHERE name = ISNULL(@Schema,''dbo'')'
							EXECUTE dbo.sp_executesql @SQL, N'@Schema sysname OUTPUT, @i int OUTPUT', @Schema=@Schema OUTPUT, @i = @i OUTPUT
							IF @i = 0 RAISERROR('Schema [%s] does not exist in database [%s].[%s]',16,1,@Schema,@Server,@Database)
							SET @SQL = 'SELECT	c.TABLE_NAME TableName, CAST(NULL AS CHAR) PartitionFunction, CAST(NULL AS CHAR) PartitionKey, CAST(NULL AS DATETIME) DDLDate, 
										c.ORDINAL_POSITION ColumnId, c.COLUMN_NAME ColumnName, c.DATA_TYPE ColumnType, 
										CASE c.DATA_TYPE WHEN ''text'' THEN -1 WHEN ''longtext'' THEN -1 WHEN ''longblob'' THEN -1 WHEN ''image'' THEN -1 ELSE c.CHARACTER_MAXIMUM_LENGTH END ColumnLength, 
										c.NUMERIC_SCALE ColumnScale, c.NUMERIC_PRECISION ColumnPrecision, c.COLLATION_NAME "ColumnCollation", y.ORDINAL_POSITION KeyId
								FROM	['+@Database+'].INFORMATION_SCHEMA.COLUMNS c
										INNER JOIN	['+@Database+'].INFORMATION_SCHEMA.TABLES o ON o.TABLE_CATALOG = c.TABLE_CATALOG AND o.TABLE_SCHEMA = c.TABLE_SCHEMA AND o.TABLE_NAME = c.TABLE_NAME AND o.TABLE_TYPE = ''BASE TABLE'' 
										LEFT JOIN	
 													(	SELECT	TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, CONSTRAINT_NAME, CONSTRAINT_TYPE
														FROM	(	SELECT	kc.TABLE_CATALOG, kc.TABLE_SCHEMA, kc.TABLE_NAME, kc.COLUMN_NAME, kc.ORDINAL_POSITION, kc.CONSTRAINT_NAME, tc.CONSTRAINT_TYPE -- , COUNT(COLUMN_NAME) OVER (PARTITION BY kc.TABLE_NAME, kc.CONSTRAINT_NAME) AS NUMBER_OF_COLUMNS
																	FROM	['+@Database+'].INFORMATION_SCHEMA.KEY_COLUMN_USAGE kc
																			INNER JOIN ['+@Database+'].INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc ON tc.CONSTRAINT_CATALOG = kc.CONSTRAINT_CATALOG COLLATE DATABASE_DEFAULT AND tc.CONSTRAINT_SCHEMA = kc.CONSTRAINT_SCHEMA COLLATE DATABASE_DEFAULT AND tc.CONSTRAINT_NAME = kc.CONSTRAINT_NAME COLLATE DATABASE_DEFAULT AND tc.TABLE_SCHEMA = kc.TABLE_SCHEMA COLLATE DATABASE_DEFAULT AND tc.TABLE_NAME = kc.TABLE_NAME COLLATE DATABASE_DEFAULT
																	WHERE	tc.CONSTRAINT_TYPE IN (''PRIMARY KEY'')
																) x
														) y ON y.TABLE_CATALOG = c.TABLE_CATALOG COLLATE DATABASE_DEFAULT AND y.TABLE_SCHEMA = c.TABLE_SCHEMA COLLATE DATABASE_DEFAULT AND y.TABLE_NAME = c.TABLE_NAME COLLATE DATABASE_DEFAULT AND y.COLUMN_NAME = c.COLUMN_NAME COLLATE DATABASE_DEFAULT -- AND y.RNK = 1
								WHERE	c.TABLE_SCHEMA = '''+@Schema+'''								
								'
						END
					ELSE			-- All later SQL Server versions
						BEGIN
							SET @i = 0
							SET @SQL = 'SELECT @i = 1, @Schema = name FROM '+Control.RemoteQuery('SELECT name FROM ['+@Database+'].sys.schemas',@Server,@Proxy)+' x WHERE name = ISNULL(@Schema,''dbo'')'
							EXECUTE dbo.sp_executesql @SQL, N'@Schema sysname OUTPUT, @i int OUTPUT', @Schema=@Schema OUTPUT, @i = @i OUTPUT
							IF @i = 0 RAISERROR('Schema [%s] does not exist in database [%s].[%s]',16,1,@Schema,@Server,@Database)
							SET @SQL = 'SELECT	t.name TableName, f.name PartitionFunction, 
												MIN(CASE WHEN c.Name IN(''__$From'',''__$To'') AND ic.partition_ordinal > 0 THEN c.Name ELSE CONVERT(sysname,NULL) END) OVER (PARTITION BY t.name) PartitionKey,
												t.modify_date DDLDate, 
												c.column_id ColumnId, c.name ColumnName, y.name ColumnType, c.max_length ColumnLength, c.scale ColumnScale, c.precision ColumnPrecision, c.collation_name ColumnCollation, 
												CASE
													WHEN MAX(CONVERT(tinyint,k.is_primary_key)) OVER (PARTITION BY t.name) = 1 THEN kc.key_ordinal
													ELSE ic.key_ordinal
												END KeyId
										FROM	['+@Database+'].sys.schemas s WITH (NOLOCK)
												INNER JOIN ['+@Database+'].sys.tables t WITH (NOLOCK) ON (t.schema_id = s.schema_id)
												INNER JOIN ['+@Database+'].sys.columns c WITH (NOLOCK) ON (c.object_id = t.object_id)
												INNER JOIN ['+@Database+'].sys.types y WITH (NOLOCK) ON (y.system_type_id = c.system_type_id and y.system_type_id = y.user_type_id)
												LEFT JOIN ['+@Database+'].sys.indexes i WITH (NOLOCK) ON (i.object_id = t.object_id AND i.type_desc IN (''CLUSTERED'',''CLUSTERED COLUMNSTORE''))
												LEFT JOIN ['+@Database+'].sys.index_columns ic WITH (NOLOCK) ON (ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.column_id = c.column_id)
												LEFT JOIN ['+@Database+'].sys.partition_schemes p WITH (NOLOCK) ON (p.data_space_id = i.data_space_id)
												LEFT JOIN ['+@Database+'].sys.partition_functions f WITH (NOLOCK) ON (f.function_id = p.function_id)
												LEFT JOIN ['+@Database+'].sys.indexes k WITH (NOLOCK) ON (k.object_id = t.object_id AND k.is_primary_key = 1)
												LEFT JOIN ['+@Database+'].sys.index_columns kc WITH (NOLOCK) ON (kc.object_id = k.object_id AND kc.index_id = k.index_id AND kc.column_id = c.column_id)
										WHERE	s.name = '''+@Schema+'''
												AND NOT (s.name = ''dbo'' AND t.is_ms_shipped = 1) -- exclude SQL Server system tables
										'
						END
				END
			ELSE IF @DBMS = 'Oracle'
				BEGIN
					SET @DatabaseCollation = @ServerCollation -- the host of the linked server sets the collation
					SET @CompatibilityLevel = 0 -- only relevant to SQL Server
					SET @SQL = 'SELECT	c.TABLE_NAME "TableName", CAST(NULL AS VARCHAR2(128)) "PartitionFunction", CAST(NULL AS VARCHAR2(128)) "PartitionKey", o.LAST_DDL_TIME "DDLDate", 
										c.COLUMN_ID "ColumnId", c.COLUMN_NAME "ColumnName", c.DATA_TYPE "ColumnType", 
										c.DATA_LENGTH "ColumnLength", c.DATA_SCALE "ColumnScale", c.DATA_PRECISION "ColumnPrecision", CAST(NULL AS VARCHAR2(128)) "ColumnCollation", y.POSITION "KeyId"
								FROM	all_tab_columns c
										INNER JOIN	ALL_OBJECTS o ON o.OWNER = c.OWNER AND o.OBJECT_NAME = c.TABLE_NAME AND o.OBJECT_TYPE = ''TABLE'' 
										LEFT JOIN	(	SELECT	OWNER, TABLE_NAME, COLUMN_NAME, POSITION, 
																DENSE_RANK() OVER (PARTITION BY TABLE_NAME ORDER BY NUMBER_OF_COLUMNS, PRIORITY, CONSTRAINT_NAME) RNK,
																COUNT(*) OVER (PARTITION BY TABLE_NAME, CONSTRAINT_NAME) CNT
														FROM	(	SELECT	cc.OWNER, cc.TABLE_NAME, cc.COLUMN_NAME, cc.POSITION, cc.CONSTRAINT_NAME, CASE CONSTRAINT_TYPE WHEN ''P'' THEN 1 ELSE 2 END PRIORITY, COUNT(*) OVER (PARTITION BY cc.TABLE_NAME, cc.CONSTRAINT_NAME) NUMBER_OF_COLUMNS
																	FROM	ALL_CONS_COLUMNS cc
																			INNER JOIN ALL_CONSTRAINTS c ON c.OWNER = cc.OWNER AND c.TABLE_NAME = cc.TABLE_NAME AND c.CONSTRAINT_NAME = cc.CONSTRAINT_NAME AND c.CONSTRAINT_TYPE IN (''P'', ''U'')
																	UNION ALL
																	SELECT	ic.TABLE_OWNER, ic.TABLE_NAME, ic.COLUMN_NAME, ic.COLUMN_POSITION, ic.INDEX_NAME, 3, COUNT(*) OVER (PARTITION BY ic.TABLE_NAME, ic.INDEX_NAME)
																	FROM	ALL_IND_COLUMNS ic
																			INNER JOIN ALL_INDEXES i ON i.OWNER = ic.INDEX_OWNER AND i.TABLE_NAME = ic.TABLE_NAME AND i.INDEX_NAME = ic.INDEX_NAME AND i.UNIQUENESS = ''UNIQUE''
																) x
														) y ON y.OWNER = c.OWNER AND y.TABLE_NAME = c.TABLE_NAME AND y.COLUMN_NAME = c.COLUMN_NAME AND y.RNK = 1
								WHERE	c.OWNER = '''+@Schema+'''
								'
				END
			ELSE IF @DBMS = 'MySQL'
				BEGIN
					SET @DatabaseCollation = @ServerCollation -- the host of the linked server sets the collation
					SET @CompatibilityLevel = 0 -- only relevant to SQL Server
					SET @SQL = 'SELECT	c.TABLE_NAME `TableName`, CAST(NULL AS CHAR) `PartitionFunction`, CAST(NULL AS CHAR) `PartitionKey`, IFNULL(o.UPDATE_TIME,o.CREATE_TIME) `DDLDate`, 
										c.ORDINAL_POSITION `ColumnId`, c.COLUMN_NAME `ColumnName`, c.DATA_TYPE `ColumnType`, 
										CASE c.DATA_TYPE WHEN ''text'' THEN -1 WHEN ''longtext'' THEN -1 WHEN ''mediumtext'' THEN -1 WHEN ''longblob'' THEN -1 ELSE c.CHARACTER_MAXIMUM_LENGTH END `ColumnLength`, 
										c.NUMERIC_SCALE `ColumnScale`, c.NUMERIC_PRECISION `ColumnPrecision`, c.COLLATION_NAME "ColumnCollation", y.ORDINAL_POSITION `KeyId`
								FROM	INFORMATION_SCHEMA.COLUMNS c
										INNER JOIN	INFORMATION_SCHEMA.TABLES o ON o.TABLE_CATALOG = c.TABLE_CATALOG AND o.TABLE_SCHEMA = c.TABLE_SCHEMA AND o.TABLE_NAME = c.TABLE_NAME AND o.TABLE_TYPE = ''BASE TABLE''  
										LEFT JOIN	
 													(	SELECT	TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, CONSTRAINT_NAME, CONSTRAINT_TYPE
														FROM	(	SELECT	kc.TABLE_CATALOG, kc.TABLE_SCHEMA, kc.TABLE_NAME, kc.COLUMN_NAME, kc.ORDINAL_POSITION, kc.CONSTRAINT_NAME, tc.CONSTRAINT_TYPE -- , COUNT(COLUMN_NAME) OVER (PARTITION BY kc.TABLE_NAME, kc.CONSTRAINT_NAME) AS NUMBER_OF_COLUMNS
																	FROM	INFORMATION_SCHEMA.KEY_COLUMN_USAGE kc
																			INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc ON tc.CONSTRAINT_CATALOG = kc.CONSTRAINT_CATALOG AND tc.CONSTRAINT_SCHEMA = kc.CONSTRAINT_SCHEMA AND tc.CONSTRAINT_NAME = kc.CONSTRAINT_NAME AND tc.TABLE_SCHEMA = kc.TABLE_SCHEMA AND tc.TABLE_NAME = kc.TABLE_NAME
																	WHERE	tc.CONSTRAINT_TYPE IN (''PRIMARY KEY'') = 1
																) x
														) y ON y.TABLE_CATALOG = c.TABLE_CATALOG AND y.TABLE_SCHEMA = c.TABLE_SCHEMA AND y.TABLE_NAME = c.TABLE_NAME AND y.COLUMN_NAME = c.COLUMN_NAME -- AND y.RNK = 1
								WHERE	c.TABLE_SCHEMA = '''+@Schema+'''								
								'
				END
			ELSE
				RAISERROR('Cannot identift type of dbms accessed via ODBC provider for Server %s via Proxy %s',16,1,@Server, @Proxy) 

			-- Everything from here on is common to all DBMS types, the DBMS-type specific logic is all out of the way now
			-- Gather all the column metadata into a temp table for subsequent reuse (would prefer INSERT EXEC into a table variable, but nope, can't do that in SQL Server if you want (and we do) to load the returned data from this stored procedure into a table variable using INSERT EXEC in the calling stored procedure 
			SET @SQL = 'INSERT	#Metadata (TableName, DBMS, CompatibilityLevel, Form, OriginTableName, OriginSchemaName, PartitionFunction, PartitionKey, DDLDate, ColumnOrdinal, ColumnName, ColumnType, ColumnLength, ColumnScale, ColumnPrecision, ColumnCollation, KeyOrdinal)
						SELECT	TableName, '''+@DBMS+''' DBMS, '+CONVERT(varchar(3),@CompatibilityLevel)+' CompatibilityLevel, Form, TableName OriginTableName, '''+@Schema+''' OriginSchemaName, PartitionFunction, PartitionKey, DDLDate AT TIME ZONE ''GMT Standard Time'' AT TIME ZONE ''UTC'', 
								RANK() OVER (PARTITION BY TableName ORDER BY ColumnId) ColumnOrdinal, ColumnName, ColumnType, ColumnLength, ColumnScale, ColumnPrecision, 
								CASE WHEN ColumnType IN (''char'',''nchar'',''varchar'',''nvarchar'',''text'',''ntext'',''mediumtext'') THEN ISNULL(ColumnCollation,'''+@DatabaseCollation+''') ELSE NULL END ColumnColation,
								CASE WHEN KeyId IS NOT NULL THEN RANK() OVER (PARTITION BY TableName ORDER BY ISNULL(KeyId,128)) ELSE NULL END KeyOrdinal
						FROM	(	SELECT	*,
											CASE
												WHEN TableName = ''__$Complete'' THEN ''CTL'' 
												WHEN TableName LIKE ''%\_SCN\_MAPPING\_%'' ESCAPE ''\'' THEN ''XXX'' 
												WHEN TableName = ''lsn_time_mapping'' THEN ''XXX'' 
												WHEN TableName LIKE ''\_\_$%'' ESCAPE ''\'' THEN ''XXX'' 
												WHEN TableName = ''index_columns'' THEN ''XXX'' 
												WHEN TableName = ''ddl_history'' THEN ''XXX'' 
												WHEN TableName = ''change_tables'' THEN ''XXX'' 
												WHEN TableName = ''captured_columns'' THEN ''XXX'' 
												WHEN TableName LIKE ''xdbcdc%'' THEN ''XXX'' 
												WHEN SUM(CASE ColumnName WHEN ''__$start_lsn'' THEN 1 ELSE 0 END) OVER (PARTITION BY TableName) = 1 THEN ''CDC'' 
												WHEN SUM(CASE ColumnName WHEN ''__$From'' THEN 1 WHEN ''__$To'' THEN 1 ELSE 0 END) OVER (PARTITION BY TableName) = 2 AND MAX(KeyId) OVER (PARTITION BY TableName) IS NOT NULL THEN ''LAK''
												WHEN SUM(CASE ColumnName WHEN ''__$From'' THEN 1 ELSE 0 END) OVER (PARTITION BY TableName) = 1 THEN ''PUB''
												WHEN SUM(CASE ColumnName WHEN ''DateFrom'' THEN 1 WHEN ''DateTo'' THEN 1 ELSE 0 END) OVER (PARTITION BY TableName) = 2 AND MAX(KeyId) OVER (PARTITION BY TableName) IS NOT NULL AND '''+@Schema+''' <> ''dbo'' THEN ''OLD'' -- Last condition is a slight cludge to avoid source tables containing DateFrom and DateTo, which are generally in dbo schema where Lake formats are not
												ELSE ''NON''
											END Form
									FROM	'+Control.RemoteQuery(@SQL,@Server,@Proxy)+' x
								) y
						WHERE	Form <> ''XXX''  -- Exclude the metadata tables
								AND (Form <> ''CDC'' OR ColumnName NOT LIKE ''__$%'') -- Exclude the CDC metadata columns
								AND (Form <> ''LAK'' OR ColumnName NOT LIKE ''__$%'') -- Exclude the Lake metadata columns
								AND (Form <> ''PUB'' OR ColumnName NOT LIKE ''__$%'') -- Exclude the Publish metadata columns
								AND (Form <> ''OLD'' OR ColumnName NOT IN (''DateFrom'',''DateTo'')) -- Exclude the Old metadata columns
								'+ CASE WHEN @Table IS NULL THEN '' ELSE 'AND (Form = ''CTL'' OR TableName LIKE '''+ @Table +''')' END -- If @Table parameter supplied, only include that table and control tables
			IF @DryRun = 1 SELECT @SQL
			EXECUTE dbo.sp_executesql @SQL

			-- Now ascertain the DML dates and any additional specific metadata for each table using the appropriate method for the Form of the table identified above

			------------------------------------------------------ CTL (Lake control tables) ----------------------------------------------------------------

			-- Complete
			IF @DMLDates = 1 AND EXISTS (SELECT 1 FROM #Metadata WHERE Form = 'CTL' AND TableName = '__$Complete')
				BEGIN
					-- Origin (source) table names and schemas
					SET @SQL = 'UPDATE	m
								SET		DMLDate = CASE ColumnName WHEN ''CompleteDate'' THEN c.CompleteDate WHEN ''LoadedDate'' THEN c.LoadedDate WHEN ''RetentionDate'' THEN c.RetentionDate ELSE NULL END
								FROM	#Metadata m,
										'+Control.RemoteQuery('SELECT * FROM ['+@Database+'].['+@Schema+'].__$Complete',@Server,@Proxy)+' c
								WHERE	m.Form = ''CTL'' AND m.TableName = ''__$Complete'''
					EXECUTE dbo.sp_executesql @SQL
				END

			------------------------------------------------------ CDC -------------------------------------------------------------------------------------
			--NOTE - the CDC logic has been amended to the new standard stucture (remote query etc) but has not yet been tested due to lack of suitable environment
			
			-- Weed out the CDC tables (may be other non-CDC tables in the same schema that we don't want for this step)
			INSERT	#Tables (TableName)
			SELECT	DISTINCT TableName
			FROM	#Metadata 
			WHERE	Form = 'CDC'
			ORDER BY TableName DESC
			SET @i = @@ROWCOUNT

			IF @i > 0 
				BEGIN
					-- Upate CDC tables with metadata from additional CDC specific metadata tables
					-- Origin (source) table names and schemas - cdc tables have metadata keys so retrieve origin key details from source table
					SET @SQL = 'SELECT	s.name SchemaName, t.name TableName,
										ss.name OriginSchemaName, st.name OriginTableName,
										sc.name ColumnName, ic.index_ordinal KeyOrdinal
								FROM	'+QUOTENAME(@Database)+'.'+QUOTENAME(@Schema)+'.change_tables ct
										INNER JOIN '+QUOTENAME(@Database)+'.sys.tables t ON t.object_id = ct.object_id
										INNER JOIN '+QUOTENAME(@Database)+'.sys.schemas s ON s.schema_id = t.schema_id
										INNER JOIN '+QUOTENAME(@Database)+'.sys.tables st ON st.object_id = ct.source_object_id
										INNER JOIN '+QUOTENAME(@Database)+'.sys.schemas ss ON ss.schema_id = st.schema_id
										INNER JOIN '+QUOTENAME(@Database)+'.sys.columns sc ON sc.object_id = st.object_id
										LEFT JOIN '+QUOTENAME(@Database)+'.'+QUOTENAME(@Schema)+'.index_columns ic ON ic.object_id = ct.object_id AND ic.column_id = sc.column_id'
					SET @SQL = 'UPDATE	m
								SET		OriginTableName = x.OriginTableName,
										OriginSchemaName = x.OriginSchemaName,
										KeyOrdinal = x.KeyOrdinal
								FROM	#Metadata m
										INNER JOIN '+Control.RemoteQuery(@SQL,@Server,@Proxy)+' x ON x.TableName COLLATE DATABASE_DEFAULT = m.TableName AND x.SchemaName COLLATE DATABASE_DEFAULT = '''+@Schema+''' AND x.ColumnName COLLATE DATABASE_DEFAULT = m.ColumnName'
					EXECUTE dbo.sp_executesql @SQL

					-- Check for SCN_MAPPING record required to correct time from Oracle source via Attunity
					SET @SQL = 'SELECT @SCN_MAPPING = SCN_MAPPING FROM '
									+Control.RemoteQuery('SELECT	t.name SCN_MAPPING
															FROM	'+QUOTENAME(@Database)+'.sys.schemas s WITH (NOLOCK)
																	INNER JOIN '+QUOTENAME(@Database)+'.sys.tables t WITH (NOLOCK) ON (t.schema_id = s.schema_id)
															WHERE	s.name = '''+@Schema+'''
																	AND t.name LIKE ''%SCN_MAPPING%''
															',@Server,@Proxy)+' x'
					EXECUTE dbo.sp_executesql @SQL, N'@SCN_MAPPING sysname OUTPUT', @SCN_MAPPING=@SCN_MAPPING OUTPUT

					-- Capture current highest LSN and __$From as a consistent high watermark.
					IF @SCN_MAPPING IS NOT NULL -- source has SCN_MAPPING table to include in time processing
						SET @SQL = 'SELECT MAX(start_lsn) LimitLSN, MAX(__$From) LimitDate 
									FROM	(	SELECT  l.start_lsn, DATEADD(nanosecond, 100*(CONVERT(bigint,CONVERT(binary(6),start_lsn))-MIN_SCN+1), SCN_DT) __$From
												FROM	(	SELECT	CONVERT(binary(6),CONVERT(bigint,MIN_SCN)) + 0x00000000 MinLSN, CONVERT(binary(6),CONVERT(bigint,MAX_SCN)) + 0xFFFFFFFF MaxLSN, CONVERT(datetime2(7),SCN_DT) SCN_DT, MIN_SCN
															FROM	'+QUOTENAME(@Database)+'.'+QUOTENAME(@Schema)+'.'+QUOTENAME(@SCN_MAPPING)+' 
															WHERE	__$operation IN (2,4) 
																	AND CONVERT(datetime2(7),SCN_DT) <= CONVERT(datetime2(7),'''+ISNULL(CONVERT(varchar,@DateBound,121),'9999-12-31')+''')
														) s
														INNER JOIN '+QUOTENAME(@Database)+'.'+QUOTENAME(@Schema)+'.lsn_time_mapping l ON l.start_lsn BETWEEN s.MinLSN and s.MaxLSN
											) x
									WHERE	__$From <= CONVERT(datetime2(7),'''+ISNULL(CONVERT(varchar,@DateBound,121),'9999-12-31')+''')'
					ELSE -- source has no SCN_MAPPING table , just focus on lsn_time_mapping
						SET @SQL = 'SELECT	MAX(start_lsn) LimitLSN, MAX(__$From) LimitDate
									FROM	(	SELECT	start_lsn, DATEADD(nanosecond, 100*ROW_NUMBER() OVER (ORDER BY start_lsn), CONVERT(datetime2(7),CONVERT(datetime2(3),x.tran_end_time))) __$From
												FROM	(	SELECT	MAX(tran_end_time) tran_end_time 
															FROM	'+QUOTENAME(@Database)+'.'+QUOTENAME(@Schema)+'.lsn_time_mapping
															WHERE	tran_end_time <= CONVERT(datetime,'''+ISNULL(CONVERT(varchar(23),@DateBound,121),'9999-12-31')+''')
																	AND tran_end_time <= DATEADD(second,-'+CONVERT(varchar,@CDCLatencyBuffer)+',getdate())
														) x
														INNER JOIN '+QUOTENAME(@Database)+'.'+QUOTENAME(@Schema)+'.lsn_time_mapping m1 ON m1.tran_end_time = x.tran_end_time
											) x
									WHERE	__$From <= CONVERT(datetime2(7),'''+ISNULL(CONVERT(varchar,@DateBound,121),'9999-12-31')+''')'
					SET @SQL = 'SELECT @LimitLSN = LimitLSN, @LimitDate = LimitDate AT TIME ZONE ''GMT Standard Time'' AT TIME ZONE ''UTC'' FROM '+Control.RemoteQuery(@SQL,@Server,@Proxy)+' x OPTION(RECOMPILE)'
					IF @DryRun = 1 SELECT @SQL
					EXECUTE dbo.sp_executesql @SQL, N'@LimitLSN binary(10) OUTPUT, @LimitDate datetime2(7) OUTPUT', @LimitLSN=@LimitLSN OUTPUT, @LimitDate=@LimitDate OUTPUT

					-- Synthesise __$Complete record in output metadata, based upon retieved limit values above
					INSERT #Metadata (TableName, DBMS, CompatibilityLevel, Form, OriginTableName, OriginSchemaName, DDLDate, DMLDate, RetentionDate, LSN, ColumnOrdinal, ColumnName, ColumnType, ColumnLength, ColumnScale, ColumnPrecision)
						VALUES ('__$Complete', @DBMS, @CompatibilityLevel, 'CTL', '__$Complete', @Schema, @LimitDate, @LimitDate, NULL, @LimitLSN, 1, 'CompleteDate', 'datetime2', 8, 7, 27)
					INSERT #Metadata (TableName, DBMS, CompatibilityLevel, Form, OriginTableName, OriginSchemaName, DDLDate, DMLDate, RetentionDate, LSN, ColumnOrdinal, ColumnName, ColumnType, ColumnLength, ColumnScale, ColumnPrecision)
						VALUES ('__$Complete', @DBMS, @CompatibilityLevel, 'CTL', '__$Complete', @Schema, @LimitDate, @LimitDate, NULL, @LimitLSN, 2, 'LoadedDate', 'datetime2', 8, 7, 27)
				END

			-- Iterate around all CDC tables identifying specific high watermark for each table up to the general high watermark retrieved above
			WHILE @DMLDates = 1 AND @i > 0
				BEGIN
					SELECT @TableName = TableName FROM #Tables WHERE i=@i
					-- Highest Date and LSN less than or equal to LimitLSN set as high watermark at start of process
					SET @MaxDate = NULL SET @MaxLSN = NULL
					SET @SQL = 'SELECT	max(__$start_lsn) MaxLSN 
								FROM	'+QUOTENAME(@Database)+'.'+QUOTENAME(@Schema)+'.'+QUOTENAME(@TableName)+'
								WHERE	__$start_lsn <= '+CONVERT(varchar,@LimitLSN,1)
					SET @SQL = 'SELECT @MaxLSN = MaxLSN FROM '+Control.RemoteQuery(@SQL,@Server,@Proxy)+' x OPTION(RECOMPILE)'
					IF @DryRun = 1 SELECT @SQL
					EXECUTE dbo.sp_executesql @SQL, N'@MaxLSN binary(10) OUTPUT', @MaxLSN=@MaxLSN OUTPUT
					IF @SCN_MAPPING IS NOT NULL -- source has SCN_MAPPING table to include in time processing
						SET @SQL = 'SELECT	DATEADD(nanosecond, 100*('+CONVERT(varchar,CONVERT(bigint,CONVERT(binary(6),@MaxLSN)))+'-m.MIN_SCN+1), CONVERT(datetime2(7),m.SCN_DT)) MaxDate
									FROM	'+QUOTENAME(@Database)+'.'+QUOTENAME(@Schema)+'.'+QUOTENAME(@SCN_MAPPING)+' m
									WHERE	__$operation IN (2,4) AND '+CONVERT(varchar,CONVERT(bigint,CONVERT(binary(6),@MaxLSN)))+' BETWEEN MIN_SCN AND MAX_SCN'
					ELSE
						SET @SQL = 'SELECT	DATEADD(nanosecond,100*COUNT(*),CONVERT(datetime2(7),CONVERT(datetime2(3),m.tran_end_time))) MaxDate -- to deal with rounding issues on tran_end_time datetime type, first force it into datetime2(3) before converting it to datetime2(7) - ensures last 4 digits are actually all zeroes, other wite a value of .667 in datetime becomes .6666667 when converted to datetime2(7) rather than the required .6670000
									FROM	'+QUOTENAME(@Database)+'.'+QUOTENAME(@Schema)+'.lsn_time_mapping m
											INNER JOIN '+QUOTENAME(@Database)+'.'+QUOTENAME(@Schema)+'.lsn_time_mapping m1 ON m1.tran_end_time = m.tran_end_time
									WHERE	m.start_lsn = '+CONVERT(varchar,@MaxLSN,1)+'
									GROUP BY m.tran_end_time'
					SET @SQL = 'SELECT @MaxDate = MaxDate AT TIME ZONE ''GMT Standard Time'' AT TIME ZONE ''UTC'' FROM '+Control.RemoteQuery(@SQL,@Server,@Proxy)+' x OPTION(RECOMPILE)'
					IF @DryRun = 1 SELECT @SQL
					EXECUTE dbo.sp_executesql @SQL, N'@MaxDate datetime2(7) OUTPUT', @MaxDate=@MaxDate OUTPUT
					UPDATE #Metadata SET DMLDate = @MaxDate, LSN = @MaxLSN WHERE TableName = @TableName

					SET @i=@i-1
				END

			TRUNCATE TABLE #Tables

			------------------------------------------------------ LAK & PUB-------------------------------------------------------------------------------------

			-- Weed out the Lake tables (may be other non-Lake tables in the same schema that we don't want for this step)
			INSERT	#Tables (TableName, PartitionKey, Form)
			SELECT	DISTINCT TableName, PartitionKey, Form
			FROM	#Metadata 
			WHERE	Form IN ('LAK','PUB')
			ORDER BY TableName DESC
			SET @i = @@ROWCOUNT

			-- If DMLDates flag is set, retrieve DML dates for all Lake tables
			IF @DMLDates = 1 AND @i > 0 
				BEGIN
					SET @LimitDate = CASE WHEN @DateBound < CONVERT(datetime2(7),'9999-12-31') THEN @DateBound ELSE CONVERT(datetime2(7),'9999-12-31') END
					IF @Consistent = 1 SET @LimitDate = (SELECT CASE WHEN @LimitDate < DMLDate THEN @LimitDate ELSE DMLDate END FROM #Metadata WHERE Form = 'CTL' AND TableName = '__$Complete' AND ColumnName = 'CompleteDate')

					WHILE @i > 0
						BEGIN
							SELECT @TableName = TableName, @PartitionKey = PartitionKey, @Form = Form FROM #Tables WHERE i=@i
							SET @SQL = 'SELECT	TOP 1 ISNULL(__$From,__$From) __$From -- Note the completely pointless ISNULL, but without this the statement will retrieve the entire index and sort it to find the TOP rather than using TOP actions to read the first row from the availaable index - presumably keeping its options open to run in parallel for no good reason.... 
										FROM	'+QUOTENAME(@Database)+'.'+QUOTENAME(@Schema)+'.'+QUOTENAME(@TableName)+'
										WHERE	__$From <= CONVERT(datetime2(7),'''+CONVERT(varchar,@LimitDate)+''')
												'+CASE 
													WHEN @PartitionKey = '__$From' OR @Form = 'PUB' THEN '' 
													ELSE 'AND __$To = CONVERT(DATETIME2(7),''9999-12-31'')' 
												END+'
										ORDER BY __$From DESC'
							SET @SQL = 'SELECT @MaxDate = __$From FROM '+Control.RemoteQuery(@SQL,@Server,@Proxy)+' x'
							IF @DryRun = 1 SELECT @SQL
							SET @MaxDate = NULL
							EXECUTE dbo.sp_executesql @SQL, N'@MaxDate datetime2(7) OUTPUT', @MaxDate=@MaxDate OUTPUT
							UPDATE #Metadata SET DMLDate = @MaxDate WHERE TableName = @TableName
							SET @i=@i-1
						END
				END

			TRUNCATE TABLE #Tables

			------------------------------------------------------ OLD (previous format LAK) -------------------------------------------------------------------------------------

			-- Weed out the Old Lake tables (may be other non-Lake tables in the same schema that we don't want for this step)
			INSERT	#Tables (TableName, PartitionKey)
			SELECT	DISTINCT TableName, PartitionKey
			FROM	#Metadata 
			WHERE	Form = 'OLD'
			ORDER BY TableName DESC
			SET @i = @@ROWCOUNT

			-- If DMLDates flag is set, retrieve DML dates for all Lake tables
			IF @DMLDates = 1 AND @i > 0 
				BEGIN
					SET @LimitDate = CASE WHEN @DateBound < CONVERT(datetime2(3),'9999-12-30') THEN @DateBound ELSE CONVERT(datetime2(3),'9999-12-30') END
					WHILE @i > 0
						BEGIN
							SELECT @TableName = TableName, @PartitionKey = PartitionKey FROM #Tables WHERE i=@i
							SET @MaxDate = NULL
							-- Look for Highest DateFrom in current records (to date 31/12/9999)
							SET @SQL = 'SELECT	TOP 1 ISNULL(DateFrom,DateFrom) MaxDate -- Note the completely pointless ISNULL, but without this the statement will retrieve the entire index and sort it to find the TOP rather than using TOP actions to read the first row from the availaable index - presumably keeping its options open to run in parallel for no good reason.... 
										FROM	'+QUOTENAME(@Database)+'.'+QUOTENAME(@Schema)+'.'+QUOTENAME(@TableName)+'
										WHERE	DateFrom <= CONVERT(datetime2(3),'''+CONVERT(varchar,@LimitDate)+''')'+CASE @PartitionKey WHEN 'DateFrom' THEN '' ELSE '
												AND DateTo = CONVERT(DATETIME2(3),''9999-12-31'')' END+'
										ORDER BY DateFrom DESC'
							SET @SQL = 'SELECT @MaxDate = MaxDate AT TIME ZONE ''GMT Standard Time'' AT TIME ZONE ''UTC'' FROM '+Control.RemoteQuery(@SQL,@Server,@Proxy)+' x'
							EXECUTE dbo.sp_executesql @SQL, N'@MaxDate datetime2(3) OUTPUT', @MaxDate=@MaxDate OUTPUT
							UPDATE #Metadata SET DMLDate = @MaxDate WHERE TableName = @TableName
							SET @i=@i-1
						END
				END

			TRUNCATE TABLE #Tables

			------------------------------------------------------ Non-specific Form -------------------------------------------------------------------------------------

			-- Weed out the Non-specific tables (may be other types of tables in the same schema that we don't want for this step)
			INSERT	#Tables (TableName)
			SELECT	DISTINCT TableName
			FROM	#Metadata 
			WHERE	Form = 'NON'
			ORDER BY TableName DESC
			SET @i = @@ROWCOUNT

			IF @i > 0
				BEGIN
					-- Non-specific form tables (NON) can either be the source or target for Lake processing - the source when performing full copy comparisons to obtain latest delta for LAK format table or the target when
					-- maintaining a copy database without any additional metadata fed from a LAK format table. The latter will have a standard __$Complete table in the target schema to provide the currency of the target
					-- data post update processing from LAK, the former will not. So, if no __$Complete details are present in the schema, we must synthesise some on the basis the tables in the schema are source tables
					-- current as of now.
					IF NOT EXISTS (SELECT 1 FROM #Metadata WHERE Form = 'CTL' AND TableName = '__$Complete' AND ColumnName = 'CompleteDate')
						INSERT #Metadata (TableName, DBMS, CompatibilityLevel, Form, OriginTableName, OriginSchemaName, DDLDate, DMLDate, RetentionDate, LSN, ColumnOrdinal, ColumnName, ColumnType, ColumnLength, ColumnScale, ColumnPrecision)
							VALUES ('__$Complete', @DBMS, @CompatibilityLevel, 'CTL', '__$Complete', @Schema, SYSUTCDATETIME(), SYSUTCDATETIME(), NULL, NULL, 1, 'CompleteDate', 'datetime2', 8, 7, 27)
					IF NOT EXISTS (SELECT 1 FROM #Metadata WHERE Form = 'CTL' AND TableName = '__$Complete' AND ColumnName = 'LoadedDate')
						INSERT #Metadata (TableName, DBMS, CompatibilityLevel, Form, OriginTableName, OriginSchemaName, DDLDate, DMLDate, RetentionDate, LSN, ColumnOrdinal, ColumnName, ColumnType, ColumnLength, ColumnScale, ColumnPrecision)
							VALUES ('__$Complete', @DBMS, @CompatibilityLevel, 'CTL', '__$Complete', @Schema, SYSUTCDATETIME(), SYSUTCDATETIME(), NULL, NULL, 2, 'LoadedDate', 'datetime2', 8, 7, 27)

					-- If a @DateBound parameter is supplied then that will be used as the DMLDate for all tables, otherwise use the CompleteDate for the schema 
					SELECT @LimitDate = CASE WHEN @DateBound < DMLDate THEN @DateBound ELSE DMLDate END FROM #Metadata WHERE Form = 'CTL' AND TableName = '__$Complete' AND ColumnName = 'CompleteDate'
				END

			-- If DMLDates flag is set, set DML dates for all Lake tables
			IF @DMLDates = 1 AND @i > 0 
				BEGIN
					-- Update DMLDates to LimitDate except where DDLDate is higher then LimitDate (can only occur in target table) indicating Table has been created or changed since last refresh in which case set DMLDate to start of time to force full refresh
					UPDATE m SET DMLDate = CASE WHEN DDLDate > @LimitDate AND m.PartitionKey = '__$To' THEN CONVERT(datetime2(7),'1900-01-01') ELSE @LimitDate END FROM #Metadata m INNER JOIN #Tables t ON t.TableName = m.TableName
				END

			TRUNCATE TABLE #Tables

		END

	------------------------------------------------------ Retention Dates -------------------------------------------------------------------------------------

	UPDATE #Metadata SET RetentionDate = (SELECT ISNULL(MAX(DMLDate),CONVERT(datetime2(7),'1900-01-01')) FROM #Metadata WHERE Form = 'CTL' AND TableName = '__$Complete' AND ColumnName = 'RetentionDate')

	------------------------------------------------------ Return the Results -------------------------------------------------------------------------------------

	SELECT TableName, DBMS, CompatibilityLevel, Form, OriginTableName, OriginSchemaName, PartitionFunction, PartitionKey, DDLDate, DMLDate, RetentionDate, LSN, ColumnOrdinal, ColumnName, ColumnType, ColumnLength, ColumnScale, ColumnPrecision, ColumnCollation, KeyOrdinal FROM #Metadata ORDER BY TableName DESC, ColumnOrdinal DESC

END TRY
BEGIN CATCH
	THROW
END CATCH
END
GO


