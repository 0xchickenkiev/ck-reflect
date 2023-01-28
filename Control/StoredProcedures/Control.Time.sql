USE [Lake_Control]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [Control].[Time]
(	@SourceServer	sysname = NULL,			-- Optional - Linked server as the source for time mapping, default is local server
	@SourceDatabase	sysname = NULL,			-- Optional - Database as the source for time mapping, default is current database
	@SourceSchema	sysname = NULL,			-- Optional - Schema as the source for time mapping, default is dbo schema
	@SourceProxy	sysname = NULL,			-- Optional - Linked server as the Lake proxy for accessing the source for time mapping, default is no proxy
	@SourceForm		char(3),				-- Mandatory - The recognised form of the source for time mapping (e.g. LAK = Lake, PUB = Publish, CDC = CDC, NON = Non-specific)
	@TargetServer	sysname = NULL,			-- Optional - Linked server as the target for time mapping, default is local server
	@TargetDatabase	sysname = NULL,			-- Optional - Database as the target for time mapping, default is current database
	@TargetSchema	sysname = NULL,			-- Optional - Schema as the target for time mapping, default is dbo schema
	@TargetForm		char(3),				-- Mandatory - The recognised form of the target for time mapping (e.g. LAK = Lake, PUB = Publish, CDC = CDC, NON = Non-specific)
	@FromDate		datetime2(7) = NULL,	-- Optional - Upper date/time bound to apply an artificial upper limit on the time values and consequent returned Last values. Default value is no bound.
	@ToDate			datetime2(7)= NULL,		-- Optional - Upper date/time bound to apply an artificial upper limit on the time values and consequent returned Last values. Default value is no bound.
	@LastFrom datetime2(7) = NULL OUTPUT,	-- Optional	- Returns the __$From high watermark at the end of the run
	@LastLoad datetime2(7) = NULL OUTPUT,	-- Optional	- Returns the __$Load high watermark at the end of the run
	@DryRun			bit = 0					-- Optional - 1 = execute process as a dry run, with full logging to check gtenerated SQL and From/To dates without actually running any code, 0 = run as normal
)
AS
	SET NOCOUNT ON
	SET TRANSACTION ISOLATION LEVEL READ COMMITTED

	DECLARE @FromLSN binary(10),
			@ToLSN binary(10)

	DECLARE	@MinFrom datetime2(7)

	DECLARE @SCN_MAPPING sysname

	DECLARE @InsertCount int,
			@UpdateCount int

	DECLARE	@SQL nvarchar(MAX)
	DECLARE @i bit

	CREATE TABLE #Source (__$Load datetime2(7) NOT NULL, __$From datetime2(7) NOT NULL, LSN binary(10) NOT NULL, tran_begin_time datetime NOT NULL, tran_end_time datetime NOT NULL, tran_id varbinary(10) NOT NULL, tran_begin_lsn binary(10) NOT NULL,
							SCN bigint NULL, SCN_DT nvarchar(29) NULL, MIN_SCN bigint NULL, MAX_SCN bigint NULL, DT nvarchar(29) NULL, DT_LSN binary(10) NULL)
	CREATE TABLE #Target (__$Load datetime2(7) NOT NULL, __$From datetime2(7) NOT NULL, LSN binary(10) NOT NULL, tran_begin_time datetime NOT NULL, tran_end_time datetime NOT NULL, tran_id varbinary(10) NOT NULL, tran_begin_lsn binary(10) NOT NULL,
							SCN bigint NULL, SCN_DT nvarchar(29) NULL, MIN_SCN bigint NULL, MAX_SCN bigint NULL, DT nvarchar(29) NULL, DT_LSN binary(10) NULL)

BEGIN
	IF @TargetForm <> 'LAK' RETURN -- Only Lake targets currently get a __$Time table

	SET @SourceProxy = REPLACE(REPLACE(@SourceProxy,']',''),'[','')
	SET @SourceServer = REPLACE(REPLACE(@SourceServer,']',''),'[','')
	SET @SourceDatabase = ISNULL(REPLACE(REPLACE(@SourceDatabase,']',''),'[',''),db_name())
	SET @SourceSchema = ISNULL(REPLACE(REPLACE(@SourceSchema,']',''),'[',''),'dbo')
	SET @TargetServer = REPLACE(REPLACE(@TargetServer,']',''),'[','')
	SET @TargetDatabase = ISNULL(REPLACE(REPLACE(@TargetDatabase,']',''),'[',''),db_name())
	SET @TargetSchema = ISNULL(REPLACE(REPLACE(@TargetSchema,']',''),'[',''),'dbo')


	------------------------------------------------------ CREATE TABLE  ----------------------------------------------------------------

	SET @SQL = 'SELECT @i = ISNULL(MAX(x),0) FROM '
					+Control.RemoteQuery('SELECT	1 x
											FROM	['+@TargetDatabase+'].sys.schemas s WITH (NOLOCK)
													INNER JOIN ['+@TargetDatabase+'].sys.tables t WITH (NOLOCK) ON (t.schema_id = s.schema_id)
											WHERE	s.name = '''+@TargetSchema+'''
													AND t.name = ''__$Time''
											',@TargetServer,DEFAULT)+' x'
	EXECUTE dbo.sp_executesql @SQL, N'@i bit OUTPUT', @i=@i OUTPUT
	IF @i = 0
		BEGIN
			SET @SQL = 'CREATE TABLE ['+@TargetDatabase+'].['+@TargetSchema+'].__$Time
						(	[__$Load]			[datetime2](7)	NOT NULL, 
							[__$From]			[datetime2](7)	NOT NULL, 
							[LSN]				[binary](10)	NOT NULL, 
							[tran_begin_time]	[datetime]		NOT NULL, 
							[tran_end_time]		[datetime]		NOT NULL, 
							[tran_id]			[varbinary](10)	NOT NULL, 
							[tran_begin_lsn]	[binary](10)	NOT NULL, 
							[SCN]				[bigint]		NULL,
							[SCN_DT]			[nvarchar](29)	NULL,
							[MIN_SCN]			[bigint]		NULL,
							[MAX_SCN]			[bigint]		NULL,
							[DT]				[nvarchar](29)	NULL,
							[DT_LSN]			[binary](10)	NULL
						)
						CREATE UNIQUE CLUSTERED INDEX [CI_Time] ON ['+@TargetDatabase+'].['+@TargetSchema+'].__$Time([LSN] ASC) ON '+@TargetDatabase+'_Default
						CREATE NONCLUSTERED INDEX [NI_Time_From] ON ['+@TargetDatabase+'].['+@TargetSchema+'].__$Time([__$From] ASC)' 
			IF ISNULL(@TargetServer,@@SERVERNAME) <> @@SERVERNAME SET @SQL = 'EXEC('''+@SQL+''') AT ['+@TargetServer+']'
			IF @DryRun = 0 
				EXECUTE dbo.sp_executesql @SQL
			ELSE
				SELECT @SQL
		END
	ELSE IF @FromDate IS NULL 
		BEGIN
			SET @SQL = 'SELECT @FromDate = __$From FROM '+Control.RemoteQuery('SELECT TOP 1 __$From FROM  ['+@TargetDatabase+'].['+@TargetSchema+'].[__$Time] ORDER BY __$From DESC',@TargetServer,DEFAULT)+' x'
			EXECUTE dbo.sp_executesql @SQL, N'@FromDate datetime2(7) OUTPUT', @Fromdate=@FromDate OUTPUT
		END
	IF @FromDate = @ToDate RETURN -- Don't go any further unless there is a date range to process

	-- If @FromDate falls during the overlap hour when clocks go back, then we can end up losing the first part of the second pass through the same hour because it will actually be less than the current @FromDate - so any time the @FromDate falls within the overlap hour, just subtract 1 to be on safe side
	-- and let upsert logic at end ignore any duplicates already in the __$Time period for the additional hour
	IF DATEPART(mm,@FromDate) = 10 -- October
		AND DATEPART(dw,@FromDate) = 1 -- Sunday
		AND DATEPART(hh,@FromDate) = 0 -- 01:00:00.0000000 to 01:59:59.9999999, which will have had 1 hour subtracted by default, making it 00:00:00.0000000 to 00:59:59.9999999
		AND DATEDIFF(day,@FromDate, CONVERT(datetime2(3),DATEADD(day,1,EOMONTH(@FromDate)))) <= 7 -- Last week of month
		SET @FromDate = DATEADD(hour,-1,@FromDate)

	IF @SourceForm = 'LAK'
		SET @SQL = 'SELECT	__$Load, __$From, LSN, tran_begin_time, tran_end_time, tran_id, tran_begin_lsn, SCN, SCN_DT, MIN_SCN, MAX_SCN, DT, DT_LSN  
					FROM	['+@SourceDatabase+'].['+@SourceSchema+'].__$Time 
					WHERE	__$From > CONVERT(datetime2(7),'''+ISNULL(CONVERT(varchar,@FromDate,121),'1900-01-01')+''')
							AND __$From <= CONVERT(datetime2(7),'''+ISNULL(CONVERT(varchar,@ToDate,121),'9999-12-31')+''')'

	IF @SourceForm IN ('NON','PUB','OLD')
		SET @SQL = 'SELECT	CONVERT(datetime2(7), NULL) __$Load, d __$From, CONVERT(binary(10),CONVERT(bigint,DATEDIFF(second,''2010-01-01'',d)),1) LSN, ISNULL(CONVERT(datetime2(7),'''+ISNULL(CONVERT(varchar,@FromDate,121),'1900-01-01')+'''),d) tran_begin_time, d tran_end_time, 0x00000000000000000000 tran_id, 0x00000000000000000000 tran_begin_lsn, 
							CONVERT(bigint,NULL) SCN, CONVERT(nvarchar(29),NULL) SCN_DT, CONVERT(bigint,NULL) MIN_SCN, CONVERT(bigint,NULL) MAX_SCN, CONVERT(nvarchar(29),NULL) DT, CONVERT(binary(10),NULL)  DT_LSN
					FROM	(SELECT ISNULL(CONVERT(datetime2(7),'+ISNULL(''''+CONVERT(varchar,@ToDate,121)+'''','NULL')+'),SYSUTCDATETIME()) d) x'

	IF @SourceForm = 'CDC'
		BEGIN
			-- Check for SCN_MAPPING record required to correct time from Oracle source via Attunity
			SET @SQL = 'SELECT @SCN_MAPPING = SCN_MAPPING FROM '
							+Control.RemoteQuery('SELECT	t.name SCN_MAPPING
													FROM	['+@SourceDatabase+'].sys.schemas s WITH (NOLOCK)
															INNER JOIN ['+@SourceDatabase+'].sys.tables t WITH (NOLOCK) ON (t.schema_id = s.schema_id)
													WHERE	s.name = '''+@SourceSchema+'''
															AND t.name LIKE ''%SCN_MAPPING%''
													',@SourceServer,@SourceProxy)+' x'
			EXECUTE dbo.sp_executesql @SQL, N'@SCN_MAPPING sysname OUTPUT', @SCN_MAPPING=@SCN_MAPPING OUTPUT
			IF @SCN_MAPPING IS NOT NULL
				BEGIN -- Has SCN_MAPPING
					-- Set LSN range bounds for maintaining the target __$Time table
					SET @SQL = 'SELECT @FromLSN = CONVERT(binary(6),CONVERT(bigint,MIN_SCN)) + 0x00000000 
								FROM '+Control.RemoteQuery('SELECT	TOP 1 MIN_SCN 
															FROM	['+@SourceDatabase+'].['+@SourceSchema+'].['+@SCN_MAPPING+'] 
															WHERE	SCN_DT >= '''+ISNULL(CONVERT(varchar(19),@FromDate AT TIME ZONE 'UTC' AT TIME ZONE 'GMT Standard Time',121),'1900-01-01 00:00:00')+'.000000000'+''' 
																	AND __$operation in (2,4) 
															ORDER BY __$start_lsn',	@SourceServer,DEFAULT)+' x'
					EXECUTE dbo.sp_executesql @SQL, N'@FromLSN binary(10) OUTPUT', @FromLSN=@FromLSN OUTPUT
					SET @SQL = 'SELECT @ToLSN = CONVERT(binary(6),CONVERT(bigint,MAX_SCN)) + 0xFFFFFFFF
								FROM '+Control.RemoteQuery('SELECT	TOP 1 MAX_SCN 
															FROM	['+@SourceDatabase+'].['+@SourceSchema+'].['+@SCN_MAPPING+'] 
															WHERE	SCN_DT <= '''+ISNULL(CONVERT(varchar(19),@ToDate AT TIME ZONE 'UTC' AT TIME ZONE 'GMT Standard Time',121),'9999-12-31 00:00:00')+'.000000000'+''' 
																	AND __$operation in (2,4) 
															ORDER BY __$start_lsn DESC',@SourceServer,DEFAULT)+' x'
					EXECUTE dbo.sp_executesql @SQL, N'@ToLSN binary(10) OUTPUT', @ToLSN=@ToLSN OUTPUT

					SET @SQL = 'SELECT	DATEADD(nanosecond, 100*(CONVERT(bigint,CONVERT(binary(6),start_lsn))-MIN_SCN+1), CONVERT(datetime2(7),CONVERT(datetime2(0),SCN_DT) AT TIME ZONE ''GMT Standard Time'' AT TIME ZONE ''UTC'')) __$From,
										start_lsn LSN, tran_begin_time, tran_end_time, tran_id, tran_begin_lsn,
										CONVERT(bigint,CONVERT(binary(6),start_lsn)) SCN, SCN_DT, MIN_SCN, MAX_SCN, DT, __$start_lsn DT_LSN
								FROM	['+@SourceDatabase+'].['+@SourceSchema+'].lsn_time_mapping l
										INNER JOIN (	SELECT SCN_DT, MIN_SCN, LEAD(MIN_SCN -1, 1, MAX_SCN) OVER(ORDER BY MIN_SCN) MAX_SCN, MIN(DT) DT, MIN(__$start_lsn) __$start_lsn -- Grouping added to cope with unexpected scerio whereby repeat records out of sequence are provided in SCN_MAPPING table (just stick with original MIN values first encountered for any duplicate)
														FROM ['+@SourceDatabase+'].['+@SourceSchema+'].['+@SCN_MAPPING+'] 
														WHERE __$operation IN (2,4) 
														GROUP BY SCN_DT, MIN_SCN, MAX_SCN
													) s ON l.start_lsn BETWEEN CONVERT(binary(6),CONVERT(bigint,s.MIN_SCN)) + 0x00000000 AND CONVERT(binary(6),CONVERT(bigint,s.MAX_SCN)) + 0xFFFFFFFF
								WHERE	l.start_lsn between '+CONVERT(varchar,@FromLSN,1)+' AND '+CONVERT(varchar,@ToLSN,1)+'
										AND s.__$start_lsn >'+CONVERT(varchar,@FromLSN,1)
				END
			ELSE
				BEGIN	-- No SCN_MAPPING
					-- Set LSN range bounds for maintaining the target __$Time table
					SET @SQL = 'SELECT @FromLSN = start_lsn 
								FROM '+Control.RemoteQuery('SELECT	TOP 1 start_lsn
															FROM	['+@SourceDatabase+'].['+@SourceSchema+'].lsn_time_mapping 
															WHERE	tran_end_time >= CONVERT(datetime,'''+ISNULL(CONVERT(varchar(23),@FromDate AT TIME ZONE 'UTC' AT TIME ZONE 'GMT Standard Time',121),'1900-01-01')+''') 
															ORDER BY start_lsn', @SourceServer,DEFAULT)+' x'
					EXECUTE dbo.sp_executesql @SQL, N'@FromLSN binary(10) OUTPUT', @FromLSN=@FromLSN OUTPUT
					SET @SQL = 'SELECT @ToLSN = start_lsn 
								FROM '+Control.RemoteQuery('SELECT	TOP 1 start_lsn
															FROM	['+@SourceDatabase+'].['+@SourceSchema+'].lsn_time_mapping 
															WHERE	tran_end_time <= CONVERT(datetime,'''+ISNULL(CONVERT(varchar(23),@ToDate AT TIME ZONE 'UTC' AT TIME ZONE 'GMT Standard Time',121),'9999-12-31')+''') 
															ORDER BY start_lsn DESC', @SourceServer,DEFAULT)+' x'
					EXECUTE dbo.sp_executesql @SQL, N'@ToLSN binary(10) OUTPUT', @ToLSN=@ToLSN OUTPUT
							
					SET @SQL = 'SELECT	DATEADD(nanosecond, 100*ROW_NUMBER() OVER (PARTITION BY tran_end_time ORDER BY start_lsn) , CONVERT(datetime2(7),CONVERT(datetime2(3),tran_end_time) AT TIME ZONE ''GMT Standard Time'' AT TIME ZONE ''UTC'')) __$From,
										start_lsn LSN, tran_begin_time, tran_end_time, tran_id, tran_begin_lsn,
										CONVERT(bigint,NULL) SCN, CONVERT(nvarchar(29),NULL) SCN_DT, CONVERT(bigint,NULL) MIN_SCN, CONVERT(bigint,NULL) MAX_SCN, CONVERT(nvarchar(29),NULL) DT, CONVERT(binary(10),NULL)  DT_LSN 
								FROM	['+@SourceDatabase+'].['+@SourceSchema+'].lsn_time_mapping l
								WHERE	l.start_lsn between '+CONVERT(varchar,@FromLSN,1)+' AND '+CONVERT(varchar,@ToLSN,1)
				END
			SET @SQL = 'SELECT	CONVERT(datetime2(7), NULL) __$Load,
						__$From, LSN, tran_begin_time, tran_end_time, tran_id, tran_begin_lsn, SCN, SCN_DT, MIN_SCN, MAX_SCN, DT, DT_LSN 
						FROM	('+@SQL+'	
								) y
						WHERE	__$From > CONVERT(datetime2(7),'''+ISNULL(CONVERT(varchar,@FromDate,121),'1900-01-01')+''')
								AND __$From <= CONVERT(datetime2(7),'''+ISNULL(CONVERT(varchar,@ToDate,121),'9999-12-31')+''')'
		END

	-- Collect new source time data using query generated above
	SET @SQL = 'SELECT	ISNULL(__$Load, DATEADD(nanosecond, 100*ROW_NUMBER() OVER(ORDER BY LSN), CONVERT(datetime2(7),CONVERT(varchar(19),GETUTCDATE(),121)))) __$Load, -- __$Load is passed through from LAK source, otherwise set based on current date with increment to differentiate LSNs, but done here to ensure all load times set on receiving server rather than remote server
						__$From, LSN, tran_begin_time, tran_end_time, tran_id, tran_begin_lsn, SCN, SCN_DT, MIN_SCN, MAX_SCN, DT, DT_LSN 
				FROM	'+CASE @SourceForm WHEN 'NON' THEN '('+@SQL+')' ELSE Control.RemoteQuery(@SQL,@SourceServer,@SourceProxy) END+' x'
	IF @DryRun = 1 SELECT @SQL
	INSERT #Source EXEC(@SQL)

	-- ***********************************************************************************************************************************************************************************************************************
	-- **** ONLY USED ONCE A YEAR WHEN CLOCKS GO BACK
				-- Get all LSNs for the 2 hour overlap period when clocks go back (both from the hour before the clocks went back and the 1 hour repeat of the same times after the clocks went back)
				SELECT	LSN, __$From
				INTO	#ClockOverlap
				FROM	#Source 
				WHERE	DATEPART(mm,__$From) = 10 -- October
						AND DATEPART(dw,__$From) = 1 -- Sunday
						AND DATEPART(hh,__$From) = 0 -- 01:00:00.0000000 to 01:59:59.9999999, which will have had 1 hour subtracted by default, making it 00:00:00.0000000 to 00:59:59.9999999, hence hour 0
						AND DATEDIFF(day,__$From, CONVERT(datetime2(3),DATEADD(day,1,EOMONTH(__$From)))) <= 7 -- Last week of month

				SELECT @MinFrom = MIN(__$From) FROM #Source

				-- If the start of the current processing period sits in the 2 overlap period when clocks go back, then we will need to retrieve the earlier records from the overlap period from the __$Time period 
				-- so we can identify the point when the time moved backwards in the overlap
				IF DATEPART(mm,@MinFrom) = 10 -- October
					AND DATEPART(dw,@MinFrom) = 1 -- Sunday
					AND DATEPART(hh,@MinFrom) = 0 -- 01:00:00.0000000 to 01:59:59.9999999, which will have had 1 hour subtracted by default, making it 00:00:00.0000000 to 00:59:59.9999999, hence hour 0
					AND DATEDIFF(day,@MinFrom, CONVERT(datetime2(3),DATEADD(day,1,EOMONTH(@MinFrom)))) <= 7 -- Last week of month
					BEGIN
						SET @SQL = 'SELECT	LSN, __$From
							FROM	['+@TargetDatabase+'].['+@TargetSchema+'].[__$Time] 
									WHERE	__$From > CONVERT(datetime2(7),'''+CONVERT(varchar,@MinFrom,23)+''') 
											AND DATEPART(mm,__$From) = 10 -- October 
											AND DATEPART(dw,__$From) = 1  -- Sunday 
											AND DATEPART(hh,__$From) = 0  -- 01:00:00.0000000 to 01:59:59.9999999, which will have had 1 hour subtracted by default, making it 00:00:00.0000000 to 00:59:59.9999999, hence hour 0 
											AND DATEDIFF(day,__$From, CONVERT(datetime2(3),DATEADD(day,1,EOMONTH(__$From)))) <= 7  -- Last week of month
							'
						SET @SQL = 'SELECT * FROM '+Control.RemoteQuery(@SQL,@TargetServer,DEFAULT)+' x'
						INSERT	#ClockOverlap EXEC(@SQL)
					END

				-- Move __$From forward an hour where it is in the second hour of the overlapping period when clocks go back (will already have been moved back an hour by conversion to UTC so this step correctly 
				-- returns the second hour to where it should be)
				UPDATE	s
				SET		__$From = DATEADD(hour,1,__$From) 
				FROM	#Source s
						INNER JOIN	(	SELECT	MIN(CASE WHEN __$From < Previous__$From THEN LSN ELSE NULL END) MinLSN, -- The first LSN where time went backwards during the overlap 2 hour period - i.e. the start point of the second hour
												MAX(LSN) MaxLSN
										FROM	(SELECT	LSN, __$From, LAG(__$From) OVER (ORDER BY LSN) Previous__$From FROM	#ClockOverlap) x 
										GROUP BY EOMONTH(__$From) -- Just in case we ever load for more than a 12 month period - to give distinct LSN ranges for the overlap in each year
									) x ON s.LSN BETWEEN x.MinLSN AND x.MaxLSN
	-- ***************************************************************************************************************************************************************************************************************

	SELECT @FromLSN = MIN(LSN), @ToLSN = MAX(LSN) FROM #Source

	-- Collect any existing target data for the same period as source data
	SET @SQL = 'SELECT	__$Load, __$From, LSN, tran_begin_time, tran_end_time, tran_id, tran_begin_lsn, SCN, SCN_DT, MIN_SCN, MAX_SCN, DT, DT_LSN 
				FROM	['+@TargetDatabase+'].['+@TargetSchema+'].[__$Time] 
				WHERE	LSN BETWEEN '+CONVERT(varchar,@FromLSN,1)+' AND '+CONVERT(varchar,@ToLSN,1)
	SET @SQL = 'SELECT * FROM '+Control.RemoteQuery(@SQL,@TargetServer,DEFAULT)+' x'
	IF @DryRun = 1 SELECT @SQL
	INSERT #Target EXEC(@SQL)

	-- Compare and identify changes
	SELECT	CASE WHEN COUNT(*) OVER (PARTITION BY LSN) = 2 THEN 'U'+CASE MAX(src) WHEN 'S' THEN '+' ELSE '-' END WHEN MAX(src) = 'S' THEN 'I' WHEN MAX(src) = 'T' THEN 'D' ELSE 'X' END Upsert,
			__$Load, __$From, LSN, tran_begin_time, tran_end_time, tran_id, tran_begin_lsn, SCN, SCN_DT, MIN_SCN, MAX_SCN, DT, DT_LSN
	INTO	#Differences
	FROM	(	SELECT 'S' src, * FROM #Source
				UNION ALL
				SELECT 'T' src, * FROM #Target
			) x
	GROUP BY __$Load, __$From, LSN, tran_begin_time, tran_end_time, tran_id, tran_begin_lsn, SCN, SCN_DT, MIN_SCN, MAX_SCN, DT, DT_LSN
	HAVING COUNT(*) <> 2

	IF @DryRun = 1 SELECT TOP 10000 * FROM #Differences ORDER BY LSN, Upsert

	SET @SQL = 'UPDATE	t
				SET		__$Load = s.__$Load, __$From = s.__$From, tran_begin_time = s.tran_begin_time, tran_end_time = s.tran_end_time, tran_id = s.tran_id, tran_begin_lsn = s.tran_begin_lsn,
						SCN = s.SCN, SCN_DT = s.SCN_DT, MIN_SCN = s.MIN_SCN, MAX_SCN = s.MAX_SCN, DT = s.DT, DT_LSN = s.DT_LSN
				FROM	' + CASE WHEN ISNULL(@TargetServer,@@SERVERNAME) <> @@SERVERNAME THEN '['+@TargetServer+'].' ELSE '' END + '['+@TargetDatabase+'].['+@TargetSchema+'].[__$Time] t
						INNER JOIN #Differences s ON s.LSN = t.LSN
				WHERE	s.Upsert = ''U+''
						AND t.LSN BETWEEN '+CONVERT(varchar,@FromLSN,1)+' AND '+CONVERT(varchar,@ToLSN,1)
	IF @DryRun = 0 
		BEGIN
			EXECUTE dbo.sp_executesql @SQL
			SET @UpdateCount = @@ROWCOUNT
		END
	ELSE
		BEGIN
			SELECT @SQL
			SELECT @UpdateCount = COUNT(*) FROM #Differences WHERE Upsert = 'U+'
		END

	SET @SQL = 'INSERT	' + CASE WHEN ISNULL(@TargetServer,@@SERVERNAME) <> @@SERVERNAME THEN '['+@TargetServer+'].' ELSE '' END + '['+@TargetDatabase+'].['+@TargetSchema+'].[__$Time]
						(__$Load, __$From, LSN, tran_begin_time, tran_end_time, tran_id, tran_begin_lsn, SCN, SCN_DT, MIN_SCN, MAX_SCN, DT, DT_LSN)
				SELECT	__$Load, __$From, LSN, tran_begin_time, tran_end_time, tran_id, tran_begin_lsn, SCN, SCN_DT, MIN_SCN, MAX_SCN, DT, DT_LSN
				FROM	#Differences s
				WHERE	s.Upsert = ''I'''
	IF @DryRun = 0 
		BEGIN
			EXECUTE dbo.sp_executesql @SQL
			SET @InsertCount = @@ROWCOUNT
		END
	ELSE
		BEGIN
			SELECT @SQL
			SELECT @InsertCount = COUNT(*) FROM #Differences WHERE Upsert = 'I'
		END

	PRINT 'Maintain __$Time: Insert = '+CONVERT(varchar,@InsertCount)+', Update = '+CONVERT(varchar,@UpdateCount)

	SELECT TOP 1 @LastFrom = __$From, @LastLoad = __$Load FROM #Differences WHERE Upsert IN ('I','U+') ORDER BY __$Load DESC
	IF @DryRun = 1 SELECT @LastLoad, @LastFrom
END
GO


