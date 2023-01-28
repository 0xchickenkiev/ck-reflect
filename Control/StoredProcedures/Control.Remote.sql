USE [Lake_Control]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [Control].[Remote]
	(	@TablePath		varchar(MAX),
		@ColumnNames	varchar(MAX),
		@KeyNames		varchar(MAX),
		@RetentionKey	sysname,
		@FromDate		datetime2(7),
		@ToDate			datetime2(7),
		@RetentionDate	Datetime2(7),
		@NetChanges		bit				= 0,	-- Optional switch to either return all versions of a record within the From/To Date window (default) or just the first and last version
		@BufferTable	varchar(128)	= NULL,	-- Optional table name to act as a physical buffer for the data produced otherwise default behaviour is to select results for direct return to calling procedure
		@Autonomous		bit				= 1		-- Option switch to disable autonomous invocation if desired (1 = ensure invocation runs in its own transaction, 0 = inherit any transaction status from invokation
	)
AS
	DECLARE	@SQL			nvarchar(MAX)
	DECLARE @BufferPath		varchar(256)

BEGIN TRY
	IF @@TRANCOUNT > 0 AND @Autonomous = 1
		BEGIN -- There is a current active transaction, so recall this procedure via the autonomous recursive link which begins an independent transaction for this procedure so that the initiation of Snapshot isolation below will not conflict with the current transaction
			SELECT	@SQL = '[LOOPBACK_AUTONOMOUS].['+db_name()+'].['+s.name+'].['+OBJECT_NAME(@@PROCID)+']
							@TablePath, @ColumnNames, @KeyNames, @RetentionKey, @FromDate, @ToDate, @RetentionDate, @NetChanges, @BufferTable, @Autonomous'
			FROM	sys.schemas s 
					INNER JOIN sys.objects o ON (o.schema_id = s.schema_id) 
					WHERE o.object_id = @@PROCID
			EXEC dbo.sp_executesql
				@SQL,
				N'@TablePath varchar(MAX), @ColumnNames varchar(MAX), @KeyNames varchar(MAX), @RetentionKey sysname, @FromDate datetime2(7), @ToDate datetime2(7), @RetentionDate datetime2(7), @NetChanges bit, @BufferTable varchar(128), @Autonomous bit', 
					@TablePath=@TablePath, @ColumnNames=@ColumnNames, @KeyNames=@KeyNames, @RetentionKey=@RetentionKey, @FromDate=@FromDate, @ToDate=@ToDate, @RetentionDate=@RetentionDate, @NetChanges=@NetChanges, @BufferTable=@BufferTable, @Autonomous = 0
		END
	ELSE
		BEGIN -- No current active transaction so begin the processing including initiating Snapshot isolation for consistency
			IF @BufferTable IS NULL 
				SET @BufferPath = '#s'
			ELSE
				BEGIN
					SET @BufferPath = 'Buffer.'+@BufferTable
					SET @SQL = 'DROP TABLE '+@BufferPath
					BEGIN TRY EXECUTE dbo.sp_executesql @SQL END TRY BEGIN CATCH END CATCH
				END

			SET @SQL = 
'						SET TRANSACTION ISOLATION LEVEL SNAPSHOT
						BEGIN TRANSACTION Local

						SELECT	*
						INTO	#x
						FROM	'+@TablePath+' a
						WHERE	__$From > @FromDate 
								AND __$From <= @ToDate' + CASE WHEN @RetentionDate <= @FromDate THEN '' ELSE '
								AND '+@RetentionKey+' > @RetentionDate' END+'
						OPTION(RECOMPILE)

				'+CASE @NetChanges WHEN 1 THEN '
						SELECT	__$Load, __$From, __$To, 
								CASE WHEN __$Action = ''D'' THEN ''D'' WHEN First__$Action = ''I'' THEN ''I'' ELSE ''U'' END __$Action, 
								__$Source, '+@ColumnNames+' 
						INTO	'+@BufferPath+'
						FROM	(	SELECT	*, FIRST_VALUE(__$Action) OVER (PARTITION BY '+@KeyNames+', __$Source ORDER BY __$From) First__$Action
									FROM	#x 
								) a
						WHERE __$To > @ToDate'
					ELSE '
						SELECT	__$Load, __$From, __$To, __$Action, __$Source, '+@ColumnNames+' 
						INTO	'+@BufferPath+'
						FROM	(	SELECT * FROM #x
									UNION ALL 
									SELECT * FROM '+@TablePath+' a WHERE __$From <= @FromDate AND __$To > @FromDate AND __$To <= @ToDate
								) x
						OPTION(RECOMPILE)'
				END+'
				
						COMMIT TRANSACTION Local

						SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED'

			IF @BufferTable IS NULL
				SET @SQL = @SQL+'	
					SELECT __$Load, __$From, __$To, __$Action, __$Source, '+@ColumnNames+' FROM '+@BufferPath
			EXECUTE dbo.sp_executesql @SQL, N'@FromDate datetime2(7), @ToDate datetime2(7), @RetentionDate datetime2(7)', @FromDate=@FromDate, @ToDate=@ToDate, @RetentionDate=@RetentionDate
		END
END TRY
BEGIN CATCH
	THROW
END CATCH
GO


