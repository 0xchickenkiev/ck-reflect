USE [Lake_Control]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [Control].[Logging]
(	@BatchDate datetime2(7),			-- (M) 2014-04-24 17:50:37.035
	@BatchSuffix varchar(25)	= NULL,	-- (O) (Waiting), (Dry Run), None or any other suffix to identify a distinct run for a given Batch date
	@Task varchar(50),					-- (M) LAK-LAK
	@Step varchar(50)			= 'N/A',-- (O) For future use
	@Server varchar(128),				-- (M) EQ3DBS05
	@Database varchar(128),				-- (M) Acquisition
	@Schema varchar(128),				-- (M) Intrabet
	@Object varchar(128),				-- (M) tblBets
	@Outcome varchar(10),				-- (M) Running, Failed, Complete, Rollback
	@StartTime datetime2(7)		= NULL,	-- (O) 2014-04-24 17:51:29.073
	@EndTime datetime2(7)		= NULL,	-- (O) 2014-04-24 17:52:03.912
	@Error int					= NULL,
	@ErrorMessage varchar(2048)	= NULL,
	@Query varchar(MAX)			= NULL,
	@FromDate datetime2(7)		= NULL,
	@ToDate datetime2(7)		= NULL,
	@Additional1 varchar(2048)	= NULL,
	@Additional2 varchar(2048)	= NULL,
	@Additional3 varchar(2048)	= NULL,
	@Additional4 varchar(2048)	= NULL,
	@InsertCount bigint			= NULL,
	@UpdateCount bigint			= NULL,
	@DeleteCount bigint			= NULL
) AS

DECLARE @SQL nvarchar(MAX)

BEGIN
	IF @@TRANCOUNT > 0
		BEGIN -- There is a current active transaction, so recall this procedure via the autonomous recursive link so that the log entry can be committed independent of the current active transaction
			SELECT	@SQL = '[LOOPBACK_AUTONOMOUS].['+db_name()+'].['+s.name+'].['+OBJECT_NAME(@@PROCID)+']
							@BatchDate,@BatchSuffix,@Task,@Step,@Server,@Database,@Schema,@Object,@Outcome,@StartTime,@EndTime,
							@Error,@ErrorMessage,@Query,@FromDate,@ToDate,@Additional1,@Additional2,@Additional3,@Additional4,@InsertCount,@UpdateCount,@DeleteCount'
			FROM	sys.schemas s 
					INNER JOIN sys.objects o ON (o.schema_id = s.schema_id) 
					WHERE o.object_id = @@PROCID
			EXEC dbo.sp_executesql
				@SQL,
				N'@BatchDate datetime2(7),@BatchSuffix varchar(25),@Task varchar(50),@Step varchar(50),@Server varchar(128),@Database varchar(128),@Schema varchar(128),@Object varchar(128),
					@Outcome varchar(10),@StartTime datetime2(7),@EndTime datetime2(7),@Error int,@ErrorMessage varchar(2048),@Query varchar(MAX),@FromDate datetime2(7),@ToDate datetime2(7),
					@Additional1 varchar(2048),@Additional2 varchar(2048),@Additional3 varchar(2048),@Additional4 varchar(2048),@InsertCount bigint,@UpdateCount bigint,@DeleteCount bigint', 
					@BatchDate=@BatchDate,@BatchSuffix=@BatchSuffix,@Task=@Task,@Step=@Step,@Server=@Server,@Database=@Database,@Schema=@Schema,@Object=@Object,
					@Outcome=@Outcome,@StartTime=@StartTime,@EndTime=@EndTime,@Error=@Error,@ErrorMessage=@ErrorMessage,@Query=@Query,@FromDate=@FromDate,@ToDate=@ToDate,
					@Additional1=@Additional1,@Additional2=@Additional2,@Additional3=@Additional3,@Additional4=@Additional4,@InsertCount=@InsertCount,@UpdateCount=@UpdateCount,@DeleteCount=@DeleteCount
		END
	ELSE
		BEGIN -- No current active transaction so write the log details to the database and commit locally
			SET @StartTime = ISNULL(@StartTime,SYSDATETIME())
			SET @EndTime = ISNULL(@EndTime, CASE @Outcome WHEN 'Success' THEN SYSDATETIME() WHEN 'Failure' THEN SYSDATETIME() WHEN 'Pass' THEN SYSDATETIME() ELSE NULL END)
			BEGIN TRANSACTION
			UPDATE	[Control].[Log]
			SET		UserId = @@SPID, UserName = SYSTEM_USER, Query = ISNULL(@Query, Query),
					EndTime = ISNULL(@EndTime, EndTime), 
					Outcome = ISNULL(@Outcome, Outcome), Error = @Error, ErrorMessage = @ErrorMessage, 
					FromDate = ISNULL(@FromDate,FromDate), ToDate = ISNULL(@ToDate,ToDate),
					Additional1 = ISNULL(@Additional1, Additional1), Additional2 = ISNULL(@Additional2, Additional2), Additional3 = ISNULL(@Additional3, Additional3), Additional4 = ISNULL(@Additional4, Additional4),
					InsertCount = ISNULL(@InsertCount, InsertCount), UpdateCount = ISNULL(@UpdateCount, UpdateCount), DeleteCount = ISNULL(@DeleteCount, DeleteCount)
			WHERE	BatchDate = @BatchDate AND Batch = CONVERT(varchar,@BatchDate)+ISNULL(@BatchSuffix,'') AND Task = @Task AND Step = @Step AND ServerName = @Server AND DatabaseName = @Database AND SchemaName = @Schema AND ObjectName = @Object

			IF @@ROWCOUNT = 0
				INSERT INTO [Control].[Log]
					(BatchDate,Batch,Task,Step,ServerName,DatabaseName,SchemaName,ObjectName,Outcome,StartTime,EndTime,Error,ErrorMessage,Query,
						FromDate,ToDate,Additional1,Additional2,Additional3,Additional4,InsertCount,UpdateCount,DeleteCount,UserId,UserName)
				VALUES
					(@BatchDate,CONVERT(varchar,@BatchDate)+ISNULL(@BatchSuffix,''),@Task,@Step,@Server,@Database,@Schema,@Object,@Outcome,@StartTime,@EndTime,@Error,@ErrorMessage,@Query,
					@FromDate,@ToDate,@Additional1,@Additional2,@Additional3,@Additional4,@InsertCount,@UpdateCount,@DeleteCount,@@SPID,SYSTEM_USER)

			COMMIT TRANSACTION
		END
END
GO


