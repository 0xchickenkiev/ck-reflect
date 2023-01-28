CREATE VIEW [Control].[RunSummary] AS
SELECT	BatchDate,
		Batch, 
		MAX(FromDate) HighestFromDate,
		MAX(ToDate) HighestToDate,
		Task Task,
		DatabaseName Target,
		CASE SchemaName WHEN 'dbo' THEN DatabaseName ELSE SchemaName END Subject, 
		MIN(StartTime) FirstQueryStart, 
		CONVERT(time(3),DATEADD(ms,DATEDIFF(ms,min(StartTime),max(EndTime)), '00:00:00.000')) Elapsed,
--		CONVERT(time,DATEADD(ms, SUM(DATEDIFF(ms, '00:00:00.000', CASE Comments1 WHEN Comments2 THEN '00:00:00.000' ELSE query_run_time END)), '00:00:00.000')) TotalRuntimes, 
		COUNT(*) Considered, 
		SUM(CASE Outcome WHEN 'Pass' THEN 1 ELSE 0 END) Passed,
		SUM(CASE Outcome WHEN 'Success' THEN 1 ELSE 0 END) Processed,
		SUM(CASE Outcome WHEN 'Failure' THEN 1 ELSE 0 END) Failed,
		SUM(CASE Outcome WHEN 'Running' THEN 1 ELSE 0 END) Running,
		SUM(InsertCount) Inserted,
		SUM(UpdateCount) Updated,
		SUM(DeleteCount) Deleted,
		MAX(CASE Outcome WHEN 'Running' THEN ObjectName ELSE NULL END) InProgress
FROM	Control.Log WITH (NOLOCK)
GROUP BY BatchDate, Batch, Task, DatabaseName, SchemaName