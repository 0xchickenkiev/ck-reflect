SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
DECLARE @FromLSN binary(10), @ToLSN binary(10)
--SELECT __$Load, __$From, LSN INTO #Time FROM #TargetSchema#.__$Time WHERE __$From > CONVERT(datetime2(7),@FromDate) AND __$From <= CONVERT(datetime2(7),@ToDate)
--SELECT TOP 1 @FromLSN = LSN FROM #Time ORDER BY __$From, LSN
--SELECT TOP 1 @ToLSN = LSN FROM #Time ORDER BY __$From DESC, LSN DESC

SELECT @FromLSN = MIN(LSN), @ToLSN = MAX(LSN) FROM #TargetSchema#.__$Time WHERE __$From > CONVERT(datetime2(7),@FromDate) AND __$From <= CONVERT(datetime2(7),@ToDate )
SELECT __$Load, __$From, LSN INTO #Time FROM #TargetSchema#.__$Time WHERE LSN BETWEEN @FromLSN AND @ToLSN OPTION(RECOMPILE)

-- Create empty capture table using target table definitions so that subsequent insert implicitly resolves any collation differences between source and target
SELECT	#Columns#, __$Load, __$From, __$Action, __$Source, CONVERT(binary(10),NULL) __$start_lsn, CONVERT(binary(10),NULL) __$seqval, CONVERT(int,NULL) __$operation INTO #t FROM (SELECT NULL x WHERE 1=0) x LEFT JOIN #TargetTable# ON 1=0 -- Create empty temp table using target table as source of column definitions to deal with any difference in collations between source and target. The join to the NULL table definition ensures any nullability definitions in Target table are overridden with all columns nullable
INSERT	#t
SELECT	#Columns#, __$Load, __$From, CASE __$operation WHEN 1 THEN 'D' WHEN 2 THEN 'I' WHEN 4 THEN 'U' ELSE 'X' END __$Action, @Source __$Source, __$start_lsn, __$seqval, __$operation
FROM	#SourceTable# s
		LEFT JOIN #Time ON LSN = __$start_lsn -- Left join so than any missing time records will generate an eror in the subsequent script
WHERE	__$operation IN (4,2,1)
		AND __$start_lsn BETWEEN @FromLSN AND @ToLSN

		AND (__$From IS NULL OR (__$From > CONVERT(datetime2(7),@FromDate) AND __$From <= CONVERT(datetime2(7),@ToDate)))

-- Merge multiple changes for the same date-time stamp and derive DateTo using next DateFrom 
SELECT	#Columns#, __$Source, __$Load, __$From, 
		LEAD(__$From,1,'9999-12-31 00:00:00.000') OVER(PARTITION BY #Keys#, __$Source ORDER BY __$From) AS __$To, 
--		__$Action
		CASE WHEN __$Action = 'D' THEN 'D' WHEN First__$Action = 'I' THEN 'I' ELSE 'U' END __$Action
INTO	#t1
FROM	(	SELECT	#Columns#, __$operation, __$Load, __$From, __$Action, __$Source, __$start_lsn, __$seqval, 
					MAX(__$start_lsn) OVER (PARTITION BY #Keys#, __$Source, __$From) Max__$start_lsn,
					MAX(__$seqval) OVER (PARTITION BY #Keys#, __$Source, __$From) Max__$seqval,
					FIRST_VALUE(__$Action) OVER (PARTITION BY #Keys#, __$Source, __$From ORDER BY __$seqval) First__$Action
			FROM	#t
		) x
WHERE	__$seqval=Max__$seqval AND __$start_lsn=MAX__$start_lsn

-- Close off any pre-existing records for the same key using the lowest date encounterd in the new processing window as the DateTo
UPDATE	a SET a.__$To = b.__$From 
FROM	(SELECT #Keys#, __$Source, MIN(__$From) __$From FROM #t1 GROUP BY #Keys#, __$Source) b 
		INNER LOOP JOIN #TargetTable# a ON (#Condition_b_a# AND a.__$Source = b.__$Source)-- AND a.__$To > CONVERT(datetime2(7),@FromDate))
WHERE	a.__$To = CONVERT(datetime2(7),'9999-12-31')
--		a.__$To > b.__$From
--		AND a.__$From < b.__$From
SET @UpdateCount = @@ROWCOUNT

-- Insert all new changes
INSERT INTO #TargetTable# (__$Load, __$From, __$To, __$Action, __$Source, #Columns#)  
SELECT	__$Load, __$From, __$To, __$Action, __$Source, #Columns#
FROM	#t1
WHERE	__$Action <> 'D'
SET @InsertCount = @@ROWCOUNT

-- Insert deletion confirmations
INSERT INTO #TargetTable# (__$Load, __$From, __$To, __$Action, __$Source, #Columns#)  
SELECT	__$Load, __$From, __$To, __$Action, __$Source, #Columns#
FROM	#t1
WHERE	__$Action = 'D'
SET @DeleteCount = @@ROWCOUNT

DROP TABLE #t
DROP TABLE #t1