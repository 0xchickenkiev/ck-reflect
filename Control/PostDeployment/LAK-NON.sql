SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
	EXEC #SourceProxy#.Lake.Control.Remote '#SourceTable#', 'DateFrom, DateTo, #Columns#', '#Keys#', 'DateTo',	@FromDate, @ToDate, '1900-01-01', 1, '#BufferTable#'
	SELECT * INTO #t FROM #SourceProxy#.Lake.Buffer.#BufferTable# 

CREATE TABLE #Actions (Change varchar(20))

MERGE #TargetTable# b
USING	(	select	#Columns# 
			from	#t 
			where	DateFrom > @FromDate 
					AND DateFrom <= @ToDate 
					AND DateTo > @ToDate 
		) a ON (#Condition_b_a#)
WHEN MATCHED THEN 
	UPDATE SET #Assignment_b_a#
WHEN NOT MATCHED THEN 
	INSERT (#Columns#) VALUES (#Columns_a#)
OUTPUT $action INTO #Actions;

SELECT @InsertCount = COUNT(*) FROM #Actions WHERE Change ='INSERT'
SELECT @UpdateCount = COUNT(*) FROM #Actions WHERE Change ='UPDATE'

DELETE	b 
FROM	(	SELECT	#Keys# 
			FROM	#t 
			WHERE	(	DateFrom > @FromDate 
						AND DateFrom <= @ToDate 
						AND DateTo > @ToDate
					)
					OR	
					(	DateTo > @FromDate 
						AND DateTo <= @ToDate 
						AND DateFrom <= @FromDate 
					)
			GROUP BY #Keys#
			HAVING	COUNT(*) = 1
					AND MIN(DateFrom) <= @FromDate 
		) a
		INNER JOIN #TargetTable# b ON (#Condition_b_a#)
SET @DeleteCount = @@ROWCOUNT