SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
	EXEC #SourceProxy#.Lake_Control.Control.Remote '#SourceTable#', '#Columns#', '#Keys#', '#Retentionkey#',	@FromDate, @ToDate, @RetentionDate, 1, '#BufferTable#'
	SELECT * INTO #t FROM #SourceProxy#.Lake_Control.Buffer.#BufferTable# 

CREATE TABLE #Actions (Change varchar(20))

MERGE #TargetTable# b
USING	(SELECT	* FROM	#t WHERE __$Action IN ('I','U')) a ON (#Condition_b_a# AND a.__$Source = b.__$Source)
WHEN MATCHED THEN 
	UPDATE SET  b.__$From = a.__$From, b.__$Load = a.__$Load, b.__$Action = a.__$Action, #Assignment_b_a#
WHEN NOT MATCHED THEN 
	INSERT (__$From, __$Load, __$Action, __$Source, #Columns#) VALUES (a.__$From, a.__$Load, a.__$Action, a.__$Source, #Columns_a#)
OUTPUT $action INTO #Actions;

SELECT @InsertCount = COUNT(*) FROM #Actions WHERE Change ='INSERT'
SELECT @UpdateCount = COUNT(*) FROM #Actions WHERE Change ='UPDATE'

DELETE	b 
FROM	(SELECT	#Keys#, __$Source FROM	#t WHERE __$Action = 'D') a
		INNER JOIN #TargetTable# b ON (#Condition_b_a# AND a.__$Source = b.__$Source)
SET @DeleteCount = @@ROWCOUNT