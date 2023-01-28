SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
	EXEC #SourceProxy#.Lake_Control.Control.Remote '#SourceTable#', '#Columns#', '#Keys#', '#Retentionkey#', @FromDate, @ToDate, @RetentionDate, 0, '#BufferTable#'
	SELECT * INTO #t FROM #SourceProxy#.Lake_Control.Buffer.#BufferTable# 

	UPDATE	a SET a.__$To = b.__$To
	FROM	#TargetTable# a --WITH (FORCESEEK, INDEX(1))
			INNER JOIN #t b ON (#Condition_b_a# AND b.__$Source = a.__$Source AND b.__$From = a.__$From)
	WHERE	b.__$From <= @FromDate
			AND a.__$To = CONVERT(DATETIME2(7),'9999-12-31 00:00:00.0000000')
	SET @UpdateCount = @@ROWCOUNT

	INSERT INTO #TargetTable# (__$Load, __$From, __$To, __$Action, __$Source, #Columns#)  
	SELECT	__$Load, __$From, CASE WHEN __$To > @ToDate THEN CONVERT(datetime2(7),'9999-12-31 00:00:00.0000000') ELSE __$To END __$To, __$Action, __$Source, #Columns#
	FROM	#t
	WHERE	__$From > @FromDate
	SET @InsertCount = @@ROWCOUNT