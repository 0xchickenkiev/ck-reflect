SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
DECLARE @Load datetime2(7), @From datetime2(7)
SELECT TOP 1 @Load = __$Load, @From = __$From FROM #TargetSchema#.__$Time WHERE __$From > CONVERT(datetime2(7),@FromDate) AND __$From <= CONVERT(datetime2(7),@ToDate ) ORDER BY __$From DESC

	SELECT	#Columns# INTO #s FROM (SELECT NULL x WHERE 1=0) x LEFT JOIN #TargetTable# ON 1=0 -- Create empty temp table using target table as source of column definitions to deal with any difference in collations between source and target. The join to the NULL table definition ensures any nullability definitions in Target table are overridden with all columns nullable
	INSERT #s SELECT #Columns# FROM #SourceTable# x

	SELECT * 
	INTO #t
	FROM 
		(
		SELECT *,  CASE WHEN MAX(__$OldAction) OVER (PARTITION BY #Keys#, __$Source) IS NULL AND  MAX(__$NewAction) OVER (PARTITION BY #Keys#, __$Source) = 'I' THEN 'I'
						WHEN MAX(__$OldAction) OVER (PARTITION BY #Keys#, __$Source) = 'I' AND  MAX(__$NewAction) OVER (PARTITION BY #Keys#, __$Source) = 'I' THEN 'U'
						WHEN MAX(__$OldAction) OVER (PARTITION BY #Keys#, __$Source) = 'I' AND  MAX(__$NewAction) OVER (PARTITION BY #Keys#, __$Source) IS NULL THEN 'D'
						WHEN MAX(__$OldAction) OVER (PARTITION BY #Keys#, __$Source) = 'U' AND  MAX(__$NewAction) OVER (PARTITION BY #Keys#, __$Source) = 'I' THEN 'U'
						WHEN MAX(__$OldAction) OVER (PARTITION BY #Keys#, __$Source) = 'U' AND  MAX(__$NewAction) OVER (PARTITION BY #Keys#, __$Source) IS NULL THEN 'D'
						WHEN MAX(__$OldAction) OVER (PARTITION BY #Keys#, __$Source) = 'D' AND  MAX(__$NewAction) OVER (PARTITION BY #Keys#, __$Source) = 'I' THEN 'I'
						WHEN MAX(__$OldAction) OVER (PARTITION BY #Keys#, __$Source) = 'D' AND  MAX(__$NewAction) OVER (PARTITION BY #Keys#, __$Source) IS NULL THEN 'Z'
					END __$Action
			FROM
			(
					SELECT	MAX(__$From) __$From, MAX(__$To) __$To, __$Source, #Columns#, MAX(__$OldAction) __$OldAction, MAX(__$NewAction) __$NewAction
					FROM (	SELECT	__$From, @From __$To, __$Source, #Columns#,  CASE __$Action WHEN 'X' THEN 'I' ELSE __$Action END __$OldAction, NULL __$NewAction FROM #TargetTable# WHERE __$To = CONVERT(DATETIME2(7),'9999-12-31 00:00:00.000')
							UNION ALL
							SELECT	@From __$From, CONVERT(datetime2(7),'9999-12-31 00:00:00.000') __$To, @Source __$Source, #Columns#, NULL __$OldAction, 'I' __$NewAction FROM #s
						) x
					GROUP BY __$Source, #Columns#
					HAVING COUNT(*) <> 2
			)y
		) k
		WHERE __$Action <> 'Z'

	UPDATE	a SET a.__$To = b.__$To
	FROM	#TargetTable# a WITH (FORCESEEK, INDEX(1))
			INNER JOIN #t b ON (#Condition_b_a# AND b.__$Source = a.__$Source)
	WHERE	a.__$To = CONVERT(DATETIME2(7),'9999-12-31 00:00:00.000')
			AND b.__$OldAction IS NOT NULL
	SET @UpdateCount = @@ROWCOUNT

	INSERT INTO #TargetTable# (__$From, __$To, __$Load, __$Action, __$Source, #Columns#)
	SELECT	__$From, __$To, @Load, __$Action, __$Source, #Columns# 
	FROM	#t
	WHERE	__$NewAction IS NOT NULL
	SET @InsertCount = @@ROWCOUNT
	
	INSERT INTO #TargetTable# (__$From, __$To, __$Load, __$Action, __$Source, #Columns#)
	SELECT	@From __$From, CONVERT(datetime2(7),'9999-12-31 00:00:00.000') __$To, @Load, 'D' __$Action, __$Source, #Columns# 
	FROM	#t
	WHERE	__$NewAction IS NULL AND __$Action = 'D'
	SET @DeleteCount = @@ROWCOUNT