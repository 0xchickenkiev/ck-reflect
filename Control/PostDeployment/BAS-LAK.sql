SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
DECLARE @Load datetime2(7), @From datetime2(7)


	SELECT	#Columns# INTO #s FROM (SELECT NULL x WHERE 1=0) x LEFT JOIN #TargetTable# ON 1=0 -- Create empty temp table using target table as source of column definitions to deal with any difference in collations between source and target. The join to the NULL table definition ensures any nullability definitions in Target table are overridden with all columns nullable
	INSERT #s SELECT #Columns# FROM #SourceTable# x
	SELECT	MAX(__$From) __$From, MAX(__$To) __$To, __$Source, #Columns#, MAX(__$Upsert) __$Upsert
	INTO	#t
	FROM (	SELECT	__$From, @From __$To, __$Source, #Columns#, 'U' __$Upsert FROM #TargetTable# WHERE __$To = CONVERT(DATETIME2(7),'9999-12-31 00:00:00.000')
			UNION ALL
			SELECT	@From __$From, CONVERT(datetime2(7),'9999-12-31 00:00:00.000') __$To, @Source __$Source, #Columns#, 'I' __$Upsert FROM #s
		) x
	GROUP BY __$Source, #Columns#
	HAVING COUNT(*) <> 2
	
	INSERT INTO #TargetTable# (__$From, __$To, __$Load, __$Action, __$Source, #Columns#)
	SELECT	CONVERT(datetime2(7),'1900-01-01 00:00:00.0000000' ) __$From, CONVERT(datetime2(7),'9999-12-31' ) __$To, CONVERT(datetime2(7),@ToDate ) __$Load, 'X' __$Action, __$Source, #Columns# 
	FROM	#t
	WHERE	__$Upsert = 'I'
	SET @InsertCount = @@ROWCOUNT