SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

	IF EXISTS(SELECT TOP 1 1 FROM #TargetTable#) RAISERROR ('Target table is not empty', 16, 1)

	SELECT	#Columns# INTO #s FROM (SELECT NULL x WHERE 1=0) x LEFT JOIN #TargetTable# ON 1=0 -- Create empty temp table using target table as source of column definitions to deal with any difference in collations between source and target. The join to the NULL table definition ensures any nullability definitions in Target table are overridden with all columns nullable
	INSERT #s SELECT #Columns# FROM #SourceTable# x
	
	INSERT INTO #TargetTable# (__$From, __$To, __$Load, __$Action, __$Source, #Columns#)
	SELECT CONVERT(datetime2(7),@FromDate ) __$From, CONVERT(datetime2(7),'9999-12-31' ) __$To, CONVERT(datetime2(7),@ToDate ) __$Load, 'X' __$Action, 0 __$Source, #Columns# FROM #s
	SET @InsertCount = @@ROWCOUNT