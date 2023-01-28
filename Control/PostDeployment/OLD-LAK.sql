SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
--DECLARE @Load datetime2(7)
--SELECT TOP 1 @Load = __$Load FROM #TargetSchema#.__$Time WHERE __$From > CONVERT(datetime2(7),@FromDate) AND __$From <= CONVERT(datetime2(7),@ToDate ) ORDER BY __$From DESC

	INSERT INTO #TargetTable# (__$Load, __$From, __$To, __$Action, __$Source, #Columns#)  
	SELECT	DateFrom AT TIME ZONE 'GMT Standard Time' AT TIME ZONE 'UTC' __$Load, 
			DateFrom AT TIME ZONE 'GMT Standard Time' AT TIME ZONE 'UTC' __$From, 
			DateTo AT TIME ZONE 'GMT Standard Time' AT TIME ZONE 'UTC' __$To, 
			CASE DateFrom WHEN MIN(DateFrom) OVER (PARTITION BY 1) THEN 'X' WHEN MIN(DateFrom) OVER (PARTITION BY #Keys#) THEN 'I' ELSE 'U' END __$Action, 
			0 __$Source, 
			#Columns#
	FROM	#SourceTable# x
	SET @InsertCount = @@ROWCOUNT

	INSERT INTO #TargetTable# (__$Load, __$From, __$To, __$Action, __$Source, #Columns#)  
	SELECT	DateFrom AT TIME ZONE 'GMT Standard Time' AT TIME ZONE 'UTC' __$Load, 
			DateTo AT TIME ZONE 'GMT Standard Time' AT TIME ZONE 'UTC' __$From, 
			'9999-12-31' __$To, 
			'D' __$Action, 
			0 __$Source, 
			#Columns#
	FROM	(SELECT *, MAX(DateTo) OVER (PARTITION BY #Keys#) MaxDateTo FROM #SourceTable# x) y
	WHERE	MaxDateTo <> CONVERT(datetime2(3),'9999-12-31')
			AND MaxDateTo = DateTo
	SET @DeleteCount = @@ROWCOUNT