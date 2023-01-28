USE [Lake_Control]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [Control].[IndexBlueprint]
(
	@Server			sysname = NULL,
	@Database		sysname = NULL,		
	@Schema			sysname,					-- Madatory - Limits any index definitions to a single schema
	@Proxy			sysname = NULL,				-- Optional - Lake proxy server allows the execution of this procedure on another nominated Lake server so that only one result set for all tables is returned where WAN latency for individual queries is an issue
	@Table			sysname = NULL,				-- Optional - Focusses index definition to just one table rather than all tables in the nominated schema
	@Form			char(3) = 'LAK',			-- Optional - Specifies the form of the subject table(s) and consequently the indexing strategy. e.g. 'LAK' = standard Lake format, 'PUB' = CLUSTERED and DateFrom NONCLUSTERED only, NON = Clustered only, Anything else, drop all indexes
	@Scheme			sysname = NULL,				-- Optional - Specifies a new/changed partition scheme to be used for index target. If not specified, the current scheme (if one exists) is maintained
	@PartitionKey	sysname = NULL,				-- Optional - Specifies a partition key column to be used for index target. If not specified, defaults to current key, otherwise no partitioning
	@PrimaryKey		Control.NameList READONLY	-- Optional - Specifies a new primary key for the Table's clustered index (can only be supplied with the @Table parameter)
)
AS
SET NOCOUNT ON
BEGIN

	CREATE TABLE #Indexes (TableName sysname, IndexName sysname NULL, IndexType sysname, PartitionScheme sysname NULL, PartitionKey sysname NULL, ColumnName sysname NULL, ColumnOrder int NULL, Additional smallint NULL, Columns tinyint NULL)

	DECLARE @SQL nvarchar(max)

	SET @SQL = 'SELECT	t.name TableName, 
						i.name IndexName, 
						i.type_desc IndexType,
						p.name PartitionScheme, 
						MAX(CASE c.partition_ordinal WHEN 0 THEN NULL ELSE l.name END) OVER (PARTITION BY t.name) PartitionKey,
						l.name ColumnName, 
						c.key_ordinal ColumnOrder, 
						CASE WHEN c.is_included_column = 1 THEN 0 WHEN c.is_descending_key = 1 THEN -1 ELSE 1 END Additional,
						ISNULL(COUNT(*) OVER (PARTITION BY t.name, i.Name),0) Columns
				FROM	['+ISNULL(@Database,db_name())+'].sys.tables t
						INNER JOIN ['+ISNULL(@Database,db_name())+'].sys.schemas s ON (s.schema_id = t.schema_id)
						INNER JOIN ['+ISNULL(@Database,db_name())+'].sys.indexes i ON (i.object_id = t.object_id)
						LEFT JOIN ['+ISNULL(@Database,db_name())+'].sys.index_columns c ON (c.object_id = i.object_id AND c.index_id = i.index_id)
						LEFT JOIN ['+ISNULL(@Database,db_name())+'].sys.columns l ON (l.object_id = c.object_id AND l.column_id = c.column_id)
						LEFT JOIN ['+ISNULL(@Database,db_name())+'].sys.partition_schemes p ON (p.data_space_id = i.data_space_id)
				WHERE	s.name = '''+@Schema+'''' + CASE WHEN @Table IS NOT NULL THEN '
						AND t.name = '''+@Table+'''' ELSE '' END+'
						AND t.name NOT IN (''__$Time'', ''__$Complete'')'
	SET @SQL = 'INSERT #Indexes SELECT * FROM '+Control.RemoteQuery(@SQL,@Server,@Proxy)+' x'
	EXECUTE (@SQL)

	SELECT CONVERT(int,i) i, Name INTO #PrimaryKey FROM @PrimaryKey -- The CONVERT is to shed the identity associated with the i column in the source table parameter
	IF NOT EXISTS (SELECT 1 FROM #PrimaryKey) INSERT #PrimaryKey SELECT ColumnOrder,ColumnName from #Indexes WHERE IndexType = 'CLUSTERED' AND ColumnName NOT IN ('__$From','__$To','__$Source','__$Action','__$ID')

	;WITH 
	Tables AS
		(	SELECT	DISTINCT TableName, PartitionScheme, PartitionKey
			FROM	#Indexes
			WHERE	IndexType IN ('CLUSTERED','HEAP') 
		)
	SELECT	TableName, IndexName, IndexType, UniqueIndex, PartitionScheme, PartitionKey, ColumnName, ROW_NUMBER() OVER (PARTITION BY TableName, IndexName ORDER BY ColumnOrder) ColumnOrder, Additional, ISNULL(COUNT(*) OVER (PARTITION BY TableName, IndexName),0) Columns
	FROM	(	
				-- 1(a) CLUSTERED Primary Key columns
				SELECT	t.TableName, 'CI_'+t.TableName+'(A)' IndexName, 'CLUSTERED' IndexType, 1 UniqueIndex, ISNULL(@Scheme,PartitionScheme) PartitionScheme, ISNULL(@PartitionKey,PartitionKey) PartitionKey, p.Name ColumnName,
						CASE ROW_NUMBER() OVER (PARTITION BY t.TableName ORDER BY p.i) 
							WHEN 1 THEN 1 
							ELSE 1 + ROW_NUMBER() OVER (PARTITION BY t.TableName ORDER BY p.i) 
						END ColumnOrder, 
						1 Additional
				FROM	Tables t,
						#PrimaryKey p
				WHERE	@Form IN ('LAK','PUB','NON')
				UNION ALL

				-- 1(b) CLUSTERED Primary Key always has __$Source added to cover when multiple source tables are merged into a single lake table without a universal primary key
				SELECT	TableName, 'CI_'+TableName+'(A)' IndexName, 'CLUSTERED' IndexType, CASE WHEN EXISTS (SELECT 1 FROM #PrimaryKey) THEN 1 ELSE 0 END UniqueIndex, ISNULL(@Scheme,PartitionScheme) PartitionScheme, ISNULL(@PartitionKey,PartitionKey) PartitionKey, '__$Source' ColumnName, 98 ColumnOrder, 1 Additional
				FROM	Tables
				WHERE	@Form IN ('LAK','PUB','NON')
				UNION ALL
					
				-- 1(c) CLUSTERED Primary Key always has partition key added to end if specified
				SELECT	TableName, 'CI_'+TableName+'(A)' IndexName, 'CLUSTERED' IndexType, CASE WHEN EXISTS (SELECT 1 FROM #PrimaryKey) THEN 1 ELSE 0 END UniqueIndex, ISNULL(@Scheme,PartitionScheme) PartitionScheme, ISNULL(@PartitionKey,PartitionKey) PartitionKey, ISNULL(@PartitionKey,PartitionKey) ColumnName, 99 ColumnOrder, 1 Additional
				FROM	Tables
				WHERE	@Form IN ('LAK','PUB','NON') AND ISNULL(@PartitionKey,PartitionKey) IS NOT NULL
				UNION ALL
					
				-- 2 NON CLUSTERED on __$From
				SELECT	TableName, 'NI_'+TableName+'_FD(A)' IndexName, 'NONCLUSTERED' IndexType, 0 UniqueIndex, ISNULL(@Scheme,PartitionScheme) PartitionScheme, ISNULL(@PartitionKey,PartitionKey) PartitionKey, '__$From' ColumnName, 1 ColumnOrder, 1 Additional
				FROM	Tables
				WHERE	@Form IN ('LAK','PUB')
				UNION ALL
					
				-- 3 NON CLUSTERED on __$To
				SELECT	TableName, 'NI_'+TableName+'_TD(A)' IndexName, 'NONCLUSTERED' IndexType, 0 UniqueIndex, ISNULL(@Scheme,PartitionScheme) PartitionScheme, ISNULL(@PartitionKey,PartitionKey) PartitionKey, '__$To' ColumnName, 1 ColumnOrder, 1 Additional
				FROM	Tables
				WHERE	@Form IN ('LAK')
				UNION ALL
					
				-- 4 NON CLUSTERED on __$Load
				SELECT	TableName, 'NI_'+TableName+'_LD(A)' IndexName, 'NONCLUSTERED' IndexType, 0 UniqueIndex, ISNULL(@Scheme,PartitionScheme) PartitionScheme, ISNULL(@PartitionKey,PartitionKey) PartitionKey, '__$Load' ColumnName, 1 ColumnOrder, 1 Additional
				FROM	Tables
				WHERE	@Form IN ('LAK','PUB')
			) x
	RETURN;
END;
GO


