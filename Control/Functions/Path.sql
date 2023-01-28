CREATE FUNCTION [Control].[Path]
(	@Table				sysname = NULL,		--Optional - table to include in path or default if path doesn't go down to table level
	@Schema				sysname = NULL,		--Optional - schema to include in path or default if explicit schema name is not required
	@Database			sysname = NULL,		--Optional - database to include in path or default if explicit database name is not required
	@Server				sysname = NULL,		--Optional - server to include in path or default if explicit server name is not required
	@Proxy				sysname = NULL,		--Optional - Nominates an optional intermediate server that the path must be accessed via (e.g. the Linked Server connection for the nominated server is only available on the proxy)
	@DBMS				varchar(10) = NULL,	--Optional - The DBMS for which the path is to be formatted - currently 'SQL Server' or 'Oracle' - i.e. Oracle does not specify a database and prefers "" name delimiters rather than []
	@CompatibilityLevel	smallint = NULL,	--Optional - The Compatibility Level for SQL Server databases, ignored for other DBMS. Default is latest compatibility level
	@Columns			varchar(MAX) = NULL,--Optional - Specifies the a list of columns to be retieved from the table to reduce data volumes from remote servers - @Table parameter must be specified in conjunction with @Columns
	@Form				char(3) = NULL		--Optional - The Form the table in question is in which will dictate any additional metadata columns added to the column list above if provided
	)
RETURNS varchar(MAX)
AS
BEGIN
	DECLARE @LocalParentheses char(1) = CASE @DBMS WHEN 'Oracle' THEN '"' WHEN 'MySQL' THEN '`' ELSE '[' END
	DECLARE @SQL varchar(MAX)

	SET @DBMS = ISNULL(@DBMS,'SQL Server')

	-- Remove any name parentheses already present in other parameters in favour of enforcing our own when constructing queries below
	SET @Table = QUOTENAME(NULLIF(REPLACE(REPLACE(REPLACE(@Table,'[',''),']',''),'"',''),''),@LocalParentheses)
	SET @Schema = QUOTENAME(NULLIF(REPLACE(REPLACE(REPLACE(@Schema,'[',''),']',''),'"',''),''),@LocalParentheses)
	SET @Database = QUOTENAME(NULLIF(REPLACE(REPLACE(REPLACE(@Database,'[',''),']',''),'"',''),''))
	SET @Server = QUOTENAME(NULLIF(NULLIF(REPLACE(REPLACE(REPLACE(@Server,'[',''),']',''),'"',''),''),@@SERVERNAME))
	SET @Proxy = QUOTENAME(NULLIF(REPLACE(REPLACE(REPLACE(@Proxy,'[',''),']',''),'"',''),''))
	-- Standardise column list formatting to be acceptable to the DBMS in question - specifically the parentheses used around column names 
	-- i.e.	Oracle = double quote (Oracle doesn't recognise square brackets)
	--		MySQL  = 'back tick' (because being designed for web developers it doesn't recognise any standard from anyone or anything else)
	IF @DBMS <> 'SQL Server' SET @Columns = NULLIF(REPLACE(REPLACE(@Columns,'[',@LocalParentheses),']',@LocalParentheses),'') ELSE SET @Columns = NULLIF(@Columns,'')

	SET @SQL = CASE WHEN @DBMS = 'SQL Server' AND ISNULL(@CompatibilityLevel,0) <> 80 THEN ISNULL(@Server+'.','') ELSE '' END -- If DBMS can use 4 part naming, then add add server to front of name - otherwise use RemoteQuery below
			+	ISNULL(@Database,'')
			+	CASE WHEN (@Table IS NOT NULL OR @Schema IS NOT NULL) AND @Database IS NOT NULL THEN '.' ELSE '' END
			+	ISNULL(@Schema,'')
			+	CASE WHEN @Table IS NOT NULL AND (@Schema IS NOT NULL OR @Database IS NOT NULL) THEN '.' ELSE '' END
			+	ISNULL(@Table,'')

	IF @Columns IS NOT NULL SET @SQL = 'SELECT '+CASE @Form WHEN 'CDC' THEN '__$start_lsn, __$seqval, __$operation, ' WHEN 'LAK' THEN '__$From, __$To, __$Load, __$Action, __$Source, ' WHEN 'OLD' THEN 'DateFrom, DateTo, ' ELSE '' END+@Columns+' FROM '+@SQL

	IF @DBMS = 'SQL Server' AND ISNULL(@CompatibilityLevel,0) <> 80 --If DBMS can use 4 part naming for server, just call RemoteQuery with proxy, knowing the server will already be handled by 4 part name in main query
		SET @SQL = Control.RemoteQuery(@SQL,@Proxy,DEFAULT)
	ELSE				-- Otherwise, call RemoteQuery nominating the server via the proxy
		SET @SQL = Control.RemoteQuery(@SQL,@Server,@Proxy)

	IF @Columns IS NULL AND @SQL LIKE '(%' SET @SQL = SUBSTRING(@SQL,2,LEN(@SQL)-2) 
	      
	RETURN @SQL
END
