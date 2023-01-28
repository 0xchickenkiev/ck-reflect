CREATE FUNCTION [Control].[RemoteQuery]
(	@Query		varchar(MAX),			--Mandatory - Specifies the query to be run on the nominated server via the proxy
	@Server		sysname = NULL,	--Optional - The server that the above query is to be run on
	@Proxy		sysname = NULL	--Optional - Nominates an intermediate server that the query must be submittd via (e.g. the Linked Server connection for the nominated server is only available on the proxy)
)
RETURNS varchar(MAX)
AS
BEGIN
	DECLARE @SQL varchar(MAX)

	SET @Proxy = REPLACE(REPLACE(REPLACE(@Proxy,'[',''),']',''),'"','') -- Remove any name parentheses already present in other parameters in favour of enforcing our own when constructing queries below
	SET @Server = REPLACE(REPLACE(REPLACE(@Server,'[',''),']',''),'"','') -- ditto

	SET @SQL = @Query

	IF ISNULL(@Server,@@SERVERNAME) <> @@SERVERNAME OR ISNULL(@Proxy,@@SERVERNAME) <> @@SERVERNAME
		BEGIN
			IF @SQL NOT LIKE 'SELECT%' SET @SQL = 'SELECT * FROM '+@SQL
			SET @SQL = 'OPENQUERY('+QUOTENAME(@Server)+', '''+REPLACE(@SQL,CHAR(39),CHAR(39)+CHAR(39))+''')'
			IF ISNULL(@Proxy,@@SERVERNAME) <> @@SERVERNAME 
				SET @SQL = 'OPENQUERY('+QUOTENAME(@Proxy)+', ''SELECT * FROM '+REPLACE(@SQL,CHAR(39),CHAR(39)+CHAR(39))+''')'
		END
	ELSE
		SET @SQL = '('+@Query+')'

	RETURN @SQL
END
