USE [Lake_Control]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [Control].[Rollback]
(	
	@Date			datetime2(3),			-- Mandatory - Specifies the dtae-time to which the acquisition layer is to be rolled back
	@Schema			varchar(128),			-- Mandatory - Nominates the schema containing tables to be rolled back
	@Table			varchar(128) = NULL		-- Optional - Limits the rollback to a specific table
)
AS

	DECLARE	@Tables TABLE (i int IDENTITY(1,1),Name varchar(128), Deleted int, Updated int)

	DECLARE	@i int,
			@j int

	DECLARE	@SQL varchar(MAX)

BEGIN

	SET NOCOUNT ON 

	-- IDENTIFY ALL TABLES BASED UPON THE PARAMETERS SUPPLIED
	INSERT INTO @Tables (Name)
		SELECT	t.Name 
		FROM	sys.Schemas s
				INNER JOIN sys.Tables t ON (t.schema_id = s.schema_id)
				INNER JOIN sys.Columns cf ON (cf.object_id = t.object_id AND cf.name = 'DateFrom')
				INNER JOIN sys.Columns ct ON (ct.object_id = t.object_id AND ct.name = 'DateTo')
		WHERE	s.Name = @Schema
				AND t.Name = ISNULL(@Table,t.Name)
		ORDER BY t.Name DESC
		SET @i = @@ROWCOUNT;

	WHILE @i > 0
		BEGIN
			SELECT	@SQL = 'DELETE FROM '+@Schema+'.'+Name+' WHERE DateFrom > '''+CONVERT(varchar,@Date)+'''' FROM @Tables WHERE	i = @i
			EXEC(@SQL)
			SET @j = @@ROWCOUNT
			UPDATE @Tables SET Deleted = @j WHERE i = @i
			SELECT	@SQL = 'UPDATE '+@Schema+'.'+Name+' SET DateTo = CONVERT(datetime2(3),''9999-12-31'') WHERE DateTo > '''+CONVERT(varchar,@Date)+''' AND DateTo < CONVERT(datetime2(3),''9999-01-01'') AND DateFrom <= '''+CONVERT(varchar,@Date)+'''' FROM @Tables WHERE	i = @i
			EXEC(@SQL)
			SET @j = @@ROWCOUNT
			UPDATE @Tables SET Updated = @j WHERE i = @i
			SET @i = @i - 1	
		END
	SELECT * FROM @Tables ORDER BY Name

END
GO


