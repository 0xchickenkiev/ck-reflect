CREATE VIEW [Control].[RunLatest] AS
SELECT	TOP 20 * -- TOP doesn't do anything to results but allows ORDER BY to be specified in view for ease of use
FROM	control.RunSummary 
WHERE	BatchDate IN	
		(	SELECT	DISTINCT TOP 20 BatchDate 
			FROM	Control.Log 
			ORDER BY BatchDate DESC
		)
ORDER BY FirstQueryStart DESC