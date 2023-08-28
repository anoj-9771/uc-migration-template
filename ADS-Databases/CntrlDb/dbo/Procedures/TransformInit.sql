

CREATE PROCEDURE [dbo].[TransformInit] 
	@BatchID VARCHAR(MAX)
AS
BEGIN

BEGIN
	
	INSERT INTO [dbo].[TransformStatus] (
	[BatchID]
    ,[TransformID]
	,[CreatedDTS])
	SELECT 
	@BatchID [BatchID]
	,M.[TransformID]
	,CONVERT(DATETIME, CONVERT(DATETIMEOFFSET, GETDATE()) AT TIME ZONE 'AUS Eastern Standard Time') [CreatedDTS]
	FROM [dbo].[TransformManifest] M
	WHERE M.[Enabled] = 1 
	  AND (JSON_VALUE([ExtendedProperties],'$.runDayOfMonth') IS NULL
	    OR JSON_VALUE([ExtendedProperties],'$.runDayOfMonth') 
			= DAY(CONVERT(DATETIME, CONVERT(DATETIMEOFFSET, GETDATE()) AT TIME ZONE 'AUS Eastern Standard Time'))		
	  )
END

BEGIN
	SELECT 
	S.BatchID
	,S.ID
	,M.*
	,(SELECT STRING_AGG([ParallelGroup], ',') [List] FROM (SELECT DISTINCT TOP 100 PERCENT [ParallelGroup] FROM [dbo].[TransformManifest] ORDER BY [ParallelGroup]) T) [List]
	,COALESCE(C4.[Value], 0) [WorkspaceSwitch]
	FROM [dbo].[TransformStatus] S
	JOIN [dbo].[TransformManifest] M ON M.[TransformID] = S.[TransformID]
	LEFT JOIN [dbo].[Config] C4 ON C4.[KeyGroup] = 'WorkspaceSwitch' AND C4.[Key] = '_DefaultValue_'
	WHERE 
	S.BatchID = @BatchID
	AND M.[Enabled] = 1
END

END
GO

