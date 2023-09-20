

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
	,S.[TransformID]
    ,[EntityType]
    ,[EntityName]
    ,[ProcessorType]
    ,[TargetKeyVaultSecret]
    ,[Command]
    ,[Dependancies]
    ,[ParallelGroup]
    ,IIF(C4.[Value] = 1, 
		JSON_MODIFY(
			ISNULL([ExtendedProperties], '{}'), '$.OverrideClusterName', COALESCE(JSON_VALUE([ExtendedProperties], '$.OverrideClusterName'), C8.[Value], C7.[Value])
		)
	,[ExtendedProperties]) [ExtendedProperties]
    ,[Enabled]
	,(SELECT STRING_AGG([ParallelGroup], ',') [List] FROM (SELECT DISTINCT TOP 100 PERCENT [ParallelGroup] FROM [dbo].[TransformManifest] ORDER BY [ParallelGroup]) T) [List]
	,COALESCE(C4.[Value], 0) [WorkspaceSwitch]
	FROM [dbo].[TransformStatus] S
	JOIN [dbo].[TransformManifest] M ON M.[TransformID] = S.[TransformID]
	LEFT JOIN [dbo].[Config] C4 ON C4.[KeyGroup] = 'WorkspaceSwitch' AND C4.[Key] = '_Default_'
	LEFT JOIN [dbo].[Config] C7 ON C7.[KeyGroup] = 'TransformOverrideClusterName' AND C7.[Key] = '_Default_'
	LEFT JOIN [dbo].[Config] C8 ON C8.[KeyGroup] = 'TransformOverrideClusterName' AND C8.[Key] = [EntityName]
	WHERE 
	S.BatchID = @BatchID
	AND M.[Enabled] = 1
END

END
GO

