



CREATE PROCEDURE [dbo].[ExtractLoadInit] 
	@BatchID VARCHAR(MAX),
	@SystemCode VARCHAR(MAX),
	@ExtraConfig varchar(MAX) = NULL
AS
BEGIN

BEGIN
	WITH [LastWatermark] AS (
		SELECT [SourceID], [HighWatermark] FROM (
			SELECT 
			[SourceID]
			,[HighWatermark]
			,RANK() OVER (PARTITION BY [SourceID] ORDER BY [CreatedDTS] DESC) [Rank]
			FROM [dbo].[ExtractLoadStatus]
			WHERE [HighWatermark] IS NOT NULL
			AND RawStatus = 'Success'
		) T WHERE [Rank] = 1
	)
	INSERT INTO [dbo].[ExtractLoadStatus] (
	[BatchID]
	,[SystemCode]
	,[SourceID]
	,[LowWatermark]
    ,[HighWatermark]
	,[SourceRowCount]
	,[SinkRowCount]
	,[RawPath]
	,[RawStatus]
	,[RawStartDTS]
	,[RawEndDTS]
	,[CreatedDTS])
	SELECT 
	@BatchID [BatchID]
	,@SystemCode [SystemCode]
	,S.[SourceID]
	,W.[HighWatermark] [LowWatermark] /*LOAD PREVIOUS */
	,NULL [HighWatermark]
	,NULL [SourceRowCount]
	,NULL [SinkRowCount]
	,NULL [RawPath]
	,NULL [RawStatus]
	,NULL [RawStartDTS]
	,NULL [RawEndDTS] 
	,CONVERT(DATETIME, CONVERT(DATETIMEOFFSET, GETDATE()) AT TIME ZONE 'AUS Eastern Standard Time') [CreatedDTS]
	FROM [dbo].[ExtractLoadManifest] S
	LEFT JOIN [LastWatermark] W ON W.SourceID = S.SourceID 
	WHERE SystemCode = @SystemCode
	AND S.[Enabled] = 1
END

BEGIN

	SELECT S.BatchID, S.ID, S.LowWatermark
	,R.[SourceID]
    ,R.[SystemCode]
    ,[SourceSchema]
    ,[SourceTableName]
    ,[SourceQuery]
    ,[SourceFolderPath]
    ,[SourceFileName]
    ,[SourceKeyVaultSecret]
    ,[SourceHandler]
    ,[LoadType]
    ,[BusinessKeyColumn]
    ,[WatermarkColumn]
    ,[RawHandler]
    ,R.[RawPath]
    ,[CleansedHandler] = 
        iif(
                @systemCode in (
                select ss.value
                from dbo.Config c
                cross apply string_split(value,',') ss
                where 
                        KeyGroup = 'cleansedLayer' 
                    and [key] = 'skipIngestion'
            ), 
            '',
            r.CleansedHandler
         )
    ,[CleansedPath]
    ,[DestinationSchema]
    ,[DestinationTableName]
    ,[DestinationKeyVaultSecret]
    ,JSON_MODIFY(ISNULL([ExtendedProperties], '{}'),'$.OverrideClusterName', COALESCE(JSON_VALUE([ExtendedProperties], '$.OverrideClusterName') ,C5.[Value])) [ExtendedProperties]
	,COALESCE(C2.Value, C1.Value) AS [QueryFilter]
	,@ExtraConfig AS [ExtraConfig]
    ,COALESCE(C3.[Value], C4.[Value], 0) [WorkspaceSwitch]
	FROM [dbo].[ExtractLoadStatus] S
	JOIN [dbo].[ExtractLoadManifest] R ON R.[SourceID] = S.[SourceID]
	LEFT JOIN [dbo].[Config] C1 ON C1.[KeyGroup] = R.[SystemCode] AND C1.[Key] = 'DefaultDataFilter'
	LEFT JOIN [dbo].[Config] C2 ON C2.[KeyGroup] = R.[SystemCode] AND C2.[Key] = R.[SourceTableName]
    LEFT JOIN [dbo].[Config] C3 ON C3.[KeyGroup] = 'WorkspaceSwitch' AND C3.[Key] = R.[SystemCode]
	LEFT JOIN [dbo].[Config] C4 ON C4.[KeyGroup] = 'WorkspaceSwitch' AND C4.[Key] = '_DefaultValue_'
	LEFT JOIN [dbo].[Config] C5 ON C5.[KeyGroup] = 'OverrideClusterNameDefault' AND C5.[Key] = R.[SystemCode]
	WHERE 
	S.BatchID = @BatchID
	AND S.SystemCode = @SystemCode
	AND R.[Enabled] = 1
END

END
GO


