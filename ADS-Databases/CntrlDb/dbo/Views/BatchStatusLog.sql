CREATE VIEW [dbo].[BatchStatusLog] AS
/* ================ LOGS ================ */
WITH [_Log] AS (
	SELECT 
	[ID]
	,[ExtractLoadStatusID]
	,NULL [PipelineRunID]
	,[ActivityType]
	,REPLACE(REPLACE([Message], CHAR(13), ''), CHAR(10), '') [Message]
	,RANK() OVER (PARTITION BY [ExtractLoadStatusID] ORDER BY ID DESC) [Rank]
	,[CreatedDTS]
	FROM [dbo].[Log] (NOLOCK)
    where isjson([Message]) = 1
),[_LogCopyTask] AS (
	SELECT * 
	,FORMAT((CAST(JSON_VALUE([Message],'$.Output.dataRead') AS DECIMAL(16, 2)) / (1024 ^ 2)), '0.00') [DataRead]
	,FORMAT((CAST(JSON_VALUE([Message],'$.Output.dataWritten') AS DECIMAL(16, 2)) / (1024 ^ 2)), '0.00') [DataWritten]
	,FORMAT((CAST(JSON_VALUE([Message],'$.Output.rowsRead') AS INT)), '') [RowsRead]
	,FORMAT((CAST(JSON_VALUE([Message],'$.Output.rowsCopied') AS INT)), '') [RowsCopied]
	,FORMAT((CAST(JSON_VALUE([Message],'$.Output.copyDuration') AS INT)), '') [CopyDuration]
	,FORMAT((CAST(JSON_VALUE([Message],'$.Output.throughput') AS DECIMAL(16, 2))), '0.00') [Throughput]
	,RANK() OVER (PARTITION BY [ExtractLoadStatusID] ORDER BY ID) [Rank]
	FROM [dbo].[Log] (NOLOCK)
	WHERE [ActivityType] IN ('copy-data')
),[_LogParsedRank] AS (
	SELECT 	* 
	,JSON_VALUE([Message],'$.PipelineRunId') [JsonPipelineRunId]
	,JSON_VALUE([Message],'$.Status') [Status]
	,COALESCE(JSON_VALUE([Message],'$.Output.errors[0].Message'), JSON_VALUE([Message],'$.Error.message'), JSON_VALUE([Message],'$.Error')) [Error]
	,COUNT(*) OVER (PARTITION BY [ExtractLoadStatusID]) [Logs]
	FROM [_Log] 
/* ================ TASK ================ */
),[_RawTask] AS (
	SELECT 
	[ID]
	,[BatchID]
	,B.[SystemCode]
	,B.[SourceID]
	,[SourceSchema]
	,[SourceTableName]
	,[SourceRowCount]
	,[SinkRowCount]
	--,B.[RawPath]
	/* --------------- Raw --------------- */
	,IIF([RawStartDTS] IS NOT NULL AND [RawEndDTS] IS NULL, 'In Progress', [RawStatus]) [RawStatus]
	,[RawStartDTS]
	,[RawEndDTS]
	,CAST(ISNULL([RawEndDTS], CONVERT(DATETIME, CONVERT(DATETIMEOFFSET, GETDATE()) AT TIME ZONE 'AUS Eastern Standard Time'))-[RawStartDTS] AS TIME) [RawDuration]
	/* --------------- Cleansed --------------- */
	,IIF([CleansedStartDTS] IS NOT NULL AND [CleansedEndDTS] IS NULL, 'In Progress', [CleansedStatus]) [CleansedStatus]
	,[CleansedStartDTS]
	,[CleansedEndDTS]
	,CAST(ISNULL([CleansedEndDTS], CONVERT(DATETIME, CONVERT(DATETIMEOFFSET, GETDATE()) AT TIME ZONE 'AUS Eastern Standard Time'))-[CleansedStartDTS] AS TIME) [CleansedDuration]
	,B.[CreatedDTS]
	,[EndedDTS]
	FROM [dbo].[ExtractLoadStatus] (NOLOCK) B
	JOIN [dbo].[ExtractLoadManifest] (NOLOCK) S ON S.SourceID = B.SourceID
),[_TaskCurrentStage] AS (
	SELECT *
	,CAST(ISNULL(IIF([CleansedStatus] IS NOT NULL, [CleansedEndDTS], [RawEndDTS]), CONVERT(DATETIME, CONVERT(DATETIMEOFFSET, GETDATE()) AT TIME ZONE 'AUS Eastern Standard Time'))-[RawStartDTS] AS TIME) [TotalDuration]
	,DENSE_RANK() OVER (PARTITION BY [BatchID], [SystemCode] ORDER BY [RawStartDTS]) [RawStartRank]
	,DENSE_RANK() OVER (PARTITION BY [BatchID], [SystemCode] ORDER BY [RawEndDTS]) [RawEndRank]
	,DENSE_RANK() OVER (PARTITION BY [BatchID], [SystemCode] ORDER BY [CleansedStartDTS]) [CleansedStartRank]
	,DENSE_RANK() OVER (PARTITION BY [BatchID], [SystemCode] ORDER BY [CleansedEndDTS]) [CleansedEndRank]
	,IIF([RawStartDTS] IS NOT NULL AND [RawEndDTS] IS NULL, 1, 0) [RawStage]
	,IIF([CleansedStartDTS] IS NOT NULL AND [CleansedEndDTS] IS NULL, 1, 0) [CleansedStage]
	,CASE 
		WHEN [RawStatus] = 'In Progress' THEN 'Raw'
		WHEN [CleansedStatus] = 'In Progress' THEN 'Cleansed'
	ELSE NULL END [CurrentStage]
	FROM[_RawTask]
),[_TaskLogic] AS (
	SELECT *
	,IIF([CurrentStage] IS NULL AND ([CleansedStatus] IS NULL OR [RawStatus] IS NULL), 1, 0) [Pending]
	,IIF([CurrentStage] IS NOT NULL, 1, 0) [InProgress]
	,IIF([CleansedStatus]='Success' AND [RawStatus]='Success', 1, 0) [Success]
	,IIF([CleansedStatus]='Fail' OR [RawStatus]='Fail', 1, 0) [Fail]
	FROM [_TaskCurrentStage]
/* ================ SYSTEM ================ */
),[_System] AS (
	SELECT *
	,SUM([Pending]) OVER (PARTITION BY [BatchID], [SystemCode]) [SystemPendingTasks]
	,SUM([InProgress]) OVER (PARTITION BY [BatchID], [SystemCode]) [SystemInProgressTasks]
	,SUM([Success]) OVER (PARTITION BY [BatchID], [SystemCode]) [SystemSuccessTasks]
	,SUM([Fail]) OVER (PARTITION BY [BatchID], [SystemCode]) [SystemFailTasks]
	FROM [_TaskLogic]
),[_SystemEnd] AS (
	SELECT *
	,SUM(COALESCE([SystemSuccessTasks], [SystemFailTasks])) OVER (PARTITION BY [BatchID], [SystemCode], [SourceID]) [SystemCompletedTasks]
	,COUNT([ID]) OVER (PARTITION BY [BatchID], [SystemCode]) [SystemTotalTasks]
	,MIN([CreatedDTS]) OVER (PARTITION BY [BatchID], [SystemCode]) [SystemStartDTS]
	,MAX([EndedDTS]) OVER (PARTITION BY [BatchID], [SystemCode]) [SystemEndDTS]
	FROM [_System]
),[_SystemDuration] AS (
	SELECT *
	,CAST(ISNULL([SystemEndDTS], CONVERT(DATETIME, CONVERT(DATETIMEOFFSET, GETDATE()) AT TIME ZONE 'AUS Eastern Standard Time'))-[SystemStartDTS] AS TIME) [SystemDuration]
	-- ,IIF(([SystemCompletedTasks]+[SystemFailTasks])=[SystemTotalTasks] AND [SystemEndDTS] IS NOT NULL, 'Completed', 'In Progress') [SystemStatus]
	,IIF([SystemEndDTS] IS NOT NULL, 'Completed', 'In Progress') [SystemStatus]
	FROM [_SystemEnd]
/* ================ BATCH ================ */
),[_Batch] AS (
	SELECT *
	,SUM([Pending]) OVER (PARTITION BY [BatchID]) [BatchPendingTasks]
	,SUM([InProgress]) OVER (PARTITION BY [BatchID]) [BatchInProgressTasks]
	,SUM([Success]) OVER (PARTITION BY [BatchID]) [BatchSuccessTasks]
	,SUM([Fail]) OVER (PARTITION BY [BatchID])  [BatchFailTasks]
	FROM [_SystemDuration]
),[_BatchEnd] AS (
	SELECT *
	,SUM([Success] + [Fail]) OVER (PARTITION BY [BatchID]) [BatchCompletedTasks]
	,COUNT([ID]) OVER (PARTITION BY [BatchID]) [BatchTotalTasks]
	,MIN([CreatedDTS]) OVER (PARTITION BY [BatchID]) [BatchStartDTS]
	,MAX([EndedDTS]) OVER (PARTITION BY [BatchID]) [BatchEndDTS]
	FROM [_Batch]
),[_BatchDuration] AS (
	SELECT *
	,CAST(ISNULL([BatchEndDTS], CONVERT(DATETIME, CONVERT(DATETIMEOFFSET, GETDATE()) AT TIME ZONE 'AUS Eastern Standard Time'))-[BatchStartDTS] AS TIME) [BatchDuration]
	,IIF([BatchCompletedTasks]=[BatchTotalTasks] AND [BatchEndDTS] IS NOT NULL, 'Completed', 'In Progress') [BatchStatus]
	,DENSE_RANK() OVER (PARTITION BY [BatchID] ORDER BY [CleansedEndRank]) [BatchTaskRank]
	,DENSE_RANK() OVER (PARTITION BY [BatchID], [SystemCode] ORDER BY [SystemEndDTS]) [BatchSystemRank]
	,DENSE_RANK() OVER (PARTITION BY NULL ORDER BY [BatchEndDTS] DESC) [BatchRank]
	FROM [_BatchEnd]
)
SELECT 
B.*
,[Logs]
,C.[DataRead]
,C.[DataWritten]
,L.[Error]
,IIF([BatchRank]=1, 1, 0) [LatestBatch]

FROM [_BatchDuration] B
LEFT JOIN [_LogParsedRank] L ON L.[ExtractLoadStatusID] = B.[ID] AND L.[Rank] = 1
LEFT JOIN [_LogCopyTask] C ON C.[ExtractLoadStatusID] = B.[ID] AND C.[Rank] = 1
GO