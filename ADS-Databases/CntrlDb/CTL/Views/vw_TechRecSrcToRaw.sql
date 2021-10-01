CREATE VIEW CTL.vw_TechRecSrcToRaw
AS
SELECT [ManifestID]
	,S.SourceGroup
    ,S.SourceLocation
	,[SourceFileDateStamp]
	,[M_DeltaRecordCount] AS [ManifestDeltaRecordCount]
	,[RecordCountDeltaTable] AS RecordCount_ReadFromDataFile
	,[RecordCountTargetTable] AS RecordCount_SavedToDeltaTable
	,CASE WHEN M.M_DeltaRecordCount = M.[RecordCountTargetTable] THEN 'Passed' ELSE 'Failed' END AS SrcToRawMatchStatus
	,TL.StartTime
	,TL.EndTime
	,B.BatchExecutionStatus
	,TL.ExecutionStatus AS TaskExecutionStatus
FROM [CTL].[ControlManifest] M
	INNER JOIN CTL.BatchExecutionLog B ON M.BatchExecutionLogID = B.BatchExecutionLogId
	INNER JOIN CTL.TaskExecutionLog TL ON M.TaskExecutionLogID = TL.ExecutionLogId
	INNER JOIN CTL.ControlTasks T ON TL.ControlTaskId = T.TaskId
	INNER JOIN CTL.ControlSource S ON T.SourceId = S.SourceId
	INNER JOIN CTL.ControlTypes ST ON S.SourceTypeId = ST.TypeId
WHERE ST.ControlType = 'BLOB Storage (json)'