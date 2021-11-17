CREATE VIEW [CTL].[vw_TechRecRawToCleansed]
AS 
SELECT [TechRecID]
	,S.SourceGroup
	,S.SourceLocation	
	,[SourceFileDateStamp]
	,[TargetName]
	,ManifestId
	,[ManifestTotalNoRecords]
	,[TargetTableRowCount]
	,CASE WHEN ManifestTotalNoRecords = TargetTableRowCount THEN 'Passed' ELSE 'Failed' END AS TechRecRawToCleansed
	,TL.StartTime
	,TL.EndTime
	,BL.BatchExecutionStatus
	,TL.ExecutionStatus AS TaskExecutionStatus
FROM [CTL].TechRecRawToCleansed TR
	INNER JOIN CTL.ControlTasks T ON TR.TaskId = T.TaskId
	INNER JOIN CTL.ControlSource S ON T.SourceId = S.SourceId
	INNER JOIN CTL.TaskExecutionLog TL ON TR.TaskExecutionLogId = TL.ExecutionLogId
	INNER JOIN CTL.BatchExecutionLog BL ON TR.BatchExecutionId = BL.BatchExecutionLogId
