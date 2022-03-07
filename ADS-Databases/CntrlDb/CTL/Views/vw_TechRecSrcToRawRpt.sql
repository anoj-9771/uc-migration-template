CREATE VIEW [dbo].[vw_TechRecSrcToRawRpt] AS
SELECT * FROM
(
	SELECT 
		 ManifestID
		,S.SourceGroup
		,S.SourceLocation
		,SourceFileDateStamp
		,M_DeltaRecordCount AS ManifestDeltaRecordCount
		,RecordCountDeltaTable AS RecordCount_ReadFromDataFile
		,RecordCountTargetTable AS RecordCount_SavedToDeltaTable
		,CASE WHEN M.M_DeltaRecordCount = M.RecordCountTargetTable THEN 'Passed' ELSE 'Failed' END AS SrcToRawMatchStatus
		,TL.StartTime AS StartDateTime
		,TRY_CONVERT(DATE, TL.StartTime) AS StartDate
		,TL.EndTime AS EndDateTime
		,TRY_CONVERT(DATE, TL.EndTime) AS EndDate
		,B.BatchExecutionStatus
		,TL.ExecutionStatus AS TaskExecutionStatus
		,ROW_NUMBER() over (partition by S.SourceLocation, SourceFileDateStamp order by EndTime desc) as ValidRecord
	FROM [CTL].[ControlManifest] M
		INNER JOIN CTL.BatchExecutionLog B ON M.BatchExecutionLogID = B.BatchExecutionLogId
		INNER JOIN CTL.TaskExecutionLog TL ON M.TaskExecutionLogID = TL.ExecutionLogId
		INNER JOIN CTL.ControlTasks T ON TL.ControlTaskId = T.TaskId
		INNER JOIN CTL.ControlSource S ON T.SourceId = S.SourceId
		INNER JOIN CTL.ControlTypes ST ON S.SourceTypeId = ST.TypeId
	WHERE ST.ControlType = 'BLOB Storage (json)'
) src WHERE src.ValidRecord = 1
