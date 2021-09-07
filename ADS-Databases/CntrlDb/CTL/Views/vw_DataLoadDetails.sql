
CREATE VIEW CTL.vw_DataLoadDetails
AS
SELECT 
M.BatchExecutionLogID
,M.TaskExecutionLogID
,M.SourceObject
,M.StartCounter
,M.EndCounter
,M.RecordCountLoaded
,M.RecordCountDeltaTable
,M.FolderName
,M.FileName
,M.ProcessedToCleansedZone
,M.DeltaColumn
,M.ProcessedToSQLEDW
,M.RawZonePipelineRunID
,M.CleansedZonePipelineRunID
,M.SQLEDWPipelineRunID
,B.StartDate as BatchStart
,B.EndDate AS BatchEnd
,B.BatchExecutionStatus
,B.ProjectID
,T.ControlTaskId
,T.StartTime TaskStart
,T.EndTime AS TaskEnd
,DATEDIFF(MINUTE, T.StartTime, T.EndTime) AS TaskDuration
,T.ExecutionStatus
FROM CTL.ControlManifest M
LEFT JOIN CTL.BatchExecutionLog B ON M.BatchExecutionLogID = B.BatchExecutionLogId
LEFT JOIN CTL.TaskExecutionLog T ON M.TaskExecutionLogID = T.ExecutionLogId