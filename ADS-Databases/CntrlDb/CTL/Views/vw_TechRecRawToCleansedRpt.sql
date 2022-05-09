CREATE VIEW [CTL].[vw_TechRecRawToCleansedRpt] AS
SELECT * FROM 
(
	SELECT 
		 TechRecID
		,S.SourceGroup
		,S.SourceLocation	
		,SourceFileDateStamp
		,TargetName
		,ManifestId
		,ManifestTotalNoRecords
		,TargetTableRowCount
		,CASE WHEN ManifestTotalNoRecords = TargetTableRowCount THEN 'Passed' ELSE 'Failed' END AS RawToCleansedMatchStatus
		,TL.StartTime AS StartDateTime
		,TRY_CONVERT(DATE, TL.StartTime) AS StartDate
		,TL.EndTime AS EndDateTime
		,TRY_CONVERT(DATE, TL.EndTime) AS EndDate
		,BL.BatchExecutionStatus
		,TL.ExecutionStatus AS TaskExecutionStatus
		 ,TR.TaskExecutionLogId
		,ROW_NUMBER() over (partition by S.SourceLocation, SourceFileDateStamp order by EndTime desc, TechRecID desc) as CurrentRecord
	FROM [CTL].TechRecRawToCleansed TR
		INNER JOIN CTL.ControlTasks T ON TR.TaskId = T.TaskId
		INNER JOIN CTL.ControlSource S ON T.SourceId = S.SourceId
		INNER JOIN CTL.TaskExecutionLog TL ON TR.TaskExecutionLogId = TL.ExecutionLogId
		INNER JOIN CTL.BatchExecutionLog BL ON TR.BatchExecutionId = BL.BatchExecutionLogId
		INNER JOIN CTL.TechRecCleansedConfig TRC ON TRC.TargetObject = TR.TargetName and TRC.TechRecDashboardReady = 'Y'
) src WHERE CurrentRecord = 1

