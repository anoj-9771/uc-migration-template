CREATE VIEW [dbo].[vw_TechRecRawToCleansedRpt] AS
SELECT * FROM 
(
	SELECT 
		 TechRecID
		,P.ProjectName
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
		INNER JOIN CTL.ControlProjects P ON P.ProjectId = T.ProjectId
) src WHERE CurrentRecord = 1
Union
SELECT * FROM
(
	SELECT id TechRecID, 
	    replace(replace(replace(log.systemcode,'ref',''),'data',''),'|15Min','') ProjectName, 
	    log.systemcode SourceGroup, config.SourceTableName SourceLocation, 
		format(RawStartDTS,'yyyyMMddhhmmss') SourceFileDateStamp,config.DestinationTableName TargetName, id ManifestID, 
		log.CleansedSourceCount ManifestTotalNoRecords, log.CleansedSinkCount TagetTableRowCount,  
		CASE WHEN log.CleansedSourceCount = log.CleansedSinkCount THEN 'Passed' ELSE 'Failed' END AS RawToCleansedMatchStatus,
		log.RawStartDTS as StartDateTime,
		TRY_CONVERT(DATE,log.RawStartDTS) as StartDate,
		log.RawEndDTS as EndDateTime,
		TRY_CONVERT(DATE, log.RawEndDTS) as EndDate,
		CleansedStatus BatchExecutionStatus,
		CleansedStatus TaskExecutionStatus,
		ID TaskExecutionLogId,
		row_number() over (partition by log.systemcode, log.sourceid order by log.rawstartdts desc) as CurrentRecord		 
	from dbo.ExtractLoadStatus log, dbo.ExtractLoadManifest config 
   where config.sourceid = log.SourceID
	 and CHARINDEX(config.systemCode, log.RawPath) > 0
) srcMDP  WHERE srcMDP.CurrentRecord = 1


GO


