




--[CTL].[getExecutionTasks] 1,1

CREATE Procedure [CTL].[GetExecutionTasks] (@StageId BigInt, @ProjectId BigInt)
As

With allTasks as
(
Select 
	styp.ControlType SourceType, 
	src.SourceServer, 
	src.SourceGroup,
	src.SourceName, 
	src.SourceLocation, 
	src.AdditionalProperty,
	src.Processor, 
	src.IsAuditTable,
	ISNULL(Audit.SourceName, '') AS SoftDeleteSource,
	p.ProjectName,
	p.ProjectId,
	ttyp.ControlType TargetType, 
	targ.TargetName, 
	targ.TargetLocation, 
	targ.TargetServer, 
--	targ.Compressed,
    ct.DataLoadMode, 
	DLT.DeltaExtract,
	DLT.CDCSource,
	DLT.TruncateTarget,
	DLT.UpsertTarget,
	DLT.AppendTarget,
	CT.TrackChanges,
	CT.LoadToSqlEDW,
	ct.TaskName, 
	ct.ControlStageId, 
	--cps.CronExpression,  
	ct.TaskId, 
	cs.StageSequence, 
	cs.StageName, 
	src.SourceId, 
	ct.TargetId,
	ct.ObjectGrain, 
	ctc.CommandTypeId, 
	CASE WHEN ct.DataLoadMode IN ('FULL-EXTRACT', 'TRUNCATE-LOAD') THEN '' ELSE CW.Watermarks END AS Watermarks,
	FORMAT(TRY_CONVERT(DATETIME, CW.Watermarks), 'yyyy-MM-ddTHH:mm:ss') WatermarksDT,
	ISNULL(CW.SourceColumn, '') AS WatermarkColumn,
	src.BusinessKeyColumn,
	src.PartitionColumn, --useful for dynamic partitioning of SQL source data while copying to Delta Table in ADF's Copy Activity
	ct.UpdateMetaData,
	src.SourceTimeStampFormat,
	CASE 
	    WHEN (styp.ControlType IN ('SQL Server', 'Oracle', 'MySQL')) AND DLT.DeltaExtract = 1 Then CTL.[udf_GetDeltaSQL](ct.TaskId)
		ELSE ctc.Command
	END Command,
	coalesce(tel.StartTime, '01.01.2000') as LastSuccessfulExecutionTs,
	CTL.[udf_GetLastLoadedFile](src.SourceName, src.SourceLocation) LastLoadedFile

  From CTL.ControlSource src
    Join CTL.ControlTypes styp On src.SourceTypeId = styp.TypeId
    Join CTL.ControlTasks ct On src.SourceId = ct.SourceId
	Join CTL.ControlTaskCommand ctc On ct.TaskId = ctc.ControlTaskId
	Join CTL.ControlStages cs On ct.ControlStageId = cs.ControlStageId
	Join CTL.ControlTarget targ On ct.TargetId = targ.TargetId
	Join CTL.ControlTypes ttyp On targ.TargetTypeId = ttyp.TypeId
	LEFT JOIN CTL.ControlProjects P ON ct.ProjectId = p.ProjectId
	LEFT JOIN CTL.ControlWatermark CW ON SRC.SourceId = CW.ControlSourceId
	LEFT JOIN CTL.ControlDataLoadTypes DLT ON CT.DataLoadMode = DLT.DataLoadType
	LEFT JOIN CTL.ControlSource Audit ON ISNULL(src.SoftDeleteSource, '') = Audit.SourceLocation AND Audit.IsAuditTable = 1
	LEFT JOIN CTL.TaskExecutionLog tel On ct.TaskId = tel.ControlTaskId
		   And tel.StartTime = (Select Max(StartTime) From CTL.TaskExecutionLog Where ControlTaskId = ct.TaskId and ExecutionStatus in ('Success'))
 Where ct.TaskEnabled = 1
   And ct.ControlStageId = @StageId
   And ct.ProjectId = @ProjectId
),
lastFullExecutions As
(
Select ct.TaskId, Max(tel.EndTime) LastExecutionDate
  From CTL.ControlSource src
    Join CTL.ControlTasks ct On src.SourceId = ct.SourceId 
	Join CTL.TaskExecutionLog tel On ct.TaskId = tel.ControlTaskId
 Where ct.TaskEnabled = 1
   And ct.ControlStageId = @StageId  
   And ct.ProjectId = @ProjectId
   And tel.ExecutionStatus In ('Success', 'Failure', 'Terminated')
 Group By ct.TaskId
),
noExecutions As
(
Select ct.TaskId
  From CTL.ControlSource src
    Join CTL.ControlTasks ct On src.SourceId = ct.SourceId
	Left Join CTL.TaskExecutionLog tel On ct.TaskId = tel.ControlTaskId
 Where ct.TaskEnabled = 1
   And ct.ControlStageId = @StageId
   And ct.ProjectId = @ProjectId
   And tel.ControlTaskId Is Null
)

SELECT * FROM allTasks t
 WHERE (
	TaskId In (Select TaskId From noExecutions)
	OR t.TaskId In (Select ct.TaskId From CTL.ControlTasks ct Join lastFullExecutions ex On ct.TaskId = ex.TaskId)
	)
 ORDER BY t.StageSequence, t.TaskId