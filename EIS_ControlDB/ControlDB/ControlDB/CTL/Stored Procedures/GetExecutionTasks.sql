




--[CTL].[getExecutionTasks] 1,1

CREATE Procedure [CTL].[GetExecutionTasks] (@StageId BigInt, @ProjectId BigInt)
As

With allTasks as
(
Select 
	styp.ControlType SourceType, 
	src.SourceServer, 
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
	CW.Watermarks,
	FORMAT(TRY_CONVERT(DATETIME, CW.Watermarks), 'yyyy-MM-ddTHH:mm:ss') WatermarksDT,
	ISNULL(CW.SourceColumn, '') AS WatermarkColumn,
	src.BusinessKeyColumn,
	Case 
	    When (ctc.CommandTypeId = 1 OR ctc.CommandTypeId = 6) AND DLT.DeltaExtract = 1
		Then CTL.[udf_GetDeltaSQL](ct.TaskId)
		Else ctc.Command
	End Command,
	CTL.[udf_GetLastLoadedFile](src.SourceName, src.SourceLocation) LastLoadedFile

  From CTL.ControlSource src
    Join CTL.ControlTypes styp On src.SourceTypeId = styp.TypeId
    Join CTL.ControlTasks ct On src.SourceId = ct.SourceId
	Join CTL.ControlTaskCommand ctc On ct.TaskId = ctc.ControlTaskId
	Join CTL.ControlStages cs On ct.ControlStageId = cs.ControlStageId
	Join CTL.ControlTarget targ On ct.TargetId = targ.TargetId
	Join CTL.ControlTypes ttyp On targ.TargetTypeId = ttyp.TypeId
	LEFT JOIN CTL.ControlProjects P ON ct.ProjectId = p.ProjectId
	--join [CTL].[ControlProjectSchedule] cps on ct.ProjectId = cps.ControlProjectId
	LEFT JOIN CTL.ControlWatermark CW ON SRC.SourceId = CW.ControlSourceId
	LEFT JOIN CTL.ControlDataLoadTypes DLT ON CT.DataLoadMode = DLT.DataLoadType
	LEFT JOIN CTL.ControlSource Audit ON ISNULL(src.SoftDeleteSource, '') = Audit.SourceLocation AND Audit.IsAuditTable = 1
	LEFT JOIN CTL.TaskExecutionLog tel On ct.TaskId = tel.ControlTaskId
		   And tel.StartTime = (Select Max(StartTime) From CTL.TaskExecutionLog Where ControlTaskId = ct.TaskId)
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

Select *
  From allTasks t
 Where (TaskId In (Select TaskId From noExecutions)
   Or t.TaskId In (Select ct.TaskId 
                   From CTL.ControlTasks ct
				     Join lastFullExecutions ex
					   On ct.TaskId = ex.TaskId))
				  --Where (Case 
				  --        When ct.RunFrequencyTypeId = 1 and DATEDIFF(Hour,ex.lastExecutionDate, getdate()) > 24 --Daily
						--    then 1
						--  When ct.RunFrequencyTypeId = 2 and DATEDIFF(Hour, ex.lastExecutionDate, getdate()) > 1 --Hourly
						--    then 1
						--  When ct.RunFrequencyTypeId = 3 and DATEDIFF(day, ex.lastExecutionDate, getdate()) > 7 --Annualy
						--    then 1
						--  Else 0
						--End) = 1))
 Order BY t.StageSequence, t.TaskId