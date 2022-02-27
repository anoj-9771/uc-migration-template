










CREATE VIEW [CTL].[vw_ControlConfiguration]
AS
SELECT 
	TSK.TaskName 
	,PRJ.ProjectName
	,PRJ.RunSequence
	,CPS.TriggerName
	,SRC.SourceGroup
	,TSK.DataLoadMode
	,SRC.BusinessKeyColumn 
	,TYP.ControlType
	,TSK.ControlStageId
	,TSK.TrackChanges
	,SRC.SourceLocation
	,TGT.TargetLocation
	,TSK.TaskEnabled
	,SRC.SourceServer
	,SRC.AdditionalProperty
	,CTC.Command
	,COALESCE(WMK.SourceColumn, '') AS WatermarkColumn
	,COALESCE(WMK.Watermarks, '') AS Watermarks
	,SRC.Processor
	,SRC.SoftDeleteSource
	,SRC.IsAuditTable
	,TSK.TaskId
	,SRC.SourceId
	,TGT.TargetId
	,PRJ.ProjectId
	,CTC.CommandId
	,SRC.SourceTimeStampFormat
	,COALESCE(AUD.SourceName, '') AS AuditTask
FROM CTL.ControlTasks TSK
INNER JOIN CTL.ControlSource SRC ON TSK.SourceId = SRC.SourceId
INNER JOIN CTL.ControlTarget TGT ON TSK.TargetId = TGT.TargetId
INNER JOIN CTL.ControlProjects PRJ ON TSK.ProjectId = PRJ.ProjectId
INNER JOIN CTL.ControlProjectSchedule CPS ON PRJ.ProjectId = CPS.ControlProjectId
INNER JOIN CTL.ControlTypes TYP ON SRC.SourceTypeId = TYP.TypeId
LEFT JOIN CTL.ControlTaskCommand CTC ON TSK.TaskId = CTC.ControlTaskId
LEFT JOIN CTL.ControlWatermark WMK ON WMK.ControlSourceId = SRC.SourceId
LEFT JOIN CTL.ControlSource AUD ON SRC.SoftDeleteSource = AUD.SourceLocation