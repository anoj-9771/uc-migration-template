





CREATE VIEW [CTL].[vw_ControlConfiguration]
AS
SELECT 
	TaskName 
	,ProjectName
	,DataLoadMode
	,BusinessKeyColumn 
	,TYPES.ControlType
	,TASKS.ControlStageId
	,TrackChanges
	,SourceLocation
	,TargetLocation
	,TaskEnabled
	,S.SourceServer
	,AdditionalProperty
	,Command
	,W.SourceColumn
	,W.Watermarks
	,S.Processor
	,S.SoftDeleteSource
	,S.IsAuditTable
	,TASKS.TaskId
	,S.SourceId
	,TGT.TargetId
	,P.ProjectId
FROM CTL.ControlTasks TASKS
INNER JOIN CTL.ControlSource S ON TASKS.SourceId = S.SourceId
INNER JOIN CTL.ControlTarget TGT ON TASKS.TargetId = TGT.TargetId
INNER JOIN CTL.ControlProjects P ON TASKS.ProjectId = P.ProjectId
INNER JOIN CTL.ControlTypes TYPES ON S.SourceTypeId = TYPES.TypeId
LEFT JOIN CTL.ControlTaskCommand CTC ON Tasks.TaskId = CTC.ControlTaskId
LEFT JOIN CTL.ControlWatermark W ON W.ControlSourceId = S.SourceId