CREATE     PROC [CTL].[DeleteProjectConfig]
@projectId int
AS

Delete From [CTL].[ControlTaskCommand]
where ControlTaskId in (select TaskId from CTL.ControlTasks where ProjectId = @projectId)

Delete From [CTL].[TaskExecutionLog]
where ControlTaskId in (select TaskId from CTL.ControlTasks where ProjectId = @projectId)

Delete From [CTL].[BatchExecutionLog]
Where ProjectID = @projectId


Drop table if exists #tempSource
Select distinct SourceId 
Into #tempSource
From CTL.ControlSource
where SourceId in (select SourceId from CTL.ControlTasks Where ProjectId = @projectId)

Drop table if exists #tempTarget
Select distinct TargetId 
Into #tempTarget
From CTL.ControlTarget
where TargetId in (select TargetId from CTL.ControlTasks Where ProjectId = @projectId)

Delete From [CTL].[ControlWatermark]
where ControlSourceId in (select SourceId from #tempSource)

Delete From [CTL].[ControlTasks]
where SourceId in (select SourceId from #tempSource)
			
Delete From [CTL].[ControlSource]
where SourceId in (select SourceId from #tempSource)
			
Delete From [CTL].[ControlTarget]
where TargetId in (select TargetId from #tempTarget)

Drop table if exists #tempBatch
Drop table if exists #tempSource
Drop table if exists #tempTarget