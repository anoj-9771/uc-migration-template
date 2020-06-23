-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/01/2019
-- Description: Checks to see if a task is waiting on dependents
-- returns a table of task, process and project completion status
-- =============================================

create           function [CTL].[udf_getQueueStatus]()
returns table
as	
return
	with cte_TaskLog as (
		select distinct tsk.ProcessId, tsk.TaskId, BatchId, 
		case when qm.Status =  3 then 1.0
				when qm.Status > 3 then 2.0 
				when qm.Status = 2 then 0.5  
				when qm.Status  = 1 then 0.25 
				when qm.Status = 0 then 0.125 
				else 2 end  TaskStatus
		from ctl.QueueMeta qm
		join  ctl.Task tsk on tsk.TaskId = qm.TaskId 
		where qm.Status is not null
	), cte_ProcessLog as(
		select prc.ProjectId, 
			tl.ProcessId, 
			avg(case when TaskStatus > 0.5 then 1 else TaskStatus end) ProcessStatus,
			tl.BatchId
		from cte_TaskLog tl
		join ctl.Process prc on tl.ProcessId = prc.ProcessId
		group by ProjectId, tl.ProcessId, tl.BatchId
	), cte_ProjectLog as(
		select ProjectId, 
			avg(ProcessStatus) ProjectStatus,
			BatchId
		from cte_ProcessLog
		group by ProjectId, BatchId
	)
	select tl.TaskId, 
		TaskStatus, 
		prc.ProcessId, 
		ProcessStatus, 
		prc.ProjectId, 
		ProjectStatus,
		tl.BatchId
	from cte_TaskLog tl
	join cte_ProcessLog prc on tl.ProcessId = prc.ProcessId
	join cte_ProjectLog prj on prc.ProjectId = prj.ProjectId