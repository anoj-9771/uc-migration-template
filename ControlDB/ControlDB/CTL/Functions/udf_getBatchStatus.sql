--/****** Object:  UserDefinedFunction [CTL].[udf_getBatchStatus]    Script Date: 14/01/2020 11:00:35 PM ******/
--SET ANSI_NULLS ON
--GO
--SET QUOTED_IDENTIFIER ON
--GO
-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/01/2019
-- Description: Checks to see if a task is waiting on dependents
-- returns a table of project status... can be expanded to include process and task
-- =============================================

CREATE   function [CTL].[udf_getBatchStatus]()
returns table
as	
return
	with cte_TaskLog as (
		select distinct prc.ProjectId, tsk.ProcessId, tsk.TaskId,  
		(select * from [CTL].[udf_split_table](prc.ProjectDependency, ',')) ParentId,
		case when tl.Status = 'Succeeded' or qm.Status =  3 then 1.0
				when tl.Status in ('Failed', 'Incomplete') or qm.Status > 3 then 2.0 
				when tl.Status = 'Processing' or  qm.Status = 2 then 0.5  
				when qm.Status  = 1 then 0.25 
				when qm.Status = 0 then 0.125 
				else 0.0 end  TaskStatus, 
		coalesce(qm.BatchId, tl.BatchId) BatchId
		from  ctl.Task tsk
		left join ctl.QueueMeta qm on tsk.TaskId = qm.TaskId
		left join ctl.TaskLog tl on tsk.TaskId = coalesce(qm.TaskId, tl.TaskId)-- and BatchId = @BatchId
		left join ctl.Process prc on tsk.ProcessId  = prc.ProcessId
		left join ctl.Project prj on prc.ProjectId = prj.ProjectId
		where  prc.Enabled = 1 and prj.Enabled = 1
		--and qm.BatchId = @BatchId
)
select distinct 
	tlp.ProjectId, 
	avg(tlp.TaskStatus) ProjectStatus, 
	tlp.ParentId, 
	avg(tlc.TaskStatus) ParentProjectStatus
from cte_TaskLog tlp
left join cte_TaskLog tlc on tlc.ProjectId = tlp.ParentId
group by tlp.ProjectId, tlp.ParentId