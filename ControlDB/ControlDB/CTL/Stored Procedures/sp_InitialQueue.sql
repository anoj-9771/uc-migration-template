-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/10/2019
-- Description: Stored proc to place new items in the queue from a given Project
-- Update proc to support cleanup for any tasks that have been running for over 4 hrs
--==============================================
CREATE       procedure [CTL].[sp_InitialQueue] @ProjectId bigint, @BatchId bigint
AS
/*Check Queue for existing tasks and add task to queue if it does not exist or status = 3
--and delete if > 2 */
--declare @ProjectId bigint = 6
--declare @BatchId bigint = 20200718232301
begin
	-- Get Task in initial Project
	with cte_Q as (
		select tsk.TaskId, prj.Priority pjPriority, prc.Priority pcPriority, 0 Status, prj.ProjectId
		from ctl.Task tsk
		join ctl.Process prc on tsk.ProcessId = prc.ProcessId
		join ctl.Project prj on prc.ProjectId = prj.ProjectId
		where  1 = 1
		and prj.ProjectId = @ProjectId
		and tsk.Rank= 1
		and prj.Enabled = 1
		and prc.Enabled = 1
	), 
	-- Get dependent Projects tasks
	cte_Prc as (
		select tsk.TaskId, prc.Priority prcPriority, prj.Priority prjPriority, 0 Status, prc.ProjectId
		from ctl.Task tsk
		join ctl.Process prc on tsk.ProcessId = prc.ProcessId
		join ctl.Project prj on prc.ProjectId = prj.ProjectId
		where 1 = 1
		and prc.ProjectId in (select Dependents from [CTL].[udf_getProjectDependents](@ProjectId))
		and tsk.Rank= 1
	)

	--select * from 
	--cte_Prc
	----cte_Q
	--end
	-- Insert Rank 1 Project Tasks and successor Project Dependency tasks into queue
	insert into ctl.QueueMeta 
	select TaskId, pjPriority, pcPriority, 0, @BatchId
	from (
		select * from cte_Q
		union all 
		select * from cte_Prc
	) a
	where hashbytes('sha2_256', concat_ws('|',a.TaskId, @BatchId)) not in (select hashbytes('sha2_256', concat_ws('|',TaskId, BatchId)) from ctl.QueueMeta where Status >= 0)
end