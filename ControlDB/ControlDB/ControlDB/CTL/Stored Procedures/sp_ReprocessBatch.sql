-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/10/2019
-- Description: Inserts all failed or incomplete tasks from a batch run into QueueMeta for processing
--==============================================
create     proc [CTL].[sp_ReprocessBatch] @BatchId bigint
as

delete from ctl.QueueMeta where BatchId = @BatchId
begin
	with cte_FailedTasks as (
		select tl.TaskId,
			prj.Priority ProjectPriority,
			prc.Priority ProcessPriority,
			0 Status,
			@BatchId BatchId
		from ctl.TaskLog tl
		join ctl.Task tsk on tl.TaskId = tsk.TaskId
		join ctl.Process prc on tsk.ProcessId = prc.ProcessId
		join ctl.Project prj on prc.ProjectId = prj.ProjectId
		where tl.BatchId = @BatchId
		and tl.Status in ('Failed', 'Processing', 'Incomplete')
	)
	insert into QueueMeta(TaskId, ProjectPriority, ProcessPriority, Status, BatchId)
	select TaskId, ProjectPriority, ProcessPriority, Status, BatchId from cte_FailedTasks
end