-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/10/2019
-- Description: Stored proc to place new items in the queue from a given Project
-- Update proc to support cleanup for any tasks that have been running for over 4 hrs
--==============================================
CREATE       procedure [CTL].[sp_InitialQueue] @ProjectId bigint, @BatchId bigint
AS
/*Check Queue for existing tasks and add task to queue if it does not exist or status = 3
and delete if > 2 */
--declare @BatchId bigint = (select convert(bigint, replace([CTL].[udf_getFileDateHierarchy]('Second',@ExecTime), '/','')))
begin
	with cte_Q as (
		select t.TaskId, pj.Priority pjPriority, pc.Priority pcPriority, 0 Status
		from ctl.Task t
		join ctl.Process pc on t.ProcessId = pc.ProcessId
		join ctl.Project pj on pc.ProjectId = pj.ProjectId
		where  pj.ProjectId = @ProjectId
		and t.Rank= 1
		and pj.Enabled = 1
		and pc.Enabled = 1
	), cte_MQ as (
		select TaskId, ProjectPriority, ProcessPriority, Status, BatchId
		from ctl.QueueMeta
		where Status >= 0
	)
	
		insert into ctl.QueueMeta 
		select q.TaskId, pjPriority, pcPriority, 0, @BatchId
		from cte_Q q
		left join cte_MQ mq on q.TaskId = mq.TaskId
		where mq.TaskId is null
end
--begin
--	declare @dupeCount int
--	declare @prcTable table (TaskId bigint, QueueId bigint, BatchId bigint)
--	declare @prcLog table (TaskId bigint, QueueId bigint, BatchId bigint, Status varchar(50));
--	with cte_ET as (
--			select row_number()over(partition by TaskId order by batchId desc ) RowNum, QueueId, TaskId, BatchId
--			from  ctl.QueueMeta 
--		)
--	insert into @prcTable
--	select TaskId, QueueId, BatchId 
--	from cte_ET where RowNum > 1;

--	with cte_TL as (
--		select tl.TaskId, qm.QueueID, tl.BatchId, tl.Status
--		from ctl.TaskLog tl
--		join ctl.QueueMeta qm on tl.TaskId = qm.TaskId 
--		and tl.BatchId = qm.BatchId
--		where datediff(hour, [CTL].[udf_getWAustDateTime](getdate()), tl.InitialLogTime) >= 3 
--		and tl.Status = 'Processing'
--	)
--	insert into @prcLog
--	select TaskId, QueueId, BatchId, Status
--	from cte_TL
----Perform Queue Cleanup 
--	--Update Incomplete tasks to Incomplete where task = inProgress and new task is executed
--	select @dupeCount = count(*) from @prcTable
--	if @dupeCount > 0
--	begin
--		update ctl.TaskLog 
--		set Status = 'Incomplete' 
--		where TaskId in (select TaskId from @prcTable)
--		and BatchId in (select BatchId from @prcTable)
--	end
--	begin
--		delete from ctl.QueueMeta 
--		--select * from ctl.QueueMeta 
--		where QueueId in (select QueueId from @prcTable)
--	end
--	--Update Tasks that have been running for over 3 hrs
--	begin
--		update ctl.TaskLog
--		set Status = 'Incomplete'
--		where TaskId in (select TaskId from @prcLog)
--		and BatchId in (select BatchId from @prcLog)
--	end
--	begin
--		delete from ctl.QueueMeta
--		--select * from ctl.QueueMeta 
--		where QueueId in (select QueueId from @prcLog)
--	end
--end