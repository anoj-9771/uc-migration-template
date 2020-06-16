-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/10/2019
-- Description: Stored proc to get next task in sequnce, place next task in queue
-- and remove current task from queue
--==============================================
CREATE     procedure [CTL].[sp_getNextTask] 
	@BatchId bigint, 
	@TaskId bigint
AS
--Get the next task Id in process
--Check status of previous task
declare @ProcessId bigint = (select ProcessID from [CTL].[Task] where TaskId = @TaskId)
declare @Status varchar(25)
declare @CurrentRank int = (select [Rank] from ctl.Task where TaskId = @TaskId)
declare @TaskId_nxt bigint
declare @MaxRank int = (select max([Rank]) 
							from [CTL].[Task] t 
							where ProcessId = @ProcessId
							group by ProcessId) 
declare @Process int = (select ProcessId from ctl.Task where TaskId = @TaskId)

set @Status = (
	select [Status]
	from (
		select row_number()over(partition by TaskId order by TaskLogId desc) RowNum, [Status] 
		from [CTL].[TaskLog] 
		where BatchId = @BatchId  
		and TaskId = @TaskId 
	) a
	where RowNum = 1
)


if @Status in ('Succeeded')--, 'Processing') 
	begin
		begin
			update qm 
			set status = 3
			--output inserted.QueueID, inserted.TaskId, inserted.ProcessPriority, inserted.ProjectPriority
			from ctl.QueueMeta qm
			--join [CTL].[udf_getStatus]() gs on qm.TaskId =gs.TaskId 
			where qm.TaskId = @TaskId
			and qm.BatchId = @BatchId
		end

		set @TaskId_nxt = (select case when (
				select t.TaskId
				from ctl.Task t
				where [Rank] = @CurrentRank + 1
				and ProcessId = @Process
			) is null then -1
			else  (
				select t.TaskId
				from ctl.Task t
				where [Rank] = @CurrentRank + 1
				and ProcessId = @Process
			)end)
	end
if @Status = 'Failed' 
	begin 
		begin
			update ctl.QueueMeta
			set status = (@MaxRank + 1 - @CurrentRank) * 3
			output inserted.QueueID, inserted.TaskId, inserted.ProcessPriority, inserted.ProjectPriority
			from ctl.QueueMeta
			where Status = 2
			and TaskId = @TaskId
		end
		set @TaskId_nxt = -1
	end
begin
	exec [CTL].[sp_Queue] @TaskId_nxt, @BatchId
end
begin
	delete from ctl.QueueMeta where TaskId = @TaskId and BatchId = @BatchId
end