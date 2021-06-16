-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/10/2019
-- Description: Stored proc to place new items in the queue
--==============================================
CREATE     procedure [CTL].[sp_Queue] @TaskId bigint, @BatchId bigint, @Streaming bit = 0
AS

--Check Queue for existing task
declare @TaskCount int 
if @Streaming = 1 
begin
	set @TaskCount = (select count(*) 
					from ctl.QueueMeta_Stream 
					where TaskId = @TaskId 
					and BatchId = @BatchId
					and status > 0 )

	if (@TaskCount = 0)
	begin
		insert into ctl.QueueMeta_Stream
		select t.TaskId, 0, @BatchId
		from ctl.Task t
		join ctl.Process pc on t.ProcessId = pc.ProcessId
		join ctl.Project pj on pc.ProjectId = pj.ProjectId
		where t.TaskId = @TaskId
	end
end
if @Streaming = 0
begin
	--Check Queue for existing task
	set @TaskCount = (select count(*) 
					from ctl.QueueMeta 
					where TaskId = @TaskId 
					and BatchId = @BatchId
					and status > 0 )

	if (@TaskCount = 0)
	begin
		insert into ctl.QueueMeta
		select t.TaskId, pj.Priority, pc.Priority, 0, @BatchId
		from ctl.Task t
		join ctl.Process pc on t.ProcessId = pc.ProcessId
		join ctl.Project pj on pc.ProjectId = pj.ProjectId
		where t.TaskId = @TaskId
	end
end