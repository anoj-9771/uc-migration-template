-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/10/2019
-- Description: Stored proc to execute dequeue or purge queue

-- Update: Delete Project tasks from Queue once a all task have completed
-- Removed TaskId check, moved logic to ProcessProject Pipeline
--==============================================
CREATE       procedure [CTL].[sp_PurgeQueue]  @BatchId bigint, @Streaming bit = null
AS

-- Delete Dependent tasks
if @Streaming = 1
begin
	delete qms
	from ctl.QueueMeta_Stream qms
	join ctl.Task tsk on qms.TaskId = tsk.TaskId
	join ctl.Process prc on tsk.ProcessId = prc.ProcessId
	where qms.BatchId = @BatchId
end 
else
begin
	delete qm
	from ctl.QueueMeta qm
	join ctl.Task tsk on qm.TaskId = tsk.TaskId
	join ctl.Process prc on tsk.ProcessId = prc.ProcessId
	where qm.BatchId = @BatchId
end 
begin 
	update ctl.TaskLog set Status = 'Incomplete'
	where Status = 'Processing' and BatchId = @BatchId
end