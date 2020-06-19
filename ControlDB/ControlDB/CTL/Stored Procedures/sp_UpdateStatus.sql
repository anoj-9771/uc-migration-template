-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/10/2019
-- Description: Stored proc to place new items in the queue
--==============================================
CREATE       procedure [CTL].[sp_UpdateStatus] @TaskId bigint
AS
begin
	update ctl.QueueMeta with (updlock, readpast)
	set status = 2
	output inserted.QueueID, inserted.TaskId, inserted.ProcessPriority, inserted.ProjectPriority
	from ctl.QueueMeta
	where Status = 1
	and TaskId = @TaskId
end