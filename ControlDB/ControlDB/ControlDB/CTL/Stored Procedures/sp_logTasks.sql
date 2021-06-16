-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/10/2019
-- Description: Insert Task output into log from ADF
-- Update added Worker Type to list
--==============================================
CREATE   proc [CTL].[sp_logTasks] @BatchId bigint, @TaskId bigint, @FullBlobName varchar(500) 
as
begin
	if (select count(*) from ctl.TaskLog where BatchId = @BatchId and TaskId = @TaskId and FullBlobName = FullBlobName) = 0 
	begin
		insert into ctl.TaskLog (BatchId, [RunId], [Status],TaskId, FullBlobName, [Output],[InitialLogTime])
		values (
			@BatchId,
			'Waiting',
			'Processing',
			@TaskId,
			@FullBlobName,
			'{}',
			getdate()
		)
	end
end