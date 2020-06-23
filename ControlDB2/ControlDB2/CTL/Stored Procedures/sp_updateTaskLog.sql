-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/10/2019
-- Description: Insert Task output into log from ADF
-- Update added Worker Type to list
--==============================================
CREATE     proc [CTL].[sp_updateTaskLog] 
	@BatchId bigint,
	@TaskId bigint, 
	@FullBlobName varchar(500), 
	@Output varchar(max), 
	@RunId varchar(100)
as
begin
	update  ctl.TaskLog
		set 
			[Status] = case when @Output like '%ErrorCode%' then 'Failed' else 'Succeeded' end,
			FullBlobName = @FullBlobName,
			[Output] = coalesce(case when @Output = '' then '{}' else @Output end,'{}'),
			[EndLogTime] = [CTL].[udf_getWAustDateTime](getdate()),
			[RunId] = @RunId
	where BatchId = @BatchId
		and TaskId = @TaskId
end
begin
	exec [CTL].[sp_getNextTask] 
		@BatchId, 
		@TaskId
end