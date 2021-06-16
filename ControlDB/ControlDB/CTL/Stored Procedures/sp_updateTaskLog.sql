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
	@RunId varchar(100),
	@Watermark varchar(max) = null,
	@Streaming bit = 0
as
begin
	declare @SourceId bigint = (select SourceId from ctl.Task where TaskId = @TaskId)
	declare @Status varchar(10) = case when @Output like '%ErrorCode%' then 'Failed' else 'Succeeded' end
	update  ctl.TaskLog
		set 
			[Status] = @Status,
			FullBlobName = @FullBlobName,
			[Output] = coalesce(case when @Output = '' then '{}' else @Output end,'{}'),
			[EndLogTime] = getdate(),
			[RunId] = @RunId
	where BatchId = @BatchId
		and TaskId = @TaskId
end
begin
	exec [CTL].[sp_getNextTask] 
		@BatchId, 
		@TaskId,
		@Streaming
end
begin
	if @Watermark is not null and @Status = 'Succeeded'
	begin
		exec [CTL].[sp_updateWatermarks] @SourceId = @SourceId, @Watermark = @Watermark
	end
end