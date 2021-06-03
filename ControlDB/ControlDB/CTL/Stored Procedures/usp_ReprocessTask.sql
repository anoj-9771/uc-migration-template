-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/10/2019
-- Description: Inserts Current and previous task in Queue for Reprocessing
--==============================================
CREATE   proc [CTL].[usp_ReprocessTask] @TaskId bigint
as
declare @rank_max int = (select [Rank] from [CTL].[Task] where TaskId = @TaskId),
	@rank_current int = 1,
	@ProcessId int = (select ProcessId from [CTL].[Task] where TaskId = @TaskId)

begin
	while @rank_current <= @rank_max
	begin
		declare @TaskId_current int = (select TaskId from [CTL].[Task] where [Rank] = @rank_current and ProcessId = @ProcessId)
		if (select count(*) from ctl.QueueMeta where TaskId = @TaskId_current) = 0
		begin
			insert into [CTL].[QueueMeta]
			select t.TaskId,
				1,
				1,
				case when @rank_current < @rank_max then 2 else 0 end [Status],
				-9999
			from [CTL].[Task] t
			join [CTL].[Process] prc on t.ProcessId = prc.ProcessId
			where t.ProcessId = @ProcessId and t.[Rank] = @rank_current
		end
		begin
			insert into [CTL].[TaskLog] ([BatchId], [Status], [TaskId], [FullBlobName], [Output], [RunID])
			select -9999,
				'Succeeded',
				t.TaskId,
				'Full blob path to be updated later',
				'{"message" : "To be updated later to include details of re-run"}',
				'Full runId to be updated later'
			from [CTL].[Task] t
			join [CTL].[Process] prc on t.ProcessId = prc.ProcessId
			where t.ProcessId = @ProcessId and t.[Rank] = @rank_current
		end
		set @rank_current = @rank_current + 1
	end
end