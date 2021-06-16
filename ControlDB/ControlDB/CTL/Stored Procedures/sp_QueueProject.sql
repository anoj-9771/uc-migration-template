-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/10/2019
-- Description: Stored proc to place new items in the queue from a given Project
--==============================================
CREATE         procedure [CTL].[sp_QueueProject]  @Items varchar(max), @Streaming bit = null
AS
--Check Queue for existing tasks and add task to queue if it does not exist or status = 2
declare @projects int = (select json_value(@Items, '$.count')),
	@count int = 0,
	@ProjectId bigint,
	@ExecTime varchar(100)	
	
while @count < @projects
begin
	set @ProjectId = (select json_value(@Items, concat('$.value[', @count,'].ProjectId')))
	set @ExecTime = (select json_value(@Items, concat('$.value[', @count,'].ExecTime')))
	set @count = @count + 1
	declare @BatchId bigint = convert(bigint, replace([CTL].[udf_getFileDateHierarchy]('Second',@ExecTime), '/',''))

	begin
		exec [CTL].[sp_InitialQueue] @ProjectId, @BatchId, @Streaming
	end	
end
begin
	select @BatchId BatchId, @ExecTime ExecTime
end
--begin
--	exec [CTL].[sp_Dequeue]
--end