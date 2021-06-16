create   proc ctl.sp_deletsQueueTask
@TaskId bigint
as
begin
	declare @batch_id bigint
	declare @source_id bigint

	select @batch_id = batchId from ctl.QueueMeta where TaskId = @TaskId

	delete from ctl.QueueMeta where TaskId = @TaskId and BatchId = @batch_id

	update ctl.TaskLog set status = 'Succeeded', 
		Output = '{"message": "No files to be processed"}' 
	where TaskId = @TaskId and BatchId = @batch_id
end