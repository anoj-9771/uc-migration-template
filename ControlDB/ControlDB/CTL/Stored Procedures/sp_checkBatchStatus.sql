-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/10/2019
-- Description: Returns the Project Task count as count or 0 if project is taking 3 time as long to complete 
-- and updates log status to incomplete if. 
--==============================================
CREATE       Procedure [CTL].[sp_checkBatchStatus] @BatchId bigint, @Streaming bit = 0
As
declare @maxRank table (ProcessId bigint, MaxRank int)
declare @delay varchar(5)
declare @batchSize int 
declare @tskCount int
declare @avgPrjDuration int
declare @curPrjDuration int
declare @minBatchSize int 

if @Streaming = 1
begin
	insert into @maxRank 
		select ProcessId, max(rank) 
		from ctl.Task tsk
		group by ProcessId	
		
	set @tskCount = (
		select distinct count(*)
		from ctl.QueueMeta_Stream	qms
		join ctl.Task tsk on tsk.TaskId = qms.TaskId
		join @maxRank mr on tsk.ProcessId = mr.ProcessId 
		where BatchId = @BatchId 
		and qms.status < 2 	
		or (
				qms.status >= 2 and 
				tsk.rank < mr.MaxRank
			)	
	)
	set @batchSize = (
			select count(*) 
			from ctl.QueueMeta_Stream qms
			where qms.Status < 2
		)
	set @minBatchSize = 1
	
	while @batchSize <  @minBatchSize and  @tskCount > 0 
	begin
		waitfor delay '00:00:01'
		set @tskCount =  (
			select distinct count(*)
			from ctl.QueueMeta_Stream	qms
			join ctl.Task tsk on tsk.TaskId = qms.TaskId
			join @maxRank mr on tsk.ProcessId = mr.ProcessId 
			where BatchId = @BatchId 
			and qms.status < 2 	
			or (
				qms.status >= 2 and 
				tsk.rank < mr.MaxRank
			)	
		)
		set @batchSize = (
			select count(*) 
			from ctl.QueueMeta_Stream qms
			where qms.Status < 2
			and BatchId = @BatchId
		)
	end
end
if @Streaming = 0
begin
	set @batchSize = (select [CTL].[udf_getBatchDelay](@BatchId))
	insert into @maxRank 
		select ProcessId, max(rank) 
		from ctl.Task tsk
		group by ProcessId	

	set @tskCount = (
		select distinct count(*)
		from ctl.QueueMeta	qm
		join ctl.Task tsk on tsk.TaskId = qm.TaskId
		left join @maxRank mr on tsk.ProcessId = mr.ProcessId and tsk.rank < mr.MaxRank
		where qm.BatchId = @BatchId 
		and qm.status <= 3 
	)
		
	set @minBatchSize = 1

	while @batchSize <  @minBatchSize and  @tskCount > 0 
	begin
		waitfor delay '00:00:01'	
		set @tskCount =  (
			select distinct count(*)
			from ctl.QueueMeta	qms
			join ctl.Task tsk on tsk.TaskId = qms.TaskId
			join @maxRank mr on tsk.ProcessId = mr.ProcessId 
			where BatchId = @BatchId 
			and qms.status < 2 	
			or (
				qms.status >= 2 and 
				tsk.rank < mr.MaxRank
			)	
		)
		set @batchSize = (select [CTL].[udf_getBatchDelay](@BatchId))
	end
end
select @tskCount TaskCount