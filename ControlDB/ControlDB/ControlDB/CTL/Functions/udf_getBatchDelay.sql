-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/10/2019
-- Description: Get the amount of time to delay before processing new batch
--==============================================

CREATE      function [CTL].[udf_getBatchDelay](@BatchId bigint)
returns int
as	
begin
	declare @Worker table (Row_Num int, WorkerId int, WorkerBatchSize int) 
	--Populate @Worker snapshot table
	delete from @Worker
	insert into @Worker select RowNum, WorkerId, WorkerBatchSize from [CTL].[udf_GetWorkerExeLimit]()
	--Declare variables
	declare @wc int 
	declare @w int
	declare @WorkerId int
	declare @WorkerLimit int
	declare @Workers int 
	declare @WorkerBatchSize int
	declare @DataStore table (Row_Num int, SourceDataStoreId bigint, TargetDataStoreId bigint, Source_Limit int, Target_Limit int, DS_AllocatedUnits int)		
	declare @Tasks table(QueueId bigint, TaskID bigint)
	declare @dsc int 
	declare @ds int 
	declare @Source_Limit int 
	declare @Target_Limit int
	declare @DS_AllocatedUnits int
	declare @Source_DataStoreId int
	declare @Target_DataStoreId int
	declare @Source_BatchSize int 
	declare @Target_BatchSize int
	declare @DSBatchSize int
	declare @BatchSize int
	declare @bs int = 0
	declare @tc int = 0
	declare @Batch table (QueueID int, TaskId bigint, ProjectPriority int, ProcessPriority int)
	declare @TaskCount int
	declare @dsCount int = 0

	set @wc = (select count(*) from @Worker)
	set @w = 1
	set @WorkerBatchSize = (select sum(WorkerBatchSize) from @Worker)
	delete from @Tasks

	while @w <= @wc and @WorkerBatchSize > 0
	begin
		set @WorkerId = (select WorkerId from @Worker where Row_Num = @w)
		--Clear Dataset Table
		delete from @DataStore
		--insert datastore dataset into temp table
		insert into @DataStore select * from [CTL].[udf_GetDataStoreExeLimit](@WorkerId) 

		set @dsc = (select count(*) from @DataStore)
		set @ds = 1
		set @WorkerBatchSize = (select WorkerBatchSize from [CTL].[udf_GetWorkerExeLimit]() where WorkerId = @WorkerId)
		set @DSBatchSize = 1
		while @ds <= @dsc  --and @DSBatchSize > 0
		begin
			set @WorkerBatchSize = (select WorkerBatchSize from [CTL].[udf_GetWorkerExeLimit]() where WorkerId = @WorkerId)
			set @Source_Limit = (select Source_Limit from @DataStore where Row_Num = @ds)
			set @Target_Limit = (select Target_Limit from @DataStore where Row_Num = @ds)
			set @DS_AllocatedUnits = (select DS_AllocatedUnits from @DataStore where Row_Num = @ds)
			set @Source_DataStoreId = (select SourceDataStoreId from @DataStore where Row_Num = @ds)
			set @Target_DataStoreId = (select TargetDataStoreId from @DataStore where Row_Num = @ds)
			set @Source_BatchSize = case when (@Source_Limit - @DS_AllocatedUnits) < 0 then 0 else (@Source_Limit - @DS_AllocatedUnits) end
			set @Target_BatchSize = case when (@Target_Limit - @DS_AllocatedUnits) < 0 then 0 else (@Target_Limit - @DS_AllocatedUnits) end
			set @DSBatchSize = case when @Source_BatchSize <= @Target_BatchSize then @Source_BatchSize else @Target_BatchSize end
			set @BatchSize = case when @WorkerBatchSize <= @DSBatchSize then @WorkerBatchSize else @DSBatchSize end
			set @TaskCount = (			
				select count(*)
					from ctl.QueueMeta qm
					join ctl.Task tsk on qm.TaskId = tsk.TaskId
					join ctl.Process prc on tsk.ProcessId = prc.ProcessId
					join ctl.DataSet dst_src on tsk.SourceId = dst_src.DataSetId
					join ctl.DataSet dst_trg on tsk.TargetId = dst_trg.DataSetId
					join ctl.DataStore dsr_src on dst_src.DataStoreId = dsr_src.DataStoreId
					join ctl.DataStore dsr_trg on dst_trg.DataStoreId = dsr_trg.DataStoreId
					where qm.Status = 0 
					and tsk.WorkerId = @WorkerId
					and dst_src.DataStoreId = @Source_DataStoreId
					and dst_trg.DatastoreId = @Target_DataStoreId
					and (select count(*) from [CTL].[udf_split_table](prc.ProjectDependency, ',') 
						where Dependents in (select distinct p.ProjectId 
						from ctl.QueueMeta q
						join ctl.Task t on q.TaskId = t.TaskId
						join ctl.Process p on t.ProcessId = p.ProcessId)) = 0
					and (select count(*) from [CTL].[udf_split_table](prc.ProcessDependency, ',') 
						where Dependents in (select distinct p.ProcessId 
						from ctl.QueueMeta q
						join ctl.Task t on q.TaskId = t.TaskId
						join ctl.Process p on t.ProcessId = p.ProcessId)) = 0
					and qm.BatchID = @BatchId)
			set @bs += case when @BatchSize < @TaskCount then @BatchSize 
				when @TaskCount = 0 or @BatchSize = 0 then 0
				else @TaskCount end
			set @tc += @TaskCount
			--print concat('Worker size: ',@WorkerBatchSize)
			--print concat('Task count: ',@TaskCount)
			--print concat('Woker count: ',@wc)
			--print concat('batch size: ', @BatchSize)
			--print concat('Accumulated batch size: ', @bs)
			
			set @DSBatchSize -= @BatchSize
			set @WorkerBatchSize -= @DSBatchSize
			set @ds+= 1 
			set @dsCount += 1
		end	
		set @w += 1 
	end
	--print concat('Accumulated Task Count: ',@tc)
	--print concat('Task count: ',@TaskCount)
	--print concat('Woker count: ',@wc)
	--print concat('Avg task size: ',  ceiling(@tc*1.0/@dsCount))
	--print concat('avg Batch Size: ', ceiling(@bs*1.0/@dsCount) )
	return case when @bs < @tc then ceiling(@bs*1.0/@dsCount) when @tc = 0 or @bs = 0 then 0 else ceiling(@tc*1.0/@dsCount) end
end