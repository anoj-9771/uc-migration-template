-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/10/2019
-- Description: Stored proc to order queue

-- Update: The commented out script runs faster however, there is no precise ordering performed for priority batch selection
-- Update: Removed uncommented fater running script
--==============================================
CREATE   procedure [CTL].[sp_Dequeue]
AS
SET ANSI_WARNINGS OFF
set nocount on
-- Declare worker and datastore snapshot tables
declare @Worker table (Row_Num int, WorkerId int) 
--Populate @Worker snapshot table
delete from @Worker
insert into @Worker select RowNum, WorkerId from [CTL].[udf_GetWorkerExeLimit]()

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

declare @Batch table (QueueID int, TaskId bigint, ProjectPriority int, ProcessPriority int)


set @wc = (select count(*) from @Worker)
set @w = 1
delete from @Tasks

while @w <= @wc
begin

	set @WorkerId = (select WorkerId from @Worker where Row_Num = @w)
	--Clear Dataset Table
	delete from @DataStore
	--insert datastore dataset into temp table
	set nocount on
	insert into @DataStore select * from [CTL].[udf_GetDataStoreExeLimit](@WorkerId) 

	set @dsc = (select count(*) from @DataStore)
	set @ds = 1

	while @ds <= @dsc
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

		delete from @Batch

		begin tran

		insert into @Batch
		select Top (case when @BatchSize is null then 0 else @BatchSize end) QueueID, qm.TaskId, ProjectPriority, ProcessPriority 
		from ctl.QueueMeta qm
		join ctl.Task tsk on qm.TaskId = tsk.TaskId
		join ctl.Process prc on tsk.ProcessId = prc.ProcessId
		join ctl.DataSet dst_src on tsk.SourceId = dst_src.DataSetId
		join ctl.DataSet dst_trg on tsk.TargetId = dst_trg.DataSetId
		--WITH (UPDLOCK, HOLDLOCK)
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
		--and qm.TaskId in (select distinct TaskId from @ProcessStatus where ProcessStatus = 1)
		order by QueueId asc, ProjectPriority asc, ProcessPriority asc
		declare @ItemsToUpdate int
		set @ItemsToUpdate = @@ROWCOUNT

		update ctl.QueueMeta
		SET Status = 1
		WHERE QueueID IN (select QueueID from @Batch)	
		AND Status = 0

		if @@ROWCOUNT = @ItemsToUpdate
		begin
			commit tran
			--select * from @Batch
			print 'SUCCESS'
		end
		else
		begin
			rollback tran
			print 'FAILED'
		end
		set @ds = @ds + 1 
	end	
	set @w = @w + 1 	
end