CREATE   proc [CTL].[sp_getFusionProcesses]
@json nvarchar(max),
@stage nvarchar(100),
@batch bigint = null
as
begin
	declare @MetaData table (BatchId bigint, ExecTime datetime2, SourceName varchar(250))
	declare @BatchId table (RowNum int, BatchId bigint, ExecTime datetime2) 
	declare @TaskId table (RowNum int, TaskId bigint, DatasetName varchar(500)) 
	declare @DatasetName nvarchar(250)
	declare @Path nvarchar(2000)
	declare @count int =1
	declare @max int
	declare @t_count int =1
	declare @t_max int
	declare @execTime datetime2 = [CTL].[udf_BatchIdtoDatetime](@batch)
	declare @t bigint

	delete from @MetaData
	delete from @TaskId

	insert into @MetaData
	select distinct cast(right(left(SourceName, len(SourceName) - 11),8) + '000000' as bigint) BatchId,
		[CTL].[udf_BatchIdtoDatetime](cast(right(left(SourceName, len(SourceName) - 11),8) + '000000' as bigint)) ExecTIme, 
		SourceName
	from openjson(@json)
	with(
		SourceName nvarchar(max) '$.name'
	)

	
	if @stage = 'getBatches'
	begin
		select distinct BatchId, ExecTime
		from @MetaData
		where BatchId >= coalesce(@batch, 19990101000000)
		order by BatchId asc
	end
	
	if @stage = 'processBatch'
	begin
		delete from @TaskId
		exec [CTL].[sp_InitialQueue] @ProjectId = 8, @BatchId = @batch

		insert into @TaskId (RowNum, TaskId, DatasetName)
		select row_number()over(order by (select 1)), qm.TaskId, dst.Name
		from ctl.QueueMeta qm
		join ctl.Task tsk on qm.TaskId = tsk.TaskId
		join ctl.DataSet dst on tsk.SourceId = dst.DataSetId 
		where tsk.StageId = 1 and batchId = @batch
		--and dst.Name = 'C'

		select @t_max =  max(RowNum) from @TaskId 
		while @t_count <= @t_max
		begin
			select @t = TaskId from @TaskId where RowNum = @t_count
			select @DatasetName = DatasetName from @TaskId where RowNum = @t_count
			select @Path = 'OracleFusion/' + @DatasetName + [CTL].[udf_getFileDateHierarchy]('day',@execTime)
			update ctl.QueueMeta set Status = 2 where TaskId = @t and BatchId = @batch
			exec [CTL].[sp_logTasks] @batch, @t, @Path
			set @t_count +=1
		end

		select qm.TaskId,
			qm.BatchId,
			md.ExecTime,
			md.SourceName,
			dst.Location, 
			'OracleFusion/' + dst.Location + [CTL].[udf_getFileDateHierarchy]('day',md.ExecTime) TargetBlobPath
		from ctl.QueueMeta qm
		join ctl.Task tsk on qm.TaskId = tsk.TaskId
		join ctl.DataSet dst on tsk.SourceId = dst.DataSetId 
		join @MetaData md on qm.BatchId = md.BatchId and md.SourceName like dst.Name + '%'
		where DataStoreId = 14
		and md.BatchId = @batch
	end
end